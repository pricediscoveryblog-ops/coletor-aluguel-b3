#!/usr/bin/env python3
"""
Coletor diário – empréstimo de ações (BTB) e dados de apoio – Boletim Diário da B3.

Fonte: https://arquivos.b3.com.br/bdi/tabelas
Endpoint: POST /bdi/table/{tabela}/{data}/{data}/{pagina}/{tamanho}  com corpo "{}"

- A B3 mantém as tabelas estruturadas por ~21 dias úteis (D-21) e publica o
  dia D por volta das 8h de D+1.
- Tabelas "diárias": baixa todo dia útil da janela que ainda não esteja salvo
  (backfill automático e autocorreção de dias perdidos).
- Tabelas "ultimo": guarda só o retrato mais recente disponível (cadastro).
- Saída: dados/<tabela>/<AAAA>/<AAAA-MM-DD>.parquet + _colunas.json
- Integridade (v2.1): toda página é conferida (páginas intermediárias devem vir
  com exatamente TAMANHO_PAGINA linhas; a última com 1 a TAMANHO_PAGINA). A B3
  às vezes responde "200 OK" com página vazia ou incompleta; nesse caso o
  coletor repete a requisição. O controle de cada arquivo salvo (páginas e
  linhas brutas) fica em dados/_controle.json. Arquivos da janela sem registro
  de controle são verificados e, se incompletos, baixados de novo.
"""

import datetime as dt
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

BASE = "https://arquivos.b3.com.br/bdi/table"
TAMANHO_PAGINA = 1000   # máximo aceito pela B3
JANELA_DIAS = 35        # dias corridos para trás (cobre ~21 dias úteis)
THREADS = 3             # requisições simultâneas (moderado, para não sobrecarregar a B3)
TENTATIVAS = 5

CATEGORIAS_EXCLUIDAS = ("OPTION", "FORWARD", "INDEX")

TABELAS = [
    # código B3,                 pasta,                      modo,     filtro
    ("BTBLendingOpenPosition",     "posicoes_em_aberto",       "diario", None),
    ("BTBLoanBalance",             "emprestimos_registrados",  "diario", None),
    ("BTBTrade",                   "negocios",                 "diario", None),
    ("Renewals",                   "renovacoes",               "diario", None),
    ("ConsolidatedTradesEquities", "negociacao_a_vista",       "diario",
        lambda df: df[df["SgmtNm"] == "CASH"]),
    ("InstrumentsEquities",        "cadastro_instrumentos",    "ultimo",
        lambda df: df[~df["SctyCtgyNm"].fillna("").str.contains("|".join(CATEGORIAS_EXCLUIDAS))]),
]

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("coletor")

sessao = requests.Session()
sessao.headers.update({"Content-Type": "application/json",
                       "User-Agent": "Mozilla/5.0 (coletor-aluguel-b3)"})


def requisitar(tabela: str, data: str, pagina: int) -> dict:
    url = f"{BASE}/{tabela}/{data}/{data}/{pagina}/{TAMANHO_PAGINA}"
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            r = sessao.post(url, data="{}", timeout=90)
            if r.status_code == 200:
                return r.json()["table"]
            log.warning("HTTP %s em %s (tentativa %d)", r.status_code, url, tentativa)
        except (requests.RequestException, ValueError, KeyError) as e:
            log.warning("Erro em %s (tentativa %d): %s", url, tentativa, e)
        time.sleep(5 * tentativa)
    raise RuntimeError(f"Falha após {TENTATIVAS} tentativas: {url}")


def pagina_valida(tabela: str, data: str, pagina: int, n_paginas: int) -> dict:
    """Baixa uma página e confere o tamanho; repete se vier vazia/incompleta."""
    for tentativa in range(1, TENTATIVAS + 1):
        t = requisitar(tabela, data, pagina)
        valores = t.get("values") or []
        pc = t.get("pageCount") or 0
        if pagina < n_paginas:
            ok = len(valores) == TAMANHO_PAGINA and pc == n_paginas
        else:
            ok = 1 <= len(valores) <= TAMANHO_PAGINA and pc == n_paginas
        if ok:
            return t
        log.warning("%s %s pág. %d/%d incompleta (%d linhas, pageCount %s) – tentativa %d",
                    tabela, data, pagina, n_paginas, len(valores), pc, tentativa)
        time.sleep(5 * tentativa)
    raise RuntimeError(f"Página incompleta após {TENTATIVAS} tentativas: {tabela} {data} pág. {pagina}")


def contar_paginas(tabela: str, data: str) -> int:
    """Número de páginas do dia; consulta 3 vezes e fica com o maior (resposta vazia pode ser falha)."""
    maior = 0
    for _ in range(3):
        t = requisitar(tabela, data, 1)
        pc = t.get("pageCount") or 0
        if not (t.get("values") or []):
            pc = 0
        maior = max(maior, pc)
        if maior:
            break
        time.sleep(3)
    return maior


def baixar(tabela: str, data: str):
    """Retorna (DataFrame, metadados, n_paginas, linhas_brutas) ou (None, None, 0, 0)."""
    n_paginas = contar_paginas(tabela, data)
    if n_paginas == 0:
        return None, None, 0, 0

    t1 = pagina_valida(tabela, data, 1, n_paginas)
    meta = t1.get("columns") or []
    nomes = [c.get("name") or f"c{i}" for i, c in enumerate(meta)]

    linhas = list(t1["values"])
    if n_paginas > 1:
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            for t in ex.map(lambda p: pagina_valida(tabela, data, p, n_paginas),
                            range(2, n_paginas + 1)):
                linhas.extend(t["values"])

    minimo = (n_paginas - 1) * TAMANHO_PAGINA + 1
    maximo = n_paginas * TAMANHO_PAGINA
    if not (minimo <= len(linhas) <= maximo):
        raise RuntimeError(f"{tabela} {data}: {len(linhas)} linhas fora do esperado ({minimo}–{maximo})")

    if len(nomes) != len(linhas[0]):
        log.warning("%s %s: %d colunas vs %d valores; nomes genéricos",
                    tabela, data, len(nomes), len(linhas[0]))
        nomes = [f"c{i}" for i in range(len(linhas[0]))]

    vistos, unicos = {}, []
    for n in nomes:
        vistos[n] = vistos.get(n, 0) + 1
        unicos.append(n if vistos[n] == 1 else f"{n}_{vistos[n]}")

    df = pd.DataFrame(linhas, columns=unicos)
    df.insert(0, "data_pregao", data)
    return df, meta, n_paginas, len(linhas)


def salvar_dicionario(pasta: Path, meta: list) -> None:
    dicionario = [{"codigo": c.get("name"), "nome_pt": c.get("friendlyNamePt"),
                   "nome_en": c.get("friendlyNameEn"), "tooltip_pt": c.get("tooltipPt")}
                  for c in meta]
    (pasta / "_colunas.json").write_text(
        json.dumps(dicionario, ensure_ascii=False, indent=2), encoding="utf-8")


def dias_candidatos() -> list:
    hoje = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    return [(hoje - dt.timedelta(days=k)).isoformat()
            for k in range(JANELA_DIAS, 0, -1)
            if (hoje - dt.timedelta(days=k)).weekday() < 5]


CONTROLE = DADOS / "_controle.json"


def ler_controle() -> dict:
    if CONTROLE.exists():
        return json.loads(CONTROLE.read_text(encoding="utf-8"))
    return {}


def gravar_controle(ctrl: dict) -> None:
    CONTROLE.parent.mkdir(parents=True, exist_ok=True)
    CONTROLE.write_text(json.dumps(ctrl, ensure_ascii=False, indent=1, sort_keys=True),
                        encoding="utf-8")


def arquivo_integro(tabela, pasta_nome, filtro, data, destino, ctrl) -> bool:
    """Confere arquivo existente sem registro de controle. True = pode manter."""
    if data in ctrl.get(pasta_nome, {}):
        return True
    if filtro is not None:
        # tabelas filtradas não guardam a contagem bruta: baixa de novo por segurança
        log.info("%s %s: sem registro de controle, será baixado de novo", pasta_nome, data)
        return False
    try:
        n_pag = contar_paginas(tabela, data)
    except RuntimeError as e:
        log.error(str(e))
        return True  # não conseguiu verificar agora; mantém e tenta na próxima execução
    if n_pag == 0:
        return True  # fora da janela da B3; nada a comparar
    salvas = len(pd.read_parquet(destino, columns=["data_pregao"]))
    minimo = (n_pag - 1) * TAMANHO_PAGINA + 1
    if minimo <= salvas <= n_pag * TAMANHO_PAGINA:
        ctrl.setdefault(pasta_nome, {})[data] = {"paginas": n_pag, "linhas_brutas": salvas,
                                                 "verificado": True}
        return True
    log.warning("%s %s: arquivo incompleto (%d linhas, esperado >= %d); será baixado de novo",
                pasta_nome, data, salvas, minimo)
    return False


def processar(tabela, pasta_nome, filtro, data, ctrl) -> str:
    """Baixa e salva um dia. Retorna 'novo', 'existe', 'vazio' ou 'falha'."""
    pasta = DADOS / pasta_nome
    destino = pasta / data[:4] / f"{data}.parquet"
    if destino.exists() and arquivo_integro(tabela, pasta_nome, filtro, data, destino, ctrl):
        return "existe"
    try:
        df, meta, n_pag, brutas = baixar(tabela, data)
    except RuntimeError as e:
        log.error(str(e))
        return "falha"
    if df is None:
        log.info("%s %s: sem dados (feriado ou fora da janela da B3)", pasta_nome, data)
        return "vazio"
    if filtro is not None:
        df = filtro(df)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(destino, index=False)
    salvar_dicionario(pasta, meta)
    ctrl.setdefault(pasta_nome, {})[data] = {"paginas": n_pag, "linhas_brutas": brutas,
                                             "linhas_salvas": len(df)}
    gravar_controle(ctrl)
    log.info("%s %s: %d linhas salvas (%d brutas, %d páginas)",
             pasta_nome, data, len(df), brutas, n_pag)
    return "novo"


def main() -> int:
    dias = dias_candidatos()
    ctrl = ler_controle()
    contagem = {"novo": 0, "falha": 0}
    for tabela, pasta_nome, modo, filtro in TABELAS:
        if modo == "diario":
            for data in dias:
                st = processar(tabela, pasta_nome, filtro, data, ctrl)
                contagem[st] = contagem.get(st, 0) + 1
        else:  # "ultimo": só o dia mais recente disponível
            for data in reversed(dias):
                st = processar(tabela, pasta_nome, filtro, data, ctrl)
                contagem[st] = contagem.get(st, 0) + 1
                if st in ("novo", "existe", "falha"):
                    break
    gravar_controle(ctrl)
    log.info("Fim: %s", contagem)
    return 1 if contagem["falha"] else 0


if __name__ == "__main__":
    sys.exit(main())
