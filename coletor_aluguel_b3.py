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
THREADS = 4             # requisições simultâneas (moderado, para não sobrecarregar a B3)
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


def baixar(tabela: str, data: str):
    """Retorna (DataFrame, metadados das colunas) ou (None, None) se não houver dados."""
    t = requisitar(tabela, data, 1)
    n_paginas = t.get("pageCount") or 0
    valores = t.get("values") or []
    if n_paginas == 0 or not valores:
        return None, None

    meta = t.get("columns") or []
    nomes = [c.get("name") or f"c{i}" for i, c in enumerate(meta)]

    linhas = list(valores)
    if n_paginas > 1:
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            for parte in ex.map(lambda p: requisitar(tabela, data, p).get("values") or [],
                                range(2, n_paginas + 1)):
                linhas.extend(parte)

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
    return df, meta


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


def processar(tabela, pasta_nome, filtro, data) -> str:
    """Baixa e salva um dia. Retorna 'novo', 'existe', 'vazio' ou 'falha'."""
    pasta = DADOS / pasta_nome
    destino = pasta / data[:4] / f"{data}.parquet"
    if destino.exists():
        return "existe"
    try:
        df, meta = baixar(tabela, data)
    except RuntimeError as e:
        log.error(str(e))
        return "falha"
    if df is None:
        log.info("%s %s: sem dados (feriado ou ainda não publicado)", pasta_nome, data)
        return "vazio"
    if filtro is not None:
        df = filtro(df)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(destino, index=False)
    salvar_dicionario(pasta, meta)
    log.info("%s %s: %d linhas salvas", pasta_nome, data, len(df))
    return "novo"


def main() -> int:
    dias = dias_candidatos()
    contagem = {"novo": 0, "falha": 0}
    for tabela, pasta_nome, modo, filtro in TABELAS:
        if modo == "diario":
            for data in dias:
                st = processar(tabela, pasta_nome, filtro, data)
                contagem[st] = contagem.get(st, 0) + 1
        else:  # "ultimo": só o dia mais recente disponível
            for data in reversed(dias):
                st = processar(tabela, pasta_nome, filtro, data)
                contagem[st] = contagem.get(st, 0) + 1
                if st in ("novo", "existe", "falha"):
                    break
    log.info("Fim: %s", contagem)
    return 1 if contagem["falha"] else 0


if __name__ == "__main__":
    sys.exit(main())
