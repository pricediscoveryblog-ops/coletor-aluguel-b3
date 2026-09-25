#!/usr/bin/env python3
"""
Coletor diário – empréstimo de ações (BTB) e dados de apoio – Boletim Diário da B3.
Versão 3.1: download pelo CSV de exportação (tabela inteira, sem paginação).

Por que CSV: a consulta paginada da B3 (/bdi/table/.../{pagina}/{tamanho}) devolve
as linhas em ordem instável; as páginas se sobrepõem e parte dos dados some
(ex.: 23/09/2026, negociação: 73.868 linhas recebidas, só 45.751 distintas).
O endpoint de exportação (/bdi/table/export/csv) entrega a tabela completa.

Fluxo por tabela e dia:
  1. Metadados: POST /bdi/table/{tabela}/{d}/{d}/1/1 -> códigos e nomes das colunas
     e o total de linhas (pageCount com tamanho 1).
  2. Dados: POST /bdi/table/export/csv?lang=pt-BR -> CSV completo.
  3. Converte números no formato BR (1.234,56 e 0,25%), datas dd/mm/aaaa e "-" vazio;
     renomeia as colunas para os mesmos códigos da versão anterior (RptDt, TckrSymb…).
  4. Confere: linhas do CSV == total informado pela B3. Se não bater, tenta de novo.
     (v3.1) Se o CSV vier com linhas a mais, remove duplicadas/vazias; aceita diferença
     residual de até 1 linha (visto no cadastro de instrumentos), com aviso no log.

- A B3 mantém as tabelas por ~21 dias úteis (D-21) e publica D por volta das 8h de D+1.
- Tabelas "diárias": baixa todo dia útil da janela ainda não salvo pela v3.
- Tabela "ultimo" (cadastro): guarda só o retrato mais recente disponível.
- Controle: dados/_controle.json registra linhas e fonte de cada arquivo. Arquivos da
  janela que não vieram do CSV (versões 1 e 2) são baixados de novo automaticamente.
"""

import csv
import datetime as dt
import io
import json
import logging
import re
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

BASE = "https://arquivos.b3.com.br/bdi/table"
JANELA_DIAS = 35        # dias corridos para trás (cobre ~21 dias úteis)
TENTATIVAS = 5
TENTATIVAS_CSV = 3      # a B3 às vezes não gera CSVs grandes (HTTP 499/504); cada tentativa leva ~4 min
TOLERANCIA_LINHAS = 1
PAUSA = 1.0             # segundos entre downloads
FONTE = "csv-v3"

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
        lambda df: df[~df["SctyCtgyNm"].fillna("").astype(str)
                      .str.contains("|".join(CATEGORIAS_EXCLUIDAS))]),
]

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"
CONTROLE = DADOS / "_controle.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("coletor")

sessao = requests.Session()
sessao.headers.update({"Content-Type": "application/json",
                       "Accept": "application/json",
                       "User-Agent": "Mozilla/5.0 (coletor-aluguel-b3)"})


# ---------------------------------------------------------------- acesso à B3
def post(url: str, corpo: str, timeout: int = 180, tentativas: int = TENTATIVAS) -> requests.Response:
    for tentativa in range(1, tentativas + 1):
        try:
            r = sessao.post(url, data=corpo, timeout=timeout)
            if r.status_code == 200:
                return r
            log.warning("HTTP %s em %s (tentativa %d)", r.status_code, url, tentativa)
        except requests.RequestException as e:
            log.warning("Erro em %s (tentativa %d): %s", url, tentativa, e)
        time.sleep(5 * tentativa)
    raise RuntimeError(f"Falha após {tentativas} tentativas: {url}")


def metadados(tabela: str, data: str):
    """(colunas, total_de_linhas). total 0 = sem dados (feriado ou fora da janela)."""
    r = post(f"{BASE}/{tabela}/{data}/{data}/1/1", "{}")
    try:
        t = r.json()["table"]
    except (ValueError, KeyError):
        return [], 0
    if not (t.get("values") or []):
        return t.get("columns") or [], 0
    return t.get("columns") or [], int(t.get("pageCount") or 0)


def baixar_csv(tabela: str, data: str) -> str:
    corpo = json.dumps({"Name": tabela, "Date": data, "FinalDate": data,
                        "ClientId": "", "Filters": {}})
    r = post(f"{BASE}/export/csv?lang=pt-BR", corpo, timeout=300, tentativas=TENTATIVAS_CSV)
    try:
        return r.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return r.content.decode("latin-1")


# ---------------------------------------------------------------- conversão
RE_NUM = re.compile(r"^-?\d{1,3}(\.\d{3})*(,\d+)?%?$|^-?\d+(,\d+)?%?$")
RE_DATA = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def converter_coluna(s: pd.Series) -> pd.Series:
    valores = s.dropna()
    if valores.empty:
        return s
    if valores.map(lambda v: bool(RE_NUM.match(v))).all():
        def num(v):
            if v is None:
                return None
            pct = v.endswith("%")
            x = float(v.rstrip("%").replace(".", "").replace(",", "."))
            return x / 100 if pct else x
        return s.map(num)
    if valores.map(lambda v: bool(RE_DATA.match(v))).all():
        return s.map(lambda v: None if v is None else
                     "{2}-{1}-{0}".format(*RE_DATA.match(v).groups()))
    return s


def ler_csv(texto: str, colunas_meta: list) -> pd.DataFrame:
    linhas = list(csv.reader(io.StringIO(texto), delimiter=";"))
    nomes_meta = {c.get("friendlyNamePt") for c in colunas_meta if c.get("friendlyNamePt")}

    # cabeçalho = primeira linha em que a maioria dos campos são nomes de coluna da B3
    idx = None
    for i, l in enumerate(linhas[:10]):
        campos = [c.strip() for c in l if c.strip()]
        casados = sum(1 for c in campos if c in nomes_meta)
        if len(campos) >= 3 and casados >= max(3, len(campos) // 2):
            idx = i
            break
    if idx is None:
        raise ValueError("cabeçalho do CSV não encontrado")

    cabecalho = [c.strip() for c in linhas[idx]]
    dados = [l for l in linhas[idx + 1:] if any(c.strip() for c in l)]

    # nome amigável (n-ésima ocorrência) -> código da B3 (n-ésima ocorrência)
    por_nome = {}
    for c in colunas_meta:
        por_nome.setdefault(c.get("friendlyNamePt"), []).append(c.get("name"))
    vistos, codigos = {}, []
    for nome in cabecalho:
        k = vistos.get(nome, 0)
        vistos[nome] = k + 1
        lista = por_nome.get(nome, [])
        codigo = lista[k] if k < len(lista) else (nome if k == 0 else f"{nome}_{k + 1}")
        codigos.append(codigo)

    n = len(cabecalho)
    dados = [(l + [""] * n)[:n] for l in dados]
    df = pd.DataFrame(dados, columns=codigos)
    df = df.apply(lambda s: s.str.strip())
    df = df.astype(object).where(~df.isin(["", "-"]), None)
    for c in df.columns:
        df[c] = converter_coluna(df[c])
    return df


def baixar(tabela: str, data: str):
    """Retorna (DataFrame, metadados, total) ou (None, None, 0) se não houver dados."""
    meta, total = metadados(tabela, data)
    if total == 0:
        return None, None, 0
    for tentativa in range(1, TENTATIVAS_CSV + 1):
        df = ler_csv(baixar_csv(tabela, data), meta)
        if len(df) > total:
            df = df.dropna(how="all").drop_duplicates()
        excesso = len(df) - total
        if 0 <= excesso <= TOLERANCIA_LINHAS:
            if excesso:
                log.warning("%s %s: CSV com %d linha(s) a mais que o total da B3 (%d); aceito",
                            tabela, data, excesso, total)
            df.insert(0, "data_pregao", data)
            return df, meta, total
        log.warning("%s %s: CSV com %d linhas, B3 informa %d (tentativa %d)",
                    tabela, data, len(df), total, tentativa)
        time.sleep(10 * tentativa)
    raise RuntimeError(f"{tabela} {data}: contagem do CSV não confere com a B3")


# ---------------------------------------------------------------- controle
def ler_controle() -> dict:
    if CONTROLE.exists():
        return json.loads(CONTROLE.read_text(encoding="utf-8"))
    return {}


def gravar_controle(ctrl: dict) -> None:
    CONTROLE.parent.mkdir(parents=True, exist_ok=True)
    CONTROLE.write_text(json.dumps(ctrl, ensure_ascii=False, indent=1, sort_keys=True),
                        encoding="utf-8")


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


def processar(tabela, pasta_nome, filtro, data, ctrl) -> str:
    """Retorna 'novo', 'existe', 'vazio' ou 'falha'."""
    pasta = DADOS / pasta_nome
    destino = pasta / data[:4] / f"{data}.parquet"
    registro = ctrl.get(pasta_nome, {}).get(data, {})
    if destino.exists() and registro.get("fonte") == FONTE:
        return "existe"
    try:
        df, meta, total = baixar(tabela, data)
    except (RuntimeError, ValueError) as e:
        log.error("%s %s: %s", pasta_nome, data, e)
        return "falha"
    if df is None:
        log.info("%s %s: sem dados (feriado ou fora da janela da B3)", pasta_nome, data)
        return "vazio"
    if filtro is not None:
        df = filtro(df)
    ja_existia = destino.exists()
    destino.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(destino, index=False)
    salvar_dicionario(pasta, meta)
    ctrl.setdefault(pasta_nome, {})[data] = {"fonte": FONTE, "linhas_b3": total,
                                             "linhas_salvas": len(df)}
    gravar_controle(ctrl)
    substituiu = " (substituiu versão anterior)" if ja_existia else ""
    log.info("%s %s: %d linhas salvas de %d%s", pasta_nome, data, len(df), total, substituiu)
    time.sleep(PAUSA)
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
