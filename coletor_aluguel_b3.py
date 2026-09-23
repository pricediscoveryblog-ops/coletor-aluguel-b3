#!/usr/bin/env python3
"""
Coletor diário de dados de empréstimo de ações (BTB) – Boletim Diário da B3.

Fonte: https://arquivos.b3.com.br/bdi/tabelas  (seção "Empréstimo de ativos")
Endpoint: POST /bdi/table/{tabela}/{data}/{data}/{pagina}/{tamanho}  com corpo "{}"

Comportamento:
- A B3 mantém as tabelas estruturadas por ~21 dias úteis (limitDate D-21) e
  publica os dados do dia D por volta das 8h de D+1.
- A cada execução, o script percorre os últimos JANELA_DIAS dias corridos e
  baixa todo dia/tabela que ainda não esteja salvo. Assim ele se recupera
  sozinho de dias perdidos e, na primeira execução, faz o backfill do que a B3
  ainda disponibiliza.
- Dados brutos: dados/<tabela>/<AAAA>/<AAAA-MM-DD>.parquet
- Painel para Excel: dados/painel_emprestimos_registrados.csv (formato BR)
"""

import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

BASE = "https://arquivos.b3.com.br/bdi/table"
TABELAS = {
    "BTBLendingOpenPosition": "posicoes_em_aberto",
    "BTBLoanBalance": "emprestimos_registrados",
    "BTBTrade": "negocios",
    "Renewals": "renovacoes",
}
TAMANHO_PAGINA = 1000   # máximo aceito pela B3 (2000 retorna erro 400)
JANELA_DIAS = 35        # dias corridos para trás (cobre os ~21 dias úteis)
PAUSA = 0.4             # segundos entre requisições, para não sobrecarregar a B3
TENTATIVAS = 5

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("coletor")

sessao = requests.Session()
sessao.headers.update({
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (coletor-aluguel-b3)",
})


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
    """Retorna (DataFrame, colunas) ou (None, None) se não houver dados."""
    t = requisitar(tabela, data, 1)
    n_paginas = t.get("pageCount") or 0
    valores = t.get("values") or []
    if n_paginas == 0 or not valores:
        return None, None

    colunas_meta = t.get("columns") or []
    nomes = [c.get("name") or f"c{i}" for i, c in enumerate(colunas_meta)]

    linhas = list(valores)
    for p in range(2, n_paginas + 1):
        time.sleep(PAUSA)
        linhas.extend(requisitar(tabela, data, p).get("values") or [])

    if len(nomes) != len(linhas[0]):
        log.warning("%s %s: %d colunas vs %d valores; usando nomes genéricos",
                    tabela, data, len(nomes), len(linhas[0]))
        nomes = [f"c{i}" for i in range(len(linhas[0]))]

    # garante nomes únicos
    vistos = {}
    unicos = []
    for n in nomes:
        vistos[n] = vistos.get(n, 0) + 1
        unicos.append(n if vistos[n] == 1 else f"{n}_{vistos[n]}")

    df = pd.DataFrame(linhas, columns=unicos)
    df.insert(0, "data_pregao", data)
    return df, colunas_meta


def salvar_dicionario(pasta: Path, colunas_meta: list) -> None:
    dicionario = [
        {"codigo": c.get("name"), "nome_pt": c.get("friendlyNamePt"),
         "nome_en": c.get("friendlyNameEn"), "tooltip_pt": c.get("tooltipPt")}
        for c in colunas_meta
    ]
    (pasta / "_colunas.json").write_text(
        json.dumps(dicionario, ensure_ascii=False, indent=2), encoding="utf-8")


def dias_uteis_candidatos() -> list:
    hoje = dt.datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    dias = []
    for k in range(JANELA_DIAS, 0, -1):
        d = hoje - dt.timedelta(days=k)
        if d.weekday() < 5:  # feriados são tratados pela própria B3 (retorna vazio)
            dias.append(d.isoformat())
    return dias


def gerar_painel() -> None:
    """CSV consolidado de empréstimos registrados, só linhas com contratos no dia."""
    arquivos = sorted((DADOS / "emprestimos_registrados").glob("*/*.parquet"))
    if not arquivos:
        return
    df = pd.concat((pd.read_parquet(a) for a in arquivos), ignore_index=True)
    if "QtyCtrctsDay" in df.columns:
        df = df[pd.to_numeric(df["QtyCtrctsDay"], errors="coerce").fillna(0) > 0]
    df.to_csv(DADOS / "painel_emprestimos_registrados.csv",
              index=False, sep=";", decimal=",", encoding="utf-8-sig")
    log.info("Painel atualizado: %d linhas", len(df))


def main() -> int:
    falhas = 0
    novos = 0
    for data in dias_uteis_candidatos():
        for tabela, nome in TABELAS.items():
            pasta = DADOS / nome
            destino = pasta / data[:4] / f"{data}.parquet"
            if destino.exists():
                continue
            try:
                df, meta = baixar(tabela, data)
            except RuntimeError as e:
                log.error(str(e))
                falhas += 1
                continue
            if df is None:
                log.info("%s %s: sem dados (feriado ou ainda não publicado)", nome, data)
                continue
            destino.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(destino, index=False)
            salvar_dicionario(pasta, meta)
            novos += 1
            log.info("%s %s: %d linhas salvas", nome, data, len(df))
            time.sleep(PAUSA)

    gerar_painel()
    log.info("Fim: %d arquivos novos, %d falhas", novos, falhas)
    return 1 if falhas else 0


if __name__ == "__main__":
    sys.exit(main())
