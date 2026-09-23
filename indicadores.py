#!/usr/bin/env python3
"""
Indicadores de posicionamento (aluguel de ações) a partir da base do coletor.

Por ticker e pregão:
  saldo_alugado_qtd     saldo em aberto de empréstimo (linha "Total" da B3)
  saldo_alugado_rs      idem, em R$
  acoes_classe          ações da classe (campo MktCptlstn do cadastro B3)
  pct_classe_alugada    saldo_alugado_qtd / acoes_classe
  qtd_negociada         ações negociadas no pregão (segmento à vista)
  adv20_qtd             média de ações negociadas nos últimos 20 pregões
  dias_para_zerar       saldo_alugado_qtd / adv20_qtd
  taxa_tomador_media    taxa média ponderada dos NOVOS contratos do dia (fração a.a.)
  taxa_doador_media     idem, lado doador
  contratos_dia, qtd_nova_dia
  var_saldo_5d          variação % do saldo alugado em 5 pregões

Saídas (CSV em formato BR, abre direto no Excel):
  dados/painel_posicionamento.csv          último pregão, ordenado por % da classe
  dados/historico_posicionamento.csv       histórico completo
  dados/painel_emprestimos_registrados.csv contratos do dia (linhas com contratos > 0)
"""

from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parent
DADOS = RAIZ / "dados"
CSV_BR = dict(index=False, sep=";", decimal=",", encoding="utf-8-sig")


def ler(pasta: str) -> pd.DataFrame:
    arquivos = sorted((DADOS / pasta).glob("*/*.parquet"))
    if not arquivos:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(a) for a in arquivos), ignore_index=True)


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def main() -> None:
    pos = ler("posicoes_em_aberto")
    emp = ler("emprestimos_registrados")
    avista = ler("negociacao_a_vista")
    cad = ler("cadastro_instrumentos")
    if pos.empty:
        print("Sem posições em aberto; nada a calcular.")
        return

    # 1) Saldo alugado: linha "Total" por ticker
    pos = pos[pos["Market"] == "Total"]
    base = pd.DataFrame({
        "data_pregao": pos["data_pregao"],
        "ticker": pos["TckrSymb"],
        "empresa": pos["Company"],
        "tipo": pos["Type"].astype(str).str.split().str.join(" "),
        "saldo_alugado_qtd": num(pos["StockBalance"]),
        "saldo_alugado_rs": num(pos["Balance"]),
    })

    # 2) Negociação à vista + média de 20 pregões
    if not avista.empty:
        av = pd.DataFrame({
            "data_pregao": avista["data_pregao"],
            "ticker": avista["TckrSymb"],
            "preco_fechamento": num(avista["LastPric"]),
            "qtd_negociada": num(avista["FinInstrmQty"]),
            "volume_rs": num(avista["NtlFinVol"]),
        }).groupby(["ticker", "data_pregao"], as_index=False).sum(min_count=1)
        av = av.sort_values(["ticker", "data_pregao"])
        av["adv20_qtd"] = (av.groupby("ticker")["qtd_negociada"]
                             .transform(lambda s: s.rolling(20, min_periods=5).mean()))
        av["pregoes_na_media"] = (av.groupby("ticker")["qtd_negociada"]
                                    .transform(lambda s: s.rolling(20, min_periods=1).count()))
        base = base.merge(av, on=["ticker", "data_pregao"], how="left")

    # 3) Ações da classe: retrato de cadastro mais recente até a data
    if not cad.empty:
        c = pd.DataFrame({
            "data_cad": pd.to_datetime(cad["data_pregao"]),
            "ticker": cad["TckrSymb"],
            "acoes_classe": num(cad["MktCptlstn"]),
            "governanca": cad.get("CorpGovnLvlNm"),
        }).dropna(subset=["acoes_classe"]).sort_values("data_cad")
        base["_dt"] = pd.to_datetime(base["data_pregao"])
        base = base.sort_values("_dt")
        base = pd.merge_asof(base, c, left_on="_dt", right_on="data_cad",
                             by="ticker", direction="backward")
        # dias anteriores ao primeiro retrato usam o retrato mais antigo disponível
        faltando = base["acoes_classe"].isna()
        if faltando.any():
            primeiro = c.drop_duplicates("ticker").set_index("ticker")
            base.loc[faltando, "acoes_classe"] = base.loc[faltando, "ticker"].map(primeiro["acoes_classe"])
            base.loc[faltando, "governanca"] = base.loc[faltando, "ticker"].map(primeiro["governanca"])
        base = base.drop(columns=["_dt", "data_cad"])

    # 4) Novos contratos do dia: taxas ponderadas por quantidade
    if not emp.empty:
        e = pd.DataFrame({
            "data_pregao": emp["data_pregao"],
            "ticker": emp["TckrSymb"],
            "contratos_dia": num(emp["QtyCtrctsDay"]),
            "qtd_nova_dia": num(emp["ValCtrctsDay"]),
            "tx_tom": num(emp["TkrAvrgRate"]),
            "tx_doa": num(emp["DnrAvrgRate"]),
        })
        e = e[e["contratos_dia"].fillna(0) > 0]
        e["p_tom"] = e["tx_tom"] * e["qtd_nova_dia"]
        e["p_doa"] = e["tx_doa"] * e["qtd_nova_dia"]
        g = e.groupby(["ticker", "data_pregao"], as_index=False)[
            ["contratos_dia", "qtd_nova_dia", "p_tom", "p_doa"]].sum()
        g["taxa_tomador_media"] = g["p_tom"] / g["qtd_nova_dia"]
        g["taxa_doador_media"] = g["p_doa"] / g["qtd_nova_dia"]
        base = base.merge(g.drop(columns=["p_tom", "p_doa"]),
                          on=["ticker", "data_pregao"], how="left")

        painel_emp = emp[num(emp["QtyCtrctsDay"]).fillna(0) > 0]
        painel_emp.to_csv(DADOS / "painel_emprestimos_registrados.csv", **CSV_BR)

    # 5) Indicadores derivados
    if "acoes_classe" in base:
        base["pct_classe_alugada"] = base["saldo_alugado_qtd"] / base["acoes_classe"]
    if "adv20_qtd" in base:
        base["dias_para_zerar"] = base["saldo_alugado_qtd"] / base["adv20_qtd"]
    base = base.sort_values(["ticker", "data_pregao"])
    base["var_saldo_5d"] = base.groupby("ticker")["saldo_alugado_qtd"].pct_change(5)

    ordem = ["data_pregao", "ticker", "empresa", "tipo", "governanca",
             "saldo_alugado_qtd", "saldo_alugado_rs", "acoes_classe", "pct_classe_alugada",
             "qtd_negociada", "adv20_qtd", "pregoes_na_media", "dias_para_zerar",
             "taxa_tomador_media", "taxa_doador_media", "contratos_dia", "qtd_nova_dia",
             "var_saldo_5d", "preco_fechamento", "volume_rs"]
    base = base[[c for c in ordem if c in base.columns]]

    (DADOS / "indicadores").mkdir(parents=True, exist_ok=True)
    base.to_parquet(DADOS / "indicadores" / "posicionamento.parquet", index=False)
    base.to_csv(DADOS / "historico_posicionamento.csv", **CSV_BR)

    ultimo = base["data_pregao"].max()
    painel = base[base["data_pregao"] == ultimo]
    if "pct_classe_alugada" in painel:
        painel = painel.sort_values("pct_classe_alugada", ascending=False)
    painel.to_csv(DADOS / "painel_posicionamento.csv", **CSV_BR)
    print(f"Indicadores: {len(base)} linhas; painel de {ultimo} com {len(painel)} tickers")


if __name__ == "__main__":
    main()
