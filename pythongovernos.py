#!/usr/bin/env python3
"""
coleta_economia_governos.py
Gera dois CSVs prontos para Power BI:

  data/csv/lula_ii_2007_2010.csv
  data/csv/bolsonaro_2019_2022.csv

Colunas: period (AAAAMM), desemprego (%), ipca (%), pib_vol (índice 1995=100)
"""

from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd, requests, time

# ── CONFIGURAÇÃO ────────────────────────────────────────────
OUT_DIR = Path("data/csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

GOVERNOS = {
    "lula_ii":   ("2007-01-01", "2010-12-31"),
    "bolsonaro": ("2019-01-01", "2022-12-31"),
}

# Códigos SGS (Banco Central)
SGS = dict(
    desemprego = 24369,   # PNAD Contínua média mensal (%)
    ipca       = 433,     # IPCA variação mensal (%)
    pib_trimes = 4380,    # PIB índice de volume (trimestral)
)

TIMEOUT = 15   # seg
RETRY   = 3

# ── FUNÇÕES AUXILIARES ─────────────────────────────────────
def _fmt(d: str) -> str:
    """Converte 'AAAA-MM-DD' → 'DD/MM/AAAA'."""
    return pd.to_datetime(d).strftime("%d/%m/%Y")

def fetch_sgs(code: int, ini: str, fim: str) -> pd.Series:
    """Baixa série SGS; retorna Series indexada por datetime."""
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
           f"?formato=json&dataInicial={_fmt(ini)}&dataFinal={_fmt(fim)}")

    for i in range(1, RETRY + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                if data:
                    df = pd.DataFrame(data)
                    df["date"] = pd.to_datetime(df["data"], dayfirst=True)
                    df["value"] = pd.to_numeric(df["valor"].str.replace(",", "."),
                                                errors="coerce")
                    return df.set_index("date")["value"].sort_index()
            print(f"⚠️  SGS {code} tentativa {i}/{RETRY} status={r.status_code}")
        except Exception as e:
            print(f"⚠️  SGS {code} tentativa {i}/{RETRY} erro {e}")
        time.sleep(2)
    return pd.Series(dtype="float64")

def expand_trimestre_para_meses(s_q: pd.Series) -> pd.Series:
    """Replica valor trimestral para os três meses do trimestre."""
    rep = {}
    for d, v in s_q.items():
        mes_inicial = {1: 1, 4: 4, 7: 7, 10: 10}[d.month]
        for m in range(mes_inicial, mes_inicial + 3):
            rep[d.replace(month=m, day=1)] = v
    return pd.Series(rep, dtype="float64").sort_index()

def calendario_mensal(ini: str, fim: str) -> pd.DatetimeIndex:
    d0, d1 = pd.to_datetime(ini), pd.to_datetime(fim)
    return pd.date_range(d0, d1, freq="MS")

def montar_dataset(ini: str, fim: str) -> pd.DataFrame:
    idx = calendario_mensal(ini, fim)
    df  = pd.DataFrame({"date": idx})

    # Desemprego & IPCA (mensais)
    for col, code in (("desemprego", SGS["desemprego"]),
                      ("ipca", SGS["ipca"])):
        df[col] = fetch_sgs(code, ini, fim).reindex(idx).values

    # PIB (trimestral → mensal)
    pib_tri = fetch_sgs(SGS["pib_trimes"], ini, fim)
    df["pib_vol"] = expand_trimestre_para_meses(pib_tri).reindex(idx).values

    df["period"] = df["date"].dt.strftime("%Y%m")
    return df[["period", "desemprego", "ipca", "pib_vol"]]

# ── MAIN ────────────────────────────────────────────────────
def main():
    for gov, (ini, fim) in GOVERNOS.items():
        tabela = montar_dataset(ini, fim)
        arq = OUT_DIR / f"{gov}_{ini[:4]}_{fim[:4]}.csv"
        tabela.to_csv(arq, index=False)
        print("✓ CSV salvo:", arq.resolve())

if __name__ == "__main__":
    main()
