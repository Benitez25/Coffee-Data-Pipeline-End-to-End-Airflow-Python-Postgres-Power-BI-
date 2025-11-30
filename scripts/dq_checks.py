# dq_checks.py
import pandas as pd
from pathlib import Path
import sys

def run_checks(path_csv):
    df = pd.read_csv(path_csv)
    issues = []
    expected = {"date","datetime","cash_type","card","money","coffee_name"}
    if not expected.issubset(set(df.columns)):
        issues.append(f"Faltan columnas: {expected - set(df.columns)}")
    na_counts = df.isna().sum()
    if na_counts.sum() > 0:
        issues.append(f"NaNs detectados: {na_counts[na_counts>0].to_dict()}")
    try:
        df["money"] = pd.to_numeric(df["money"])
    except Exception:
        issues.append("Columna money no es numérica.")
    for c in ["date","datetime"]:
        try:
            pd.to_datetime(df[c])
        except Exception:
            issues.append(f"Columna {c} tiene formatos no válidos.")
    return issues

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Uso: python dq_checks.py path/to/file.csv"); exit(1)
    issues = run_checks(sys.argv[1])
    if issues:
        print("Issues encontrados:")
        for i in issues: print("-", i)
        sys.exit(2)
    print("Checks OK")
