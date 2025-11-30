# transform.py
import pandas as pd
from pathlib import Path

RAW_DIR = Path("/opt/airflow/data/raw")
OUT_DIR = Path("/opt/airflow/data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def transform(latest_raw):
    print(f"ESTA ES LA RUTA DEL ARCHIVO ->>>>> {latest_raw}")
    df = pd.read_csv(RAW_DIR / latest_raw, parse_dates=['datetime','date'], dayfirst=False)
    df['money'] = pd.to_numeric(df['money'], errors='coerce').fillna(0)
    df['card']  = pd.to_numeric(df['card'], errors='coerce').fillna(0)
    df['total_amount'] = df['money'] + df['card']
    df['hour'] = df['datetime'].dt.hour
    df['weekday'] = df['datetime'].dt.day_name()
    def metodo_pago(row):
        if row['card']>0 and row['money']>0: return 'mixed'
        if row['card']>0: return 'card'
        if row['money']>0: return 'cash'
        return 'unknown'
    df['payment_method'] = df.apply(metodo_pago, axis=1)
    out = OUT_DIR / f"sales_processed_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(out, index=False)
    print("Transformado ->", out)

if __name__ == "__main__":
    import sys
    latest = sys.argv[1]
    transform(latest)