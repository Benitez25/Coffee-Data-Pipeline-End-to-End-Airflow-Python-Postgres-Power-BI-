# load_db.py
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
PROCESSED_DIR = Path("/opt/airflow/data/processed")

def load_to_postgres(processed_csv):
    engine = create_engine(DB_URL)
    df = pd.read_csv(PROCESSED_DIR / processed_csv, parse_dates=['datetime','date'])
    df.to_sql('sales', engine, if_exists='append', index=False)
    print("Cargado en Postgres")

if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print("Uso: python load_db.py filename.csv"); exit(1)
    load_to_postgres(sys.argv[1])
