# ingest.py
from pathlib import Path
from datetime import datetime
import shutil

SRC = Path("/opt/airflow/data/original.csv")
RAW_DIR = Path("/opt/airflow/data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def ingest():
    print("#############COMIENZA###############")
    src = Path(SRC)
    if not src.exists():
        raise FileNotFoundError(f"No encuentro {SRC}")
    else:
        print(f"SI EXISTE EL ARCHIVO {src}")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = RAW_DIR / f"sales_raw_{ts}.csv"
    shutil.copy(src, dest)
    print(f"Ingestado: {dest}")

if __name__ == "__main__":
    ingest()
