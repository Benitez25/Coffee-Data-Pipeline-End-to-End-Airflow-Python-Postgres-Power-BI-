from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pathlib import Path
import glob

SCRIPTS_DIR = "/opt/airflow/scripts"
RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"

def task_ingest(**ctx):
    os.system(f"python {SCRIPTS_DIR}/ingest.py")

def task_find_latest_raw(**ctx):
    files = sorted(glob.glob(f"{RAW_DIR}/*.csv"))
    if not files:
        raise FileNotFoundError("No raw files")
    latest = Path(files[-1]).name
    ctx['ti'].xcom_push(key='latest_raw', value=latest)

def task_transform(**ctx):
    ti = ctx['ti']
    latest = ti.xcom_pull(key='latest_raw')
    os.system(f"python {SCRIPTS_DIR}/transform.py {latest}")

def task_find_latest_processed(**ctx):
    files = sorted(glob.glob(f"{PROCESSED_DIR}/*.csv"))
    if not files:
        raise FileNotFoundError("No processed files")
    latest = Path(files[-1]).name
    ctx['ti'].xcom_push(key='latest_processed', value=latest)

def task_load_db(**ctx):
    ti = ctx['ti']
    latest = ti.xcom_pull(key='latest_processed')
    os.system(f"python {SCRIPTS_DIR}/load_db.py {latest}")

with DAG(dag_id="cafe_pipeline", start_date=datetime(2024,1,1), schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(task_id="ingest", python_callable=task_ingest)
    t1b = PythonOperator(task_id="find_latest_raw", python_callable=task_find_latest_raw)
    t2 = PythonOperator(task_id="transform", python_callable=task_transform)
    t2b = PythonOperator(task_id="find_latest_processed", python_callable=task_find_latest_processed)
    t3 = PythonOperator(task_id="load_db", python_callable=task_load_db)

    t1 >> t1b >> t2 >> t2b >> t3
