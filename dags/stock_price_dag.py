# dags/stock_price_dag.py
import os
import sys
from datetime import datetime, timedelta

# Ensure /opt/airflow/scripts is on path (where we mount scripts/)
SCRIPTS_PATH = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts")
if SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, SCRIPTS_PATH)

# import the fetch function from your scripts folder
from fetch_and_upsert import fetch_and_store  # noqa: E402

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "stock-pipeline",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.getenv("ALERT_EMAIL", "admin@example.com")],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_stock_prices",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks"],
) as dag:

    def _run(**context):
        symbol_env = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL")
        symbols = [s.strip() for s in symbol_env.split(",") if s.strip()]
        fetch_and_store(symbols)

    run_task = PythonOperator(
        task_id="fetch_and_upsert",
        python_callable=_run,
    )

    run_task
