"""
Orchestrates the bronze -> silver -> gold -> Delta pipeline.

Deliberately fails fast and loud at each gate instead of silently limping
forward: if bronze data is missing, dbt tests fail, or the Delta write
errors, the DAG run is marked failed and nothing downstream runs. An
on-call engineer reading Airflow's UI should be able to tell exactly which
stage broke without reading logs line by line.
"""
import glob
import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = "/Users/shaifalisingh/Desktop/Personal_Projs/Git/medallion-pipeline"
VENV_BIN = f"{PROJECT_ROOT}/.venv/bin"


def check_bronze_data():
    pattern = f"{PROJECT_ROOT}/bronze_source/*/*/*.jsonl"
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(
            f"No bronze files found at {pattern}. Upstream connector "
            f"(Project 1) needs to run before this pipeline can proceed."
        )
    empty = [f for f in files if os.path.getsize(f) == 0]
    if empty:
        raise ValueError(f"Found {len(empty)} empty bronze file(s), refusing to proceed: {empty}")
    print(f"Found {len(files)} bronze file(s), proceeding.")


with DAG(
    dag_id="medallion_pipeline",
    description="bronze (JSONL) -> dbt-duckdb silver/gold -> Delta Lake",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "dbt", "delta"],
) as dag:

    check_bronze = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{VENV_BIN}/dbt run --project-dir dbt --profiles-dir dbt",
        cwd=PROJECT_ROOT,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{VENV_BIN}/dbt test --project-dir dbt --profiles-dir dbt",
        cwd=PROJECT_ROOT,
    )

    write_gold_to_delta = BashOperator(
        task_id="write_gold_to_delta",
        bash_command=f"{VENV_BIN}/python scripts/write_gold_to_delta.py",
        cwd=PROJECT_ROOT,
    )

    check_bronze >> dbt_run >> dbt_test >> write_gold_to_delta
