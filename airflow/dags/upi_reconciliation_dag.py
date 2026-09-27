"""Daily orchestration for the UPI reconciliation pipeline.

extract -> validate -> load -> dbt snapshot -> dbt run -> dbt test

Each run's logical date (Airflow's ds) is passed through as --run-date /
--vars '{"run_date": ...}' so a backfill of this DAG reprocesses exactly one
day per DAG run, matching `make run DATE=...`.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_DIR = "/opt/airflow/upi-payment-reconciliation-pipeline"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="upi_reconciliation_pipeline",
    description="Extract, validate, load and reconcile UPI transactions for a single day",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=True,
    max_active_runs=1,
    tags=["upi", "reconciliation"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {REPO_DIR} && python ingestion/extract.py --run-date {{{{ ds }}}}",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"cd {REPO_DIR} && python ingestion/validate.py --run-date {{{{ ds }}}}",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {REPO_DIR} && python ingestion/load.py --run-date {{{{ ds }}}}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {REPO_DIR}/dbt_upi && dbt snapshot --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {REPO_DIR}/dbt_upi && dbt run --profiles-dir . "
            '--vars \'{"run_date": "{{ ds }}"}\''
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {REPO_DIR}/dbt_upi && dbt test --profiles-dir .",
    )

    extract >> validate >> load >> dbt_snapshot >> dbt_run >> dbt_test
