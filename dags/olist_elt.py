"""
Two DAGs:

  olist_bootstrap  - manual trigger, once. Loads the static dimension tables.
  olist_elt_daily  - one run per simulated day. Loads that day's orders into raw,
                     then runs and tests the dbt project.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
INGEST = "python /opt/airflow/ingestion/load_olist.py"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="olist_bootstrap",
    start_date=datetime(2017, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["olist", "bootstrap"],
) as bootstrap:
    BashOperator(task_id="load_static_tables", bash_command=f"{INGEST} static")


with DAG(
    dag_id="olist_elt_daily",
    start_date=datetime(2018, 7, 1),
    end_date=datetime(2018, 8, 31),   # Olist order volume ends around here
    schedule="@daily",
    catchup=True,                      # replays history one day at a time
    max_active_runs=1,                 # keep days in order for the incremental models
    default_args=default_args,
    tags=["olist", "elt"],
) as daily:
    load_day = BashOperator(
        task_id="load_raw_orders",
        bash_command=f"{INGEST} day --date {{{{ ds }}}}",
    )
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && {DBT} deps",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT} run",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT} test",
    )

    load_day >> dbt_deps >> dbt_run >> dbt_test
