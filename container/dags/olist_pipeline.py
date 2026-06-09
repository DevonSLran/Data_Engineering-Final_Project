"""
Olist medallion ELT pipeline.

Runs the whole flow on a daily schedule, one task after another:

    extract_raw        download + verify the 9 Olist CSVs into data/raw
        |
    load_bronze        raw CSVs -> bronze schema (Python COPY loader)
        |
    dbt_run_staging    bronze -> staging (silver) views
        |
    dbt_test_staging   data-quality tests on the silver layer
        |
    dbt_run_marts      staging -> marts (gold) star-schema tables
        |
    dbt_test_marts     PK + referential-integrity tests on the gold layer
        |
    dbt_docs_generate  build the dbt docs site (manifest + catalog)

dbt lives in an isolated venv inside the Airflow image; we call it by its
absolute path so it never collides with Airflow's own dependencies.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="olist_elt_pipeline",
    description="Olist medallion ELT: bronze (Python) -> silver/gold (dbt)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "medallion", "dbt"],
) as dag:

    extract_raw = BashOperator(
        task_id="extract_raw",
        bash_command="python /opt/airflow/ingestion/download_data.py",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command="python /opt/airflow/ingestion/load_raw.py",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT} run --select staging {DBT_FLAGS}",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT} test --select staging {DBT_FLAGS}",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT} run --select marts {DBT_FLAGS}",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT} test --select marts {DBT_FLAGS}",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate {DBT_FLAGS}",
    )

    (
        extract_raw
        >> load_bronze
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> dbt_docs_generate
    )
