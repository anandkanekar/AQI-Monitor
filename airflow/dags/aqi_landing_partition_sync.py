"""Registers new landing-zone partitions every 15 minutes so ad-hoc SQL over
`hive.landing.raw_readings` sees near-real-time data (the Flink sink creates a new
dt=/hour= folder every hour)."""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="aqi_landing_partition_sync",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["aqi", "hive", "metadata"],
    doc_md=__doc__,
) as dag:
    SQLExecuteQueryOperator(
        task_id="sync_raw_partitions",
        conn_id="trino_default",
        sql="CALL hive.system.sync_partition_metadata('landing', 'raw_readings', 'ADD')",
    )
