"""Daily AQI ETL: landing (raw JSON) -> curated (Parquet) -> daily/weekly aggregates.

All heavy lifting runs in Trino against the Hive Metastore catalog; Airflow orchestrates,
checks data quality and keeps Hive partition metadata in sync. Every step is idempotent
(partitions are overwritten), so re-runs and backfills are safe:

    airflow dags backfill aqi_daily_aggregation -s 2026-09-01 -e 2026-09-07
    airflow dags trigger aqi_daily_aggregation -c '{"target_date": "2026-09-23"}'
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from aqi_common.aqi import category_sql, co2_level_sql, plausible_sql, sub_index_sql, truncated_sql

TRINO_CONN_ID = "trino_default"
TARGET_DATE = "{{ params.target_date or ds }}"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="aqi_daily_aggregation",
    description="Curate raw AQI readings and build daily / weekly aggregates",
    schedule="30 0 * * *",  # 00:30 UTC: give late events and the last hourly file roll time to land
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=["/opt/airflow/dags/sql"],
    user_defined_macros={
        "plausible_sql": plausible_sql,
        "truncated_sql": truncated_sql,
        "sub_index_sql": sub_index_sql,
        "category_sql": category_sql,
        "co2_level_sql": co2_level_sql,
    },
    params={
        "target_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Day to process (YYYY-MM-DD). Defaults to the run's logical date.",
        )
    },
    tags=["aqi", "etl", "trino", "hive"],
    doc_md=__doc__,
) as dag:
    sync_raw_partitions = SQLExecuteQueryOperator(
        task_id="sync_raw_partitions",
        conn_id=TRINO_CONN_ID,
        sql="CALL hive.system.sync_partition_metadata('landing', 'raw_readings', 'FULL')",
        doc_md="Register new dt/hour folders written by the Flink landing sink in the Hive Metastore.",
    )

    wait_for_raw_data = SqlSensor(
        task_id="wait_for_raw_data",
        conn_id=TRINO_CONN_ID,
        sql=f"SELECT count(*) FROM hive.landing.raw_readings WHERE dt = '{TARGET_DATE}'",
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 3600,
    )

    build_curated = SQLExecuteQueryOperator(
        task_id="build_curated_readings",
        conn_id=TRINO_CONN_ID,
        sql="build_curated_readings.sql",
    )

    check_curated = SQLCheckOperator(
        task_id="check_curated_quality",
        conn_id=TRINO_CONN_ID,
        sql=f"""
            SELECT
                c.cnt > 0                                         AS has_rows,
                c.cnt <= r.cnt                                    AS not_more_than_raw,
                c.null_aqi <= 0.05 * c.cnt                        AS aqi_mostly_present,
                c.cnt = c.distinct_keys                           AS no_duplicates,
                c.min_ts >= TIMESTAMP '{TARGET_DATE} 00:00:00'    AS ts_in_partition
            FROM (
                SELECT count(*) AS cnt,
                       count_if(aqi IS NULL) AS null_aqi,
                       count(DISTINCT (sensor_id, event_ts)) AS distinct_keys,
                       min(event_ts) AS min_ts
                FROM hive.curated.readings WHERE dt = '{TARGET_DATE}'
            ) c
            CROSS JOIN (
                SELECT count(*) AS cnt FROM hive.landing.raw_readings WHERE dt = '{TARGET_DATE}'
            ) r
        """,
    )

    daily_sensor = SQLExecuteQueryOperator(
        task_id="aggregate_daily_sensor",
        conn_id=TRINO_CONN_ID,
        sql="daily_sensor_aqi.sql",
    )

    daily_city = SQLExecuteQueryOperator(
        task_id="aggregate_daily_city",
        conn_id=TRINO_CONN_ID,
        sql="daily_city_aqi.sql",
    )

    weekly_city = SQLExecuteQueryOperator(
        task_id="aggregate_weekly_city",
        conn_id=TRINO_CONN_ID,
        sql="weekly_city_aqi.sql",
    )

    # Refresh table/column statistics in the metastore for the new partitions (better query plans).
    # NOTE: hive.curated.readings is intentionally skipped - Hive Metastore 4 rejects timestamp
    # column statistics (HIVE_INVALID_METADATA on event_ts), which makes ANALYZE fail on it.
    analyze = SQLExecuteQueryOperator(
        task_id="analyze_partitions",
        conn_id=TRINO_CONN_ID,
        sql=[
            f"ANALYZE hive.analytics.daily_sensor_aqi WITH (partitions = ARRAY[ARRAY['{TARGET_DATE}']])",
            f"ANALYZE hive.analytics.daily_city_aqi WITH (partitions = ARRAY[ARRAY['{TARGET_DATE}']])",
        ],
    )

    sync_raw_partitions >> wait_for_raw_data >> build_curated >> check_curated
    check_curated >> [daily_sensor, daily_city]
    daily_city >> weekly_city
    [daily_sensor, weekly_city] >> analyze
