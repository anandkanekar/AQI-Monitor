"""Raw landing-zone sink (PyFlink Table API / Flink SQL).

Continuously archives every raw sensor reading from Kafka to the MinIO landing zone as
newline-delimited JSON, Hive-partitioned by event date and hour:

  s3a://landing/aqi/raw/dt=YYYY-MM-DD/hour=HH/part-*.json

Files are finalised on each checkpoint (exactly-once file commits). The Hive table
`hive.landing.raw_readings` points at this location; Airflow registers new partitions.
"""
from __future__ import annotations

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "aqi.raw")
LANDING_PATH = os.getenv("LANDING_PATH", "s3a://landing/aqi/raw")
PARALLELISM = os.getenv("LANDING_PARALLELISM", "2")


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    conf = t_env.get_config()
    conf.set("pipeline.name", "aqi-landing-zone")
    conf.set("parallelism.default", PARALLELISM)
    conf.set("table.local-time-zone", "UTC")
    conf.set("execution.checkpointing.interval", "60 s")
    conf.set("execution.checkpointing.mode", "EXACTLY_ONCE")

    t_env.execute_sql(f"""
        CREATE TABLE raw_readings_kafka (
            schema_version INT,
            sensor_id      STRING,
            city           STRING,
            latitude       DOUBLE,
            longitude      DOUBLE,
            pm25           DOUBLE,
            pm10           DOUBLE,
            co2            DOUBLE,
            no2            DOUBLE,
            o3             DOUBLE,
            firmware       STRING,
            event_time     STRING,
            kafka_ts        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
            kafka_partition INT              METADATA FROM 'partition' VIRTUAL,
            kafka_offset    BIGINT           METADATA FROM 'offset'    VIRTUAL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC_RAW}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'aqi-landing',
            'properties.auto.offset.reset' = 'earliest',
            'properties.commit.offsets.on.checkpoint' = 'true',
            'scan.startup.mode' = 'group-offsets',
            'scan.topic-partition-discovery.interval' = '60 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE landing_raw (
            schema_version  INT,
            sensor_id       STRING,
            city            STRING,
            latitude        DOUBLE,
            longitude       DOUBLE,
            pm25            DOUBLE,
            pm10            DOUBLE,
            co2             DOUBLE,
            no2             DOUBLE,
            o3              DOUBLE,
            firmware        STRING,
            event_time      STRING,
            ingest_time     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            dt              STRING,
            `hour`          STRING
        ) PARTITIONED BY (dt, `hour`) WITH (
            'connector' = 'filesystem',
            'path' = '{LANDING_PATH}',
            'format' = 'json',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '1 h',
            'sink.partition-commit.policy.kind' = 'success-file'
        )
    """)

    # Partition by event time (UTC ISO-8601 from the sensor); fall back to broker time.
    t_env.execute_sql("""
        INSERT INTO landing_raw
        SELECT
            schema_version, sensor_id, city, latitude, longitude,
            pm25, pm10, co2, no2, o3, firmware, event_time,
            DATE_FORMAT(kafka_ts, 'yyyy-MM-dd HH:mm:ss.SSS') AS ingest_time,
            kafka_partition, kafka_offset,
            CASE WHEN REGEXP(event_time, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}')
                 THEN SUBSTRING(event_time FROM 1 FOR 10) ELSE DATE_FORMAT(kafka_ts, 'yyyy-MM-dd') END AS dt,
            CASE WHEN REGEXP(event_time, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}')
                 THEN SUBSTRING(event_time FROM 12 FOR 2) ELSE DATE_FORMAT(kafka_ts, 'HH') END AS `hour`
        FROM raw_readings_kafka
        WHERE sensor_id IS NOT NULL
    """)


if __name__ == "__main__":
    main()
