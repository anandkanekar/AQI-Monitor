-- AQI lakehouse catalog (Hive Metastore via Trino). Idempotent; run by the `trino-init` service.
--
--   landing   -> s3a://landing/aqi/...        raw JSON exactly as received (Flink sink)
--   curated   -> s3a://processed/curated/...  validated, de-duplicated, AQI-enriched Parquet
--   analytics -> s3a://processed/analytics/.. daily / weekly aggregates for BI

CREATE SCHEMA IF NOT EXISTS hive.landing   WITH (location = 's3a://landing/aqi/');
CREATE SCHEMA IF NOT EXISTS hive.curated   WITH (location = 's3a://processed/curated/');
CREATE SCHEMA IF NOT EXISTS hive.analytics WITH (location = 's3a://processed/analytics/');

-- Raw zone: external table over the Flink landing sink (JSON lines, partitioned by event date/hour).
CREATE TABLE IF NOT EXISTS hive.landing.raw_readings (
    schema_version  integer,
    sensor_id       varchar,
    city            varchar,
    latitude        double,
    longitude       double,
    pm25            double,
    pm10            double,
    co2             double,
    no2             double,
    o3              double,
    firmware        varchar,
    event_time      varchar,
    ingest_time     varchar,
    kafka_partition integer,
    kafka_offset    bigint,
    dt              varchar,
    hour            varchar
)
WITH (
    format = 'JSON',
    external_location = 's3a://landing/aqi/raw/',
    partitioned_by = ARRAY['dt', 'hour']
);

-- Curated zone: one row per (sensor_id, event_time), invalid values nulled, AQI computed.
CREATE TABLE IF NOT EXISTS hive.curated.readings (
    sensor_id          varchar,
    city               varchar,
    latitude           double,
    longitude          double,
    event_ts           timestamp(3),
    pm25               double,
    pm10               double,
    co2                double,
    no2                double,
    o3                 double,
    pm25_index         integer,
    pm10_index         integer,
    no2_index          integer,
    o3_index           integer,
    aqi                integer,
    aqi_category       varchar,
    dominant_pollutant varchar,
    co2_level          varchar,
    firmware           varchar,
    dt                 varchar
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['dt']
);

-- Aggregated zone ------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS hive.analytics.daily_sensor_aqi (
    sensor_id          varchar,
    city               varchar,
    latitude           double,
    longitude          double,
    readings           bigint,
    avg_aqi            double,
    min_aqi            integer,
    max_aqi            integer,
    p95_aqi            double,
    avg_pm25           double,
    max_pm25           double,
    avg_pm10           double,
    avg_no2            double,
    avg_o3             double,
    avg_co2            double,
    unhealthy_readings bigint,
    pct_unhealthy      double,
    worst_pollutant    varchar,
    worst_category     varchar,
    dt                 varchar
)
WITH (format = 'PARQUET', partitioned_by = ARRAY['dt']);

CREATE TABLE IF NOT EXISTS hive.analytics.daily_city_aqi (
    city               varchar,
    sensors            bigint,
    readings           bigint,
    avg_aqi            double,
    max_aqi            integer,
    p95_aqi            double,
    avg_pm25           double,
    avg_pm10           double,
    avg_no2            double,
    avg_o3             double,
    avg_co2            double,
    unhealthy_readings bigint,
    pct_unhealthy      double,
    dominant_pollutant varchar,
    aqi_category       varchar,
    dt                 varchar
)
WITH (format = 'PARQUET', partitioned_by = ARRAY['dt']);

CREATE TABLE IF NOT EXISTS hive.analytics.weekly_city_aqi (
    city               varchar,
    days_covered       bigint,
    readings           bigint,
    avg_aqi            double,
    max_aqi            integer,
    worst_day          varchar,
    worst_day_avg_aqi  double,
    avg_pm25           double,
    avg_pm10           double,
    unhealthy_readings bigint,
    pct_unhealthy      double,
    aqi_category       varchar,
    week_start         varchar
)
WITH (format = 'PARQUET', partitioned_by = ARRAY['week_start']);

-- Convenience view for BI tools: daily city trend with a proper DATE column.
CREATE OR REPLACE VIEW hive.analytics.v_city_daily_trend AS
SELECT CAST(dt AS date) AS day, city, avg_aqi, max_aqi, p95_aqi, pct_unhealthy, dominant_pollutant, aqi_category
FROM hive.analytics.daily_city_aqi;
