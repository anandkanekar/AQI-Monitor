{#- Landing (raw JSON) -> curated Parquet for one day. Re-runnable: the dt partition is overwritten. -#}
{%- set d = params.target_date or ds -%}
INSERT INTO hive.curated.readings
WITH dedup AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY sensor_id, event_time
            ORDER BY ingest_time DESC, kafka_offset DESC
        ) AS rn
    FROM hive.landing.raw_readings
    WHERE dt = '{{ d }}'
      AND sensor_id IS NOT NULL
      AND event_time IS NOT NULL
),
typed AS (
    SELECT
        sensor_id,
        city,
        latitude,
        longitude,
        firmware,
        CAST(try(from_iso8601_timestamp(event_time)) AT TIME ZONE 'UTC' AS timestamp(3)) AS event_ts,
        {{ plausible_sql('pm25', 'pm25') }} AS pm25,
        {{ plausible_sql('pm10', 'pm10') }} AS pm10,
        {{ plausible_sql('co2', 'co2') }} AS co2,
        {{ plausible_sql('no2', 'no2') }} AS no2,
        {{ plausible_sql('o3', 'o3') }} AS o3,
        {{ truncated_sql('pm25', 'pm25') }} AS pm25_t,
        {{ truncated_sql('pm10', 'pm10') }} AS pm10_t,
        {{ truncated_sql('no2', 'no2') }} AS no2_t,
        {{ truncated_sql('o3', 'o3') }} AS o3_t
    FROM dedup
    WHERE rn = 1
),
indexed AS (
    SELECT
        *,
        {{ sub_index_sql('pm25', 'pm25_t') }} AS pm25_index,
        {{ sub_index_sql('pm10', 'pm10_t') }} AS pm10_index,
        {{ sub_index_sql('no2', 'no2_t') }} AS no2_index,
        {{ sub_index_sql('o3', 'o3_t') }} AS o3_index
    FROM typed
    WHERE event_ts IS NOT NULL
),
scored AS (
    SELECT
        *,
        nullif(greatest(
            coalesce(pm25_index, -1), coalesce(pm10_index, -1),
            coalesce(no2_index, -1), coalesce(o3_index, -1)
        ), -1) AS aqi
    FROM indexed
)
SELECT
    sensor_id,
    city,
    latitude,
    longitude,
    event_ts,
    pm25,
    pm10,
    co2,
    no2,
    o3,
    pm25_index,
    pm10_index,
    no2_index,
    o3_index,
    aqi,
    {{ category_sql('aqi') }} AS aqi_category,
    CASE
        WHEN aqi IS NULL THEN CAST(NULL AS varchar)
        WHEN aqi = pm25_index THEN 'pm25'
        WHEN aqi = pm10_index THEN 'pm10'
        WHEN aqi = no2_index THEN 'no2'
        ELSE 'o3'
    END AS dominant_pollutant,
    {{ co2_level_sql('co2') }} AS co2_level,
    firmware,
    '{{ d }}' AS dt
FROM scored
