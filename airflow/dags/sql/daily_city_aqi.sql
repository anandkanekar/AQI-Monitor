{%- set d = params.target_date or ds -%}
INSERT INTO hive.analytics.daily_city_aqi
WITH base AS (
    SELECT *
    FROM hive.curated.readings
    WHERE dt = '{{ d }}'
      AND aqi IS NOT NULL
),
dominant AS (
    SELECT city, max_by(dominant_pollutant, n) AS dominant_pollutant
    FROM (
        SELECT city, dominant_pollutant, count(*) AS n
        FROM base
        GROUP BY city, dominant_pollutant
    )
    GROUP BY city
),
agg AS (
    SELECT
        city,
        count(DISTINCT sensor_id)                                        AS sensors,
        count(*)                                                         AS readings,
        avg(aqi)                                                         AS avg_aqi,
        max(aqi)                                                         AS max_aqi,
        approx_percentile(CAST(aqi AS double), 0.95)                     AS p95_aqi,
        avg(pm25)                                                        AS avg_pm25,
        avg(pm10)                                                        AS avg_pm10,
        avg(no2)                                                         AS avg_no2,
        avg(o3)                                                          AS avg_o3,
        avg(co2)                                                         AS avg_co2,
        count_if(aqi > 150)                                              AS unhealthy_readings,
        round(CAST(count_if(aqi > 150) AS double) * 100 / count(*), 2)   AS pct_unhealthy
    FROM base
    GROUP BY city
)
SELECT
    agg.city,
    agg.sensors,
    agg.readings,
    agg.avg_aqi,
    agg.max_aqi,
    agg.p95_aqi,
    agg.avg_pm25,
    agg.avg_pm10,
    agg.avg_no2,
    agg.avg_o3,
    agg.avg_co2,
    agg.unhealthy_readings,
    agg.pct_unhealthy,
    dominant.dominant_pollutant,
    {{ category_sql('CAST(round(agg.avg_aqi) AS integer)') }} AS aqi_category,
    '{{ d }}' AS dt
FROM agg
JOIN dominant ON agg.city = dominant.city
