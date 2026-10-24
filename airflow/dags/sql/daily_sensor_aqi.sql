{%- set d = params.target_date or ds -%}
INSERT INTO hive.analytics.daily_sensor_aqi
SELECT
    sensor_id,
    arbitrary(city)                                                  AS city,
    avg(latitude)                                                    AS latitude,
    avg(longitude)                                                   AS longitude,
    count(*)                                                         AS readings,
    avg(aqi)                                                         AS avg_aqi,
    min(aqi)                                                         AS min_aqi,
    max(aqi)                                                         AS max_aqi,
    approx_percentile(CAST(aqi AS double), 0.95)                     AS p95_aqi,
    avg(pm25)                                                        AS avg_pm25,
    max(pm25)                                                        AS max_pm25,
    avg(pm10)                                                        AS avg_pm10,
    avg(no2)                                                         AS avg_no2,
    avg(o3)                                                          AS avg_o3,
    avg(co2)                                                         AS avg_co2,
    count_if(aqi > 150)                                              AS unhealthy_readings,
    round(CAST(count_if(aqi > 150) AS double) * 100 / count(*), 2)   AS pct_unhealthy,
    max_by(dominant_pollutant, aqi)                                  AS worst_pollutant,
    max_by(aqi_category, aqi)                                        AS worst_category,
    dt
FROM hive.curated.readings
WHERE dt = '{{ d }}'
  AND aqi IS NOT NULL
GROUP BY sensor_id, dt
