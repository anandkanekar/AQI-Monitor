{#- Recomputes the ISO week (Mon-Sun) containing the target date from daily city aggregates.
    Runs daily so the current week is always up to date; the week_start partition is overwritten. -#}
{%- set d = params.target_date or ds -%}
INSERT INTO hive.analytics.weekly_city_aqi
WITH days AS (
    SELECT *
    FROM hive.analytics.daily_city_aqi
    WHERE dt BETWEEN CAST(date_trunc('week', DATE '{{ d }}') AS varchar)
                 AND CAST(date_trunc('week', DATE '{{ d }}') + INTERVAL '6' DAY AS varchar)
),
weekly AS (
    SELECT
        city,
        count(*)                                                AS days_covered,
        sum(readings)                                           AS readings,
        sum(avg_aqi * readings) / sum(readings)                 AS avg_aqi,
        max(max_aqi)                                            AS max_aqi,
        max_by(dt, avg_aqi)                                     AS worst_day,
        max(avg_aqi)                                            AS worst_day_avg_aqi,
        sum(avg_pm25 * readings) / sum(readings)                AS avg_pm25,
        sum(avg_pm10 * readings) / sum(readings)                AS avg_pm10,
        sum(unhealthy_readings)                                 AS unhealthy_readings,
        round(CAST(sum(unhealthy_readings) AS double) * 100 / sum(readings), 2) AS pct_unhealthy
    FROM days
    GROUP BY city
)
SELECT
    city,
    days_covered,
    readings,
    avg_aqi,
    max_aqi,
    worst_day,
    worst_day_avg_aqi,
    avg_pm25,
    avg_pm10,
    unhealthy_readings,
    pct_unhealthy,
    {{ category_sql('CAST(round(avg_aqi) AS integer)') }} AS aqi_category,
    CAST(date_trunc('week', DATE '{{ d }}') AS varchar) AS week_start
FROM weekly
