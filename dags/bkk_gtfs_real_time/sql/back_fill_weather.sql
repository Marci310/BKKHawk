WITH matches AS (
    SELECT
        f.stop_time_delay_id_pk,
        (
            SELECT w.weather_id
            FROM dim_weather w
            WHERE w.weather_timestamp BETWEEN f.timestamp - INTERVAL '1 hour'
                                          AND f.timestamp + INTERVAL '1 hour'
            ORDER BY ABS(EXTRACT(EPOCH FROM (f.timestamp - w.weather_timestamp))) ASC
            LIMIT 1
        ) AS best_match_weather_id
    FROM
        fact_stop_time_delay f
    WHERE
        f.weather_id IS NULL
)
UPDATE fact_stop_time_delay f
SET weather_id = m.best_match_weather_id
FROM matches m
WHERE f.stop_time_delay_id_pk = m.stop_time_delay_id_pk
  AND m.best_match_weather_id IS NOT NULL;