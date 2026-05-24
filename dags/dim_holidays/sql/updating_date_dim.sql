WITH holiday_data AS (

    WITH latest_raw AS (
        SELECT api_response
        FROM raw_staging_date
        WHERE year = %(year_to_process)s
        ORDER BY fetch_date DESC
        LIMIT 1
    ),

    expanded_days AS (
        SELECT jsonb_array_elements(api_response -> 'days') AS day_data
        FROM latest_raw
    )

    SELECT 
        (day_data ->> 'date')::DATE    AS date,
        day_data ->> 'name'            AS name,
        (day_data ->> 'type')::INTEGER AS type
    FROM expanded_days
)


UPDATE dim_date
SET 

    is_holiday = CASE 
        WHEN T.date IS NOT NULL THEN TRUE
        ELSE FALSE
    END,

    holiday_name = T.name,
    holiday_type = T.type

FROM dim_date AS D
LEFT JOIN holiday_data AS T ON D.date = T.date

WHERE 
    dim_date.date_id = D.date_id
    AND D.year = %(year_to_process)s;