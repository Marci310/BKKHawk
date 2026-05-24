INSERT INTO dim_date (
            date, year, month, day, day_of_week, is_weekend
        )
        SELECT
            d::DATE AS date,
            EXTRACT(YEAR FROM d) AS year,
            EXTRACT(MONTH FROM d) AS month,
            EXTRACT(DAY FROM d) AS day,
            EXTRACT(ISODOW FROM d) AS day_of_week,
            EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
        FROM generate_series(
            '2020-01-01'::DATE,
            '2030-12-31'::DATE,
            '1 day'::INTERVAL
        ) AS d
        ON CONFLICT (date) DO NOTHING;