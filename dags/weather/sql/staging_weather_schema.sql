CREATE TABLE IF NOT EXISTS raw_staging_weather (
    id SERIAL PRIMARY KEY,
    fetch_date TIMESTAMP,
    date_start TIMESTAMP,
    api_response JSONB
);