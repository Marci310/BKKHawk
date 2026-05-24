CREATE TABLE IF NOT EXISTS raw_staging_date (
    id SERIAL PRIMARY KEY,
    year INTEGER,
    fetch_date TIMESTAMP,
    api_response JSONB
);