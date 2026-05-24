CREATE TABLE IF NOT EXISTS raw_gtfs_trip_up_staging (
    id SERIAL PRIMARY KEY,
    fetch_time TIMESTAMP NOT NULL,
    bkk_time TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL
)