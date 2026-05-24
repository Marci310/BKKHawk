CREATE TABLE IF NOT EXISTS raw_gtfs_vehicle_pos_staging (
    id SERIAL PRIMARY KEY,
    fetch_time TIMESTAMP NOT NULL,
    bkk_time TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL
)