CREATE TABLE IF NOT EXISTS dim_trip (
    trip_id_pk SERIAL PRIMARY KEY,
    trip_id VARCHAR(50) NOT NULL,
    route_id VARCHAR(50) NOT NULL,
    service_id VARCHAR(50) NOT NULL,
    trip_headsign VARCHAR(100),
    direction_id INT,
    shape_id VARCHAR(50),
    start_date TIMESTAMP DEFAULT NOW(),
    end_date TIMESTAMP DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_trip_active
ON dim_trip(trip_id) WHERE end_date IS NULL;