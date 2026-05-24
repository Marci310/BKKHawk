CREATE TABLE IF NOT EXISTS dim_stop (
    stop_id_pk SERIAL PRIMARY KEY,
    stop_id VARCHAR(50) NOT NULL,
    stop_name VARCHAR(100) NOT NULL,
    stop_lat FLOAT NOT NULL,
    stop_lon FLOAT NOT NULL,
    wheelchair_boarding INT,
    start_date TIMESTAMP DEFAULT NOW(),
    end_date TIMESTAMP DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_stop_active
ON dim_stop(stop_id) WHERE end_date IS NULL;