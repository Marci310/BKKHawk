CREATE TABLE IF NOT EXISTS dim_route (
    route_id_pk SERIAL PRIMARY KEY,
    route_id VARCHAR(50) NOT NULL,
    route_short_name VARCHAR(20) NOT NULL,
    route_long_name VARCHAR(100),
    route_type INT NOT NULL,
    route_color VARCHAR(6),
    start_date TIMESTAMP DEFAULT NOW(),
    end_date TIMESTAMP DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_route_active ON dim_route(route_id) WHERE end_date IS NULL;