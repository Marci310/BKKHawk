CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id_pk SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(100) NOT NULL,
    license_plate VARCHAR(100) NOT NULL,
    model VARCHAR(100),
    type INT,
    wheelchair_accessible INT,
    current_route_id VARCHAR(50),
    start_date TIMESTAMP DEFAULT NOW(),
    end_date TIMESTAMP DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_trip ON dim_vehicle(current_route_id) WHERE end_date IS NULL;
CREATE INDEX IF NOT EXISTS idx_dim_vehicle_active ON dim_vehicle(vehicle_id) WHERE end_date IS NULL;