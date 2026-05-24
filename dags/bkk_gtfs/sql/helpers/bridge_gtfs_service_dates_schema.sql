CREATE TABLE IF NOT EXISTS bridge_gtfs_service_dates (
    service_date_id_pk SERIAL PRIMARY KEY,
    service_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    exception_type INT NOT NULL,
    UNIQUE (service_id, date),
    CONSTRAINT fk_bridge_gtfs_service_date FOREIGN KEY (date) REFERENCES dim_date(date) ON DELETE CASCADE
);