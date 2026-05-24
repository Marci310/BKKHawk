CREATE TABLE IF NOT EXISTS fact_stop_time_delay (
    stop_time_delay_id_pk BIGSERIAL PRIMARY KEY,

    gtfs_trip_update_id VARCHAR(100) NOT NULL,
    trip_id_pk BIGINT REFERENCES dim_trip(trip_id_pk),
    route_id_pk BIGINT REFERENCES dim_route(route_id_pk),
    stop_id_pk BIGINT REFERENCES dim_stop(stop_id_pk),

    date_id BIGINT REFERENCES dim_date(date_id),
    weather_id BIGINT REFERENCES dim_weather(weather_id),

    vehicle_id_pk BIGINT REFERENCES dim_vehicle(vehicle_id_pk),

    stop_sequence INT,

    arrival_time TIMESTAMP,
    scheduled_arrival_time TIMESTAMP,

    departure_time TIMESTAMP,

    delay_seconds INT,

    status VARCHAR(20),

    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unq_trip_stop_date UNIQUE ( trip_id_pk , stop_sequence, date_id)
);