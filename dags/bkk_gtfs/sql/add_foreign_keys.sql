DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_dim_trip_route') THEN
            ALTER TABLE dim_trip
            ADD CONSTRAINT fk_dim_trip_route
            FOREIGN KEY (route_id)
            REFERENCES dim_route(route_id)
            ON DELETE CASCADE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_bridge_gtfs_service_dates_service_date') THEN
            ALTER TABLE bridge_gtfs_service_dates
            ADD CONSTRAINT fk_bridge_gtfs_service_dates_service_date
            FOREIGN KEY (date)
            REFERENCES dim_date(date)
            ON DELETE CASCADE;
        END IF;
END $$