INSERT INTO fact_stop_time_delay (
    gtfs_trip_update_id, trip_id_pk, route_id_pk, stop_id_pk, vehicle_id_pk,
    date_id, weather_id,
    stop_sequence,
    arrival_time, scheduled_arrival_time, departure_time,
    delay_seconds, status, timestamp
)
WITH latest_batch AS (
    SELECT raw_data
    FROM raw_gtfs_trip_up_staging
    ORDER BY fetch_time DESC
    LIMIT 1
),
flattened_data AS (
    SELECT
        (entity ->> 'id') AS gtfs_update_id,
        (entity -> 'tripUpdate' -> 'trip' ->> 'scheduleRelationship') AS schedule_relationship,
        (entity -> 'tripUpdate' -> 'trip' ->> 'tripId') AS nk_trip_id,
        (entity -> 'tripUpdate' -> 'trip' ->> 'routeId') AS nk_route_id,
        (entity -> 'tripUpdate' -> 'vehicle' ->> 'id') AS nk_vehicle_id,
        (stu ->> 'stopId') AS nk_stop_id,
        (stu ->> 'stopSequence')::INT AS stop_sequence,

        to_timestamp((stu -> 'arrival' ->> 'time')::bigint) AS arrival_time,
        to_timestamp((stu -> 'departure' ->> 'time')::bigint) AS departure_time,

        -- Tervezett idő
        to_timestamp(
            (stu -> '[realcity.stop_time_update]' -> 'scheduledArrival' ->> 'time')::bigint
        ) AS scheduled_arrival_time

    FROM latest_batch,
         jsonb_array_elements(raw_data -> 'entity') AS entity,
         jsonb_array_elements(entity -> 'tripUpdate' -> 'stopTimeUpdate') AS stu
    WHERE (entity -> 'tripUpdate') IS NOT NULL
),
calculated_data AS (
    SELECT
        *,
        COALESCE(arrival_time, scheduled_arrival_time, NOW()) as ref_time
    FROM flattened_data
),
joined_data AS (
    SELECT
        src.gtfs_update_id,
        dt.trip_id_pk,
        dr.route_id_pk,
        ds.stop_id_pk,
        dv.vehicle_id_pk,
        dd.date_id,
        dw.weather_id,
        src.stop_sequence,
        src.arrival_time,
        src.scheduled_arrival_time,
        src.departure_time,

        EXTRACT(EPOCH FROM (src.arrival_time - src.scheduled_arrival_time))::INT AS delay_seconds,

        CASE
            WHEN src.schedule_relationship = 'CANCELED' THEN 'CANCELED'
            WHEN src.arrival_time IS NULL THEN 'NO_DATA'
            WHEN (src.arrival_time - src.scheduled_arrival_time) > INTERVAL '180 seconds' THEN 'LATE'
            WHEN (src.arrival_time - src.scheduled_arrival_time) < INTERVAL '-60 seconds' THEN 'EARLY'
            ELSE 'ON_TIME'
        END AS status,

        src.ref_time AS timestamp

    FROM calculated_data src

    LEFT JOIN dim_trip dt
        ON src.nk_trip_id = dt.trip_id
        AND src.ref_time >= dt.start_date
        AND (src.ref_time < dt.end_date OR dt.end_date IS NULL)

    LEFT JOIN dim_route dr
        ON src.nk_route_id = dr.route_id
        AND src.ref_time >= dr.start_date
        AND (src.ref_time < dr.end_date OR dr.end_date IS NULL)

    LEFT JOIN dim_stop ds
        ON src.nk_stop_id = ds.stop_id
        AND src.ref_time >= ds.start_date
        AND (src.ref_time < ds.end_date OR ds.end_date IS NULL)

    -- D. VEHICLE (Trip ID alapján!)
    LEFT JOIN dim_vehicle dv
        ON src.nk_vehicle_id = dv.vehicle_id
        AND src.arrival_time >= dv.start_date
        AND (src.arrival_time < dv.end_date OR dv.end_date IS NULL)

    LEFT JOIN dim_date dd
        ON dd.date = src.ref_time::DATE

    LEFT JOIN dim_weather dw
        ON dw.weather_timestamp = date_trunc('hour', src.ref_time)
)

SELECT DISTINCT ON (trip_id_pk, stop_sequence, date_id)
    *
FROM joined_data
ORDER BY trip_id_pk, stop_sequence, date_id, timestamp DESC

ON CONFLICT (trip_id_pk, stop_sequence, date_id)
DO UPDATE SET
    gtfs_trip_update_id = EXCLUDED.gtfs_trip_update_id,
    route_id_pk = EXCLUDED.route_id_pk,
    stop_id_pk = EXCLUDED.stop_id_pk,
    weather_id = EXCLUDED.weather_id,
    arrival_time = EXCLUDED.arrival_time,
    scheduled_arrival_time = EXCLUDED.scheduled_arrival_time,
    departure_time = EXCLUDED.departure_time,
    delay_seconds = EXCLUDED.delay_seconds,
    status = EXCLUDED.status,
    timestamp = EXCLUDED.timestamp,
    vehicle_id_pk = EXCLUDED.vehicle_id_pk;