import datetime
import zipfile
import pandas as pd
import json
import logging
import requests
from airflow.hooks.postgres_hook import PostgresHook

TABLE_VEHICLE = {
    "name": "dim_vehicle",
    "temp_table": "staging_vehicle_positions_temp",
    "natural_key": "vehicle_id",
    "columns": [
        "vehicle_id",
        "license_plate",
        "model",
        "type",
        "wheelchair_accessible",
        "current_route_id"
    ],
    "json_cols": [
        "vehicle.vehicle.id",
        "vehicle.vehicle.licensePlate",
        "vehicle.vehicle.[realcity.vehicle].vehicleModel",
        "vehicle.vehicle.[realcity.vehicle].vehicleType",
        "vehicle.vehicle.wheelchairAccessible",
        "vehicle.trip.routeId"
    ],
    "dynamic_columns": [
        "current_route_id"
    ],
    "static_columns": [
        "vehicle_id",
        "license_plate",
        "model",
        "type",
        "wheelchair_accessible"
    ]

}


def get_from_staging_vehicle_positions(cur):
    sql = """
          SELECT raw_data
          FROM raw_gtfs_vehicle_pos_staging
          ORDER BY fetch_time DESC
          LIMIT 1;
          """
    cur.execute(sql)
    result = cur.fetchone()
    if result:
        return result[0]
    return None


def stage_vehicle_positions(cur, vehicle_positions_json):
    vehicle_positions = pd.json_normalize(vehicle_positions_json, record_path='entity')
    cur.execute(f"DROP TABLE IF EXISTS {TABLE_VEHICLE['temp_table']};")
    logging.info(f"Creating table {TABLE_VEHICLE['temp_table']}")

    cur.execute(f"""
                    CREATE TEMP TABLE {TABLE_VEHICLE['temp_table']} 
                    AS SELECT {', '.join(TABLE_VEHICLE['columns'])}
                    FROM dim_vehicle 
                    WITH NO DATA
                """)
    data = vehicle_positions[TABLE_VEHICLE['json_cols']].to_numpy()
    cols = ', '.join(TABLE_VEHICLE['columns'])
    placeholders = ', '.join(['%s'] * len(TABLE_VEHICLE['columns']))
    tuples = [tuple(x) for x in data]
    args_bytes = b','.join(cur.mogrify(f"({placeholders})", x) for x in tuples)
    args_str = args_bytes.decode('utf-8')
    cur.execute(f"INSERT INTO {TABLE_VEHICLE['temp_table']} ({cols}) VALUES " + args_str + " ON CONFLICT DO NOTHING;")


def update_dim_vehicle(cur):
    static_cols = ', '.join(TABLE_VEHICLE['static_columns'])
    cols = ', '.join(TABLE_VEHICLE['columns'])
    compare_condition = " OR ".join([
        f"target.{col} IS DISTINCT FROM source.{col}"
        for col in TABLE_VEHICLE['static_columns'] if col != TABLE_VEHICLE['natural_key']
    ])
    logging.warning(f"Comparing static columns: {compare_condition}")
    sql_update = f"""
                    UPDATE dim_vehicle target
                    SET end_date = CURRENT_TIMESTAMP
                    FROM {TABLE_VEHICLE['temp_table']} source
                    WHERE target.{TABLE_VEHICLE['natural_key']} = source.{TABLE_VEHICLE['natural_key']}
                      AND target.end_date IS NULL
                      AND ({compare_condition});
                    """
    cur.execute(sql_update)

    logging.info(f"Closed changed records in {TABLE_VEHICLE['name']}")
    select_cols = ', '.join([f"source.{c}" for c in TABLE_VEHICLE['columns']])
    sql_insert = f"""
                    INSERT INTO dim_vehicle ({cols}, start_date, end_date)
                    SELECT {select_cols}, CURRENT_TIMESTAMP, NULL
                    FROM {TABLE_VEHICLE['temp_table']} source
                    LEFT JOIN dim_vehicle target
                        ON source.{TABLE_VEHICLE['natural_key']} = target.{TABLE_VEHICLE['natural_key']} AND target.end_date IS NULL
                    WHERE target.{TABLE_VEHICLE['natural_key']} IS NULL
                    """
    cur.execute(sql_insert)

    sql_update = f"""
                    UPDATE dim_vehicle target
                    SET current_route_id = source.current_route_id
                    FROM {TABLE_VEHICLE['temp_table']} source
                    WHERE target.{TABLE_VEHICLE['natural_key']} = source.{TABLE_VEHICLE['natural_key']}
                      AND target.end_date IS NULL
                      AND NOT ({compare_condition})
                      AND target.current_route_id IS DISTINCT FROM source.current_route_id;
                    """
    cur.execute(sql_update)

    cur.execute(f"DROP TABLE {TABLE_VEHICLE['temp_table']}")
    logging.info(f"Closed changed records in {TABLE_VEHICLE['name']}")


def transform_and_load_vehicle(**kwargs):
    hook = PostgresHook(postgres_conn_id='bkk_dwh')
    conn = hook.get_conn()
    cur = conn.cursor()
    vehicle_data = get_from_staging_vehicle_positions(cur)
    if not vehicle_data:
        logging.warning("No data found in staging tables.")
        return
    if not vehicle_data or 'entity' not in vehicle_data or not vehicle_data['entity']:
        logging.warning("No 'entity' found in GTFS-RT vehicle data. Skipping processing.")
        return
    stage_vehicle_positions(cur, vehicle_data)
    update_dim_vehicle(cur)
    conn.commit()
    cur.close()


if __name__ == '__main__':
    transform_and_load_vehicle()
