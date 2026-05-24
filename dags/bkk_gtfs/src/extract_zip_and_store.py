import datetime
import zipfile
import pandas as pd
import json
import logging
import requests
from airflow.hooks.postgres_hook import PostgresHook

TABLE_FILLS = {
    "dim_route": {
        "table_name": "routes.txt",
        "natural_key": "route_id",
        "columns": [
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "route_color"]
    },
    "dim_trip": {
        "table_name": "trips.txt",
        "natural_key": "trip_id",
        "columns": [
            "route_id",
            "trip_id",
            "service_id",
            "trip_headsign",
            "direction_id",
            "shape_id"
        ]
    },
    "dim_stop": {
        "table_name": "stops.txt",
        "natural_key": "stop_id",
        "columns": [
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "wheelchair_boarding"]
    },
    "bridge_gtfs_service_dates": {
        "table_name": "calendar_dates.txt",
        "columns": [
            "service_id",
            "date",
            "exception_type"
        ]
    },
    "gtfs_shape_paths": {
        "table_name": "shapes.txt",
        "columns": [
            "shape_id",
            "shape_pt_sequence",
            "shape_pt_lat",
            "shape_pt_lon",
            "shape_dist_traveled"
        ]
    },
}


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date
    if "wheelchair_boarding" in df.columns:
        df["wheelchair_boarding"] = pd.to_numeric(df["wheelchair_boarding"], errors="coerce").astype("Int64")
    if "route_type" in df.columns:
        df["route_type"] = pd.to_numeric(df["route_type"], errors="coerce").astype("Int64")
    return df.replace({float('nan'): None})

def store_data(df: pd.DataFrame, info, table, cur):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(info["columns"])
    values_placeholder = ','.join(['%s'] * len(info["columns"]))
    args_bytes = b','.join(cur.mogrify(f"({values_placeholder})", x) for x in tuples)
    args_str = args_bytes.decode('utf-8')
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES " + args_str + " ON CONFLICT DO NOTHING;")

def extract_and_store(**kwargs):
    zip_file = zipfile.ZipFile(kwargs['ti'].xcom_pull(key="gtfs_zip_path"))

    hook = PostgresHook(postgres_conn_id='bkk_dwh')
    conn = hook.get_conn()
    cur = conn.cursor()

    for table, info in TABLE_FILLS.items():
        logging.info(f"Processing table: {table}")
        with zip_file.open(info["table_name"]) as file:
            df = pd.read_csv(file, usecols=info["columns"])
            df = _transform(df)

            if info.get("natural_key"):
                temp_table = f"staging_{table}"
                col_list_str = ", ".join(info["columns"])
                nk = info["natural_key"]
                cols = info["columns"]

                logging.info(f"Natural key: {nk}")

                cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
                logging.info(f"Creating table: {temp_table}")
                cur.execute(f"""
                                CREATE TEMP TABLE {temp_table} 
                                AS SELECT {col_list_str} 
                                FROM {table} 
                                WITH NO DATA
                            """)

                store_data(df, info, temp_table, cur)

                compare_condition = " OR ".join([
                    f"target.{col} IS DISTINCT FROM source.{col}"
                    for col in cols if col != nk
                ])

                sql_update = f"""
                                UPDATE {table} target
                                SET end_date = CURRENT_TIMESTAMP
                                    FROM {temp_table} source
                                WHERE target.{nk} = source.{nk}
                                  AND target.end_date IS NULL
                                  AND ({compare_condition});
                                """
                cur.execute(sql_update)
                logging.info(f"Closed changed records in {table}")

                select_cols = ', '.join([f"source.{c}" for c in cols])

                sql_insert = f"""
                                INSERT INTO {table} ({col_list_str}, start_date, end_date)
                                SELECT {select_cols}, CURRENT_TIMESTAMP, NULL
                                FROM {temp_table} source
                                LEFT JOIN {table} target 
                                    ON source.{nk} = target.{nk} AND target.end_date IS NULL
                                WHERE 
                                    target.{nk} IS NULL          -- Teljesen új rekord
                                    OR ({compare_condition});    -- Változott (új verzió nyitása)
                                """
                cur.execute(sql_insert)
                logging.info(f"Inserted new and changed records into {table}")
                cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

            else:
                store_data(df, info, table, cur)


    conn.commit()
    cur.close()
    logging.info("GTFS data extraction and storage completed.")


if __name__ == '__main__':
    extract_and_store()
