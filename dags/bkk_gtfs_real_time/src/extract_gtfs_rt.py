import datetime
import json
import logging
import requests
import pytz
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.protobuf.json_format import MessageToJson

# Try relative import first (for Airflow), fall back to absolute import
try:
    from . import gtfs_realtime_pb2
    from . import gtfs_realtime_realcity_pb2
except ImportError:
    import sys
    from pathlib import Path
    # Add the src directory to the path for standalone execution
    src_dir = Path(__file__).parent
    sys.path.insert(0, str(src_dir))
    import gtfs_realtime_pb2
    import gtfs_realtime_realcity_pb2


def store_gtfs_rt_data(hook, api_url, table_name):
    response = requests.get(api_url)

    # Parse the protobuf message
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Convert to JSON
    data = MessageToJson(feed, preserving_proto_field_name=False, use_integers_for_enums=True)
    data_dict = json.loads(data)

    sql = f"""
          INSERT INTO {table_name} (fetch_time, bkk_time, raw_data)
          VALUES (NOW(), %s, %s); \
          """
    hook.run(sql=sql, parameters=(datetime.datetime.fromtimestamp(int(data_dict['header']['timestamp']), tz=pytz.UTC), data))

def remove_old_data(hook, table_name, hours=2):
    sql = f"""
          DELETE FROM {table_name}
          WHERE fetch_time < NOW() - INTERVAL '{hours} hours';
          """
    hook.run(sql=sql)
    logging.info(f"Old data older than {hours} hours removed from {table_name}")


def extract_gtfs_rt(**kwargs):
    hook = PostgresHook(postgres_conn_id='bkk_dwh')
    api_key = Variable.get('bkk_api_key')
    trip_api_url = f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb?key={api_key}"
    vehicle_api_url = f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key={api_key}"
    #data_retention = Variable.get('data_retention')*24  # in hours
    store_gtfs_rt_data(hook, trip_api_url, 'raw_gtfs_trip_up_staging')
    store_gtfs_rt_data(hook, vehicle_api_url, 'raw_gtfs_vehicle_pos_staging')
    # data_retention = Variable.get('data_retention')*24  # in hours
    #remove_old_data(hook, 'raw_gtfs_trip_up_staging', data_retention)
    #remove_old_data(hook, 'raw_gtfs_vehicle_pos_staging', data_retention)
    remove_old_data(hook, 'raw_gtfs_trip_up_staging')
    remove_old_data(hook, 'raw_gtfs_vehicle_pos_staging')

    logging.info(f"GTFS-RT data inserted into staging tables")

if __name__ == "__main__":
    extract_gtfs_rt()