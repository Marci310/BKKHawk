import datetime
import json
import logging
import requests
import pytz
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

UTC = pytz.UTC

def store_weather_data(hook, api_url, start_date=None):
    response = requests.get(api_url)
    data = response.json()

    if 'currently' in data:
        actual_start_time = datetime.datetime.fromtimestamp(data['currently']['time'], tz=UTC)
    elif 'hourly' in data and len(data['hourly']) > 0:
        actual_start_time = datetime.datetime.fromtimestamp(data['hourly']['data'][0]['time'], tz=UTC)
    else:
        actual_start_time = start_date

    sql = """
          INSERT INTO raw_staging_weather (fetch_date, date_start, api_response)
          VALUES (NOW(), %s, %s); \
          """
    hook.run(sql=sql, parameters=( actual_start_time.isoformat(), json.dumps(data)))
    logging.info("Successfully stored weather data for date: %s", actual_start_time.isoformat())

def load_historical_weather(date: datetime.datetime, hook, api_key, end_date: datetime.datetime = None):
    if end_date > datetime.datetime.now(pytz.UTC):
        end_date = datetime.datetime.now(pytz.UTC)

    current_process_date = date

    while current_process_date < end_date:
        logging.info(f"Loading 24h historical weather chunk starting from: {current_process_date}")
        unix_ts = int(current_process_date.timestamp())
        api_url = f"https://timemachine.pirateweather.net/forecast/{api_key}/47.497913,19.040236,{unix_ts}?exclude=currently&units=si"

        try:
            store_weather_data(hook, api_url, current_process_date)
        except Exception as e:
            logging.error(f"Error loading history chunk: {e}")
        current_process_date += datetime.timedelta(days=1)


def get_last_weather(hook):
    result = hook.get_first("SELECT MAX(weather_timestamp) FROM dim_weather")
    if result and result[0]:
        ts = result[0]
        if ts.tzinfo is None:
            ts = UTC.localize(ts)
        else :
            ts = ts.astimezone(UTC)
        return ts

    return datetime.datetime(2025, 1, 1, tzinfo=UTC)

def load_latest_weather(hook, api_key):
    logging.info("Loading latest weather data")
    api_url = f"https://api.pirateweather.net/forecast/{api_key}/47.497913,19.040236?exclude=minutely%2Chourly%2Cdaily%2Calerts&version=2&units=si"
    store_weather_data(hook, api_url)
    logging.info(f"Latest weather data inserted into raw_staging_weather table")

def delete_old_weather_data(hook, hours=2):
    sql = f"""
          DELETE FROM raw_staging_weather
          WHERE fetch_date < NOW() - INTERVAL '{hours} hours';
          """
    hook.run(sql=sql)
    logging.info(f"Old weather data older than {hours} hours removed from raw_staging_weather")


def extract_weather(**kwargs):
    hook = PostgresHook(postgres_conn_id='bkk_dwh')
    api_key = Variable.get('pirate_weather_api_key')
    latest_weather = get_last_weather(hook)

    now_utc = datetime.datetime.now(UTC)

    time_diff = now_utc - latest_weather
    if time_diff > datetime.timedelta(hours=2):
        logging.info(f"Gap detected ({time_diff}), switching to HISTORICAL load.")

        load_historical_weather(latest_weather + datetime.timedelta(hours=1), hook, api_key, now_utc)
    else:
        # Ha nincs nagy lyuk, csak a friss adatot kérjük le
        logging.info("No significant gap, loading CURRENT weather.")
        load_latest_weather(hook, api_key)
    #data_retention = Variable.get('data_retention_days')*24  # in hours
    #delete_old_weather_data(hook)


if __name__ == "__main__":
    extract_weather()