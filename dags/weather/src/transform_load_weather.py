import datetime
import json
import logging
import requests
import pytz
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


PIRATE_ICON_MAP = {
    'clear-day': '01d',
    'clear-night': '01n',
    'thunderstorm': '11',
    'rain': '10',
    'snow': '13',
    'sleet': '13',
    'wind': '50',
    'fog': '01',
    'cloudy': '04',
    'partly-cloudy-day': '02d',
    'partly-cloudy-night': '02n'
}

def get_from_staging_weather(hook, start_date):
    sql = """
          SELECT api_response
          FROM raw_staging_weather
          WHERE date_start >= %s
            AND date_start < %s;
          """
    results = hook.get_records(sql=sql, parameters=(start_date.isoformat(), datetime.datetime.now().isoformat()))
    weather_data = [row[0] if isinstance(row[0], dict) else json.loads(row[0]) for row in results if row[0] is not None]
    return weather_data

def get_perticipation(entry):
    return entry.get("precipIntensity", 0.0)

def assign_weather_variables(entry):
    icon_str = entry.get('icon', 'clear-day')
    isDay = entry.get('time', 0) % 86400 >= 21600 and entry.get('time', 0) % 86400 <= 64800
    mapped_icon = PIRATE_ICON_MAP.get(icon_str, '02d')
    if icon_str not in ['clear-day', 'clear-night', 'partly-cloudy-day', 'partly-cloudy-night']:
        mapped_icon = mapped_icon + ('d' if isDay else 'n')
    precip_type = entry.get('precipType', '')
    

    weather_info = {
        'weather_timestamp': datetime.datetime.fromtimestamp(entry['time'], tz=pytz.UTC),
        'temperature_celsius': entry.get('temperature', 0.0),
        'wind_speed_kph': entry.get('windSpeed', 0.0),
        'pressure_hpa': entry.get('pressure', 0.0),
        'humidity_percent': entry.get('humidity', 0),
        'precipitation_mm': entry.get("precipIntensity", 0.0), 
        'weather_description': entry.get('summary', ''),
        'condition_code': icon_str[:10], # Using the pirate weather string or prefix as code
        'is_rain_event': precip_type == 'rain' or 'rain' in icon_str,
        'is_snow_event': precip_type == 'snow' or precip_type == 'sleet' or 'snow' in icon_str,
        'is_ice_risk': entry.get('temperature', 10.0) <= 2.0 and (precip_type in ['snow', 'sleet', 'rain']),
        'is_foggy': 'fog' in icon_str,
        'icon_url': f"http://openweathermap.org/img/wn/{mapped_icon}@2x.png"
    }
    return weather_info


def transform_data(weather_data):
    transformed_data = []
    for entry in weather_data:
        # Pirate Weather API structures the data differently
        # Usually data is inside `daily` or `hourly` -> `data`, or just passed as the main body for "currently"
        
        # If the API response contains `hourly` with a `data` list
        if 'hourly' in entry and 'data' in entry['hourly']:
            for item in entry['hourly']['data']:
                if item['time'] <= datetime.datetime.now(pytz.UTC).timestamp():  # Only include past and current data
                    transformed_data.append(assign_weather_variables(item))
        # If it contains `daily` with a `data` list
        elif 'daily' in entry and 'data' in entry['daily']:
            for item in entry['daily']['data']:
                if item['time'] <= datetime.datetime.now(pytz.UTC).timestamp():  # Only include past and current data
                    transformed_data.append(assign_weather_variables(item))
        # If it's a single entry (e.g., `currently`)
        elif 'currently' in entry:
            transformed_data.append(assign_weather_variables(entry['currently']))
        else:
            # Fallback assuming the entry itself is the data dictionary
            transformed_data.append(assign_weather_variables(entry))
    return transformed_data


def load_transformed_data(hook, transformed_data):
    sql = """
          INSERT INTO dim_weather (weather_timestamp, temperature_celsius, wind_speed_kph, pressure_hpa,
                                   humidity_percent, precipitation_mm, weather_description, condition_code,
                                   is_rain_event, is_snow_event, is_ice_risk, is_foggy, icon_url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (weather_timestamp) DO UPDATE SET
        temperature_celsius = EXCLUDED.temperature_celsius,
        wind_speed_kph = EXCLUDED.wind_speed_kph,
        pressure_hpa = EXCLUDED.pressure_hpa,
        humidity_percent = EXCLUDED.humidity_percent,
        precipitation_mm = EXCLUDED.precipitation_mm,
        weather_description = EXCLUDED.weather_description,
        condition_code = EXCLUDED.condition_code,
        is_rain_event = EXCLUDED.is_rain_event,
        is_snow_event = EXCLUDED.is_snow_event,
        is_ice_risk = EXCLUDED.is_ice_risk,
        is_foggy = EXCLUDED.is_foggy,
        icon_url = EXCLUDED.icon_url
    """
    for data in transformed_data:
        hook.run(sql=sql, parameters=(
            data['weather_timestamp'],
            data['temperature_celsius'],
            data['wind_speed_kph'],
            data['pressure_hpa'],
            data['humidity_percent'],
            data['precipitation_mm'],
            data['weather_description'],
            data['condition_code'],
            data['is_rain_event'],
            data['is_snow_event'],
            data['is_ice_risk'],
            data['is_foggy'],
            data['icon_url']
        ))
    logging.info(f"Transformed weather data loaded into dim_weather table")

def get_last_weather(hook):
    result = hook.get_first("SELECT MAX(weather_timestamp) FROM dim_weather")
    if result and result[0]:
        return result[0]
    return datetime.datetime(2025, 1, 1, tzinfo=pytz.UTC)

def transform_load_weather(**kwargs):
    hook = PostgresHook(postgres_conn_id='bkk_dwh')
    start_date = get_last_weather(hook)
    logging.info(f"Transforming and loading weather data from {start_date} to current time")

    weather_data = get_from_staging_weather(hook, start_date)
    transformed_data = transform_data(weather_data)
    load_transformed_data(hook, transformed_data)
    logging.info("Transform and load process completed")


if __name__ == "__main__":
    transform_load_weather(
        data_interval_start=datetime.datetime(2024, 11, 22, 0, 0, tzinfo=pytz.UTC),
        data_interval_end=datetime.datetime(2024, 11, 22, 1, 0, tzinfo=pytz.UTC)
    )
