import datetime
import json
import logging
import requests
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


def extract_holidays(**kwargs):
    hook = PostgresHook(postgres_conn_id='bkk_dwh')

    logical_date = kwargs["data_interval_start"]

    year = logical_date.year+1 if logical_date else datetime.datetime.now().year
    api_key = Variable.get('holiday_api_key')
    api_url = f"https://szunetnapok.hu/api/{api_key}/{year}/json"

    logging.info(f"Getting data for the year: {year}")
    response = requests.get(api_url)
    response.raise_for_status()

    data = response.json()
    if data.get('response') != 'OK':
        raise ValueError(f"API hiba: {data.get('message')}")

    logging.info("Data retrieved successfully from the API")

    sql = """
          INSERT INTO raw_staging_date (year, fetch_date, api_response)
          VALUES (%s,%s,%s); \
          """
    hook.run(sql=sql, parameters=(year,datetime.datetime.now(), json.dumps(data)))
    logging.info("Data inserted into raw_staging_holiday table")


if __name__ == "__main__":
    extract_holidays()
