from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from weather.src.extract_weather import extract_weather
from weather.src.transform_load_weather import transform_load_weather

with DAG(
    dag_id="load_dim_weather",
    start_date=datetime(2019, 1, 1, 0, 0),
    schedule='@hourly',
    catchup=False,
) as dag:

    create_staging_table = PostgresOperator(
        task_id="create_staging_weather",
        postgres_conn_id="bkk_dwh",
        sql="sql/staging_weather_schema.sql",
    )
    create_dim_weather = PostgresOperator(
        task_id="create_dim_weather",
        postgres_conn_id="bkk_dwh",
        sql="sql/dim_weather_schema.sql",
    )
    extract_weather = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )
    transform_load_weather = PythonOperator(
        task_id="transform_load_weather",
        python_callable=transform_load_weather,
    )
    create_staging_table >> create_dim_weather >> extract_weather >> transform_load_weather