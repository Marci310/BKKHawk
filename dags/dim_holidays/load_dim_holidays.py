from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from dim_holidays.src.extract_holidays import extract_holidays

with DAG(
    dag_id="load_dim_holidays",
    start_date=datetime(2019, 1, 1),
    schedule='@yearly',
    catchup=False,
) as dag:

    create_staging_table = PostgresOperator(
        task_id="create_staging_holiday",
        postgres_conn_id="bkk_dwh",
        sql="sql/staging_date_schema.sql",
    )
    extract_holidays = PythonOperator(
        task_id="extract_holidays",
        python_callable=extract_holidays,
    )
    transform_load_holidays = PostgresOperator(
        task_id="transform_load_holidays",
        postgres_conn_id="bkk_dwh",
        sql="sql/updating_date_dim.sql",
        parameters={
            "year_to_process": "{{ data_interval_start.year+1 }}"
        }
    )
    create_staging_table >> extract_holidays >> transform_load_holidays


