from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from bkk_gtfs_real_time.src.extract_gtfs_rt import extract_gtfs_rt
from bkk_gtfs_real_time.src.transform_and_load_vehicle import transform_and_load_vehicle

with DAG(
    dag_id="load_fact_stop_time_delay_bkk",
    start_date=datetime(2019, 1, 1, 0, 0),
    schedule='*/5 * * * *',
    catchup=False,
) as dag:

    create_staging_tables = PostgresOperator(
        task_id="create_staging_bkk_rt",
        postgres_conn_id="bkk_dwh",
        sql=["sql/staging/staging_gtfs_trip.sql", "sql/staging/staging_gtfs_vehicle.sql"]
    )
    create_dim_and_facts = PostgresOperator(
        task_id="create_dim_and_facts",
        postgres_conn_id="bkk_dwh",
        sql=["sql/dim_and_facts/dim_vehicle_schema.sql", "sql/dim_and_facts/fact_stop_time_delay_schema.sql"]
    )


    # Placeholder for extract_bkk function
    extract_bkk = PythonOperator(
        task_id="extract_gtfs_rt",
        python_callable=extract_gtfs_rt,
    )
    # Placeholder for transform_load_bkk function
    transform_load_bkk = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load_vehicle
    )

    load_fact_stop_time_delay_bkk = PostgresOperator(
        task_id="load_fact_stop_time_delay_bkk",
        postgres_conn_id="bkk_dwh",
        sql="sql/dim_and_facts/load_fact_stop_time_delay.sql"
    )

    back_fill_weather = PostgresOperator(
        task_id="back_fill_weather",
        postgres_conn_id="bkk_dwh",
        sql="sql/back_fill_weather.sql",
    )

    create_staging_tables >> create_dim_and_facts >> extract_bkk >> transform_load_bkk >> load_fact_stop_time_delay_bkk >> back_fill_weather