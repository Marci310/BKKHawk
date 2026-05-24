from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from bkk_gtfs.src.get_gtfs_zip import get_gtfs_zip
from bkk_gtfs.src.extract_zip_and_store import extract_and_store

with DAG(
    dag_id="load_dim_bkk_gtfs",
    start_date=datetime(2019, 1, 1, 0, 0),
    schedule='@daily',
    catchup=False,
) as dag:

    create_dims = PostgresOperator(
        task_id="create_bkk_gtfs_dims",
        postgres_conn_id="bkk_dwh",
        sql=["sql/dims/dim_route_schema.sql", "sql/dims/dim_stop_schema.sql", "sql/dims/dim_trip_schema.sql"])

    create_helpers = PostgresOperator(
        task_id="create_bkk_gtfs_helpers",
        postgres_conn_id="bkk_dwh",
        sql=["sql/helpers/bridge_gtfs_service_dates_schema.sql", "sql/helpers/gtfs_shape_paths_schema.sql"])

    #create_foreign_keys = PostgresOperator(
    #    task_id="create_bkk_gtfs_foreign_keys",
    #    postgres_conn_id="bkk_dwh",
    #    sql="sql/add_foreign_keys.sql"
    #)

    get_gtfs = PythonOperator(
        task_id="get_bkk_gtfs_zip",
        python_callable=get_gtfs_zip,
    )

    extract_store = PythonOperator(
        task_id="extract_and_store_bkk_gtfs",
        python_callable=extract_and_store,
    )

    create_dims >> create_helpers >> get_gtfs >> extract_store





