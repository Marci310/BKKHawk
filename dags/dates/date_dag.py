import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

with DAG(
        dag_id="date_dag",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
) as dag:
    create_dim_date = PostgresOperator(
        task_id="create_dim_date",
        postgres_conn_id="bkk_dwh",
        sql="sql/dim_date_schema.sql",
    )
    generate_dates = PostgresOperator(
        task_id="generate_dates",
        postgres_conn_id="bkk_dwh",
        sql="sql/generate_dates.sql",
    )
    create_dim_date >> generate_dates
