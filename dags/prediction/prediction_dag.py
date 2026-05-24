from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from prediction.src.predict_future import predict_future


with DAG(
        dag_id="prediction_dag",
        start_date=datetime(2025, 1, 1),
        schedule_interval='*/30 * * * *',
        catchup=False,
) as dag:
    create_fact_prediction = PostgresOperator(
        task_id="create_fact_prediction",
        postgres_conn_id="bkk_dwh",
        sql="sql/fact_prediction_schema.sql",
    )
    predict_future = PythonOperator(
        task_id="transform_load_weather",
        python_callable=predict_future,
    )
    create_fact_prediction >> predict_future
