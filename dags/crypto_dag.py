from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main import run_full_pipeline 

default_args = {
    'owner': 'abdallah',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_etl_pipeline_v1',
    default_args=default_args,
    start_date=datetime(2026, 4, 7),
    schedule_interval='@hourly', 
    catchup=False
) as dag:

    task_extract_load = PythonOperator(
        task_id='fetch_and_load_to_postgres',
        python_callable=run_full_pipeline
    )