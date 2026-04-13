from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import historical_loader 
default_args = {
    'owner': 'abdallah',
    'retries': 0, 
}

with DAG(
    dag_id='crypto_historical_setup_v1',
    default_args=default_args,
    start_date=datetime(2026, 4, 7),
    schedule_interval='@once',
    catchup=False
) as dag:

    task_hist = PythonOperator(
        task_id='load_historical_data',
        python_callable=historical_loader
    )