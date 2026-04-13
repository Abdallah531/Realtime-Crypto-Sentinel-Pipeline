from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main import transform_crypto_data

default_args = {
    'owner': 'abdallah',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='crypto_data_transformation_v1',
    default_args=default_args,
    start_date=datetime(2026, 4, 7),
    schedule_interval='@hourly', 
    catchup=False
) as dag:

  
    wait_for_hourly_data = ExternalTaskSensor(
        task_id='wait_for_hourly_ingestion',
        external_dag_id='crypto_etl_pipeline_v1', 
        external_task_id='fetch_and_load_to_postgres',  
        timeout=600,
        poke_interval=30,
        mode='reschedule', 
        check_existence=True 
    )


    task_transform = PythonOperator(
        task_id='clean_and_merge_data',
        python_callable=transform_crypto_data
    )


    wait_for_hourly_data >> task_transform