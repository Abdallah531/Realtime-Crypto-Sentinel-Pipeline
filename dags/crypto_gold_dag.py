from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from main import create_gold_correlation, create_gold_signals, create_gold_daily_summary

default_args = {
    'owner': 'abdallah',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_gold_analysis_v1',
    default_args=default_args,
    description='Calculates RSI, Anomalies, and Correlation',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    
    wait_for_transformation = ExternalTaskSensor(
        task_id='wait_for_silver_layer',
        external_dag_id='crypto_data_transformation_v1',
        external_task_id='clean_and_merge_data', 
        timeout=600,
        poke_interval=60,
        mode='reschedule'
    )

  
    def run_gold_pipeline():
        create_gold_correlation()
        create_gold_signals()
        create_gold_daily_summary()

    task_gold_logic = PythonOperator(
        task_id='calculate_gold_metrics',
        python_callable=run_gold_pipeline
    )

    wait_for_transformation >> task_gold_logic