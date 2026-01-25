from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# import your existing run_pipeline function
from etl import run_pipeline

# default args for retries, etc.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'country_etl',
    default_args=default_args,
    description='Daily ETL for country borders',
    schedule_interval='@daily',  # runs every day
    start_date=datetime(2026, 1, 13),
    catchup=False,
    tags=['ETL'],
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=run_pipeline
    )

    run_etl
