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

