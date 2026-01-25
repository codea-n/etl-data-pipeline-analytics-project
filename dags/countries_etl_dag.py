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
    dag_id="country_etl_daily",
    default_args=default_args,
    description="Daily ETL for country borders data",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "countries"],
) as dag:

    run_etl = PythonOperator(
        task_id='run_country_etl',
        python_callable=run_pipeline
    )

    run_etl
