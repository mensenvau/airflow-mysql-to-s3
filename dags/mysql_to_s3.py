# type: ignore

from datetime import datetime, timedelta
from airflow import DAG 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'retries': 5
}

dag = DAG(
    dag_id='mysql_to_s3',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)
