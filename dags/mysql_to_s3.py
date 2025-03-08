# type: ignore

import logging as log
import datetime as dt
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2025, 1, 1),
    'retry_delay': dt.timedelta(minutes=5),
    'retries': 5
}

def get_dag_configs():
    mysql_hook = MySqlHook(mysql_conn_id="my_mysql")
    records = mysql_hook.get_records("select * from jobgram_main.jobs_main")
    log.info(f"Records: {records}")
    return records

dag = DAG(
    dag_id='mysql_to_s3',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

task1 = PythonOperator(
    task_id='get_dag_configs',
    python_callable=get_dag_configs,
    dag=dag
)

task1