# type: ignore

import os
import io
import csv
import json
import logging
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

CONFIG_FILE_PATH = "/opt/airflow/config/dag_metadata.json"

default_args = {
    'owner': '@mensenvau',
    'start_date': dt.datetime(2025, 3, 1),
    'retry_delay': dt.timedelta(minutes=5),
    'retries': 5
}

def get_dag_configs():
    configs = []
    if os.path.exists(CONFIG_FILE_PATH):
        with open(CONFIG_FILE_PATH, "r") as f:
            configs = json.load(f)
    logging.info(f"DAG Configs Count: {len(configs)}")
    return configs

def mysql_to_s3(config, ds, ds_nodash):
    logging.info(f"Config: {config}")
    
    hook = MySqlHook(mysql_conn_id=config["src_conn_id"])
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    query = f"SELECT * FROM {config['src_database_name']}.{config['src_table_name']} WHERE CAST({config["src_delta_column"]} AS DATE) = '{ds}'"
    cursor.execute(query)

    column_names = [i[0] for i in cursor.description]
    result = cursor.fetchall()
    logging.info(f"Extracted {len(result)} rows from MySQL")

    if not result:
        logging.warning("No data extracted. Skipping CSV creation.")
        return False

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(column_names)
    csv_writer.writerows(result)

    s3 = S3Hook(aws_conn_id=config["tgt_conn_id"])
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=f"{config['tgt_folder_name']}/data_{ds_nodash}.csv",
        bucket_name=config["tgt_bucket_name"],
        replace=True
    )
    logging.info("Data successfully uploaded to S3")

    cursor.close()
    connection.close()
    return True

for config in get_dag_configs():
    dag = DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        schedule_interval='@daily'
    )

    copy_data = PythonOperator(
        task_id='mysql_to_s3_task',
        python_callable=mysql_to_s3,
        provide_context=True,
        op_kwargs={"config": config},
        dag=dag
    )

    globals()[config["dag_id"]] = dag
