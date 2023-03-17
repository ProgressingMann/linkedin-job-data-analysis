import sys
sys.path.append('/opt/airflow/dags/modules/')
from db import *
from scrape import *
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task

default_args = {
    'owner': 'Mann',
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='truncate_data_dag',
    default_args=default_args,
    description='This dag performs miscellaneous tasks',
    start_date=datetime(2023, 3, 11, 10),
    catchup=False,
    schedule_interval=None
) as dag:
    
    @task
    def task_truncate_dynamo_db_jobs():
        delete_all_dynamo_db_jobs()
    
    @task
    def task_truncate_rds_jobs_info():
        query = "TRUNCATE jobs_info"
        rds_execute_query(query)

    task_truncate_dynamo_db_jobs() >> task_truncate_rds_jobs_info()