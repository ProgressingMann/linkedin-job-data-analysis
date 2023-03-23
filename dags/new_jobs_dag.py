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

def helper_rds_store_unique_job_ids(ti):
    new_job_ids = ti.xcom_pull(task_ids='scrape_new_job_ids')['new_job_ids']
    rds_store_unique_job_ids(new_job_ids)
    print(f"Successully added {len(new_job_ids)} new job ids")

new_jobs_needed = 100
refresh=True
posted_ago='month'
title='data_scientist'

with DAG(
    dag_id='get_new_job_ids',
    default_args=default_args,
    description='This dag will scrape new job ids and will store it on the AWS RDS',
    start_date=datetime(2023, 3, 11, 10),
    catchup=False,
    schedule_interval='@daily'
) as dag:
    task_scrape_new_job = PythonOperator(
        task_id = 'scrape_new_job_ids',
        python_callable=get_new_jobs,
        op_kwargs={'new_jobs_needed': new_jobs_needed, 'refresh': refresh, 
                    'posted_ago': posted_ago, 'title': title}
    )

    task_store_new_jobs = PythonOperator(
        task_id='store_new_job_ids',
        python_callable=helper_rds_store_unique_job_ids
    )

    task_scrape_new_job >> task_store_new_jobs