import sys
sys.path.append('/opt/airflow/dags/modules/')
from db import *
from scrape import *
from analyze_data import *
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from numpy.random import uniform

default_args = {
    'owner': 'Mann',
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

parallel_tasks = 4
num_scrapes = 52

with DAG(
    dag_id='analyze_jd_dag',
    default_args=default_args,
    description='This dag will analyze the job description (jd) and will extract information like '\
                'programming languages and libraries',
    start_date=datetime(2023, 3, 11, 10),
    catchup=False,
    schedule_interval='@daily'
) as dag:
    @task
    def get_split_job_ids(parallel_tasks=parallel_tasks, num_scrapes=num_scrapes):
        query = "SELECT COUNT(*) FROM jobs_info WHERE scraped_jd=false"
        rds_len = rds_get_records(query)[0][0]
        num_scrapes = min(rds_len, num_scrapes)
        print('Split Job Ids success')
        return split_job_ids(parallel_tasks, limit=num_scrapes)
    
    @task
    def analyze_jobs(job_ids):
        for job_id in job_ids:



    job_ids_split = get_split_job_ids(parallel_tasks=parallel_tasks, num_scrapes=num_scrapes)
    analyze_jobs.expand(job_ids=job_ids_split)