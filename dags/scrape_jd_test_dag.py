import sys
sys.path.append('/opt/airflow/dags/modules/')
from db import *
from scrape import *
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
    dag_id='scrape_jds_new_jobs',
    default_args=default_args,
    description='This dag will scrape job description for job IDs where scraped_jd=false and'\
                'will store it on the AWS RDS and DynamoDB',
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
    def scrape_jobs(job_ids):
        for job_id in job_ids:
            store_job_description_data(job_id)
            sleep_time = np.random.uniform(2, 4)
            print(f'sleeping for {sleep_time}')
            time.sleep(sleep_time)
    
    
    jobs_ids_split = get_split_job_ids(parallel_tasks=parallel_tasks, num_scrapes=num_scrapes)
    scrape_jobs.expand(job_ids=jobs_ids_split)


# https://marclamberti.com/blog/dynamic-task-mapping-in-apache-airflow/
# https://docs.astronomer.io/learn/dynamic-tasks?tab=taskflow#dynamic-task-concepts