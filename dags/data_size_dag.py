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
    dag_id='data_size_dag',
    default_args=default_args,
    description='This dag gives the total number of scraped jobs and '\
                'verifies that the datasize length for AWS RSD and AWS DynamoDB is the same',
    start_date=datetime(2023, 3, 11, 10),
    catchup=False,
    schedule_interval='@daily'
) as dag:
    @task
    def get_num_scraped_jobs_cnt():
        query = "SELECT COUNT(*) FROM jobs_info WHERE scraped_jd=true"
        rds_scraped_jds = rds_get_records(query)[0][0]
        print("RDS scraped JDs length is : ", rds_scraped_jds)

        ddb_client = get_dynamodb_con()
        ddb_table_jobs = dynamo_client.Table('Jobs')
        items_len = len(get_all_dynamodb_items(ddb_table_jobs))

        print("Dynamo DB length is ", items_len)

    @task
    def get_dataset_len():
        
        query = "SELECT COUNT(*) FROM jobs_info"
        rds_len = rds_get_records(query)[0][0]
        get_num_scraped_jobs_cnt()
        print("RDS total length is ", rds_len)

        ddb_client = get_dynamodb_con()
        ddb_table_jobs = dynamo_client.Table('Jobs')
        items_len = len(get_all_dynamodb_items(ddb_table_jobs))

        print("Dynamo DB length is ", items_len)

    get_dataset_len() >> get_num_scraped_jobs_cnt()
