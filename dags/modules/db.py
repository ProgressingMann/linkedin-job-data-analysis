import boto3 # to interact with AWS
import os
import requests
import tqdm
import mysql.connector
import sys

from AWS_access_management import host, dbname, user, pwd, port, aws_access_key_id, aws_secret_access_key

def get_rds_connection():
     return mysql.connector.connect(host=host, database=dbname, user=user, password=pwd)

def rds_execute_query(query):
    rds_con, cursor = None, None
    try:
        rds_con = get_rds_connection()
        cursor = rds_con.cursor()

        cursor.execute(query)
        rds_con.commit()

    except Exception as e:
        print("ERROR!")
        print(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if rds_con is not None:
            rds_con.close()

        return None
    
def rds_insert_update_records(query, records):
    rds_con, cursor = None, None
    records = list(records)
    try:
        rds_con = get_rds_connection()
        cursor = rds_con.cursor()
        if not isinstance(records[0], tuple):
            for i in range(len(records)):
                records[i] = (records[i], )
                
        cursor.executemany(query, records)
        rds_con.commit()
        
        print(f'Added {len(records)} records')
    
    except Exception as e:
        print("ERROR!")
        print(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if rds_con is not None:
            rds_con.close()
        
        return None

    
def rds_get_records(query):
    rds_con, cursor = None, None
    result = None
    try:
        rds_con = get_rds_connection()
        cursor = rds_con.cursor()
        
        cursor.execute(query)
        result = cursor.fetchall()

    except Exception as e:
        print("ERROR!")
        print(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if rds_con is not None:
            rds_con.close()
        return result
    

def rds_store_unique_job_ids(new_job_ids):
    query = "INSERT INTO jobs_info(id, salary_lower, salary_upper, date_posted, scraped_jd) "\
            "VALUES (%s,%s,%s,%s,%s)"
    new_records = []

    for job_id, job_data in new_job_ids.items():
        record = (job_id, job_data['salary_lower'], job_data['salary_upper'], 
                            job_data['date_posted'], False)
        new_records.append(tuple(record))
    rds_insert_update_records(query, new_records)
    
    print(f'Successfully added {len(new_records)} records')


def rds_get_job_ids(job_type='all', limit=None):
    query = None
    if job_type == 'all': # get all the unique job ids
        query = 'SELECT id FROM jobs_info'
    if job_type == 'unscrapped': # retrieve job ids whose job descriptions are unscrapped
        query = f'SELECT id FROM jobs_info WHERE scraped_jd=false LIMIT {limit}'

    result = rds_get_records(query)
    unique_job_ids = [row[0] for row in result]
    return unique_job_ids


def split_job_ids(num_splits, limit=52):
    unique_job_ids = rds_get_job_ids(job_type='unscrapped', limit=limit)
    i = 0
    dr = len(unique_job_ids) // num_splits
    split_values = []
    
    for i in range(num_splits-1):
        start, end = dr*i, dr*(i+1)
        split_values.append(unique_job_ids[start:end])

    split_values.append(unique_job_ids[dr*(i+1):])
    
    return split_values


def get_dynamodb_con():
    dynamo_client = boto3.resource(service_name = 'dynamodb',region_name = 'us-east-2',
                aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    ddb_jobs_con = None
    try:
        ddb_jobs_con = dynamo_client.Table('Jobs')
        # print(f'Status of the Table in DynamoDB is {ddb_jobs_con.table_status}')
    
    except Exception as e:
        print("ERROR!")
        print(e)
        return
    
    return ddb_jobs_con


def get_all_dynamodb_items(ddb_table):
    lastEvaluatedKey = None
    items = [] # Result Array

    while True:
        if lastEvaluatedKey == None:
            response = ddb_table.scan() # This only runs the first time - provide no ExclusiveStartKey initially
        else:
            response = ddb_table.scan(
            ExclusiveStartKey=lastEvaluatedKey # In subsequent calls, provide the ExclusiveStartKey
        )

        items.extend(response['Items']) # Appending to our resultset list
        

        # Set our lastEvlauatedKey to the value for next operation,
        # else, there's no more results and we can exit
        if 'LastEvaluatedKey' in response:
            lastEvaluatedKey = response['LastEvaluatedKey']
        else:
            break

    return items

def store_job_description_dynamo_db(json):
    ddb_jobs_con = get_dynamodb_con()
    ddb_jobs_con.put_item(Item = json)
    if json:
        job_id = json['job_id']
        # print(f'stored record for job id {job_id} into DynamoDB successfully')


def delete_all_dynamo_db_jobs():
    ddb_jobs_con = get_dynamodb_con()
    
    with ddb_jobs_con.batch_writer() as batch:
        for item in ddb_jobs_con.scan()['Items']:
            response = batch.delete_item(Key={
                "job_id": item["job_id"], 
                "job_title": item["job_title"]
            })