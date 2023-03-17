import boto3 # to interact with AWS
import os
import requests
import tqdm
import mysql.connector
from AWS_access_management import host, dbname, user, pwd, port

def get_rds_connection():
     return mysql.connector.connect(host=host, database=dbname, user=user, password=pwd)

def rds_execute_query(rds_con, query):
    rds_con, cursor = None, None
    try:
        rds_con = get_rds_connection()
        cursor = rds_con.cursor()

        cursor.execute(query)
        rds_con.commit()

        cursor.close()
        rds_con.close()
    
    except Exception as e:
        print(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if rds_con is not None:
            rds_con.close()

        return None
    
def rds_insert_records(rds_con, query, records):
    rds_con, cursor = None, None
    records = list(records)
    try:
        rds_con = get_rds_connection()
        cursor = rds_con.cursor()
        rt = [10, 20, 30]
        if not isinstance(records[0], tuple):
            for i in range(len(records)):
                records[i] = (records[i], )
                
        cursor.executemany(query, records)
        
        rds_con.commit()
        
        cursor.close()
        rds_con.close()
    
    except Exception as e:
        print(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if rds_con is not None:
            rds_con.close()
        
        return None

    
def rds_get_records(rds_con, query):
    rds_con, cursor = None, None
    result = None
    try:
        rds_con = get_rds_connection()
        cursor = rds_con.cursor()
        
        cursor.execute(query)
        result = cursor.fetchall()

        cursor.close()
        rds_con.close()
    
    except Exception as e:
        print(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if rds_con is not None:
            rds_con.close()
        return result
    
def rds_store_unique_job_ids(rds_con, new_job_ids):
    query = "INSERT INTO jobs_info(id) VALUES (%s)"
    rds_insert_records(rds_con, query, new_job_ids)
    rds_con.commit()
    rds_con.close()

def rds_get_job_ids(rds_con):
    query = 'SELECT id FROM jobs_info'
    result = rds_get_records(rds_con, query)
    unique_job_ids = [row[0] for row in result]
    return unique_job_ids