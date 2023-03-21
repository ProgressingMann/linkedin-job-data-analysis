import requests
import json
import regex as re
import mysql.connector
import time
import random
import datetime as dt
import boto3 # to interact with AWS
import os
import tqdm
import numpy as np

from AWS_access_management import dynamo_client
from db import *
from os import listdir
from os.path import isfile, join
from numpy.random import uniform
from bs4 import BeautifulSoup

def get_salary_range(salary_soup):
    lower, upper = re.sub('\s', '', salary_soup.text).split('-')
    pt = r'(?:\$)([0-9]+)(?:\.*)'
    lower, upper = lower.replace(',', ''), upper.replace(',', '')
    lower, upper = re.search(pt, lower).groups()[0], re.search(pt, upper).groups()[0]
    if len(upper) <= 3:
        lower = int(lower)*40*52
        upper = int(upper)*40*52
    return int(lower), int(upper)


def helper_get_job_ids(jobs_list):
    class_div_job = 'base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card'
    jobs_search_card_data = {}

    try:
        jobs = jobs_list.findAll('div', {'class': class_div_job})
        for job in jobs:
            date_posted, salary_lower, salary_upper = None, None, None
            
            job_here = job.attrs
            job_id = job_here['data-entity-urn'][-10:]
            
            date_posted = job.find('time')
            if date_posted: 
                date_posted = date_posted.attrs['datetime']
            
            salary = job.find('span', {'class': 'job-search-card__salary-info'})
            if salary:
                salary_lower, salary_upper = get_salary_range(salary)
                
            jobs_search_card_data[job_id] = {'date_posted': date_posted, 'salary_lower': salary_lower, 
                                            'salary_upper': salary_upper}
    except:
        pass
    finally:
        return jobs_search_card_data


def map_title(title):
    f_T = None
    if title == 'data_scientist':
        f_T = '25190'
    elif title == 'data_analyst':
        f_T = ', 340'
    elif title == 'data_engineer':
        f_T = '2732'
        
    return f_T


def map_time(posted_ago):
    f_TPR = None
    if posted_ago == 'week':
        f_TPR = 'r604800'
    elif posted_ago == 'day':
        f_TPR = 'r86400'
    elif posted_ago == 'month':
        f_TPR = 'r2592000'
    else: # set it to week
        f_TPR = 'r604800'
    
    return f_TPR


def get_new_job_ids(total_jobs_loaded, refresh, posted_ago, title):
    
    f_TPR = map_time(posted_ago) # Used to filter the jobs by their posted dates
    f_T = map_title(title)
    title_url = title.replace('_', '%20')
    
    if refresh: # first request
        url = f'https://www.linkedin.com/jobs/search/?f_TPR={f_TPR}&keywords={title_url}&'\
                f'position=1&pageNum=0&refresh=true&f_JT=F&f_T={f_T}&location=United%20States'
    else: # scrolling for more jobs
        url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?f_TPR={f_TPR}&'\
                f'keywords={title_url}&position=1&pageNum=0&start={total_jobs_loaded}'\
                f'&f_JT=F&f_T={f_T}&location=United%20States'
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content)
    jobs_list = []
    
    if refresh:
        jobs_list = soup.find('ul', {'class': 'jobs-search__results-list'})
    else:
        jobs_list = soup
        
    jobs_search_card_data = helper_get_job_ids(jobs_list)
    
    return jobs_search_card_data


def get_new_jobs(new_jobs_needed, refresh=True, posted_ago='week', title='data_scientist'):
    new_job_ids = dict()
    total_jobs_loaded, new_jobs_loaded = 0, 0
    existing_job_ids = set(rds_get_job_ids()) # using set reduces the time complexity to find existing job ids

    while new_jobs_loaded < new_jobs_needed:
        jobs_search_card_data = get_new_job_ids(total_jobs_loaded, refresh, posted_ago, title)
        total_jobs_loaded += 25

        for job_id in jobs_search_card_data:
            if (job_id not in new_job_ids) and (job_id not in existing_job_ids):
                new_job_ids[job_id] = jobs_search_card_data[job_id]
            
        new_jobs_loaded = len(new_job_ids)
        refresh = False
    
        print(f'Total jobs loaded {total_jobs_loaded}. New jobs loaded {new_jobs_loaded}')
        
        sleep_time = np.random.uniform(2, 4) # to avoid getting detected as a scrapper
        print(f'sleeping for {sleep_time}')
        time.sleep(sleep_time)
    
    return {'new_jobs_loaded': new_jobs_loaded, 'new_job_ids': new_job_ids}


def get_job_criteria_subheader(job_criteria_soup):
    d = {'job_function': 'na', 'industries': 'na', 'seniority_level': 'na', 'employment_type': 'na'}
    for elem in job_criteria_soup:
        key = elem.find('h3', {'class': 'description__job-criteria-subheader'}).text.strip().replace(' ', '_').lower()
        value = elem.find('span', {'class': 'description__job-criteria-text'}).text.strip()
        d[key] = value
    return d


def get_update_scraped_jobs_data(json):
        return (json['job_title'], json['company_name'], json['location'], json['num_applicants'],
            json['job_function'], json['industries'], json['seniority_level'], 
            json['employment_type'], json['job_id'])


def update_records(records):
        update_query = "UPDATE jobs_info SET scraped_jd=true, title=%s, company_name=%s, location=%s, "\
        "num_applicants=%s, job_function=%s, industries=%s, seniority_level=%s, employment_type=%s"\
        "WHERE id=%s"
        rds_insert_update_records(update_query, records)
        
        return None


def store_job_description_data(job_id):
    print('Scrapping JobID - ', job_id)
    view_job_url = 'https://www.linkedin.com/jobs/view/'
    job_des_resp = requests.get(view_job_url + job_id)
    job_des_soup = BeautifulSoup(job_des_resp.content)

    elem_job_title = 'top-card-layout__title' # Job title
    job_title = job_des_soup.find('h1', {'class': elem_job_title})
    job_title = job_title.text.strip() if job_title else 'na'
    
    elem_company_name = 'topcard__org-name-link topcard__flavor--black-link' # name of the company
    company_name = job_des_soup.find('a', {'class': elem_company_name})
    company_name = company_name.text.strip() if company_name else 'na'
    
    elem_location = 'topcard__flavor topcard__flavor--bullet' # job location
    location = job_des_soup.find('span', {'class': elem_location})
    location = location.text.strip() if location else 'na'

    elem_posted_ago = 'posted-time-ago__text' # when was the job posted
    posted_ago = job_des_soup.find('span', {'class': elem_posted_ago})
    posted_ago = posted_ago.text.strip() if posted_ago else 'na'
    
    elem_num_applicants = 'num-applicants__figure' # total number of job applicants, when > 200
    num_applicants = job_des_soup.find('figure', {'class': elem_num_applicants})
    
    if num_applicants:
        num_applicants = num_applicants.text.strip()
    else:
        elem_num_applicants = 'num-applicants__caption' # total number of job applicants, when < 200
        num_applicants = job_des_soup.find('span', {'class': elem_num_applicants})
        num_applicants = num_applicants.text.strip() if num_applicants else 'na'
    
    elem_job_description = 'show-more-less-html__markup' # job description
    job_description = job_des_soup.find('div', {'class': elem_job_description})
    job_description = job_description.text.strip() if job_description else 'na'
    
    job_criteria_soup = job_des_soup.findAll('li', {'class': 'description__job-criteria-item'})
    job_criteria_d = get_job_criteria_subheader(job_criteria_soup)

    json = {'job_id': job_id, 'job_title': job_title, 'company_name': company_name, 'location': location, 
            'num_applicants': num_applicants, 'job_function': job_criteria_d['job_function'], 
            'industries': job_criteria_d['industries'], 'seniority_level': job_criteria_d['seniority_level'], 
            'employment_type': job_criteria_d['employment_type']}

    values = get_update_scraped_jobs_data(json)
    update_records([values])
    store_job_description_dynamo_db({'job_id': job_id, 'job_title': job_title,
                                    'job_description': job_description})

    return 

# Data Scientist
# https://www.linkedin.com/jobs/search/?f_JT=F&f_T=25190&keywords=data%20scientist&location=United%20States

# Data Analyst
# https://www.linkedin.com/jobs/search/?f_JT=F&f_T=340&keywords=data%20analyst&location=United%20States

# Data Engineer
# https://www.linkedin.com/jobs/search/?f_JT=F&f_T=2732&keywords=data%20engineer&location=United%20States

# export DOCKER_DEFAULT_PLATFORM=linux/amd64