# General libraries imports
import datetime
import pandas as pd
import numpy as np

# External API imports
import requests

# Google Cloud Connectors
from google.cloud import firestore
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.firestore_v1.base_query import FieldFilter

# Cloud Functions framework
import functions_framework


# Message types
INFO_MSG = 1
WARNING_MSG = 2
ERROR_MSG = 3

# Helper function for saving message into operational log
def log_msg(ts, level, msg):
    # Initialize access to BigQuery and Firestore
    client = bigquery.Client()
    db = firestore.Client()
    # Get working queries from admin document
    doc_ref = db.collection("ebird_dwh_admin").document("bq_queries")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        log_insert = doc['log_insert']
    else:
        raise Exception('Fatal error. No config document!')

    query_job = client.query(log_insert.format(ts, level, msg))  # BigQuery API request
    query_job.result()  # Waits for query to finish
    client.close()


# Load current high_water_mark
def load_high_water_mark(tbl = 'dwh_fact_observation'):
    # Initialize access to BigQuery and Firestore
    client = bigquery.Client()
    db = firestore.Client()

    # Get working queries from admin document
    doc_ref = db.collection("ebird_dwh_admin").document("bq_queries")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        get_hwm = doc['get_hwm']
    else:
        raise Exception('Fatal error. No config document!')

    query_job = client.query(get_hwm.format(tbl))  # BigQuery API request
    rows = query_job.result()  # Waits for query to finish
    if rows.total_rows == 0:
        high_water_mark = datetime.datetime.strptime('2023-07-27 00:00:00', '%Y-%m-%d %H:%M:%S')
    else:
        high_water_mark = next(rows)[0]
        if high_water_mark is None:
            high_water_mark = datetime.datetime.strptime('2023-07-27 00:00:00', '%Y-%m-%d %H:%M:%S')
    client.close()
    return high_water_mark


@functions_framework.http
def src_facts_to_fs(request):
    log_msg(datetime.datetime.now(), INFO_MSG, "ebird-analytics-dwh job has been started.")

    # Connect to GCP instances
    db = firestore.Client()
    client = bigquery.Client()

    # Set ETL variables
    doc_ref = db.collection("ebird_dwh_admin").document("config")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        regionCode = doc['regionCode']  # Geographic location for analysis
        api_key = doc['api_key']        # API Key
    else:
        raise Exception('Fatal error. No config document!')

    # Load endpoints for API requests
    doc_ref = db.collection("ebird_dwh_admin").document("api_endpoints")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        url_cl_ist = doc['url_cl_ist']
        url_get_cl = doc['url_get_cl']
    else:
        raise Exception('Fatal error. No config document!')

    # Load current high_water_mark
    high_water_mark = load_high_water_mark()

    subId_list = set()
    
    for dt in pd.date_range(high_water_mark.date(), datetime.datetime.now().date(), freq='d'):
        # Make request for checklists for given day
        r = requests.get(url_cl_ist.format(regionCode, dt.year, dt.month, dt.day), headers = {'X-eBirdApiToken' : api_key})
        # Check status
        if r.status_code != 200:
            raise ValueError('Bad response from e-bird')
        
        # Load documents with query results into checklists and locations collections
        batch = db.batch()
        batch_size = 0
        for entry in r.json():
            if entry['obsTime'] == 'nan':
                entry['obsTime'] = '00:00'
            entry['obsFullDt'] = datetime.datetime.strptime(entry['obsDt'] + ' ' + entry['obsTime'], '%d %b %Y %H:%M')
            loc_entry = entry['loc']
            del entry['obsDt']
            del entry['obsTime']
            del entry['subID']
            del entry['loc']
            del loc_entry['name']
            del loc_entry['latitude']
            del loc_entry['longitude']
            del loc_entry['locID']
            loc_entry['updateDt'] = entry['obsFullDt']
            if entry['obsFullDt'] > high_water_mark:
                subId_list.add(entry['subId'])
            ref = db.collection('mrr_checklist').document(entry['subId'])
            batch.set(ref, entry)
            ref = db.collection('mrr_location').document(loc_entry['locId'])
            batch.set(ref, loc_entry)
            batch_size += 2
            if batch_size > 480:
                batch.commit()
                batch_size = 0
                batch = db.batch()
        batch.commit()
        new_observations_cnt = 0
        for cl in subId_list:
            # Make request
            r = requests.get(url_get_cl.format(cl), headers = {'X-eBirdApiToken' : api_key})
            # Check status
            if r.status_code != 200:
                raise ValueError('Bad response from e-bird')
            # Load documents with query results into observations collections
            new_observations_cnt += len(r.json()['obs'])
            batch = db.batch()
            batch_size = 0
            for entry in r.json()['obs']:
                entry['obsDt'] = datetime.datetime.strptime(entry['obsDt'], '%Y-%m-%d %H:%M')
                del entry['hideFlags']
                del entry['subnational1Code']
                del entry['howManyAtleast']
                del entry['howManyAtmost']
                if entry['howManyStr'] == 'X':
                    entry['howManyStr'] = '0'
                entry['howMany'] = int(entry['howManyStr'])
                del entry['howManyStr']
                ref = db.collection('mrr_observation').document(entry['obsId'])
                batch.set(ref, entry)
                batch_size += 1
                if batch_size > 480:
                    batch.commit()
                    batch_size = 0
                    batch = db.batch()
            batch.commit()

    log_msg(datetime.datetime.now(), INFO_MSG, f"{len(subId_list)} new checklists ingested.")
    log_msg(datetime.datetime.now(), INFO_MSG, f"{new_observations_cnt} new observations ingested.")

    log_msg(datetime.datetime.now(), INFO_MSG, "MRR copy of e-bird db successfully ingested.")
    client.close()
    
    return 'src_facts_to_fs task completed'
