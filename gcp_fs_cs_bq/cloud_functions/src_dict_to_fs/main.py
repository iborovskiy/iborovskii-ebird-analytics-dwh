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

@functions_framework.http
def src_dict_to_fs(request):
    log_msg(datetime.datetime.now(), INFO_MSG, "Updating of dictionaries has been started.")

    # Connect to GCP instances
    db = firestore.Client()

    # Set ETL variables
    doc_ref = db.collection("ebird_dwh_admin").document("config")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        locale = doc['locale']          # Language for common name
        days_back = doc['days_back']    # How many days back to fetch
        regionCode = doc['regionCode']  # Geographic location for analysis
        api_key = doc['api_key']        # API Key
    else:
        raise Exception('Fatal error. No config document!')

    # Load endpoints for API requests
    doc_ref = db.collection("ebird_dwh_admin").document("api_endpoints")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        url_locs = doc['url_locs']
        url_countries = doc['url_countries']
        url_sub_regions = doc['url_sub_regions']
        url_taxonomy = doc['url_taxonomy']
    else:
        raise Exception('Fatal error. No config document!')

    # Make request
    r_loc = requests.get(url_locs.format(regionCode, days_back), headers = {'X-eBirdApiToken' : api_key})
    r_countries = requests.get(url_countries, headers = {'X-eBirdApiToken' : api_key})
    r_subregions = requests.get(url_sub_regions.format(regionCode), headers = {'X-eBirdApiToken' : api_key})
    r_taxonomy = requests.get(url_taxonomy.format(locale), headers = {'X-eBirdApiToken' : api_key})

    if r_taxonomy.status_code != 200 or r_loc.status_code != 200 or r_countries.status_code != 200 or r_subregions.status_code != 200:
        raise ValueError('Bad response from e-bird')

    # Load documents with queries results into dictionaries collections
    batch = db.batch()
    batch_size = 0
    for entry in r_loc.json():
        entry['latestObsDt'] = datetime.datetime.strptime(entry['latestObsDt'], '%Y-%m-%d %H:%M')
        ref = db.collection('mrr_hotspots').document(entry['locId'])
        batch.set(ref, entry)
        batch_size += 1
        if batch_size == 480:
            batch.commit()
            batch_size = 0
            batch = db.batch()
    batch.commit()

    batch_size = 0
    batch = db.batch()
    for entry in r_countries.json():
        ref = db.collection('mrr_countries').document(entry['code'])
        batch.set(ref, entry)
        batch_size += 1
        if batch_size == 480:
            batch.commit()
            batch_size = 0
            batch = db.batch()
    batch.commit()

    batch_size = 0
    batch = db.batch()
    for entry in r_subregions.json():
        ref = db.collection('mrr_subregions').document(entry['code'])
        batch.set(ref, entry)
        batch_size += 1
        if batch_size == 480:
            batch.commit()
            batch_size = 0
            batch = db.batch()
    batch.commit()

    batch_size = 0
    batch = db.batch()
    for entry in r_taxonomy.json():
        ref = db.collection('mrr_taxonomy').document(entry['speciesCode'])
        batch.set(ref, entry)
        batch_size += 1
        if batch_size == 480:
            batch.commit()
            batch_size = 0
            batch = db.batch()
    batch.commit()

    log_msg(datetime.datetime.now(), INFO_MSG, "Updating of dictionaries successfully completed.")
    
    return 'src_dict_to_fs task completed'
