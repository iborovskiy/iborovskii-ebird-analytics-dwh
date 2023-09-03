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
def src_weather_to_fs(request):
    log_msg(datetime.datetime.now(), INFO_MSG, "Update of weather conditions has been started.")
    
    # Connect to GCP instances
    db = firestore.Client()
    client = bigquery.Client()

    # Set ETL variables
    doc_ref = db.collection("ebird_dwh_admin").document("config")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        regionCode = doc['regionCode']                  # Geographic location for analysis
        meteostat_api_key = doc['meteostat_api_key']    # Weather API Key
    else:
        raise Exception('Fatal error. No config document!')
    
    # Load endpoints for API requests
    doc_ref = db.collection("ebird_dwh_admin").document("api_endpoints")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        url_get_weather = doc['url_get_weather']
    else:
        raise Exception('Fatal error. No config document!')

    # Load working queries for DWH
    doc_ref = db.collection("ebird_dwh_admin").document("bq_queries")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        get_weather_sql = doc['get_weather_sql']
    else:
        raise Exception('Fatal error. No config document!')

    # Load weather conditions for all locations of new observations
    query_job = client.query(get_weather_sql) # BigQuery API request
    new_weather_rows = query_job.result()  # Waits for query to finish

    cur_loc_id = None
    loc_no_data = False
    weather_cnt = 0
    # Load documents with query results intoweather collections
    batch = db.batch()
    batch_size = 0

    for row in new_weather_rows:
        if loc_no_data and row[0] == cur_loc_id:
            # No previous date for given loc_id, skip all subsequent requests
            continue
        loc_no_data = False
        # Make a request to the Meteostat JSON API
        r = requests.get(url_get_weather.format(row[1], row[2], row[3], row[3]), headers = {'x-rapidapi-key' : meteostat_api_key})
        if r.status_code != 200 or 'data' not in r.json():
            batch.commit()
            log_msg(datetime.datetime.now(), INFO_MSG, "API limit exceeded. We'll try in month.")
            client.close()
            return 'src_weather_to_fs task completed'
        r = r.json()['data']
        if len(r) == 1 and not np.isnan(r[0]['tavg']):
            entry = r[0]
            entry['loc_id'] = row[0]
            entry['update_ts'] = datetime.datetime.now()
            ref = db.collection('mrr_weather').document(entry['loc_id'])
            ref = ref.collection('dates').document(entry['date'])
            batch.set(ref, entry)
            weather_cnt += 1
            batch_size += 1
            if batch_size > 10:
                batch.commit()
                batch_size = 0
                batch = db.batch()
        else:
            cur_loc_id = row[0]
            loc_no_data = True
    
    batch.commit()

    log_msg(datetime.datetime.now(), INFO_MSG, f"{weather_cnt} rows of weather conditions ingested into MRR document store.")
    log_msg(datetime.datetime.now(), INFO_MSG, "Update of weather conditions completed.")

    client.close()
    return 'src_weather_to_fs task completed'
