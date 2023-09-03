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
def stg_to_dwh(request):
    log_msg(datetime.datetime.now(), INFO_MSG, "Updating data model from Staging area has been started.")

    # Connect to GCP instances
    client = bigquery.Client()
    db = firestore.Client()

    # Set ETL variables
    doc_ref = db.collection("ebird_dwh_admin").document("config")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        DWH_INTERNAL_MODEL = doc['DWH_INTERNAL_MODEL']
    else:
        raise Exception('Fatal error. No config document!')

    # Load queries for DWH
    doc_ref = db.collection("ebird_dwh_admin").document("bq_queries")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        dwh_load_taxonomy_dict_bq_sql = doc['dwh_load_taxonomy_dict_bq_sql']
        dwh_load_hotspots_dict_bq_sql = doc['dwh_load_hotspots_dict_bq_sql']
        dwh_load_location_dict_bq_sql = doc['dwh_load_location_dict_bq_sql']
        dwh_load_checklist_dict_bq_sql = doc['dwh_load_checklist_dict_bq_sql']
        dwh_load_observation_bq_sql = doc['dwh_load_observation_bq_sql']
        dwh_load_weather_bq_sql = doc['dwh_load_weather_bq_sql']
        dwh_update_bq_sql = doc['dwh_update_bq_sql']
        dwh_update_model_proc = doc['dwh_update_model_proc']
    else:
        raise Exception('Fatal error. No config document!')

    # Load current high_water_mark
    high_water_mark = load_high_water_mark()

    # Create actual data model - fact and dimension table for Star Schema
    
    # Import csv files with new data sets from staging area:
    # - Import new observations into temporary fact table
    # - Import new taxonomy dictionary into DWH
    # - Import new public locations dictionary into DWH
    
    # Create new data model for DWH:
    # - Create fact table - dwh_fact_observation (incremental update)
    # - Create dimension table - dwh_dim_dt (incremental update)
    # - Create dimension table - dwh_dim_location (incremental update)
    # - Create dimension table - dwh_dim_species (full update  - temporary solution for easy filling common names)
    # - Truncate temporary observation table after successful model's update
    # - Update current high_water_mark on success of current DAG

    exp_time = (datetime.datetime.now() + datetime.timedelta(minutes = 2)).astimezone() # Temp tables expiration (10 min)

    if DWH_INTERNAL_MODEL == False:
        
        # Import new batch of data into temporary dataset
        query_job = client.query(dwh_load_taxonomy_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(dwh_load_hotspots_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(dwh_load_location_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(dwh_load_checklist_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(dwh_load_observation_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(dwh_load_weather_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(dwh_update_bq_sql) # BigQuery API request
        query_job.result()  # Waits for query to finish
        
    else:
        query_job = client.query(dwh_update_model_proc.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish
    
    log_msg(datetime.datetime.now(), INFO_MSG, "Data model in DWH was successfully updated.")
    client.close()

    return 'stg_to_dwh task completed'
