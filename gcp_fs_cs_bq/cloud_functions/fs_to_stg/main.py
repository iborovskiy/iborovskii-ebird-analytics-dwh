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
def fs_to_stg(request):
    log_msg(datetime.datetime.now(), INFO_MSG, "Moving source data from Firestore into Cloud Storage (Staging area) has been started.")

    # Connect to GCP instances
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('fiery-rarity-396614-ebird')
    db = firestore.Client()

    # Set ETL variables
    doc_ref = db.collection("ebird_dwh_admin").document("config")
    doc = doc_ref.get()
    if doc.exists:
        doc = doc.to_dict()
        locale = doc['locale']
    else:
        raise Exception('Fatal error. No config document!')

    # Load current high_water_mark
    high_water_mark = load_high_water_mark()
    weather_high_water_mark = load_high_water_mark('mrr_fact_weather_observations')

    # Export trusted new data rows for observations into Staging area 
    # in Google Cloud Storage (incremental using high water mark)
    # - Clear STG observations from bad data
    # - Store clean date to Staging area (redy to model DWH)
    docs = db.collection('mrr_location').where(filter=FieldFilter("updateDt", ">", high_water_mark)).stream()
    df = pd.DataFrame([doc.to_dict() for doc in docs])
    if len(df) > 0:
        df = df[['locId', 'locName', 'lat', 'lng', 'countryName', 'subnational1Name', 'updateDt']]
        df['numspeciesalltime'] = None
        df['updateDt'] = df['updateDt'].dt.tz_localize(None)
        df.columns = ['locid', 'locname', 'lat', 'lon', 'countryname', 'subregionname', 'latestobsdt', 'numspeciesalltime']
        blob = bucket.blob('stg_location.csv')
        blob.upload_from_string(df.to_csv(index = False), 'text/csv')
    
    docs = db.collection('mrr_checklist').where(filter=FieldFilter("obsFullDt", ">", high_water_mark)).stream()
    df = pd.DataFrame([doc.to_dict() for doc in docs])
    if len(df) > 0:
        df = df[['locId', 'subId', 'userDisplayName', 'numSpecies', 'obsFullDt']]
        df['obsFullDt'] = df['obsFullDt'].dt.tz_localize(None)
        df.columns = ['locid', 'subid', 'userdisplayname', 'numspecies', 'obsfulldt']
        blob = bucket.blob('stg_checklist.csv')
        blob.upload_from_string(df.to_csv(index = False), 'text/csv')

    docs = db.collection('mrr_observation').where(filter=FieldFilter("obsDt", ">", high_water_mark)).stream()
    df = pd.DataFrame([doc.to_dict() for doc in docs])
    if len(df) > 0:
        df = df[['speciesCode', 'obsDt', 'subId', 'obsId', 'howMany']]
        df['obsDt'] = df['obsDt'].dt.tz_localize(None)
        df.columns = ['speciesCode', 'obsDt', 'subId', 'obsId', 'howMany']
        blob = bucket.blob('stg_observation.csv')
        blob.upload_from_string(df.to_csv(index = False), 'text/csv')
    
    docs = db.collection_group('dates').where(filter=FieldFilter("update_ts", ">", weather_high_water_mark)).stream()
    df = pd.DataFrame([doc.to_dict() for doc in docs])
    if len(df) > 0:
        df = df[['loc_id', 'date', 'tavg', 'tmin', 'tmax', 'prcp', 'snow', 'wdir', 'wspd', 'wpgt', 'pres', 'tsun', 'update_ts']]
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df['update_ts'] = df['update_ts'].dt.tz_localize(None)
        df.columns = ['loc_id', 'obsdt', 'tavg', 'tmin',
                        'tmax', 'prcp', 'snow', 'wdir', 'wspd', 'wpgt', 'pres', 'tsun', 'update_ts']
        blob = bucket.blob('stg_weather.csv')
        blob.upload_from_string(df.to_csv(index = False), 'text/csv')

    docs = db.collection('mrr_hotspots').stream()
    df_hs = pd.DataFrame([doc.to_dict() for doc in docs])
    docs = db.collection('mrr_countries').stream()
    df_countries = pd.DataFrame([doc.to_dict() for doc in docs])
    docs = db.collection('mrr_subregions').stream()
    df_subregions = pd.DataFrame([doc.to_dict() for doc in docs])
    if len(df_hs) > 0 and len(df_countries) > 0 and len(df_subregions) > 0:
        df_countries.columns = ['countryname', 'countryCode']
        df_subregions.columns = ['subregionname', 'subnational1Code']
        df_hs_full = pd.merge(df_hs, df_countries, on='countryCode', how='left')
        df_hs_full = pd.merge(df_hs_full, df_subregions, on='subnational1Code', how='left')
        df_hs_full = df_hs_full[['locId', 'locName', 'countryname', 'subregionname', 'lat', 'lng',
                                'numSpeciesAllTime', 'latestObsDt']]
        df_hs_full['latestObsDt'] = pd.to_datetime(df_hs_full['latestObsDt']).dt.tz_localize(None)
        df_hs_full.columns = ['locid', 'locname', 'countryname', 'subregionname', 
                            'lat', 'lon', 'numspeciesalltime', 'latestobsdt']
        blob = bucket.blob('stg_hotspots.csv')
        blob.upload_from_string(df_hs_full.to_csv(index = False), 'text/csv')

    docs = db.collection('mrr_taxonomy').stream()
    df_taxonomy = pd.DataFrame([doc.to_dict() for doc in docs])
    docs = db.collection_group('orders').where(filter=FieldFilter("locale", "==", locale)).stream()
    df_orders_cn = pd.DataFrame([doc.to_dict() for doc in docs])
    docs = db.collection_group('families').where(filter=FieldFilter("locale", "==", locale)).stream()
    df_families_cn = pd.DataFrame([doc.to_dict() for doc in docs])
    if len(df_taxonomy) > 0 and len(df_orders_cn) > 0 and len(df_families_cn) > 0:
        df_taxonomy_full = pd.merge(df_taxonomy, df_orders_cn, left_on='order', right_on='ordersciname', how='left')
        df_taxonomy_full = pd.merge(df_taxonomy_full, df_families_cn, left_on='familyCode', right_on='familycode', how='left')
        df_taxonomy_full = df_taxonomy_full[['speciesCode', 'sciName', 'comName', 'category', 'order', 
                                            'ordercomname', 'familycode', 'familycomname', 'familysciname']]
        df_taxonomy_full.columns = ['speciesCode', 'sciName', 'comName', 'category', 'orderSciName', 
                                'orderComName', 'familyCode', 'familyComName', 'familySciName']
        blob = bucket.blob('stg_taxonomy.csv')
        blob.upload_from_string(df_taxonomy_full.to_csv(index = False), 'text/csv')

    log_msg(datetime.datetime.now(), INFO_MSG, "Data successfully loaded into Staging area.")

    return 'src_facts_to_fs task completed'
