# General libraries imports
import pandas as pd
import numpy as np
import os
import datetime

# DAG imports
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook
# DAG utils imports
from airflow.models import Variable

# Google Cloud Connectors
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# Local modules imports
import ebird_analytics_dwh.etl_logging as etl_log
import ebird_analytics_dwh.configs as sq


def load_dwh_from_stg(*args, **kwargs):
    # Get DAG variables values
    DWH_INTERNAL_MODEL = False if Variable.get("EBIRD_DWH_INTERNAL_MODEL", 
                        default_var='false').lower() == 'false' else True       # Model creation mode
    home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')              # Temporary working directory
    key_path = Variable.get("EBIRD_BIGQUERY_KEY_PATH", default_var='/tmp/')     # Path to the service account key file
    # Get cursors to DBs
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
    bucket = storage_client.get_bucket('fiery-rarity-396614-ebird')

    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")


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
        # Use several SQL queries in single transaction
        print("Use external load mode of STG")

        # Import new batch of data into temporary dataset
        query_job = client.query(sq.dwh_load_taxonomy_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(sq.dwh_load_hotspots_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(sq.dwh_load_location_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(sq.dwh_load_checklist_dict_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(sq.dwh_load_observation_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(sq.dwh_load_weather_bq_sql.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish

        query_job = client.query(sq.dwh_update_bq_sql) # BigQuery API request
        query_job.result()  # Waits for query to finish
    else:
        # Use stored procedure from DWH db (single transaction)
        print("Use internal load mode of DWH")
        query_job = client.query(sq.dwh_update_model_proc.format(exp_time)) # BigQuery API request
        query_job.result()  # Waits for query to finish


    print('All fact and dimension tables of DWH are updated.')

    # Close the connection to STG and MRR db
    client.close()
    storage_client.close()
