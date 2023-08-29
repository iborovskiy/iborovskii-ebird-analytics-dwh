# General libraries imports
import datetime

# DAG imports
# DAG main imports
from airflow import DAG
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook
# DAG utils imports
from airflow.models import Variable

# Google Cloud Connectors
from google.cloud import bigquery
from google.oauth2 import service_account

# Local modules imports
import ebird_analytics_dwh.configs as sq


# Load current high_water_mark
def load_high_water_mark(tbl = 'dwh_fact_observation'):
    # Get DAG variables values 
    key_path = Variable.get("EBIRD_BIGQUERY_KEY_PATH", default_var='/tmp/') # Path to the service account key file
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    query_job = client.query(sq.get_hwm.format(tbl))  # BigQuery API request
    rows = query_job.result()  # Waits for query to finish
    if rows.total_rows == 0:
        high_water_mark = '2023-07-27 00:00:00'
    else:
        high_water_mark = next(rows)[0]
        if high_water_mark is None:
            high_water_mark = '2023-07-27 00:00:00'
    client.close()
    return high_water_mark


# Message types
INFO_MSG = 1
WARNING_MSG = 2
ERROR_MSG = 3

# Helper function for saving message into operational log
def log_msg(ts, level, msg):
    # Get DAG variables values 
    key_path = Variable.get("EBIRD_BIGQUERY_KEY_PATH", default_var='/tmp/') # Path to the service account key file
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    query_job = client.query(sq.log_insert.format(ts, level, msg))  # BigQuery API request
    query_job.result()  # Waits for query to finish
    client.close()


# Logging events
def on_DAG_start_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"Daily DAG has been started, DAG run at {context['ts']}.")

def on_DAG_mrr_loaded_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"MRR copy of e-bird db successfully ingested, DAG run at {context['ts']}.")

def on_DAG_stg_loaded_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"STG db successfully updated wuth new data from MRR, DAG run at {context['ts']}.")

def on_DAG_dwh_loaded_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"OLAP model in DWH db successfully updated wuth new data from STG, DAG run at {context['ts']}.")

def on_DAG_full_backup_created_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"Full backup of databased successfully created, DAG run at {context['ts']}.")

def on_DAG_success_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"Daily DAG has been completed, DAG run at {context['ts']}.")

def on_DAG_error_alert(context):
    log_msg(datetime.datetime.now(), ERROR_MSG, f"Daily DAG has been failed, task_instance_key_str={context['task_instance_key_str']}, DAG run at {context['ts']}.")

def on_DAG_retry_alert(context):
    log_msg(datetime.datetime.now(), ERROR_MSG,
            f"Daily DAG encountered a problem and task is up to retry, task_instance_key_str={context['task_instance_key_str']}, DAG run at {context['ts']}.")

