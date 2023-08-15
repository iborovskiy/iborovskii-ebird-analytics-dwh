# General libraries imports
import datetime

# DAG imports
# DAG main imports
from airflow import DAG
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook


# Load current high_water_mark
def load_high_water_mark(tbl = 'dwh_fact_observation'):
    pgs_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    high_water_mark = (pgs_hook.get_records(f"SELECT get_current_HWM('{tbl}')"))[0][0]
    pgs_hook.conn.close()
    return high_water_mark


# Message types
INFO_MSG = 1
WARNING_MSG = 2
ERROR_MSG = 3

# Helper function for saving message into operational log
def log_msg(ts, level, msg):
    pgs_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    log_insert = 'INSERT INTO etl_log (ts, event_type, event_description) VALUES (%s, %s, %s)'
    pgs_hook.run(log_insert, parameters = (ts, level, msg))
    pgs_hook.conn.close()

# Logging events
def on_DAG_start_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"Daily DAG has been started, DAG run at {context['ts']}.")

def on_DAG_mrr_loaded_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"MRR copy of e-bird db successfully ingested, DAG run at {context['ts']}.")

def on_DAG_stg_loaded_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"STG db successfully updated wuth new data from MRR, DAG run at {context['ts']}.")

def on_DAG_dwh_loaded_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"OLAP model in DWH db successfully updated wuth new data from STG, DAG run at {context['ts']}.")

def on_DAG_success_alert(context):
    log_msg(datetime.datetime.now(), INFO_MSG, f"Daily DAG has been completed, DAG run at {context['ts']}.")

def on_DAG_error_alert(context):
    log_msg(datetime.datetime.now(), ERROR_MSG, f"Daily DAG has been failed, task_instance_key_str={context['task_instance_key_str']}, DAG run at {context['ts']}.")

def on_DAG_retry_alert(context):
    log_msg(datetime.datetime.now(), ERROR_MSG, f"Daily DAG encountered a problem and task is up to retry, \
            task_instance_key_str={context['task_instance_key_str']}, DAG run at {context['ts']}.")

