# The DAG main imports
from airflow import DAG
# Access connectors
from airflow.hooks.postgres_hook import PostgresHook

# Utilities
import datetime

INFO_MSG = 1
WARNING_MSG = 2
ERROR_MSG = 3

def log_msg(ts, level, msg):
    pgs_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    log_insert = 'insert into etl_log (ts, event_type, event_description) values (%s, %s, %s)'
    pgs_hook.run(log_insert, parameters = (ts, level, msg))
    pgs_hook.conn.close()




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




