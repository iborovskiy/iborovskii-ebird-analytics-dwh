# The DAG main imports
from airflow import DAG
# Access connectors
from airflow.hooks.postgres_hook import PostgresHook

# Utilities
import datetime


# Load current high_water_mark
def load_high_water_mark():
    pgs_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    tmp_water_mark = pgs_hook.get_records("SELECT get_high_water_mark('dwh_fact_observation')")
    if len(tmp_water_mark) < 1 or not tmp_water_mark[0][0]:
        high_water_mark = '2020-01-21 16:35'
        pgs_hook.run("INSERT INTO high_water_mark VALUES(%s, %s) ON CONFLICT (table_id) DO UPDATE \
                            SET current_high_ts = EXCLUDED.current_high_ts", parameters = ('dwh_fact_observation', high_water_mark))

    else:
        high_water_mark = tmp_water_mark[0][0]
    
    return high_water_mark


# Update current high_water_mark
def update_high_water_mark():
    pgs_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    tmp_water_mark = pgs_hook.get_records("SELECT MAX(obsdt) FROM dwh_fact_observation")
    pgs_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'dwh_fact_observation'",
                     parameters = (tmp_water_mark[0][0],))
    pgs_hook.conn.close()


# Message types
INFO_MSG = 1
WARNING_MSG = 2
ERROR_MSG = 3

# Helper function for saving message into operational log
def log_msg(ts, level, msg):
    pgs_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    log_insert = 'insert into etl_log (ts, event_type, event_description) values (%s, %s, %s)'
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




