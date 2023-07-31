# Import support libraries
from datetime import timedelta

# The DAG main imports
from airflow import DAG
# Access connectors
from airflow.hooks.postgres_hook import PostgresHook
# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# DAG support imports
from airflow.utils.dates import days_ago

# Import local modules
import assignment_dwh.etl_logging as etl_log
import assignment_dwh.ebird_source as ebird
import assignment_dwh.stg_load as stg
import assignment_dwh.dwh_load as dwh

# define DAG
default_args = {
    'owner': 'Ivan Borovskii',
    'start_date': days_ago(0),
    'email': ['iborovskiy.ge@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='assignment-etl',
    default_args=default_args,
    description='Assignment demo-ETL',
    schedule_interval=timedelta(hours=1),
    #schedule_interval=timedelta(minutes=10),
    catchup=False,
    on_failure_callback = etl_log.on_DAG_error_alert,
)

# define tasks
etl_start_task = DummyOperator(
    task_id='etl_start_task',
    on_execute_callback = etl_log.on_DAG_start_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
    dag=dag)


def dummy_task(*args, **kwargs):
    print('And here we are too! Python is here:')
    return 'Ret from Python'

load_mrr_from_ebird_task = PythonOperator(
    task_id = 'load_mrr_from_ebird_task',
    provide_context=True,
    python_callable = ebird.load_mrr_from_ebird,
    dag = dag,
    on_success_callback = etl_log.on_DAG_mrr_loaded_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

load_stg_from_mrr_task = PythonOperator(
    task_id = 'load_stg_from_mrr_task',
    provide_context=True,
    python_callable = stg.load_stg_from_mrr,
    dag = dag,
    on_success_callback = etl_log.on_DAG_stg_loaded_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

load_dwh_from_stg_task = PythonOperator(
    task_id = 'load_dwh_from_stg_task',
    provide_context=True,
    python_callable = dwh.load_dwh_from_stg,
    dag = dag,
    on_success_callback = etl_log.on_DAG_dwh_loaded_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

etl_end_task = DummyOperator(
    task_id='etl_end_task',
    on_success_callback = etl_log.on_DAG_success_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
    dag=dag)

# task pipeline
etl_start_task >> load_mrr_from_ebird_task >> load_stg_from_mrr_task >> load_dwh_from_stg_task >> etl_end_task