# General libraries imports
from datetime import timedelta
import configparser
import os

# DAG imports
# DAG main imports
from airflow import DAG
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook
# DAG operators imports
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# DAG utils imports
from airflow.utils.dates import days_ago
# # DAG utils imports
from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import exc
from airflow.models import Variable

# Local modules imports
import ebird_analytics_dwh.etl_logging as etl_log
import ebird_analytics_dwh.mrr_process as mrr
import ebird_analytics_dwh.stg_process as stg
import ebird_analytics_dwh.dwh_process as dwh
import ebird_analytics_dwh.configs as sq

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
    dag_id='ebird-analytics-dwh',
    default_args=default_args,
    description='Analytics DWH for the eBird observations',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    on_failure_callback = etl_log.on_DAG_error_alert,
)

# define tasks
# Initialize environment
def etl_cfg_task(*args, **kwargs):
    print('Initialize environment')
    print('Working dir: ', dag.folder)
    # Read config file on every DAG start and update connections and environments
    config = configparser.ConfigParser()
    config.read(dag.folder + '/ebird_analytics_dwh.cfg')
    
    # Update db connections
    host = config.get('MRR_DB_INSTANCE', 'DB_HOST')
    login = config.get('MRR_DB_INSTANCE', 'DB_USER')
    password = config.get('MRR_DB_INSTANCE', 'DB_PASSWORD')
    port = config.get('MRR_DB_INSTANCE', 'DB_PORT')
    schema = config.get('MRR_DB_INSTANCE', 'DB_NAME')
    conn = Connection(
            conn_id='postgres_mrr_conn',
            conn_type='postgres',
            host=host,
            login=login,
            password=password,
            port=port,
            schema=schema
    )
    session = settings.Session()
    try:
        old_conn = (session.query(Connection).filter(Connection.conn_id == 'postgres_mrr_conn').one())
        session.delete(old_conn)
        session.commit()
        print(f'Old connection "postgres_mrr_conn" deleted.')
    except exc.NoResultFound:
            print(f'Create connection for the first time.')
    finally:
        session.add(conn)
        session.commit()
        
    # Use common credentials for all backups (To change in future)
    Variable.set(key="EBIRD_DB_COMMON_HOST", value=host)
    Variable.set(key="EBIRD_DB_COMMON_PORT", value=port)
    Variable.set(key="EBIRD_DB_COMMON_USERNAME", value=login)
    Variable.set(key="EBIRD_DB_COMMON_PASSWORD", value=password)

    print(f'Connection "postgres_mrr_conn" created.')
    
    # Update variables
    # Google Cloud connection variables
    try:
        val = config.get('DWH_DB_INSTANCE', 'EBIRD_BIGQUERY_KEY_PATH')
    except:
        val = ''
    Variable.set(key="EBIRD_BIGQUERY_KEY_PATH", value=val)

    # Model parameters
    try:
        val = config.get('MODEL', 'EBIRD_DWH_INTERNAL_MODEL')
    except:
        val = False
    Variable.set(key="EBIRD_DWH_INTERNAL_MODEL", value=val)

    try:
        val = config.get('MODEL', 'EBIRD_USE_SPARK')
    except:
        val = False
    Variable.set(key="EBIRD_USE_SPARK", value=val)

    try:
        val = config.get('MODEL', 'EBIRD_DAYS_BACK')
    except:
        val = 30
    Variable.set(key="EBIRD_DAYS_BACK", value=val)

    try:
        val = config.get('LOCATION', 'EBIRD_REGION_CODE')
    except:
        val = 'GE'
    Variable.set(key="EBIRD_REGION_CODE", value=val)

    try:
        val = config.get('LOCATION', 'EBIRD_LOCALE')
    except:
        val = 'ru'
    Variable.set(key="EBIRD_LOCALE", value=val)

    try:
        val = config.get('API_KEYS', 'EBIRD_API_KEY')
    except:
        val = ''
    Variable.set(key="EBIRD_API_KEY", value=val)

    try:
        val = config.get('API_KEYS', 'EBIRD_WEATHER_API_KEY')
    except:
        val = ''
    Variable.set(key="EBIRD_WEATHER_API_KEY", value=val)

    try:
        val = config.get('LOCAL_PATHS', 'EBIRD_HOME_DIR')
    except:
        val = ''
    Variable.set(key="EBIRD_HOME_DIR", value=val)
   
    try:
        val = config.get('LOCAL_PATHS', 'EBIRD_BACKUP_DIR')
    except:
        val = ''
    Variable.set(key="EBIRD_BACKUP_DIR", value=val)


# Start ETL, load config parameters
etl_start_task = PythonOperator(
    task_id='etl_start_task',
    provide_context=True,
    python_callable = etl_cfg_task,
    dag=dag,
    on_success_callback = etl_log.on_DAG_start_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

# Load MRR from multiple sources
load_mrr_from_ebird_task = PythonOperator(
    task_id = 'load_mrr_from_ebird_task',
    provide_context=True,
    python_callable = mrr.load_mrr_from_ebird,
    dag = dag,
    on_success_callback = etl_log.on_DAG_mrr_loaded_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

# Load new data into staging area (from MRR to STG db)
load_stg_from_mrr_task = PythonOperator(
    task_id = 'load_stg_from_mrr_task',
    provide_context=True,
    python_callable = stg.load_stg_from_mrr,
    dag = dag,
    on_success_callback = etl_log.on_DAG_stg_loaded_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

# Load processed data from staging area
# into target data model (Star Schema) of DWH 
load_dwh_from_stg_task = PythonOperator(
    task_id = 'load_dwh_from_stg_task',
    provide_context=True,
    python_callable = dwh.load_dwh_from_stg,
    dag = dag,
    on_success_callback = etl_log.on_DAG_dwh_loaded_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

# Create full backups of all three DBs in instance
full_db_backup_task = BashOperator(
    task_id = 'full_db_backup_task',
    bash_command = sq.full_backup_sh,
    dag = dag,
    cwd = dag.folder,
    on_success_callback = etl_log.on_DAG_full_backup_created_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
)

# End ETL
etl_end_task = DummyOperator(
    task_id='etl_end_task',
    on_success_callback = etl_log.on_DAG_success_alert,
    on_retry_callback = etl_log.on_DAG_retry_alert,
    dag=dag
)

# task pipeline
etl_start_task              >> load_mrr_from_ebird_task 
load_mrr_from_ebird_task    >> load_stg_from_mrr_task 
load_stg_from_mrr_task      >> load_dwh_from_stg_task 
load_dwh_from_stg_task      >> full_db_backup_task
full_db_backup_task         >> etl_end_task
