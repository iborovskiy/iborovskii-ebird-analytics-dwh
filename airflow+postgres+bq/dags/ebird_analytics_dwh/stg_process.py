# General libraries imports
import pandas as pd
import numpy as np
import os

# DAG imports
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook
# DAG utils imports
from airflow.models import Variable

# Google Cloud Connectors
from google.cloud import storage
from google.oauth2 import service_account

# Local modules imports
import ebird_analytics_dwh.etl_logging as etl_log
import ebird_analytics_dwh.configs as sq


def load_stg_from_mrr(*args, **kwargs):
    # Get DAG variables values
    locale = Variable.get("EBIRD_LOCALE", default_var='ru')                 # Language for common name
    home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')          # Temporary working directory
    key_path = Variable.get("EBIRD_BIGQUERY_KEY_PATH", default_var='/tmp/') # Path to the service account key file
    # Get cursors to DBs
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
    bucket = storage_client.get_bucket('fiery-rarity-396614-ebird')
    
    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    weather_high_water_mark = etl_log.load_high_water_mark('mrr_fact_weather_observations')
    print(f"high_water_mark = {high_water_mark}")
    print(f"weather_high_water_mark = {weather_high_water_mark}")

    # Export trusted new data rows for observations from MRR db to CSV files 
    # and upload them into Staging area in Google Cloud Storage (incremental using high water mark)
    # - Clear STG observations from bad data
    # - Store clean date to Staging area (redy to model DWH)
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_fact_locations_to_csv_sql.format(high_water_mark)),
                                    columns = ['locid', 'locname', 'lat', 'lon', 'countryname', 'subregionname',
                                               'latestobsdt', 'numspeciesalltime'])
    mrr_new_frame.to_csv(home_dir + '/stg_location.csv', index = False)
    blob = bucket.blob('stg_location.csv')
    blob.upload_from_filename(home_dir + '/stg_location.csv')

    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_fact_checklists_to_csv_sql.format(high_water_mark)),
                                    columns = ['locid', 'subid', 'userdisplayname', 'numspecies', 'obsfulldt'])
    mrr_new_frame.to_csv(home_dir + '/stg_checklist.csv', index = False)
    blob = bucket.blob('stg_checklist.csv')
    blob.upload_from_filename(home_dir + '/stg_checklist.csv')

    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_fact_observations_to_csv_sql.format(high_water_mark)),
                                    columns = ['speciesCode', 'obsDt', 'subId', 'obsId', 'howMany'])
    mrr_new_frame.to_csv(home_dir + '/stg_observation.csv', index = False)
    blob = bucket.blob('stg_observation.csv')
    blob.upload_from_filename(home_dir + '/stg_observation.csv')
    print('Stored new observations to STG - ', len(mrr_new_frame), 'rows.')

    # Export new weather observation for updated locations (incremental using high water mark)
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_weather_to_csv_sql.format(weather_high_water_mark)),
                                    columns = ['loc_id', 'obsdt', 'tavg', 'tmin',
                                                'tmax', 'prcp', 'snow', 'wdir', 'wspd', 'wpgt', 'pres', 'tsun', 'update_ts'])
    mrr_new_frame.to_csv(home_dir + '/stg_weather.csv', index = False)
    blob = bucket.blob('stg_weather.csv')
    blob.upload_from_filename(home_dir + '/stg_weather.csv')

    # Export actual MRR dictionaries to CSV files 
    # and upload them into Staging area in Google Cloud Storage
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_dict_locations_to_csv_sql), 
                                    columns = ['locid', 'locname', 'countryname', 'subregionname', 
                                                'lat', 'lon', 'numspeciesalltime', 'latestobsdt'])
    mrr_new_frame.to_csv(home_dir + '/stg_hotspots.csv', index = False)
    blob = bucket.blob('stg_hotspots.csv')
    blob.upload_from_filename(home_dir + '/stg_hotspots.csv')

    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_dict_taxonomy_to_csv_sql.format(locale)),
                                    columns = ['speciesCode', 'sciName', 'comName', 'category',
                                                'orderSciName', 'orderComName', 'familyCode',
                                                'familyComName', 'familySciName'])
    mrr_new_frame.to_csv(home_dir + '/stg_taxonomy.csv', index = False)
    blob = bucket.blob('stg_taxonomy.csv')
    blob.upload_from_filename(home_dir + '/stg_taxonomy.csv')

    # Remove temporary csv files
    os.remove(home_dir + '/stg_checklist.csv')
    os.remove(home_dir + '/stg_location.csv')
    os.remove(home_dir + '/stg_observation.csv')
    os.remove(home_dir + '/stg_weather.csv')
    os.remove(home_dir + '/stg_hotspots.csv')
    os.remove(home_dir + '/stg_taxonomy.csv')

    # Close the connection to STG and MRR db
    pgs_mrr_hook.conn.close()
    storage_client.close()
