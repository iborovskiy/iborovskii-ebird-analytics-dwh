# General libraries imports
import pandas as pd
import numpy as np
import os

# DAG imports
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook
# DAG utils imports
from airflow.models import Variable

# Local modules imports
import ebird_analytics_dwh.etl_logging as etl_log
import ebird_analytics_dwh.configs as sq


def load_stg_dictionaries_from_mrr():
    # Get DAG variables values
    locale = Variable.get("EBIRD_LOCALE", default_var='ru')                 # Language for common name
    DWH_INTERNAL_MODEL = False if Variable.get("EBIRD_DWH_INTERNAL_MODEL", 
                        default_var='false').lower() == 'false' else True   # Model creation mode
    home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')          # Temporary working directory
    # Get cursors to DBs
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Export actual MRR dictionaries to CSV files
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_dict_locations_to_csv_sql), 
                                    columns = ['locid', 'locname', 'countryname', 'subregionname', 
                                                'lat', 'lon', 'latestobsdt', 'numspeciesalltime'])
    mrr_new_frame.to_csv(home_dir + '/locations_stg.csv', index = False)

    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_dict_taxonomy_to_csv_sql),
                                    columns = ['speciesCode', 'sciName', 'comName', 'category',
                                                'orderSciName', 'orderComName', 'familyCode',
                                                'familyComName', 'familySciName'])
    mrr_new_frame.to_csv(home_dir + '/taxonomy_stg.csv', index = False)

    # Store exported dictionaries into STG db
    # - Clear STG dictionaries tables from old data
    # - Load full new update from exported CSV (not incremental) 
    # - and add common names of birds for selected locale

    if DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of STG")
        sql_q = sq.import_dict_from_csv_to_stg_sql.format(home_dir, locale)
    
    else:
        # Use stored procedure from STG db (single transaction)
        print("Use internal load mode of STG")
        sql_q = sq.import_dict_from_csv_to_stg_proc.format(home_dir, locale)
    pgs_stg_hook.run(sql_q)

    # Remove temporary csv files
    os.remove(home_dir + '/locations_stg.csv')
    os.remove(home_dir + '/taxonomy_stg.csv')

    # Close the connection to STG and MRR db
    pgs_mrr_hook.conn.close()
    pgs_stg_hook.conn.close()



def load_stg_from_mrr(*args, **kwargs):
    # Get DAG variables values
    DWH_INTERNAL_MODEL = False if Variable.get("EBIRD_DWH_INTERNAL_MODEL", 
                        default_var='false').lower() == 'false' else True   # Model creation mode
    home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')          # Temporary working directory
    # Get cursors to DBs
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")
    
    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    weather_high_water_mark = etl_log.load_high_water_mark('mrr_fact_weather_observations')
    print(f"high_water_mark = {high_water_mark}")
    print(f"weather_high_water_mark = {weather_high_water_mark}")

    # Export trusted new data rows for observations from MRR db to CSV files (incremental using high water mark)
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_observations_to_csv_sql.format(high_water_mark)),
                                    columns = ['speciesCode', 'sciName', 'locId', 'locName',
                                                'obsDt', 'howMany', 'lat', 'lon', 'subId', 'comName'])
    mrr_new_frame.to_csv(home_dir + '/observations_stg.csv', index = False)

    # Export new weather observation for updated locations (incremental using high water mark)
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(sq.mrr_weather_to_csv_sql.format(weather_high_water_mark)),
                                    columns = ['loc_id', 'obsdt', 'tavg', 'tmin',
                                                'tmax', 'prcp', 'snow', 'wdir', 'wspd', 'wpgt', 'pres', 'tsun', 'update_ts'])
    mrr_new_frame.to_csv(home_dir + '/weather_stg.csv', index = False)
    
    # Store exported CSV into STG db
    # - Clear STG observations table from old data
    # - Store clean date to STG (redy to model DWH)

    if DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of STG")
        sql_q = sq.import_observation_from_csv_to_stg_sql.format(home_dir)
    else:
        # Use stored procedure from STG db (single transaction)
        print("Use internal load mode of STG")
        sql_q = sq.import_observation_from_csv_to_stg_proc.format(home_dir)
    pgs_stg_hook.run(sql_q)
    
    print('Stored to STG - ', len(mrr_new_frame), 'rows.')

    # Remove temporary csv files
    os.remove(home_dir + '/observations_stg.csv')
    os.remove(home_dir + '/weather_stg.csv')

    # Load and transform dictionaries from MRR to STG
    load_stg_dictionaries_from_mrr()

    # Close the connection to STG and MRR db
    pgs_mrr_hook.conn.close()
    pgs_stg_hook.conn.close()



