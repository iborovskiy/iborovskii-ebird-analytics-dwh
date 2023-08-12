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


def load_dwh_from_stg(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")


    # Export data accumulated in staging area (STG db) into CSV files
        
    # Export new observation accumulated from previous high water mark ts
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(sq.stg_observations_to_csv_sql.format(high_water_mark)),
                                    columns = ['speciesCode', 'sciName', 'locId', 'locName',
                                    'obsDt', 'howMany', 'lat', 'lon', 'subId', 'comName'])
    stg_new_frame.to_csv(sq.home_dir + '/observations_dwh.csv', index = False)
    print('Importing', len(stg_new_frame), 'new observations.')

    # Export actual bird taxonomy dictionary
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(sq.stg_dict_taxonomy_to_csv_sql),
                                    columns = ['speciesCode', 'sciName', 'comName', 
                                                'category', 'orderSciName', 'orderComName', 
                                                'familyCode', 'familyComName', 'familySciName'])
    stg_new_frame.to_csv(sq.home_dir + '/taxonomy_dwh.csv', index = False)

    # Export actual public locations dictionary
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(sq.stg_dict_location_to_csv_sql),
                                    columns = ['locId', 'locName', 'countryName', 
                                                'subRegionName', 'lat', 'lon', 'latestObsDt', 'numSpeciesAllTime'])
    stg_new_frame.to_csv(sq.home_dir + '/locations_dwh.csv', index = False)

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

    if sq.DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of STG")
        sql_q = sq.dwh_update_model_sql
    else:
        # Use stored procedure from DWH db (single transaction)
        print("Use internal load mode of DWH")
        sql_q = sq.dwh_update_model_proc
    pgs_dwh_hook.run(sql_q)

    print('All fact and dimension tables of DWH are updated.')

    # Remove temporary csv files
    os.remove(sq.home_dir + '/observations_dwh.csv')
    os.remove(sq.home_dir + '/taxonomy_dwh.csv')
    os.remove(sq.home_dir + '/locations_dwh.csv')

    # Close the connection to STG and MRR db
    pgs_dwh_hook.conn.close()
    pgs_stg_hook.conn.close()
