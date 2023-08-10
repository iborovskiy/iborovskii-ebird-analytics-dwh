# Import libraries
import pandas as pd
import numpy as np
import os

from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

import assignment_dwh.etl_logging as etl_log

# Select model creation mode
var_tmp = Variable.get("EBIRD_DWH_INTERNAL_MODEL", default_var='false').lower()
DWH_INTERNAL_MODEL = False if var_tmp == 'false' else True

home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/') # Temporary storage location


def load_dwh_from_stg_inside(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")
    
    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")

    # Create data model - fact and dimensions
    stg_new_frame = pgs_stg_hook.get_records(f"SELECT * FROM stg_fact_observation WHERE obsdt > '{high_water_mark}'")
    for row in stg_new_frame:
        pgs_dwh_hook.run(f"CALL process_new_row('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5] if row[5] else 'NaN'}', {row[6]}, {row[7]}, '{row[8]}', '{row[9]}')")
    
    print('Stored to dwh_fact_observation - ', len(stg_new_frame), 'rows.')
    pgs_dwh_hook.conn.close()
    pgs_stg_hook.conn.close()

    # Update current high_water_mark
    etl_log.update_high_water_mark()
   


def load_dwh_from_stg_outside(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")

    # Load new rows from stg
    select_sql = f"SELECT * FROM stg_fact_observation WHERE obsdt > '{high_water_mark}'"
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'locId', 'locName',
                                                                                'obsDt', 'howMany', 'lat', 'lon', 'subId', 'comName'])
    stg_new_frame.to_csv(home_dir + '/observations_dwh.csv', index = False)
    insert_sql = f"COPY dwh_fact_raw_observations_tmp \
                            FROM '{home_dir}/observations_dwh.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_dwh_hook.run(insert_sql)
    os.remove(home_dir + '/observations_dwh.csv')

    select_sql = f"SELECT * FROM stg_fact_taxonomy"
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'comName', 
                                                                                  'category', 'orderSciName', 'orderComName', 
                                                                                  'familyCode', 'familyComName', 'familySciName'])
    stg_new_frame.to_csv(home_dir + '/taxonomy_dwh.csv', index = False)
    pgs_dwh_hook.run("DELETE FROM dwh_dim_species_details")
    insert_sql = f"COPY dwh_dim_species_details \
                            FROM '{home_dir}/taxonomy_dwh.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_dwh_hook.run(insert_sql)
    os.remove(home_dir + '/taxonomy_dwh.csv')


    # Create dimension table - dwh_dim_dt
    insert_sql = f"""
                    INSERT INTO dwh_dim_dt
                    SELECT DISTINCT obsdt, extract(day from obsdt), extract(month from obsdt),
                                    TO_CHAR(obsdt, 'Month'), extract(year from obsdt), extract(quarter from obsdt)
                    FROM dwh_fact_raw_observations_tmp
                    WHERE obsdt > '{high_water_mark}'
                    ON CONFLICT (obsdt) DO NOTHING
                """
    pgs_dwh_hook.run(insert_sql)

    print('dwh_dim_dt updated')    

    # Create fact table - dwh_fact_observation
    insert_sql = f"""
                    INSERT INTO dwh_fact_observation
                    SELECT subid, speciescode, locid, obsdt, howmany
                    FROM dwh_fact_raw_observations_tmp
                    WHERE obsdt > '{high_water_mark}'
                    ON CONFLICT DO NOTHING
                """
    pgs_dwh_hook.run(insert_sql)
    
    print('Stored to dwh_fact_observation - ', len(stg_new_frame), 'rows.')

    # Create dimension table - dwh_dim_location
    select_sql = f"""SELECT l.locid, l.locname, l.countryname, l.subRegionName, 
                                l.lat, l.lon, l.latestObsDt, l.numSpeciesAllTime
                    FROM stg_fact_locations l
                """
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['locId', 'locName', 'countryName', 
                                                                                  'subRegionName', 'lat', 'lon', 'latestObsDt', 'numSpeciesAllTime'])
    stg_new_frame.to_csv(home_dir + '/locations_dwh.csv', index = False)

    pgs_dwh_hook.run("DELETE FROM dwh_dim_location_details")
    insert_sql = f"""
                    COPY dwh_dim_location_details
                    FROM '{home_dir}/locations_dwh.csv'
                    DELIMITER ','
                    CSV HEADER
                """
    pgs_dwh_hook.run(insert_sql)
    os.remove(home_dir + '/locations_dwh.csv')

    insert_sql = f"""
                    INSERT INTO dwh_dim_location
                    SELECT DISTINCT o.locid, o.locname, o.lat, o.lon, l.countryname,
                                l.subRegionName, l.latestObsDt, l.numSpeciesAllTime
                    FROM dwh_fact_raw_observations_tmp o
                    LEFT JOIN dwh_dim_location_details l
                    ON o.locid = l.locid
                    ON CONFLICT (locid) DO NOTHING
                """
    pgs_dwh_hook.run(insert_sql)


    # Create dimension table - dwh_dim_species
    pgs_dwh_hook.run("DELETE FROM dwh_dim_species")
    insert_sql = f"""
                    INSERT INTO dwh_dim_species
                    SELECT DISTINCT o.speciescode, d.sciName, d.comName, d.category, d.orderSciName, d.orderComName,
                                    d.familyCode, d.familyComName, d.familySciName
                    FROM dwh_fact_observation o
                    LEFT JOIN dwh_dim_species_details d
                    ON o.speciescode = d.speciescode
                """
    pgs_dwh_hook.run(insert_sql)

    print('dwh_dim_species updated')


    pgs_dwh_hook.run('DELETE FROM dwh_fact_raw_observations_tmp')

    pgs_dwh_hook.conn.close()
    pgs_stg_hook.conn.close()

    # Update current high_water_mark on success of current DAG
    etl_log.update_high_water_mark()


def load_dwh_from_stg(*args, **kwargs):
    if DWH_INTERNAL_MODEL:
        return load_dwh_from_stg_inside(*args, **kwargs)
    else:
        return load_dwh_from_stg_outside(*args, **kwargs)