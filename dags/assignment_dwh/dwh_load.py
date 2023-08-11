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
import assignment_dwh.etl_logging as etl_log

# Initialize configuration parameters
var_tmp = Variable.get("EBIRD_DWH_INTERNAL_MODEL", default_var='false').lower()
DWH_INTERNAL_MODEL = False if var_tmp == 'false' else True      # Model creation mode
home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')  # Temporary workig directory


def load_dwh_from_stg(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")


    # Export data accumulated in staging area (STG db) into CSV files
        
    # Export new observation accumulated from previous high water mark ts
    select_sql = f"SELECT * FROM stg_fact_observation WHERE obsdt > '{high_water_mark}'"
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'locId', 'locName',
                                                                                'obsDt', 'howMany', 'lat', 'lon', 'subId', 'comName'])
    stg_new_frame.to_csv(home_dir + '/observations_dwh.csv', index = False)
    print('Importing', len(stg_new_frame), 'new observations.')

    # Export actual bird taxonomy dictionary
    select_sql = f"SELECT * FROM stg_fact_taxonomy"
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'comName', 
                                                                                  'category', 'orderSciName', 'orderComName', 
                                                                                  'familyCode', 'familyComName', 'familySciName'])
    stg_new_frame.to_csv(home_dir + '/taxonomy_dwh.csv', index = False)

    # Create actual public locations dictionary
    select_sql = f"""SELECT l.locid, l.locname, l.countryname, l.subRegionName, 
                                l.lat, l.lon, l.latestObsDt, l.numSpeciesAllTime
                    FROM stg_fact_locations l
                """
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['locId', 'locName', 'countryName', 
                                                                                  'subRegionName', 'lat', 'lon', 'latestObsDt', 'numSpeciesAllTime'])
    stg_new_frame.to_csv(home_dir + '/locations_dwh.csv', index = False)


    # Create actual data model - fact and dimension table for Star Schema
    if DWH_INTERNAL_MODEL == False:
        # If property set to TRUE then use stored procedure from DWH db (loading as a single transaction)
        print("Use external load mode of STG")

        # Import csv files with new data sets from staging area

        # Import new observations into temporary fact table
        insert_sql = f"""
                        COPY dwh_fact_raw_observations_tmp
                        FROM '{home_dir}/observations_dwh.csv'
                        DELIMITER ','
                        CSV HEADER
                    """
        pgs_dwh_hook.run(insert_sql)

        # Import new taxonomy dictionary into DWH
        pgs_dwh_hook.run("DELETE FROM dwh_dim_species_details")
        insert_sql = f"""
                        COPY dwh_dim_species_details
                        FROM '{home_dir}/taxonomy_dwh.csv'
                        DELIMITER ','
                        CSV HEADER
                    """
        pgs_dwh_hook.run(insert_sql)

        # Import new public locations dictionary into DWH
        pgs_dwh_hook.run("DELETE FROM dwh_dim_location_details")
        insert_sql = f"""
                        COPY dwh_dim_location_details
                        FROM '{home_dir}/locations_dwh.csv'
                        DELIMITER ','
                        CSV HEADER
                    """
        pgs_dwh_hook.run(insert_sql)

        # Create new data model for DWH

        # Create dimension table - dwh_dim_dt (incremental update)
        insert_sql = f"""
                    INSERT INTO dwh_dim_dt
                    SELECT DISTINCT obsdt, extract(day from obsdt), extract(month from obsdt),
                                    TO_CHAR(obsdt, 'Month'), extract(year from obsdt), extract(quarter from obsdt)
                    FROM dwh_fact_raw_observations_tmp
                    ON CONFLICT (obsdt) DO NOTHING
                    """
        pgs_dwh_hook.run(insert_sql)
        print('dwh_dim_dt updated')
        
        # Create fact table - dwh_fact_observation (incremental update)
        insert_sql = f"""
                    INSERT INTO dwh_fact_observation
                    SELECT subid, speciescode, locid, obsdt, howmany
                    FROM dwh_fact_raw_observations_tmp
                    ON CONFLICT DO NOTHING
                    """
        pgs_dwh_hook.run(insert_sql)
        print('dwh_fact_observation updated')

        # Create dimension table - dwh_dim_location (incremental update)
        insert_sql = f"""
                    INSERT INTO dwh_dim_location
                    SELECT DISTINCT o.locid, o.locname, o.lat, o.lon, l.countryname,
                                l.subRegionName, l.latestObsDt, l.numSpeciesAllTime
                    FROM dwh_fact_raw_observations_tmp o
                    LEFT JOIN dwh_dim_location_details l
                    ON o.locid = l.locid
                    ON CONFLICT (locid) DO UPDATE
                    SET locname = EXCLUDED.locname, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                        countryname = EXCLUDED.countryname, subRegionName = EXCLUDED.subRegionName,
                        latestObsDt = EXCLUDED.latestObsDt, numSpeciesAllTime = EXCLUDED.numSpeciesAllTime
                    """
        pgs_dwh_hook.run(insert_sql)
        print('dwh_dim_location updated')
    
        # Create dimension table - dwh_dim_species (full update not incremental)
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

        # Truncate temporary observation table
        pgs_dwh_hook.run('DELETE FROM dwh_fact_raw_observations_tmp')
        
        # Update current high_water_mark on success of current DAG
        update_sql = f"""
	                    UPDATE high_water_mark
	                    SET current_high_ts = (SELECT MAX(obsdt) FROM dwh_fact_observation)
	                    WHERE table_id = 'dwh_fact_observation';
                    """
        pgs_dwh_hook.run(update_sql)
        print('high_water_mark updated.')

    else:
        # Use stored procedure from DWH db (loading as a single transaction + updating high_water_mark)
        print("Use internal load mode of DWH")
        pgs_dwh_hook.run(f"CALL dwh_process_observations('{home_dir}', 'observations_dwh.csv', 'locations_dwh.csv', 'taxonomy_dwh.csv')")
        print('All fact and dimension tables of DWH are updated.')


    # Remove temporary csv files
    os.remove(home_dir + '/observations_dwh.csv')
    os.remove(home_dir + '/taxonomy_dwh.csv')
    os.remove(home_dir + '/locations_dwh.csv')

    # Close the connection to STG and MRR db
    pgs_dwh_hook.conn.close()
    pgs_stg_hook.conn.close()
