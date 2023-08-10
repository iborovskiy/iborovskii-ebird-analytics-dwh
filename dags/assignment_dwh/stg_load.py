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

# Get params
locale = Variable.get("EBIRD_LOCALE", default_var='ru') # Language for common name
home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/') # Temporary storage location


def load_stg_dictionaries_from_mrr():
    # Get cursors to DBs
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Export current MRR dictionaries to CSV files
    select_sql = """
        SELECT l.locid locid, l.locname locname, c.countryname countryname, s.subnationalname subregionname, 
                l.lat lat, l.lon lon, l.latestobsdt latestobsdt, l.numspeciesalltime numspeciesalltime
        FROM mrr_fact_locations l
        JOIN mrr_fact_countries c
        ON l.countrycode = c.countrycode
        JOIN mrr_fact_subnational s
        ON l.subnational1code = s.subnationalcode
    """
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(select_sql), columns = ['locid', 'locname', 'countryname', 'subregionname',
                                                                                'lat', 'lon', 'latestobsdt', 'numspeciesalltime'])
    mrr_new_frame.to_csv(home_dir + '/locations_stg.csv', index = False)

    select_sql = """
        SELECT t.speciesCode speciesCode, t.sciName sciName, t.comName comName, t.category category,
                t.orderSciName orderSciName, NULL AS orderComName, t.familyCode familyCode, NULL AS familyComName,
                t.familySciName familySciName
        FROM mrr_fact_taxonomy t
    """
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'comName', 'category',
                                                                                'orderSciName', 'orderComName', 'familyCode',
                                                                                'familyComName', 'familySciName'])

    mrr_new_frame.to_csv(home_dir + '/taxonomy_stg.csv', index = False)

    # Store exported dictionaries into STG db

    if DWH_INTERNAL_MODEL == False:
        # If property set to TRUE then use stored procedure from STG db (loading as a single transaction)
        print("Use external load mode of STG")

        # Clear STG dictionaries tables from old data
        pgs_stg_hook.run('DELETE FROM stg_fact_locations')
        pgs_stg_hook.run('DELETE FROM stg_fact_taxonomy')

        # Load full new update from exported CSV (not incremental)
        insert_sql = f"""
                    COPY stg_fact_locations
                    FROM '{home_dir}/locations_stg.csv'
                    DELIMITER ','
                    CSV HEADER
                """
        pgs_stg_hook.run(insert_sql)

        insert_sql = f"""
                        COPY stg_fact_taxonomy
                        FROM '{home_dir}/taxonomy_stg.csv'
                        DELIMITER ','
                        CSV HEADER
                    """
        pgs_stg_hook.run(insert_sql)

        # Add common names of birds for selected locale
        update_sql = f"""
            UPDATE stg_fact_taxonomy
            SET orderComName = stg_fact_order_comnames.orderComName,
                familyComName = stg_fact_family_comnames.familyComName
            FROM stg_fact_order_comnames, stg_fact_family_comnames
            WHERE stg_fact_taxonomy.orderSciName = stg_fact_order_comnames.orderSciName
                AND stg_fact_order_comnames.orderLocale = '{locale}'
                AND stg_fact_taxonomy.familyCode = stg_fact_family_comnames.familyCode
                AND stg_fact_family_comnames.familyLocale = '{locale}'
        """
        pgs_stg_hook.run(update_sql)
    
    else:
        # Use stored procedure from STG db (loading as a single transaction)
        print("Use internal load mode of STG")
        pgs_stg_hook.run(f"CALL stg_process_dictionaries({home_dir}, 'locations_stg.csv', 'taxonomy_stg.csv', {locale})")

    # Remove temporary csv files
    os.remove(home_dir + '/locations_stg.csv')
    os.remove(home_dir + '/taxonomy_stg.csv')

    # Close the connection to STG and MRR db
    pgs_mrr_hook.conn.close()
    pgs_stg_hook.conn.close()



def load_stg_from_mrr(*args, **kwargs):
    # Get cursors to DBs
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")
    
    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")

    # Load trusted new data rows from mrr database (incremental using high water mark) and save them into CSV files
    select_sql = f"""
                    SELECT speciesCode, sciName, locId, locName, obsDt, howMany, lat, lon, subId, comName
                    FROM mrr_fact_recent_observation
                    WHERE obsValid = TRUE and obsdt > '{high_water_mark}'
                """
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'locId', 'locName',
                                                                                'obsDt', 'howMany', 'lat', 'lon', 'subId', 'comName'])
    mrr_new_frame.to_csv(home_dir + '/observations_stg.csv', index = False)
    
    # Store exported CSV into STG db
    if DWH_INTERNAL_MODEL == False:
        # If property set to TRUE then use stored procedure from STG db (loading as a single transaction)
        print("Use external load mode of STG")

        # Clear STG observations table from old data
        pgs_stg_hook.run('DELETE FROM stg_fact_observation')

        # Store clean date to STG (redy to model DWH)
        insert_sql = f"""
                        COPY stg_fact_observation
                        FROM '{home_dir}/observations_stg.csv'
                        DELIMITER ','
                        CSV HEADER
                    """
        pgs_stg_hook.run(insert_sql)

    else:
        # Use stored procedure from STG db (loading as a single transaction)
        print("Use internal load mode of STG")
        pgs_stg_hook.run(f"CALL stg_process_observations({home_dir}, 'observations_stg.csv')")
    
    print('Stored to STG - ', len(mrr_new_frame), 'rows.')

    # Remove temporary csv files
    os.remove(home_dir + '/observations_stg.csv')

    # Load and transform dictionaries from MRR to STG
    load_stg_dictionaries_from_mrr()

    # Close the connection to STG and MRR db
    pgs_mrr_hook.conn.close()
    pgs_stg_hook.conn.close()



