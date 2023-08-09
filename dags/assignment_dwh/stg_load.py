# Import libraries
import pandas as pd
import numpy as np
import os

from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/') # Temporary storage location


def load_stg_dictionaries_from_mrr():
    # Get cursors to DBs
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Clear STG tables from old data
    pgs_stg_hook.run('DELETE FROM stg_fact_locations')
    pgs_stg_hook.run('DELETE FROM stg_fact_taxonomy')

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
    insert_sql = f"COPY stg_fact_locations \
                            FROM '{home_dir}/locations_stg.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_stg_hook.run(insert_sql)
    os.remove(home_dir + '/locations_stg.csv')

    select_sql = """
        SELECT t.speciesCode speciesCode, t.sciName sciName, t.comName comName, t.category category,
                t.orderSciName orderSciName, t.familyCode familyCode, t.familySciName familySciName
        FROM mrr_fact_taxonomy t
    """
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'comName', 'category',
                                                                                'orderSciName', 'familyCode', 'familySciName'])

    mrr_new_frame.to_csv(home_dir + '/taxonomy_stg.csv', index = False)
    insert_sql = f"COPY stg_fact_taxonomy_tmp \
                            FROM '{home_dir}/taxonomy_stg.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_stg_hook.run(insert_sql)
    os.remove(home_dir + '/taxonomy_stg.csv')
    insert_sql = """
        INSERT INTO stg_fact_taxonomy
        SELECT t.speciesCode speciesCode, t.sciName sciName, t.comName comName, t.category category,
                t.orderSciName orderSciName, o.orderComName orderComName, t.familyCode familyCode,
                f.familyComName familyComName, t.familySciName familySciName
        FROM stg_fact_taxonomy_tmp t
        LEFT JOIN stg_fact_order_comnames o
        ON t.orderSciName = o.orderSciName AND o.orderLocale = 'ru'
        LEFT JOIN stg_fact_family_comnames f
        ON t.familyCode = f.familyCode AND f.familyLocale = 'ru'
        ON CONFLICT DO NOTHING
    """
    pgs_stg_hook.run(insert_sql)
    pgs_stg_hook.run('DELETE FROM stg_fact_taxonomy_tmp')
    pgs_mrr_hook.conn.close()
    pgs_stg_hook.conn.close()



def load_stg_from_mrr(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")
    
    # Load current high_water_mark
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT current_high_ts FROM high_water_mark WHERE table_id = 'stg_fact_observation'")
    if len(tmp_water_mark) < 1 or not tmp_water_mark[0][0]:
        high_water_mark = '2020-01-21 16:35'
        pgs_dwh_hook.run("INSERT INTO high_water_mark VALUES(%s, %s) ON CONFLICT (table_id) DO UPDATE \
                            SET current_high_ts = EXCLUDED.current_high_ts", parameters = ('stg_fact_observation', high_water_mark))
    else:
        high_water_mark = tmp_water_mark[0][0]
    print(f"high_water_mark = {high_water_mark}")

    # Load data from mrr database, clean and transfort
    select_sql = f"SELECT speciesCode, sciName, locId, locName, obsDt, howMany, lat, lon, subId, comName \
                    FROM mrr_fact_recent_observation \
                    WHERE obsValid = TRUE and obsdt > '{high_water_mark}'"
    mrr_new_frame = pd.DataFrame(pgs_mrr_hook.get_records(select_sql), columns = ['speciesCode', 'sciName', 'locId', 'locName',
                                                                                'obsDt', 'howMany', 'lat', 'lon', 'subId', 'comName'])
    mrr_new_frame.to_csv(home_dir + '/observations_stg.csv', index = False)
    
    # Store clean date to STG (redy to model DWH)
    insert_sql = f"COPY stg_fact_observation_tmp \
                            FROM '{home_dir}/observations_stg.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_stg_hook.run(insert_sql)
    os.remove(home_dir + '/observations_stg.csv')

    insert_sql = """
        INSERT INTO stg_fact_observation
        SELECT *
        FROM stg_fact_observation_tmp t
        ON CONFLICT DO NOTHING
    """
    pgs_stg_hook.run(insert_sql)    
    pgs_stg_hook.run('DELETE FROM stg_fact_observation_tmp')
    
    print('Stored to STG - ', len(mrr_new_frame), 'rows.')

    # Save current high_water_mark
    tmp_water_mark = pgs_stg_hook.get_records("SELECT MAX(obsdt) FROM stg_fact_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'stg_fact_observation'",
                     parameters = (tmp_water_mark[0][0],))

    # Load and transform dictionaries from MRR to STG
    load_stg_dictionaries_from_mrr()
    pgs_mrr_hook.conn.close()
    pgs_stg_hook.conn.close()
    pgs_dwh_hook.conn.close()



