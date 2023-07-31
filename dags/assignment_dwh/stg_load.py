# Import libraries
import pandas as pd
import numpy as np


from airflow.hooks.postgres_hook import PostgresHook

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
    mrr_new_frame = pgs_mrr_hook.get_records(f"SELECT speciesCode, sciName, locId, locName, obsDt, howMany, \
                                                       lat, lon, subId, comName \
                                             FROM mrr_fact_recent_observation \
                                             WHERE obsValid = TRUE and obsdt > '{high_water_mark}'")
    
    # Store clean date to STG (redy to model DWH)
    insert_sql = "INSERT INTO stg_fact_observation VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for row in mrr_new_frame:
        pgs_stg_hook.run(insert_sql, parameters = (row[0], row[1], row[2], row[3], row[4], row[5],
                                                    row[6], row[7], row[8], row[9]))
    
    print('Stored to STG - ', len(mrr_new_frame), 'rows.')

    # Save current high_water_mark
    tmp_water_mark = pgs_stg_hook.get_records("SELECT MAX(obsdt) FROM stg_fact_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'stg_fact_observation'",
                     parameters = (tmp_water_mark[0][0],))


