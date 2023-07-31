# Import libraries
import pandas as pd
import numpy as np


from airflow.hooks.postgres_hook import PostgresHook

def load_dwh_from_stg(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")
    # Load current high_water_mark
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT get_high_water_mark('dwh_fact_observation')")
    if len(tmp_water_mark) < 1 or not tmp_water_mark[0][0]:
        high_water_mark = '2020-01-21 16:35'
        pgs_dwh_hook.run("INSERT INTO high_water_mark VALUES(%s, %s) ON CONFLICT (table_id) DO UPDATE \
                            SET current_high_ts = EXCLUDED.current_high_ts", parameters = ('dwh_fact_observation', high_water_mark))

    else:
        high_water_mark = tmp_water_mark[0][0]
    print(f"high_water_mark = {high_water_mark}")
    # Create data model - fact and dimensions
    stg_new_frame = pgs_stg_hook.get_records(f"SELECT * FROM stg_fact_observation WHERE obsdt > '{high_water_mark}'")
    for row in stg_new_frame:
        pgs_dwh_hook.run(f"CALL process_new_row('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5]}', {row[6]}, {row[7]}, '{row[8]}', '{row[9]}')")
    
    print('Stored to dwh_fact_observation - ', len(stg_new_frame), 'rows.')




def load_dwh_from_stg_old(*args, **kwargs):
    # Get cursors to DBs
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    pgs_stg_hook = PostgresHook(postgres_conn_id="postgres_stg_conn")

    # Load current high_water_mark
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT current_high_ts FROM high_water_mark WHERE table_id = 'dwh_fact_observation'")
    if len(tmp_water_mark) < 1:
        high_water_mark = '2020-01-21 16:35'
        pgs_dwh_hook.run("INSERT INTO high_water_mark VALUES(%s, %s)", parameters = ('dwh_fact_observation', high_water_mark))
    else:
        high_water_mark = tmp_water_mark[0][0]
    print(f"high_water_mark = {high_water_mark}")

    # Create dimension table - dwh_dim_dt
    stg_new_frame = pgs_stg_hook.get_records(f"SELECT DISTINCT obsdt, extract(day from obsdt), extract(month from obsdt), \
                                                TO_CHAR(obsdt, 'Month'), extract(year from obsdt), extract(quarter from obsdt)  \
                                                FROM stg_fact_observation \
                                                WHERE obsdt > '{high_water_mark}'")

    insert_sql = "INSERT INTO dwh_dim_dt VALUES(%s, %s, %s, %s, %s, %s) ON CONFLICT (obsdt) DO NOTHING"
    for row in stg_new_frame:
        pgs_dwh_hook.run(insert_sql, parameters = (row[0], row[1], row[2], row[3], row[4], row[5]))

    print('Stored to dwh_dim_dt - ', len(stg_new_frame), 'rows.')

    # Create dimension table - dwh_dim_location
    stg_new_frame = pgs_stg_hook.get_records(f"SELECT DISTINCT locid, locname, lat, lon \
                                                FROM stg_fact_observation \
                                                WHERE obsdt > '{high_water_mark}'")
    
    insert_sql = "INSERT INTO dwh_dim_location VALUES('{0}', '{1}', {2}, {3}) \
                    ON CONFLICT (locid) \
                    DO UPDATE \
                    SET locname = excluded.locname, lat = excluded.lat, lon = excluded.lon"

    for row in stg_new_frame:
        pgs_dwh_hook.run(insert_sql.format(row[0], row[1], row[2], row[3]))

    print('Stored to dwh_dim_location - ', len(stg_new_frame), 'rows.')

    # Create dimension table - dwh_dim_species
    stg_new_frame = pgs_stg_hook.get_records(f"SELECT DISTINCT speciescode, sciname, comname \
                                                FROM stg_fact_observation \
                                                WHERE obsdt > '{high_water_mark}'")
    
    insert_sql = "INSERT INTO dwh_dim_species VALUES('{0}', '{1}', '{2}') \
                    ON CONFLICT (speciescode) DO NOTHING"
    
    for row in stg_new_frame:
        pgs_dwh_hook.run(insert_sql.format(row[0], row[1], row[2]))

    print('Stored to dwh_dim_species - ', len(stg_new_frame), 'rows.')

    # Create fact table - dwh_fact_observation
    stg_new_frame = pgs_stg_hook.get_records(f"SELECT subid, speciescode, locid, obsdt, howmany \
                                                FROM stg_fact_observation \
                                                WHERE obsdt > '{high_water_mark}'")
    
    insert_sql = "INSERT INTO dwh_fact_observation VALUES(%s, %s, %s, %s, %s)"
    for row in stg_new_frame:
        pgs_dwh_hook.run(insert_sql, parameters = (row[0], row[1], row[2], row[3], row[4]))
    
    print('Stored to dwh_fact_observation - ', len(stg_new_frame), 'rows.')

    # Save current high_water_mark
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT MAX(obsdt) FROM dwh_fact_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'dwh_fact_observation'",
                     parameters = (tmp_water_mark[0][0],))
