# Import libraries
import pandas as pd
import numpy as np
import os


from airflow.hooks.postgres_hook import PostgresHook

# Select model creation mode
DWH_INTERNAL_MODEL = False

home_dir = '/home/iborovskii'

def load_dwh_from_stg_inside(*args, **kwargs):
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
        pgs_dwh_hook.run(f"CALL process_new_row('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', '{row[5] if row[5] else 'NaN'}', {row[6]}, {row[7]}, '{row[8]}', '{row[9]}')")
    
    print('Stored to dwh_fact_observation - ', len(stg_new_frame), 'rows.')
    pgs_dwh_hook.conn.close()
    pgs_stg_hook.conn.close()


def load_dwh_from_stg_outside(*args, **kwargs):
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


    # Create dimension table - dwh_dim_location

    select_sql = f"""SELECT DISTINCT o.locid, o.locname, o.lat, o.lon, l.countryname,
                                l.subRegionName, l.latestObsDt, l.numSpeciesAllTime
                    FROM stg_fact_observation o
                    LEFT JOIN stg_fact_locations l
                    ON o.locid = l.locid    
                """
    stg_new_frame = pd.DataFrame(pgs_stg_hook.get_records(select_sql), columns = ['locId', 'locName', 'countryName', 
                                                                                  'subRegionName', 'lat', 'lon', 'latestObsDt', 'numSpeciesAllTime'])
    stg_new_frame.to_csv(home_dir + '/locations_dwh.csv', index = False)
    pgs_dwh_hook.run("DELETE FROM dwh_dim_location")
    insert_sql = f"COPY dwh_dim_location \
                            FROM '{home_dir}/locations_dwh.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_dwh_hook.run(insert_sql)
    os.remove(home_dir + '/locations_dwh.csv')


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

    # Save current high_water_mark
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT MAX(obsdt) FROM dwh_fact_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'dwh_fact_observation'",
                     parameters = (tmp_water_mark[0][0],))
    pgs_dwh_hook.conn.close()
    pgs_stg_hook.conn.close()


def load_dwh_from_stg(*args, **kwargs):
    if DWH_INTERNAL_MODEL:
        return load_dwh_from_stg_inside(*args, **kwargs)
    else:
        return load_dwh_from_stg_outside(*args, **kwargs)