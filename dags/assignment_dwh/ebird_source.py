# Import libraries
import pandas as pd
import numpy as np
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from airflow.hooks.postgres_hook import PostgresHook

import requests
import datetime
import os

import assignment_dwh.etl_logging as etl_log

# API Key
api_key = '8r8719eq7h6u'

# Select work mode - Spark / Local pandas df
USE_SPARK = False

# Set params
lat = '41.64357186598966' # Latitude for analysis (Batumi)
lon = '41.628441391459546' # Longitude for analysis (Batumi)
search_distance = '25' # The search radius in km
locale = 'ru' # Language for common name
days_back = '30' # How many days back to fetch
regionCode = 'GE'
home_dir = '/home/iborovskii'


# Requests
#url = f'https://api.ebird.org/v2/data/obs/geo/recent?lat={lat}&lng={lon}&sppLocale={locale}&back={days_back}&dist={search_distance}'
url = f'https://api.ebird.org/v2/data/obs/{regionCode}/recent?sppLocale={locale}&back={days_back}'
url_locs = f'https://api.ebird.org/v2/ref/hotspot/{regionCode}?back={days_back}&fmt=json'
url_countries = f'https://api.ebird.org/v2/ref/region/list/country/world'
url_sub_regions = f'https://api.ebird.org/v2/ref/region/list/subnational1/{regionCode}'
url_taxonomy = f'https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json&locale={locale}'

def load_mrr_from_ebird_spark(*args, **kwargs):
    findspark.init()
    # Make request
    r = requests.get(url, headers = {'X-eBirdApiToken' : api_key})
    # Check status
    if r.status_code != 200:
        raise ValueError('Bad response from e-bird')
    
    # Load results from JSON to pandas df
    df = pd.DataFrame(r.json())
    print('Extracted from ebird source - ', len(df), 'rows.')
    
    # Create Spark context and session
    sc = SparkContext()
    spark = SparkSession.builder\
        .config("spark.driver.memory", "0.5G")\
        .config("spark.driver.maxResultSize", "0.5G")\
        .appName("ETL Spark process").getOrCreate()
    spark.sparkContext.setSystemProperty('spark.executor.memory', '0.5G')

    # Load current high_water_mark
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT current_high_ts FROM high_water_mark WHERE table_id = 'mrr_fact_recent_observation'")
    if len(tmp_water_mark) < 1 or not tmp_water_mark[0][0]:
        high_water_mark = '2020-01-21 16:35'
        pgs_dwh_hook.run("INSERT INTO high_water_mark VALUES(%s, %s) ON CONFLICT (table_id) DO UPDATE \
                            SET current_high_ts = EXCLUDED.current_high_ts", parameters = ('mrr_fact_recent_observation', high_water_mark))

    else:
        high_water_mark = tmp_water_mark[0][0]
    print(f"high_water_mark = {high_water_mark}")

    # Transform pandas df to Spark df
    sdf = spark.createDataFrame(df) 
    sdf.createTempView("tmp_ebird_recent")

    # Load source mirror to mrr db 
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    new_rows = spark.sql(f"""SELECT speciescode, sciname, locid, locname, obsdt, howmany, 
                                    lat, lng, obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory
                            FROM tmp_ebird_recent
                            WHERE obsdt > '{high_water_mark}'
                         """).collect()
    
    # Exprort ingested rows to csv tmp file and load it into tmp table in MRR database
    new_rows.write.csv(home_dir + '/observations.csv')
    insert_tmp_sql = f"COPY mrr_fact_recent_observation_tmp(speciescode, sciname, locid, locname, obsdt, howmany, lat, lon, \
                            obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory) \
                            FROM '{home_dir}/observations.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_mrr_hook.run(insert_tmp_sql)
    os.remove(home_dir + '/observations.csv')

    # Load source rows to mrr db 
    insert_sql = "INSERT INTO mrr_fact_recent_observation \
                    SELECT * \
                    FROM mrr_fact_recent_observation_tmp \
                    ON CONFLICT(speciesCode, subId) DO NOTHING"
    pgs_mrr_hook.run(insert_sql)

    pgs_mrr_hook.run('DELETE FROM mrr_fact_recent_observation_tmp')
        
    print('Loaded in mrr - ', len(new_rows), 'rows.')
    etl_log.log_msg(datetime.datetime.now(), etl_log.INFO_MSG, f"{len(new_rows)} new observation rows ingested from e-bird source, DAG run at {kwargs['ts']}.")

    # Save current high_water_mark
    tmp_water_mark = pgs_mrr_hook.get_records("SELECT MAX(obsdt) FROM mrr_fact_recent_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'mrr_fact_recent_observation'", parameters = (tmp_water_mark[0][0],))

    spark.stop()
    pgs_mrr_hook.conn.close()
    pgs_dwh_hook.conn.close()



def load_mrr_from_ebird_pandas(*args, **kwargs):
    # Make request
    r = requests.get(url, headers = {'X-eBirdApiToken' : api_key})
    # Check status
    if r.status_code != 200:
        raise ValueError('Bad response from e-bird')
    
    # Load results from JSON to pandas df
    df = pd.DataFrame(r.json())
    print('Extracted obeservations from ebird source - ', len(df), 'rows.')
    
    # Load current high_water_mark
    pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
    tmp_water_mark = pgs_dwh_hook.get_records("SELECT current_high_ts FROM high_water_mark WHERE table_id = 'mrr_fact_recent_observation'")
    if len(tmp_water_mark) < 1 or not tmp_water_mark[0][0]:
        high_water_mark = '2020-01-21 16:35'
        pgs_dwh_hook.run("INSERT INTO high_water_mark VALUES(%s, %s) ON CONFLICT (table_id) DO UPDATE \
                            SET current_high_ts = EXCLUDED.current_high_ts", parameters = ('mrr_fact_recent_observation', high_water_mark))
    else:
        high_water_mark = tmp_water_mark[0][0]
    print(f"mrr_fact_recent_observation high_water_mark = {high_water_mark}")

    # Filter out old records using high_water_mark
    df['obsDt'] = pd.to_datetime(df['obsDt'])
    df = df[df['obsDt'] > high_water_mark]
    new_rows = df[['speciesCode', 'sciName', 'locId', 'locName', 'obsDt', 'howMany', 'lat', 'lng',
                    'obsValid', 'obsReviewed', 'locationPrivate', 'subId', 'comName']]
    if 'exoticCategory' in df.columns:
        new_rows['exoticCategory'] = df['exoticCategory']
    else:
        new_rows['exoticCategory'] = None

    # Exprort ingested rows to csv tmp file and load it into tmp table in MRR database
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    new_rows.to_csv(home_dir + '/observations.csv', index = False)
    insert_tmp_sql = f"COPY mrr_fact_recent_observation_tmp(speciescode, sciname, locid, locname, obsdt, howmany, lat, lon, \
                            obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory) \
                            FROM '{home_dir}/observations.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_mrr_hook.run(insert_tmp_sql)
    os.remove(home_dir + '/observations.csv')

    # Load source rows to mrr db 
    insert_sql = "INSERT INTO mrr_fact_recent_observation \
                    SELECT * \
                    FROM mrr_fact_recent_observation_tmp \
                    ON CONFLICT(speciesCode, subId) DO NOTHING"
    
    pgs_mrr_hook.run(insert_sql)

    pgs_mrr_hook.run('DELETE FROM mrr_fact_recent_observation_tmp')

    print('Loaded observations in mrr - ', len(new_rows), 'rows.')
    etl_log.log_msg(datetime.datetime.now(), etl_log.INFO_MSG, f"{len(new_rows)} new observation rows ingested from e-bird source, DAG run at {kwargs['ts']}.")

    # Save current high_water_mark
    tmp_water_mark = pgs_mrr_hook.get_records("SELECT MAX(obsdt) FROM mrr_fact_recent_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'mrr_fact_recent_observation'", parameters = (tmp_water_mark[0][0],))
    pgs_mrr_hook.conn.close()
    pgs_dwh_hook.conn.close()



def load_mrr_from_ebird(*args, **kwargs):
    if USE_SPARK:
        load_mrr_dictionaries_from_ebird(*args, **kwargs)
        return load_mrr_from_ebird_spark(*args, **kwargs)
    else:
        load_mrr_dictionaries_from_ebird(*args, **kwargs)
        return load_mrr_from_ebird_pandas(*args, **kwargs)

def load_mrr_dictionaries_from_ebird(*args, **kwargs):
    # Make request
    r_loc = requests.get(url_locs, headers = {'X-eBirdApiToken' : api_key})
    r_countries = requests.get(url_countries, headers = {'X-eBirdApiToken' : api_key})
    r_subregions = requests.get(url_sub_regions, headers = {'X-eBirdApiToken' : api_key})
    r_taxonomy = requests.get(url_taxonomy, headers = {'X-eBirdApiToken' : api_key})
    
    if r_taxonomy.status_code != 200 or r_loc.status_code != 200 or r_countries.status_code != 200 \
        or r_subregions.status_code != 200:
        raise ValueError('Bad response from e-bird')

    # Load results from JSON to pandas df
    df_loc = pd.DataFrame(r_loc.json())
    print('Extracted locations from ebird source - ', len(df_loc), 'rows.')
    df_countries = pd.DataFrame(r_countries.json())
    print('Extracted countries from ebird source - ', len(df_countries), 'rows.')
    df_subregions = pd.DataFrame(r_subregions.json())
    print('Extracted subregions from ebird source - ', len(df_subregions), 'rows.')
    df_taxonomy = pd.DataFrame(r_taxonomy.json())
    print('Extracted taxonomy entries from ebird source - ', len(df_taxonomy), 'rows.')
    

    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    # Load locations info
    df_loc['latestObsDt'] = pd.to_datetime(df_loc['latestObsDt'])
    new_rows = df_loc[['locId', 'locName', 'countryCode', 'subnational1Code', 'lat', 'lng', 'latestObsDt', 'numSpeciesAllTime']]
    new_rows.to_csv(home_dir + '/locations.csv', index = False)
    pgs_mrr_hook.run('DELETE FROM mrr_fact_locations')
    insert_loc_sql = f"COPY mrr_fact_locations(locid, locname, countrycode, subnational1Code, lat, lon, latestObsDt, numSpeciesAllTime) \
                            FROM '{home_dir}/locations.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_mrr_hook.run(insert_loc_sql)
    os.remove(home_dir + '/locations.csv')

    print('Loaded locations in mrr - ', len(new_rows), 'rows.')

    # Load countries info
    new_rows = df_countries[['code', 'name']]
    new_rows.to_csv(home_dir + '/countries.csv', index = False)
    pgs_mrr_hook.run('DELETE FROM mrr_fact_countries')
    insert_countries_sql = f"COPY mrr_fact_countries(countrycode, countryname) \
                            FROM '{home_dir}/countries.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_mrr_hook.run(insert_countries_sql)
    os.remove(home_dir + '/countries.csv')

    print('Loaded countries in mrr - ', len(new_rows), 'rows.')

    # Load subregions info
    new_rows = df_subregions[['code', 'name']]
    new_rows.to_csv(home_dir + '/subregions.csv', index = False)
    pgs_mrr_hook.run('DELETE FROM mrr_fact_subnational')
    insert_subregions_sql = f"COPY mrr_fact_subnational(subnationalCode, subnationalName) \
                            FROM '{home_dir}/subregions.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_mrr_hook.run(insert_subregions_sql)
    os.remove(home_dir + '/subregions.csv')

    print('Loaded subregions in mrr - ', len(new_rows), 'rows.')

    # Load taxonomy info
    new_rows = df_taxonomy[['speciesCode', 'sciName', 'comName', 'category', 'order', 'familyCode', 'familySciName']]
    new_rows.to_csv(home_dir + '/taxonomy.csv', index = False)
    pgs_mrr_hook.run('DELETE FROM mrr_fact_taxonomy')
    insert_taxonomy_sql = f"COPY mrr_fact_taxonomy(speciesCode, sciName, comName, category, orderSciName, familyCode, familySciName) \
                            FROM '{home_dir}/taxonomy.csv' \
                            DELIMITER ',' \
                            CSV HEADER"
    pgs_mrr_hook.run(insert_taxonomy_sql)
    os.remove(home_dir + '/taxonomy.csv')

    print('Loaded taxonomy entries in mrr - ', len(new_rows), 'rows.')
    pgs_mrr_hook.conn.close()

