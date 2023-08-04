# Import libraries
import pandas as pd
import numpy as np
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from airflow.hooks.postgres_hook import PostgresHook

import requests
import datetime

import assignment_dwh.etl_logging as etl_log

# API Key
api_key = '' # Use your eBird API key here!

# Select work mode - Spark / Local pandas df
USE_SPARK = False

# Set params
lat = '41.64357186598966' # Latitude for analysis (Batumi)
lon = '41.628441391459546' # Longitude for analysis (Batumi)
search_distance = '25' # The search radius in km
locale = 'ru' # Language for common name
days_back = '30' # How many days back to fetch

# Requests
#url = f'https://api.ebird.org/v2/data/obs/geo/recent?lat={lat}&lng={lon}&sppLocale={locale}&back={days_back}&dist={search_distance}'
url = f'https://api.ebird.org/v2/data/obs/GE/recent?sppLocale={locale}&back={days_back}'

def load_mrr_from_ebird_spark(*args, **kwargs):
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
                                    lat, lng, obsvalid, obsreviewed, locationprivate, subid, exoticcategory, comname
                            FROM tmp_ebird_recent
                            WHERE obsdt > '{high_water_mark}'
                         """).collect()
    insert_sql = "INSERT INTO mrr_fact_recent_observation VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for row in new_rows:
        pgs_mrr_hook.run(insert_sql, parameters = (row[0], row[1], row[2], row[3], row[4], float(row[5]),
                                                    row[6], row[7], row[8], row[9], row[10], row[11], row[12] if row[12] != 'NaN' else '', row[13]))
    
    print('Loaded in mrr - ', len(new_rows), 'rows.')
    etl_log.log_msg(kwargs['ts'], etl_log.INFO_MSG, f"{len(new_rows)} new rows ingested from e-bird source.")

    # Save current high_water_mark
    tmp_water_mark = pgs_mrr_hook.get_records("SELECT MAX(obsdt) FROM mrr_fact_recent_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'mrr_fact_recent_observation'", parameters = (tmp_water_mark[0][0],))

    spark.stop()


def load_mrr_from_ebird_pandas(*args, **kwargs):
    # Make request
    r = requests.get(url, headers = {'X-eBirdApiToken' : api_key})
    # Check status
    if r.status_code != 200:
        raise ValueError('Bad response from e-bird')
    
    # Load results from JSON to pandas df
    df = pd.DataFrame(r.json())
    print('Extracted from ebird source - ', len(df), 'rows.')
    
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

    # Filter out old records using high_water_mark
    df['obsDt'] = pd.to_datetime(df['obsDt'])
    df = df[df['obsDt'] > high_water_mark]
    new_rows = df[['speciesCode', 'sciName', 'locId', 'locName', 'obsDt', 'howMany', 'lat', 'lng',
                    'obsValid', 'obsReviewed', 'locationPrivate', 'subId', 'comName']]
    if 'exoticCategory' in df.columns:
        new_rows['exoticCategory'] = df['exoticCategory']
    else:
        new_rows['exoticCategory'] = None

    # Load source mirror to mrr db 
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    insert_sql = "INSERT INTO mrr_fact_recent_observation VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    for row in new_rows.values:
        pgs_mrr_hook.run(insert_sql, parameters = (row[0], row[1], row[2], row[3], row[4], float(row[5]),
                                                    row[6], row[7], row[8], row[9], row[10], row[11], '', row[12]))
    
    print('Loaded in mrr - ', len(new_rows), 'rows.')
    etl_log.log_msg(kwargs['ts'], etl_log.INFO_MSG, f"{len(new_rows)} new rows ingested from e-bird source.")

    # Save current high_water_mark
    tmp_water_mark = pgs_mrr_hook.get_records("SELECT MAX(obsdt) FROM mrr_fact_recent_observation")
    pgs_dwh_hook.run("UPDATE high_water_mark SET current_high_ts = %s WHERE table_id = 'mrr_fact_recent_observation'", parameters = (tmp_water_mark[0][0],))



def load_mrr_from_ebird(*args, **kwargs):
    if USE_SPARK:
        return load_mrr_from_ebird_spark(*args, **kwargs)
    else:
        return load_mrr_from_ebird_pandas(*args, **kwargs)