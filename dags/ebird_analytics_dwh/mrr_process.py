# General libraries imports
import pandas as pd
import numpy as np
import datetime
import os

# Spark imports
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# External API imports
import requests

# DAG imports
# DAG access connectors imports
from airflow.hooks.postgres_hook import PostgresHook
# DAG utils imports
from airflow.models import Variable

# Local modules imports
import ebird_analytics_dwh.etl_logging as etl_log
import ebird_analytics_dwh.configs as sq


def load_mrr_dictionaries_from_ebird(*args, **kwargs):
    # Make request
    r_loc = requests.get(sq.url_locs, headers = {'X-eBirdApiToken' : sq.api_key})
    r_countries = requests.get(sq.url_countries, headers = {'X-eBirdApiToken' : sq.api_key})
    r_subregions = requests.get(sq.url_sub_regions, headers = {'X-eBirdApiToken' : sq.api_key})
    r_taxonomy = requests.get(sq.url_taxonomy, headers = {'X-eBirdApiToken' : sq.api_key})
    
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

    # Exporting loaded dictionaries into csv
    df_loc['latestObsDt'] = pd.to_datetime(df_loc['latestObsDt'])
    new_rows = df_loc[['locId', 'locName', 'countryCode', 'subnational1Code', 'lat', 'lng', 'latestObsDt', 'numSpeciesAllTime']]
    new_rows.to_csv(sq.home_dir + '/locations.csv', index = False)

    new_rows = df_countries[['code', 'name']]
    new_rows.to_csv(sq.home_dir + '/countries.csv', index = False)

    new_rows = df_subregions[['code', 'name']]
    new_rows.to_csv(sq.home_dir + '/subregions.csv', index = False)

    new_rows = df_taxonomy[['speciesCode', 'sciName', 'comName', 'category', 'order', 'familyCode', 'familySciName']]
    new_rows.to_csv(sq.home_dir + '/taxonomy.csv', index = False)

    # Loading dictionaries in MRR database
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    if sq.DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of MRR")
        sql_q = sq.ingest_dict_sql
    else:
        # Use stored procedure from MRR db (single transaction)
        print("Use internal load mode of MRR")
        sql_q = sq.ingest_dict_proc
    pgs_mrr_hook.run(sql_q)

    # Remove temporary csv files
    os.remove(sq.home_dir + '/locations.csv')
    os.remove(sq.home_dir + '/countries.csv')
    os.remove(sq.home_dir + '/subregions.csv')
    os.remove(sq.home_dir + '/taxonomy.csv')

    # Close the connection to MRR db
    pgs_mrr_hook.conn.close()


def ingest_new_rows_from_csv(*args, **kwargs):
    # Make request
    r = requests.get(sq.url, headers = {'X-eBirdApiToken' : sq.api_key})
    # Check status
    if r.status_code != 200:
        raise ValueError('Bad response from e-bird')
    
    # Load results from JSON to pandas df
    df = pd.DataFrame(r.json())
    print('Extracted obeservations from ebird source - ', len(df), 'rows.')

    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")


    if sq.USE_SPARK:
        # Use Spark to transform source dataset

        # Create Spark context and session
        findspark.init()
        sc = SparkContext()
        spark = SparkSession.builder\
            .config("spark.driver.memory", "0.5G")\
            .config("spark.driver.maxResultSize", "0.5G")\
            .appName("ETL Spark process").getOrCreate()
        spark.sparkContext.setSystemProperty('spark.executor.memory', '0.5G')
        # Transform pandas df to Spark df and export ingested rows into csv file
        sdf = spark.createDataFrame(df) 
        sdf.createTempView("tmp_ebird_recent")
        new_rows = spark.sql(sq.spark_sql_req.format(high_water_mark)).collect()

        new_rows.write.csv(sq.home_dir + '/observations.csv')

    else:
        # Use pandas df to transform source dataset

        # Filter out old records using high_water_mark and export ingested rows into csv file
        df['obsDt'] = pd.to_datetime(df['obsDt'])
        df = df[df['obsDt'] > high_water_mark]
        new_rows = df[['speciesCode', 'sciName', 'locId', 'locName', 'obsDt', 'howMany', 'lat', 'lng',
                        'obsValid', 'obsReviewed', 'locationPrivate', 'subId', 'comName']]
        if 'exoticCategory' in df.columns:
            new_rows['exoticCategory'] = df['exoticCategory']
        else:
            new_rows['exoticCategory'] = None

        new_rows.to_csv(sq.home_dir + '/observations.csv', index = False)

    print('Loaded observations from ebird source - ', len(new_rows), 'rows.')

    # Import prepared csv file into the MRR database (through temporary table)
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    if sq.DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of MRR")
        sql_q = sq.ingest_observations_sql
    else:
        # Use stored procedure from MRR db (single transaction)
        print("Use internal load mode of MRR")
        sql_q = sq.ingest_observations_proc
    pgs_mrr_hook.run(sql_q)

    # Remove temporary csv files
    os.remove(sq.home_dir + '/observations.csv')

    # Make log record on success
    etl_log.log_msg(datetime.datetime.now(), etl_log.INFO_MSG, f"{len(new_rows)} new observation rows ingested from e-bird source, DAG run at {kwargs['ts']}.")

    # Close the connection to MRR db
    pgs_mrr_hook.conn.close()


def load_mrr_from_ebird(*args, **kwargs):
        load_mrr_dictionaries_from_ebird(*args, **kwargs)
        return ingest_new_rows_from_csv(*args, **kwargs)



