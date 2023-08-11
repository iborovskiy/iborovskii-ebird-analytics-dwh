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
import assignment_dwh.etl_logging as etl_log

# Initialize configuration parameters
api_key = Variable.get("EBIRD_API_KEY", default_var=None)           # API Key
var_tmp = Variable.get("EBIRD_USE_SPARK", default_var='false').lower()
USE_SPARK = False if var_tmp == 'false' else True                   # Work mode - Spark / Local pandas df
var_tmp = Variable.get("EBIRD_DWH_INTERNAL_MODEL", default_var='false').lower()
DWH_INTERNAL_MODEL = False if var_tmp == 'false' else True          # Model creation mode
locale = Variable.get("EBIRD_LOCALE", default_var='ru')             # Language for common name
days_back = Variable.get("EBIRD_DAYS_BACK", default_var='30')       # How many days back to fetch
regionCode = Variable.get("EBIRD_REGION_CODE", default_var='GE')    # Geographic location for analysis
home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')      # Temporary storage location


# Requests strings
url = f'https://api.ebird.org/v2/data/obs/{regionCode}/recent?sppLocale={locale}&back={days_back}'
url_locs = f'https://api.ebird.org/v2/ref/hotspot/{regionCode}?back={days_back}&fmt=json'
url_countries = f'https://api.ebird.org/v2/ref/region/list/country/world'
url_sub_regions = f'https://api.ebird.org/v2/ref/region/list/subnational1/{regionCode}'
url_taxonomy = f'https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json&locale={locale}'


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

    # Exporting loaded dictionaries into csv
    df_loc['latestObsDt'] = pd.to_datetime(df_loc['latestObsDt'])
    new_rows = df_loc[['locId', 'locName', 'countryCode', 'subnational1Code', 'lat', 'lng', 'latestObsDt', 'numSpeciesAllTime']]
    new_rows.to_csv(home_dir + '/locations.csv', index = False)

    new_rows = df_countries[['code', 'name']]
    new_rows.to_csv(home_dir + '/countries.csv', index = False)

    new_rows = df_subregions[['code', 'name']]
    new_rows.to_csv(home_dir + '/subregions.csv', index = False)

    new_rows = df_taxonomy[['speciesCode', 'sciName', 'comName', 'category', 'order', 'familyCode', 'familySciName']]
    new_rows.to_csv(home_dir + '/taxonomy.csv', index = False)

    # Loading dictionaries in MRR database
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    if DWH_INTERNAL_MODEL == False:
        # If property set to TRUE then use stored procedure from MRR db (loading as a single transaction)
        print("Use external load mode of MRR")

        # Clear previous dictionaries, load full new update from ebird (not incremental)
        pgs_mrr_hook.run('DELETE FROM mrr_fact_locations')
        pgs_mrr_hook.run('DELETE FROM mrr_fact_countries')
        pgs_mrr_hook.run('DELETE FROM mrr_fact_subnational')
        pgs_mrr_hook.run('DELETE FROM mrr_fact_taxonomy')

        insert_loc_sql = f"""
                            COPY mrr_fact_locations(locid, locname, countrycode, subnational1Code, lat, lon, latestObsDt, numSpeciesAllTime)
                            FROM '{home_dir}/locations.csv'
                            DELIMITER ','
                            CSV HEADER
                        """
        pgs_mrr_hook.run(insert_loc_sql)
        insert_countries_sql = f"""
                            COPY mrr_fact_countries(countrycode, countryname)
                            FROM '{home_dir}/countries.csv'
                            DELIMITER ','
                            CSV HEADER
                        """
        pgs_mrr_hook.run(insert_countries_sql)
        insert_subregions_sql = f"""
                            COPY mrr_fact_subnational(subnationalCode, subnationalName)
                            FROM '{home_dir}/subregions.csv'
                            DELIMITER ','
                            CSV HEADER
                        """
        pgs_mrr_hook.run(insert_subregions_sql)
        insert_taxonomy_sql = f"""
                            COPY mrr_fact_taxonomy(speciesCode, sciName, comName, category, orderSciName, familyCode, familySciName)
                            FROM '{home_dir}/taxonomy.csv'
                            DELIMITER ','
                            CSV HEADER
                        """
        pgs_mrr_hook.run(insert_taxonomy_sql)
    else:
        # Use stored procedure from MRR db (loading as a single transaction)
        print("Use internal load mode of MRR")
        pgs_mrr_hook.run(f"CALL mrr_process_dictionaries('{home_dir}', 'locations.csv', 'countries.csv', 'subregions.csv', 'taxonomy.csv')")

    # Remove temporary csv files
    os.remove(home_dir + '/locations.csv')
    os.remove(home_dir + '/countries.csv')
    os.remove(home_dir + '/subregions.csv')
    os.remove(home_dir + '/taxonomy.csv')

    # Close the connection to MRR db
    pgs_mrr_hook.conn.close()


def ingest_new_rows_from_csv(*args, **kwargs):
    # Make request
    r = requests.get(url, headers = {'X-eBirdApiToken' : api_key})
    # Check status
    if r.status_code != 200:
        raise ValueError('Bad response from e-bird')
    
    # Load results from JSON to pandas df
    df = pd.DataFrame(r.json())
    print('Extracted obeservations from ebird source - ', len(df), 'rows.')

    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")


    if USE_SPARK:
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
        new_rows = spark.sql(f"""
                                SELECT speciescode, sciname, locid, locname, obsdt, howmany, 
                                    lat, lng, obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory
                                FROM tmp_ebird_recent
                                WHERE obsdt > '{high_water_mark}'
                         """).collect()

        new_rows.write.csv(home_dir + '/observations.csv')

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

        new_rows.to_csv(home_dir + '/observations.csv', index = False)

    print('Loaded observations from ebird source - ', len(new_rows), 'rows.')

    # Import prepared csv file into the MRR database (through temporary table)
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    if DWH_INTERNAL_MODEL == False:
        # If property set to TRUE then use stored procedure from MRR db (loading as a single transaction)
        insert_tmp_sql = f"""
                            COPY mrr_fact_recent_observation_tmp(speciescode, sciname, locid, locname, obsdt, howmany, lat, lon,
                                    obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory)
                            FROM '{home_dir}/observations.csv'
                            DELIMITER ','
                            CSV HEADER
                        """
        pgs_mrr_hook.run(insert_tmp_sql)
        insert_sql = """
                    INSERT INTO mrr_fact_recent_observation
                    SELECT *
                    FROM mrr_fact_recent_observation_tmp
                    ON CONFLICT(speciesCode, subId) DO NOTHING
                """
        pgs_mrr_hook.run(insert_sql)
        pgs_mrr_hook.run('DELETE FROM mrr_fact_recent_observation_tmp')

    else:
        # Use stored procedure from MRR db (loading as a single transaction)
        pgs_mrr_hook.run(f"CALL mrr_process_new_observations('{home_dir}', 'observations.csv')")

    # Remove temporary csv files
    os.remove(home_dir + '/observations.csv')

    # Make log record on success
    etl_log.log_msg(datetime.datetime.now(), etl_log.INFO_MSG, f"{len(new_rows)} new observation rows ingested from e-bird source, DAG run at {kwargs['ts']}.")

    # Close the connection to MRR db
    pgs_mrr_hook.conn.close()


def load_mrr_from_ebird(*args, **kwargs):
        load_mrr_dictionaries_from_ebird(*args, **kwargs)
        return ingest_new_rows_from_csv(*args, **kwargs)



