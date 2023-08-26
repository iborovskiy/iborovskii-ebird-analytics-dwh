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
    # Get DAG variables values
    locale = Variable.get("EBIRD_LOCALE", default_var='ru')                 # Language for common name
    days_back = Variable.get("EBIRD_DAYS_BACK", default_var='30')           # How many days back to fetch
    regionCode = Variable.get("EBIRD_REGION_CODE", default_var='GE')        # Geographic location for analysis
    DWH_INTERNAL_MODEL = False if Variable.get("EBIRD_DWH_INTERNAL_MODEL", 
            default_var='false').lower() == 'false' else True               # Model creation mode
    api_key = Variable.get("EBIRD_API_KEY", default_var=None)               # API Key
    home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')          # Temporary working directory

    # Make request
    r_loc = requests.get(sq.url_locs.format(regionCode, days_back), headers = {'X-eBirdApiToken' : api_key})
    r_countries = requests.get(sq.url_countries, headers = {'X-eBirdApiToken' : api_key})
    r_subregions = requests.get(sq.url_sub_regions.format(regionCode), headers = {'X-eBirdApiToken' : api_key})
    r_taxonomy = requests.get(sq.url_taxonomy.format(locale), headers = {'X-eBirdApiToken' : api_key})
    
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
    new_rows.to_csv(home_dir + '/mrr_hotspots.csv', index = False)

    new_rows = df_countries[['code', 'name']]
    new_rows.to_csv(home_dir + '/mrr_countries.csv', index = False)

    new_rows = df_subregions[['code', 'name']]
    new_rows.to_csv(home_dir + '/mrr_subregions.csv', index = False)

    new_rows = df_taxonomy[['speciesCode', 'sciName', 'comName', 'category', 'order', 'familyCode', 'familySciName']]
    new_rows.to_csv(home_dir + '/mrr_taxonomy.csv', index = False)

    # Loading dictionaries in MRR database
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")

    if DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of MRR")
        sql_q = sq.ingest_dict_sql.format(home_dir)
    else:
        # Use stored procedure from MRR db (single transaction)
        print("Use internal load mode of MRR")
        sql_q = sq.ingest_dict_proc.format(home_dir)
    pgs_mrr_hook.run(sql_q)

    # Remove temporary csv files
    os.remove(home_dir + '/mrr_hotspots.csv')
    os.remove(home_dir + '/mrr_countries.csv')
    os.remove(home_dir + '/mrr_subregions.csv')
    os.remove(home_dir + '/mrr_taxonomy.csv')

    # Close the connection to MRR db
    pgs_mrr_hook.conn.close()


def ingest_new_rows_from_csv(*args, **kwargs):
    # Get DAG variables values
    regionCode = Variable.get("EBIRD_REGION_CODE", default_var='GE')            # Geographic location for analysis
    USE_SPARK = False if Variable.get("EBIRD_USE_SPARK",
                default_var='false').lower() == 'false' else True               # Work mode - Spark / Local pandas df
    DWH_INTERNAL_MODEL = False if Variable.get("EBIRD_DWH_INTERNAL_MODEL", 
                default_var='false').lower() == 'false' else True               # Model creation mode
    api_key = Variable.get("EBIRD_API_KEY", default_var=None)                   # API Key
    meteostat_api_key = Variable.get("EBIRD_WEATHER_API_KEY", default_var=None) # Weather API Key
    home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')              # Temporary working directory
    
    # Load current high_water_mark
    high_water_mark = etl_log.load_high_water_mark()
    print(f"high_water_mark = {high_water_mark}")

    # Ingest new checklists and locations
    loc_df = pd.DataFrame(columns=['locId', 'name', 'latitude', 'longitude', 'countryCode', 'countryName',
                                    'subnational1Name', 'subnational1Code', 'isHotspot', 'locName',
                                    'lat', 'lng', 'hierarchicalName', 'locID'])
    obs_df = pd.DataFrame(columns = ['locId', 'subId', 'userDisplayName', 'numSpecies', 'obsDt', 'obsTime', 'subID'])
    #for dt in pd.date_range(high_water_mark, datetime.datetime.now() - datetime.timedelta(days=1),freq='d'):
    for dt in pd.date_range(high_water_mark, datetime.datetime.now(),freq='d'):
        # Make request for checklists for given day
        r = requests.get(sq.url_cl_ist.format(regionCode, dt.year, dt.month, dt.day), headers = {'X-eBirdApiToken' : api_key})
        # Check status
        if r.status_code == 200:
            print('Request completed successfully!')
        else:
            print(f'Error! Code - {r.status_code}')
            raise ValueError('Bad response from e-bird')
        
        # Load results from JSON to pandas df
        df = pd.DataFrame(r.json())
        # Combine all results in shared dateframes - one for locations and one for observations
        loc_df = pd.concat([loc_df, df['loc'].apply(pd.Series)])
        obs_df = pd.concat([obs_df, df[['locId', 'subId', 'userDisplayName', 'numSpecies', 'obsDt', 'obsTime', 'subID']]])

    
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
        s_obs_df = spark.createDataFrame(obs_df)
        s_loc_df = spark.createDataFrame(loc_df)
        s_obs_df.createTempView("tmp_ebird_checklists")
        s_loc_df.createTempView("tmp_ebird_locations")
        new_rows = spark.sql(sq.spark_sql_req.format(high_water_mark)).collect()
        print('Deprecated and removed! There is no demand for Spark distributed data processing for such a small workload.')
        raise ValueError("This mode isn't supported anymore!")
        # REMOVED!

    else:
        # Process new location records
        loc_df = loc_df.drop_duplicates()
        loc_df = loc_df.drop(labels=['name', 'latitude', 'longitude', 'locID'], axis=1)
        loc_df['obsFullDt'] = datetime.datetime.now()

        # Process new checklist records
        obs_df = obs_df.drop_duplicates()
        obs_df['obsTime'] = obs_df['obsTime'].astype(str)
        obs_df.loc[obs_df['obsTime'] == 'nan', 'obsTime'] = '00:00'
        obs_df['obsFullDt'] = pd.to_datetime(obs_df['obsDt'] + ' ' + obs_df['obsTime'])
        obs_df = obs_df.drop(labels=['obsDt', 'obsTime', 'subID'], axis=1)
        obs_df = obs_df[obs_df['obsFullDt'] > high_water_mark]

        print('Extracted new checklists from ebird source - ', len(obs_df), 'rows.')

        # Process new observation records
        obs_details_df = pd.DataFrame(columns=['speciesCode', 'obsDt', 'subId', 'projId', 'obsId', 
                                               'howManyStr', 'present'])
        for sub_id in obs_df['subId']:
            # Make request
            r = requests.get(sq.url_get_cl.format(sub_id), headers = {'X-eBirdApiToken' : api_key})
            # Check status
            if r.status_code == 200:
                print('Request completed successfully!')
            else:
                print(f'Error! Code - {r.status_code}')
                raise ValueError('Bad response from e-bird')
            df = pd.DataFrame(r.json()['obs'])
            obs_details_df = pd.concat([obs_details_df, df[['speciesCode', 'obsDt', 'subId', 'projId', 'obsId', 'howManyStr', 'present']]])
    
        obs_details_df.loc[obs_details_df['howManyStr'] == 'X', 'howManyStr'] = '0'
        obs_details_df['howManyStr'].astype(int)

        print('Extracted new observstions from ebird source - ', len(obs_details_df), 'rows.')

        # Exporting new ingested records to CSV files
        loc_df.to_csv(home_dir + '/mrr_location.csv', index = False)
        obs_df.to_csv(home_dir + '/mrr_checklist.csv', index = False)
        obs_details_df.to_csv(home_dir + '/mrr_observation.csv', index=False)

    # Import prepared csv file into the MRR database (through temporary table)
    pgs_mrr_hook = PostgresHook(postgres_conn_id="postgres_mrr_conn")
    

    if DWH_INTERNAL_MODEL == False:
        # Use several SQL queries in single transaction
        print("Use external load mode of MRR")
        sql_q = sq.ingest_checklists_sql.format(home_dir)
    else:
        # Use stored procedure from MRR db (single transaction)
        print("Use internal load mode of MRR")
        sql_q = sq.ingest_observations_proc.format(home_dir)
    pgs_mrr_hook.run(sql_q)
    

    # Remove temporary csv files
    os.remove(home_dir + '/mrr_location.csv')
    os.remove(home_dir + '/mrr_checklist.csv')
    os.remove(home_dir + '/mrr_observation.csv')

    # Load weather conditions for all locations of new observations
    # Update interval is set to 10 days and applied algorithm for reduction of number of API calls (for API billing purposes)
    weather_high_water_mark = etl_log.load_high_water_mark('mrr_fact_weather_observations')
    if datetime.datetime.now() - weather_high_water_mark > datetime.timedelta(days=31):
        print('Time to update weather')
        pgs_dwh_hook = PostgresHook(postgres_conn_id="postgres_dwh_conn")
        new_weather_rows = pgs_dwh_hook.get_records(sq.get_weather_sql.format(high_water_mark))
        cur_loc_id = None
        loc_no_data = False
        for row in new_weather_rows:
            if loc_no_data and row[0] == cur_loc_id:
                print(f'No previous date for {cur_loc_id=}, skip all subsequent requests.')
                continue
            loc_no_data = False
            # Make a request to the Meteostat JSON API
            r = requests.get(sq.url_get_weather.format(row[1], row[2], row[3], row[3]), headers = {'x-rapidapi-key' : meteostat_api_key})
            if r.status_code != 200:
                print(f"Can't fetch weather data for station_id - {row[0]}, {row[1], row[2]}. Status - {r.status_code}")
                continue
            df = pd.DataFrame(r.json()['data'])
            print(f'Fetched rows - {len(df)}')
            if len(df) == 1 and not np.isnan(df.loc[0, 'tavg']):
                print(f'Loaded data for loc_id - {row[0]}, {row[1]}, {row[2]}, date - {row[3]}')
                pgs_mrr_hook.run(sq.insert_weather_observations_sql,
                             parameters = (row[0], df.loc[0, 'date'], df.loc[0, 'tavg'], df.loc[0, 'tmin'], df.loc[0, 'tmax'],
                                 df.loc[0, 'prcp'], df.loc[0, 'snow'], df.loc[0, 'wdir'], df.loc[0, 'wspd'],
                                 df.loc[0, 'wpgt'], df.loc[0, 'pres'], df.loc[0, 'tsun']))
            else:
                print(f'No data for loc_id - {row[0]}, {row[1]}, {row[2]}, date - {row[3]}. Set flag to skip all subsequent dates!')
                cur_loc_id = row[0]
                loc_no_data = True
        print(f'{len(new_weather_rows)} rows added to weather conditions.')
        pgs_dwh_hook.conn.close()
    else:
        print(f'Skip weather update. {weather_high_water_mark=}, Current time - {datetime.datetime.now()}.')

    # Close the connection to MRR db
    pgs_mrr_hook.conn.close()

    # Make log record on success
    etl_log.log_msg(datetime.datetime.now(), etl_log.INFO_MSG,
                    f"{len(obs_df)} new checklists and observations ingested from e-bird source, DAG run at {kwargs['ts']}.")


def load_mrr_from_ebird(*args, **kwargs):
        load_mrr_dictionaries_from_ebird(*args, **kwargs)
        return ingest_new_rows_from_csv(*args, **kwargs)

