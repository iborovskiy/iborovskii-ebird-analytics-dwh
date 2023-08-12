# DAG imports
# DAG utils imports
from airflow.models import Variable

# Initialize configuration parameters
locale = Variable.get("EBIRD_LOCALE", default_var='ru')                 # Language for common name
days_back = Variable.get("EBIRD_DAYS_BACK", default_var='30')           # How many days back to fetch
regionCode = Variable.get("EBIRD_REGION_CODE", default_var='GE')        # Geographic location for analysis
home_dir = Variable.get("EBIRD_HOME_DIR", default_var='/tmp/')          # Temporary working directory
USE_SPARK = False if Variable.get("EBIRD_USE_SPARK",
            default_var='false').lower() == 'false' else True           # Work mode - Spark / Local pandas df
DWH_INTERNAL_MODEL = False if Variable.get("EBIRD_DWH_INTERNAL_MODEL", 
            default_var='false').lower() == 'false' else True           # Model creation mode
api_key = Variable.get("EBIRD_API_KEY", default_var=None)               # API Key


# API Requests strings
url = f'https://api.ebird.org/v2/data/obs/{regionCode}/recent?sppLocale={locale}&back={days_back}'
url_locs = f'https://api.ebird.org/v2/ref/hotspot/{regionCode}?back={days_back}&fmt=json'
url_countries = f'https://api.ebird.org/v2/ref/region/list/country/world'
url_sub_regions = f'https://api.ebird.org/v2/ref/region/list/subnational1/{regionCode}'
url_taxonomy = f'https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json&locale={locale}'


# -------------------------------------------
# Queries for MRR DB
# -------------------------------------------
# Clear previous dictionaries, load full new dictionaries from ebird (not incremental)
# As sequence of SQL queries in single transaction
ingest_dict_sql = f"""
    BEGIN;

    TRUNCATE TABLE mrr_fact_locations;
    TRUNCATE TABLE mrr_fact_countries;
    RUNCATE TABLE mrr_fact_subnational;
    TRUNCATE TABLE mrr_fact_taxonomy;

    COPY mrr_fact_locations(locid, locname, countrycode, subnational1Code, lat, lon, latestObsDt, numSpeciesAllTime)
    FROM '{home_dir}/locations.csv'
    DELIMITER ','
    CSV HEADER;

    COPY mrr_fact_countries(countrycode, countryname)
    FROM '{home_dir}/countries.csv'
    DELIMITER ','
    CSV HEADER;

    COPY mrr_fact_subnational(subnationalCode, subnationalName)
    FROM '{home_dir}/subregions.csv'
    DELIMITER ','
    CSV HEADER;

    COPY mrr_fact_taxonomy(speciesCode, sciName, comName, category, orderSciName, familyCode, familySciName)
    FROM '{home_dir}/taxonomy.csv'
    DELIMITER ','
    CSV HEADER;

    COMMIT;
"""
# As stored procedure (single transaction)
ingest_dict_proc = f"CALL mrr_process_dictionaries('{home_dir}', 'locations.csv', 'countries.csv', 'subregions.csv', 'taxonomy.csv')"

# Spark SQL ingestion query
spark_sql_req = """
    SELECT speciescode, sciname, locid, locname, obsdt, howmany, 
            lat, lng, obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory
    FROM tmp_ebird_recent
    WHERE obsdt > '{}'
"""

# Import prepared csv file into the MRR database (through temporary table)
# As sequence of SQL queries in single transaction
ingest_observations_sql = f"""
    BEGIN;

    COPY mrr_fact_recent_observation_tmp(speciescode, sciname, locid, locname, obsdt, howmany, lat, lon,
        obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory)
    FROM '{home_dir}/observations.csv'
    DELIMITER ','
    CSV HEADER;

    INSERT INTO mrr_fact_recent_observation
    SELECT *
    FROM mrr_fact_recent_observation_tmp
    ON CONFLICT(speciesCode, subId) DO NOTHING;

    TRUNCATE TABLE mrr_fact_recent_observation_tmp;

    COMMIT;
"""
# As stored procedure (single transaction)
ingest_observations_proc = f"CALL mrr_process_new_observations('{home_dir}', 'observations.csv')"


# -------------------------------------------
# Queries for STG DB
# -------------------------------------------
# Export actual MRR public locations dictionary to CSV files
mrr_dict_locations_to_csv_sql = f"""
    SELECT l.locid locid, l.locname locname, c.countryname countryname, s.subnationalname subregionname, 
            l.lat lat, l.lon lon, l.latestobsdt latestobsdt, l.numspeciesalltime numspeciesalltime
    FROM mrr_fact_locations l
    JOIN mrr_fact_countries c
    ON l.countrycode = c.countrycode
    JOIN mrr_fact_subnational s
    ON l.subnational1code = s.subnationalcode
"""
# Export actual MRR taxonomy dictionary to CSV files
mrr_dict_taxonomy_to_csv_sql = f"""
    SELECT t.speciesCode speciesCode, t.sciName sciName, t.comName comName, t.category category,
            t.orderSciName orderSciName, NULL AS orderComName, t.familyCode familyCode, NULL AS familyComName,
            t.familySciName familySciName
    FROM mrr_fact_taxonomy t
"""

# Store exported dictionaries into STG db
# - Clear STG dictionaries tables from old data
# - Load full new update from exported CSV (not incremental) 
# - and add common names of birds for selected locale
# As sequence of SQL queries in single transaction
import_dict_from_csv_to_stg_sql = f"""
    BEGIN;

    TRUNCATE TABLE stg_fact_locations;
    TRUNCATE TABLE stg_fact_taxonomy;
        
    COPY stg_fact_locations
    FROM '{home_dir}/locations_stg.csv'
    DELIMITER ','
    CSV HEADER;

    COPY stg_fact_taxonomy
    FROM '{home_dir}/taxonomy_stg.csv'
    DELIMITER ','
    CSV HEADER;

    UPDATE stg_fact_taxonomy
    SET orderComName = stg_fact_order_comnames.orderComName,
        familyComName = stg_fact_family_comnames.familyComName
    FROM stg_fact_order_comnames, stg_fact_family_comnames
    WHERE stg_fact_taxonomy.orderSciName = stg_fact_order_comnames.orderSciName
            AND stg_fact_order_comnames.orderLocale = '{locale}'
            AND stg_fact_taxonomy.familyCode = stg_fact_family_comnames.familyCode
            AND stg_fact_family_comnames.familyLocale = '{locale}';
                    
    COMMIT;
"""
# As stored procedure (single transaction)
import_dict_from_csv_to_stg_proc = f"""
    CALL stg_process_dictionaries('{home_dir}', 'locations_stg.csv', 'taxonomy_stg.csv', '{locale}')
"""

# Export trusted new data rows for observations from MRR db to CSV files (incremental using high water mark)
mrr_observations_to_csv_sql = """
    SELECT speciesCode, sciName, locId, locName, obsDt, howMany, lat, lon, subId, comName
    FROM mrr_fact_recent_observation
    WHERE obsValid = TRUE and obsdt > '{}'
"""

# Store exported CSV into STG db
# - Clear STG observations table from old data
# - Store clean date to STG (redy to model DWH)
# As sequence of SQL queries in single transaction
import_observation_from_csv_to_stg_sql = f"""
    BEGIN;

    TRUNCATE TABLE stg_fact_observation;

    COPY stg_fact_observation
    FROM '{home_dir}/observations_stg.csv'
    DELIMITER ','
    CSV HEADER;

    COMMIT;
"""
# As stored procedure (single transaction)
import_observation_from_csv_to_stg_proc = f"""
    CALL stg_process_observations('{home_dir}', 'observations_stg.csv')
"""

# -------------------------------------------
# Queries for DWH DB
# -------------------------------------------
# Export data accumulated in staging area (STG db) into CSV files
# Export new observation accumulated from previous high water mark ts
stg_observations_to_csv_sql = """
    SELECT * FROM stg_fact_observation WHERE obsdt > '{}'
"""
# Export actual bird taxonomy dictionary
stg_dict_taxonomy_to_csv_sql = f"""
    SELECT * FROM stg_fact_taxonomy
"""
# Create actual public locations dictionary
stg_dict_location_to_csv_sql = f"""
    SELECT l.locid, l.locname, l.countryname, l.subRegionName, 
            l.lat, l.lon, l.latestObsDt, l.numSpeciesAllTime
    FROM stg_fact_locations l
"""

# Create actual data model - fact and dimension table for Star Schema
    
# Import csv files with new data sets from staging area:
# - Import new observations into temporary fact table
# - Import new taxonomy dictionary into DWH
# - Import new public locations dictionary into DWH
    
# Create new data model for DWH:
# - Create fact table - dwh_fact_observation (incremental update)
# - Create dimension table - dwh_dim_dt (incremental update)
# - Create dimension table - dwh_dim_location (incremental update)
# - Create dimension table - dwh_dim_species (full update  - temporary solution for easy filling common names)
# - Truncate temporary observation table after successful model's update
# - Update current high_water_mark on success of current DAG

# As sequence of SQL queries in single transaction
dwh_update_model_sql = f"""
    BEGIN;

    COPY dwh_fact_raw_observations_tmp
    FROM '{home_dir}/observations_dwh.csv'
    DELIMITER ','
    CSV HEADER;

    TRUNCATE TABLE dwh_dim_species_details;

    COPY dwh_dim_species_details
    FROM '{home_dir}/taxonomy_dwh.csv'
    DELIMITER ','
    CSV HEADER;

    TRUNCATE TABLE dwh_dim_location_details;

    COPY dwh_dim_location_details
    FROM '{home_dir}/locations_dwh.csv'
    DELIMITER ','
    CSV HEADER;

    INSERT INTO dwh_fact_observation
    SELECT subid, speciescode, locid, obsdt, howmany
    FROM dwh_fact_raw_observations_tmp
    ON CONFLICT DO NOTHING;   

    INSERT INTO dwh_dim_dt
    SELECT DISTINCT obsdt, extract(day from obsdt), extract(month from obsdt),
                    TO_CHAR(obsdt, 'Month'), extract(year from obsdt), extract(quarter from obsdt)
    FROM dwh_fact_raw_observations_tmp
    ON CONFLICT (obsdt) DO NOTHING;

    INSERT INTO dwh_dim_location
    SELECT DISTINCT o.locid, o.locname, o.lat, o.lon, l.countryname,
                    l.subRegionName, l.latestObsDt, l.numSpeciesAllTime
    FROM dwh_fact_raw_observations_tmp o
    LEFT JOIN dwh_dim_location_details l
    ON o.locid = l.locid
    ON CONFLICT (locid) DO UPDATE
    SET locname = EXCLUDED.locname, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
        countryname = EXCLUDED.countryname, subRegionName = EXCLUDED.subRegionName,
        latestObsDt = EXCLUDED.latestObsDt, numSpeciesAllTime = EXCLUDED.numSpeciesAllTime;

    TRUNCATE TABLE dwh_dim_species;

    INSERT INTO dwh_dim_species
    SELECT DISTINCT o.speciescode, d.sciName, d.comName, d.category, d.orderSciName, d.orderComName,
                    d.familyCode, d.familyComName, d.familySciName
    FROM dwh_fact_observation o
    LEFT JOIN dwh_dim_species_details d
    ON o.speciescode = d.speciescode;

    TRUNCATE TABLE dwh_fact_raw_observations_tmp;

	UPDATE high_water_mark
	SET current_high_ts = (SELECT MAX(obsdt) FROM dwh_fact_observation)
	WHERE table_id = 'dwh_fact_observation';
                                                    
    COMMIT;
"""
# As stored procedure (single transaction)
dwh_update_model_proc = f"""
    CALL dwh_process_observations('{home_dir}', 'observations_dwh.csv', 'locations_dwh.csv', 'taxonomy_dwh.csv')
"""