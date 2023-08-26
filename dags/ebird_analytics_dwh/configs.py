# DAG imports
# DAG utils imports
from airflow.models import Variable


# API Requests strings
url = 'https://api.ebird.org/v2/data/obs/{}/recent?sppLocale={}&back={}'
url_cl_ist = 'https://api.ebird.org/v2/product/lists/{}/{}/{}/{}?maxResults=200'
url_get_cl = 'https://api.ebird.org/v2/product/checklist/view/{}'
url_locs = 'https://api.ebird.org/v2/ref/hotspot/{}?back={}&fmt=json'
url_countries = 'https://api.ebird.org/v2/ref/region/list/country/world'
url_sub_regions = 'https://api.ebird.org/v2/ref/region/list/subnational1/{}'
url_taxonomy = 'https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json&locale={}'
url_get_station = 'https://meteostat.p.rapidapi.com/stations/nearby?lat={}&lon={}&limit=1&radius=100000'
url_get_weather = 'https://meteostat.p.rapidapi.com/point/daily?lat={}&lon={}&start={}&end={}&model=false'


# -------------------------------------------
# Queries for MRR DB
# -------------------------------------------
# Clear previous dictionaries, load full new dictionaries from ebird (not incremental)
# As sequence of SQL queries in single transaction
ingest_dict_sql ="""
    BEGIN;

    TRUNCATE TABLE mrr_fact_hotspots;
    TRUNCATE TABLE mrr_fact_countries;
    TRUNCATE TABLE mrr_fact_subnational;
    TRUNCATE TABLE mrr_fact_taxonomy;

    COPY mrr_fact_hotspots(locid, locname, countrycode, subnational1Code, lat, lon, latestObsDt, numSpeciesAllTime)
    FROM '{0}/mrr_hotspots.csv'
    DELIMITER ','
    CSV HEADER;

    COPY mrr_fact_countries(countrycode, countryname)
    FROM '{0}/mrr_countries.csv'
    DELIMITER ','
    CSV HEADER;

    COPY mrr_fact_subnational(subnationalCode, subnationalName)
    FROM '{0}/mrr_subregions.csv'
    DELIMITER ','
    CSV HEADER;

    COPY mrr_fact_taxonomy(speciesCode, sciName, comName, category, orderSciName, familyCode, familySciName)
    FROM '{0}/mrr_taxonomy.csv'
    DELIMITER ','
    CSV HEADER;

    COMMIT;
"""
# As stored procedure (single transaction)
ingest_dict_proc = """
    CALL mrr_process_dictionaries('{0}', 'mrr_hotspots.csv', 'mrr_countries.csv', 
                                    'mrr_subregions.csv', 'mrr_taxonomy.csv')
"""

# Spark SQL ingestion query
spark_sql_req = """
    SELECT locId, subId, userDisplayName, numSpecies, to_date(obsDt + ' ' + obsTime, '') as obsFullDt
    FROM tmp_ebird_checklists
    WHERE obsdt > '{}'
"""

# Import prepared csv file into the MRR database (through temporary table)
# As sequence of SQL queries in single transaction
ingest_checklists_sql = """
    BEGIN;

    COPY mrr_fact_location_tmp(locId, countryCode, countryName, subnational1Name, subnational1Code,
                                isHotspot, locName, lat, lng, hierarchicalName, obsFullDt)
    FROM '{0}/mrr_location.csv'
    DELIMITER ','
    CSV HEADER;

    INSERT INTO mrr_fact_location
    SELECT *
    FROM mrr_fact_location_tmp
    ON CONFLICT (locId) DO UPDATE
    SET countryCode = EXCLUDED.countryCode, countryName = EXCLUDED.countryName,
        subnational1Name = EXCLUDED.subnational1Name,
        subnational1Code = EXCLUDED.subnational1Code,
        isHotspot = EXCLUDED.isHotspot, locName = EXCLUDED.locName,
        lat = EXCLUDED.lat, lng = EXCLUDED.lng,
        hierarchicalName = EXCLUDED.hierarchicalName,
        obsFullDt = EXCLUDED.obsFullDt;
        
    COPY mrr_fact_checklist_tmp(locId, subId, userDisplayName, numSpecies, obsFullDt)
    FROM '{0}/mrr_checklist.csv'
    DELIMITER ','
    CSV HEADER;

    INSERT INTO mrr_fact_checklist
    SELECT *
    FROM mrr_fact_checklist_tmp
    ON CONFLICT (subId) DO UPDATE
    SET locId = EXCLUDED.locId, userDisplayName = EXCLUDED.userDisplayName,
        numSpecies = EXCLUDED.numSpecies,
        obsFullDt = EXCLUDED.obsFullDt;

    COPY mrr_fact_observation_tmp(speciesCode, obsDt, subId, projId, obsId, howMany, present)
    FROM '{0}/mrr_observation.csv'
    DELIMITER ','
    CSV HEADER;

    INSERT INTO mrr_fact_observation
    SELECT *
    FROM mrr_fact_observation_tmp
    ON CONFLICT (obsId) DO UPDATE
    SET speciesCode = EXCLUDED.speciesCode, obsDt = EXCLUDED.obsDt,
        subId = EXCLUDED.subId,
        projId = EXCLUDED.projId,
        howMany = EXCLUDED.howMany,
        present = EXCLUDED.present;

    TRUNCATE TABLE mrr_fact_location_tmp;
    TRUNCATE TABLE mrr_fact_checklist_tmp;
    TRUNCATE TABLE mrr_fact_observation_tmp;

    COMMIT;
"""

# As stored procedure (single transaction)
ingest_observations_proc = """
    CALL mrr_process_new_observations('{0}', 'mrr_location.csv', 'mrr_checklist.csv', 'mrr_observation.csv')
"""

# Load weather conditions for all locations of new observations
get_weather_sql = """
    SELECT DISTINCT o.locid, l.lat, l.lon, CAST(o.obsdt AS DATE) obsdt
    FROM dwh_fact_observation o
    JOIN dwh_dim_location l
    ON o.locid = l.locid
    WHERE o.tavg IS NULL
    ORDER BY o.locid, CAST(o.obsdt AS DATE)
"""

# Insert new record in mrr_fact_weather_observations table
insert_weather_observations_sql = """
    INSERT INTO mrr_fact_weather_observations 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT DO NOTHING
"""

# -------------------------------------------
# Queries for STG DB
# -------------------------------------------
# Export actual MRR public locations dictionary to CSV files
mrr_dict_locations_to_csv_sql = """
    SELECT l.locid locid, l.locname locname, c.countryname countryname, s.subnationalname subregionname, 
            l.lat lat, l.lon lon, l.latestobsdt latestobsdt, l.numspeciesalltime numspeciesalltime
    FROM mrr_fact_hotspots l
    JOIN mrr_fact_countries c
    ON l.countrycode = c.countrycode
    JOIN mrr_fact_subnational s
    ON l.subnational1code = s.subnationalcode
"""
# Export actual MRR taxonomy dictionary to CSV files
mrr_dict_taxonomy_to_csv_sql = """
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
import_dict_from_csv_to_stg_sql = """
    BEGIN;

    TRUNCATE TABLE stg_fact_hotspots;
    TRUNCATE TABLE stg_fact_taxonomy;
        
    COPY stg_fact_hotspots
    FROM '{0}/stg_hotspots.csv'
    DELIMITER ','
    CSV HEADER;

    COPY stg_fact_taxonomy
    FROM '{0}/stg_taxonomy.csv'
    DELIMITER ','
    CSV HEADER;

    UPDATE stg_fact_taxonomy
    SET orderComName = stg_fact_order_comnames.orderComName,
        familyComName = stg_fact_family_comnames.familyComName
    FROM stg_fact_order_comnames, stg_fact_family_comnames
    WHERE stg_fact_taxonomy.orderSciName = stg_fact_order_comnames.orderSciName
            AND stg_fact_order_comnames.orderLocale = '{1}'
            AND stg_fact_taxonomy.familyCode = stg_fact_family_comnames.familyCode
            AND stg_fact_family_comnames.familyLocale = '{1}';

    COMMIT;
"""
# As stored procedure (single transaction)
import_dict_from_csv_to_stg_proc = """
    CALL stg_process_dictionaries('{0}', 'stg_hotspots.csv', 'stg_taxonomy.csv', '{1}')
"""

# Export trusted new data rows for observations from MRR db to CSV files (incremental using high water mark)
mrr_fact_locations_to_csv_sql = """
    SELECT locid, countryname, subnational1name, ishotspot, locname, lat, lng, hierarchicalname, obsfulldt
    FROM mrr_fact_location
    WHERE obsFullDt > '{}'
"""
mrr_fact_checklists_to_csv_sql = """
    SELECT *
    FROM mrr_fact_checklist
    WHERE obsFullDt > '{}'
"""
mrr_fact_observations_to_csv_sql = """
    SELECT speciesCode, obsDt, subId, obsId, howMany
    FROM mrr_fact_observation
    WHERE obsdt > '{}'
"""
# Export new weather observation for updated locations (incremental using high water mark)
mrr_weather_to_csv_sql = """
    SELECT *
    FROM mrr_fact_weather_observations
    WHERE update_ts > '{}'
"""

# Store exported CSV into STG db
# - Clear STG observations table from old data
# - Store clean date to STG (redy to model DWH)
# As sequence of SQL queries in single transaction
import_observation_from_csv_to_stg_sql = """
    BEGIN;

    TRUNCATE TABLE stg_fact_location;
    TRUNCATE TABLE stg_fact_checklist;
    TRUNCATE TABLE stg_fact_observation;
    TRUNCATE TABLE stg_fact_weather_observations;

    COPY stg_fact_location
    FROM '{0}/stg_location.csv'
    DELIMITER ','
    CSV HEADER;

    COPY stg_fact_checklist
    FROM '{0}/stg_checklist.csv'
    DELIMITER ','
    CSV HEADER;
    
    COPY stg_fact_observation
    FROM '{0}/stg_observation.csv'
    DELIMITER ','
    CSV HEADER;

    COPY stg_fact_weather_observations
    FROM '{0}/stg_weather.csv'
    DELIMITER ','
    CSV HEADER;

    COMMIT;
"""
# As stored procedure (single transaction)
import_observation_from_csv_to_stg_proc = """
    CALL stg_process_observations('{0}', 'stg_location.csv', 'stg_checklist.csv',
                                    'stg_observation.csv', 'stg_weather.csv')
"""

# -------------------------------------------
# Queries for DWH DB
# -------------------------------------------
# Export data accumulated in staging area (STG db) into CSV files
# Export new observation accumulated from previous high water mark ts
stg_location_to_csv_sql = """
    SELECT locId, locName, lat, lng, countryName, subnational1Name, obsFullDt, NULL
    FROM stg_fact_location
    WHERE obsFullDt > '{}'
"""
stg_checklist_to_csv_sql = """
    SELECT locId, subId, userDisplayName, numSpecies, obsFullDt
    FROM stg_fact_checklist
    WHERE obsFullDt > '{}'
"""
stg_observation_to_csv_sql = """
    SELECT speciescode, obsdt, subid, obsid, howmany
    FROM stg_fact_observation
    WHERE obsdt > '{}'
"""
# Export actual bird taxonomy dictionary
stg_dict_taxonomy_to_csv_sql = """
    SELECT * FROM stg_fact_taxonomy
"""
# Export actual public locations dictionary
stg_dict_location_to_csv_sql = """
    SELECT l.locid, l.locname, l.countryname, l.subRegionName, 
            l.lat, l.lon, l.latestObsDt, l.numSpeciesAllTime
    FROM stg_fact_hotspots l
"""
# Export actual weather observations accumulated from previous high water mark ts
stg_weather_to_csv_sql = """
    SELECT *
    FROM stg_fact_weather_observations
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
dwh_update_model_sql = """
    BEGIN;

    COPY dwh_dim_location_tmp
    FROM '{0}/dwh_location.csv'
    DELIMITER ','
    CSV HEADER;

    COPY dwh_dim_checklist_tmp
    FROM '{0}/dwh_checklist.csv'
    DELIMITER ','
    CSV HEADER;

    COPY dwh_fact_observation_tmp
    FROM '{0}/dwh_observation.csv'
    DELIMITER ','
    CSV HEADER;

    COPY dwh_fact_weather_observations_tmp
    FROM '{0}/dwh_weather.csv'
    DELIMITER ','
    CSV HEADER;

    TRUNCATE TABLE dwh_dim_species_details;

    COPY dwh_dim_species_details
    FROM '{0}/dwh_taxonomy.csv'
    DELIMITER ','
    CSV HEADER;

    TRUNCATE TABLE dwh_dim_location_details;

    COPY dwh_dim_location_details
    FROM '{0}/dwh_hotspots.csv'
    DELIMITER ','
    CSV HEADER;
    
    INSERT INTO dwh_dim_location
    SELECT DISTINCT l.locid, l.locname, l.lat, l.lon, l.countryname, l.subregionname,
                    l.latestobsdt, d.numspeciesalltime
    FROM dwh_dim_location_tmp l
    LEFT JOIN dwh_dim_location_details d
    ON l.locid = d.locid
    ON CONFLICT (locid) DO UPDATE
    SET locname = EXCLUDED.locname, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
        countryname = EXCLUDED.countryname, subRegionName = EXCLUDED.subRegionName,
        latestObsDt = EXCLUDED.latestObsDt, numSpeciesAllTime = EXCLUDED.numSpeciesAllTime;

    INSERT INTO dwh_dim_dt
    SELECT DISTINCT obsdt, extract(day from obsdt), extract(month from obsdt),
                    TO_CHAR(obsdt, 'Month'), extract(year from obsdt), extract(quarter from obsdt)
    FROM dwh_fact_observation_tmp
    ON CONFLICT (obsdt) DO NOTHING;

    INSERT INTO dwh_dim_checklist
    SELECT DISTINCT c.locId, c.subId, c.userDisplayName, c.numSpecies, c.obsFullDt
    FROM dwh_dim_checklist_tmp c
    ON CONFLICT (subId) DO UPDATE
    SET locId = EXCLUDED.locId, userDisplayName = EXCLUDED.userDisplayName,
        numSpecies = EXCLUDED.numSpecies, obsFullDt = EXCLUDED.obsFullDt;

    INSERT INTO dwh_fact_observation
    SELECT o.subid, o.speciescode, c.locId, o.obsdt, o.howmany
    FROM dwh_fact_observation_tmp o
    LEFT JOIN dwh_dim_checklist c
    ON o.subid = c.subid
    ON CONFLICT (subId, speciescode) DO UPDATE
    SET locId = EXCLUDED.locId, obsdt = EXCLUDED.obsdt, howmany = EXCLUDED.howmany;

    UPDATE dwh_fact_observation o
    SET tavg = w.tavg, tmin = w.tmin, tmax = w.tmax, prcp = w.prcp,
        snow = w.snow, wdir = w.wdir, wspd = w.wspd, wpgt = w.wpgt, 
        pres = w.pres, tsun = w.tsun
    FROM dwh_fact_weather_observations_tmp w
    WHERE o.locid = w.loc_id AND CAST(o.obsdt AS DATE) = w.obsdt;

    TRUNCATE TABLE dwh_dim_species;

    INSERT INTO dwh_dim_species
    SELECT DISTINCT o.speciescode, d.sciName, d.comName, d.category, d.orderSciName, d.orderComName,
                    d.familyCode, d.familyComName, d.familySciName
    FROM dwh_fact_observation o
    LEFT JOIN dwh_dim_species_details d
    ON o.speciescode = d.speciescode;

	UPDATE high_water_mark
	SET current_high_ts = (SELECT MAX(obsdt) FROM dwh_fact_observation)
	WHERE table_id = 'dwh_fact_observation';

	UPDATE high_water_mark
	SET current_high_ts = (SELECT MAX(update_ts) FROM dwh_fact_weather_observations_tmp)
	WHERE table_id = 'mrr_fact_weather_observations' AND 
        (SELECT MAX(update_ts) FROM dwh_fact_weather_observations_tmp) IS NOT NULL;

    TRUNCATE TABLE dwh_dim_checklist_tmp;
    TRUNCATE TABLE dwh_dim_location_tmp;
    TRUNCATE TABLE dwh_fact_observation_tmp;
    TRUNCATE TABLE dwh_fact_weather_observations_tmp;
                      
    COMMIT;
"""
# As stored procedure (single transaction)
dwh_update_model_proc = """
    CALL dwh_process_observations('{0}', 'dwh_location.csv', 'dwh_checklist.csv', 
                                    'dwh_observation.csv', 'dwh_hotspots.csv', 
                                    'dwh_taxonomy.csv', 'dwh_weather.csv')
"""


# -------------------------------------------
# Bash scripts
# -------------------------------------------
# Bash script for full backup of all three DBs (MRR, STG, and DWH) in the PostgreSQL instance
full_backup_sh = "./ebird-full-backup.sh {{ var.value.EBIRD_DB_COMMON_HOST }} \
 {{ var.value.EBIRD_DB_COMMON_PORT }} {{ var.value.EBIRD_DB_COMMON_USERNAME }} \
 {{ var.value.EBIRD_DB_COMMON_PASSWORD }} {{ var.value.EBIRD_BACKUP_DIR }} "