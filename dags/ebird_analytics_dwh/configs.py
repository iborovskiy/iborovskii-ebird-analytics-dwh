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
    FROM `fiery-rarity-396614.ds_ebird_dwh.dwh_fact_observation` o
    JOIN `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_location` l
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
# Queries for Staging area in Google Cloud Storage
# -------------------------------------------
# Export actual MRR public locations dictionary to CSV files
mrr_dict_locations_to_csv_sql = """
    SELECT l.locid locid, l.locname locname, c.countryname countryname, s.subnationalname subregionname, 
            l.lat lat, l.lon lon, CAST(l.numspeciesalltime AS INT) numspeciesalltime,
			l.latestobsdt latestobsdt
    FROM mrr_fact_hotspots l
    JOIN mrr_fact_countries c
    ON l.countrycode = c.countrycode
    JOIN mrr_fact_subnational s
    ON l.subnational1code = s.subnationalcode
"""
# Export actual MRR taxonomy dictionary to CSV files
mrr_dict_taxonomy_to_csv_sql = """
    SELECT t.speciesCode speciesCode, t.sciName sciName, t.comName comName, t.category category,
            t.orderSciName orderSciName, o.orderComName AS orderComName, t.familyCode familyCode,
			f.familyComName AS familyComName, t.familySciName familySciName
    FROM mrr_fact_taxonomy t
    LEFT JOIN mrr_fact_order_comnames o
    ON t.orderSciName = o.orderSciName AND o.orderLocale = '{0}'
    LEFT JOIN mrr_fact_family_comnames f
    ON t.familyCode = f.familyCode AND f.familyLocale = '{0}'
"""
# Export trusted new data rows for observations from MRR db to CSV files (incremental using high water mark)
mrr_fact_locations_to_csv_sql = """
    SELECT locid, locname, lat, lng, countryname, subnational1name, obsfulldt, NULL
    FROM mrr_fact_location
    WHERE obsFullDt > '{}'
"""
mrr_fact_checklists_to_csv_sql = """
    SELECT c.locid, c.subid, c.userdisplayname, c.numspecies, c.obsfulldt
    FROM mrr_fact_checklist c
    WHERE obsFullDt > '{}'
"""
mrr_fact_observations_to_csv_sql = """
    SELECT speciesCode, obsDt, subId, obsId, howMany
    FROM mrr_fact_observation
    WHERE obsdt > '{}'
"""
# Export new weather observation for updated locations (incremental using high water mark)
mrr_weather_to_csv_sql = """
    SELECT w.loc_id, w.obsdt, w.tavg, w.tmin, w.tmax, w.prcp, w.snow, w.wdir,
		w.wspd, w.wpgt, w.pres, w.tsun, w.update_ts
    FROM mrr_fact_weather_observations w
    WHERE update_ts > '{}'
"""

# -------------------------------------------
# Queries for DWH DB
# -------------------------------------------
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

# Import new batch of data into temporary dataset
dwh_load_taxonomy_dict_bq_sql = """
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_species_details_tmp`
    (speciesCode STRING, sciName STRING, comName STRING, category STRING, orderSciName STRING,
    orderComName STRING, familyCode STRING, familyComName STRING, familySciName STRING)
    OPTIONS(
        expiration_timestamp="{}"
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_taxonomy.csv'],
        skip_leading_rows=1
    );
"""
dwh_load_hotspots_dict_bq_sql = """
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_location_details_tmp`
    (locid STRING, locname STRING, countryname STRING, subregionname STRING,
    lat FLOAT64, lon FLOAT64, numspeciesalltime INTEGER, latestobsdt DATETIME)
    OPTIONS(
        expiration_timestamp="{}"
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_hotspots.csv'],
        skip_leading_rows=1
    );
"""
dwh_load_location_dict_bq_sql = """
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_location_tmp`
    (locid STRING, locname STRING, lat FLOAT64, lon FLOAT64, countryname STRING,
     subregionname STRING, latestobsdt DATETIME, numspeciesalltime INTEGER)
    OPTIONS(
        expiration_timestamp="{}"
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_location.csv'],
        skip_leading_rows=1
    );
"""

dwh_load_checklist_dict_bq_sql = """
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_checklist_tmp`
    (locid STRING, subid STRING, userdisplayname STRING,
     numspecies INTEGER, obsfulldt DATETIME)
    OPTIONS(
        expiration_timestamp="{}"
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_checklist.csv'],
        skip_leading_rows=1
    );
"""

dwh_load_observation_bq_sql = """
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_observation_tmp`
    (speciescode STRING, obsdt DATETIME, subid STRING,
     obsid STRING, howmany INTEGER)
    OPTIONS(
        expiration_timestamp="{}"
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_observation.csv'],
        skip_leading_rows=1
    );
"""
dwh_load_weather_bq_sql = """
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_weather_observations_tmp`
    (loc_id STRING, obsdt DATETIME, tavg FLOAT64, tmin FLOAT64, tmax FLOAT64, prcp FLOAT64,
     snow FLOAT64, wdir FLOAT64, wspd FLOAT64, wpgt FLOAT64, pres FLOAT64, tsun FLOAT64,
     update_ts DATETIME)
    OPTIONS(
        expiration_timestamp="{}"
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_weather.csv'],
        skip_leading_rows=1
    );
"""

# As sequence of SQL queries in single transaction
dwh_update_bq_sql = """
    BEGIN TRANSACTION;

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_location_details` T
    USING `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_location_details_tmp` S
    ON T.locid = S.locid
    WHEN MATCHED THEN
        UPDATE SET locname = S.locname, countryname = S.countryname,
                    subregionname = S.subregionname, lat = S.lat, lon = S.lon,
                    numspeciesalltime = S.numspeciesalltime, latestobsdt = S.latestobsdt
    WHEN NOT MATCHED THEN
        INSERT (locid, locname, countryname, subregionname, lat, lon, 
                numspeciesalltime, latestobsdt)
        VALUES(locid, locname, countryname, subregionname, lat, lon, 
                numspeciesalltime, latestobsdt);

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_species_details` T
    USING `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_species_details_tmp` S
    ON T.speciescode = S.speciescode
    WHEN MATCHED THEN
        UPDATE SET sciname = S.sciname, comname = S.comname, category = S.category,
                    ordersciname = S.ordersciname, ordercomname = S.ordercomname,
                    familycode = S.familycode, familycomname = S.familycomname,
                    familysciname = S.familysciname
    WHEN NOT MATCHED THEN
        INSERT (speciescode, sciname, comname, category, ordersciname, ordercomname, 
                familycode, familycomname, familysciname)
        VALUES(speciescode, sciname, comname, category, ordersciname, ordercomname, 
                familycode, familycomname, familysciname); 

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_dt` T
    USING (
        SELECT DISTINCT obsdt, extract(day from obsdt) day, extract(month from obsdt) month,
                FORMAT_DATETIME("%B",obsdt) month_name, extract(year from obsdt) year, 
                extract(quarter from obsdt) quarter
        FROM `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_observation_tmp`
    ) S
    ON T.obsdt = S.obsdt
    WHEN NOT MATCHED THEN
        INSERT (obsdt, day, month, month_name, year, quarter)
        VALUES (obsdt, day, month, month_name, year, quarter);

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_location` T
    USING (
        SELECT DISTINCT l.locid locid, l.locname locname, l.lat lat, l.lon lon,
                    l.countryname countryname, l.subregionname subregionname,
                    l.latestobsdt latestobsdt, d.numspeciesalltime numspeciesalltime
        FROM `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_location_tmp` l
        LEFT JOIN `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_location_details` d
        ON l.locid = d.locid
    ) S
    ON T.locid = S.locid
    WHEN MATCHED THEN
        UPDATE SET locname = S.locname, lat = S.lat, lon = S.lon,
                    countryname = S.countryname, subregionname = S.subregionname,
                    latestobsdt = S.latestobsdt, numspeciesalltime = S.numspeciesalltime
    WHEN NOT MATCHED THEN
        INSERT (locid, locname, lat, lon, countryname, subregionname, 
                latestobsdt, numspeciesalltime)
        VALUES (locid, locname, lat, lon, countryname, subregionname, 
                latestobsdt, numspeciesalltime);

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_checklist` T
    USING `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_checklist_tmp` S
    ON T.subid = S.subid
    WHEN MATCHED THEN
        UPDATE SET locid = S.locid, userdisplayname = S.userdisplayname,
                    numspecies = S.numspecies, obsfulldt = S.obsfulldt
    WHEN NOT MATCHED THEN
        INSERT (locid, subid, userdisplayname, numspecies, obsfulldt)
        VALUES (locid, subid, userdisplayname, numspecies, obsfulldt);

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_fact_observation` T
    USING (
        SELECT o.subid subid, o.speciescode speciescode, c.locId locId, 
                o.obsdt obsdt, o.howmany howmany
        FROM `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_observation_tmp` o
        LEFT JOIN `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_checklist` c
        ON o.subid = c.subid
    ) S
    ON T.subId = S.subId AND T.speciescode = S.speciescode
    WHEN MATCHED THEN
        UPDATE SET locid = S.locid, obsdt = S.obsdt, howmany = S.howmany
    WHEN NOT MATCHED THEN
        INSERT (subId, speciescode, locid, obsdt, howmany)
        VALUES (subId, speciescode, locid, obsdt, howmany);

    UPDATE `fiery-rarity-396614.ds_ebird_dwh.dwh_fact_observation` o
    SET tavg = w.tavg, tmin = w.tmin, tmax = w.tmax, prcp = w.prcp,
        snow = w.snow, wdir = w.wdir, wspd = w.wspd, wpgt = w.wpgt, 
        pres = w.pres, tsun = w.tsun
    FROM `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_weather_observations_tmp` w
    WHERE o.locid = w.loc_id AND CAST(o.obsdt AS DATE) = w.obsdt;

    MERGE `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_species` T
    USING (
        SELECT DISTINCT o.speciescode speciescode, d.sciName sciName, d.comName comName,
                        d.category category, d.orderSciName orderSciName,
                        d.orderComName orderComName, d.familyCode familyCode,
                        d.familyComName familyComName, d.familySciName familySciName
        FROM `fiery-rarity-396614.ds_ebird_dwh.dwh_fact_observation` o
        LEFT JOIN `fiery-rarity-396614.ds_ebird_dwh.dwh_dim_species_details` d
        ON o.speciescode = d.speciescode
    ) S
    ON T.speciescode = S.speciescode
    WHEN MATCHED THEN
        UPDATE SET sciName = S.sciName, comName = S.comName, category = S.category,
                    orderSciName = S.orderSciName, orderComName = S.orderComName,
                    familyCode = S.familyCode, familyComName = S.familyComName,
                    familySciName = S.familySciName
    WHEN NOT MATCHED THEN
        INSERT (speciescode, sciName, comName, category, orderSciName, orderComName,
                familyCode, familyComName, familySciName)
        VALUES (speciescode, sciName, comName, category, orderSciName, orderComName,
                familyCode, familyComName, familySciName);

    UPDATE `fiery-rarity-396614.ds_ebird_dwh.high_water_mark`
    SET current_high_ts = (SELECT MAX(obsdt)
                           FROM `fiery-rarity-396614.ds_ebird_dwh.dwh_fact_observation`)
    WHERE table_id = 'dwh_fact_observation' AND
            (SELECT MAX(obsdt)
             FROM `fiery-rarity-396614.ds_ebird_dwh.dwh_fact_observation`) IS NOT NULL;

    UPDATE `fiery-rarity-396614.ds_ebird_dwh.high_water_mark`
    SET current_high_ts = (SELECT MAX(update_ts)
                           FROM `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_weather_observations_tmp`)
    WHERE table_id = 'mrr_fact_weather_observations' AND
            (SELECT MAX(update_ts)
             FROM `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_weather_observations_tmp`) IS NOT NULL;
                
    COMMIT TRANSACTION;
"""

# As stored procedure (single transaction)
dwh_update_model_proc = """
    CALL ds_ebird_dwh.dwh_process_observations("{0}");
"""


# -------------------------------------------
# Helper queries
# -------------------------------------------
# Insert msg into service log
log_insert = """
    INSERT INTO `fiery-rarity-396614.ds_ebird_dwh.etl_log` (ts, event_type, event_description) VALUES ('{}', {}, '{}')
"""
get_hwm = """
    SELECT CAST(current_high_ts AS DATETIME)
    FROM `fiery-rarity-396614.ds_ebird_dwh.high_water_mark`
    WHERE table_id = '{}'
"""


# -------------------------------------------
# Bash scripts
# -------------------------------------------
# Bash script for full backup of all three DBs (MRR, STG, and DWH) in the PostgreSQL instance
full_backup_sh = "./ebird-full-backup.sh {{ var.value.EBIRD_DB_COMMON_HOST }} \
 {{ var.value.EBIRD_DB_COMMON_PORT }} {{ var.value.EBIRD_DB_COMMON_USERNAME }} \
 {{ var.value.EBIRD_DB_COMMON_PASSWORD }} {{ var.value.EBIRD_BACKUP_DIR }} "
