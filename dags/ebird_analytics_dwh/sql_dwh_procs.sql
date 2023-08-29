-- Stored procedures in DWH dataset in Google BigQuery
-- Load new observations from Staging area into DWH and create OLAP data model (Star Schema)

CREATE OR REPLACE PROCEDURE ds_ebird_dwh.dwh_process_observations(
  IN exp_timestamp STRING
)
BEGIN
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_species_details_tmp`
    (speciesCode STRING, sciName STRING, comName STRING, category STRING, orderSciName STRING,
    orderComName STRING, familyCode STRING, familyComName STRING, familySciName STRING)
    OPTIONS(
        expiration_timestamp=CAST(exp_timestamp AS TIMESTAMP)
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_taxonomy.csv'],
        skip_leading_rows=1
    );

    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_location_details_tmp`
    (locid STRING, locname STRING, countryname STRING, subregionname STRING,
    lat FLOAT64, lon FLOAT64, numspeciesalltime INTEGER, latestobsdt DATETIME)
    OPTIONS(
        expiration_timestamp=CAST(exp_timestamp AS TIMESTAMP)
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_hotspots.csv'],
        skip_leading_rows=1
    );

    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_location_tmp`
    (locid STRING, locname STRING, lat FLOAT64, lon FLOAT64, countryname STRING,
     subregionname STRING, latestobsdt DATETIME, numspeciesalltime INTEGER)
    OPTIONS(
        expiration_timestamp=CAST(exp_timestamp AS TIMESTAMP)
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_location.csv'],
        skip_leading_rows=1
    );

    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_dim_checklist_tmp`
    (locid STRING, subid STRING, userdisplayname STRING,
     numspecies INTEGER, obsfulldt DATETIME)
    OPTIONS(
        expiration_timestamp=CAST(exp_timestamp AS TIMESTAMP)
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_checklist.csv'],
        skip_leading_rows=1
    );
  
    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_observation_tmp`
    (speciescode STRING, obsdt DATETIME, subid STRING,
     obsid STRING, howmany INTEGER)
    OPTIONS(
        expiration_timestamp=CAST(exp_timestamp AS TIMESTAMP)
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_observation.csv'],
        skip_leading_rows=1
    );

    LOAD DATA INTO `fiery-rarity-396614.ds_ebird_dwh_tmp.dwh_fact_weather_observations_tmp`
    (loc_id STRING, obsdt DATETIME, tavg FLOAT64, tmin FLOAT64, tmax FLOAT64, prcp FLOAT64,
     snow FLOAT64, wdir FLOAT64, wspd FLOAT64, wpgt FLOAT64, pres FLOAT64, tsun FLOAT64,
     update_ts DATETIME)
    OPTIONS(
        expiration_timestamp=CAST(exp_timestamp AS TIMESTAMP)
    )
    FROM FILES(
        FORMAT='CSV',
        uris = ['gs://fiery-rarity-396614-ebird/stg_weather.csv'],
        skip_leading_rows=1
    );

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

END;

