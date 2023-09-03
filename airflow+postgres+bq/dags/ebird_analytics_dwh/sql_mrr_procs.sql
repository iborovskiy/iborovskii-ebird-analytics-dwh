-- Stored procedures in MRR db

-- PROCEDURE: public.mrr_process_dictionaries(character varying, character varying, character varying, character varying, character varying)
-- Ingest new dictionaries from ebird

-- DROP PROCEDURE IF EXISTS public.mrr_process_dictionaries(character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE PROCEDURE public.mrr_process_dictionaries(
	IN path_to_csv character varying,
	IN loc_filename character varying,
	IN coutries_filename character varying,
	IN subregions_filename character varying,
	IN taxonomy_filename character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
-- declare variables here
BEGIN
	-- delete old dictionaries tables
	TRUNCATE TABLE mrr_fact_locations;
	TRUNCATE TABLE mrr_fact_countries;
	TRUNCATE TABLE mrr_fact_subnational;
	TRUNCATE TABLE mrr_fact_taxonomy;
	
	-- import new data from csv files
	EXECUTE format
	(
		$str$
			COPY mrr_fact_locations(locid, locname, countrycode, subnational1Code, lat, lon, latestObsDt, numSpeciesAllTime)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || loc_filename
	);

	EXECUTE format
	(
		$str$
			COPY mrr_fact_countries(countrycode, countryname)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || coutries_filename
	);

	EXECUTE format
	(
		$str$
			COPY mrr_fact_subnational(subnationalCode, subnationalName)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || subregions_filename
	);

	EXECUTE format
	(
		$str$
			COPY mrr_fact_taxonomy(speciesCode, sciName, comName, category, orderSciName, familyCode, familySciName)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || taxonomy_filename
	);
	
END;
$BODY$;


-- PROCEDURE: public.mrr_process_new_observations(character varying, character varying, character varying, character varying)
-- Ingest new observations from ebird

-- DROP PROCEDURE IF EXISTS public.mrr_process_new_observations(character varying, character varying, character varying, character varying);

CREATE OR REPLACE PROCEDURE public.mrr_process_new_observations(
	IN path_to_csv character varying,
	IN loc_filename character varying,
	IN cl_filename character varying,
	IN obs_filename character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
-- declare variables here
BEGIN	
	-- import new data from csv files into temporary table
	EXECUTE format
	(
		$str$
			COPY mrr_fact_location_tmp(locId, countryCode, countryName, subnational1Name, subnational1Code,
                                		isHotspot, locName, lat, lng, hierarchicalName, obsFullDt)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || loc_filename
	);
	
	EXECUTE format
	(
		$str$
			COPY mrr_fact_checklist_tmp(locId, subId, userDisplayName, numSpecies, obsFullDt)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || cl_filename
	);
	
	EXECUTE format
	(
		$str$
			COPY mrr_fact_observation_tmp(speciesCode, obsDt, subId, projId, obsId, howMany, present)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || obs_filename
	);

	-- copy new data from tmp table into target fact table
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
		
    INSERT INTO mrr_fact_checklist
    SELECT *
    FROM mrr_fact_checklist_tmp
    ON CONFLICT (subId) DO UPDATE
    SET locId = EXCLUDED.locId, userDisplayName = EXCLUDED.userDisplayName,
        numSpecies = EXCLUDED.numSpecies,
        obsFullDt = EXCLUDED.obsFullDt;
		
    INSERT INTO mrr_fact_observation
    SELECT *
    FROM mrr_fact_observation_tmp
    ON CONFLICT (obsId) DO UPDATE
    SET speciesCode = EXCLUDED.speciesCode, obsDt = EXCLUDED.obsDt,
        subId = EXCLUDED.subId,
        projId = EXCLUDED.projId,
        howMany = EXCLUDED.howMany,
        present = EXCLUDED.present;
					
	-- delete temporary table
	TRUNCATE TABLE mrr_fact_location_tmp;
	TRUNCATE TABLE mrr_fact_checklist_tmp;
	TRUNCATE TABLE mrr_fact_observation_tmp;

END;
$BODY$;
