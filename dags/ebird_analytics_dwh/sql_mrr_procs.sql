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


-- PROCEDURE: public.mrr_process_new_observations(character varying, character varying)
-- Ingest new observations from ebird

-- DROP PROCEDURE IF EXISTS public.mrr_process_new_observations(character varying, character varying);

CREATE OR REPLACE PROCEDURE public.mrr_process_new_observations(
	IN path_to_csv character varying,
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
			COPY mrr_fact_recent_observation_tmp(speciescode, sciname, locid, locname, obsdt, howmany, lat, lon,
												 obsvalid, obsreviewed, locationprivate, subid, comname, exoticcategory)
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || obs_filename
	);

	-- copy new data from tmp table into target fact table
	INSERT INTO mrr_fact_recent_observation
    SELECT *
    FROM mrr_fact_recent_observation_tmp
    ON CONFLICT(speciesCode, subId) DO NOTHING;
					
	-- delete temporary table
	DELETE FROM mrr_fact_recent_observation_tmp;

END;
$BODY$;
