-- Stored procedures in STG db

-- PROCEDURE: public.stg_process_dictionaries(character varying, character varying, character varying, character varying)
-- Import new dictionaries from CSV into Staging area

-- DROP PROCEDURE IF EXISTS public.stg_process_dictionaries(character varying, character varying, character varying, character varying);

CREATE OR REPLACE PROCEDURE public.stg_process_dictionaries(
	IN path_to_csv character varying,
	IN loc_filename character varying,
	IN taxonomy_filename character varying,
	IN locale character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
-- declare variables here
BEGIN

	-- clear STG dictionaries tables from old data
	TRUNCATE TABLE stg_fact_locations;
	TRUNCATE TABLE stg_fact_taxonomy;
	
	-- import new data from csv files into dictionaries tables
	EXECUTE format
	(
		$str$
			COPY stg_fact_locations
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || loc_filename
	);

	EXECUTE format
	(
		$str$
			COPY stg_fact_taxonomy
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || taxonomy_filename
	);

	-- add common names of birds for selected locale
	EXECUTE format
	(
		$str$
            UPDATE stg_fact_taxonomy
            SET orderComName = stg_fact_order_comnames.orderComName,
                familyComName = stg_fact_family_comnames.familyComName
            FROM stg_fact_order_comnames, stg_fact_family_comnames
            WHERE stg_fact_taxonomy.orderSciName = stg_fact_order_comnames.orderSciName
                AND stg_fact_order_comnames.orderLocale = %L
                AND stg_fact_taxonomy.familyCode = stg_fact_family_comnames.familyCode
                AND stg_fact_family_comnames.familyLocale = %L
		$str$, locale, locale
	);

END;
$BODY$;


-- PROCEDURE: public.stg_process_observations(character varying, character varying)
-- Import new observations from CSV into Staging area

-- DROP PROCEDURE IF EXISTS public.stg_process_observations(character varying, character varying);

CREATE OR REPLACE PROCEDURE public.stg_process_observations(
	IN path_to_csv character varying,
	IN obs_filename character varying,
	IN weather_filename character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
-- declare variables here
BEGIN

	-- clear STG observation table from old data
	TRUNCATE TABLE stg_fact_observation;
	TRUNCATE TABLE stg_fact_weather_observations;
	
	-- import new data from csv files into staging area
	EXECUTE format
	(
		$str$
			COPY stg_fact_observation
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || obs_filename
	);

	EXECUTE format
	(
		$str$
			COPY stg_fact_weather_observations
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || weather_filename
	);

END;
$BODY$;