-- Stored procedures in DWH db

-- PROCEDURE: public.dwh_process_observations(character varying, character varying, character varying, character varying)
-- Import new observations from CSV into DWH and create OLAP data model (Star Schema)

-- DROP PROCEDURE IF EXISTS public.dwh_process_observations(character varying, character varying, character varying, character varying);

-- DROP PROCEDURE IF EXISTS public.dwh_process_observations(character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE PROCEDURE public.dwh_process_observations(
	IN path_to_csv character varying,
	IN obs_filename character varying,
	IN loc_filename character varying,
	IN taxonomy_filename character varying,
	IN weather_filename character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
-- declare variables here
BEGIN
	-- import csv files with new data sets from staging area
	
	-- import new observations into temporary fact table
	EXECUTE format
	(
		$str$
			COPY dwh_fact_raw_observations_tmp
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || obs_filename
	);
	
	-- import new weather observations into temporary table
	EXECUTE format
	(
		$str$
			COPY dwg_fact_weather_observations_tmp
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || weather_filename
	);
	
	-- import new taxonomy dictionary into DWH
	TRUNCATE TABLE dwh_dim_species_details;
	
	EXECUTE format
	(
		$str$
			COPY dwh_dim_species_details
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || taxonomy_filename
	);
	
	-- import new public locations dictionary into DWH
	TRUNCATE TABLE dwh_dim_location_details;

	EXECUTE format
	(
		$str$
			COPY dwh_dim_location_details
			FROM %L
			DELIMITER ','
			CSV HEADER
		$str$, path_to_csv || '/' || loc_filename
	);

	-- create new data model for DWH

	-- create dimension table - dwh_dim_dt (incremental update)
	INSERT INTO dwh_dim_dt
    SELECT DISTINCT obsdt, extract(day from obsdt), extract(month from obsdt),
                    TO_CHAR(obsdt, 'Month'), extract(year from obsdt), extract(quarter from obsdt)
    FROM dwh_fact_raw_observations_tmp
    ON CONFLICT (obsdt) DO NOTHING;

	-- create fact table - dwh_fact_observation (incremental update)
    INSERT INTO dwh_fact_observation
    SELECT o.subid, o.speciescode, o.locid, o.obsdt, o.howmany
    FROM dwh_fact_raw_observations_tmp o
    ON CONFLICT DO NOTHING;
	
	-- update weather conditions for observations
    UPDATE dwh_fact_observation o
    SET tavg = w.tavg, tmin = w.tmin, tmax = w.tmax, prcp = w.prcp,
        snow = w.snow, wdir = w.wdir, wspd = w.wspd, wpgt = w.wpgt, 
        pres = w.pres, tsun = w.tsun
    FROM dwg_fact_weather_observations_tmp w
    WHERE o.locid = w.loc_id AND CAST(o.obsdt AS DATE) = w.obsdt;

	-- create dimension table - dwh_dim_location (incremental update)
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
		
	-- create dimension table - dwh_dim_species (full update not incremental)
	TRUNCATE TABLE dwh_dim_species;
	
	INSERT INTO dwh_dim_species
    SELECT DISTINCT o.speciescode, d.sciName, d.comName, d.category, d.orderSciName, d.orderComName,
                    d.familyCode, d.familyComName, d.familySciName
    FROM dwh_fact_observation o
    LEFT JOIN dwh_dim_species_details d
    ON o.speciescode = d.speciescode;
	
	-- update current high_water_mark on success of current DAG
	UPDATE high_water_mark
	SET current_high_ts = (SELECT MAX(obsdt) FROM dwh_fact_observation)
	WHERE table_id = 'dwh_fact_observation';

	UPDATE high_water_mark
	SET current_high_ts = (SELECT MAX(update_ts) FROM dwg_fact_weather_observations_tmp)
	WHERE table_id = 'mrr_fact_weather_observations' AND
		(SELECT MAX(update_ts) FROM dwg_fact_weather_observations_tmp) IS NOT NULL;

	-- truncate temporary observation table
	TRUNCATE TABLE dwh_fact_raw_observations_tmp;
	TRUNCATE TABLE dwg_fact_weather_observations_tmp;
		
END;
$BODY$;


-- PROCEDURE: public.write_log(timestamp without time zone, integer, character varying)
-- Write new record into operational log

-- DROP PROCEDURE IF EXISTS public.write_log(timestamp without time zone, integer, character varying);

CREATE OR REPLACE PROCEDURE public.write_log(
	IN ts timestamp without time zone,
	IN lvl integer,
	IN msg character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
BEGIN
	INSERT INTO etl_log (ts, event_type, event_description) values (ts, lvl, msg);
END;
$BODY$;


-- Functions in DWH db

-- FUNCTION: public.get_current_hwm(character varying)
-- Get current value for high water mark (timestamp of the most recent observation)

-- DROP FUNCTION IF EXISTS public.get_current_hwm(character varying);

CREATE OR REPLACE FUNCTION public.get_current_hwm(
	tbl_name character varying)
    RETURNS timestamp without time zone
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	hwm TIMESTAMP;
BEGIN
	SELECT current_high_ts INTO STRICT hwm
	FROM high_water_mark
	WHERE table_id = tbl_name AND current_high_ts IS NOT NULL;
	
	RETURN hwm;
	
EXCEPTION
	WHEN no_data_found  THEN
		RETURN '2020-01-21 16:35';
END;
$BODY$;



-------------------------------------------------
-- FOR DEVELOPMENT, DEBUGGING, and TESTING ONLY!!
-------------------------------------------------

-- PROCEDURE: public.process_new_row(character varying, character varying, character varying, character varying, timestamp without time zone, real, real, real, character varying, character varying)
-- Manual insertion into DWH model (don't use in production!!!!)

-- DROP PROCEDURE IF EXISTS public.process_new_row(character varying, character varying, character varying, character varying, timestamp without time zone, real, real, real, character varying, character varying);

CREATE OR REPLACE PROCEDURE public.process_new_row(
	IN i_speciescode character varying,
	IN i_sciname character varying,
	IN i_locid character varying,
	IN i_locname character varying,
	IN i_obsdt timestamp without time zone,
	IN i_howmany real,
	IN i_lat real,
	IN i_lon real,
	IN i_subid character varying,
	IN i_comname character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	new_high_water_mark TIMESTAMP;
BEGIN
	-- Insrt into dwh_dim_dt
	INSERT INTO dwh_dim_dt VALUES(i_obsdt, EXTRACT(day from i_obsdt), EXTRACT(month from i_obsdt),
								  TO_CHAR(i_obsdt, 'Month'), EXTRACT(year from i_obsdt),
								  EXTRACT(quarter from i_obsdt))
	ON CONFLICT (obsdt) DO NOTHING;
								  
	-- Insrt into dwh_dim_location
	INSERT INTO dwh_dim_location VALUES(i_locid, i_locname, i_lat, i_lon)
                    ON CONFLICT (locid)
                    DO UPDATE
                    SET locname = EXCLUDED.locname, lat = EXCLUDED.lat, lon = EXCLUDED.lon;
					
	-- Insrt into dwh_dim_species
	INSERT INTO dwh_dim_species VALUES(i_speciescode, i_sciname, i_comname) 
                    ON CONFLICT (speciescode) DO NOTHING;
	
	-- Insrt into dwh_fact_observation
	INSERT INTO dwh_fact_observation VALUES(i_subid, i_speciescode, i_locid, i_obsdt, i_howmany);
	
	-- update current high_water_mark on success
	UPDATE high_water_mark
	SET current_high_ts = (SELECT MAX(obsdt) FROM dwh_fact_observation)
	WHERE table_id = 'dwh_fact_observation';
	
EXCEPTION
	WHEN unique_violation THEN
		CALL write_log(NOW()::TIMESTAMP, 3, 'INTERNAL ERROR: Can''t process new source row into DWH target data model.');
END;
$BODY$;


-- FUNCTION: public.get_bird_species_by_loc(character varying)
-- Get bird species list for given location

-- DROP FUNCTION IF EXISTS public.get_bird_species_by_loc(character varying);

CREATE OR REPLACE FUNCTION public.get_bird_species_by_loc(
	search_locid character varying)
    RETURNS TABLE(full_bird_species_names character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
declare
	-- variable declaration
	cur_species cursor(search_locid VARCHAR)
		for select d.comname, d.sciname
		from dwh_dim_species d
		where speciescode in (select distinct speciescode
						  from dwh_fact_observation
						  where locid = search_locid)
						  ORDER BY d.sciname;
	r record;
	--full_bird_species_names VARCHAR := '';
begin
	
	open cur_species(search_locid);
	loop
		fetch cur_species into r;
		exit when not found;
		full_bird_species_names := r.comname || ' (' || r.sciname || ')';
		return next;
	end loop;
	close cur_species;
end;
$BODY$;