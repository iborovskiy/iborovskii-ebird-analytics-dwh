### Date created
2023-08-26 - **Version 1.5**


### Personal Pet Project: eBird Analytics Data Warehouse



### General Description

This pet project contains a simplified implementation of the data pipeline that transforms source data for bird observations from **eBird** data store (JSON responses from **eBird API 2.0**) into the target OLAP data model for the Analytics Data Warehouse.

As secondary data source **the Meteostat JSON API** was used to ingest weather conditions for locations of bird observations. 

Additionally, the project contains a simple analytical dashboard on the presentation layer (implemented in Power BI). 

The project uses a target architecture consisting of three layers implemented as independent databases (for simplicity all three layers are implemented on the instance of PostgreSQL):
- **Data Mirroring (MRR database)** replicates data from the source system (eBird) to the data warehouse environment. The primary purpose is to ensure high availability and data redundancy.
- **Data Staging (STG database)** collects and prepares data from MRR before loading it into the data model of the target data warehouse. This stage is used for applying business rules and performing data validation and integration.
- **Data Modelling (DWH database)** designs the target OLAP data model of the data warehouse. For this project, we use a star schema optimized for analytical queries. According to the selected schema, we build one fact table which contains observations of birds from submitted checklists and several dimension tables which contain attributes describing our target entities (time, location, bird species).

For our fact table - **dwh_fact_observation** - we take data records for bird observations from submitted checklists.

We use three dimensions of aggregation for our fact data:

**dwh_dim_dt** - timestamps of records in our fact table broken down into specific units (day, month, quarter, year, etc.)

**dwh_dim_location** - list of all locations in the eBird repository used for submitting checklists (location name, latitude, longitude)

**dwh_dim_species** - list of all submitted bird species in the eBird repository (common name, scientific name)

Using this data model we can build some useful analytical queries:
- Finding the geographical distribution of particular bird species 
- Finding seasonal changes in the diversity of bird species on time of the year and geographical location
etc.
- etc.

The Data Warehouse (DWH) database also contains two supporting tables:
**etl_log** - system log for the ETL process
**high_water_mark** - timestamps for high water marks of every system layer (MRR, STG, DWH)


The data warehouse database (DWH) also contains several simple functions and stored procedures used in the ETL process.



The main ETL pipeline is implemented on Apache Airflow and includes the following steps:

1. **Data extraction from external source (MRR layer)**
    - Load required dataset in JSON format from eBird and Meteostat document stores into target MRR database
    - Use Spark engine or simple pandas data frame for ingestion
    - Create schema for the source table
    - Save raw source data document to the RDBMS (PostgreSQL) table
    - Use Python connector for PostgreSQL
    - Implemented high water mark mechanism for loading only new data rows from the source

2. **Data cleaning, aligning, and source quality checks (STG layer)**
    - Load source data table from MRR database
    - Drop rows with unreliable values critical for the consistency
    - Further process source data (transform table shapes, data fields, etc.)
    - Save preprocessed source table to the staging table in STG database (PostgreSQL)
    - Use Python connector for PostgreSQL
    - Implemented high water mark mechanism for loading only new data rows from the MRR db
    
3. **Forming the target OLAP data model (DWH layer)**
    - Load preprocessed staging data set from STG database
    - Create target star schema: fact table (bird observations) and dimension tables (time, location, bird species)
    - Save prepared data model into target analytics data warehouse (PostgreSQL)
    - Use Python connector for PostgreSQL
    - Implemented high water mark mechanism for loading only new data rows from the STG db

4. **Full backup of the three production databases - MRR, STG, and DWH**
    - Implemented as bash script and can be run independently in shell (**ebird-full-backup.sh**)

Script **ebird-airflow_setup** allows you to set up new ETL process and production instances of all databases.


### Data sources

**Main dataset**: eBird recent bird observations in Georgia region. Ingestion is performed using eBird API 2.0.

API documentation: https://documenter.getpostman.com/view/664302/S1ENwy59


**Secondary dataset**: The archive of historical weather data. Ingestion is performed using Meteostat JSON API.

API documentation: https://dev.meteostat.net/api/


### Credits

**This entire study project is inspired by eBird.org:**

https://ebird.org
