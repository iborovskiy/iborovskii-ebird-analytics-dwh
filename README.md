### Date created
2023-08-29 - **Version 2.0**


### Personal Pet Project: eBird Analytics Data Warehouse (Google Cloud + Apache Airflow Edition)

**LIVE INTERACTIVE DASHBOARD:** https://lookerstudio.google.com/u/0/reporting/610d9d6b-cddf-410f-bd62-9a67f1a1c107/page/tEnnC?s=u6r669OnF50



### General Description

This pet project contains a simplified implementation of the data pipeline that transforms source data for bird observations from **eBird** data store (JSON responses from **eBird API 2.0**) into the target OLAP data model for the Analytics Data Warehouse deployed in Google Cloud.

As secondary data source **the Meteostat JSON API** was used to ingest weather conditions for locations of bird observations. 

Additionally, the project contains a simple analytical dashboard on the presentation layer (implemented in **Looker Studio** - https://lookerstudio.google.com/u/0/reporting/610d9d6b-cddf-410f-bd62-9a67f1a1c107/page/tEnnC?s=u6r669OnF50). 

The project uses a target architecture consisting of three layers implemented as independent databases:
- **Data Mirroring (MRR database)** Implemented in **PostgreSQL**. Replicates data from the source systems (eBird and Meteostat) to the data warehouse environment. The primary purpose is to ensure high availability and data redundancy.
- **Data Staging Area** Implemented as Bucket in **Google Cloud Storage**. Collects and prepares data from MRR before loading it into the data model of the target data warehouse. This stage is used for applying business rules and performing data validation and integration.
- **Data Modelling (Analytical Data Warehouse)** Implemented in **Google BigQuery**. Designs the target OLAP data model of the data warehouse. For this project, we use a star schema optimized for analytical queries. According to the selected schema, we build one fact table which contains observations of birds from submitted checklists and several dimension tables which contain attributes describing our target entities (time, location, bird species).

For our fact table - **dwh_fact_observation** - we take data records for bird observations from submitted checklists.

We use four dimensions of aggregation for our fact data:

**dwh_dim_dt** - timestamps of records in our fact table broken down into specific units (day, month, quarter, year, etc.)

**dwh_dim_location** - all locations in the eBird repository used for submitting checklists (location name, latitude, longitude)

**dwh_dim_species** - all submitted bird species in the eBird repository (common name, scientific name)

**dwh_dim_checklist** - lall submitted checklists in the eBird repository (incl. author name)

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
    - Load new rows of source data in MRR database (using high water mark for last recorded observation)
    - Drop rows with unreliable values critical for the consistency
    - Further process source data (transform table shapes, data fields, etc.)
    - Save preprocessed source table to the staging area in dedicated Bucket in Google Cloud Storage
    - Use Python connector for Google Cloud
    
3. **Forming the target OLAP data model (DWH layer)**
    - Load preprocessed dataset from staging area (bucket in Cloud Storage) into Google BigQuery
    - Create target star schema: fact table (bird observations) and dimension tables (time, location, checklist and bird species)
    - Save prepared data model into target dataset for analytics data warehouse (Google BigQuery)
    - Use Python connector for Google Cloud and Google BigQuery

4. **Full backup of the production MRR database**
    - Implemented as bash script and can be run independently in shell (**ebird-full-backup.sh**)

Script **ebird-airflow_setup** allows you to set up production instance of MRR database with up-to-date copy.


### Data sources

**Main dataset**: eBird recent bird observations in Georgia region. Ingestion is performed using eBird API 2.0.

API documentation: https://documenter.getpostman.com/view/664302/S1ENwy59


**Secondary dataset**: The archive of historical weather data. Ingestion is performed using Meteostat JSON API.

API documentation: https://dev.meteostat.net/api/


### Credits

**This entire study project is inspired by eBird.org:**

https://ebird.org
