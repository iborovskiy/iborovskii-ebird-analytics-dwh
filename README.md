### Date created
2023-09-03 - **Version 3.0**


### Personal Pet Project: eBird Analytics Data Warehouse

**LIVE INTERACTIVE DASHBOARD:** https://lookerstudio.google.com/u/0/reporting/610d9d6b-cddf-410f-bd62-9a67f1a1c107/page/tEnnC?s=u6r669OnF50



### General Description

This pet project contains a simplified implementation of the data pipeline that transforms source data for bird observations from **eBird** data store (JSON responses from **eBird API 2.0**) into the target OLAP data model for the Analytics Data Warehouse deployed in Google Cloud.

As secondary data source **the Meteostat JSON API** was used to ingest weather conditions for locations of bird observations. 

Additionally, the project contains a simple analytical dashboard on the presentation layer (implemented in **Looker Studio** - https://lookerstudio.google.com/u/0/reporting/610d9d6b-cddf-410f-bd62-9a67f1a1c107/page/tEnnC?s=u6r669OnF50). 


The project is implemented in two separate editions:
- **Hybrid solution: Apache Airflow (ETL) + PostgreSQL (MRR db) + Google Cloud: Cloud Storage (Staging Area) + BigQuery (Analytical DWH)**. Folder - **/airflow+postgres+bq**


- **Fully Google Cloud managed: Cloud Scheduler / Cloud Functions (ETL) + Firestore (MRR db) + Cloud Storage (Staging Area) + BigQuery (Analytical DWH)**. Folder - **/gcp_fs_cs_bq**


### Data sources

**Main dataset**: eBird recent bird observations in Georgia region. Ingestion is performed using eBird API 2.0.

API documentation: https://documenter.getpostman.com/view/664302/S1ENwy59


**Secondary dataset**: The archive of historical weather data. Ingestion is performed using Meteostat JSON API.

API documentation: https://dev.meteostat.net/api/


### Credits

**This entire study project is inspired by eBird.org:**

https://ebird.org
