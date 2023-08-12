#!/bin/sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ] || [ -z "$6" ] || [ -z "$7" ] || [ -z "$8" ] || [ -z "$9" ]
then
	echo "USAGE: ebird-airflow-setup.sh <host> <port> <username> <password> <api_key> <backup_dir> <working_dir> <region> <names_locale>"
else

	echo "Install dependencies..."
	pip install pandas findspark pyspark requests apache-airflow-providers-postgres

	echo
	echo "Airflow connection setup..."
	echo

	airflow connections add 'postgres_mrr_conn' \
		--conn-json '{
			"conn_type": "postgres",
			"login": "'$3'",
			"password": "'$4'",
			"host": "'$1'",
			"port": '$2',
			"schema": "mrr" }'

	echo "Connection 'postgres_mrr_conn' created."

	airflow connections add 'postgres_stg_conn' \
		--conn-json '{
			"conn_type": "postgres",
			"login": "'$3'",
			"password": "'$4'",
			"host": "'$1'",
			"port": '$2',
			"schema": "stg" }'

	echo "Connection 'postgres_stg_conn' created."

	airflow connections add 'postgres_dwh_conn' \
		--conn-json '{
			"conn_type": "postgres",
			"login": "'$3'",
			"password": "'$4'",
			"host": "'$1'",
			"port": '$2',
			"schema": "dwh" }'

	echo "Connection 'postgres_dwh_conn' created."
	echo 
	echo "Airflow connections configured."
	echo
	echo "Airflow variables setup..."
	airflow variables set EBIRD_DWH_INTERNAL_MODEL False
	echo "EBIRD_DWH_INTERNAL_MODEL created."
	airflow variables set EBIRD_USE_SPARK False
	echo "EBIRD_USE_SPARK created."
	airflow variables set EBIRD_REGION_CODE $8 # e.g. GE
	echo "EBIRD_REGION_CODE created."
	airflow variables set EBIRD_LOCALE $9 # e.g. ru
	echo "EBIRD_LOCALE created."
	airflow variables set EBIRD_DAYS_BACK 30
	echo "EBIRD_DAYS_BACK created."
	airflow variables set EBIRD_API_KEY $5
	echo "EBIRD_API_KEY created."
	airflow variables set EBIRD_HOME_DIR $7 # e.g. /home/iborovskii
	echo "EBIRD_HOME_DIR created."
	echo
	echo "Airflow variables configured."
	echo
	echo "Postgres databases setup..."
	echo
	export PGPASSWORD=$4
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $6/mrr_backup.tar
	echo "MRR db restored from backup."
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $6/stg_backup.tar
	echo "STG db restored from backup."
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $6/dwh_backup.tar
	echo "DWH db restored from backup."
	echo
	echo "MRR, STG and DWH databases configured."
fi
