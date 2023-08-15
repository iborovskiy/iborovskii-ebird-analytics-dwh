#!/bin/sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ]
then
	echo "USAGE: ebird-airflow-setup.sh <host> <port> <username> <password> <backup_dir>"
else

	echo "Install dependencies..."
	pip install pandas findspark pyspark requests apache-airflow-providers-postgres

	echo
	echo "Postgres databases setup..."
	echo
	export PGPASSWORD=$4
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $5/mrr_backup.tar
	echo "MRR db restored from backup."
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $5/stg_backup.tar
	echo "STG db restored from backup."
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $5/dwh_backup.tar
	echo "DWH db restored from backup."
	echo
	echo "MRR, STG and DWH databases configured."
fi
