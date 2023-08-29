#!/bin/sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ]
then
	echo "USAGE: ebird-airflow-setup.sh <host> <port> <username> <password> <backup_dir>"
else

	echo "Install dependencies..."
	pip install -r requirements.txt

	echo
	echo "Postgres databases setup..."
	echo
	export PGPASSWORD=$4
	pg_restore --host=$1 --port=$2 --username=$3 --clean --if-exists --create -d postgres $5/mrr_backup.tar
	echo "MRR db restored from backup."
fi
