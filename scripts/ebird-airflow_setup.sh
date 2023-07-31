#!/bin/sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]
then
	echo "USAGE: ebird-airflow-setup.sh <host> <port> <username> <password>"
else

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
	echo Airflow connections configured.
fi
