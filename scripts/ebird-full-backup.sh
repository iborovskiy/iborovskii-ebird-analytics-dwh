#!/bin/sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ]
then
	echo "USAGE: ebird-full-backup.sh <host> <port> <username> <password> <dirname>"
else

	echo Starting full db backup...
	echo

	export PGPASSWORD=$4

	if [ ! -d $5 ]
	then
		mkdir $5
	fi

	pg_dump -h $1 -p $2 -U $3 -F t mrr > $5/mrr_backup.tar

	echo MRR - backup completed.

	pg_dump -h $1 -p $2 -U $3 -F t stg > $5/stg_backup.tar

	echo STG - backup completed.

	pg_dump -h $1 -p $2 -U $3 -F t dwh > $5/dwh_backup.tar

	echo DWH - backup completed.
	echo 
	echo Full db backup completed.
fi
