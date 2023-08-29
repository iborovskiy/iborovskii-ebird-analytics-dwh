#!/bin/sh

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ]
then
	echo "USAGE: ebird-full-backup.sh <host> <port> <username> <password> <dirname>"
else

	echo Starting full MRR db backup...
	echo

	export PGPASSWORD=$4

	if [ ! -d $5 ]
	then
		mkdir $5
	fi

	pg_dump -h $1 -p $2 -U $3 -F t mrr > $5/mrr_backup.tar

	echo Full MRR db backup completed.
fi
