#!/bin/bash
echo "Read bd credentials"
source /run/secrets/task_db_creds
echo "Running create database script"


create_task_db="$(db_user=${db_user//[$'\t\r\n ']} db_pswd="'${db_pswd//[$'\t\r\n ']}'" db_name=${db_name//[$'\t\r\n ']} envsubst < custom_scripts/create_db.sql )"

psql -v ON_ERROR_STOP=1 --username "user_pg" --dbname "postgres" <<-EOSQL
$create_task_db
EOSQL

echo "init tasks db"
psql -v ON_ERROR_STOP=1 --username "user_pg" --dbname "tasks_db" -f /custom_scripts/init_tasks_db.sql

echo "Running drop default database"
psql -v ON_ERROR_STOP=1 --username "user_pg" --dbname "tasks_db" -f /custom_scripts/drop_default_bd.sql
