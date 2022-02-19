# B2B Ecommerce ETL Project

This repository is used to store code for a training ETL Project

The ETL project has 3 different sources, a OLTP Database, weblogs in combined log format ("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"") and marketing data in spreadsheet format.

All data used in this repo is dummy.

## To Setup the environment

1. Build Docker using:
docker build --rm -t puckel/docker-airflow .

Used Puckel Airflow repository but added MS SQL Server Database.

2. Run Docker using:
docker-compose -f docker-compose-CeleryExecutor.yml up

## Airflow

Access Airflow on http://localhost:8080/admin

1. Execute DAG 'setup' to create and setup databases.

2. Execute DAG 'etl' to run the ETL process.

## Database

Access SQL Server Database using
Server: localhost
Port: 1433
User: sa
Password: YourStrong@Passw0rd
