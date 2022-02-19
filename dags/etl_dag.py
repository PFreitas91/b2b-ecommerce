from airflow import DAG
# Operator imports
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.mssql_hook import MsSqlHook

# Utils imports
from airflow.macros import datetime, timedelta
from airflow.models import Variable
from sys import path
from datetime import datetime, date, time, timedelta
import pendulum
import re
import glob, os
import shutil
import json
import pandas as pd
import numpy as np
import logging
import pyodbc
import os, sys
import ast
from pathlib import Path
from configparser import ConfigParser
import apache_log_parser



dag = DAG('etl', 
    description='ETL DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20), 
    catchup=False
    )




##################################
# Python functions
##################################

def load_weblogs():
    parser = apache_log_parser.make_parser(
                    "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
            )
    
    log_file = "./csv/logs.csv"
    
    with open(log_file, mode='r') as f:
    
        for line in f:
    
            d = parser(line)
    
            # Line below adds minimalistic date stamp column 
            # in format that sqlite3 date functions can work with
            print(d)
            d['date'] = d['time_received_datetimeobj'].date().isoformat()
            
            hook = MsSqlHook(mssql_conn_id="mssql_tt")
            hook.run("INSERT INTO [B2B_DB].dbo.stg_weblogs ( remote_host,  remote_user,  time_received_utc_isoformat,  request_header_user_agent) VALUES ('"+d['remote_host']+"','"+d['remote_user']+"','"+d['time_received_utc_isoformat']+"','"+d['request_header_user_agent']+"')")
                            

def load_marketing():

    df = pd.read_csv("./csv/marketinglead.csv")

    hook = MsSqlHook(mssql_conn_id="mssql_tt")

    for index, row in df.iterrows():
        a=("INSERT INTO [B2B_DB].dbo.stg_marketinglead (FirstName,LastName,fullname,Document,BirthDate,campaign) values('"+row.FirstName+"','"+row.LastName+"','"+row.fullname+"','"+row.Document+"',convert(date,'"+row.BirthDate+"',103),'"+row.campaign+"')")
        hook.run(a)
    
################################
# Operators
################################

load_marketing_operator = PythonOperator(
    task_id='load_marketing_to_stg', 
    python_callable=load_marketing, 
    dag=dag
    )

load_dim_companies_sql = open("./sql/load_dim_companies.sql").read()

load_dim_companies_operator = MsSqlOperator(
    task_id='load_dim_companies',
    mssql_conn_id='mssql_tt',
    sql=load_dim_companies_sql,            
    autocommit=True,
    database='master',
    dag=dag
)

dim_marketinglead_sql = open("./sql/load_dim_marketinglead.sql").read()

dim_marketinglead_operator = MsSqlOperator(
    task_id='dim_marketinglead',
    mssql_conn_id='mssql_tt',
    sql=dim_marketinglead_sql,            
    autocommit=True,
    database='master',
    dag=dag
)

load_fact_orders_sql = open("./sql/load_fact_orders.sql").read()

load_fact_orders_operator = MsSqlOperator(
    task_id='load_fact_orders',
    mssql_conn_id='mssql_tt',
    sql=load_fact_orders_sql,            
    autocommit=True,
    database='master',
    dag=dag
)

load_fact_webvisits_sql = open("./sql/load_fact_webvisits.sql").read()

load_fact_webvisits_operator = MsSqlOperator(
    task_id='load_fact_webvisits',
    mssql_conn_id='mssql_tt',
    sql=load_fact_webvisits_sql,            
    autocommit=True,
    database='master',
    dag=dag
)

start_etl = DummyOperator(
    dag=dag,
    task_id = "Start_ETL"
)

load_dimensions = DummyOperator(
    dag=dag,
    task_id = "Load_Dimensions"
)

load_facts = DummyOperator(
    dag=dag,
    task_id = "Load_facts"
)

end_etl = DummyOperator(
    dag=dag,
    task_id = "End_ETL"
)


load_weblogs_operator = PythonOperator(
    task_id='load_weblogs_to_stg', 
    python_callable=load_weblogs, 
    dag=dag
    )

##################################
## Dynamic dimensions operators
##################################
queries_path = "./json_etl/dimensions/"


for file in sorted(os.listdir(queries_path)):
    with open(str(queries_path + file)) as F:
        file_operator = ast.literal_eval(F.read())

    name = file_operator[0]["name"]
    query = file_operator[0]["query"]
    downstream = file_operator[0]["downstream"]

    temp_operator = MsSqlOperator(
        task_id=name,
        mssql_conn_id='mssql_tt',
        sql=query,
        autocommit=True,
        database='master',
        dag=dag)

    exec (name + " = temp_operator")
    exec (name + ".set_upstream(load_dim_companies_operator)")
    exec (name + ".set_downstream("+downstream+")")

    
    
    
start_etl >> load_marketing_operator >> load_weblogs_operator >> load_dimensions >> load_dim_companies_operator
dim_marketinglead_operator >> load_facts >> load_fact_orders_operator >> load_fact_webvisits_operator >> end_etl