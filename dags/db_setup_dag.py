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
#sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pathlib import Path
from configparser import ConfigParser
import random
import socket
import struct
from datetime import datetime

# Python function
def generate_weblog():
    ##Read files
    users = pd.read_csv('./csv/customerdata.csv',encoding = "ISO-8859-1")
    #users = pd.read_csv("users.csv",encoding = "ISO-8859-1", sep = ';')
    codes = pd.read_csv("./csv/status.csv",encoding = "ISO-8859-1")

    #Create lists
    unique_users = users['username'].values.tolist()
    user_agent = users['Agent'].values.tolist()
    user_identity = users['Document'].values.tolist()
    codes = codes['status'].values.tolist()


    print(users)
    i = 1
    with open('./csv/logs.csv','a') as csv_file:
        while i < 1000:
            #Get unique values
            rand_user_idx = random.randint(0,len(unique_users)-1)
            rand_code_idx = random.randint(0,len(codes)-1)
        
            rand_user = unique_users[rand_user_idx]
            rand_agent = user_agent[rand_user_idx]
            rand_identity = str(user_identity[rand_user_idx])
            rand_code = str(codes[rand_code_idx])
            ip = str(socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff))))
            r = '"GET / HTTP/1.0"'
        
        
        
            dt_string = datetime.now().strftime("%d/%b/%Y:%H:%M:%S")
            log = ip +' '+ rand_identity +' '+ rand_user +' ['+ dt_string + ' 0000] '+ r +' '+ rand_code +' ' + str(rand_user_idx) +' "-" "' + rand_agent + '"\n'
            print(log)
            csv_file.write(log)
            i += 1

#####################
# DAG
#####################

dag = DAG('setup', 
        description='Setup DAG',
        schedule_interval='0 12 * * *',
        start_date=datetime(2017, 3, 20), 
        catchup=False
        )

start_setup = DummyOperator(
    dag=dag,
    task_id = "Start_Setup"
)

generate_weblog_operator = PythonOperator(
    task_id='generate_weblog', 
    python_callable=generate_weblog, 
    dag=dag
    )


end_setup = DummyOperator(
    dag=dag,
    task_id = "End_Setup"
)
##########################
# Dynamic operators
##########################
dimensions_list = []
queries_path = "./json_setup/"


for file in sorted(os.listdir(queries_path)):
    with open(str(queries_path + file)) as F:
        file_operator = ast.literal_eval(F.read())

    name = file_operator[0]["name"]
    query = file_operator[0]["query"]
    upstream = file_operator[0]["upstream"]
    database = file_operator[0]["database"]

    temp_operator = MsSqlOperator(
        task_id=name,
        mssql_conn_id='mssql_tt',
        sql=query,            
        autocommit=True,
        database=database,
        dag=dag
    )
    exec (name + "_operator = temp_operator")
    exec (name + "_operator.set_upstream("+upstream+")")
    
    if name == "create_most_popular_devices":
        exec (name + "_operator.set_downstream(end_setup)")





start_setup >> generate_weblog_operator 

