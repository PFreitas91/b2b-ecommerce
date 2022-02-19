import pandas as pd
import glob
import Utils
import time
import pyodbc

def getDatabaseConnection(rootconnection = False):
    db_configs = Utils.readConfigFile(folder = 'config', filename='config.ini', section='sqlserver')
    print("opening DB connection...")
    db_server = db_configs.get("db_server")
    if rootconnection:
        db_username = db_configs.get("root_db_username")
        db_password = db_configs.get("root_db_password")
        database_name = db_configs.get("root_database_name")
    else:
        db_username = db_configs.get("db_username")
        db_password = db_configs.get("db_password")
        database_name = db_configs.get("database_name")

    # db connection details 
    try:
        db_connection = pyodbc.connect('DRIVER={SQL Server};SERVER='+db_server+';DATABASE='+database_name+';UID='+db_username+';PWD='+db_password, autocommit=True)
        print("database is ready!", db_connection)
        return db_connection;
    except:
        print("database still loading...")
        time.sleep(15)
        return False;

def installDatabase():
    # db connection details 
    db_connection = getDatabaseConnection(rootconnection = True)

    print('Validating if database existis...')
    database_validate_schema_sql = "SELECT * FROM msdb.sys.databases d WHERE name = 'B2B_DB'"
    
    valdiate_schema_df = pd.read_sql(database_validate_schema_sql,db_connection)
    schema_name_exists = valdiate_schema_df.empty
    if not schema_name_exists:
        print("Database exists - proceed with process execution")
    else:
        print("Database does not exists - installing DB")
        for file in glob.glob("db\*.sql"):
            print("installing database script: ", file)
            db_cursor = db_connection.cursor()
            sql_file = open(file)
            sql_as_string = sql_file. read()
            db_cursor.execute(sql_as_string)
    db_connection.close()