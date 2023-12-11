import os
import pyodbc

def sima_connection():
  host = os.getenv('DB_HOST1')
  database = os.getenv('DB_NAME1')
  username = os.getenv('DB_USER1')
  password = os.getenv('DB_PASSWORD1')
  
  config = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+host+";DATABASE="+database+";UID="+username+";PWD="+password+";"
  
  connection = pyodbc.connect(config)
  
  return connection