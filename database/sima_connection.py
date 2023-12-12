import os
import pyodbc

"""
  Establishes a connection to the SIMA database using the provided environment variables.

  :return: A connection object to the SIMA database.
"""
def sima_connection() -> pyodbc.Connection:
  host = os.getenv('DB_HOST1')
  port = os.getenv('DB_PORT1')
  database = os.getenv('DB_NAME1')
  username = os.getenv('DB_USER1')
  password = os.getenv('DB_PASSWORD1')
  
  config = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+host+","+port+";DATABASE="+database+";UID="+username+";PWD="+password+";"
  
  connection = pyodbc.connect(config)
  
  return connection