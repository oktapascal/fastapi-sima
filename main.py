from dotenv import load_dotenv
from fastapi import FastAPI

from database.sima_connection import sima_connection

load_dotenv()

app = FastAPI()
dbsima = sima_connection()

"""
A function that serves as the root endpoint for the FastAPI application.
This function returns a dictionary with the keys 'status' and 'message',
representing the status and message of the API response.

Parameters:
None

Returns:
A dictionary with the keys 'status' and 'message'.
"""
@app.get('/')
def root():
  return {'status': 'OK', 'message': 'Hello From Fastapi-SIMA'}