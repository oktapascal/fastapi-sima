import os
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse

from database.sima_connection import sima_connection
from repositories.PerangkatRepositories import PerangkatRepositories
from services.PerangkatService import PerangkatServices

load_dotenv()

app = FastAPI()
dbsima = sima_connection()

perangkat_repositories = PerangkatRepositories(dbsima)
perangkat_services = PerangkatServices(perangkat_repositories)

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

@app.get('/api/export/excel/perangkat')
def exportExcelPerangkat(background_task: BackgroundTasks, kode_jenis: str | None = None,kode_lokasi: str | None = None, tahun: str | None = None, kode_area: str | None = None, kode_fm: str | None = None, kode_bm: str | None = None, kode_ktg: str | None = None, kode_subktg: str | None = None):
  try:
    result = perangkat_services.get_data_perangkat(kode_jenis, kode_lokasi, tahun, kode_area, kode_fm, kode_bm, kode_ktg, kode_subktg)
    
    headerResponse = {
      'Content-Disposition': 'attachment; filename="'+result+'"'
    }
    
    background_task.add_task(os.remove, result)
    
    return FileResponse(path=result, headers=headerResponse, filename=result)
  except Exception:
    return Exception