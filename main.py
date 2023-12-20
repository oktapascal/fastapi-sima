from datetime import datetime
import os
import pandas
import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse

from database.sima_connection import sima_connection

load_dotenv()

app = FastAPI()

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

@app.get('/test-db')
def test_db():
  try:
    dbsima = sima_connection()
    print("{c} is working".format(c=dbsima))
    dbsima.close()
    return {'status': 'OK', 'message': 'Success Connect Database'}
  except pyodbc.Error as ex:
    print("{c} is not working".format(c=dbsima))

@app.get('/api/export/excel/perangkat')
def export_excel_perangkat(background_task: BackgroundTasks, kode_jenis: str | None = None,kode_lokasi: str | None = None, tahun: str | None = None, kode_area: str | None = None, kode_fm: str | None = None, kode_bm: str | None = None, kode_ktg: str | None = None, kode_subktg: str | None = None, status_aktif: str | None = None):
  dbsima = sima_connection()
  
  columns = ["no", "id_group", "id_area", "id_unit", "nama_unit", "id_witel", "nama_witel", "id_location", "nama_lokasi", "id_gedung", "nama_gedung", "id_kelas", "id_room", "id_lantai",
  "nama_lantai", "id_jenis", "nama_jenis", "id_kategori", "nama_kategori", "id_subkategori", "nama_subkategori", "nama_perangkat", "is_ceklis", "merk", "satuan", "jumlah",
  "kapasitas", "no_seri", "tipe", "tahun", "kondisi", "milik", "keterangan", "id_perangkat"];
    
  where = "where e.flag_aktif <> '3'"
  if kode_jenis is not None and kode_jenis != "" and kode_jenis != "null":
    where = where + f"and a.jid in ({kode_jenis})"
    
  if kode_lokasi is not None and kode_lokasi != "" and kode_lokasi != "null":
    where = where + f"and a.location_id in ({kode_lokasi})"
    
  if tahun is not None and tahun != "" and tahun != "null":
    where = where + f"and a.tahun in ({tahun})"
    
  if kode_area is not None and kode_area != "" and kode_area != "null":
    where = where + f"and e.kode_area in ({kode_area})"
  
  if kode_fm is not None and kode_fm != "" and kode_fm != "null":
    where = where + f"and e.kode_fm in ({kode_fm})"
    
  if kode_bm is not None and kode_bm != "" and kode_bm != "null":
    where = where + f"and e.kode_bm in ({kode_bm})"
    
  if kode_ktg is not None and kode_ktg != "" and kode_ktg != "null":
    where = where + f"and a.kid in ({kode_ktg})"
    
  if kode_subktg is not None and kode_subktg != "" and kode_subktg != "null":
    where = where + f"and a.skid in ({kode_subktg})"
    
  if status_aktif is not None and status_aktif != "" and status_aktif != "null":
    where = where + f"and isnull(a.status_aktif, '0') in ({status_aktif})"
    
  try:
    cursor = dbsima.cursor()
    cursor.execute(f"""
        select a.id no_perangkat, a.group_id, e.kode_area, a.unit_id, b.nama_unit, a.witel_id, k.nama, a.location_id, d.nama_lokasi,
        a.kode_gedung, e.nama_gedung, a.kelas_id, a.room_id, a.floor_id, g.nama_lantai, a.jid, h.nama_jenis,
        a.kid, i.nama_kategori, a.skid, j.nama_sub_kategori, a.nama_perangkat, a.is_ceklis, a.merk, a.satuan, a.jumlah,
        a.kapasitas, a.no_seri, a.type, a.tahun, a.kondisi, a.milik, a.keterangan, a.id_perangkat
        from am_perangkat a
        inner join am_gedung as e ON e.kode_gedung = a.kode_gedung and e.kode_lokasi='11'
        inner join am_units as b ON a.unit_id = b.id
        inner join am_locations as d ON a.location_id = d.id
        inner join gsd_rooms as f ON a.room_id = f.id
        inner join am_floors as g ON a.floor_id = g.id
        left join am_perangkat_jenis as h ON a.jid = h.jenis_id
        left join am_perangkat_kategori as i ON a.kid = i.kategori_id
        left join am_perangkat_sub_kategori as j ON a.skid = j.sub_kategori_id
        left join am_witel as k ON e.kode_witel = k.kode_witel
        {where}
      """)
    
    dataframe = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns)
    
    today = datetime.today()
    unique_id = today.strftime('%Y%m%d%H%M%S')
    
    file_name = f'DATA_PERANGKAT_{unique_id}.xlsx'
    
    writer = pandas.ExcelWriter(file_name)
    
    dataframe.to_excel(writer,index=False)
    
    writer.close()
    
    headerResponse = {
      'Content-Disposition': 'attachment; filename="'+file_name+'"'
    }
    
    background_task.add_task(os.remove, file_name)
    
    return FileResponse(path=file_name, headers=headerResponse, filename=file_name)
  except Exception as ex:
    return {"status": False, "message": str(ex)}
  finally:
    cursor.close()
    dbsima.close()