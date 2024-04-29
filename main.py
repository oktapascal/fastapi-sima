from typing import Annotated
from datetime import datetime
import math
import time
import os
from typing import List
import pandas
import pyodbc
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, BackgroundTasks, File, UploadFile, Form, Body
from fastapi.responses import FileResponse
from openpyxl.styles import NamedStyle, PatternFill, Alignment
from scipy import stats

from database import database

load_dotenv()

def connect_dbsima():
  engine = database.Database(os.getenv('DB_USER1'),os.getenv('DB_PASSWORD1'),os.getenv('DB_HOST1'),os.getenv('DB_NAME1'))
  
  dbsima_engine = engine.connect()
  
  return dbsima_engine

app = FastAPI()

app.add_middleware(
  CORSMiddleware,
  allow_methods=["*"],
  allow_headers=["GET","POST"]
)

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
    dbsima = connect_dbsima()
    print("{c} is working".format(c=dbsima))
    dbsima.close()
    
    return {'status': 'OK', 'message': 'Success Connect Database'}
  except pyodbc.Error as ex:
    print("{c} is not working".format(c=dbsima))

@app.get('/api/export/excel/perangkat')
def export_excel_perangkat(background_task: BackgroundTasks, kode_jenis: str | None = None,kode_lokasi: str | None = None, tahun: str | None = None, kode_area: str | None = None, kode_fm: str | None = None, kode_bm: str | None = None, kode_ktg: str | None = None, kode_subktg: str | None = None, status_aktif: str | None = None):
  columns = ["no", "id_regional", "id_area", "nama_area", "id_bm", "nama_bm", "id_witel", "nama_witel", "id_location", "nama_lokasi", "id_gedung", "nama_gedung", "id_room", "id_lantai",
  "nama_lantai", "id_jenis", "nama_jenis", "id_kategori", "nama_kategori", "id_subkategori", "nama_subkategori", "nama_perangkat", "is_ceklis", "kode_merk", "nama_merk", "satuan", "jumlah",
  "kapasitas", "no_seri", "tipe", "tahun", "kondisi", "kode_milik", "nama_milik", "keterangan", "id_perangkat", "status_aktif", "tanggal_periksa", "nik_input", "updated_at"]
    
  where = "where e.flag_aktif <> '3'"
  if kode_jenis is not None and kode_jenis != "" and kode_jenis != "null":
    where = where + f"and a.kode_jenis in ({kode_jenis})"
    
  if kode_lokasi is not None and kode_lokasi != "" and kode_lokasi != "null":
    where = where + f"and a.kode_lokasi in ({kode_lokasi})"
    
  if tahun is not None and tahun != "" and tahun != "null":
    where = where + f"and a.tahun in ({tahun})"
    
  if kode_area is not None and kode_area != "" and kode_area != "null":
    where = where + f"and e.kode_area in ({kode_area})"
  
  if kode_fm is not None and kode_fm != "" and kode_fm != "null":
    where = where + f"and a.kode_fm in ({kode_fm})"
    
  if kode_bm is not None and kode_bm != "" and kode_bm != "null":
    where = where + f"and a.kode_bm in ({kode_bm})"
    
  if kode_ktg is not None and kode_ktg != "" and kode_ktg != "null":
    where = where + f"and a.kode_kategori in ({kode_ktg})"
    
  if kode_subktg is not None and kode_subktg != "" and kode_subktg != "null":
    where = where + f"and a.kode_subkategori in ({kode_subktg})"
    
  if status_aktif is not None and status_aktif != "" and status_aktif != "null":
    where = where + f"and isnull(a.status_aktif, '1') in ({status_aktif})"
    
  try:
    dbsima = connect_dbsima()
    cursor = dbsima.cursor()
    
    cursor.execute(f"""
        select a.id no_perangkat, e.kode_area, a.kode_fm, l.nama nama_fm, a.kode_bm, m.nama nama_bm, a.kode_witel, k.nama nama_witel, a.kode_lokasi, d.nama_lokasi,
        a.kode_gedung, e.nama_gedung, a.kode_room, a.kode_lantai, g.nama_lantai, a.kode_jenis, h.nama_jenis,
        a.kode_kategori, i.nama_kategori, a.kode_subkategori, j.nama_sub_kategori, a.nama_perangkat, a.is_ceklis, a.kode_merk, n.nama_merk, a.satuan, a.jumlah,
        a.kapasitas, a.no_seri, a.model, a.tahun, a.kondisi, a.kode_milik, o.nama nama_milik, a.keterangan, a.id_perangkat, 
        case when isnull(a.status_aktif, '1') = '1' then 'ACTIVE' else 'INACTIVE' end status_aktif, a.kondisi_terakhir, a.nik_user, a.tgl_input
        from dev_am_perangkat a
        inner join am_gedung as e ON e.kode_gedung = a.kode_gedung and e.kode_lokasi='11'
        inner join am_locations as d ON a.kode_lokasi = d.id
        inner join gsd_rooms as f ON a.kode_room = f.id
        inner join am_floors as g ON a.kode_lantai = g.id
        left join am_perangkat_jenis as h ON a.kode_jenis = h.jenis_id
        left join am_perangkat_kategori as i ON a.kode_kategori = i.kategori_id
        left join am_perangkat_sub_kategori as j ON a.kode_subkategori = j.sub_kategori_id
        left join am_witel as k ON e.kode_witel = k.kode_witel
        left join am_fm l on a.kode_fm=l.kode_fm
        left join am_bm m on a.kode_bm=m.kode_bm
        left join am_perangkat_merk n on a.kode_merk=n.kode_merk
        left join am_milik o on a.kode_milik=o.kode_milik
        {where}
      """)
    
    dataframe = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns)
    
    today = datetime.today()
    unique_id = today.strftime('%Y%m%d%H%M%S')
    
    file_name = f'DATA_PERANGKAT_{unique_id}.xlsx'
    
    writer = pandas.ExcelWriter(file_name)
    
    dataframe.to_excel(writer,index=False)
    
    writer.close()
    cursor.close()
    dbsima.close()
    
    headerResponse = {
      'Content-Disposition': 'attachment; filename="'+file_name+'"'
    }
    
    background_task.add_task(os.remove, file_name)
    
    del dataframe
    
    return FileResponse(path=file_name, headers=headerResponse, filename=file_name)
  except Exception as ex:
    return {"status": False, "message": str(ex)}
    
@app.post('/api/import/csv/sap3')
def import_csv_sap3(background_task: BackgroundTasks, file: UploadFile = File(...)):
  try:
    dbsima = connect_dbsima()
    cursor = dbsima.cursor()
    
    t0 = time.perf_counter()
    
    contents = file.file.read()
    
    with open(file.filename, 'wb') as f:
      f.write(contents)
    
    dataframe = pandas.read_csv(file.filename, sep=';',na_values='0',index_col=False,keep_default_na=False,dtype=str)
    dataframe.fillna(0, inplace=True)
    
    read_time = f"{time.perf_counter() - t0:.1f} seconds"
    
    
    cursor.execute("truncate table dev_real_sap3_tmp")
    cursor.commit()
    
    count = 0
    sql_statement = "BEGIN TRANSACTION \r\n"
    
    for index,df in dataframe.iterrows():
      count += 1
      
      dataframe.loc[index,'amount_in_doc_curr'] = str(dataframe.loc[index,'amount_in_doc_curr']).replace('.','').replace(',','.')
      dataframe.loc[index,'dalam_jutaan'] = str(dataframe.loc[index,'dalam_jutaan']).replace('.','').replace(',','.')
      
      sql_statement += f"""
      insert into dev_real_sap3_tmp (kode_akun,kode_ba,kode_cc,posting_date,posting_period,no_dokumen,reference,assignment,amount_in_doc_curr,keterangan,trading_partner,
      dalam_jutaan,tenant,grouping,c3,c4,c5,c6,cc_baru,ba_baru,periode,cap_non_cap,refference_pm,reff_1,reff_2,portfolio,leveraging,digital,segmen,segmen_for_pl,dc,id_crm,id_ampm)
      values ('{df['kode_akun']}','{df['kode_ba']}','{df['kode_cc']}','{df['posting_date']}','{df['posting_period']}','{df['no_dokumen']}','{df['reference']}','{df['assignment']}',
      '{dataframe.loc[index,'amount_in_doc_curr']}','{df['keterangan']}','{df['trading_partner']}','{dataframe.loc[index,'dalam_jutaan']}','{df['tenant']}','{df['grouping']}','{df['c3']}',
      '{df['c4']}','{df['c5']}','{df['c6']}','{df['cc_baru']}','{df['ba_baru']}','{df['periode']}','{df['cap_non_cap']}','{df['refference_pm']}','{df['reff_1']}','{df['reff_2']}',
      '{df['portfolio']}','{df['leveraging']}','{df['digital']}','{df['segmen']}','{df['segmen_for_pl']}','{df['dc']}','{df['id_crm']}','{df['id_ampm']}')
      """
      
      if count % 100 == 0:
        sql_statement += "COMMIT TRANSACTION"
        cursor.execute(sql_statement)
        cursor.commit()
        sql_statement = "BEGIN TRANSACTION \r\n"
    
    if sql_statement != "BEGIN TRANSACTION \r\n":
      sql_statement += "COMMIT TRANSACTION"
      cursor.execute(sql_statement)
      cursor.commit()
    
    file.file.close()
    cursor.close()
    dbsima.close()
    
    background_task.add_task(os.remove, file.filename)
    
    return {"status": True, "message": "Import CSV berhasil", "read_time": read_time,"process_time": f"{time.perf_counter() - t0:.1f} seconds"}
  except Exception as ex:
    return {"status": False, "message": str(ex)}
    
@app.post('/api/import/excel/sap3')
def import_excel_sap3(background_task: BackgroundTasks, file: UploadFile = File(...)):
  try:
    dbsima = connect_dbsima()
    cursor = dbsima.cursor()
    
    t0 = time.perf_counter()
    
    contents = file.file.read()
    
    with open(file.filename, 'wb') as f:
      f.write(contents)
      
    dataframe = pandas.read_excel(file.filename,na_values='0',index_col=False,keep_default_na=False,dtype=str)
    dataframe.fillna(0, inplace=True)
    
    read_time = f"{time.perf_counter() - t0:.1f} seconds"
    
    cursor.execute("truncate table dev_real_sap3_tmp")
    cursor.commit()
    
    count = 0
    sql_statement = "BEGIN TRANSACTION \r\n"
    
    for index,df in dataframe.iterrows():
      count += 1
      
      dataframe.loc[index,'amount_in_doc_curr'] = str(dataframe.loc[index,'amount_in_doc_curr']).replace('.','').replace(',','.')
      dataframe.loc[index,'dalam_jutaan'] = str(dataframe.loc[index,'dalam_jutaan']).replace('.','').replace(',','.')
      
      sql_statement += f"""
      insert into dev_real_sap3_tmp (kode_akun,kode_ba,kode_cc,posting_date,posting_period,no_dokumen,reference,assignment,amount_in_doc_curr,keterangan,trading_partner,
      dalam_jutaan,tenant,grouping,c3,c4,c5,c6,cc_baru,ba_baru,periode,cap_non_cap,refference_pm,reff_1,reff_2,portfolio,leveraging,digital,segmen,segmen_for_pl,dc,id_crm,id_ampm)
      values ('{df['kode_akun']}','{df['kode_ba']}','{df['kode_cc']}','{df['posting_date']}','{df['posting_period']}','{df['no_dokumen']}','{df['reference']}','{df['assignment']}',
      '{dataframe.loc[index,'amount_in_doc_curr']}','{df['keterangan']}','{df['trading_partner']}','{dataframe.loc[index,'dalam_jutaan']}','{df['tenant']}','{df['grouping']}','{df['c3']}',
      '{df['c4']}','{df['c5']}','{df['c6']}','{df['cc_baru']}','{df['ba_baru']}','{df['periode']}','{df['cap_non_cap']}','{df['refference_pm']}','{df['reff_1']}','{df['reff_2']}',
      '{df['portfolio']}','{df['leveraging']}','{df['digital']}','{df['segmen']}','{df['segmen_for_pl']}','{df['dc']}','{df['id_crm']}','{df['id_ampm']}')
      """
      
      if count % 100 == 0:
        sql_statement += "COMMIT TRANSACTION"
        cursor.execute(sql_statement)
        cursor.commit()
        sql_statement = "BEGIN TRANSACTION \r\n"
    
    if sql_statement != "BEGIN TRANSACTION \r\n":
      sql_statement += "COMMIT TRANSACTION"
      cursor.execute(sql_statement)
      cursor.commit()
    
    file.file.close()
    cursor.close()
    dbsima.close()
    
    background_task.add_task(os.remove, file.filename)
    
    return {"status": True, "message": "Import Excel berhasil", "read_time": read_time, "process_time": f"{time.perf_counter() - t0:.1f} seconds"}
  except Exception as ex:
    return {"status": False, "message": str(ex)}
    
@app.get('/api/template/perangkat-update')
def export_template_update_perangkat(background_task: BackgroundTasks, kode_regional: str | None = None, kode_area: str | None = None, kode_bm: str | None = None, kode_witel: str | None = None, kode_gedung: str | None = None):
  columns_perangkat = ["id", "kode_area", "kode_bm", "kode_witel", "kode_lokasi", "kode_gedung", "kode_lantai", "kode_room", "kode_milik", "kode_jenis", "kode_kategori", "kode_subkategori", "kode_merk", 
                      "jumlah", "satuan", "kapasitas", "satuan_kapasitas", "model", "no_seri", "tahun", "kondisi", "waktu_pengadaan", "cek_birawa", "tanggal_cek", "keterangan", 
                      "status_aktif", "updated_by", "updated_at"]
  
  where_perangkat = "where b.flag_aktif <> '3'"
  where_regional = "where a.kode_lokasi = '11'"
  where_area = "where a.kode_lokasi = '11'"
  where_bm = "where a.kode_lokasi = '11'"
  where_witel = "where a.kode_lokasi = '11'"
  where_location = "where a.flag_aktif = '1'"
  where_gedung = "where a.kode_lokasi = '11' and a.flag_aktif <> '3'"
  where_lantai = "where a.flag_aktif = '1'"
  where_ruang = "where a.flag_aktif = '1'"
  
  if kode_regional is not None and kode_regional != "" and kode_regional != "null":
    where_perangkat = where_perangkat + f"and b.kode_area = '{kode_regional}'"
    where_regional = where_regional + f"and a.kode_area = '{kode_regional}'"
    where_area = where_area + f"and a.kode_area = '{kode_regional}'"
    where_bm = where_bm + f"and a.kode_area = '{kode_regional}'"
    where_witel = where_witel + f"and b.kode_area = '{kode_regional}'"
    where_gedung = where_gedung + f"and a.kode_area = '{kode_regional}'"
    where_lantai = where_lantai + f"and b.kode_area = '{kode_regional}'"
    where_ruang = where_ruang + f"and b.kode_area = '{kode_regional}'"
  
  if kode_area is not None and kode_area != "" and kode_area != "null":
    where_perangkat = where_perangkat + f"and a.kode_fm = '{kode_area}'"
    where_bm = where_bm + f"and a.kode_fm = '{kode_area}'"
    where_witel = where_witel + f"and b.kode_fm = '{kode_area}'"
    where_gedung = where_gedung + f"and a.kode_fm = '{kode_area}'"
    where_lantai = where_lantai + f"and b.kode_fm = '{kode_area}'"
    where_ruang = where_ruang + f"and b.kode_fm = '{kode_area}'"
    
  if kode_bm is not None and kode_bm != "" and kode_bm != "null":
    where_perangkat = where_perangkat + f"and a.kode_bm = '{kode_bm}'"
    where_witel = where_witel + f"and b.kode_bm = '{kode_bm}'"
    where_location = where_location + f"and a.kode_bm = '{kode_bm}'"
    where_gedung = where_gedung + f"and a.kode_bm = '{kode_bm}'"
    where_lantai = where_lantai + f"and b.kode_bm = '{kode_bm}'"
    where_ruang = where_ruang + f"and b.kode_bm = '{kode_bm}'"
    
  if kode_witel is not None and kode_witel != "" and kode_witel != "null":
    where_perangkat = where_perangkat + f"and a.kode_witel = '{kode_witel}'"
    where_witel = where_witel + f"and b.kode_witel = '{kode_witel}'"
    where_gedung = where_gedung + f"and a.kode_witel = '{kode_witel}'"
    where_lantai = where_lantai + f"and b.kode_witel = '{kode_witel}'"
    where_ruang = where_ruang + f"and b.kode_witel = '{kode_witel}'"
    
  if kode_gedung is not None and kode_gedung != "" and kode_gedung != "null":
    where_perangkat = where_perangkat + f"and a.kode_gedung = '{kode_gedung}'"
    where_gedung = where_gedung + f"and a.kode_gedung = '{kode_gedung}'"
    where_lantai = where_lantai + f"and a.kode_gedung_sima = '{kode_gedung}'"
    where_ruang = where_ruang + f"and a.kode_gedung = '{kode_gedung}'"
    
  sql_statement_perangkat = f"""
  select top 20 a.id, a.kode_fm, a.kode_bm, a.kode_witel, a.kode_lokasi, a.kode_gedung, a.kode_lantai, a.kode_room, a.kode_milik, a.kode_jenis, a.kode_kategori, a.kode_subkategori, a.kode_merk, a.jumlah,
  a.satuan, a.kapasitas, a.satuan_kapasitas, a.model, a.no_seri, a.tahun, a.kondisi, a.tahun_pengadaan, a.is_ceklis, a.kondisi_terakhir, a.keterangan, isnull(a.status_aktif, 1) status_aktif, '' updated_by, '' updated_at
  from dev_am_perangkat a
  inner join am_gedung b on a.kode_gedung=b.kode_gedung and b.kode_lokasi='11'
  {where_perangkat}
  """
  
  columns_regional = ["kode_regional", "nama_regional"]
  
  sql_statement_regional = f"""
  select a.kode_area, a.nama
  from am_area a
  {where_regional}
  """
  
  columns_area = ["kode_area", "nama_area"]
  
  sql_statement_area = f"""
  select a.kode_fm, a.nama
  from am_fm a
  {where_area}
  """
  
  columns_bm = ["kode_bm", "nama"]
  
  sql_statement_bm = f"""
  select a.kode_bm, a.nama
  from am_bm a
  {where_bm}
  """
  
  columns_witel = ["kode_witel", "nama"]
  
  sql_statement_witel = f"""
  select a.kode_witel, a.nama
  from am_witel a
  where a.kode_witel = 'WT04'
  union all
  select a.kode_witel, a.nama
  from am_witel a
  inner join dev_am_perangkat_witel b on a.kode_witel=b.kode_witel
  {where_witel}
  group by a.kode_witel, a.nama
  """
  
  columns_location = ["kode_lokasi", "nama_lokasi", "penanggung_jawab"]
  
  sql_statement_location = f"""
  select a.id, a.nama_lokasi, a.penanggung_jawab
  from am_locations a
  {where_location}
  """
  
  columns_gedung = ["kode_gedung", "nama_gedung"]
  
  sql_statement_gedung = f"""
  select a.kode_gedung, a.nama_gedung
  from am_gedung a
  {where_gedung}
  """
  
  columns_lantai = ["kode_lantai", "nama_lantai", "kode_gedung"]
  
  sql_statement_lantai = f"""
  select a.id, a.nama_lantai, a.kode_gedung_sima kode_gedung
  from am_floors a
  inner join am_gedung b on a.kode_gedung_sima=b.kode_gedung and b.kode_lokasi='11'
  {where_lantai}
  order by a.kode_gedung_sima asc
  """
  
  columns_ruang = ["kode_ruang", "nama_ruang", "kode_lantai", "kode_gedung"]
  
  sql_statement_ruang = f"""
  select a.id, a.peruntukan, a.floor_id, a.kode_gedung
  from gsd_rooms a
  inner join am_gedung b on a.kode_gedung=b.kode_gedung and b.kode_lokasi='11'
  {where_ruang}
  order by a.kode_gedung asc
  """
  
  columns_jenis = ["kode_jenis", "nama_jenis"]
  
  sql_statement_jenis = f"""
  select a.jenis_id kode_jenis, a.nama_jenis
  from am_perangkat_jenis a
  """
  
  columns_kategori = ["kode_kategori", "nama_kategori", "kode_jenis"]
  
  sql_statement_kategori = f"""
  select a.kategori_id kode_kategori, a.nama_kategori, a.jenis_id kode_jenis
  from am_perangkat_kategori a
  """
  
  columns_subkategori = ["kode_kategori", "nama_kategori", "kode_jenis"]
  
  sql_statement_subkategori = f"""
  select a.sub_kategori_id kode_subkategori, a.nama_sub_kategori, a.kategori_id kode_kategori
  from am_perangkat_sub_kategori a
  """
  
  columns_merk = ["kode_merk", "nama_merk"]
  
  sql_statement_merk = f"""
  select a.kode_merk, a.nama_merk
  from am_perangkat_merk a
  """
  
  today = datetime.today()
  unique_id = today.strftime('%Y%m%d%H%M%S')
  file_name = f'UPDATE_PERANGKAT_{unique_id}.xlsx'
  
  try:
    dbsima = connect_dbsima()
    cursor = dbsima.cursor()
    
    writer = pandas.ExcelWriter(file_name, engine='openpyxl')
    
    cursor.execute(sql_statement_perangkat)
    
    dataframe_perangkat = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_perangkat)
    dataframe_perangkat.to_excel(writer,index=False, sheet_name='DATA_PERANGKAT')
    
    workbook = writer.book
    worksheet = workbook.active
    
    worksheet.merge_cells(start_row=1, start_column=30, end_row=6, end_column=35)
    worksheet['AD1'].alignment = Alignment(wrap_text=True)
    worksheet['AD1'].value = f"""
    Catatan :\n
    Mohon mengisi kolom updated_by dan updated_at\n
    jika melakukan perubahan data perangkat
    """
    
    worksheet['B1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['C1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['D1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['E1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['F1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['G1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['H1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['I1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['J1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['K1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['L1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    worksheet['M1'].fill = PatternFill(start_color='FFEA00',end_color='FFEA00',fill_type='solid')
    
    nsyyyymmdd = NamedStyle(name="nsyyyymmdd", number_format="YYYY-MM-DD")
    nsyyyymm = NamedStyle(name="nsyyyymm", number_format="YYYY-MM")
    
    for i in range(2, len(dataframe_perangkat) + 2):
      worksheet.cell(row=i, column=22).style = nsyyyymm
      worksheet.cell(row=i, column=24).style = nsyyyymmdd
      worksheet.cell(row=i, column=28).style = nsyyyymmdd
      
    cursor.execute(sql_statement_regional)
    
    dataframe_regional = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_regional)
    dataframe_regional.to_excel(writer,index=False, sheet_name='DATA_REGIONAL')
    
    cursor.execute(sql_statement_area)
    
    dataframe_area = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_area)
    dataframe_area.to_excel(writer,index=False, sheet_name='DATA_AREA')
    
    cursor.execute(sql_statement_bm)
    
    dataframe_bm = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_bm)
    dataframe_bm.to_excel(writer,index=False, sheet_name='DATA_BM')
    
    cursor.execute(sql_statement_witel)
    
    dataframe_witel = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_witel)
    dataframe_witel.to_excel(writer,index=False, sheet_name='DATA_WITEL')
    
    cursor.execute(sql_statement_location)
    
    dataframe_location = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_location)
    dataframe_location.to_excel(writer,index=False, sheet_name='DATA_LOCATION')
    
    cursor.execute(sql_statement_gedung)
    
    dataframe_gedung = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_gedung)
    dataframe_gedung.to_excel(writer,index=False, sheet_name='DATA_GEDUNG')
    
    cursor.execute(sql_statement_lantai)
    
    dataframe_lantai = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_lantai)
    dataframe_lantai.to_excel(writer,index=False, sheet_name='DATA_LANTAI')
    
    cursor.execute(sql_statement_ruang)
    
    dataframe_ruang = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_ruang)
    dataframe_ruang.to_excel(writer,index=False, sheet_name='DATA_RUANG')
    
    cursor.execute(sql_statement_jenis)
    
    dataframe_jenis = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_jenis)
    dataframe_jenis.to_excel(writer,index=False, sheet_name='DATA_JENIS')
    
    cursor.execute(sql_statement_kategori)
    
    dataframe_kategori = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_kategori)
    dataframe_kategori.to_excel(writer,index=False, sheet_name='DATA_KATEGORI')
    
    cursor.execute(sql_statement_subkategori)
    
    dataframe_subkategori = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_subkategori)
    dataframe_subkategori.to_excel(writer,index=False, sheet_name='DATA_SUBKATEGORI')
    
    cursor.execute(sql_statement_merk)
    
    dataframe_merk = pandas.DataFrame.from_records(cursor.fetchall(), columns=columns_merk)
    dataframe_merk.to_excel(writer,index=False, sheet_name='DATA_MERK')
    
    headerResponse = {
      'Content-Disposition': 'attachment; filename="'+file_name+'"'
    }
    
    workbook.close()
    writer.close()
    cursor.close()
    dbsima.close()
    
    background_task.add_task(os.remove, file_name)
    
    del dataframe_perangkat
    del dataframe_regional
    del dataframe_area
    del dataframe_bm
    del dataframe_witel
    del dataframe_location
    del dataframe_gedung
    del dataframe_lantai
    del dataframe_ruang
    del dataframe_jenis
    del dataframe_kategori
    del dataframe_subkategori
    del dataframe_merk
    
    return FileResponse(path=file_name, headers=headerResponse, filename=file_name)
  except Exception as ex:
    return {"status": False, "message": str(ex)}
    
@app.post('/api/import/perangkat-update')
def import_excel_perangkat_update(background_task: BackgroundTasks, file: UploadFile = File(...), nik: str = Form()):
  try:
    dbsima = connect_dbsima()
    cursor = dbsima.cursor()
    
    t0 = time.perf_counter()
    
    contents = file.file.read()
    
    with open(file.filename, 'wb') as f:
      f.write(contents)
      
    dataframe = pandas.read_excel(file.filename,na_values='0',index_col=False,keep_default_na=False,dtype=str,usecols='A:AB')
    dataframe.fillna(0, inplace=True)
    
    data_updated = dataframe.loc[(dataframe['updated_by'] != '') & (dataframe['updated_by'] != '')]
    
    read_time = f"{time.perf_counter() - t0:.1f} seconds"
    
    sql_statement = f"delete from dev_am_perangkat_upload where nik_upload = '{nik}'"
    cursor.execute(sql_statement)
    cursor.commit()
    
    count = 0
    sql_statement = "BEGIN TRANSACTION \r\n"
    
    for index,df in data_updated.iterrows():
      count += 1
      
      sql_statement += f"""
      insert into dev_am_perangkat_upload (id,kode_gedung,kode_room,kode_lantai,kode_merk,kode_lokasi,kode_fm,kode_bm,satuan,jumlah,kapasitas,satuan_kapasitas,model,no_seri,tahun,kondisi,
      kode_milik,keterangan,kode_jenis,kode_kategori,kode_subkategori,is_ceklis,kode_witel,kondisi_terakhir,tahun_pengadaan,status_aktif,updated_by,updated_at,nik_upload)
      values ('{df['id']}','{df['kode_gedung']}','{df['kode_room']}','{df['kode_lantai']}','{df['kode_merk']}','{df['kode_lokasi']}','{df['kode_area']}','{df['kode_bm']}','{df['satuan']}',
      '{df['jumlah']}','{df['kapasitas']}','{df['satuan_kapasitas']}','{df['model']}','{df['no_seri']}','{df['tahun']}','{df['kondisi']}','{df['kode_milik']}','{df['keterangan']}',
      '{df['kode_jenis']}','{df['kode_kategori']}','{df['kode_subkategori']}','{df['cek_birawa']}','{df['kode_witel']}','{df['tanggal_cek']}','{df['waktu_pengadaan']}',
      '{df['status_aktif']}','{df['updated_by']}','{df['updated_at']}','{nik}')
      """
      
      if count % 100 == 0:
        sql_statement += "COMMIT TRANSACTION"
        cursor.execute(sql_statement)
        cursor.commit()
        sql_statement = "BEGIN TRANSACTION \r\n"
    
    if sql_statement != "BEGIN TRANSACTION \r\n":
      sql_statement += "COMMIT TRANSACTION"
      cursor.execute(sql_statement)
      cursor.commit()
    
    file.file.close()
    cursor.close()
    dbsima.close()
    
    background_task.add_task(os.remove, file.filename)
    
    return {"status": True, "message": "Import Excel berhasil", "read_time": read_time, "process_time": f"{time.perf_counter() - t0:.1f} seconds"}
  except Exception as ex:
    return {"status": False, "message": str(ex)}
  
@app.post('/api/scoring/linear-regression')
def calculate_linear_regression(nik_input: Annotated[str, Body(embed=True)], kode_gedung: Annotated[str, Body(embed=True)], tipe_aset: Annotated[str, Body(embed=True)], rasio: Annotated[float, Body(embed=True)], var_model: Annotated[float, Body(embed=True)], x: Annotated[List[float], Body(embed=True)], y: Annotated[List[int], Body(embed=True)]):
  slope, intercept, r, p, std_err = stats.linregress(x, y)
  
  def calculate(x):
        return slope * x + intercept

  def roundup(x):
      return math.ceil(x)+0.000
      
  try:
    dbsima = connect_dbsima()
    cursor = dbsima.cursor()
    
    sql_delete_statement = f"delete from sbr_m_tmp where nik_input = '{nik_input}'"
    cursor.execute(sql_delete_statement)
    cursor.commit()
    
    
    
    nilai = calculate(var_model)
    nilai_bs = roundup(calculate(var_model))
    min_x = min(x)
    max_x = max(x)
    line_reg_start = calculate(min(x))
    line_reg_end = calculate(max(x))
    r_square = r**2
    
    sql_insert_statement = f"""insert into sbr_m_tmp (kode_gedung,tipe_aset,rasio,nilai_bs,pengali,r_square,slope,intercept,r,p_value,standard_error,min_x,max_x,line_reg_start,
    line_reg_end,var_model,nilai,nik_input) 
    values ('{kode_gedung}','{tipe_aset}','{rasio}','{nilai_bs}','0','{r_square}','{slope}','{intercept}','{r}','{p}','{std_err}','{min_x}','{max_x}','{line_reg_start}','{line_reg_end}',
    '{var_model}','{nilai}', '{nik_input}')"""
    cursor.execute(sql_insert_statement)
    cursor.commit()
    
    sql_update_statement = f"update sbr_d_tmp set nilai_base_rent = '{nilai_bs}' where kode_prop = '{kode_gedung}' and nik_input = '{nik_input}'"
    cursor.execute(sql_update_statement)
    cursor.commit()
    
    payload = {'r_squared': r_square, 'slope': slope, 'intercept': intercept, 'r': r, 'p': p, 'standard_error': std_err, 'min_x': min_x, 'max_x': max_x, 'line_reg_start': line_reg_start, 'line_ged_end': line_reg_end, 'x': x, 'y': y}
    result = {'nilai': nilai, 'nilai_bs': nilai_bs, 'var_model': var_model}
    
    return {"status": True, "message": "Success", "payload": payload, "result": result}
  except Exception as ex:
    return {"status": False, "message": str(ex)}