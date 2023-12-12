import pandas
from datetime import datetime
from repositories.PerangkatRepositories import PerangkatRepositories


class PerangkatServices:
  def __init__(self, repositories: PerangkatRepositories) -> None:
    self.repositories = repositories

  def get_data_perangkat(self, kode_jenis: str | None = None,kode_lokasi: str | None = None, tahun: str | None = None, kode_area: str | None = None, kode_fm: str | None = None, kode_bm: str | None = None, kode_ktg: str | None = None, kode_subktg: str | None = None):
    columns = ["no", "id_group", "id_area", "id_unit", "nama_unit", "id_witel", "nama_witel", "id_location", "nama_lokasi", "id_gedung", "nama_gedung", "id_kelas", "id_room", "id_lantai",
               "nama_lantai", "id_jenis", "nama_jenis", "id_kategori", "nama_kategori", "id_subkategori", "nama_subkategori", "nama_perangkat", "is_ceklis", "merk", "satuan", "jumlah",
               "kapasitas", "no_seri", "tipe", "tahun", "kondisi", "milik", "keterangan", "id_perangkat"];
    
    result = self.repositories.get_data_perangkat(kode_jenis, kode_lokasi, tahun, kode_area, kode_fm, kode_bm, kode_ktg, kode_subktg)
    
    dataframe = pandas.DataFrame.from_records(result, columns=columns)
    
    
    today = datetime.today()
    unique_id = today.strftime('%Y%m%d%H%M%S')
    
    file_name = f'DATA_PERANGKAT_{unique_id}.xlsx'
    
    writer = pandas.ExcelWriter(file_name)
    
    dataframe.to_excel(writer,index=False)
    
    writer.close()
    
    return file_name