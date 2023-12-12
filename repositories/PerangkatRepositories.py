from pyodbc import Connection

class PerangkatRepositories:
  def __init__(self, connection: Connection) -> None:
    self.connection = connection
    
  def get_data_perangkat(self, kode_jenis: str | None = None,kode_lokasi: str | None = None, tahun: str | None = None, kode_area: str | None = None, kode_fm: str | None = None, kode_bm: str | None = None, kode_ktg: str | None = None, kode_subktg: str | None = None):
    if kode_jenis is None:
      where_jenis = ''
    else:
      where_jenis = f"and a.jid in (${kode_jenis})"
      
    if kode_lokasi is None:
      where_lokasi = ''
    else:
      where_lokasi = f"and a.location_id in (${kode_lokasi})"
      
    if tahun is None:
      where_tahun = ''
    else:
      where_tahun = f"and a.tahun in (${tahun})"
      
    if kode_area is None:
      where_area = ''
    else:
      where_area = f"and e.kode_area in (${kode_area})"
    
    if kode_fm is None:
      where_fm = ''
    else:
      where_fm = f"and e.kode_fm in (${kode_fm})"
      
    if kode_bm is None:
      where_bm = ''
    else:
      where_bm = f"and e.kode_bm in (${kode_bm})"
      
    if kode_ktg is None:
      where_kategori = ''
    else:
      where_kategori = f"and a.kid in (${kode_ktg})"
      
    if kode_subktg is None:
      where_subkategori = ''
    else:
      where_subkategori = f"and a.skid in (${kode_subktg})"
    
    try:
      ## SET CONNECTION
      cursor = self.connection.cursor()
      ## SELECT DATA
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
        where e.flag_aktif <> '3' {where_jenis} {where_lokasi} {where_tahun} {where_area} {where_fm} {where_bm} {where_kategori} {where_subkategori}
      """)
      
      return cursor.fetchall()
    except Exception:
      return Exception
    finally:
      self.connection.close()