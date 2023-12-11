from pydantic import BaseModel

class Perangkat(BaseModel):
  id: int
  group_id: str | None = None
  kode_area: str | None = None
  unit_id: str | None = None
  nama_unit: str | None = None
  witel_id: str | None = None
  nama_witel: str | None = None
  location_id: str | None = None
  nama_lokasi: str | None = None
  kode_gedung: str | None = None
  nama_gedung: str | None = None
  kelas_id: str | None = None
  room_id: int | None = None
  floor_id: int | None = None
  nama_lantai: str | None = None
  jenis_id: str | None = None
  nama_jenis: str | None = None
  kategori_id: str | None = None
  nama_kategori: str | None = None
  sub_kategori_id: str | None = None
  nama_sub_kategori: str | None = None
  nama_perangkat: str | None = None
  is_ceklis: int | None = None
  merk: str | None = None
  satuan: str | None = None
  jumlah: int | None = None
  kapasitas: str | None = None
  no_seri: str | None = None
  tipe: str | None = None
  tahun: str | None = None
  kondisi: str | None = None
  milik: str | None = None
  keterangan: str | None = None
  perangkat_id: str | None = None