# Plan — 30 Agustus 2026

Rencana perubahan yang belum diimplementasi, ditulis lebih dulu supaya bisa dinilai
sebelum ada kode yang ditulis. Semuanya berangkat dari
[`../relation-evaluation.md`](../relation-evaluation.md).

| # | Dokumen | Isi | Menutup temuan |
|---|---|---|---|
| 01 | [`01-relation.md`](01-relation.md) | Relation berbasis closure (`Relate`), resolve batch, randId dipertahankan | #1, #2, #6, #7 |
| 02 | [`02-set-if-absent.md`](02-set-if-absent.md) | `AddItem` memperbarui TTL item existing lewat `SetIfAbsent` | — (temuan baru) |
| 03 | [`03-remove-purge.md`](03-remove-purge.md) | `RemoveItem` dan `Purge` berhenti menghapus key `Base` | #3 |

## Dua aturan yang menyatukannya

> **Setiap `ZADD` harus dibarengi tulis ke `Base` di pipeline yang sama** — untuk item
> itu sendiri, dan untuk entity yang direlasikannya.

Kalau aturan ini dipegang, umur key `Base` selalu ≥ umur entri yang merujuknya di sorted
set, sehingga index miss maupun relation miss jadi mustahil. Dokumen 02 menutup
pelanggaran aturan ini untuk item; dokumen 01 §5 dan §9 menetapkannya sebagai invariant
untuk entity relasi.

> **Koleksi tidak pernah menghapus key `Base`. Titik.**

Turunan dari prinsip "kebocoran sembuh sendiri lewat TTL, lubang index tidak". Dokumen 03
menegakkannya, dan dengan itu `RemoveItem`, `Purge`, dan `Base.Del` masing-masing
menjawab satu pertanyaan yang berbeda: *masih anggota?*, *cache-nya masih dipercaya?*,
*entity-nya masih ada?*

## Urutan pengerjaan

1. **02** lebih dulu — kecil, terlokalisasi, dan menjadi prasyarat bagi argumen TTL di
   01 §9.
2. **03** berikutnya — juga terlokalisasi, dan menutup satu-satunya lubang index yang
   tersisa setelah 02.
3. **01** menyusul, beserta migrasi consumer (`Base[*Post]`, tag `json:"-"`, ganti
   `NewRelation` → `Relate`).
4. Test `miniredis` berjalan beriringan, bukan setelahnya — temuan #1 di evaluasi bisa
   hidup selama ini justru karena nol test.

## Belum masuk rencana ini

- Bug satu baris di luar Relation: pipe terbalik di `TimeSeries`
  ([temuan #9.2](../relation-evaluation.md)) dan `processorArgs` yang hilang satu level
  variadic ([temuan #9.3](../relation-evaluation.md)).
