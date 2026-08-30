# `RemoveItem` dan `Purge` — koleksi tidak pernah menghapus key `Base`

> **Status: sudah diimplementasi** di branch `redifu-simplified`.
> Menutup [temuan #3](../relation-evaluation.md) — lubang index yang tidak berhubungan
> dengan TTL, dan satu-satunya yang tersisa setelah
> [`02-set-if-absent.md`](02-set-if-absent.md).

---

## 1. Prinsip

> Kebocoran memori sembuh sendiri lewat TTL. Lubang index tidak sembuh sendiri, karena
> `ZCard > 0` membuat `RequiresSeeding` mengembalikan `false`.

Kalau ragu, pilih bocor daripada bolong. Biaya bocor: beberapa key nganggur, maksimal
selama TTL `Base`. Biaya bolong: data hilang senyap dari koleksi lain, dan tidak ada
yang akan memperbaikinya.

Dari prinsip itu turun satu invariant tanpa pengecualian:

> **Koleksi tidak pernah menghapus key `Base`. Titik.**

---

## 2. Tiga pertanyaan, tiga operasi

| Pertanyaan | Operasi | Redis | `Base` |
|---|---|---|---|
| "masih anggota koleksi ini atau tidak?" | `Collection.RemoveItem` | `ZREM` satu member | tidak disentuh |
| "cache koleksi ini masih bisa dipercaya?" | `Collection.Purge` | `DEL` key index + marker | tidak disentuh |
| "entity-nya masih ada atau tidak?" | `Base.Del` | `DEL` key item | ← memang ini tugasnya |

Bentuk pemanggilannya, setelah §5.5:

```go
PostTimeline.RemoveItem(ctx, post, userRandId)   // ZREM satu member
PostTimeline.Purge(ctx, userRandId)              // DEL index + marker
PostBase.Del(ctx, post)                          // DEL key item
```

Yang membedakan `RemoveItem` dan `Purge` bukan cuma cakupan, tapi pemulihannya:

- **`Purge` adalah invalidasi.** Setelah `DEL`, `ZCard = 0` dan marker bersih →
  `RequiresSeeding` true → koleksi dibangun ulang dari SQL pada fetch berikutnya.
  Self-healing. Dipakai saat definisi koleksi berubah (kolom sort, filter) atau saat
  isinya dicurigai melenceng.
- **`RemoveItem` adalah mutasi.** Setelah `ZREM`, koleksi tetap dianggap valid dan
  **tidak** di-seed ulang — memang itu yang diinginkan. Konsekuensinya: `RemoveItem`
  hanya benar kalau query seeder-mu memang sudah tidak mengembalikan item itu. Kalau di
  DB masih ada, seed berikutnya membawanya kembali, dan itu memang seharusnya — SQL yang
  jadi sumber kebenaran, bukan Redis.

---

## 3. Kenapa tidak ada `DeleteItem`, dan tidak ada `Purge().WithItems()`

Keduanya sempat dipertimbangkan, keduanya ditolak karena alasan yang sama: **cakupan
pemanggilannya berbohong tentang efeknya.**

`PostTimeline.DeleteItem(ctx, post, userRandId)` terbaca seolah cakupannya timeline itu,
padahal `post:X` lenyap untuk semua koleksi yang memuatnya. Kalau post ada di lima
koleksi, ada lima receiver berbeda untuk satu operasi yang efeknya identik dan global —
tidak ada satu pun yang jujur. `Base.Del` jujur, karena `Base` memang penyimpanan
globalnya.

`Purge().WithItems()` gugur karena alasan yang sama, satu tingkat lebih luas.

Ergonomisnya kebetulan jatuh ke arah yang benar juga: lupa `ZREM` menghasilkan lubang,
lupa `Base.Del` cuma menghasilkan kebocoran yang sembuh sendiri. Jadi operasi yang aman
menempel langsung di koleksi, sementara yang berbahaya mengharuskan orang meraih `Base`
dulu.

Refcounting (tiap item menyimpan set berisi index yang memuatnya) **tidak** dipakai:
sorted set bisa mati karena TTL tanpa memberi tahu siapa pun, jadi ref-nya basi dan
`Base` malah tidak pernah terbebaskan. Satu key tambahan per item untuk hasil yang lebih
buruk daripada membiarkan TTL bekerja.

---

## 4. Menghapus entity

Dua panggilan, satu pipeline:

```go
pipe := rdb.Pipeline()
PostTimeline.WithPipeline(pipe).RemoveItem(ctx, post, userRandId)
PostBase.WithPipeline(pipe).Del(ctx, post)
_, err := pipe.Exec(ctx)
```

Ini **satu-satunya** operasi yang sengaja membolongi koleksi lain: `feed:global` yang
masih memuat randId-nya akan melewati item itu sampai di-seed ulang. Itu harga yang
memang mau dibayar — alternatifnya post yang sudah dihapus tetap tersaji dari koleksi
lain sampai TTL habis. Kalau lubang sementara itu tidak dapat diterima untuk suatu
koleksi, `Purge` koleksi tersebut setelahnya; di situ `Purge` bekerja sebagai alat
perbaikan, bukan alat penghapus.

---

## 5. Perubahan

### 5.1 `Sorted.removeItem` — buang `Base.Del`

[`sorted.go:144-167`](../../sorted.go#L144-L167). Hapus blok ini:

```go
errDelBase := srtd.baseClient.WithPipeline(pipe).Del(ctx, item)   // ← hapus
if errDelBase != nil {
    return errDelBase
}
```

Sisanya (`sortedSetClient.RemoveItem` + pola selfPipe) tetap.

### 5.2 `Timeline.removeItem` — sama persis

[`timeline.go:224-264`](../../timeline.go#L224-L264), `Base.Del` di
[baris 232](../../timeline.go#L232).

`TimeSeries.RemoveItem` ([`time-series.go:110`](../../time-series.go#L110)) mewarisi
perbaikan ini lewat `Sorted`, tidak perlu disentuh.

### 5.3 `Remove()` dan `Purge()` jadi identik → gabungkan

Ini konsekuensi yang gampang terlewat. `sortedRemoveBuilder` punya flag `purge` yang
**satu-satunya** efeknya adalah menghapus key `Base`
([`sorted.go:314-329`](../../sorted.go#L314-L329)); di luar itu, `Remove()` dan
`Purge()` sama-sama `DEL` key sorted set-nya
([`sortedset.go:91`](../../sortedset.go#L91)).

Begitu penghapusan `Base` dibuang, keduanya jadi byte-for-byte sama. Maka:

- hapus field `purge` dari `sortedRemoveBuilder` dan `timelineRemovalBuilder`;
- hapus blok `if s.purge { ... }` — termasuk `Fetch`/`FetchAll` di dalamnya;
- hapus method `Sorted.Remove()` ([`sorted.go:235`](../../sorted.go#L235)) dan
  `Timeline.Remove()` ([`timeline.go:406`](../../timeline.go#L406)); sisakan `Purge`,
  dalam bentuk baru di §5.5.

Dua nama untuk operasi yang identik hanya membuat orang menebak-nebak. Kalau breaking
change-nya ingin ditahan dulu, `Remove()` boleh disisakan sebagai alias `Purge()` yang
ditandai deprecated — tapi jangan dua implementasi.

### 5.4 Bug bonus: `Sorted.Purge` tidak membersihkan marker `:blankpage`

Ditemukan saat menelusuri jalur ini. `timelineRemovalBuilder.Exec` membersihkan ketiga
marker-nya ([`timeline.go:600-602`](../../timeline.go#L600-L602)), tapi
`sortedRemoveBuilder.Exec` ([`sorted.go:330-336`](../../sorted.go#L330-L336)) langsung
`Exec` setelah `Delete` — `:blankpage` dibiarkan hidup.

Akibatnya, `Sorted` yang pernah ditandai kosong lalu di-`Purge` akan **tetap** dianggap
kosong: `RequiresSeeding` ([`sorted.go:219`](../../sorted.go#L219)) mengembalikan `false`
selama `IsEmpty` true, jadi koleksi itu tidak pernah dibangun ulang sampai marker-nya
expire sendiri. Purge yang justru mematikan koleksi — kebalikan dari maksudnya.

Perbaikannya satu baris, sebelum `pipe.Exec`:

```go
if err := s.srtd.HasData(ctx, pipe, s.keyParams...); err != nil {   // DEL :blankpage
    return err
}
```

`Page.Purge` ([`page.go:98`](../../page.go#L98)) ikut benar dengan sendirinya, karena ia
mem-purge tiap halaman lewat `sorted.Purge()`.

### 5.5 `Purge` jadi method biasa, builder-nya dihapus

Builder di redifu ada untuk menampung **knob opsional**, bukan untuk menampung
`keyParams`. Hampir seluruh API mengambil `keyParams ...string` sebagai variadic di ekor
— `AddItem`, `RemoveItem`, `IngestItem`, `SetExpiration`, `RequiresSeeding`, `IsEmpty`,
`Count`, seluruh marker. Builder hanya dipakai di dua tempat:

| Builder | Knob yang dibawa |
|---|---|
| `Fetch` (4 struktur) | `WithProcessor`, `processorArgs`, `WithRange`, cursor, direction |
| `Remove()` / `Purge()` (Sorted + Timeline) | `params` dan flag `purge` |

Begitu flag `purge` dibuang di §5.3, builder itu tinggal membawa satu field —
`keyParams` — dan jadi builder yang tidak membangun apa pun. Bahwa bentuk method biasa
sudah cukup pun sudah terbukti di repo sendiri: `Page.Purge`
([`page.go:98`](../../page.go#L98)) memang sudah berbentuk demikian, sementara
`Sorted.Purge` dan `Timeline.Purge` builder — operasi yang sama, dua bentuk berbeda, di
library yang sama.

Ratakan ke bentuk `Page`:

```go
func (srtd *Sorted[T]) Purge(ctx context.Context, keyParams ...string) error
func (cr *Timeline[T]) Purge(ctx context.Context, keyParams ...string) error
```

Yang dihapus: tipe `sortedRemoveBuilder` ([`sorted.go:303-336`](../../sorted.go#L303-L336))
dan `timelineRemovalBuilder` ([`timeline.go:568-604`](../../timeline.go#L568-L604))
beserta `WithParams`/`Exec` masing-masing — sekitar 70 baris.

Hasilnya pemanggilan yang sebaris dengan tetangganya:

```go
PostTimeline.RemoveItem(ctx, post, userRandId)
PostTimeline.Purge(ctx, userRandId)
```

bukan `PostTimeline.Purge().WithParams(userRandId).Exec(ctx)` — tiga panggilan untuk satu
`DEL`.

Builder tetap dipertahankan di `Fetch`, karena di situ ia memang dipakai.

Breaking change, tapi tertangkap compiler.

### 5.6 Efek samping: `Purge` jadi jauh lebih murah

Sekarang `Purge` memanggil `Fetch`/`FetchAll` atas **seluruh** isi koleksi — N kali `GET`
plus resolve relasi — semata untuk mendapatkan item yang akan di-`Del`. Setelah
perubahan ini, seluruh langkah itu hilang dan `Purge` tinggal satu `DEL` plus pembersihan
marker.

---

## 6. Yang paling gawat sekarang: `Page`

[`CLAUDE.consumer.md`](../../CLAUDE.consumer.md) menyatakan cara mengupdate sebuah page
adalah **purge lalu re-seed** — pages are snapshots. Tapi `Page.Purge`
([`page.go:98`](../../page.go#L98)) mem-purge tiap halaman lewat `sorted.Purge()`, yang
menghapus key `Base` setiap item ([`sorted.go:324`](../../sorted.go#L324)).

Artinya jalur update yang direkomendasikan dokumen sendiri menghapus `post:X` dari
`Base`, padahal post yang sama ada di timeline dan koleksi lain. Setelah pemisahan ini,
`Page.Purge` jadi benar tanpa perubahan tambahan.

---

## 7. Breaking change dan dokumen yang harus dibalik

1. **Invariant #1 di [`CLAUDE.md`](../../CLAUDE.md)** sekarang berbunyi *"Never delete an
   item from Base without removing it from the index. Always use `RemoveItem` (which
   handles both), not `Base.Del` alone."* — itu persis perilaku yang dibuang. Ganti
   menjadi: `RemoveItem` hanya menyentuh index; `Base.Del` adalah satu-satunya cara
   menghapus entity; setelah `Base.Del`, index lain yang masih memuat randId-nya akan
   pendek sampai di-seed ulang atau di-`Purge`.
2. **Baris di "What not to do" [`CLAUDE.consumer.md`](../../CLAUDE.consumer.md)** — *"Do
   not delete an item with `Base.Del` alone — always use `RemoveItem` on its
   collection"* — dibalik menjadi anjuran idiom dua panggilan di §4.
3. **Perubahan perilaku senyap.** Kode consumer yang memanggil `RemoveItem` dengan
   harapan item ikut terhapus akan berubah tanpa error kompilasi: item-nya jadi
   bertahan. Arahnya aman (bocor, bukan bolong), tapi wajib tertulis di catatan rilis.
4. **Penghapusan `Remove()`** (kalau tidak disisakan sebagai alias) adalah breaking
   change yang tertangkap compiler — aman.

---

## 8. Test yang menyertai

Pakai `miniredis`:

1. Item ada di dua index; `RemoveItem` dari index A → item tetap terambil lewat index B.
   *(regression langsung untuk temuan #3)*
2. `RemoveItem` → key `Base` masih ada dan TTL-nya tidak berubah.
3. `Purge` → key index dan seluruh marker hilang, key `Base` semua member masih ada.
4. `Purge` atas koleksi yang `:blankpage`-nya diset → `RequiresSeeding` mengembalikan
   `true` sesudahnya. *(regression untuk §5.4)*
5. `Base.Del` + `RemoveItem` dalam satu pipeline → satu `Exec`, keduanya berlaku.

---

## 9. Checklist

- [x] Buang `Base.Del` dari `Sorted.removeItem` ([`sorted.go:151`](../../sorted.go#L151))
- [x] Buang `Base.Del` dari `Timeline.removeItem` ([`timeline.go:232`](../../timeline.go#L232))
- [x] Buang field `purge` + blok penghapusan `Base` dari kedua removal builder
- [x] Hapus (atau deprecate jadi alias) `Sorted.Remove()` dan `Timeline.Remove()`
- [x] `Purge` jadi method biasa; hapus `sortedRemoveBuilder` dan `timelineRemovalBuilder`
- [x] `HasData` di `sortedRemoveBuilder.Exec` ([`sorted.go:330`](../../sorted.go#L330))
- [x] Lima test di §8
- [x] Invariant #1 `CLAUDE.md` dan baris "What not to do" `CLAUDE.consumer.md`
- [x] Catatan rilis untuk perubahan perilaku senyap `RemoveItem`
