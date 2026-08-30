# Evaluasi Implementasi Redifu — Fokus pada Fitur Relation

> Tanggal analisis: 2026-08-29
> Commit: `742715e`
> Cakupan: seluruh package `redifu`, dengan penekanan pada mekanisme Relation dan
> sejauh mana implementasi saat ini memenuhi tujuan singletonisasi cache.
>
> Rencana perbaikan yang lahir dari temuan #1, #2, #6, dan #7 di bawah ditulis sebagai
> panduan pemakaian di [`plan-2026-08-30/01-relation.md`](plan-2026-08-30/01-relation.md).

---

## Konteks dan tolok ukur

Tujuan repository ini adalah menerapkan singletonisasi untuk setiap key yang disimpan
di cache, guna mengurangi frekuensi pembacaan ke database. Redis bukan pengganti
database — database tetap source of truth, Redis hanya lapisan cache yang menjaga
objek yang telah di-*seed* tetap tersedia selama belum dihapus eksplisit atau TTL-nya
belum habis.

Dua konsekuensi desain yang jadi tolok ukur evaluasi ini:

1. Pembaruan satu item **tidak boleh** memaksa penghapusan seluruh cache list dan
   re-seeding dari database. Cache harus bisa diperbarui secara terarah.
2. Pembaruan satu item **harus** langsung tercermin di seluruh cache list yang
   mereferensikan item tersebut, tanpa membangun ulang list dari database.

Temuan di bawah diurutkan berdasarkan seberapa jauh ia menjauhkan implementasi dari
dua tolok ukur tersebut.

---

## 1. Relation tidak pernah aktif untuk tipe value — gagal diam-diam

**Severity: kritis. Fitur inti mati total tanpa menghasilkan error.**

Di [`sortedset.go:181`](../sortedset.go#L181):

```go
v := reflect.ValueOf(fetchedItem)
if v.Kind() == reflect.Ptr { v = v.Elem() }
...
if !relationAttrField.IsValid() || !relationAttrField.CanSet() { continue }
```

`reflect.ValueOf` atas sebuah **struct value** menghasilkan `reflect.Value` yang tidak
addressable. Akibatnya `CanSet()` selalu `false`, dan blok resolusi relasi selalu
jatuh ke `continue`.

Dokumentasi konsumen ([`CLAUDE.consumer.md:47`](../CLAUDE.consumer.md#L47)) memakai
tipe value: `Base[Post]`, `NewRelation[account.Account]`. Ini sah secara tipe karena
`item.Foundation` di-embed sebagai pointer, sehingga method set `*Foundation`
ter-promote ke `Post` value dan `Post` tetap memenuhi `item.Blueprint`.

### Verifikasi

Blok relasi direplikasi persis dan dijalankan atas `Post` value dan `*Post`:

```
value type:   relation SILENTLY SKIPPED (CanSet == false)
post.Account after fetch = {Foundation:<nil> Name:}
pointer type: relation RESOLVED
NewRelation err=<nil>  itemAttribute="Account" randIdAttribute="AccountRandId"
```

### Kenapa berbahaya

`NewRelation` berhasil. `AddRelation` berhasil. `Fetch` tidak mengembalikan error.
Konsumen hanya menerima `Account` kosong, lalu menyimpulkan datanya memang belum ada
di DB — dan kemungkinan besar "memperbaikinya" dengan menyimpan objek `Account` penuh
di dalam `Post`. Itu persis membatalkan seluruh tujuan repository ini.

### Perbaikan

Bekerja di atas salinan yang addressable, lalu tulis balik:

```go
ptr := reflect.New(reflect.TypeOf(fetchedItem))
ptr.Elem().Set(reflect.ValueOf(fetchedItem))
v := ptr.Elem()
if v.Kind() == reflect.Ptr { v = v.Elem() }

// ... resolve semua relasi di sini ...

fetchedItem = ptr.Elem().Interface().(T)
```

Selain itu, `continue` di jalur relasi jangan senyap. Bedakan minimal dua kasus:

- randId kosong → wajar, lanjut tanpa pesan;
- field tidak bisa di-set / tidak valid → bug konfigurasi, harus menjadi error.

---

## 2. Hasil fetch tidak aman ditulis balik — sekali write-back, singleton hilang

**Severity: kritis. Kerusakan bersifat permanen sampai TTL habis.**

[`sortedset.go:209`](../sortedset.go#L209) mengosongkan `AccountRandId` setelah field
relasi diisi. Item hasil `Fetch` karenanya berbentuk **flat**: `Account` terisi penuh,
`AccountRandId` kosong.

Pola read-modify-write sangat wajar dilakukan konsumen:

```go
posts := PostTimeline.Fetch(nil).Exec(ctx).Items()
p := posts[0]
p.Title = "judul baru"
PostBase.Set(ctx, p)   // ← merusak singleton
```

Setelah baris terakhir, key `post:X` di Redis menyimpan **salinan penuh** `Account`,
dan penunjuk `AccountRandId` sudah hilang. Artinya:

- update `Account` tidak lagi tercermin di `Post` tersebut — janji utama repo gugur;
- tidak ada jalan pulih, karena penunjuk yang dibutuhkan untuk resolve sudah tidak ada.

Ini juga bisa terjadi dari dalam library sendiri: `Sorted.addItem` dan
`Timeline.addItem` memanggil `baseClient.Set` untuk item yang belum ada di Base, tanpa
normalisasi apa pun.

### Opsi perbaikan

| Opsi | Deskripsi | Catatan |
|------|-----------|---------|
| **1 (disarankan)** | Jangan hapus randId. Biarkan `AccountRandId` tetap terisi. | Write-back aman dan idempoten. Redundansi payload ditutup dengan tag `json:"-"` pada field relasi, sehingga yang tersimpan tetap hanya randId. |
| 2 | Sediakan `Detach(item)` di `RelationFormat`, panggil otomatis di setiap jalur tulis. | Perlu `Base.AddRelation` juga, karena `Base` sekarang tidak tahu apa-apa soal relasi. |
| 3 | `Fetch` mengembalikan view terpisah, tidak memutasi `T`. | Paling bersih secara desain, paling besar perubahan API-nya. |

Opsi 1 dengan `json:"-"` adalah yang termurah dan langsung menutup seluruh kelas bug
write-back.

---

## 3. `RemoveItem` menghapus key Base — item lenyap dari semua list lain

**Severity: kritis. Kebalikan langsung dari tujuan singletonisasi.**

[`sorted.go:151`](../sorted.go#L151) dan [`timeline.go:232`](../timeline.go#L232):

```go
errDelBase := srtd.baseClient.WithPipeline(pipe).Del(ctx, item)
errDel := srtd.sortedSetClient.RemoveItem(ctx, pipe, item, keyParams...)
```

Bila `post:X` berada di `feed:user:123` **dan** `feed:global`, maka
`RemoveItem(post, "123")` menghapus `post:X` sepenuhnya dari Base. Sorted set
`feed:global` masih menyimpan randId-nya, dan `Fetch` akan `continue` melewatinya
([`sortedset.go:174`](../sortedset.go#L174)).

Akibatnya item hilang dari feed global tanpa jejak, sementara `RequiresSeeding` tetap
mengembalikan `false` karena `ZCard > 0`. Cache menjadi diam-diam bolong dan tidak
akan pernah pulih sendiri.

`Purge` ([`timeline.go:590`](../timeline.go#L590)) punya masalah yang sama, dengan
cakupan lebih luas.

### Perbaikan

Pisahkan dua operasi yang sekarang menyatu:

- `RemoveItem` → hanya `ZRem` dari index yang bersangkutan;
- `DeleteItem` / `Forget` → hapus key Base, eksplisit, hanya dipanggil saat entitas
  benar-benar mati di DB.

Alternatif otomatis adalah refcounting (sebuah set berisi daftar index yang memuat
item, dievaluasi dengan `SCARD`), tetapi pemisahan eksplisit lebih jujur dan lebih
mudah dinalar untuk sebuah lapisan cache.

### Catatan dokumentasi

Invariant #1 di [`CLAUDE.md`](../CLAUDE.md) hanya melarang arah sebaliknya — "jangan
hapus item dari Base tanpa menghapusnya dari index". Arah yang justru berbahaya ini
belum terdokumentasi sama sekali.

---

## 4. Relation miss menghasilkan objek separuh kosong, tanpa sinyal

**Severity: tinggi.**

[`sortedset.go:197`](../sortedset.go#L197):

```go
relationItem, errGet := relationFormat.GetByRandId(ctx, relationRandId)
if errGet != nil { continue }
```

Bila `account:a1` sudah expired atau kena eviction, `Post` tetap dikembalikan dengan
`Account` kosong. Tidak ada error, tidak ada hook, dan pemanggil tidak punya cara
untuk tahu.

Ini penting karena ketidaksinkronan TTL memang bagian dari desain — konvensi di
dokumen konsumen adalah item 7 hari dan sorted set 2 hari, sementara TTL `Account` dan
TTL `Post` dikelola oleh dua instance `Base` berbeda dan bisa jauh berbeda.

Menariknya, mesinnya sudah ada tetapi tidak dipakai: `MarkAsMissing` dan `IsMissing` di
[`base.go:158-197`](../base.go#L158-L197) tidak pernah dipanggil dari mana pun di
dalam library.

### Perbaikan

Jalur relasi adalah tempat paling tepat untuk memakai mesin tersebut, ditambah sebuah
callback seperti `OnRelationMiss(randId)` agar konsumen dapat me-*re-seed* satu
entitas dari DB — bukan membuang seluruh list.

---

## 5. TTL relasi tidak pernah diperpanjang saat dibaca

**Severity: tinggi.**

`Base.Get` ([`base.go:38`](../base.go#L38)) memakai `GET` biasa. Sebuah `Account` yang
dirujuk ribuan `Post` dan dibaca terus-menerus tetap akan expire tepat 7 hari setelah
ditulis.

Ini bertentangan dengan tujuan menjaga objek yang telah di-seed tetap tersedia:
entitas yang paling panas justru yang paling sering hilang. Dan hilangnya satu
`Account` langsung mengosongkan author di semua `Post` yang merujuknya (lihat
temuan #4).

### Perbaikan

`GETEX key EX <ttl>` menyelesaikan ini dalam satu round-trip. Sebaiknya dijadikan opsi
(misalnya `Base` dengan flag `refreshOnRead`), karena tidak semua entitas menginginkan
sliding TTL.

---

## 6. N+1 dan tidak ada dedupe — singleton menghemat memori, bukan round-trip

**Severity: tinggi. Menabrak tujuan "mengurangi frekuensi pembacaan".**

Loop di [`sortedset.go:172-217`](../sortedset.go#L172-L217) melakukan, untuk satu
halaman 20 item dengan 2 relasi:

- 20 `GET` untuk item, ditambah
- 40 `GET` untuk relasi

= **60 round-trip serial**.

Dan tidak ada memoisasi: 20 post dari author yang sama menghasilkan 20 `GET` ke key
`account:a1` yang sama persis, di dalam satu pemanggilan `Fetch`. Ironisnya, justru
identitas tunggal itulah yang membuat dedupe menjadi trivial.

### Perbaikan

Dua langkah yang berdiri sendiri:

1. Ganti loop `Get` per item dengan satu `MGET` atas seluruh `listRandIds`.
2. Kumpulkan dulu semua randId relasi, dedupe ke dalam `map[string]interface{}`,
   lakukan satu `MGET` per tipe relasi, baru sebar hasilnya ke tiap item.

Hasilnya: dari 60 round-trip menjadi 3.

---

## 7. Relation dipilih berdasar tipe, bukan nama field

**Severity: menengah.**

[`main.go:120-133`](../main.go#L120-L133) mengambil **field pertama** yang tipenya
cocok. Bila `Post` memiliki `Author Account` dan `Editor Account`, hanya satu yang
dapat dipetakan — dan yang mana bergantung pada urutan deklarasi field.

Lebih jauh, `identifier` pada `AddRelation("author", ...)` **tidak dipakai sama sekali**
saat resolve. [`sortedset.go:179`](../sortedset.go#L179) melakukan
`for _, relationFormat := range relation`, mengabaikan key-nya. Jadi identifier hanya
dekorasi, dan urutan resolve mengikuti iterasi map Go yang acak.

### Perbaikan

Sediakan varian yang menyebut field secara eksplisit, misalnya:

```go
redifu.NewRelationFor[Account](base, redifu.TypeOf[Post](), "Editor")
```

dengan `identifier` dipakai sebagai kunci yang sungguh-sungguh berarti.

---

## 8. Temuan lebih kecil di sekitar Relation

### 8.1 `String()` dipanggil tanpa cek `Kind()`

[`sortedset.go:192`](../sortedset.go#L192) memanggil `relationRandIdField.String()`
tanpa memeriksa kind-nya. Bila field `XxxRandId` ternyata bukan string,
`reflect.Value.String()` mengembalikan `"<int Value>"` — bukan string kosong —
sehingga lookup tetap berjalan dengan id sampah.

Tambahkan `if relationRandIdField.Kind() != reflect.String { continue }`, atau lebih
baik lagi validasi ini di `NewRelation` agar gagal saat startup, bukan saat fetch.

### 8.2 Error `SetItem` dibuang di dokumentasi

`RelationFormat.SetItem` ([`main.go:104`](../main.go#L104)) melakukan `item.(T)`.
Contoh di [`CLAUDE.consumer.md:374`](../CLAUDE.consumer.md#L374) memanggil
`rel.SetItem(ctx, author)` dan membuang return error-nya. Bila ada ketidakcocokan
value/pointer, seeding relasi gagal total tanpa suara. Contoh di dokumen sebaiknya
menangani error tersebut.

### 8.3 `Base.Get` tidak resolve relasi

Mengambil satu `Post` lewat `Base.Get` menghasilkan bentuk objek yang berbeda dari
`Post` yang sama lewat `Timeline.Fetch` — satu flat, satu tidak. Bentuk objek yang
bergantung pada jalur pengambilan adalah sumber bug yang halus.

### 8.4 Relasi bersarang tidak didukung

Hanya satu tingkat yang di-resolve. Bila `Account` sendiri memiliki relasi, relasi itu
tidak ikut ter-resolve.

---

## 9. Di luar Relation, tetapi menabrak tujuan yang sama

### 9.1 `Base.Set` tidak menyentuh score di sorted set

[`CLAUDE.consumer.md:314`](../CLAUDE.consumer.md#L314) menyarankan
`PostBase.Set(ctx, updatedPost)` untuk update. Itu benar untuk isi, tetapi bila
`sortingReference` diset ke `UpdatedAt`, urutan di semua sorted set menjadi basi dan
tidak ada yang memperbaikinya.

Perlu `UpdateItem` yang melakukan `Set` + `ZADD` ulang pada index yang relevan, atau
setidaknya peringatan eksplisit di dokumentasi.

### 9.2 Bug: cek pipe terbalik di TimeSeries

[`time-series.go:87-91`](../time-series.go#L87-L91):

```go
if pipe != nil {
    errAdd = s.sorted.AddItem(ctx, item, keyParams...)           // bikin pipeline sendiri + Exec
} else {
    errAdd = s.sorted.WithPipeline(pipe).AddItem(ctx, item, ...) // pipe di sini nil
}
```

Kedua cabang tertukar. Saat pemanggil menyerahkan pipeline, pipeline itu diabaikan dan
operasi langsung dieksekusi — melanggar Pipeline Discipline (invariant #2 di
[`CLAUDE.md`](../CLAUDE.md)). Cabang `else` kebetulan tetap berjalan karena `addItem`
menangani pipe `nil`.

### 9.3 Bug: `processorArgs` hilang satu level variadic

[`page.go:145`](../page.go#L145) dan [`time-series.go:365`](../time-series.go#L365)
memanggil `WithProcessor(f.processor, f.processorArgs)` — bukan `f.processorArgs...`.

Akibatnya args terbungkus menjadi `[]interface{}{[]interface{}{...}}`, dan contoh
`args[0].(string)` di [`CLAUDE.consumer.md:346`](../CLAUDE.consumer.md#L346) akan panic
lewat jalur Page dan TimeSeries. Jalur Timeline aman karena builder-nya meneruskan
dengan benar.

### 9.4 `Base.Exists` adalah mutator yang salah nama

[`base.go:200`](../base.go#L200) hanyalah alias `UnmarkMissing` — namanya menyiratkan
pemeriksaan, isinya `DEL`. Fungsi bernama `Exists` yang justru menulis adalah jebakan;
sebaiknya dihapus atau diganti nama.

### 9.5 `keyParam != nil` seharusnya `len(keyParam) > 0`

[`base.go:68`](../base.go#L68), [`base.go:100`](../base.go#L100), dan
[`base.go:121`](../base.go#L121). Slice kosong non-nil lolos pemeriksaan, lalu
`keyParam[0]` panic. Belum terpicu dari dalam library, tetapi terbuka untuk kode
konsumen yang meneruskan slice.

### 9.6 `getFieldValue` memakai sentinel, bukan error

[`main.go:18`](../main.go#L18) mengembalikan `time.Time{}` saat field tidak ditemukan.
Nilai itu kemudian dipakai langsung sebagai argumen cursor di
[`sql.go:106`](../sql.go#L106) — salah ketik nama field menghasilkan query dengan
cursor zero-time, bukan error. Seharusnya mengembalikan `(interface{}, error)`.

---

## 10. Tidak ada test sama sekali

**Severity: proses.**

`main_test.go` hanya berisi deklarasi package. Untuk library yang seluruh nilainya
terletak pada perilaku konsistensi cache yang halus, inilah yang membuat temuan #1
bisa hidup sampai sekarang — satu test sederhana atas resolusi relasi akan langsung
menangkapnya.

`miniredis` cukup untuk menutup sebagian besar permukaan ini tanpa Redis sungguhan.
Prioritas urutan test:

1. Resolusi relasi, untuk tipe value dan pointer.
2. Round-trip `Fetch` → `Set` (regression untuk temuan #2).
3. `RemoveItem` atas item yang berada di dua index (regression untuk temuan #3).
4. Transisi marker `blankpage` / `firstpage` / `lastpage`.

---

## Ringkasan prioritas

| # | Temuan | Dampak |
|---|--------|--------|
| 1 | Relation tidak jalan untuk tipe value | Fitur inti mati total, tanpa error |
| 3 | `RemoveItem` menghapus key Base bersama | Item lenyap senyap dari list lain |
| 2 | Hasil fetch tidak aman ditulis balik | Singleton rusak permanen sekali write-back |
| 4 | Relation miss senyap | Objek separuh kosong sampai ke pengguna |
| 6 | N+1 tanpa dedupe | 60 round-trip untuk satu halaman |
| 5 | TTL relasi tidak diperpanjang | Entitas terpanas paling cepat hilang |
| 9 | Pipe terbalik, `processorArgs` | Bug konkret, perbaikan satu baris |
| 10 | Nol test | Alasan temuan #1 bisa lolos sejauh ini |

### Urutan pengerjaan yang disarankan

1. **Temuan #1 dan #3** lebih dulu. Keduanya perbaikan kecil dan terlokalisasi, tetapi
   keduanya persis membatalkan tujuan yang dirumuskan repository ini.
2. **Temuan #2** menyusul. Solusinya — biarkan randId tetap terisi, pakai `json:"-"`
   pada field relasi — sekaligus menghilangkan seluruh kelas bug write-back.
3. **Temuan #9.2 dan #9.3** kapan saja; keduanya perbaikan satu baris.
4. **Temuan #10** sebaiknya berjalan beriringan dengan #1–#3, sebagai regression guard.
