# `SetIfAbsent` — TTL item diperbarui di setiap `AddItem`

> **Status: sudah diimplementasi** di branch `redifu-simplified`.
> Prasyarat bagi argumen TTL di [`01-relation.md` §9](01-relation.md#9-yang-sengaja-tidak-dilakukan).
> Dasar temuan: [`relation-evaluation.md`](../relation-evaluation.md).

---

## 1. Aturan yang sedang dijaga

> **Setiap `ZADD` harus dibarengi tulis ke `Base` di pipeline yang sama.**

Kalau aturan ini dipegang, umur key `Base` selalu ≥ umur entri yang merujuknya di sorted
set, dan index miss jadi mustahil. Seluruh dokumen ini tentang satu tempat di mana kode
sekarang melanggarnya.

---

## 2. Bug

`AddItem` tidak memperbarui TTL item yang sudah ada di `Base`.

[`sorted.go:80-110`](../../sorted.go#L80-L110) dan
[`timeline.go:119-149`](../../timeline.go#L119-L149):

```go
_, errGet := srtd.baseClient.Get(ctx, item.GetRandId())   // round-trip sinkron
...
if errors.Is(errGet, redis.Nil) {
    errSet := srtd.baseClient.WithPipeline(pipe).Set(ctx, item)   // ← hanya kalau belum ada
}
errIngest := srtd.IngestItem(ctx, pipe, item, false, keyParams...)  // ZADD tetap jalan
```

Item yang sudah ada masuk ke sorted set membawa **sisa umur lamanya**. Tidak ada jalur
lain yang menambalnya: satu-satunya penulis TTL key item adalah
[`base.go:78`](../../base.go#L78) lewat argumen `SET ... EX`, dan `Base.Get`
([`base.go:41`](../../base.go#L41)) memakai `GET` biasa — bukan `GETEX` — jadi membaca
item tidak memperpanjang apa pun. Tidak ada satu pun `pipe.Expire` terhadap key item di
seluruh package.

### Kenapa "TTL Base 7 hari > TTL sorted set 2 hari" tidak cukup

TTL item dihitung sejak terakhir kali ia **ditulis**, bukan sejak ia di-`ZADD`. Syarat
amannya:

```
sisa umur key Base saat di-ZADD  >  sisa umur sorted set
```

Itu pecah begitu item lebih tua dari (TTL Base − TTL sorted set) = **5 hari** saat
ditambahkan:

```
20 Agu   post:X di-Set oleh seed koleksi lain   → post:X mati 27 Agu
26 Agu   sorted set S dibuat, SetExpiration     → S mati 28 Agu
26 Agu   AddItem(X, S) → X sudah ada → TTL-nya TIDAK diperbarui
27 Agu   post:X mati, S masih hidup sehari      → S bolong
         ZCard > 0 → RequiresSeeding false      → tidak pulih sendiri
```

Bentuk nyatanya: user mem-bookmark post lama, post lama masuk trending, moderator
mem-feature post lama. Item yang di-`AddItem` justru sering bukan item yang baru dibuat.

Kerusakannya senyap dan tidak sembuh sendiri: `Fetch` melewati randId yang key-nya hilang
([`sortedset.go:174`](../../sortedset.go#L174)) tanpa error, sementara `ZCard > 0`
membuat `RequiresSeeding` mengembalikan `false`. Halaman jadi pendek sampai sorted
set-nya expire.

---

## 3. Kenapa bukan "selalu `Set`"

Menghapus cabang `if errors.Is(errGet, redis.Nil)` memang menutup bug ini, tapi mengubah
semantik `AddItem` dan menimbulkan regresi yang lebih buruk.

Cabang itu bukan kelalaian — ia menyatakan bahwa `AddItem` bersifat
**insert-if-absent**, dan `Base` tetap source of truth. Consumer karenanya boleh
memanggil `AddItem` dengan item **stub** — hanya `RandId` plus field sorting — misalnya
dari payload event, semata untuk menempatkannya ke index kedua. Kalau `Set` dijadikan
tanpa syarat, stub itu menimpa value lengkap di `Base`, dan hilangnya senyap.

Jalur update payload sudah ada dan sudah didokumentasikan: `Base.Set`. Jadi `AddItem`
cukup memperbarui TTL.

---

## 4. Perubahan

### 4.1 `Base` — method baru

Di sebelah `set` ([`base.go:63`](../../base.go#L63)):

```go
func (cr *Base[T]) SetIfAbsent(ctx context.Context, item T) error {
    return cr.setIfAbsent(ctx, nil, item)
}

func (cr *Base[T]) setIfAbsent(ctx context.Context, pipe redis.Pipeliner, item T) error {
    key := fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())

    itemInByte, err := json.Marshal(item)
    if err != nil {
        return err
    }

    if pipe != nil {
        pipe.SetNX(ctx, key, string(itemInByte), cr.timeToLive)  // isi hanya kalau belum ada
        pipe.Expire(ctx, key, cr.timeToLive)                     // TTL selalu diperbarui
    } else {
        if res := cr.client.SetNX(ctx, key, string(itemInByte), cr.timeToLive); res.Err() != nil {
            return res.Err()
        }
        if res := cr.client.Expire(ctx, key, cr.timeToLive); res.Err() != nil {
            return res.Err()
        }
    }

    return cr.UnmarkMissing(ctx, pipe, item.GetRandId())
}
```

`SETNX` + `EXPIRE` memberi semantik insert-if-absent **tanpa** perlu tahu lebih dulu
apakah key-nya ada. Dua perintah, dua-duanya bisa di-enqueue.

Tambahkan juga ke `BaseWithPipeline` ([`base.go:19`](../../base.go#L19)), di sebelah
`Set` dan `Del`:

```go
func (bw *BaseWithPipeline[T]) SetIfAbsent(ctx context.Context, item T) error {
    return bw.baseClient.setIfAbsent(ctx, bw.pipe, item)
}
```

### 4.2 `Sorted.addItem` — `Get` dibuang

[`sorted.go:80`](../../sorted.go#L80):

```go
func (srtd *Sorted[T]) addItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
    var selfPipe bool
    if pipe == nil {
        pipe = srtd.client.Pipeline()
        selfPipe = true
    }

    if err := srtd.baseClient.WithPipeline(pipe).SetIfAbsent(ctx, item); err != nil {
        return err
    }

    if err := srtd.IngestItem(ctx, pipe, item, false, keyParams...); err != nil {
        return err
    }

    if selfPipe {
        _, errPipe := pipe.Exec(ctx)
        return errPipe
    }
    return nil
}
```

### 4.3 `Timeline.addItem` — sama persis

[`timeline.go:119`](../../timeline.go#L119). Perubahan identik. Perhatikan
perbandingannya di sana memakai `errGet != redis.Nil` (bukan `errors.Is`), tapi itu ikut
hilang bersama `Get`-nya.

---

## 5. Efeknya

| | Sebelum | Sesudah |
|---|---|---|
| TTL item existing saat `AddItem` | tidak berubah | diperbarui penuh |
| Payload item existing | tidak berubah | tidak berubah (tetap insert-if-absent) |
| Round-trip `AddItem` (dengan pipe caller) | 1 `Get` sinkron + pipeline | pipeline saja |
| Baris di `addItem` | ~30 (×2 struct) | ~20 (×2 struct) |
| Melanggar Pipeline Discipline | tidak | tidak |

`Get` sinkron sebelum pipeline hilang, jadi perbaikan ini **menurunkan** biaya
`AddItem`, bukan menaikkannya.

---

## 6. Yang sengaja tidak ikut diselesaikan

**Divergensi score vs payload.** `IngestItem` tetap meng-`ZADD` dengan score dari item
yang dioper caller, sementara payload di `Base` tetap versi lama. Jadi kalau
`sortingReference` diset ke `UpdatedAt` dan kamu memanggil `AddItem` dengan item yang
`UpdatedAt`-nya lebih baru, urutannya ikut baru tapi isinya tidak.

Itu konsekuensi wajar dari semantik insert-if-absent, asal terdokumentasi:

> `AddItem` menempatkan item ke dalam index. Untuk mengubah isinya, pakai `Base.Set`.

Kalimat itu perlu masuk ke `CLAUDE.md` dan `CLAUDE.consumer.md`.

**`RemoveItem` yang menghapus key `Base`.** Lubang index yang satu lagi, dan sama sekali
tidak berhubungan dengan TTL — [`sorted.go:151`](../../sorted.go#L151) dan
[`timeline.go:232`](../../timeline.go#L232). Ditangani terpisah.

---

## 7. Test yang menyertai

Pakai `miniredis`. Yang wajib:

1. `AddItem` atas item yang sudah ada → TTL key item kembali penuh, **payload tidak
   berubah** (oper stub, pastikan value lengkap bertahan).
2. `AddItem` atas item yang belum ada → key terbentuk dengan payload yang dioper dan TTL
   penuh.
3. Skenario 5 hari di §2, dijalankan dengan waktu miniredis dimajukan: sorted set tidak
   boleh bolong sebelum expire-nya sendiri.
4. `AddItem` lewat `WithPipeline` caller → tidak ada `Exec` di dalam library.

---

## 8. Checklist

- [x] `Base.SetIfAbsent` + `Base.setIfAbsent` ([`base.go`](../../base.go))
- [x] `BaseWithPipeline.SetIfAbsent` ([`base.go:19`](../../base.go#L19))
- [x] `Sorted.addItem` ([`sorted.go:80`](../../sorted.go#L80))
- [x] `Timeline.addItem` ([`timeline.go:119`](../../timeline.go#L119))
- [x] Empat test di §7
- [x] Kalimat semantik `AddItem` di `CLAUDE.md` dan `CLAUDE.consumer.md`
