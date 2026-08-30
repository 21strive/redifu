# Relation — Panduan Implementasi dan Rencana Penyederhanaan

> **Status: sudah diimplementasi** di branch `redifu-simplified`. `redifu.NewRelation`
> dan `redifu.TypeOf` sudah dihapus — lihat
> [Bagian 8 — Apa yang berubah](#8-apa-yang-berubah-dari-api-sekarang) untuk migrasinya.
>
> Dokumen ini ditulis lebih dulu sebagai spesifikasi, dari sudut pandang pemakai library,
> supaya bisa dinilai enak dipakai atau tidak sebelum ada kodenya. Sekarang ia berfungsi
> sebagai panduan pemakaian.
>
> Dasar temuan yang melatarbelakangi perubahan: [`relation-evaluation.md`](../relation-evaluation.md).

---

## 1. Masalah yang diselesaikan Relation

Sebuah `Account` muncul di dalam 500 `Post`. Kalau setiap `Post` menyimpan salinan penuh
`Account`-nya, maka:

- ganti nama account → 500 key harus ditulis ulang;
- satu key yang terlewat akan menyimpan nama lama sampai TTL-nya habis;
- untuk menemukan 500 key itu, kamu butuh index tambahan yang juga harus dijaga.

Relation menghapus seluruh kelas masalah ini. `Post` hanya menyimpan `AuthorRandId`.
`Account` disimpan **sekali** di `account:a7`. Update satu key itu, dan semua 500 post
langsung mencerminkannya pada fetch berikutnya — tanpa list dibangun ulang, tanpa
invalidasi, tanpa query ke database.

Itu keseluruhan janji fitur ini. Semua aturan di bawah ada untuk menjaga janji itu.

---

## 2. Bentuk entity

```go
package post

type Post struct {
    redifu.Record                            // embed → memenuhi SQLItemBlueprint
    Title   string `json:"title"`
    Content string `json:"content"`

    AuthorRandId string           `json:"authorRandId"`  // pointer — INI yang disimpan Redis
    Author       *account.Account `json:"-"`             // diisi saat fetch, tidak pernah diserialisasi
}
```

Dua field per relasi, dan dua aturan yang tidak boleh dilanggar:

| Aturan | Kenapa |
|---|---|
| Simpan field randId-nya | Itu satu-satunya penunjuk yang ada. Tidak ada index terbalik di mana pun. |
| Tag field entity dengan `json:"-"` | Supaya item hasil fetch aman ditulis balik ke `Base` — yang tersimpan tetap hanya randId. |

Nama field bebas. Tidak ada yang diterka dari nama — kamu menyebut kedua field itu
secara eksplisit saat mendeklarasikan relasi (bagian 3). Jadi `Author` + `AuthorRandId`,
`WrittenBy` + `WriterId`, atau apa pun, sama saja.

**Entity dipakai sebagai pointer**: `Base[*Post]`, bukan `Base[Post]`. Relation menulis
ke dalam struct-mu; itu hanya mungkin lewat pointer. `Relate` menolak tipe value dengan
error saat startup.

---

## 3. Mendeklarasikan relasi

```go
authorRelation, err := redifu.Relate(
    account.AccountBase,                                        // Base tempat entity diambil
    func(p *Post) string              { return p.AuthorRandId }, // di mana randId-nya
    func(p *Post, a *account.Account) { p.Author = a },          // ke mana hasilnya ditulis
)
if err != nil {
    log.Fatalf("redifu relation: %v", err)
}

PostTimeline.AddRelation(authorRelation)
```

Tiga argumen, tidak ada argumen keempat:

| Argumen | Arti |
|---|---|
| `account.AccountBase` | `*Base[*Account]` — sumber entity |
| `func(p *Post) string` | getter randId |
| `func(p *Post, a *Account)` | setter entity |

Keduanya fungsi Go biasa, jadi **compiler yang memeriksanya**. Rename `AuthorRandId`
dan build gagal tepat di baris deklarasi relasi ini. Tidak ada jalan bagi relasi untuk
diam-diam resolve ke nol di runtime.

`Relate` mengembalikan error hanya untuk satu hal: `P` bukan pointer (`Base[Post]`
alih-alih `Base[*Post]`). Itu kesalahan wiring, bukan kondisi runtime — tangani di
startup dan biarkan crash.

### Beberapa relasi sekaligus

```go
PostTimeline.AddRelation(authorRelation, categoryRelation, brandRelation)
```

Dua relasi ke tipe yang sama juga tidak masalah — inilah yang tidak bisa dilakukan API
lama:

```go
authorRelation, _ := redifu.Relate(account.AccountBase,
    func(p *Post) string              { return p.AuthorRandId },
    func(p *Post, a *account.Account) { p.Author = a })

editorRelation, _ := redifu.Relate(account.AccountBase,
    func(p *Post) string              { return p.EditorRandId },
    func(p *Post, a *account.Account) { p.Editor = a })

PostTimeline.AddRelation(authorRelation, editorRelation)
```

### Relasi dipasang per koleksi

`AddRelation` ada di `Timeline`, `Sorted`, `Page`, dan `TimeSeries` — bukan di `Base`.
Kalau `Post` dipakai di beberapa koleksi, daftarkan relasi yang sama ke masing-masing:

```go
PostTimeline.AddRelation(authorRelation)
PostArchivePage.AddRelation(authorRelation)
```

Objek relasi-nya stateless, aman dipakai bersama.

---

## 4. Wiring lengkap satu entity

```go
package post

const (
    postKeyFormat     = "post:%s"
    postFeedKeyFormat = "feed:user:%s:posts"
    postTTL           = 7 * 24 * time.Hour   // Base HARUS lebih panjang dari sorted set
    postFeedTTL       = 2 * 24 * time.Hour
    postItemPerPage   = 20
)

var (
    PostBase     *redifu.Base[*Post]
    PostTimeline *redifu.Timeline[*Post]
)

func InitRedis(rdb redis.UniversalClient) error {
    PostBase = redifu.NewBase[*Post](rdb, postKeyFormat, postTTL)

    PostTimeline = redifu.NewTimeline[*Post](
        rdb, PostBase, postFeedKeyFormat,
        postItemPerPage, redifu.Descending, postFeedTTL,
    )

    authorRelation, err := redifu.Relate(
        account.AccountBase,
        func(p *Post) string              { return p.AuthorRandId },
        func(p *Post, a *account.Account) { p.Author = a },
    )
    if err != nil {
        return err
    }
    PostTimeline.AddRelation(authorRelation)

    return nil
}
```

Urutan init penting: `account.InitRedis` harus jalan lebih dulu, karena
`account.AccountBase` sudah harus terisi saat `Relate` dipanggil.

**TTL relasi**: `Account` yang expire lebih cepat dari `Post` yang merujuknya
menghasilkan post dengan `Author == nil`. Jadi aturan "Base lebih panjang dari sorted
set" berlaku juga lintas-entity — `Base` entity terkait harus paling panjang umurnya.

---

## 5. Seeding — relasi tidak ikut campur

Ini bagian yang paling sering disalahpahami, jadi eksplisit saja:

> **Relation tidak melakukan apa pun saat seeding.** Seeding hanya menulis `AuthorRandId`
> ke dalam post, dan (kalau kamu mau) menulis `Account` ke `Base`-nya sendiri.
> Tidak ada API relasi yang dipanggil di jalur seeding.

### 5a. Seeding tanpa JOIN

Post di-seed dengan randId author-nya saja. Account-nya akan di-resolve saat fetch —
kalau `account:a7` sudah ada di Redis, gratis; kalau belum, post itu keluar dengan
`Author == nil` sampai ada yang menghangatkan key-nya.

> Bentuk ini hanya aman kalau ada **jalur lain** yang dijamin menghangatkan
> `account:a7` — misalnya seeder lain untuk entity yang sama, atau halaman profil yang
> selalu diakses lebih dulu. Kalau tidak ada, pakai 5b. Alasannya di
> [§9 — kenapa relation miss jarang terjadi](#9-yang-sengaja-tidak-dilakukan).

```go
rows, err := db.QueryContext(ctx, `
    SELECT p.randid, p.title, p.content, p.author_randid, p.created_at
    FROM posts p
    WHERE p.user_id = $1
    ORDER BY p.created_at DESC
    LIMIT $2`, userRandId, limit)
if err != nil {
    return err
}
defer rows.Close()

pipe := rdb.Pipeline()

for rows.Next() {
    p := &Post{}
    if err := rows.Scan(&p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt); err != nil {
        return err
    }
    PostBase.WithPipeline(pipe).Set(ctx, p)
    PostTimeline.IngestItem(ctx, pipe, p, true, userRandId)
}
if err := rows.Err(); err != nil {
    return err
}

_, err = pipe.Exec(ctx)
return err
```

Perhatikan: `p.Author` **tidak pernah** disentuh. Scanner mengisi randId, relasi mengisi
entity — dan keduanya tidak pernah bertukar peran.

### 5b. Seeding dengan JOIN — menghangatkan relasi sekalian

Kalau query-mu memang sudah menyentuh tabel account, tulis sekalian ke `Base` account
dalam pipeline yang sama. Fetch berikutnya lalu resolve relasi sepenuhnya dari Redis.

```go
rows, err := db.QueryContext(ctx, `
    SELECT p.randid, p.title, p.content, p.author_randid, p.created_at,
           a.randid, a.name, a.avatar_url
    FROM posts p
    JOIN accounts a ON a.randid = p.author_randid
    WHERE p.user_id = $1
    ORDER BY p.created_at DESC
    LIMIT $2`, userRandId, limit)
if err != nil {
    return err
}
defer rows.Close()

pipe := rdb.Pipeline()
warmed := make(map[string]bool)   // dedupe author yang berulang antar baris

for rows.Next() {
    p := &Post{}
    a := &account.Account{}
    if err := rows.Scan(
        &p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt,
        &a.RandId, &a.Name, &a.AvatarURL,
    ); err != nil {
        return err
    }

    PostBase.WithPipeline(pipe).Set(ctx, p)
    PostTimeline.IngestItem(ctx, pipe, p, true, userRandId)

    if !warmed[a.RandId] {
        account.AccountBase.WithPipeline(pipe).Set(ctx, a)   // Base-nya SENDIRI
        warmed[a.RandId] = true
    }
}
if err := rows.Err(); err != nil {
    return err
}

_, err = pipe.Exec(ctx)
return err
```

Tiga hal yang membuat ini benar:

1. `a` ditulis ke `account.AccountBase`, bukan ditempelkan ke `p.Author`;
2. post tetap hanya membawa `AuthorRandId` — singleton-nya utuh;
3. semuanya satu pipeline, satu `Exec`, satu round-trip.

Map `warmed` murni penghematan payload; tanpa itu pun benar, karena `SET` ke key yang
sama dengan value yang sama bersifat idempoten. Yang **tidak** opsional adalah
penghangatannya sendiri — itu yang menjaga TTL `account:a7` selalu lebih muda dari
sorted set yang merujuknya (§9).

### 5c. Seeder Timeline utuh (dengan cursor dan marker)

```go
func seedPostFeed(ctx context.Context, userRandId string, subtraction int64, lastRandId string) error {
    query := `
        SELECT p.randid, p.title, p.content, p.author_randid, p.created_at,
               a.randid, a.name, a.avatar_url
        FROM posts p
        JOIN accounts a ON a.randid = p.author_randid
        WHERE p.user_id = $1`
    args := []interface{}{userRandId}

    if lastRandId != "" {
        var cursorTime time.Time
        row := db.QueryRowContext(ctx, `SELECT created_at FROM posts WHERE randid = $1`, lastRandId)
        if err := row.Scan(&cursorTime); err != nil {
            return err
        }
        query += ` AND p.created_at < $2`
        args = append(args, cursorTime)
    }

    query += fmt.Sprintf(` ORDER BY p.created_at DESC LIMIT %d`,
        PostTimeline.GetItemPerPage()-subtraction)

    rows, err := db.QueryContext(ctx, query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()

    pipe := rdb.Pipeline()
    warmed := make(map[string]bool)
    var count int64

    for rows.Next() {
        p := &Post{}
        a := &account.Account{}
        if err := rows.Scan(
            &p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt,
            &a.RandId, &a.Name, &a.AvatarURL,
        ); err != nil {
            return err
        }

        PostBase.WithPipeline(pipe).Set(ctx, p)
        PostTimeline.IngestItem(ctx, pipe, p, true, userRandId)

        if !warmed[a.RandId] {
            account.AccountBase.WithPipeline(pipe).Set(ctx, a)
            warmed[a.RandId] = true
        }
        count++
    }
    if err := rows.Err(); err != nil {
        return err
    }

    isFirstPage := lastRandId == ""
    switch {
    case isFirstPage && count == 0:
        PostTimeline.MarkEmpty(ctx, pipe, userRandId)
    case isFirstPage && count < PostTimeline.GetItemPerPage():
        PostTimeline.MarkFirstPage(ctx, pipe, userRandId)
    case !isFirstPage && subtraction+count < PostTimeline.GetItemPerPage():
        PostTimeline.MarkLastPage(ctx, pipe, userRandId)
    }

    if isFirstPage {
        PostTimeline.SetExpiration(ctx, pipe, userRandId)
    }

    _, err = pipe.Exec(ctx)
    return err
}
```

Satu-satunya jejak relasi di seluruh fungsi ini adalah tiga baris `warmed` — dan itu pun
opsional.

---

## 6. Fetch — di sinilah relasi bekerja

```go
func GetPostFeed(ctx context.Context, userRandId string, lastRandIds []string) ([]*Post, string, string, error) {
    needsSeed, err := PostTimeline.RequiresSeeding(ctx, int64(len(lastRandIds)), userRandId)
    if err != nil {
        return nil, "", "", err
    }
    if needsSeed {
        if err := seedPostFeed(ctx, userRandId, 0, ""); err != nil {
            return nil, "", "", err
        }
    }

    output := PostTimeline.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)

    if errors.Is(output.Error(), redifu.ResetPagination) {
        if err := seedPostFeed(ctx, userRandId, 0, ""); err != nil {
            return nil, "", "", err
        }
        output = PostTimeline.Fetch(nil).WithParams(userRandId).Exec(ctx)
    }
    if output.Error() != nil {
        return nil, "", "", output.Error()
    }

    for _, p := range output.Items() {
        if p.Author != nil {
            log.Println(p.Title, "oleh", p.Author.Name)   // sudah terisi
        }
    }

    return output.Items(), output.ValidLastId(), output.Position(), nil
}
```

Tidak ada API relasi yang dipanggil di sini juga. Relasi sudah terpasang di
`InitRedis`; `Fetch` menyelesaikannya sendiri.

### Yang terjadi di dalam `Exec`

Fetch 20 post yang ditulis oleh 3 author berbeda, dengan 2 relasi terdaftar:

```
1.  ZREVRANGE feed:user:u1:posts 0 19            → 20 randId
2.  PIPELINE { GET post:p1 … GET post:p20 }      → 20 post
3.  kumpulkan randId per relasi, dedupe:
      author   → [a7, a9, a3]
      category → [c1, c2]
    PIPELINE { GET account:a7, account:a9, account:a3,
               GET category:c1, category:c2 }    → satu pipeline
4.  sebar hasilnya ke tiap item lewat setter
```

Pipeline berisi `GET` dipakai, bukan `MGET`, supaya tetap benar di Redis Cluster — key-key
itu bisa jatuh di slot yang berbeda, dan `MGET` lintas-slot ditolak. Jumlah round-trip-nya
sama.

**Tiga round-trip**, bukan 60. `account:a7` diambil sekali, dipakai untuk berapa pun
post yang merujuknya. Menambah relasi ketiga tidak menambah round-trip — semua relasi
berbagi pipeline yang sama di langkah 3.

Ini juga alasan kenapa dedupe di sini bukan optimasi tambahan, melainkan konsekuensi
langsung dari singleton: satu entity = satu key, jadi identitasnya sudah unik.

### randId tidak pernah dikosongkan

Post hasil fetch membawa **dua-duanya**: `Author` terisi, `AuthorRandId` tetap ada.
Itu disengaja, dan itulah yang membuat pola read-modify-write aman:

```go
p := output.Items()[0]
p.Title = "judul baru"
PostBase.Set(ctx, p)     // aman
```

Karena `Author` bertag `json:"-"`, yang tertulis ke `post:p1` tetap hanya
`AuthorRandId`. Hapus tag itu, dan baris terakhir akan membekukan salinan penuh author
ke dalam key post — singleton untuk post tersebut rusak permanen, tanpa jalan pulih.

### Kalau entity terkaitnya tidak ada

`account:a7` expired atau kena eviction → post itu keluar dengan `Author == nil`,
sementara sisa fetch tetap sukses. Tidak ada error.

Dalam pemakaian normal ini **jarang terjadi**, dan bukan karena beruntung — lihat
[§9](#9-yang-sengaja-tidak-dilakukan) untuk alasannya. Tetap jaga sebelum
di-dereference, karena "jarang" bukan "tidak pernah":

```go
resp := PostResponse{Title: p.Title}
if p.Author != nil {
    resp.AuthorName = p.Author.Name
}
```

Kalau kolom itu wajib ada di response, hangatkan `Base` account-nya dari DB lalu fetch
ulang — jangan purge list-nya. List-nya tidak salah; yang hilang cuma satu key entity.

### `WithProcessor` jalan setelah relasi

```go
output := PostTimeline.
    Fetch(lastRandIds).
    WithParams(userRandId).
    WithProcessor(func(p **Post, args []interface{}) {
        (*p).IsLikedByViewer = checkLike((*p).RandId, args[0].(string))
    }, viewerRandId).
    Exec(ctx)
```

Karena relasi sudah selesai lebih dulu, `p.Author` boleh dibaca di dalam processor —
dengan tetap menjaga `nil`.

---

## 7. Update, pindah relasi, dan hapus

### Update entity terkait

Tulis `Base`-nya. Itu saja.

```go
author.Name = "Nama Baru"
err := account.AccountBase.Set(ctx, author)
```

Semua timeline, page, sorted set, dan time series yang merujuk author ini langsung
mencerminkannya. Jangan purge, jangan re-seed — tidak ada yang basi di list mana pun.

### Post pindah ke author lain

```go
post.AuthorRandId = newAuthorRandId
err := PostBase.Set(ctx, post)
```

Satu key, satu tulis. randId di dalam item adalah satu-satunya penunjuk yang ada, jadi
tidak ada list yang perlu dicari atau diperbaiki.

Pengecualiannya adalah list yang **di-key oleh** relasi itu — `feed:author:a7`. Di sana
perpindahan author adalah perubahan keanggotaan, dan itu kamu urus eksplisit:

```go
err = AuthorTimeline.RemoveItem(ctx, post, oldAuthorRandId)
err = AuthorTimeline.AddItem(ctx, post, newAuthorRandId)
```

### Hapus

`RemoveItem` mengeluarkan item dari index koleksi yang bersangkutan. Jangan pakai
`Base.Del` sendirian — item akan lenyap dari semua index lain tanpa jejak.

---

## 8. Apa yang berubah dari API sekarang

| | Sekarang (`NewRelation`) | Rencana (`Relate`) |
|---|---|---|
| Deklarasi | `NewRelation[Account](base, TypeOf[Post]())` | `Relate(base, getRandId, setItem)` |
| Pemetaan field | konvensi nama: `Author` + `AuthorRandId` | closure eksplisit |
| Rename field | lolos build, resolve ke nol saat runtime | build gagal |
| Dua relasi ke tipe sama | tidak bisa — field pertama yang cocok yang menang | didukung |
| `identifier` di `AddRelation` | wajib diisi, tapi tidak pernah dipakai | dihapus |
| Tipe value `Base[Post]` | diterima, lalu diam-diam tidak resolve | ditolak dengan error |
| randId setelah fetch | dikosongkan | dipertahankan |
| Write-back hasil fetch | merusak singleton secara permanen | aman |
| Round-trip, 20 item 2 relasi | 60, serial | 3 |
| Author yang sama di 20 post | 20 `GET` ke key yang sama | 1 |

### Migrasi

Kode yang perlu diubah hanya di tempat deklarasi:

```go
// sebelum
rel, err := redifu.NewRelation[account.Account](account.AccountBase, redifu.TypeOf[Post]())
PostTimeline.AddRelation("author", rel)

// sesudah
rel, err := redifu.Relate(account.AccountBase,
    func(p *Post) string              { return p.AuthorRandId },
    func(p *Post, a *account.Account) { p.Author = a })
PostTimeline.AddRelation(rel)
```

Plus dua penyesuaian di entity:

1. `Base[Post]` → `Base[*Post]` (dan `Author Account` → `Author *account.Account`);
2. tambahkan `json:"-"` pada field entity relasi.

Nomor 2 wajib. Tanpa itu, randId yang sekarang dipertahankan justru akan didampingi
salinan penuh entity di dalam key — kebalikan dari yang kita mau.

### Bentuk internal yang dibutuhkan

Untuk pembaca yang akan mengimplementasi:

```go
// relasi kini bertipe terhadap entity induknya
type Relation[P any] interface {
    Resolve(ctx context.Context, items []P) error
}

func Relate[P any, R item.Blueprint](
    base *Base[R],
    getRandId func(P) string,
    setItem   func(P, R),
) (Relation[P], error)
```

Relasi menerima **seluruh halaman sekaligus** — di situlah dedupe terjadi. Konsekuensinya
`SortedSet.Fetch` tidak lagi resolve relasi per item di dalam loop; ia mengumpulkan item
dulu, baru menyelesaikan tiap relasi sekali untuk seluruh halaman.

Bentuk yang diimplementasi memakai satu method tak-terekspor, supaya relasi hanya bisa
dibuat lewat `Relate` dan supaya semua relasi bisa berbagi satu pipeline:

```go
type Relation[P any] interface {
    stage(ctx context.Context, pipe redis.Pipeliner, items []P) (func() error, error)
}
```

`stage` meng-enqueue pembacaannya ke pipeline milik pemanggil dan mengembalikan fungsi
yang menuliskan hasilnya ke item setelah pipeline dieksekusi — sesuai Pipeline Discipline,
ia tidak pernah memanggil `Exec` sendiri.

Dua primitif baru di `Base`:

```go
func (cr *Base[T]) GetMany(ctx context.Context, randIds []string) (map[string]T, error)
func (cr *Base[T]) stageGetMany(ctx context.Context, pipe redis.Pipeliner, randIds []string) func() (map[string]T, error)
```

Key yang miss cukup absen dari map — bukan error.

---

## 9. Yang sengaja tidak dilakukan

### Sliding TTL dan callback saat relation miss — tidak diperlukan

Evaluasi awal ([#4](../relation-evaluation.md) dan [#5](../relation-evaluation.md))
mengusulkan `GETEX` untuk memperpanjang TTL entity terkait saat dibaca, plus hook
`OnMissing` untuk re-seed satu entity dari DB. Keduanya **dicoret**. Satu aturan sudah
menyelesaikan keduanya:

> **Setiap `ZADD` harus dibarengi tulis ke `Base` di pipeline yang sama** — untuk item
> itu sendiri, dan untuk entity yang direlasikannya.

Kalau aturan itu dipegang, umur key `Base` selalu ≥ umur entri yang merujuknya di sorted
set. Sebuah miss — baik item maupun relasi — butuh key `Base` yang mati lebih dulu
daripada index yang menunjuknya, dan itu jadi mustahil. Tidak ada yang perlu diperpanjang
saat dibaca, dan tidak ada yang perlu di-recover saat fetch.

`GETEX` tidak menambah jaminan apa pun di atas aturan itu, tapi menambah biaya: setiap
`Base.Get` berubah dari operasi baca murni menjadi tulis, untuk **semua** entity — bukan
cuma yang jadi target relasi.

#### Kapan aturan itu bocor

Yang menyesatkan: "TTL `Base` 7 hari > TTL sorted set 2 hari" terdengar seperti sudah
cukup. Tidak. TTL item dihitung sejak **terakhir kali ia ditulis**, bukan sejak ia
di-`ZADD`. Syarat amannya:

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

Bukan kasus karangan: itu bentuk "user bookmark post lama", "post lama masuk trending",
"moderator feature post lama". Item yang di-`AddItem` justru sering bukan item baru.

Perbaikannya ada di [`02-set-if-absent.md`](02-set-if-absent.md), dan itu **prasyarat**
bagi seluruh argumen di bagian ini.

#### Dua jalur yang harus mematuhinya

1. **Seeder** — pola JOIN di
   [§5b](#5b-seeding-dengan-join--menghangatkan-relasi-sekalian). Seeder selalu memanggil
   `Base.Set` eksplisit, jadi TTL item maupun entity relasi selalu segar. Kalau seeder-mu
   berbentuk [§5a](#5a-seeding-tanpa-join) (tanpa JOIN), rantai itu putus untuk entity
   relasinya.
2. **`AddItem` saat runtime** — jalur ini tidak lewat seeder sama sekali. Untuk item-nya
   sendiri, `setIfAbsent` menutupnya di dalam library. Untuk entity relasinya, **tidak
   ada** yang melakukannya — library tidak pernah menulis `account:a7`. Itu jatuh
   sepenuhnya ke consumer:

   ```go
   pipe := rdb.Pipeline()
   account.AccountBase.WithPipeline(pipe).Set(ctx, author)   // relasi dihangatkan
   PostTimeline.WithPipeline(pipe).AddItem(ctx, post, userRandId)
   _, err := pipe.Exec(ctx)
   ```

   Kalau payload author-nya tidak ada di tangan, `SetIfAbsent` yang sama bisa dipakai —
   yang penting TTL-nya ikut segar.

Sisa risikonya tinggal eviction (`maxmemory-policy allkeys-lru`), dan di situ posisi
entity relasi justru paling kuat: ia dibaca setiap kali salah satu perujuknya di-fetch,
jadi LRU membuangnya paling belakang. Guard `nil` di §6 sudah cukup untuk menutupnya.

### Relasi bersarang — belum, dan seringnya tidak perlu

Hanya satu tingkat yang di-resolve. Kalau `Account` sendiri punya `Organization`, maka
`p.Author.Organization` tetap `nil`.

Sebelum menganggap ini kekurangan, perhatikan bahwa sebagian besar kasus dua-tingkat
bisa **diratakan penunjuknya**, bukan datanya:

```go
type Post struct {
    redifu.Record
    AuthorRandId string           `json:"authorRandId"`
    Author       *account.Account `json:"-"`

    OrgRandId string                `json:"orgRandId"`   // penunjuk tingkat dua, diratakan
    Org       *org.Organization     `json:"-"`
}
```

Dua relasi datar, dua-duanya satu tingkat, dua-duanya tetap singleton. Tidak ada data
yang diduplikasi — yang diduplikasi cuma penunjuknya, 26 byte. Resolve-nya tetap 3
round-trip karena semua relasi berbagi pipeline yang sama.

Harga yang dibayar: `OrgRandId` di dalam post adalah **salinan sebuah fakta yang
sebenarnya milik `Account`**. Kalau author pindah organisasi, semua post lama masih
menunjuk org lama, dan tidak ada yang memperbaikinya. Jadi perataan ini tepat kalau:

- penunjuk tingkat dua praktis tidak berubah (author → organisasi asal, order → mata
  uang), **atau**
- post memang ingin menyimpan keadaan saat itu (invoice → alamat penagihan saat
  transaksi).

Kalau penunjuk tingkat dua bisa berubah dan post harus mengikuti perubahannya, perataan
salah dan yang dibutuhkan memang nesting sungguhan.

**Desain baru ini tidak menutup pintunya.** `Relate` menerima `*Base[R]` dan `Resolve`
bekerja atas slice, jadi nesting tinggal membiarkan `Base` menyimpan relasinya sendiri:

```go
account.AccountBase.AddRelation(orgRelation)   // belum ada
```

Penyelesaian relasi author, setelah pembacaan account-nya selesai, memanggil relasi milik
account atas slice hasil itu. Biayanya satu round-trip tambahan **per tingkat**, bukan
per item — tetap batched. Yang harus ikut dipikirkan saat mengimplementasinya: siklus
(`Account` → `Post` → `Account`) butuh batas kedalaman atau daftar key yang sudah
dikunjungi.

Saya sarankan menunggu sampai ada kasus nyata yang tidak bisa diratakan. Kalau kasus itu
muncul, tambahannya lokal — tidak ada yang perlu dirombak dari §1–§8.

### Resolve relasi di `Base.Get`

Mengambil satu post lewat `Base` tetap memberi bentuk yang belum ter-resolve. Ini
konsisten dan aman, tapi berbeda bentuk dari hasil `Fetch` — sesuatu yang perlu diingat
saat item dari kedua jalur bertemu di kode yang sama.

---

## 10. Ringkasan aturan

- Simpan randId, jangan simpan objeknya.
- Tag field entity `json:"-"`.
- Pakai tipe pointer: `Base[*Post]`.
- Scanner mengisi randId; relasi mengisi entity. Tidak pernah tertukar.
- Seeding entity terkait ditulis ke `Base`-nya sendiri, di pipeline yang sama — wajib,
  bukan optimasi. Ini yang membuat TTL entity selalu lebih muda dari index yang
  merujuknya, sehingga relation miss praktis tidak terjadi (§9).
- Aturan yang sama berlaku untuk `AddItem` di runtime, bukan cuma untuk seeder.
- Update entity terkait = tulis satu `Base`. Jangan purge list apa pun.
- Selalu jaga `nil` sebelum dereference hasil relasi.
- TTL `Base` entity terkait harus mengungguli semua yang merujuknya.
