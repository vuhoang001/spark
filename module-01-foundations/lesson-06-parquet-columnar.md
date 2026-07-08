# Lesson 6 — Parquet & columnar format: tại sao DE sống chết với Parquet

> Module 1 · Foundations · Tuần 3 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Row-oriented vs column-oriented** — hai cách xếp dữ liệu lên đĩa và vì sao analytics thuộc về phe cột.
- **Anatomy một file Parquet**: row group → column chunk → page, footer metadata, min/max statistics.
- **Encoding**: dictionary encoding, RLE (Run-Length Encoding) — vì sao Parquet nhỏ hơn CSV 5–10× *trước cả khi* nén.
- **Compression**: snappy vs gzip vs zstd — bảng trade-off và cách chọn.
- **Nested types** trong Parquet: struct/array/map vẫn lưu columnar được — nhờ đâu.
- Ghép mảnh cuối: **Parquet + predicate pushdown = skip row group** — cơ chế đằng sau phép màu ở lesson 5.
- Đo thật: kích thước & tốc độ Parquet vs CSV trên Olist.

Sau bài này bạn phải làm được:

- Vẽ từ trí nhớ cấu trúc file Parquet và giải thích footer chứa gì.
- Trả lời tách bạch: Parquet nhỏ nhờ đâu (encoding + nén), nhanh nhờ đâu (columnar + statistics) — hai chuyện khác nhau.
- Chọn compression codec có căn cứ cho từng tầng pipeline.

Kiến thức dùng trong thực tế: Parquet là **định dạng mặc định của cả lakehouse hiện đại** — Iceberg, Delta Lake, Hudi bên dưới đều là file Parquet. Module 5 bạn học Iceberg, tức là học "hệ điều hành" quản lý các file Parquet này. Không hiểu Parquet thì Iceberg chỉ là câu thần chú.

---

## 2. Why

### Vấn đề: workload analytics đọc theo CỘT, đĩa lưu theo DÒNG

Nhìn hai kiểu truy cập vào cùng bảng `orders(order_id, customer, city, amount, status, ...20 cột)`:

- **OLTP** (app web): `SELECT * FROM orders WHERE order_id = 'o123'` — lấy **1 dòng, đủ cột**.
- **OLAP** (DE/analyst): `SELECT city, SUM(amount) GROUP BY city` — lấy **2 cột, TẤT CẢ dòng**.

Định dạng row-oriented (CSV, dữ liệu trong Postgres) tối ưu cho kiểu 1. Còn nghề của bạn 95% là kiểu 2: quét vài cột trên hàng trăm triệu dòng. Dùng CSV cho analytics tức là mỗi query trả tiền đọc + parse 18 cột không ai cần.

### Analogy: danh bạ lớp học

- **Row-oriented** = xấp **phiếu học sinh**: mỗi tờ ghi đủ tên, tuổi, địa chỉ, điểm của một em. Muốn tính điểm trung bình cả lớp? Lật đủ 50 tờ, mỗi tờ dò xuống mục điểm.
- **Column-oriented** = **sổ điểm**: một trang chỉ toàn điểm, một trang toàn tên. Tính trung bình? Mở đúng 1 trang, cộng một mạch từ trên xuống.

Bonus của sổ điểm: cả trang cùng kiểu dữ liệu (toàn số!) nên nén và mã hóa cực hiệu quả — cột `status` có 100 nghìn dòng nhưng chỉ 8 giá trị khác nhau thì việc gì phải ghi chữ "delivered" 96 nghìn lần?

### Nếu không có columnar format thì sao?

Thời tiền-Parquet (Hadoop đời đầu), người ta chạy analytics trên CSV/SequenceFile: quét thừa I/O 10×, không statistics, không schema trong file, cột kiểu gì phải đoán. Parquet (Twitter + Cloudera, 2013, lấy ý tưởng từ paper **Dremel** của Google — chính là nền của BigQuery) giải quyết trọn gói và trở thành *lingua franca*: Spark, Trino, Flink, DuckDB, Pandas, BigQuery... đều đọc được.

### Trade-off (Senior phải thuộc)

| Được | Mất |
|---|---|
| Đọc analytics nhanh 10–100× (pruning + statistics) | **Ghi chậm hơn CSV**: phải buffer, encode, nén, tính stats |
| Nhỏ hơn 5–10× → tiền storage + I/O + network | Không đọc được bằng mắt/`cat` — cần tool (`parquet-tools`, pyarrow) |
| Schema + kiểu dữ liệu nằm trong file, tự mô tả | **Immutable**: không append vào file đang có, không sửa 1 dòng — muốn "update" phải viết lại file (đây chính là lý do Iceberg/Delta tồn tại) |
| Splittable theo row group → song song hóa đẹp | Tệ cho kiểu truy cập "lấy 1 dòng theo key" (point lookup) — đó là đất của database |

> Quy tắc bỏ túi: **CSV/JSON ở rìa hệ thống (trao đổi với bên ngoài), Parquet ở mọi tầng bên trong**. Thấy pipeline nội bộ chuyền tay nhau CSV là thấy tiền đang cháy.

---

## 3. Theory

### 3.1. Row vs Column — layout trên đĩa

Cùng 4 dòng dữ liệu `(order_id, city, amount)`:

```
ROW-ORIENTED (CSV, Avro, Postgres heap):
┌────────────────────────────────────────────────────────────┐
│ o1,HN,120 │ o2,SG,80 │ o3,HN,300 │ o4,DN,150 │  →  đĩa     │
└────────────────────────────────────────────────────────────┘
  dòng nào cũng trọn vẹn, nằm cạnh nhau
  SELECT sum(amount) → vẫn phải lướt qua o1,HN, o2,SG,...

COLUMN-ORIENTED (Parquet, ORC):
┌────────────────────────────────────────────────────────────┐
│ o1 │ o2 │ o3 │ o4 ║ HN │ SG │ HN │ DN ║ 120 │ 80 │ 300 │ 150 │
└────────────────────────────────────────────────────────────┘
  └── cột order_id ──┘└──── cột city ───┘└──── cột amount ────┘
  SELECT sum(amount) → nhảy thẳng tới khúc amount, đọc một mạch
  và khúc city toàn giá trị lặp → mã hóa/nén cực gọn
```

Hệ quả 1: **đọc đúng cột cần** (column pruning ở mức vật lý).
Hệ quả 2: **dữ liệu cùng kiểu nằm cạnh nhau** → encoding + compression ăn gấp bội.
Hệ quả 3 (cái giá): ghi 1 dòng mới nghĩa là chen vào N chỗ khác nhau → columnar format chọn luôn **immutable**: ghi một lần, không sửa.

### 3.2. Parquet file anatomy — mổ banh một file

Parquet không chọn "thuần cột" mà chọn **hybrid**: cắt bảng theo dòng thành từng khối lớn (row group), *bên trong* mỗi khối mới xếp theo cột. Nhờ vậy một file vừa song song hóa theo khối, vừa hưởng lợi columnar:

```
my_file.parquet
┌──────────────────────────────────────────────────────┐
│ "PAR1"  (4 byte magic number mở đầu)                  │
├──────────────────────────────────────────────────────┤
│ ROW GROUP 0        (mặc định ~128 MB dữ liệu)         │
│ ┌──────────────────────────────────────────────────┐ │
│ │ COLUMN CHUNK: order_id  → [page][page][page]...  │ │
│ │ COLUMN CHUNK: city      → [page][page]...        │ │
│ │ COLUMN CHUNK: amount    → [page][page]...        │ │
│ └──────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────┤
│ ROW GROUP 1                                          │
│   (cùng cấu trúc — dòng 1.000.001 → 2.000.000 ...)   │
├──────────────────────────────────────────────────────┤
│ ...                                                  │
├──────────────────────────────────────────────────────┤
│ FOOTER (metadata — trái tim của file)                │
│  • schema đầy đủ + kiểu dữ liệu từng cột             │
│  • danh sách row group: offset, kích thước           │
│  • per column-chunk STATISTICS:                      │
│      min / max / null_count / distinct (tùy)         │
│  • encoding + codec của từng chunk                   │
├──────────────────────────────────────────────────────┤
│ độ dài footer (4 byte) │ "PAR1" (magic kết thúc)     │
└──────────────────────────────────────────────────────┘
```

Ba tầng, nhớ theo vai:

| Tầng | Là gì | Kích thước điển hình | Vai trò |
|---|---|---|---|
| **Row group** | Một khúc N dòng của bảng, xếp columnar bên trong | 128 MB (chỉnh được) | Đơn vị **song song hóa** (1 task đọc ≥1 row group) và đơn vị **skip** của pushdown |
| **Column chunk** | Toàn bộ dữ liệu MỘT cột trong MỘT row group | tùy cột | Đơn vị của **column pruning**: cần cột nào seek đúng chunk đó |
| **Page** | Khúc nhỏ trong chunk | ~1 MB | Đơn vị **encoding/nén/đọc** nhỏ nhất; page header cũng có stats riêng |

**Footer nằm ở CUỐI file** — có lý do: writer phải ghi hết dữ liệu mới biết offset + statistics của từng phần. Reader thì đọc ngược: nhảy tới 8 byte cuối lấy độ dài footer → đọc footer (vài chục KB) → biết toàn bộ bản đồ file **mà chưa đọc byte dữ liệu nào**. Đây là lý do `spark.read.parquet(...)` lấy schema tức thì, còn CSV + inferSchema phải cày cả file.

### 3.3. Min/max statistics — GPS của dữ liệu

Footer lưu cho **mỗi column chunk**: `min`, `max`, `null_count`. Ví dụ file orders sort theo ngày:

```
                  stats của cột order_date trong footer
Row group 0:  min=2018-01-01  max=2018-01-31
Row group 1:  min=2018-02-01  max=2018-02-28
Row group 2:  min=2018-03-01  max=2018-03-31

Query: WHERE order_date = '2018-03-15'
  → RG0: max(01-31) < 03-15  → KHÔNG THỂ chứa → skip, không đọc byte nào
  → RG1: max(02-28) < 03-15  → skip
  → RG2: khoảng [03-01, 03-31] chứa 03-15 → đọc thật
Kết quả: đọc 1/3 file. Dữ liệu càng lớn + sort càng tốt, tỉ lệ skip càng đậm.
```

Chú ý chữ "KHÔNG THỂ": statistics chỉ giúp loại trừ chắc chắn, row group được đọc vẫn phải filter lại từng dòng. Và hiệu quả **phụ thuộc layout**: nếu dữ liệu trộn ngẫu nhiên, min/max của mọi row group đều là [01-01, 12-31] → không skip được gì. Vì thế senior hay **sort theo cột filter phổ biến trước khi ghi** — cùng dữ liệu, cùng query, khác nhau 10× chỉ nhờ thứ tự ghi.

### 3.4. Encoding — nhỏ trước khi nén

Encoding = biểu diễn lại dữ liệu thông minh hơn, **tự động, per-column**, khác với compression (nén byte mù quáng, xét ở 3.5). Hai ngôi sao:

**Dictionary encoding** — cột lặp nhiều giá trị:

```
Cột order_status, 100.000 dòng, 8 giá trị khác nhau:

thô:        "delivered","delivered","shipped","delivered",...  (~1 MB text)

dictionary: dict = {0:"delivered", 1:"shipped", 2:"canceled", ...}
data        = [0,0,1,0,0,2,0,...]   ← mỗi phần tử chỉ cần 3 BIT
                                       (bit-packing: 8 giá trị = 2³)
```

**RLE (Run-Length Encoding)** — giá trị lặp liên tiếp:

```
data:  0,0,0,0,0,0,1,1,0,0,0,0
RLE:   (0 × 6)(1 × 2)(0 × 4)      ← ghi "giá trị × số lần" thay vì từng phần tử
```

Combo hủy diệt: **sort theo cột đó → dictionary index lặp liên tiếp dài → RLE nghiền nát**. Một cột status 100 nghìn dòng sort xong có thể còn vài chục byte. Đây là lý do Parquet đã nhỏ hơn CSV nhiều lần *trước khi* compression ra tay — và là câu trả lời chuẩn cho câu hỏi phỏng vấn "vì sao Parquet nhỏ hơn CSV" (đa số chỉ trả lời được "vì nó nén").

### 3.5. Compression — snappy vs gzip vs zstd

Sau encoding, từng page được nén bằng codec bạn chọn (`option("compression", ...)`):

| Codec | Tỉ lệ nén | Tốc độ nén/giải nén | CPU | Chọn khi |
|---|---|---|---|---|
| `snappy` (mặc định) | Trung bình (~2×) | Rất nhanh | Thấp | Dữ liệu **nóng**, đọc/ghi liên tục (bronze/silver/gold đang hoạt động) |
| `gzip` | Tốt (~2.5–3×) | Chậm | Cao | Đồ cũ; ngày nay hầu như luôn thua zstd — biết để đọc hệ thống legacy |
| `zstd` | Tốt (~gzip, thường hơn) | Nhanh (gần snappy khi giải nén) | Vừa | **Ngôi sao đang lên**: dữ liệu lạnh/archive, và ngày càng nhiều nơi dùng làm mặc định mới |
| không nén | 1× | — | 0 | Gần như không bao giờ (trừ benchmark) |

Cách nghĩ đúng: đây là bài toán **CPU đổi lấy I/O**. Storage rẻ nhưng scan là tiền + thời gian; codec nén sâu tiết kiệm I/O nhưng mỗi lần đọc trả thêm CPU giải nén. Snappy = "nén cho có, ưu tiên tốc độ"; zstd = "được cả hai, trả thêm chút CPU khi ghi". Lưu ý dễ quên: **nén Parquet là per-page nên KHÔNG ảnh hưởng splittability** — khác hẳn chuyện `.csv.gz` chết vì gzip nguyên file ở lesson 5.

### 3.6. Nested types — struct/array/map vẫn columnar

Parquet lưu được `struct`, `array`, `map` mà **không phá kiểu xếp cột**: mỗi *leaf field* thành một cột vật lý riêng.

```
schema: order_id string,
        customer struct<name string, city string>,
        items array<struct<sku string, qty int>>

cột vật lý trên đĩa:
  order_id            │ o1  o2  ...
  customer.name       │ An  Binh ...
  customer.city       │ HN  SG  ...
  items.list.sku      │ A1 A2 │ B7 ...    ← phẳng hóa mọi phần tử array
  items.list.qty      │ 2  1  │ 5  ...
```

Câu hỏi hóc: array mỗi dòng dài khác nhau, có dòng NULL — làm sao ráp lại đúng dòng? Parquet lưu kèm mỗi giá trị hai con số nhỏ (thuật toán từ paper Dremel): **repetition level** (giá trị này mở dòng mới hay là phần tử tiếp theo của array cũ) và **definition level** (nested đến tầng nào thì thành NULL). Bạn không cần thuộc chi tiết — cần nhớ **hệ quả ăn tiền**: `select("customer.city")` chỉ đọc đúng MỘT cột lá đó khỏi đĩa, kể cả khi struct có 50 field. Column pruning xuyên thấu vào nested. (Lesson 11 sẽ vắt kiệt nested types ở tầng API.)

### 3.7. Ghép mảnh: vì sao "Parquet + pushdown = skip row group"

Giờ ghép lesson 5 với hôm nay thành một dây chuyền hoàn chỉnh:

```
filter(col("order_date") == "2018-03-15")
        │  Catalyst: PushDownPredicate
        ▼
FileScan parquet  PushedFilters: [EqualTo(order_date, 2018-03-15)]
        │  task mở file, đọc FOOTER trước
        ▼
so predicate với min/max STATISTICS từng row group
        │
        ├── không thể chứa → SKIP cả row group (không đọc, không giải nén)
        └── có thể chứa  → đọc đúng column chunk cần (pruning)
                            → giải nén page → decode → filter từng dòng
```

CSV không có footer, không statistics, không ranh giới cột → cùng câu filter phải đọc + parse 100% file. **Đây là toàn bộ bí mật** đằng sau chênh lệch hàng chục lần bạn sẽ đo ở lab.

---

## 4. Internal

Đường đi của một lệnh `df.write.parquet(...)` bên trong mỗi task:

```
① Task nhận partition dữ liệu (các Row)
        │
② Buffer dòng vào memory, tách giá trị theo CỘT
   (đây là lý do ghi Parquet tốn memory hơn ghi CSV —
    phải giữ cả row group trong RAM trước khi flush)
        │
③ Đủ ngưỡng row group (~128 MB) → với TỪNG cột:
   chọn encoding (thử dictionary trước; dictionary phình
   quá ngưỡng thì fallback plain) → cắt thành page
   → nén từng page (snappy/zstd/...) → tính min/max/null_count
        │
④ Flush row group xuống file, ghi tiếp row group sau
        │
⑤ Đóng file: ghi FOOTER (schema, offsets, statistics
   của mọi row group) + độ dài footer + magic "PAR1"
```

Và chiều đọc trong Spark còn một tăng áp nữa: **vectorized reader** (`spark.sql.parquet.enableVectorizedReader`, mặc định bật) — giải mã cả **batch cột** vào memory dạng columnar rồi xử lý theo lô, thay vì dựng từng Row một. Nhanh hơn nhiều lần so với reader từng dòng; đây cũng là nền để module sau nói chuyện whole-stage codegen (lesson 13).

Ba con số nên biết mặt (chưa cần chỉnh vội — tuning ở module 3):

| Config | Mặc định | Ý nghĩa |
|---|---|---|
| `parquet.block.size` (row group) | 128 MB | To → statistics thô hơn nhưng ít metadata; nhỏ → skip mịn hơn, metadata phình |
| `parquet.page.size` | 1 MB | Đơn vị nén/đọc nhỏ nhất |
| `spark.sql.files.maxPartitionBytes` | 128 MB | Cỡ split khi đọc — khớp đẹp với row group size |

Để ý sự "trùng hợp" 128 MB: một task đọc trọn ~một row group — thiết kế ăn khớp có chủ đích của cả hệ sinh thái Hadoop-era (block HDFS cũng 128 MB).

---

## 5. API

### `df.write.parquet(path)` / `option("compression", ...)`

```python
(df.write.mode("overwrite")
   .option("compression", "zstd")     # mặc định là snappy
   .parquet("/workspace/data/output/lesson06/orders_zstd"))
```

- **Ý nghĩa**: ghi Parquet với codec chỉ định (per-write; hoặc set toàn cục `spark.sql.parquet.compression.codec`).
- **Pitfall**: đổi codec **không** đụng file cũ — thư mục có thể trộn snappy lẫn zstd (đọc vẫn được, codec ghi trong metadata từng chunk, nhưng khó lý luận về kích thước). Muốn đồng nhất phải rewrite.

### `sortWithinPartitions(col)` — mồi cho statistics

```python
(df.repartition("order_date")
   .sortWithinPartitions("customer_state")   # sort TRONG từng partition, không shuffle thêm
   .write.partitionBy("order_date").parquet(path))
```

- **Ý nghĩa**: sắp dữ liệu trước khi ghi để min/max các row group tách bạch → pushdown skip được nhiều.
- **Pitfall**: `orderBy` toàn cục cũng đạt mục đích nhưng gây shuffle full-sort đắt đỏ; trong pattern ghi file, `sortWithinPartitions` thường là đủ và rẻ hơn hẳn.

### `spark.read.parquet(p1, p2, ...)` + schema evolution

```python
df = spark.read.option("mergeSchema", True).parquet(path)
```

- **Ý nghĩa**: file cũ 5 cột, file mới 6 cột nằm chung thư mục — `mergeSchema` hợp nhất schema (cột thiếu thành NULL).
- **Pitfall 1**: mặc định TẮT — Spark lấy schema từ một file bất kỳ; file mới có cột mới mà bạn không bật mergeSchema thì cột đó **biến mất im lặng**.
- **Pitfall 2**: cùng tên cột nhưng **khác kiểu** giữa các file (int vs long do đổi writer) → lỗi đọc khó chịu. Schema evolution tử tế là việc của table format (Iceberg — module 5); Parquet trần chỉ chịu được kiểu "thêm cột".

### Đọc metadata bằng PyArrow (đồ nghề mổ xẻ)

```python
import pyarrow.parquet as pq
f = pq.ParquetFile("part-00000-....parquet")
print(f.metadata)                    # num_row_groups, num_rows, created_by
rg = f.metadata.row_group(0)
col = rg.column(2)                   # 1 column chunk
print(col.path_in_schema, col.compression,
      col.statistics.min, col.statistics.max,
      col.total_compressed_size, col.total_uncompressed_size)
```

- **Ý nghĩa**: nhìn tận mắt row group, statistics, encoding, tỉ lệ nén — thay cho niềm tin.
- **Pitfall**: chạy trên máy host cần `pip install pyarrow`; file output của Spark là *thư mục* — trỏ vào file `part-*.parquet` bên trong, không trỏ thư mục.

### `coalesce(n)` trước khi ghi

- **Ý nghĩa**: bảng kết quả nhỏ mà 200 task → 200 file Parquet vài chục KB = mất sạch lợi thế (mỗi file 1 footer, row group tí hon, statistics vô dụng). `coalesce(1–8)` gom lại file cỡ tử tế (nhắm 128 MB–1 GB/file).
- **Pitfall**: `coalesce(1)` trên dữ liệu lớn = 1 task ghi tất cả — nghẽn cổ chai ngược. Cân giữa số file và độ song song; chi tiết repartition vs coalesce ở lesson 16.

---

## 6. Demo nhỏ

```
Input:  1 triệu dòng tự sinh (status lặp nhiều — mồi cho dictionary+RLE)
   ↓    ghi CSV, ghi Parquet snappy, ghi Parquet zstd
Output: so kích thước 3 bản + mổ footer bằng PyArrow
```

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("demo06").master("local[2]").getOrCreate()
base = "/tmp/demo06"

df = (spark.range(1_000_000)
      .withColumn("status", F.when(F.col("id") % 100 < 97, "delivered")
                              .otherwise("canceled"))            # 97% lặp 1 giá trị
      .withColumn("city", F.element_at(
          F.array(F.lit("HN"), F.lit("SG"), F.lit("DN")),
          (F.col("id") % 3 + 1).cast("int")))
      .withColumn("amount", F.round(F.rand() * 500, 2)))

df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{base}/csv")
df.coalesce(1).write.mode("overwrite").parquet(f"{base}/pq_snappy")
df.coalesce(1).write.mode("overwrite").option("compression", "zstd").parquet(f"{base}/pq_zstd")
spark.stop()
```

```bash
du -sh /tmp/demo06/*        # kết quả điển hình:
# 31M   csv                 ← baseline
# 8.2M  pq_snappy           ← ~4× nhỏ hơn
# 5.9M  pq_zstd             ← zstd nén sâu hơn snappy
```

Mổ footer (host, cần `pip install pyarrow`):

```python
import glob, pyarrow.parquet as pq
f = pq.ParquetFile(glob.glob("/tmp/demo06/pq_snappy/part-*.parquet")[0])
c = f.metadata.row_group(0).column(1)          # cột status
print(c.total_uncompressed_size, c.total_compressed_size)
# cột status: vài trăm KB thay vì ~9 MB text — dictionary+RLE đã nghiền
# TRƯỚC khi snappy ra tay. Nhỏ nhờ encoding trước, nén sau — thấy chưa?
```

---

## 7. Production Example

Trong lakehouse `kafka-flink` của bạn, Parquet là **tầng vật lý của mọi thứ**:

```
Iceberg table "silver.orders"
   │  (Iceberg = metadata: snapshot, manifest — module 5)
   └── data files = TOÀN parquet, zstd, ~128–512 MB/file
              ▲                         ▲
        Spark ghi                 Trino/Spark đọc
        (sort theo cột            (footer + statistics
         filter phổ biến           → skip row group
         trước khi ghi)            → trả kết quả giây)
```

Quyết định thực tế các team lakehouse phải chốt — giờ bạn đủ nền để hiểu vì sao:

1. **File size target 128 MB–1 GB**: nhỏ quá thì footer/statistics loãng + listing chậm (small files — lesson 21); to quá thì kém song song. Iceberg có job compaction định kỳ chỉ để giữ chuẩn này (lesson 32).
2. **Codec theo nhiệt độ dữ liệu**: tầng hot serving → snappy (đọc nhanh nhất); bronze/archive → zstd (tiền storage). Nhiều công ty 2024+ chuyển hẳn zstd toàn bộ.
3. **Sort order là một phần của table design**: Iceberg cho khai `sort_order` chính vì min/max statistics chỉ sắc khi dữ liệu được sắp — điều bạn học ở 3.3 chứ không phải phép màu.
4. **Trino query thẳng file Spark ghi** không cần export/import gì — vì cả hai nói chung một ngôn ngữ: footer Parquet. "Compute tách storage" ở lesson 1 đứng được là nhờ format trung lập này.

---

## 8. Hands-on Lab

**Mục tiêu**: đo chênh lệch CSV vs Parquet trên Olist bằng số thật, và mổ một file Parquet ra xem nội tạng.

> Tạo file mới trong `labs/lab06/` — **không đụng** `lab06.py` sẵn có của bạn.

### Bước 1 — `labs/lab06/lesson06_benchmark.py`

```python
import time
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lesson06-benchmark").getOrCreate()
OUT = "/workspace/data/output/lesson06"

# ── Chuẩn bị: một bảng to (orders join items cho nặng ký) ──
orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
items  = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                        header=True, inferSchema=True)
big = orders.join(items, "order_id")          # ~112k dòng, 14 cột

big.coalesce(2).write.mode("overwrite").option("header", True).csv(f"{OUT}/big_csv")
(big.coalesce(2).write.mode("overwrite").parquet(f"{OUT}/big_parquet"))
(big.coalesce(2).write.mode("overwrite")
    .option("compression", "zstd").parquet(f"{OUT}/big_parquet_zstd"))

# ── Đo: cùng 1 query trên CSV vs Parquet ──
def bench(name, df):
    t0 = time.time()
    r = (df.filter(F.col("order_status") == "delivered")
           .agg(F.sum("price")).collect()[0][0])
    print(f"{name:<22} sum={r:>14,.2f}   {time.time()-t0:6.2f}s")

csv_df = spark.read.csv(f"{OUT}/big_csv", header=True, inferSchema=True)
pq_df  = spark.read.parquet(f"{OUT}/big_parquet")
for label, d in [("CSV", csv_df), ("Parquet snappy", pq_df)]:
    bench(f"{label} lần 1", d); bench(f"{label} lần 2", d)

pq_df.filter(F.col("order_status") == "delivered") \
     .select("price").explain(mode="formatted")   # soi PushedFilters/ReadSchema
spark.stop()
```

Chạy: `make run-local F=labs/lab06/lesson06_benchmark.py` (local để số đo ổn định).

### Bước 2 — đo kích thước trên host

```bash
du -sh data/output/lesson06/big_csv \
       data/output/lesson06/big_parquet \
       data/output/lesson06/big_parquet_zstd
```

Ghi lại 3 con số + tỉ lệ. (Olist bé nên tỉ lệ ~3–6×; dataset thật hàng trăm GB, khoảng cách còn tàn nhẫn hơn vì cột text lặp nhiều.)

### Bước 3 — mổ footer: `labs/lab06/lesson06_inspect.py` (chạy bằng python trên host, cần `pip install pyarrow` trong venv)

```python
import glob
import pyarrow.parquet as pq

path = glob.glob("data/output/lesson06/big_parquet/part-*.parquet")[0]
f = pq.ParquetFile(path)
m = f.metadata
print(f"rows={m.num_rows:,}  row_groups={m.num_row_groups}  cols={m.num_columns}")

rg = m.row_group(0)
for i in range(m.num_columns):
    c = rg.column(i)
    s = c.statistics
    print(f"{c.path_in_schema:<32} {c.compression:<8} "
          f"comp={c.total_compressed_size:>10,}  "
          f"min={str(s.min)[:19]:<20} max={str(s.max)[:19]}")
```

### Bước 4 — nested struct (yêu cầu của ROADMAP)

Viết `labs/lab06/lesson06_nested.py`: dựng cột struct rồi chứng minh pruning xuyên nested:

```python
nested = big.select(
    "order_id",
    F.struct("customer_id", "order_status").alias("customer_info"),
    F.struct("price", "freight_value").alias("charges"))
nested.write.mode("overwrite").parquet(f"{OUT}/nested")

back = spark.read.parquet(f"{OUT}/nested").select("charges.price")
back.explain(mode="formatted")   # ReadSchema: struct<charges:struct<price:double>>
                                 # → chỉ đọc 1 cột lá trong 5!
```

### Bước 5 — quan sát, ghi `labs/lab06/NOTES-lesson06.md`

1. Kích thước CSV vs snappy vs zstd (con số + tỉ lệ).
2. Thời gian query CSV vs Parquet — và lần 1 vs lần 2 khác gì (nhớ lesson 2: không cache thì vì sao lần 2 vẫn nhanh hơn chút? gợi ý: OS page cache — đừng nhầm với Spark cache).
3. Từ output PyArrow: cột nào nén tốt nhất/tệ nhất? Đối chiếu với lý thuyết dictionary/RLE (cột id ngẫu nhiên vs cột status lặp).
4. `ReadSchema` của query nested — bằng chứng pruning xuyên struct.

---

## 9. Assignment

**Easy** — Bằng chữ của bạn:
1. Parquet nhỏ hơn CSV 5–10× nhờ những cơ chế nào? (câu trả lời đầy đủ có ÍT NHẤT 2 tầng, "vì nó nén" chỉ được nửa điểm)
2. Vẽ lại anatomy file Parquet từ trí nhớ, chú thích vai trò từng tầng.
3. Footer nằm cuối file thay vì đầu file — tại sao?

**Medium** — Chọn codec cho 3 tình huống, biện luận bằng trade-off CPU/IO: (a) bảng gold được dashboard query 500 lần/ngày; (b) bronze archive ghi 1 lần, đọc vài lần/năm, 50 TB; (c) staging trung gian ghi xong đọc ngay rồi xóa trong cùng pipeline.

**Hard** — Lấy bảng `big` từ lab, ghi 2 phiên bản: (a) nguyên trạng, (b) `sortWithinPartitions("order_status")` (hoặc sort theo `price`). Dùng PyArrow in min/max của cột đó qua các row group ở cả hai bản (muốn nhiều row group: ghi nhiều file bằng `repartition(8)` rồi xem từng file). Chạy cùng filter trên hai bản, so `number of files read`/thời gian trong Spark UI. Kết luận 5 dòng: sort-before-write đáng giá khi nào, trả giá gì?

**Production Challenge** — (nếu `../kafka-flink` đang chạy — chính là Assignment Hard của ROADMAP) Tạo bảng Iceberg có cột struct/array từ Spark, `SELECT` chỉ 1–2 field lá qua Trino, dùng `EXPLAIN` (Trino) hoặc Spark UI chứng minh column pruning hoạt động xuyên engine. Viết 5–10 dòng: điều gì khiến hai engine khác nhau cùng prune được một file?

> Nộp bài bằng cách paste code + số đo + câu trả lời vào chat.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| `count()` trên Parquet | Gần như tức thì | Số dòng nằm sẵn trong footer/row group metadata — không quét dữ liệu. CSV: quét hết |
| Filter cột đã sort trước khi ghi | Rất nhanh | Min/max tách bạch → skip đa số row group |
| Filter cột trộn ngẫu nhiên | Không skip được | Mọi row group đều "có thể chứa" — statistics bất lực (bài học layout!) |
| Ghi Parquet vs ghi CSV | Chậm hơn | Buffer row group + encode + nén + stats. Trả trước một lần, thu về mỗi lần đọc |
| 10.000 file Parquet 200 KB | Thảm | 10.000 footer, listing, row group tí hon — small files ăn mòn mọi lợi thế columnar |
| Đọc `select("*")` trên Parquet | Mất lợi thế pruning | Không cột nào bị cắt. Thói quen `select` đúng cột cần bắt đầu từ hôm nay |

Câu tự vấn của bài này: *"layout dữ liệu tôi GHI hôm nay có giúp query ĐỌC ngày mai skip được gì không?"* — người ghi file quyết định số phận người đọc file. Ở nhiều team, hai người đó là một: chính bạn, cách nhau một tuần.

---

## 11. Spark UI

Vẫn tab **SQL / DataFrame**, nay đọc sâu hơn node **Scan parquet**:

- So cặp metric `size of files read` vs kích thước thư mục trên đĩa: đọc 15 MB từ thư mục 200 MB = pruning + skip đang ăn.
- `rows output` của node Scan: nhỏ hơn hẳn tổng dòng ⇒ filter đã chặn từ tầng scan chứ không phải node Filter phía trên làm hết.
- Đối chiếu 2 query giống hệt trên CSV vs Parquet: bản CSV **không có** `PushedFilters` — nhìn một lần nhớ mãi.
- Tab **Stages**: thời gian task đọc Parquet ngắn và đều; nếu bạn tạo bản dữ liệu sort tốt, chênh lệch với bản không sort hiện rõ ở tổng duration của stage scan.

---

## 12. Common Mistakes

1. **"Parquet nhanh" thành niềm tin tôn giáo** — ghi bừa (không sort, file vụn, select \*) rồi thắc mắc sao không nhanh. Parquet chỉ trả lãi khi layout hợp tác: file cỡ chuẩn, sort theo cột filter, đọc đúng cột.
2. **Coi output Parquet là MỘT file** — nó là thư mục `part-*`; script downstream trỏ vào "file" rồi vỡ, hoặc `coalesce(1)` mọi thứ "cho gọn" và giết song song.
3. **Nghìn file bé do partitionBy + nhiều task** (tái phạm từ lesson 5, giờ hiểu sâu hơn: mỗi file bé = footer + row group còi = statistics vô dụng).
4. **Trộn kiểu dữ liệu giữa các file cùng thư mục** (đổi schema writer giữa chừng: cột int thành string) → đọc nổ khó lường. Schema là hợp đồng — đổi phải có nghi thức (Iceberg schema evolution, module 5).
5. **Chọn gzip vì "nén tốt nhất" theo bài viết 2015** — 2026 rồi: zstd nén ngang/ngon hơn mà giải nén nhanh gần snappy. Trade-off phải cập nhật theo thời đại.
6. **Dùng Parquet cho point lookup** ("lấy đơn hàng theo order_id phục vụ API") — nhầm sân: quét statistics cứu phần nào nhưng đó là việc của database/key-value store. Columnar cho scan, row cho lookup.
7. **Không bao giờ mở footer ra xem** — PyArrow 5 dòng là thấy hết row group, stats, tỉ lệ nén. Người debug bằng chứng cứ khác người debug bằng phỏng đoán ở đúng chỗ này.

---

## 13. Interview

**Junior:**

1. *Row-oriented khác column-oriented thế nào, mỗi loại hợp workload gì?* — Row: các giá trị của MỘT dòng nằm cạnh nhau → hợp OLTP (lấy 1 dòng đủ cột). Column: các giá trị của MỘT cột nằm cạnh nhau → hợp OLAP (vài cột, mọi dòng), đọc đúng cột cần và nén tốt vì dữ liệu cùng kiểu liền kề.
2. *Tại sao Parquet nhỏ hơn CSV 5–10 lần?* — Hai tầng: (1) **encoding** — dictionary (giá trị lặp thành index vài bit) + RLE (chuỗi lặp liên tiếp thành "giá trị × số lần") + bit-packing, hiệu quả vì columnar gom dữ liệu cùng kiểu; (2) **compression** (snappy/zstd) nén từng page sau encoding. CSV là text thô: "delivered" viết đủ 9 byte mỗi dòng.
3. *File Parquet gồm những phần nào?* — Nhiều row group (~128 MB, khúc dòng xếp columnar bên trong); mỗi row group gồm column chunk per cột; chunk chia thành page (~1 MB — đơn vị encode/nén); cuối file là footer: schema, offset các row group, statistics min/max/null_count từng chunk.
4. *Vì sao `spark.read.parquet` biết schema ngay còn CSV thì không?* — Parquet tự mô tả: schema + kiểu nằm trong footer, đọc vài KB cuối file là có. CSV không mang schema — phải khai tay hoặc inferSchema quét dữ liệu để đoán.

**Mid:**

5. *Min/max statistics giúp query nhanh hơn bằng cách nào? Khi nào vô dụng?* — Reader so predicate với min/max từng row group trong footer: khoảng [min,max] không thể chứa giá trị cần → skip cả row group không đọc/giải nén. Vô dụng khi dữ liệu không sort/cluster theo cột filter (mọi row group đều phủ toàn dải giá trị), hoặc predicate không đẩy được (UDF, hàm phức tạp).
6. *So sánh snappy / gzip / zstd cho Parquet.* — Snappy: nén vừa, cực nhanh, mặc định — cho dữ liệu nóng. Gzip: nén tốt, chậm, CPU cao — legacy. Zstd: nén ngang/hơn gzip, giải nén gần snappy — lựa chọn hiện đại cho archive và ngày càng cả dữ liệu nóng. Bản chất là CPU đổi I/O; và nén Parquet per-page nên không phá splittability như gzip nguyên file CSV.
7. *Nested struct/array được lưu columnar ra sao?* — Mỗi leaf field là một cột vật lý riêng; array được phẳng hóa, kèm repetition/definition level (Dremel) để ráp lại đúng dòng và biểu diễn NULL/array rỗng. Hệ quả: select 1 field lá chỉ đọc đúng cột đó, pruning xuyên nested.
8. *Ghi Parquet cần chú ý gì để người đọc sau này được nhờ?* — File size 128 MB–1 GB (coalesce/repartition hợp lý, tránh small files); sort theo cột filter phổ biến trước khi ghi (statistics sắc); partitionBy cột low-cardinality hay dùng để lọc; codec theo nhiệt độ dữ liệu; schema ổn định, đổi có kiểm soát.

**Senior:**

9. *Bảng Parquet 2 TB, query chính filter theo `event_date` và `country` nhưng vẫn chậm dù "đã dùng Parquet". Anh/chị điều tra thế nào?* — Theo dây chuyền skip: (1) Spark UI node Scan: `PushedFilters`/`PartitionFilters` có không, `size of files read` bao nhiêu so với 2 TB; (2) bảng có partitionBy `event_date` chưa — chưa thì pruning tầng thư mục bằng 0; (3) trong partition, dữ liệu có sort/cluster theo `country` không — mổ footer vài file xem min/max có tách bạch hay row group nào cũng phủ đủ nước; (4) file layout: ngàn file bé thì listing + footer overhead nuốt hết; (5) predicate có sạch không (cast ngầm, hàm bọc cột chặn pushdown). Fix điển hình: re-layout — partition theo date, sort theo country, compact về file cỡ chuẩn. Ý ăn điểm: Parquet là *tiềm năng*, layout mới là *hiệu suất*.
10. *Parquet immutable — vậy update/delete từng dòng (GDPR xóa user) làm thế nào, và điều đó dẫn đến công nghệ gì?* — Parquet không sửa tại chỗ: phải đọc file chứa dòng đó, viết lại file mới không có nó, và cần lớp metadata để hoán đổi file cũ/mới một cách **atomic** cho mọi reader — đó chính xác là việc của table format: Iceberg/Delta/Hudi quản snapshot + manifest trỏ tới tập file Parquet hợp lệ, cho MERGE/DELETE, time travel, schema evolution. Trả lời chạm tới đây chứng tỏ hiểu vì sao lakehouse tồn tại chứ không học vẹt tên công nghệ. (Và đó là module 5 của chúng ta.)

---

## 14. Summary

### Mindmap

```
                      PARQUET (LESSON 6)
                            │
    ┌──────────────┬────────┴────────┬───────────────────┐
    ▼              ▼                 ▼                   ▼
 VÌ SAO CỘT     ANATOMY           NHỎ NHỜ            NHANH NHỜ
    │              │                 │                   │
 OLAP đọc vài   row group ~128MB  encoding TRƯỚC:     footer đọc trước
 cột × mọi dòng   └ column chunk    dictionary          → schema free
 cùng kiểu liền     └ page ~1MB     RLE + bit-pack     min/max stats
 kề → nén sướng  footer CUỐI file: nén SAU (per-page): → skip row group
 giá phải trả:    schema + offset   snappy: nóng       column chunk
 immutable        + min/max stats   zstd: hiện đại     → prune cột (cả
 (→ Iceberg)      nested: cột lá    gzip: legacy        nested)
                  riêng (Dremel)                       ĐK: layout hợp tác
                                                       (sort + file size!)
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ được anatomy Parquet: row group / column chunk / page / footer, không nhìn tài liệu.
- [ ] Trả lời "nhỏ nhờ đâu" bằng 2 tầng (encoding rồi mới compression) như phản xạ.
- [ ] Giải thích được cơ chế skip row group bằng min/max và ĐIỀU KIỆN để nó ăn (sort/layout).
- [ ] Thuộc bảng snappy/gzip/zstd và chọn được codec theo nhiệt độ dữ liệu.
- [ ] Đã chạy lab: có con số kích thước + thời gian CSV vs Parquet của chính mình.
- [ ] Đã mổ ít nhất một footer bằng PyArrow, thấy min/max tận mắt.
- [ ] Nói được vì sao Parquet immutable dẫn thẳng tới sự tồn tại của Iceberg.

---

## 15. Next Lesson

**Mini Project 1 — Batch Ingestion v0: CSV → Spark → Parquet phân vùng (→ Iceberg → Trino).**

Sáu lesson vừa rồi cho bạn đủ đồ nghề: kiến trúc + lazy evaluation + job/stage/task + partition + Data Sources API + Parquet. Giờ là lúc ráp tất cả thành một pipeline ingestion hoàn chỉnh đầu tiên — thứ bạn có thể tự tin kể trong buổi phỏng vấn: đọc Olist CSV với schema tường minh, làm sạch, ghi Parquet phân vùng theo ngày, đo before/after bằng số thật, và nếu hạ tầng kafka-flink của bạn sẵn sàng thì đẩy luôn lên Iceberg cho Trino query. Không có kiến thức mới — chỉ có kiến thức cũ bị bắt làm việc thật. Đây mới là lúc biết bạn HỌC hay mới chỉ ĐỌC.

> Gõ **"Continue"** khi sẵn sàng.
