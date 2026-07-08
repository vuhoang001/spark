# Lesson 33 — Partitioning & hidden partitioning

> Module 5 · Lakehouse & Iceberg · Tuần 17 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Hive partition truyền thống: folder vật lý, cột partition riêng, và 2 căn bệnh kinh niên (quên filter đúng cột, spec cứng vĩnh viễn).
- **Hidden partitioning** của Iceberg: partition theo TRANSFORM — `days(ts)`, `months(ts)`, `bucket(N, id)`, `truncate(N, col)` — người query filter cột GỐC vẫn được prune.
- **Partition evolution**: đổi spec giữa đời bảng, KHÔNG rewrite dữ liệu cũ.
- Nghệ thuật chọn partition spec: cardinality, kích thước partition mục tiêu, query pattern; khi nào bucketing thắng.
- **Sort order**: write ordered để min/max stats chặt — tầng pruning thứ hai sau partition.
- Pitfall số 1 của ngành: **over-partitioning**.

Sau bài này bạn phải làm được:

- Nhìn một bảng + top query pattern → đề xuất partition spec kèm lập luận bằng con số (cardinality × size).
- Giải thích cho người dùng bảng vì sao họ KHÔNG cần biết bảng partition thế nào (điều không tưởng ở thế giới Hive).
- Thực hiện partition evolution trên bảng có dữ liệu và chứng minh dữ liệu cũ không bị đụng.

Kiến thức dùng trong thực tế: partition spec là quyết định ảnh hưởng hiệu năng lâu dài nhất của một bảng — chọn lúc CREATE TABLE, mọi query về sau trả giá hoặc hưởng lợi. Interview Senior DE gần như chắc chắn có câu "em partition bảng này thế nào, tại sao?".

---

## 2. Why

### Vấn đề: partition kiểu Hive bắt người QUERY phải thuộc lòng cấu trúc VẬT LÝ

Hive-style (cũng là cách `df.write.partitionBy(...)` với Parquet trần hoạt động):

```
warehouse/orders/
   ├── dt=2018-01-01/part-001.parquet     ← partition = folder vật lý
   ├── dt=2018-01-02/part-002.parquet        cột "dt" do PIPELINE tự chế ra
   └── ...                                    (từ order_ts), lưu thành cột riêng
```

Hai căn bệnh:

**Bệnh 1 — pruning phụ thuộc trí nhớ người query.**

```sql
-- Người viết pipeline partition theo dt (string, chế từ order_ts)
SELECT * FROM orders WHERE dt = '2018-01-01';                        -- ✅ prune, nhanh
SELECT * FROM orders WHERE order_ts >= '2018-01-01 00:00:00'
                       AND order_ts <  '2018-01-02 00:00:00';        -- ❌ FULL SCAN!
```

Cùng một ý định, câu dưới quét cả bảng — vì engine Hive-style không hề biết `dt` được sinh từ `order_ts`. Mối liên hệ đó nằm trong… đầu của người viết pipeline. Analyst mới vào không biết, BI tool sinh SQL không biết → full scan âm thầm, bill nổ, không ai báo lỗi. Đây là một trong những nguồn lãng phí lớn nhất ở các data platform đời cũ.

**Bệnh 2 — spec là bê tông cốt thép.** Partition theo ngày, hai năm sau dữ liệu tăng 100×, muốn chuyển sang theo giờ? Ở Hive: tạo bảng mới, rewrite toàn bộ, sửa mọi pipeline/query trỏ bảng — một dự án hàng tuần.

### Iceberg lật bàn: partition là METADATA, không phải cấu trúc thư mục

Iceberg lưu trong metadata: "partition = `day(order_ts)`" — một **transform trên cột nguồn**. Mỗi data file được gắn partition value trong manifest (lesson 30). Query filter theo `order_ts` → engine tự áp transform lên filter → prune. Người query không cần biết, không thể quên — vì thế gọi là **hidden** (ẩn khỏi người dùng, không ẩn khỏi engine).

### Nếu không có hidden partitioning thì sao?

Bạn sẽ: duy trì tài liệu "bảng nào filter cột nào" và cầu nguyện mọi người đọc; thêm cột dẫn xuất (`dt`, `month`...) vào mọi bảng và mọi câu INSERT; review từng query của analyst để bắt full scan. Tức là lấy quy trình con người đi vá lỗ hổng thiết kế của công cụ.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| Query filter cột gốc vẫn prune — không phụ thuộc trí nhớ người dùng | Phải hiểu transform để CHỌN spec đúng (việc của bạn, DE) |
| Partition evolution: đổi spec không rewrite | Bảng nhiều spec chồng nhau → planning phức tạp hơn chút, và dữ liệu cũ vẫn ở layout cũ đến khi rewrite |
| Không còn cột partition "chế" thừa trong schema | Transform là tập hữu hạn (year/month/day/hour/bucket/truncate/identity) |
| Bucket transform: chia đều key cardinality cao | Bucket số N cố định — chọn N sai thì đổi cũng là một lần evolution |

> Bài học Senior: partition KHÔNG phải để "cho có tổ chức". Nó tồn tại vì đúng một lý do: **để query đọc ít dữ liệu hơn**. Mọi quyết định spec đều quy về câu hỏi "top query của bảng này filter theo cái gì, và mỗi partition sẽ to bao nhiêu?".

---

## 3. Theory

### 3.1. Transform — bảng cửu chương của partition Iceberg

| Transform | Ví dụ | Biến giá trị thành | Dùng khi |
|---|---|---|---|
| `identity` | `PARTITIONED BY (country)` | Chính nó | Cột cardinality thấp, filter đẳng thức (country, status) |
| `year/month/day/hour` | `days(order_ts)` | 2018-01-01 (số ngày từ epoch) | Cột thời gian — 90% bảng fact partition kiểu này |
| `bucket(N, col)` | `bucket(16, customer_id)` | hash(col) mod N → 0..15 | Key cardinality cao, filter đẳng thức / join |
| `truncate(W, col)` | `truncate(4, zip_code)` | 4 ký tự/bội số đầu | Prefix có ý nghĩa (mã vùng, mã SP) |

### 3.2. Hidden partitioning hoạt động thế nào — diagram phải nhớ

```
CREATE TABLE orders (...) USING iceberg PARTITIONED BY (days(order_ts));

GHI:  dòng {order_ts = '2018-01-01 13:45:22'}
        │  Iceberg áp transform: day('2018-01-01 13:45:22') = 17532
        ▼
      data file F1 ── manifest ghi: F1 có partition ts_day = 17532
      (KHÔNG có cột "ts_day" nào trong dữ liệu — nó chỉ sống trong metadata)

ĐỌC:  SELECT ... WHERE order_ts >= '2018-01-01' AND order_ts < '2018-01-02'
        │  Iceberg BIẾT ts_day = day(order_ts)
        │  → suy ra điều kiện tương đương: ts_day = 17532
        ▼
      manifest list → manifest: chỉ giữ file có partition 17532
      F1 ✅ được đọc | mọi file ngày khác ❌ bị loại từ planning

      Người query CHƯA TỪNG nghe nói đến "ts_day". Vẫn được prune.
```

So sánh trực diện với Hive:

```
                        HIVE                       ICEBERG
Partition là gì?        folder vật lý              transform trong metadata
Cột partition           cột riêng, tự chế,         không tồn tại trong data —
                        chiếm chỗ trong schema     suy từ cột gốc
Filter cột gốc (ts)     FULL SCAN                  prune bình thường
Filter sai kiểu         dt='2018-1-1' ≠            không có chuyện đó —
('2018-1-1' vs          '2018-01-01' → miss        so sánh trên giá trị thật
'2018-01-01')           lặng lẽ                    của order_ts
Đổi spec                rewrite cả bảng            ALTER TABLE, dữ liệu cũ giữ nguyên
```

### 3.3. Partition evolution — đổi spec không rewrite

```sql
ALTER TABLE lakehouse.olist.orders ADD PARTITION FIELD months(ts);      -- thêm
ALTER TABLE lakehouse.olist.orders DROP PARTITION FIELD days(ts);       -- bỏ
ALTER TABLE lakehouse.olist.orders REPLACE PARTITION FIELD days(ts) WITH hours(ts);
```

Cơ chế: metadata.json giữ **danh sách partition-specs** (lesson 30 bạn đã thấy trường này!), mỗi data file gắn với spec-id lúc nó được ghi. Đổi spec = thêm spec mới làm mặc định — **file cũ nằm nguyên layout cũ**, chỉ file ghi từ giờ theo spec mới:

```
   dữ liệu 2017 (ghi thời spec-0: months)   dữ liệu 2018+ (spec-1: days)
   [tháng 1][tháng 2]...[tháng 12]          [1/1][2/1][3/1]...

   Query WHERE ts BETWEEN ... :
   planning tách 2 nhánh — prune file cũ theo months, file mới theo days.
   Người query: vẫn không biết gì, vẫn nhanh.
```

Muốn dữ liệu cũ cũng theo layout mới? Đó là việc của `rewrite_data_files` (lesson 32) — làm dần, không gấp.

### 3.4. Chọn partition spec — bộ 3 câu hỏi

**① Query pattern filter theo gì?** Partition theo cột không ai filter = vô dụng. Lấy top 5 query của bảng, nhìn WHERE.

**② Mỗi partition sẽ to bao nhiêu?** Mục tiêu: partition cỡ **vài trăm MB đến vài GB** (tức ≥ 1 file cỡ chuẩn 512MB, lý tưởng vài file). Công thức nhẩm:

```
size_per_partition = tổng_size_ngày × (khoảng thời gian 1 partition)
VD: bảng nhận 2 GB/ngày → days() cho 2 GB/partition  ✅ đẹp
    bảng nhận 40 MB/ngày → days() cho 40 MB/partition ❌ quá vụn → dùng months()
    bảng nhận 500 GB/ngày → days() hơi to → cân nhắc hours() hoặc days + bucket
```

**③ Cardinality của cột là bao nhiêu?** `identity` trên cột triệu giá trị (user_id) = triệu partition = thảm họa metadata. Cardinality cao mà vẫn muốn prune theo nó → `bucket(N, col)`.

### 3.5. Bucketing — khi nào tốt hơn partition thời gian?

`bucket(16, customer_id)`: hash chia đều mọi customer vào 16 nhóm. Được gì:

- Query `WHERE customer_id = 'abc'` → tính hash → chỉ đọc 1/16 bảng, bất kể bảng to đến đâu.
- Số partition **cố định** (16) dù cardinality hàng triệu — metadata không phình.
- Storage-partitioned join giữa 2 bảng cùng bucket spec có thể bỏ shuffle (Spark 3.3+, bật `spark.sql.sources.v2.bucketing.enabled`).

Không được gì: query khoảng (`customer_id > ...`) hay filter cột khác — hash phá thứ tự. Nên bucket thường **đi kèm** partition thời gian: `PARTITIONED BY (days(ts), bucket(16, customer_id))` — nhưng chỉ khi cả hai chiều đều thật sự có trong query pattern, không thì bạn vừa nhân số partition lên 16 lần vô ích.

### 3.6. Sort order — tầng pruning thứ hai

Partition loại file theo vùng thô; trong MỘT partition vẫn có thể nhiều file. Ai loại tiếp? Min/max stats (lesson 30). Stats chỉ chặt khi dữ liệu được **xếp thứ tự** trước khi ghi:

```
Không sort: file1 price[0.9..6000], file2 price[1.2..5800]  → filter price>5000: đọc CẢ 2
Có sort:    file1 price[0.9..149],  file2 price[150..6000]  → filter price>5000: đọc 1
```

```sql
ALTER TABLE lakehouse.olist.orders WRITE ORDERED BY (order_status, ts);
```

Từ đó mọi writer (và sort compaction mặc định) xếp dòng theo cột này trong mỗi partition trước khi ghi. Nguyên tắc: partition theo cột filter *thô* (thời gian), sort theo cột filter *tinh* trong partition (status, category, price).

---

## 4. Internal

Đường đi của một filter qua bộ máy pruning — ba tầng, từ rẻ đến đắt:

```
SELECT sum(price) FROM orders
WHERE ts >= '2018-03-01' AND ts < '2018-04-01' AND price > 500

① PLANNING TIME — trên driver, CHƯA đọc data file nào:
   a. Transform-aware rewrite: spec là days(ts)
      → điều kiện partition tương đương: ts_day ∈ [17591, 17621]
   b. Manifest list: mỗi manifest mang range partition nó chứa
      → manifest toàn file 2017? loại cả manifest (không mở)
   c. Manifest entries: mỗi data file mang partition value + min/max cột
      → file ngoài range ngày: loại
      → file trong range nhưng max(price)=320 < 500: loại (nhờ sort order thì tầng này mới gắt)
        │  sống sót: danh sách file cụ thể
        ▼
② TASK TIME — executor mở từng file sống sót:
   Parquet row-group stats: min/max per row group (~128MB)
      → row group có max(price)<500: skip không decode
        │
        ▼
③ Đọc thật: chỉ cột cần (columnar) của row group sống sót
```

Chi tiết đáng giá về **partition evolution bên trong**: mỗi manifest gắn với một spec-id, nên một snapshot sau evolution có manifest "thế hệ cũ" (spec-0) lẫn "thế hệ mới" (spec-1). Planner nhóm manifest theo spec, dịch filter sang từng spec để prune tương ứng. Đây là lý do evolution rẻ (chỉ ghi metadata mới) nhưng cũng là lý do nên `rewrite_data_files` dần dần vùng dữ liệu cũ: một bảng 5 đời spec chồng chéo làm planning rối và pruning kém đều.

Còn `bucket(N, col)`: transform là `hash(col) % N` với hash **chuẩn hóa trong spec Iceberg** (murmur3) — nghĩa là Spark, Trino, Flink tính ra CÙNG bucket cho cùng giá trị. Nhờ đó filter đẳng thức từ bất kỳ engine nào cũng prune được, và storage-partitioned join giữa các engine mới khả thi.

---

## 5. API

### `PARTITIONED BY` với transform — lúc CREATE

```sql
CREATE TABLE lakehouse.olist.orders_p (
  order_id STRING, customer_id STRING, order_status STRING,
  price DOUBLE, ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts), bucket(8, customer_id))
TBLPROPERTIES ('format-version'='2');
```
- DataFrame API tương đương: `df.writeTo(t).partitionedBy(F.days("ts"), F.bucket(8, "customer_id")).create()` (import từ `pyspark.sql.functions` — Spark 3.3+ có sẵn `days/months/years/hours/bucket`).
- **Pitfall**: viết `PARTITIONED BY (ts)` (identity trên timestamp!) — mỗi giá trị timestamp một partition → nổ metadata. Timestamp LUÔN đi kèm transform thời gian.

### `ALTER TABLE ... PARTITION FIELD` — evolution

```sql
ALTER TABLE lakehouse.olist.orders_p ADD PARTITION FIELD truncate(2, order_status);
ALTER TABLE lakehouse.olist.orders_p DROP PARTITION FIELD truncate(2, order_status);
ALTER TABLE lakehouse.olist.orders_p REPLACE PARTITION FIELD days(ts) WITH months(ts);
```
- **Khi dùng**: dữ liệu đổi cỡ (ngày → giờ khi volume tăng), query pattern đổi.
- **Pitfall**: evolution chỉ áp cho dữ liệu ghi TỪ GIỜ. Kỳ vọng query trên dữ liệu cũ nhanh lên ngay là hiểu sai cơ chế.

### `WRITE ORDERED BY` — sort order

```sql
ALTER TABLE lakehouse.olist.orders_p WRITE ORDERED BY (order_status ASC, price DESC);
ALTER TABLE lakehouse.olist.orders_p WRITE LOCALLY ORDERED BY (price);  -- sort trong từng task, rẻ hơn (không shuffle toàn cục)
```
- **Pitfall**: `WRITE ORDERED BY` yêu cầu sort toàn cục khi ghi → thêm shuffle cho writer. Bảng ghi streaming dày nên dùng `LOCALLY ORDERED` hoặc dồn việc sort cho compaction ban đêm.

### Soi partition bằng metadata tables

```sql
SELECT partition, count(*) AS files, sum(file_size_in_bytes)/1e6 AS mb
FROM lakehouse.olist.orders_p.files GROUP BY partition ORDER BY partition;
-- bảng .partitions cho view gộp sẵn: record_count, file_count per partition
SELECT * FROM lakehouse.olist.orders_p.partitions;
```
- Đây là "cân sức khỏe partition": partition lệch (skew), partition vụn — nhìn phát ra ngay.

---

## 6. Demo nhỏ

```
Input:  bảng orders partition days(ts) — KHÔNG có cột ngày nào tự chế
   ↓    query filter thẳng cột ts gốc
Output: .files chứng minh chỉ file của đúng ngày được tạo/quét
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("demo33").master("local[2]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/tmp/demo_warehouse")
    .getOrCreate())

spark.sql("DROP TABLE IF EXISTS lakehouse.demo.events")
spark.sql("""CREATE TABLE lakehouse.demo.events (id INT, ts TIMESTAMP)
             USING iceberg PARTITIONED BY (days(ts))""")

spark.sql("""INSERT INTO lakehouse.demo.events VALUES
  (1, TIMESTAMP '2018-01-01 08:00:00'), (2, TIMESTAMP '2018-01-01 21:30:00'),
  (3, TIMESTAMP '2018-01-02 07:15:00'), (4, TIMESTAMP '2018-01-03 12:00:00')""")

# Partition sống trong metadata, không trong schema:
spark.table("lakehouse.demo.events").printSchema()      # chỉ id, ts — không có "ts_day"!
spark.sql("SELECT partition, record_count FROM lakehouse.demo.events.files").show()
# {ts_day=2018-01-01} 2 | {ts_day=2018-01-02} 1 | {ts_day=2018-01-03} 1

# Filter theo CỘT GỐC — vẫn prune:
q = spark.sql("SELECT * FROM lakehouse.demo.events WHERE ts < TIMESTAMP '2018-01-02 00:00:00'")
q.explain()   # tìm dòng BatchScan ... filters=ts < ... : 1 data file được chọn
q.show()
spark.stop()
```

Trong output `explain()`, node `BatchScan` của Iceberg cho thấy filter được đẩy xuống; đối chiếu tab SQL trên UI: scan chỉ đụng 1 file trên 3. Người viết query không hề gõ chữ `ts_day` nào — đó là toàn bộ tinh thần "hidden".

---

## 7. Production Example

Thiết kế partition cho 3 bảng tiêu biểu của lakehouse Olist-like (mô hình bạn sẽ dựng đầy đủ ở lesson 34):

```
BẢNG                     VOLUME & QUERY PATTERN            SPEC QUYẾT ĐỊNH
──────────────────────   ───────────────────────────────   ─────────────────────────────
bronze.orders_raw        CDC stream ~vài GB/ngày           days(ingest_ts)
(hứng Kafka)             đọc lại theo đợt ingest            + WRITE LOCALLY ORDERED BY (ts)
                                                           (sort toàn cục quá đắt cho stream)

silver.orders            MERGE theo order_id mỗi phút;     days(order_ts)
(trạng thái hiện tại)    BI query theo khoảng ngày;         + bucket(16, order_id)
                         MERGE cần tìm nhanh dòng cũ        → MERGE chỉ rewrite/ghi delete
                                                             trong bucket chứa key
                                                           + sort order (order_id)

gold.revenue_daily       vài trăm KB/ngày,                 KHÔNG PARTITION GÌ CẢ
(aggregate cho BI)       query cả bảng hoặc theo năm       bảng bé — partition chỉ sinh
                                                           file vụn; stats + sort là đủ
```

Ba bài học production trong bảng trên:

1. **Bucket phục vụ MERGE**: điều kiện `ON t.order_id = s.order_id` prune theo bucket → MERGE COW chỉ rewrite các file trong bucket dính key, không phải cả partition ngày. Với CDC dày, đây là khác biệt chi phí hàng chục lần.
2. **Bảng nhỏ can đảm không partition**: gold vài trăm MB tổng — partition theo ngày sẽ tạo nghìn file vài KB, chậm hơn cả không partition. Quyết định "không làm gì" cũng là một quyết định thiết kế.
3. **Evolution là kế hoạch, không phải tai nạn**: khi Olist tăng trưởng 50× và silver nhận 100 GB/ngày, đổi `days` → `hours` bằng một câu ALTER, dữ liệu cũ để yên, compaction dọn dần. Ở Hive đây là dự án một quý; ở Iceberg là một dòng trong PR.

---

## 8. Hands-on Lab

**Mục tiêu**: cùng dữ liệu Olist, dựng 3 phiên bản bảng (không partition / partition tốt / over-partition), đo pruning bằng số file quét + thời gian, rồi làm partition evolution.

### Bước 1 — `labs/lab33/partition_showdown.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab33-partition-showdown")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/workspace/warehouse")
    .getOrCreate())

orders = (spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                         header=True, inferSchema=True)
          .select("order_id", "customer_id", "order_status",
                  F.to_timestamp("order_purchase_timestamp").alias("ts"))
          .filter("ts IS NOT NULL"))

variants = {
    "flat":  None,                       # không partition
    "month": "months(ts)",               # ~25 partition — hợp cỡ dataset 100k dòng
    "day":   "days(ts)",                 # ~610 partition — OVER cho dataset này!
}
for name, spec in variants.items():
    tbl = f"lakehouse.olist.orders_{name}"
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    part = f"PARTITIONED BY ({spec})" if spec else ""
    spark.sql(f"""CREATE TABLE {tbl}
                  (order_id STRING, customer_id STRING, order_status STRING, ts TIMESTAMP)
                  USING iceberg {part}""")
    orders.writeTo(tbl).append()
    nfiles = spark.sql(f"SELECT count(*) FROM {tbl}.files").first()[0]
    print(f"{name:6s}: {nfiles} data files")

# Đo: query 1 tháng trên từng biến thể — filter CỘT GỐC ts ở cả 3
for name in variants:
    tbl = f"lakehouse.olist.orders_{name}"
    t0 = time.time()
    n = spark.sql(f"""SELECT count(*) FROM {tbl}
                      WHERE ts >= TIMESTAMP '2018-03-01' AND ts < TIMESTAMP '2018-04-01'""").first()[0]
    print(f"{name:6s}: {n} dòng, {time.time()-t0:.2f}s")
spark.stop()
```

### Bước 2 — `labs/lab33/evolution.py`

```python
# ... (SparkSession config như trên)
t = "lakehouse.olist.orders_month"
# Giả sử volume tăng: chuyển spec months → days cho dữ liệu MỚI
spark.sql(f"ALTER TABLE {t} REPLACE PARTITION FIELD months(ts) WITH days(ts)")
# Ghi thêm một lô "dữ liệu mới" (lấy lại đơn 2018-08 làm mẫu, cộng 1 năm)
newdata = (spark.table(t).filter("ts >= '2018-08-01'")
           .withColumn("ts", F.col("ts") + F.expr("INTERVAL 365 DAYS")))
newdata.writeTo(t).append()
# Chứng minh 2 thế hệ layout chung sống:
spark.sql(f"""SELECT partition, count(*) files FROM {t}.files
              GROUP BY partition ORDER BY partition""").show(50, truncate=False)
# → partition kiểu {ts_month=..., ts_day=null} (cũ) và {ts_month=null, ts_day=...} (mới)
```

### Bước 3 — chạy & quan sát

```bash
make run-local F=labs/lab33/partition_showdown.py
make run-local F=labs/lab33/evolution.py
```

Ghi vào `labs/lab33/NOTES.md`: (1) số file mỗi biến thể — biến thể `day` sinh bao nhiêu file cho 100k dòng, mỗi file trung bình bao nhiêu KB (đây chính là over-partitioning bằng xương bằng thịt)? (2) query 1 tháng: biến thể nào nhanh nhất, có đúng dự đoán không (dataset bé — chênh lệch có thể nhỏ; nhìn số file scan trong tab SQL của UI thay vì chỉ tin đồng hồ)? (3) sau evolution: dữ liệu cũ có bị rewrite không (soi `file_path` + timestamp file)?

---

## 9. Assignment

**Easy** — Trên bảng `orders_month`: viết 2 query cùng lấy tháng 3/2018 — một filter theo `ts`, một cố tình filter theo `date_format(ts, 'yyyy-MM') = '2018-03'`. So số file scan (tab SQL trên UI hoặc explain). Giải thích vì sao câu sau prune kém/không prune (gợi ý: engine đẩy xuống được biểu thức nào?).

**Medium** — Multi-level vs single-level: tạo bảng partition `(years(ts), months(ts))` — lưu ý cú pháp cho phép nhiều field — so với chỉ `months(ts)`. Đếm partition, số file, chạy query theo khoảng 3 tháng vắt qua ranh giới năm. Kết luận: với transform thời gian của Iceberg, multi-level year/month có còn cần thiết như thời Hive (`year=/month=/day=` folder) không? Tại sao?

**Hard** — Bucketing cho MERGE: tạo 2 phiên bản bảng orders — `days(ts)` và `days(ts), bucket(8, order_id)`. Chạy cùng một `MERGE INTO` cập nhật status của 1,000 order_id ngẫu nhiên vào cả hai. So sánh qua `.snapshots` summary: mỗi bảng rewrite bao nhiêu file, bao nhiêu record? Qua Spark UI: BatchScan của bước tìm file đọc bao nhiêu file? Kết luận khi nào bucket đáng giá cho bảng nhận CDC.

**Production Challenge** — Kiểm toán partition của các bảng Iceberg trong `../kafka-flink`: với mỗi bảng, lấy spec (SHOW CREATE TABLE hoặc metadata.json), chạy thống kê `.partitions` (file/partition, MB/partition). Chấm điểm theo bộ 3 câu hỏi mục 3.4: spec có khớp query pattern thật của Trino/BI không? Partition có vụn (< 100MB) hay lệch không? Đề xuất cụ thể: giữ nguyên / evolution sang spec gì / thêm sort order gì — kèm lập luận số liệu.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Quyết định | Hệ quả tốt | Hệ quả xấu nếu lạm dụng |
|---|---|---|
| `days(ts)` trên bảng GB+/ngày | Query theo ngày/khoảng ngày đọc đúng phần cần | Trên bảng MB/ngày → partition vụn, ngàn file bé, planning chậm hơn cả full scan bảng gọn |
| `bucket(N, key)` | Point lookup/MERGE đọc 1/N bảng; join cùng-bucket có thể bỏ shuffle | Thêm chiều nhân số file (×N mỗi partition); N sai phải evolution |
| `identity(col)` cardinality thấp | Prune sắc cho filter đẳng thức | Trên cột cardinality cao = nổ metadata |
| Sort order | Min/max chặt → prune trong partition, nén tốt hơn (dòng giống nhau gần nhau) | Sort toàn cục khi ghi = thêm shuffle cho writer — cân nhắc LOCALLY ORDERED / dồn cho compaction |
| Không partition (bảng nhỏ) | Ít file, planning nhanh, stats vẫn prune | Không có — với bảng nhỏ đây thường là lựa chọn ĐÚNG |

Quy tắc ngón tay cái: **partition mục tiêu ≥ vài trăm MB**; tổng số partition một bảng nên đếm bằng nghìn, đừng bằng triệu; và "thêm một chiều partition" phải được một query pattern thật sự bảo lãnh.

---

## 11. Spark UI

Bài này Spark UI là máy đo pruning:

**Tab SQL / DataFrame — node `BatchScan`**: click query của lab, mở node scan của bảng Iceberg. Nhìn các chỉ số: số file/split được chọn, tổng size đọc. Chạy cùng query trên `orders_flat` vs `orders_month` — số file scan chênh nhau chính là công của partition pruning. Đây là con số bạn dùng để *chứng minh* thiết kế partition, thay vì cảm giác.

**Filter đẩy xuống được hay không**: trong plan, filter trên cột gốc (`ts >= ...`) xuất hiện ở phần pushed filters của scan → prune ở planning. Filter bọc trong hàm (`date_format(ts,...)='2018-03'`) không đẩy xuống được → nằm ở node Filter phía TRÊN scan → scan đọc tất. Nhìn vị trí filter trong plan là biết ngay query "ngoan" hay "hư".

**Tab Stages khi ghi bảng sort order**: `WRITE ORDERED BY` thêm stage shuffle/sort trước stage ghi — thấy được chi phí writer trả cho stats chặt. So với `LOCALLY ORDERED` (sort trong task, không thêm shuffle) để cảm nhận trade-off.

---

## 12. Common Mistakes

1. **Over-partitioning — sai lầm số 1 của ngành.** Partition `days` (thậm chí `hours`) cho bảng vài chục MB/ngày, hoặc identity trên cột cardinality cao. Kết quả: triệu partition, file vài KB, metadata phình, planning chậm, và query CHẬM HƠN bảng không partition. Luôn nhẩm size-per-partition trước khi gõ CREATE.
2. **`PARTITIONED BY (ts)` — identity trên timestamp.** Mỗi giá trị timestamp (đến microsecond) một partition. Bảng chết từ ngày đầu. Timestamp luôn đi với `days()`/`months()`/`hours()`.
3. **Mang thói quen Hive sang: tự chế cột `dt` string rồi partition identity theo nó.** Chạy được, nhưng vứt đi toàn bộ giá trị hidden partitioning: query filter `ts` gốc lại full scan như thời Hive, thêm cột thừa trong schema. Dùng transform trên cột gốc.
4. **Bọc cột partition trong hàm khi query** (`date_format(ts, ...)`, `cast(ts as date)` tùy chỗ) → filter không đẩy xuống được → mất prune. Viết điều kiện khoảng trực tiếp trên cột gốc.
5. **Kỳ vọng partition evolution tăng tốc dữ liệu cũ ngay.** Evolution chỉ áp cho file ghi sau đó; dữ liệu cũ giữ layout cũ đến khi rewrite_data_files. Lập kế hoạch rewrite dần nếu cần.
6. **Bucket theo cột không bao giờ xuất hiện trong filter/join** — trả giá ×N số file cho zero lợi ích. Bucket phải được query pattern bảo lãnh, như mọi chiều partition khác.
7. **Bỏ quên sort order** rồi thắc mắc "partition rồi sao filter theo price vẫn quét cả partition?" — partition prune theo chiều thời gian, trong partition phải nhờ min/max; min/max chỉ chặt khi có sort. Hai tầng, thiếu một là hụt.

---

## 13. Interview

**Junior:**

1. *Partition để làm gì?* — Chia dữ liệu theo giá trị cột/transform để query có filter tương ứng chỉ đọc phần liên quan (partition pruning) thay vì cả bảng. Mục đích duy nhất: đọc ít dữ liệu hơn — không phải để "gọn thư mục".
2. *Hidden partitioning của Iceberg là gì, "hidden" ở chỗ nào?* — Partition định nghĩa bằng transform trên cột gốc (`days(ts)`, `bucket(16, id)`), lưu trong metadata; không có cột partition riêng trong dữ liệu. "Hidden" với người dùng: họ filter cột gốc như thường, engine tự dịch filter sang partition để prune — không cần biết, không thể quên.
3. *Kể các transform partition của Iceberg.* — identity; year/month/day/hour cho thời gian; bucket(N, col) — hash chia N nhóm cho key cardinality cao; truncate(W, col) — cắt prefix/bội số. Một bảng có thể kết hợp nhiều field, ví dụ `days(ts), bucket(16, user_id)`.
4. *Ở Hive, partition theo dt mà query filter theo order_ts thì sao? Iceberg thì sao?* — Hive: full scan — engine không biết dt sinh từ order_ts, quan hệ đó chỉ nằm trong đầu người viết pipeline. Iceberg: spec là `day(order_ts)` ngay trên cột gốc nên filter theo order_ts được dịch thành điều kiện partition và prune bình thường.

**Mid:**

5. *Partition evolution hoạt động thế nào mà không cần rewrite?* — Metadata giữ danh sách partition-specs; mỗi data file (qua manifest) gắn spec-id thời điểm ghi. ALTER thêm spec mới làm mặc định cho writer từ đó; file cũ giữ layout cũ. Khi query, planner prune từng nhóm manifest theo spec của nó. Muốn dữ liệu cũ theo layout mới thì rewrite_data_files dần — tùy chọn, không bắt buộc.
6. *Chọn partition spec cho bảng mới dựa trên gì?* — Bộ 3: (1) query pattern — top query filter theo cột nào; (2) size mỗi partition — mục tiêu vài trăm MB–vài GB, nhẩm từ volume/ngày; (3) cardinality — identity chỉ cho cột ít giá trị, cardinality cao thì bucket. Và can đảm không partition khi bảng nhỏ: stats + sort order đủ dùng.
7. *Bucketing khi nào tốt hơn partition thời gian?* — Khi truy cập chủ đạo là đẳng thức theo key cardinality cao: point lookup, MERGE/upsert theo id, join hai bảng lớn theo cùng key (cùng bucket spec → storage-partitioned join bỏ shuffle). Số partition cố định N nên không nổ metadata. Không giúp gì cho query khoảng thời gian — nên thực tế thường kết hợp days(ts) + bucket(N, key) khi cả hai pattern cùng tồn tại.
8. *Sort order đóng vai trò gì bên cạnh partition?* — Partition prune theo chiều thô; trong một partition còn nhiều file, pruning tiếp dựa min/max stats per file trong manifest. Ghi có sort làm khoảng min/max các file không chồng lấn → filter theo cột sort loại được hầu hết file; kèm lợi ích nén. Chi phí: sort khi ghi (shuffle) — có thể chuyển sang locally ordered hoặc để compaction sort ban đêm gánh.

**Senior:**

9. *Bảng fact 500 GB/ngày, query: 80% theo khoảng ngày + status, 15% lookup theo order_id, 5% ad-hoc. Thiết kế spec + sort? Biện luận cả điều bạn KHÔNG làm.* — `days(ts)` là nền (500 GB/partition hơi to nhưng hours sinh 24× partition — chỉ chuyển hours nếu query thường hẹp trong ngày); sort order `(order_status, order_id)` trong partition: status phục vụ 80% query qua min/max, order_id giúp lookup nhờ stats chặt. KHÔNG bucket theo order_id ngay: 15% lookup chưa bảo lãnh chi phí ×N file mọi partition — đo trước, nếu lookup thành SLA thì evolution thêm `bucket(32, order_id)` sau (Iceberg cho phép đổi mà không rewrite). KHÔNG partition theo status: cardinality thấp nhưng phân bố lệch nặng (delivered ~97%) → partition lệch không giúp prune thực chất, để sort gánh.
10. *Sau 2 năm, bảng đã REPLACE spec 3 lần, query trên vùng dữ liệu cũ chậm hơn hẳn vùng mới. Giải thích và lộ trình xử lý không downtime?* — Nguyên nhân: dữ liệu cũ vẫn layout spec cũ (prune theo tiêu chí cũ, có thể thô hơn), nhiều thế hệ manifest/spec khiến planning kém đều; file cũ có thể chưa từng được sort nên stats lỏng. Xử lý: dùng `rewrite_data_files` với `where` khoanh từng khoảng thời gian cũ, chạy dần ngoài giờ cao điểm (partial-progress) — rewrite ghi file theo spec + sort order hiện hành, commit atomic nên reader không downtime; sau đó rewrite_manifests gộp metadata, expire_snapshots dọn file cũ. Kiểm chứng bằng số file scan trên query chuẩn trước/sau từng đợt. Bài học thiết kế: evolution rẻ ở metadata nhưng nợ layout vẫn phải trả dần bằng compaction — lên lịch trả nợ ngay khi đổi spec.

---

## 14. Summary

### Mindmap

```
                 PARTITIONING & HIDDEN PARTITIONING
                              │
    ┌───────────────┬────────┴────────┬──────────────────┬───────────────┐
    ▼               ▼                 ▼                  ▼               ▼
 HIVE (cũ)      HIDDEN (Iceberg)   EVOLUTION         CHỌN SPEC       SORT ORDER
    │               │                 │                  │               │
 folder vật lý   transform:        ALTER TABLE        3 câu hỏi:     WRITE ORDERED BY
 cột dt tự chế   days/months/      ADD/DROP/REPLACE   query pattern  → min/max chặt
 quên filter dt  hours/bucket/     PARTITION FIELD    size/partition → prune TRONG
 = full scan     truncate          KHÔNG rewrite      (≥ trăm MB)    partition
 đổi spec =      filter cột GỐC    file cũ giữ        cardinality    LOCALLY ORDERED
 rewrite cả bảng vẫn prune         layout cũ          bucket cho key = rẻ hơn cho
                 (metadata, không  (spec-id per       cao; bảng nhỏ  writer
                  folder)          manifest)          KHÔNG partition PITFALL:
                                                                     over-partitioning
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 2 căn bệnh của Hive partition và cách hidden partitioning chữa từng bệnh.
- [ ] Vẽ lại được đường đi: filter cột gốc → transform → prune manifest/file.
- [ ] Thuộc 4 nhóm transform và ví dụ dùng đúng chỗ cho mỗi loại.
- [ ] Làm được partition evolution và giải thích vì sao không rewrite.
- [ ] Nhẩm được size-per-partition để bắt lỗi over-partitioning trước khi CREATE.
- [ ] Nói được khi nào bucket đáng giá, khi nào KHÔNG partition là đúng.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 34 — Medallion architecture & data modeling.**

Bạn đã có đủ đồ nghề Iceberg: internals, DDL/MERGE/time travel, maintenance, partitioning. Câu hỏi tiếp theo không còn là "công cụ dùng thế nào" mà là "xếp các bảng thành hệ thống ra sao": bronze giữ dữ liệu thô đến mức nào, silver làm sạch theo chuẩn gì, gold model theo star schema với fact/dimension thế nào, và SCD type 2 lưu lịch sử thay đổi của seller ra sao. Lesson 34 là nơi Olist của bạn từ "mấy file CSV" trở thành một lakehouse có kiến trúc — nền cho toàn bộ phần data modeling còn lại của module 5.

> Gõ **"Continue"** khi sẵn sàng.
