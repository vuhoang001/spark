# Lesson 30 — Iceberg internals: metadata, snapshot, manifest

> Module 5 · Lakehouse & Iceberg · Tuần 16 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao "bảng Hive-style" (thư mục + Parquet files) sụp đổ ở quy mô lớn — và Iceberg sinh ra để sửa đúng những vết nứt đó.
- Kiến trúc metadata 3 tầng của Iceberg: **catalog → metadata.json → manifest list → manifest file → data files**.
- **Snapshot** = một version bất biến của bảng; commit = atomic swap con trỏ metadata → đó chính là ACID.
- Mổ xẻ file `v1.metadata.json` thật: từng trường quan trọng nghĩa là gì.
- Manifest chứa **min/max stats per column** → Iceberg loại bỏ file không cần đọc TRƯỚC khi Spark chạm vào dữ liệu (file pruning).

Sau bài này bạn phải làm được:

- Vẽ lại sơ đồ metadata 3 tầng từ trí nhớ, chỉ đúng file nào nằm ở tầng nào trên đĩa.
- Mở thư mục `metadata/` của một bảng Iceberg thật, đọc `vN.metadata.json` và trả lời: bảng có mấy snapshot, snapshot hiện tại là cái nào, trỏ đến manifest list nào.
- Giải thích cho đồng nghiệp: "tại sao Iceberg có ACID trên object storage vốn không có transaction?"

Kiến thức dùng trong thực tế: khi bảng Iceberg query chậm, storage phình, hay 2 job ghi đè nhau — mọi câu trả lời đều nằm trong đống metadata này. Repo `../kafka-flink` của bạn đang chạy Iceberg + Trino production-like: sau bài này bạn mở thư mục metadata của nó ra là đọc được như đọc báo.

---

## 2. Why

### Vấn đề: "bảng" trong data lake cổ điển chỉ là... một thư mục

Trước Iceberg, một "bảng" trên data lake (kiểu Hive) được định nghĩa thế này:

```
s3://bucket/warehouse/orders/
   ├── dt=2018-01-01/
   │      ├── part-0001.parquet
   │      └── part-0002.parquet
   ├── dt=2018-01-02/
   │      └── part-0003.parquet
   └── ...
```

Bảng = "mọi file nằm trong thư mục này". Metastore (Hive) chỉ nhớ: bảng orders ở path nào, có những partition nào. Nghe ổn, cho đến khi:

1. **List directory là O(số file)**: bảng 1 triệu file trên S3 → mỗi query phải gọi hàng nghìn lệnh LIST (S3 trả tối đa 1000 key/lần) chỉ để biết "bảng gồm những file nào". Chưa đọc byte dữ liệu nào đã mất vài phút.
2. **Không ACID**: job đang ghi 200 file, chết ở file thứ 100 → người đọc thấy nửa bảng rác. Hai job cùng ghi → file trộn lẫn, không ai phát hiện. Xóa dữ liệu cũ trong khi có người đang đọc → query fail giữa chừng.
3. **Partition là cấu trúc vật lý cứng**: muốn đổi từ partition theo ngày sang theo tháng? Rewrite TOÀN BỘ bảng. Người query quên `WHERE dt = ...` đúng cột partition? Full scan, không ai cảnh báo.
4. **Schema gắn với file, đổi tên cột là thảm họa**: Hive track cột theo tên/vị trí. Rename cột → dữ liệu cũ đọc sai hoặc thành null.

Netflix chịu trận với những vấn đề này ở quy mô hàng trăm petabyte, và năm 2018 họ tạo ra Iceberg với một ý tưởng lật ngược: **đừng định nghĩa bảng bằng thư mục — định nghĩa bảng bằng metadata liệt kê ĐÍCH DANH từng file**.

### Nếu không có table format thì sao?

Bạn sẽ phải: tự viết cơ chế "ghi vào thư mục tạm rồi rename" (S3 không có rename atomic — vỡ), tự quản lý danh sách file trong DB riêng (tự chế Iceberg phiên bản lỗi), hoặc chấp nhận bảng đọc-ghi không an toàn và pipeline thỉnh thoảng ra kết quả sai không ai biết.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| ACID trên object storage "ngu" (S3/HDFS/local) | Metadata tự nó cũng là file — phải maintain (lesson 32) |
| Đọc bảng không cần LIST directory — O(metadata) | Mỗi commit sinh snapshot mới → storage phình nếu bỏ bê |
| Time travel, rollback, schema/partition evolution | Học thêm một tầng khái niệm (bài này) |
| File pruning bằng stats → skip cả file không đọc | Ghi chậm hơn chút (phải commit metadata) |

> Bài học Senior: Iceberg không thay Parquet — **data file vẫn là Parquet**. Iceberg là tầng "sổ sách kế toán" ghi lại file nào thuộc bảng ở version nào. Hiểu điều này thì mọi thứ còn lại chỉ là chi tiết.

---

## 3. Theory

### 3.1. Thuật ngữ nền (mọi bài của module 5 dùng lại)

| Thuật ngữ | Nghĩa |
|---|---|
| **Table format** | Đặc tả cách tổ chức metadata để một đống file trở thành "bảng" có ACID. Iceberg/Delta/Hudi là table format; Parquet/ORC là *file* format. |
| **Catalog** | Nơi giữ con trỏ "bảng X → metadata file hiện tại là gì". Hive Metastore, AWS Glue, REST catalog, hoặc hadoop catalog (dựa trên file). |
| **Snapshot** | Trạng thái đầy đủ của bảng tại một thời điểm — bất biến, không bao giờ sửa. |
| **Manifest list** | 1 file Avro per snapshot, liệt kê các manifest file thuộc snapshot đó. |
| **Manifest file** | File Avro liệt kê các data file kèm stats (min/max, null count...). |
| **Commit** | Hành động tạo metadata.json mới và swap con trỏ catalog — atomic. |

### 3.2. Kiến trúc metadata 3 tầng — sơ đồ phải thuộc lòng

```
┌─────────────────────────────────────────────────────────────────┐
│  CATALOG  (Hive Metastore / Glue / REST / hadoop)               │
│  "db.orders" ──► con trỏ: metadata hiện tại = v3.metadata.json  │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  METADATA FILE   warehouse/db/orders/metadata/v3.metadata.json  │
│  • schema (các version), partition spec, sort order             │
│  • danh sách MỌI snapshot còn sống                              │
│  • current-snapshot-id = 8321...                                │
│       snapshot 6412... ──► snap-6412...avro (manifest list)     │
│       snapshot 8321... ──► snap-8321...avro (manifest list) ◄── │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼  (đi theo snapshot hiện tại)
┌─────────────────────────────────────────────────────────────────┐
│  MANIFEST LIST   metadata/snap-8321...avro   (1 file/snapshot)  │
│  liệt kê manifest files + partition range + đếm added/deleted:  │
│    ├── manifest-a.avro  (partition dt: 2018-01-01..01-15)       │
│    └── manifest-b.avro  (partition dt: 2018-01-16..01-31)       │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  MANIFEST FILE   metadata/manifest-a.avro                       │
│  liệt kê ĐÍCH DANH data file + stats per column:                │
│    ├── data/dt=2018-01-01/00001.parquet                         │
│    │     rows=51200, price: min=0.85 max=4590.0, nulls=0        │
│    └── data/dt=2018-01-02/00002.parquet                         │
│          rows=48000, price: min=1.20 max=6735.0, nulls=3        │
└───────────────────────────┬─────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  DATA FILES      warehouse/db/orders/data/**.parquet            │
│  dữ liệu thật — Parquet bình thường, bất biến, không bao giờ    │
│  bị sửa tại chỗ (chỉ thêm file mới / đánh dấu file cũ removed)  │
└─────────────────────────────────────────────────────────────────┘
```

Đọc bảng = đi từ trên xuống: hỏi catalog → đọc 1 metadata.json → đọc 1 manifest list → đọc vài manifest → có danh sách đích danh data file cần đọc. **Không một lệnh LIST directory nào.** Số lần đọc metadata tỉ lệ với số manifest, không tỉ lệ với số data file.

> **Analogy thư viện**: Data file = sách trên kệ. Manifest = phiếu mục lục của một kệ (kệ này có sách gì, chủ đề từ A đến D). Manifest list = danh sách các kệ của "phiên bản thư viện" hôm nay. Metadata.json = sổ cái của thư viện, ghi mọi phiên bản. Catalog = tấm bảng ngoài cửa: "sổ cái hiện hành là quyển số 3". Muốn biết thư viện có sách gì, bạn KHÔNG đi sờ từng kệ (list directory) — bạn đọc sổ. Và muốn "quay về thư viện của tuần trước", chỉ cần mở trang sổ cũ: sách chưa hề bị dời đi.

### 3.3. Snapshot = version bất biến, commit = atomic swap

Nguyên tắc vàng của Iceberg: **không sửa gì tại chỗ, chỉ thêm mới rồi đổi con trỏ**.

```
Thời gian ──────────────────────────────────────────────►

commit 1 (INSERT)        commit 2 (INSERT)       commit 3 (DELETE)
v1.metadata.json    →    v2.metadata.json   →    v3.metadata.json
snapshot S1              snapshot S1              snapshot S1
                         snapshot S2 ◄current     snapshot S2
                                                  snapshot S3 ◄current
files: A,B               files: A,B + C,D         files: A,C,D (B removed)
```

- Mỗi lần ghi: (1) ghi data file mới, (2) ghi manifest mới, (3) ghi manifest list mới, (4) ghi `vN+1.metadata.json` chứa TOÀN BỘ lịch sử snapshot, (5) **swap con trỏ catalog** từ vN sang vN+1.
- Bước (1)–(4) là ghi file mới — người đọc chưa thấy gì, vì họ vẫn đi theo con trỏ cũ. Chỉ bước (5) — một thao tác compare-and-swap duy nhất — quyết định "commit thành hay không". Fail ở bất kỳ đâu = bảng vẫn nguyên vẹn ở version cũ, chỉ để lại vài file mồ côi (dọn ở lesson 32).
- Người đọc mở query lúc 10:00 dùng snapshot của 10:00 đến hết query, kể cả 10:01 có commit mới → **snapshot isolation**, reader không bao giờ thấy bảng "nửa nạc nửa mỡ".

Đó là toàn bộ bí mật ACID của Iceberg: **atomicity nằm ở một lần swap con trỏ**, không cần storage hỗ trợ transaction.

### 3.4. Iceberg vs Delta Lake vs Hudi (bảng ngắn để định vị)

| | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| Metadata | Cây snapshot/manifest (Avro+JSON) | Transaction log `_delta_log/*.json` + checkpoint Parquet | Timeline `.hoodie/` + metadata table |
| Hidden partitioning | ✅ (điểm mạnh nhất) | ❌ (generated columns đỡ một phần) | ❌ |
| Partition evolution | ✅ không rewrite | ❌ | ❌ |
| Engine trung lập | Rất tốt (Spark/Trino/Flink/Snowflake...) | Mạnh nhất trong hệ Databricks | Mạnh về streaming upsert |
| Xuất thân | Netflix | Databricks | Uber |

Cả ba đều cho ACID + time travel. Khóa này chọn Iceberg vì: trung lập engine (Spark ghi, Trino đọc — đúng kiến trúc repo `kafka-flink` của bạn) và hidden partitioning (lesson 33).

---

## 4. Internal

Chuyện gì xảy ra khi bạn `INSERT INTO orders ...` trên bảng Iceberg (hadoop catalog):

```
① Spark chạy job, ghi data files mới:
      data/dt=2018-01/00042-....parquet
        │
② Với mỗi data file, Iceberg thu stats từ Parquet footer:
      record_count, column_sizes, value_counts, null_value_counts,
      lower_bounds (min), upper_bounds (max) per column
        │
③ Ghi manifest file mới (Avro) liệt kê các data file + stats trên
        │
④ Ghi manifest list mới: manifest cũ (reuse, không copy data!)
      + manifest mới → thành snapshot S_new
        │
⑤ Ghi v(N+1).metadata.json = bản sao metadata cũ
      + snapshot S_new + current-snapshot-id = S_new
        │
⑥ ATOMIC SWAP con trỏ:
      • hadoop catalog: ghi đè file version-hint.text (nội dung: "N+1")
        — kèm check "version N+1 chưa tồn tại" (rename atomic trên HDFS/local)
      • Hive/Glue/REST catalog: compare-and-swap trong database
        "nếu con trỏ vẫn = vN thì đổi thành vN+1, không thì fail"
        │
⑦ Nếu ⑥ fail (người khác commit trước): đọc lại metadata mới nhất,
      kiểm tra xung đột, thử commit lại (optimistic concurrency — lesson 31)
```

Còn khi bạn **đọc** với filter `WHERE price > 5000`:

```
① Đọc metadata.json → lấy current snapshot → đọc manifest list
② PRUNE tầng 1: manifest list có partition range của từng manifest
      → bỏ qua manifest không dính partition cần đọc
③ PRUNE tầng 2: trong manifest, mỗi data file có min/max của price
      → file có max(price)=4590 < 5000 → BỎ, không mở file
④ Danh sách file sống sót → giao cho Spark tạo task đọc
```

Bước ② ③ chạy ở **planning time trên driver**, trước khi executor động đậy. Đây là lý do bảng Iceberg trả lời query chọn lọc nhanh hơn hẳn bảng thư mục: phần lớn file bị loại từ vòng gửi xe.

### Mổ xẻ `v1.metadata.json` thật — các trường phải biết

```json
{
  "format-version": 2,               // spec v1 hay v2 (v2 có row-level delete — lesson 31)
  "table-uuid": "9c12...",           // căn cước bảng — đổi tên bảng, uuid không đổi
  "location": "/workspace/warehouse/db/orders",
  "last-updated-ms": 1767851022000,
  "last-column-id": 9,               // cột track bằng ID, không bằng tên → rename an toàn
  "schemas": [ { "schema-id": 0, "fields": [
      { "id": 1, "name": "order_id", "type": "string", "required": false }, ...
  ] } ],
  "current-schema-id": 0,
  "partition-specs": [ { "spec-id": 0, "fields": [
      { "name": "ts_day", "transform": "day", "source-id": 4, "field-id": 1000 }
  ] } ],                             // partition là TRANSFORM trên cột nguồn — lesson 33
  "sort-orders": [ ... ],
  "current-snapshot-id": 8321459776,
  "snapshots": [
    { "snapshot-id": 6412...,
      "timestamp-ms": 1767850000000,
      "summary": { "operation": "append",
                   "added-data-files": "4", "added-records": "99441" },
      "manifest-list": ".../metadata/snap-6412...avro",
      "schema-id": 0 },
    { "snapshot-id": 8321..., "parent-snapshot-id": 6412..., ... }
  ],
  "snapshot-log":  [ ... ],          // lịch sử: lúc nào snapshot nào là current (time travel dùng cái này)
  "metadata-log":  [ ... ],          // các metadata.json đời trước
  "properties": { "write.parquet.compression-codec": "zstd", ... }
}
```

Ba insight quan trọng: (1) cột và partition đều track bằng **ID số** chứ không phải tên → rename/reorder không phá dữ liệu cũ; (2) metadata.json chứa **cả lịch sử** → chỉ cần 1 file này là time travel được; (3) partition là **transform + source-id** → nền móng của hidden partitioning.

---

## 5. API

Bài này thiên về đọc-hiểu, API chỉ cần đủ để tạo bảng và soi metadata.

### Config catalog trong SparkSession (dạng tối thiểu, lesson 31 học đủ)

```python
spark = (SparkSession.builder
    .appName("lesson30")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/workspace/warehouse")
    .getOrCreate())
```
- **Ý nghĩa**: đăng ký một catalog tên `lakehouse`, kiểu hadoop (metadata nằm ngay trên filesystem — hoàn hảo để lab vì mở file ra soi được).
- **Pitfall**: quên `spark.sql.extensions` → `MERGE INTO`, `CALL` procedure sau này không chạy. Lần chạy đầu tải jar từ Maven (~40 MB) — cần mạng, các lần sau dùng cache Ivy.

### `writeTo(...).create()` / SQL `CREATE TABLE ... USING iceberg`

```python
df.writeTo("lakehouse.db.orders").create()          # DataFrame API
spark.sql("CREATE TABLE lakehouse.db.t (id INT) USING iceberg")  # SQL
```
- **Pitfall**: `df.write.format("iceberg").save(path)` kiểu path-based cũng chạy nhưng né catalog → mất quyền lợi quản lý. Luôn đi qua tên bảng 3 phần `catalog.db.table`.

### Metadata tables — cửa sổ soi nội tạng

```sql
SELECT * FROM lakehouse.db.orders.snapshots;  -- mọi snapshot: id, operation, summary
SELECT * FROM lakehouse.db.orders.manifests;  -- manifest của snapshot hiện tại
SELECT * FROM lakehouse.db.orders.files;      -- từng data file + stats min/max
SELECT * FROM lakehouse.db.orders.history;    -- dòng thời gian current-snapshot
```
- **Ý nghĩa**: chính là cây metadata ở section 3.2, phơi ra dạng bảng SQL. Công cụ chẩn đoán số 1 với Iceberg — vai trò tương đương Spark UI.
- **Pitfall**: `.files` trên bảng triệu file là một query nặng — trên bảng production hãy select cột cần và limit.

---

## 6. Demo nhỏ

```
Input:  DataFrame 5 dòng tạo tay
   ↓    ghi thành bảng Iceberg (commit 1) → soi thư mục metadata/
   ↓    append thêm 2 dòng (commit 2)     → soi lại
Output: 2 snapshot trong .snapshots, v1 → v2 metadata.json trên đĩa
```

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("demo30").master("local[2]")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/tmp/demo_warehouse")
    .getOrCreate())

df = spark.createDataFrame(
    [("o1", 120.0), ("o2", 80.0), ("o3", 300.0), ("o4", 150.0), ("o5", 500.0)],
    ["order_id", "amount"])

df.writeTo("lakehouse.demo.orders").createOrReplace()      # commit 1
spark.createDataFrame([("o6", 999.0), ("o7", 5.0)], ["order_id", "amount"]) \
     .writeTo("lakehouse.demo.orders").append()            # commit 2

spark.sql("""SELECT snapshot_id, operation, summary['added-records'] AS added
             FROM lakehouse.demo.orders.snapshots ORDER BY committed_at""").show(truncate=False)
# 2 dòng: append (5 records) rồi append (2 records)
spark.stop()
```

Chạy xong, tự tay `ls /tmp/demo_warehouse/demo/orders/metadata/` — bạn sẽ thấy `v1.metadata.json`, `v2.metadata.json`, (và v3 nếu createOrReplace tạo 2 commit), các `snap-*.avro`, `*-m0.avro`, `version-hint.text`. Mở `cat version-hint.text`: chỉ một con số — đó chính là "con trỏ catalog" của hadoop catalog.

---

## 7. Production Example

Nhìn lại kiến trúc repo `kafka-flink` của bạn — giờ bạn hiểu tại sao Iceberg đứng ở vị trí đó:

```
PostgreSQL → Debezium → Kafka → Spark (bronze/silver/gold)
                                   ↓ ghi
                              ICEBERG trên MinIO/S3   ← bài hôm nay
                                   ↑ đọc
                                 Trino → Superset/BI
```

Điều mà metadata 3 tầng mua được cho kiến trúc này:

1. **Spark ghi, Trino đọc, không giẫm chân**: Trino query lúc 10:00 chốt theo snapshot 10:00; Spark commit lúc 10:01 chỉ đổi con trỏ — query đang chạy không hề hấn. Không cần lock, không cần điều phối giữa 2 engine — họ chỉ cần cùng nhìn một catalog.
2. **Streaming ghi mỗi phút vẫn an toàn**: mỗi micro-batch là 1 commit atomic. Chết giữa chừng? Version cũ còn nguyên. (Cái giá: hàng nghìn snapshot/ngày — lesson 32 xử.)
3. **Query BI nhanh**: Trino không LIST bucket MinIO; nó đọc manifest, prune bằng partition + min/max, chỉ mở đúng file cần. Với bảng fact vài trăm nghìn file, đây là khác biệt giữa 2 giây và 2 phút.
4. **Audit & debug**: "số liệu hôm qua khác hôm nay, ai ghi gì lúc nào?" → `SELECT * FROM t.snapshots` trả lời bằng chứng cứ, thay vì đi hỏi từng team.

Netflix, Apple, LinkedIn, Airbnb đều chạy Iceberg cỡ petabyte với đúng mô hình này — nhiều engine cùng đọc/ghi một kho bảng qua catalog chung.

---

## 8. Hands-on Lab

**Mục tiêu**: tạo bảng Iceberg từ Olist thật, rồi mổ xẻ từng tầng metadata bằng cả SQL lẫn... `cat`.

### Bước 1 — viết `labs/lab30/dissect_iceberg.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab30-dissect-iceberg")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/workspace/warehouse")
    .getOrCreate())

orders = (spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                         header=True, inferSchema=True)
          .withColumn("ts", F.to_timestamp("order_purchase_timestamp")))

# Commit 1: tạo bảng
orders.filter(F.year("ts") == 2017).writeTo("lakehouse.olist.orders").createOrReplace()
# Commit 2: append thêm năm 2018
orders.filter(F.year("ts") == 2018).writeTo("lakehouse.olist.orders").append()

print("=== SNAPSHOTS ===")
spark.sql("""SELECT snapshot_id, parent_id, operation,
                    summary['added-data-files']  AS add_files,
                    summary['added-records']     AS add_rows
             FROM lakehouse.olist.orders.snapshots
             ORDER BY committed_at""").show(truncate=False)

print("=== MANIFESTS (cua snapshot hien tai) ===")
spark.sql("""SELECT path, length, added_data_files_count, existing_data_files_count
             FROM lakehouse.olist.orders.manifests""").show(truncate=False)

print("=== FILES + STATS min/max ===")
spark.sql("""SELECT file_path, record_count,
                    lower_bounds[5] AS ts_min_raw, upper_bounds[5] AS ts_max_raw
             FROM lakehouse.olist.orders.files LIMIT 10""").show(truncate=False)
spark.stop()
```

### Bước 2 — chạy

```bash
make run-local F=labs/lab30/dissect_iceberg.py
# lần đầu chờ tải jar iceberg-spark-runtime từ Maven
```

### Bước 3 — mổ xẻ bằng tay (phần đắt giá nhất của lab)

```bash
docker exec spark-mastery-spark-submit-1 ls -la /workspace/warehouse/olist/orders/metadata/
docker exec spark-mastery-spark-submit-1 cat /workspace/warehouse/olist/orders/metadata/version-hint.text
docker exec spark-mastery-spark-submit-1 cat /workspace/warehouse/olist/orders/metadata/v2.metadata.json | python3 -m json.tool | head -80
```

### Bước 4 — quan sát & ghi chép

Ghi vào `labs/lab30/NOTES.md`:

1. Có mấy file `vN.metadata.json`? Đối chiếu với số commit bạn đã làm (lưu ý `createOrReplace` có thể sinh nhiều hơn 1 version).
2. Trong metadata.json mới nhất: `current-snapshot-id` là gì? Nó khớp với dòng nào trong `.snapshots`?
3. Đếm số `snap-*.avro` (manifest list) — có đúng bằng số snapshot không?
4. Snapshot 2 (append) có manifest list chứa cả manifest của snapshot 1 không (cột `existing_data_files_count`)? Điều này chứng minh Iceberg **reuse** manifest cũ chứ không copy dữ liệu.

---

## 9. Assignment

**Easy** — Từ output lab và file metadata.json: bảng `lakehouse.olist.orders` có bao nhiêu snapshot, bao nhiêu manifest, current snapshot id là gì? Trường nào trong metadata.json cho bạn biết bảng đang ở schema version nào?

**Medium** — Làm timeline: thực hiện thêm commit 3 = `DELETE FROM lakehouse.olist.orders WHERE order_status = 'canceled'` (cần `spark.sql`). Sau đó dùng `.snapshots` + `.history` vẽ timeline 3 snapshot: mỗi snapshot operation gì, thêm/xóa bao nhiêu file, bao nhiêu record (soi `summary`). Giải thích: DELETE có xóa file Parquet cũ trên đĩa không? (Kiểm chứng bằng `ls data/`.)

**Hard** — Tính % overhead metadata: dùng `du -sb` (hoặc script Python với `os.walk`) đo tổng byte của thư mục `metadata/` và `data/` của bảng. Tính tỉ lệ. Sau đó chạy vòng lặp 20 lần append nhỏ (mỗi lần ~100 dòng), đo lại. Kết luận: metadata phình theo cái gì — số dòng hay số commit? Điều này báo trước vấn đề gì với streaming (gợi ý cho lesson 32)?

**Production Challenge** — Mở repo `../kafka-flink`, tìm bảng Iceberg mà pipeline của bạn đang ghi (trong MinIO hoặc warehouse path). Đọc metadata.json mới nhất của một bảng và viết báo cáo 10 dòng: format-version mấy? Bao nhiêu snapshot đang tồn? Partition spec là gì? Có property nào bạn chưa hiểu (liệt kê — nợ kiến thức trả dần trong module này)?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Hive-style table | Iceberg | Tại sao |
|---|---|---|---|
| Planning query trên bảng 1M file | Phút (LIST directory từng trang) | Giây (đọc vài manifest Avro) | Metadata liệt kê đích danh file |
| `WHERE price > 5000` (1% file chứa giá đó) | Mở mọi file, dựa Parquet footer từng file | Loại 99% file từ manifest stats, không mở | Min/max nằm ở manifest — prune trước khi chạm file |
| Query khi job khác đang ghi | Kết quả rác hoặc fail | Snapshot isolation, đọc version nhất quán | Reader ghim theo snapshot |
| Ghi 1 batch nhỏ | Ghi file là xong | Ghi file + manifest + manifest list + metadata.json + swap | Commit có chi phí cố định → đừng commit li ti quá dày |

Câu tự vấn mới từ module này: *"query của tôi prune được bao nhiêu file — và stats trong manifest có đủ chặt để prune không?"* (sort order làm stats chặt hơn — lesson 33).

---

## 11. Spark UI

Với Iceberg, chẩn đoán chia làm 2 nơi — quen dần từ hôm nay:

**Tab SQL / DataFrame** (UI :4040): mở query đọc bảng Iceberg, click node scan — bạn sẽ thấy `BatchScan lakehouse.olist.orders` kèm số split/file thực đọc. So sánh con số này với tổng số file trong `.files` → đo được hiệu quả pruning. Job ghi Iceberg thường có thêm job nhỏ đuôi cùng — đó là bước ghi manifest/commit.

**Metadata tables** (vai trò như "Spark UI của bảng"): `.snapshots` = lịch sử commit (tương đương tab Jobs), `.files` = tồn kho file + stats, `.manifests` = sức khỏe tầng manifest. Từ lesson 32, các quyết định maintenance đều bắt đầu bằng việc đọc mấy bảng này.

Ghi nhớ: Spark UI trả lời "job chạy thế nào", metadata tables trả lời "bảng đang ở trạng thái nào".

---

## 12. Common Mistakes

1. **Nghĩ Iceberg là file format thay Parquet.** Sai tầng: Parquet là file format, Iceberg là table format quản lý *danh sách* file Parquet + version. Câu này sai ở interview là bị loại sớm.
2. **Xóa file trong thư mục `data/` bằng tay** vì "thấy file cũ không ai dùng". Metadata vẫn trỏ đến file đó → mọi query sau này fail `FileNotFoundException`. Mọi thao tác xóa phải đi qua Iceberg (expire/remove orphan — lesson 32).
3. **Ghi vào path của bảng bằng `df.write.parquet(path)` trần.** File nằm đó nhưng không manifest nào biết → bảng "không thấy" dữ liệu, và file thành rác mồ côi.
4. **Tưởng DELETE giải phóng dung lượng ngay.** DELETE chỉ tạo snapshot mới không tham chiếu file cũ (hoặc ghi delete file); byte trên đĩa còn nguyên đến khi expire snapshots.
5. **Dùng hadoop catalog cho production trên S3.** Swap con trỏ của hadoop catalog dựa vào rename atomic — S3 không có rename atomic → 2 writer có thể cùng "thắng". Lab local thì ổn; production dùng Hive/Glue/REST/JDBC catalog.
6. **Không bao giờ mở metadata tables**, chỉ query dữ liệu. Bảng chậm/phình mà không đọc `.snapshots`/`.files` thì giống job Spark chậm mà không mở Spark UI — mò mẫm trong bóng tối.

---

## 13. Interview

**Junior:**

1. *Iceberg là gì, khác Parquet chỗ nào?* — Iceberg là table format: tầng metadata biến một tập file thành bảng có ACID, snapshot, schema evolution. Parquet là file format lưu dữ liệu dạng cột. Bảng Iceberg *chứa* các data file Parquet; Iceberg quản lý file nào thuộc version nào của bảng.
2. *Kể tên các tầng metadata của Iceberg.* — Catalog (con trỏ tới metadata file hiện tại) → metadata.json (schema, partition spec, danh sách snapshot) → manifest list (1/snapshot, liệt kê manifest) → manifest file (liệt kê data file + stats) → data files.
3. *Snapshot là gì?* — Trạng thái đầy đủ, bất biến của bảng tại một thời điểm commit; trỏ tới một manifest list. Mọi write tạo snapshot mới, không sửa snapshot cũ → nền tảng của time travel và rollback.
4. *Bảng Hive-style có vấn đề gì mà Iceberg giải quyết?* — (a) phải LIST directory để biết bảng có file gì — chậm ở quy mô lớn; (b) không atomic — job chết giữa chừng để lại dữ liệu rác cho reader; (c) partition vật lý cứng, đổi spec phải rewrite; (d) schema track theo tên cột, rename nguy hiểm.

**Mid:**

5. *Iceberg đạt ACID trên S3 thế nào khi S3 không có transaction?* — Mọi write chỉ tạo file mới (data, manifest, metadata.json mới); commit là MỘT thao tác compare-and-swap con trỏ catalog từ metadata cũ sang mới. Swap thành công = commit; fail = bảng nguyên vẹn. Atomicity dồn hết vào một điểm swap duy nhất, nên storage bên dưới không cần transaction.
6. *File pruning của Iceberg hoạt động ra sao?* — Hai tầng ở planning time: manifest list chứa partition range của từng manifest → bỏ manifest ngoài range; manifest chứa min/max, null count per column của từng data file → bỏ file mà filter không thể match. Kết quả: chỉ file có khả năng chứa dữ liệu mới được giao cho executor đọc.
7. *Manifest list và manifest file khác nhau gì? Sao không gộp một tầng?* — Manifest list: 1 file/snapshot, liệt kê manifest + thống kê cấp manifest. Manifest: liệt kê data file + stats cấp file. Tách 2 tầng để commit mới **reuse** manifest cũ không đổi (chỉ ghi manifest cho file mới) → commit nhỏ trên bảng khổng lồ vẫn rẻ, và prune được theo tầng.
8. *Reader đang query thì writer commit — chuyện gì xảy ra?* — Không gì cả với reader: nó đã ghim vào snapshot tại thời điểm bắt đầu, đọc bộ file bất biến của snapshot đó đến hết query (snapshot isolation). Query sau mới thấy snapshot mới. Nguy cơ duy nhất: file của snapshot cũ bị expire *trong lúc* query rất dài đang chạy — nên retention phải dài hơn query dài nhất.

**Senior:**

9. *So sánh cơ chế metadata của Iceberg và Delta Lake, và hệ quả.* — Delta: transaction log tuần tự (`_delta_log/00001.json`...) ghi từng thay đổi, đọc bảng = replay log từ checkpoint gần nhất; commit atomic dựa vào tạo file log version kế tiếp (cần mutual exclusion trên tên file). Iceberg: mỗi commit là cây snapshot đầy đủ + swap con trỏ ở catalog. Hệ quả: Iceberg dựa vào catalog để CAS nên trung lập storage (S3 thuần vẫn an toàn nhiều writer qua catalog); partition/sort là metadata cấp bảng nên evolution không rewrite; Delta gắn chặt hệ Spark/Databricks hơn nhưng tooling hệ đó rất mạnh. Chọn theo hệ sinh thái engine của tổ chức.
10. *Bảng Iceberg nhận commit streaming mỗi 30 giây. Sau 3 tháng, planning query chậm dần dù dữ liệu không lớn. Chẩn đoán?* — ~260k snapshot: metadata.json phình (chứa toàn bộ danh sách snapshot), hàng trăm nghìn manifest nhỏ vì mỗi commit sinh manifest riêng → planning phải đọc quá nhiều Avro nhỏ; kèm small data files làm scan kém. Xử lý: expire_snapshots giữ retention ngắn, rewrite_manifests gộp manifest, rewrite_data_files compact file, cân nhắc tăng trigger interval. (Toàn bộ là nội dung lesson 32 — trả lời được câu này trước khi học là bạn đã hiểu bài hôm nay.)

---

## 14. Summary

### Mindmap

```
                       ICEBERG INTERNALS
                              │
      ┌───────────────┬──────┴─────────┬────────────────────┐
      ▼               ▼                ▼                    ▼
  TẠI SAO         METADATA 3 TẦNG   SNAPSHOT/ACID        PRUNING
      │               │                │                    │
  Hive table:     catalog (con trỏ) snapshot = version   manifest list:
  LIST chậm         → metadata.json   bất biến             partition range
  không ACID        → manifest list  commit = ghi mới    manifest:
  partition cứng    → manifest       + ATOMIC SWAP         min/max per col
  rename vỡ         → data files     con trỏ             → skip file ở
  → Netflix 2018  (đích danh file,  reader ghim          planning time
                   không LIST dir)   snapshot → isolation
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại sơ đồ catalog → metadata.json → manifest list → manifest → data files không nhìn tài liệu.
- [ ] Giải thích được commit = atomic swap con trỏ, và vì sao thế là đủ cho ACID.
- [ ] Đã mở `vN.metadata.json` thật và chỉ được: current-snapshot-id, snapshots, partition-specs, schemas.
- [ ] Nói được 2 tầng pruning (partition range ở manifest list, min/max ở manifest).
- [ ] Biết vì sao DELETE không giải phóng dung lượng ngay.
- [ ] Định vị được Iceberg vs Delta vs Hudi trong 3 câu.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 31 — Iceberg + Spark: DDL, MERGE, time travel.**

Hôm nay bạn đã hiểu bộ xương metadata. Bài sau ta *dùng* nó: CREATE TABLE với đủ lựa chọn catalog (hadoop cho lab, REST/Hive/Glue cho production), UPDATE/DELETE/MERGE row-level trên data lake — điều mà 5 năm trước là chuyện viễn tưởng, time travel về snapshot tuần trước bằng một câu SQL, rollback khi ghi nhầm, và branch/tag để làm write-audit-publish như Git cho dữ liệu. Mỗi tính năng đó chỉ là một cách chơi khác nhau trên cây snapshot bạn vừa học — nên bài hôm nay chắc, bài mai sẽ trơn tru.

> Gõ **"Continue"** khi sẵn sàng.
