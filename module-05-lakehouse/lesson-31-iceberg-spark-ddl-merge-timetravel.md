# Lesson 31 — Iceberg + Spark: DDL, MERGE, time travel

> Module 5 · Lakehouse & Iceberg · Tuần 16 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Cấu hình Spark catalog cho Iceberg: `spark.sql.catalog.*` — hadoop catalog cho lab, REST/Hive/Glue cho production.
- DDL: `CREATE TABLE ... USING iceberg`, table properties, format-version 2.
- Row-level operations: `INSERT / UPDATE / DELETE / MERGE INTO` — và 2 chiến lược thực thi **copy-on-write vs merge-on-read**.
- Time travel: `VERSION AS OF` / `TIMESTAMP AS OF`, rollback bằng `CALL rollback_to_snapshot`.
- Branch & tag → **WAP (write-audit-publish)**: ghi vào nhánh, kiểm tra chất lượng, rồi mới publish.
- Concurrent write: optimistic concurrency — 2 job cùng ghi thì ai thắng, ai retry, ai fail.

Sau bài này bạn phải làm được:

- Tự viết config catalog không nhìn tài liệu, giải thích từng dòng `spark.sql.catalog.*` làm gì.
- Viết `MERGE INTO` upsert dữ liệu Olist mới về bảng đích, và nói được câu lệnh đó tạo snapshot kiểu gì.
- Trả lời: "ghi nhầm dữ liệu vào bảng production lúc 14:00, giờ là 14:20 — cứu thế nào trong 2 phút?"

Kiến thức dùng trong thực tế: đây là bộ động tác hằng ngày của DE thời lakehouse. CDC từ Kafka đổ về? MERGE. Sửa dữ liệu sai theo yêu cầu? UPDATE/DELETE có ACID. Sự cố dữ liệu? time travel để điều tra, rollback để cứu. Repo `../kafka-flink` của bạn dùng đủ các món này.

---

## 2. Why

### Vấn đề: data lake cổ điển là "write-once, sửa là viết lại cả thế giới"

Trước table format, muốn sửa 1 dòng trong bảng Parquet trên S3, quy trình là: đọc toàn bộ partition chứa dòng đó → sửa trong DataFrame → ghi đè cả partition → cầu nguyện không ai đang đọc lúc bạn ghi đè. Muốn upsert CDC hàng ngày? Tự viết logic join + overwrite dễ sai vô hạn. Muốn xem "bảng này hôm qua trông thế nào"? Không thể — bản cũ đã bị ghi đè mất.

Iceberg biến các thao tác đó thành một câu SQL, vì mọi thứ quy về cây snapshot của lesson 30:

- `UPDATE`/`DELETE`/`MERGE` = tạo snapshot mới tham chiếu bộ file mới (hoặc file + delete file).
- Time travel = đọc theo snapshot cũ vẫn đang nằm trong metadata.
- Rollback = đổi con trỏ current về snapshot cũ.
- Branch = con trỏ có tên, di chuyển độc lập với `main` — đúng nghĩa Git cho dữ liệu.

### Nếu không có các thao tác này thì sao?

Bạn sẽ: viết pipeline "full reload" tốn gấp trăm lần cần thiết chỉ để sửa vài dòng; hoặc dựng thêm database OLTP bên cạnh lake chỉ để có UPDATE; hoặc khi ghi nhầm dữ liệu thì... restore từ backup mất nửa ngày trong khi dashboard toàn công ty đang sai.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| UPDATE/DELETE/MERGE chuẩn SQL, ACID, trên object storage | Row-level write đắt hơn append thuần (đọc-ghi lại file hoặc ghi delete file) |
| Time travel + rollback = bảo hiểm sự cố dữ liệu | Giữ snapshot = giữ file cũ = tốn storage (lesson 32) |
| Branch/tag = test trên dữ liệu thật không rủi ro | Thêm khái niệm vận hành, quên publish là downstream không thấy dữ liệu |
| Optimistic concurrency: nhiều writer không cần lock server | Xung đột thật sự thì có job phải retry/fail — phải thiết kế cho điều đó |

> Bài học Senior: Iceberg cho bạn động từ của database trên chi phí của data lake — nhưng nó vẫn KHÔNG phải OLTP. MERGE mỗi giờ một lần: đẹp. UPDATE từng dòng theo request người dùng, trăm lần mỗi phút: sai công cụ, quay lại Postgres.

---

## 3. Theory

### 3.1. Catalog — cắm Iceberg vào Spark ở đâu

Spark nhìn thế giới bảng qua **catalog**. Config mẫu (đọc kỹ từng dòng, đây là boilerplate bạn sẽ gõ cả trăm lần):

```python
# 1) Nạp code Iceberg + cú pháp mở rộng (MERGE, CALL, ALTER ... ADD PARTITION FIELD)
.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

# 2) Khai một catalog tên "lakehouse" do Iceberg quản
.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")

# 3) Catalog này lưu con trỏ ở đâu?  → LAB: hadoop (ngay trên filesystem)
.config("spark.sql.catalog.lakehouse.type", "hadoop")
.config("spark.sql.catalog.lakehouse.warehouse", "/workspace/warehouse")
```

Từ đó mọi bảng có tên 3 phần: `lakehouse.olist.orders` (catalog.namespace.table).

Production thay dòng 3 bằng một trong các lựa chọn:

| Catalog | Config chính | Khi dùng |
|---|---|---|
| **hadoop** | `type=hadoop`, `warehouse=path` | Lab/local. KHÔNG dùng multi-writer trên S3 (rename không atomic). |
| **hive** | `type=hive`, `uri=thrift://metastore:9083` | Đã có Hive Metastore (hệ Hadoop cũ, hoặc stack như repo kafka-flink). |
| **REST** | `type=rest`, `uri=http://catalog:8181` | Chuẩn mới, engine nào cũng nói chuyện được (Polaris, Lakekeeper, Nessie...). Xu hướng chính. |
| **Glue** | `catalog-impl=...aws.glue.GlueCatalog` | Trên AWS. |
| **JDBC** | `catalog-impl=...jdbc.JdbcCatalog`, `uri=jdbc:postgresql://...` | Con trỏ nằm trong Postgres — nhẹ, đủ ACID. |

Điểm chung: catalog chỉ giữ **con trỏ + hỗ trợ compare-and-swap khi commit**. Dữ liệu và metadata file vẫn nằm trên storage.

### 3.2. DDL và table properties

```sql
CREATE TABLE lakehouse.olist.orders (
    order_id     STRING,
    customer_id  STRING,
    order_status STRING,
    ts           TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts))                  -- hidden partitioning, lesson 33
TBLPROPERTIES (
    'format-version' = '2',                -- BẮT BUỘC nhớ: v2 mới có row-level delete file
    'write.parquet.compression-codec' = 'zstd'
);
```

`format-version`: v1 chỉ biết thêm/thay file nguyên khối; **v2** thêm khái niệm **delete file** → mở khóa merge-on-read. Iceberg 1.4 tạo bảng mặc định vẫn là v1, nên tự tay đặt v2 thành phản xạ.

### 3.3. Row-level writes: copy-on-write vs merge-on-read

Data file bất biến — vậy `DELETE FROM t WHERE id = 7` (1 dòng trong file 1 triệu dòng) làm gì? Hai chiến lược:

```
COPY-ON-WRITE (COW)                    MERGE-ON-READ (MOR)
─────────────────────                  ─────────────────────
file A (1M dòng, có id=7)              file A giữ NGUYÊN
   │ đọc cả file, bỏ id=7                 │ chỉ ghi thêm delete file nhỏ:
   ▼                                      ▼
file A' (999,999 dòng)                 delete-001: "file A, dòng thứ 4213 đã xóa"
snapshot mới: A' thay A                snapshot mới: A + delete-001

Ghi: ĐẮT (rewrite cả file)             Ghi: RẺ (chỉ ghi delta)
Đọc: RẺ (đọc file như thường)          Đọc: ĐẮT hơn (đọc A rồi trừ delete file)
```

Chọn per-table, per-operation qua property:

```sql
ALTER TABLE lakehouse.olist.orders SET TBLPROPERTIES (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'copy-on-write'    -- mặc định cả 3 là copy-on-write
);
```

Quy tắc chọn: bảng **đọc nhiều, sửa thưa** (bảng gold cho BI) → COW. Bảng **nhận CDC/upsert dày đặc** (bronze/silver hứng Kafka) → MOR, và định kỳ compaction dọn delete file (lesson 32). MOR v2 có 2 loại delete file: *position delete* (file X, dòng thứ N) và *equality delete* (mọi dòng có id=7 — Flink CDC hay dùng).

### 3.4. Time travel, rollback, branch & tag — chơi trên cây snapshot

```
snapshot S1 ──► S2 ──► S3 ──► S4  ◄─ main (current)
               │
               └──► S2' ──► S3'   ◄─ branch "audit"      tag "v2018-close" ──► S2
```

- **Time travel** (đọc, không đổi gì): `SELECT ... VERSION AS OF <snapshot_id>` hoặc `TIMESTAMP AS OF '...'` — Iceberg tra `snapshot-log` tìm snapshot current tại thời điểm đó.
- **Rollback** (đổi con trỏ current về quá khứ): `CALL lakehouse.system.rollback_to_snapshot(...)` — tạo "bước nhảy lùi" trong history, KHÔNG xóa snapshot nào.
- **Tag**: con trỏ có tên, bất động — đóng dấu "số liệu chốt quý" để audit về sau.
- **Branch**: con trỏ có tên, di chuyển được khi bạn ghi vào nó — nền của WAP:

```
WAP — WRITE, AUDIT, PUBLISH
① WRITE:   ghi batch mới vào branch "audit"     (main chưa hề thay đổi,
                                                 BI vẫn đọc dữ liệu cũ sạch sẽ)
② AUDIT:   query branch audit, chạy các check:
           row count hợp lý? null? duplicate key? doanh thu âm?
③ PUBLISH: pass → fast_forward main lên audit   (dữ liệu "ra mắt" atomic)
           fail → drop branch, main không dính một giọt dữ liệu bẩn
```

### 3.5. Concurrent writes — optimistic concurrency

Hai job cùng commit vào một bảng:

```
Job A: đọc metadata v5 ── ghi files ── commit: swap v5→v6  ✅ thắng
Job B: đọc metadata v5 ── ghi files ── commit: swap v5→v6  ❌ CAS fail (v6 đã tồn tại)
                                          │
                                          ▼
                            đọc lại metadata v6 mới nhất
                            KIỂM TRA XUNG ĐỘT với thay đổi của A:
                            ├─ không giẫm nhau (VD: 2 append, hoặc đụng
                            │   partition khác nhau) → viết lại metadata, retry commit ✅
                            └─ giẫm nhau thật (cùng sửa/xóa một file,
                                validation fail) → ValidationException ❌ job fail
```

"Optimistic" = không lock trước, cứ làm rồi kiểm tra lúc commit. Append + append: gần như luôn tự hòa giải được. MERGE/DELETE chồng lấn cùng vùng dữ liệu: một job sẽ fail — pipeline của bạn phải sẵn sàng retry ở tầng orchestrator. Số lần tự retry chỉnh bằng property `commit.retry.num-retries` (mặc định 4).

---

## 4. Internal

Mổ xẻ một câu `MERGE INTO` (chế độ copy-on-write) từ lúc gõ Enter:

```
MERGE INTO orders t USING updates s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *  WHEN NOT MATCHED THEN INSERT *

① Analyzer (nhờ IcebergSparkSessionExtensions) hiểu cú pháp MERGE,
   resolve bảng đích qua catalog "lakehouse"
        │
② JOB 1 — tìm file bị ảnh hưởng: join `updates` với bảng đích
   Iceberg prune bằng manifest stats: chỉ file CÓ THỂ chứa
   order_id trùng mới bị đưa vào diện "nghi vấn"
        │
③ JOB 2 — rewrite: đọc các file nghi vấn, join lại với updates,
   dòng match → bản mới, dòng không match → chép nguyên,
   dòng INSERT → thêm; ghi ra bộ data file MỚI
        │
④ Commit: manifest mới (file mới ADDED, file cũ bị thay = DELETED),
   manifest list mới, v(N+1).metadata.json, atomic swap
   → snapshot mới operation = "overwrite"
        │
⑤ Nếu swap fail vì writer khác vừa commit → quay lại kiểm tra xung
   đột như sơ đồ 3.5; các file đã ghi ở ③ vẫn dùng lại được cho retry
```

Ở chế độ **merge-on-read**, bước ③ đổi khác: không rewrite file cũ, chỉ ghi (a) data file cho dòng mới/bản mới và (b) position delete file đánh dấu dòng cũ đã chết. Còn phía **đọc** bảng MOR: scan task đọc data file, đồng thời nạp delete file áp lên — dòng bị đánh dấu sẽ bị lọc trước khi trả kết quả. Delete file càng chồng chất, đọc càng chậm — đó là món nợ mà compaction (lesson 32) phải trả.

Time travel về mặt nội bộ rẻ không ngờ: `VERSION AS OF S2` chỉ là "đi theo manifest list của S2 thay vì current" — không copy, không restore, chỉ là chọn con trỏ khác để duyệt cây.

---

## 5. API

### `CREATE TABLE ... USING iceberg` + `writeTo` API

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.olist.orders (...) USING iceberg "
          "TBLPROPERTIES ('format-version'='2')")
df.writeTo("lakehouse.olist.orders").append()            # thêm
df.writeTo("lakehouse.olist.orders").overwritePartitions()  # thay partition động
df.writeTo("lakehouse.olist.orders").createOrReplace()   # tạo/thay cả bảng
```
- **Pitfall**: `overwrite()` không tham số của DataFrameWriterV2 cần điều kiện; `overwritePartitions()` chỉ thay partition có mặt trong df — hiểu nhầm 2 cái này là mất dữ liệu.

### `MERGE INTO` — con dao chính của CDC/upsert

```sql
MERGE INTO lakehouse.olist.orders t
USING staging_updates s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
- **Khi dùng**: đồng bộ thay đổi từ nguồn (CDC, file mới) về bảng đích, một câu duy nhất, atomic.
- **Pitfall**: nguồn `s` có 2 dòng cùng `order_id` khớp 1 dòng đích → lỗi cardinality (`MERGE ... matches multiple rows`). Luôn dedupe nguồn trước (giữ bản ghi mới nhất theo timestamp).

### Time travel — SQL và DataFrame

```sql
SELECT count(*) FROM lakehouse.olist.orders VERSION AS OF 6412345678901234567;
SELECT count(*) FROM lakehouse.olist.orders TIMESTAMP AS OF '2026-07-01 09:00:00';
SELECT count(*) FROM lakehouse.olist.orders VERSION AS OF 'audit';  -- branch/tag cũng là "version"
```
```python
spark.read.option("snapshot-id", 6412345678901234567).table("lakehouse.olist.orders")
spark.read.option("as-of-timestamp", "1767851022000").table(...)   # LƯU Ý: epoch MILLIS
```
- **Pitfall**: time travel chỉ về được snapshot **chưa bị expire**. Retention 7 ngày thì đừng hứa với sếp "xem lại số liệu 3 tháng trước".

### `CALL` procedures — rollback và quản trị

```sql
CALL lakehouse.system.rollback_to_snapshot('olist.orders', 6412345678901234567);
CALL lakehouse.system.set_current_snapshot('olist.orders', 8321459776123456789); -- nhảy tự do 2 chiều
```

### Branch & tag

```sql
ALTER TABLE lakehouse.olist.orders CREATE BRANCH audit;
ALTER TABLE lakehouse.olist.orders CREATE TAG `q4-2018-close`;
INSERT INTO lakehouse.olist.orders.branch_audit SELECT ...;   -- ghi vào branch, main bất động
CALL lakehouse.system.fast_forward('olist.orders', 'main', 'audit');  -- PUBLISH
ALTER TABLE lakehouse.olist.orders DROP BRANCH audit;
```
- **Pitfall**: branch giữ file của nó sống khỏi expire — branch rác quên xóa = storage không bao giờ được giải phóng.

---

## 6. Demo nhỏ

```
Input:  bảng 3 dòng trạng thái đơn hàng
   ↓    MERGE batch CDC: sửa 1, xóa 1, thêm 1  (commit → snapshot mới)
   ↓    time travel về trước MERGE — dữ liệu cũ vẫn nguyên
Output: 2 phiên bản của cùng một bảng, sống song song
```

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("demo31").master("local[2]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/tmp/demo_warehouse")
    .getOrCreate())

spark.sql("DROP TABLE IF EXISTS lakehouse.demo.status")
spark.sql("""CREATE TABLE lakehouse.demo.status (order_id STRING, status STRING)
             USING iceberg TBLPROPERTIES ('format-version'='2')""")
spark.sql("INSERT INTO lakehouse.demo.status VALUES ('o1','created'),('o2','created'),('o3','shipped')")

spark.createDataFrame(
    [("o1", "shipped", "U"), ("o3", None, "D"), ("o4", "created", "I")],
    ["order_id", "status", "op"]).createOrReplaceTempView("cdc")

spark.sql("""MERGE INTO lakehouse.demo.status t USING cdc s ON t.order_id = s.order_id
             WHEN MATCHED AND s.op = 'D' THEN DELETE
             WHEN MATCHED THEN UPDATE SET t.status = s.status
             WHEN NOT MATCHED THEN INSERT (order_id, status) VALUES (s.order_id, s.status)""")

spark.sql("SELECT * FROM lakehouse.demo.status ORDER BY order_id").show()
# o1 shipped | o2 created | o4 created        (o3 đã bay màu)

old = spark.sql("SELECT snapshot_id FROM lakehouse.demo.status.snapshots ORDER BY committed_at").first()[0]
spark.sql(f"SELECT * FROM lakehouse.demo.status VERSION AS OF {old} ORDER BY order_id").show()
# o1 created | o2 created | o3 shipped        (quá khứ còn nguyên!)
spark.stop()
```

Tự hỏi: MERGE này tạo snapshot operation gì? (Mở `.snapshots` — "overwrite"). Bảng để mode mặc định COW — nếu đổi `write.merge.mode=merge-on-read` thì cột nào trong `.files` sẽ xuất hiện file loại mới? (content=1 — position deletes.)

---

## 7. Production Example

Pipeline CDC chuẩn mà repo `../kafka-flink` của bạn đang mô phỏng — giờ nhìn bằng con mắt Iceberg:

```
PostgreSQL (orders thay đổi liên tục)
   ↓ Debezium: mỗi thay đổi = 1 event {before, after, op: c/u/d}
Kafka topic olist.orders
   ↓ Spark Structured Streaming, mỗi micro-batch:
   │    ① dedupe: giữ event MỚI NHẤT mỗi order_id (window theo ts)
   │    ② MERGE INTO silver.orders  (op=d → DELETE, còn lại UPSERT)
Iceberg silver.orders (format-version 2, merge-on-read)
   ↓ batch hằng đêm: WAP
   │    ① ghi gold vào branch "staging"
   │    ② audit: count khớp silver? revenue không âm? key unique?
   │    ③ pass → fast_forward main;   fail → alert, BI vẫn thấy số cũ ĐÚNG
Iceberg gold.revenue_daily  ← Trino/Superset đọc
```

Vì sao từng lựa chọn:

1. **MERGE thay vì append**: bảng silver phải phản chiếu *trạng thái hiện tại* của Postgres, không phải nhật ký event — nên cần upsert + delete row-level.
2. **MOR cho silver**: micro-batch mỗi phút mà COW thì rewrite file liên tục, write amplification giết throughput. MOR ghi delta nhỏ, compaction gom lại theo giờ.
3. **WAP cho gold**: gold nuôi dashboard cho CEO — thà trễ 30 phút chứ không được sai. Branch cho phép kiểm tra trên dữ liệu thật, publish atomic.
4. **Rollback là nút cứu sinh**: backfill nhầm logic vào gold? `rollback_to_snapshot` đưa bảng về trước sự cố trong vài giây, sửa code, chạy lại — thay vì restore backup nửa ngày.

---

## 8. Hands-on Lab

**Mục tiêu**: đủ vòng đời CREATE → MERGE → time travel → rollback → WAP trên dữ liệu Olist thật.

### Bước 1 — `labs/lab31/crud_timetravel.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab31-iceberg-crud")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/workspace/warehouse")
    .getOrCreate())

orders = (spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                         header=True, inferSchema=True)
          .select("order_id", "customer_id", "order_status",
                  F.to_timestamp("order_purchase_timestamp").alias("ts")))

# ① CREATE + INSERT: nạp trước 2018-01, giả lập "bảng đang chạy"
spark.sql("DROP TABLE IF EXISTS lakehouse.olist.orders_l31")
spark.sql("""CREATE TABLE lakehouse.olist.orders_l31
             (order_id STRING, customer_id STRING, order_status STRING, ts TIMESTAMP)
             USING iceberg TBLPROPERTIES ('format-version'='2')""")
orders.filter("ts < '2018-01-01'").writeTo("lakehouse.olist.orders_l31").append()

# ② MERGE: batch "CDC" = đơn 2018 (mới) + đơn cũ đổi status (giả lập update)
updates = (orders.filter("ts >= '2018-01-01'")
           .unionByName(orders.filter("ts < '2018-01-01'").limit(500)
                        .withColumn("order_status", F.lit("archived"))))
updates.createOrReplaceTempView("updates")
spark.sql("""MERGE INTO lakehouse.olist.orders_l31 t USING updates s
             ON t.order_id = s.order_id
             WHEN MATCHED THEN UPDATE SET *
             WHEN NOT MATCHED THEN INSERT *""")

# ③ Sự cố! Ai đó xóa nhầm đơn delivered
spark.sql("DELETE FROM lakehouse.olist.orders_l31 WHERE order_status = 'delivered'")
print("Sau tai nan:", spark.table("lakehouse.olist.orders_l31").count())

# ④ Điều tra bằng time travel + ⑤ cứu bằng rollback
snaps = spark.sql("""SELECT snapshot_id, operation, committed_at
                     FROM lakehouse.olist.orders_l31.snapshots ORDER BY committed_at""")
snaps.show(truncate=False)
good = snaps.collect()[-2]["snapshot_id"]     # snapshot ngay trước tai nạn
print("Truoc tai nan (time travel):",
      spark.sql(f"SELECT count(*) FROM lakehouse.olist.orders_l31 VERSION AS OF {good}").first()[0])
spark.sql(f"CALL lakehouse.system.rollback_to_snapshot('olist.orders_l31', {good})")
print("Sau rollback:", spark.table("lakehouse.olist.orders_l31").count())
spark.stop()
```

### Bước 2 — `labs/lab31/wap_pattern.py` (tự viết theo khung)

```python
# ① ALTER TABLE ... CREATE BRANCH audit
# ② INSERT INTO lakehouse.olist.orders_l31.branch_audit  (batch mới, cố tình chèn 10 dòng order_id NULL)
# ③ AUDIT: đếm NULL key trên VERSION AS OF 'audit'  → fail
# ④ nhánh fail: DROP BRANCH; chứng minh main không dính dòng NULL nào
# ⑤ làm lại với batch sạch → audit pass → CALL fast_forward('olist.orders_l31','main','audit')
```

### Bước 3 — chạy và quan sát

```bash
make run-local F=labs/lab31/crud_timetravel.py
make run-local F=labs/lab31/wap_pattern.py
```

Ghi vào `labs/lab31/NOTES.md`: (1) MERGE tạo snapshot operation gì, thêm/xóa mấy file (soi `summary` trong `.snapshots`)? (2) Sau rollback, `.history` có mấy dòng và snapshot "tai nạn" còn tồn tại không? (3) Trong `.refs` (SELECT * FROM t.refs) thấy gì khi branch audit đang sống?

---

## 9. Assignment

**Easy** — Trên bảng lab: viết 3 query time travel — theo `VERSION AS OF`, theo `TIMESTAMP AS OF`, và bằng DataFrame API `option("snapshot-id", ...)`. Chứng minh cả 3 trả cùng kết quả với cùng snapshot.

**Medium** — Xây flow WAP hoàn chỉnh cho bảng `order_payments`: nạp `olist_order_payments_dataset.csv` vào branch `staging`, viết 3 audit check (payment_value không âm; `order_id` tồn tại trong bảng orders; không duplicate `order_id + payment_sequential`), chỉ fast_forward khi cả 3 pass. Chạy 2 lần: một lần với dữ liệu bẩn tự chế (phải bị chặn), một lần dữ liệu sạch (phải publish).

**Hard** — Tái hiện write conflict: viết 2 script, mỗi cái mở SparkSession riêng, cùng `MERGE`/`UPDATE` đúng một nhóm `order_id` vào một bảng, chạy đồng thời (2 terminal, hoặc `make run` + `make run-local` song song). Quan sát: job nào commit trước? Job sau nhận exception gì hay tự retry thành công? Lặp lại với 2 job **append** thuần — kết quả khác gì? Giải thích bằng cơ chế optimistic concurrency (mục 3.5).

**Production Challenge** — Đọc job Spark ghi Iceberg trong `../kafka-flink` (tìm chỗ `MERGE INTO` hoặc `writeTo`). Review 10–15 dòng: bảng đang format-version mấy, mode COW hay MOR — có hợp với tần suất ghi không? Có dedupe trước MERGE không? Nếu MERGE fail giữa chừng, orchestrator có retry không, và retry có an toàn (idempotent) không? Đề xuất 1 cải tiến cụ thể.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Chi phí | Ghi chú thực chiến |
|---|---|---|
| `INSERT`/append | Rẻ nhất | Ghi file mới + commit. Chỉ lo small files nếu commit quá dày. |
| `MERGE` copy-on-write | Đắt theo số **file bị đụng** | Điều kiện ON lọc theo cột có trong partition/sort → prune tốt → rewrite ít file. ON theo cột random → rewrite nửa bảng. |
| `MERGE`/`DELETE` merge-on-read | Ghi rẻ, **đọc trả nợ dần** | Mỗi scan phải áp delete files. >20–30% dòng bị delete-file hóa mà chưa compact là query thấy chậm rõ. |
| Time travel | ~0 | Chỉ chọn con trỏ khác. Đọc snapshot cũ tốc độ như đọc current. |
| Rollback | ~0 | Đổi con trỏ, không đụng data file. Nút cứu sinh miễn phí. |
| MERGE với nguồn chưa dedupe | Fail hoặc kết quả sai | Cardinality check của MERGE nghiêm — dedupe nguồn luôn luôn. |

Câu tự vấn trước mỗi MERGE production: *"điều kiện ON của tôi có prune được file không, và nguồn đã dedupe chưa?"*

---

## 11. Spark UI

Chạy lab xong mở UI (:4040) đối chiếu:

**Tab SQL / DataFrame**: một câu `MERGE INTO` COW hiện thành plan có nhánh join giữa nguồn và `BatchScan` bảng đích, rồi node `ReplaceData`/`WriteDelta`. Đếm số job: bạn sẽ thấy MERGE không phải "một job" — có job tìm file ảnh hưởng, job rewrite, bước commit. Node `BatchScan` cho biết bao nhiêu file bị kéo vào rewrite — con số này chính là chi phí thật của MERGE.

**Tab Jobs**: `DELETE FROM ... WHERE order_status='delivered'` (COW) sinh job đọc + ghi lại các file chứa dòng delivered. Nếu bảng đổi sang MOR, cùng câu lệnh job nhẹ hơn hẳn — thấy được bằng mắt qua Duration.

**Đối chiếu chéo với metadata tables**: `summary` của snapshot trong `.snapshots` (added-data-files, deleted-data-files, added-delete-files) phải khớp với những gì bạn thấy trong UI. Thói quen Senior: nghi ngờ MERGE chậm → xem `BatchScan` đọc mấy file trong UI, rồi xem snapshot summary xem rewrite mấy file.

---

## 12. Common Mistakes

1. **Quên `spark.sql.extensions`** → `MERGE INTO` báo lỗi cú pháp/`UnsupportedOperationException`, `CALL` không tồn tại. Triệu chứng kinh điển, mất nửa buổi debug nếu không biết trước.
2. **MERGE với nguồn có duplicate key** → `Cannot perform MERGE ... multiple rows match`. Dedupe nguồn (row_number theo key, lấy bản mới nhất) là bước bắt buộc của mọi pipeline CDC.
3. **Để mặc định copy-on-write cho bảng streaming upsert dày** → write amplification: sửa 100 dòng mà rewrite hàng GB. Ngược lại để MOR cho bảng BI đọc nặng mà không compaction → đọc chậm dần. Mode phải chọn theo read/write pattern.
4. **Rollback xong lại chạy nguyên job lỗi** → tai nạn lặp lại. Rollback chỉ mua thời gian; phải sửa nguyên nhân gốc trước khi resume pipeline.
5. **Tưởng `TIMESTAMP AS OF` đọc được mọi quá khứ** → chỉ về được snapshot chưa expire. Cam kết audit dài hạn phải dùng **tag** (tag giữ snapshot khỏi expire) chứ không dựa retention.
6. **Ghi vào branch rồi quên publish** — pipeline "chạy xanh" mà dashboard không có số mới. WAP phải là một flow trọn vẹn: write → audit → publish/drop, có alert khi audit fail.
7. **Coi Iceberg như OLTP** — update lẻ tẻ theo request, trăm commit/phút. Mỗi commit là một vòng metadata + swap; gom thành batch/micro-batch là cách dùng đúng.

---

## 13. Interview

**Junior:**

1. *Muốn Spark làm việc với Iceberg cần config gì?* — Jar runtime (`iceberg-spark-runtime` khớp version Spark/Scala), `spark.sql.extensions` để có cú pháp MERGE/CALL, và khai catalog: `spark.sql.catalog.<tên> = SparkCatalog` + `type` (hadoop/hive/rest/glue) + `warehouse`/`uri`. Bảng gọi theo tên 3 phần catalog.db.table.
2. *Time travel trong Iceberg là gì, dùng khi nào?* — Query bảng theo snapshot quá khứ bằng `VERSION AS OF <id>` hoặc `TIMESTAMP AS OF <ts>`. Dùng để điều tra sự cố dữ liệu ("hôm qua số này là bao nhiêu?"), so sánh trước/sau một job, reproduce kết quả cho audit/ML.
3. *MERGE INTO làm được gì mà INSERT không làm được?* — Một câu lệnh atomic vừa UPDATE dòng đã tồn tại, vừa INSERT dòng mới, vừa DELETE theo điều kiện — chuẩn cho upsert/CDC. INSERT chỉ thêm, chạy lại là duplicate.
4. *Rollback khác time travel chỗ nào?* — Time travel chỉ ĐỌC snapshot cũ, bảng không đổi. Rollback ĐỔI con trỏ current về snapshot cũ — mọi người từ giờ thấy bảng như quá khứ. Cả hai đều không xóa dữ liệu.

**Mid:**

5. *Copy-on-write vs merge-on-read — giải thích và cách chọn.* — COW: write rewrite toàn bộ file chứa dòng bị đổi → ghi đắt, đọc sạch. MOR (cần format v2): ghi delete file đánh dấu dòng chết + file dữ liệu mới → ghi rẻ, đọc phải áp delete → chậm dần nếu không compact. Chọn: bảng đọc nhiều sửa thưa → COW; bảng hứng CDC/upsert dày → MOR + compaction định kỳ. Đặt riêng cho delete/update/merge qua `write.*.mode`.
6. *Hadoop catalog vì sao không nên dùng production trên S3?* — Commit của hadoop catalog dựa vào tạo/rename file version một cách atomic; S3 không có rename atomic và tính nhất quán cần thiết → 2 writer đồng thời có thể cùng cho rằng mình thắng → mất commit. Production dùng catalog có compare-and-swap thật: Hive Metastore, Glue, REST, JDBC.
7. *WAP pattern là gì, giải quyết vấn đề nào?* — Write-Audit-Publish: ghi batch vào branch, chạy data quality check trên branch, pass mới fast-forward main (publish atomic), fail thì drop branch. Giải quyết: downstream không bao giờ nhìn thấy dữ liệu chưa kiểm định, mà audit vẫn chạy trên dữ liệu thật, và publish/không-publish đều atomic.
8. *2 job cùng commit một bảng Iceberg — cơ chế gì xảy ra?* — Optimistic concurrency: cả hai ghi file dựa trên metadata đã đọc; commit là CAS con trỏ. Job thua CAS sẽ đọc metadata mới, kiểm tra thay đổi của mình có xung đột với commit vừa thắng không: không (2 append, khác vùng dữ liệu) → rebase metadata rồi retry; có (giẫm cùng file/dòng) → ValidationException, job fail và tầng orchestrator quyết định retry.

**Senior:**

9. *Thiết kế pipeline CDC Kafka → Iceberg đảm bảo đúng đắn: những điểm chết nằm ở đâu?* — (a) Ordering & dedupe: event cùng key phải áp theo thứ tự — dedupe mỗi micro-batch giữ bản mới nhất theo LSN/ts trước khi MERGE, tránh lỗi cardinality lẫn ghi ngược trạng thái; (b) delete: op=d phải map thành WHEN MATCHED DELETE, đừng biến thành update null; (c) chọn MOR + compaction theo giờ vì tần suất ghi cao; (d) exactly-once: MERGE theo key là idempotent với cùng batch, kết hợp checkpoint của Structured Streaming; (e) concurrent: chỉ một writer cho một bảng silver, hoặc chấp nhận retry storm; (f) schema evolution từ nguồn phải được xử lý có chủ đích, không auto-drift.
10. *Rollback bảng nguồn nhưng các bảng downstream đã đọc dữ liệu bẩn và ghi tiếp — xử lý thế nào? Thiết kế gì để lần sau đỡ đau?* — Xử lý: xác định "vùng nhiễm" bằng snapshot timeline (`.history` các bảng, đối chiếu thời gian), rollback/re-run downstream theo thứ tự phụ thuộc — về bản chất là cascading rollback thủ công. Thiết kế phòng ngừa: WAP ở mọi tầng công bố ra ngoài (dữ liệu bẩn bị chặn ở audit, không lan); pipeline tham chiếu snapshot-id tường minh (job downstream đọc "snapshot X của bảng nguồn" ghi vào lineage/log thay vì "latest") để trace và replay chính xác; tag các mốc chốt sổ; orchestrator lưu mapping run → snapshot để tự động hóa việc xác định vùng nhiễm.

---

## 14. Summary

### Mindmap

```
                    ICEBERG + SPARK: DDL/MERGE/TIME TRAVEL
                                   │
     ┌──────────────┬──────────────┼───────────────────┬──────────────────┐
     ▼              ▼              ▼                   ▼                  ▼
  CATALOG         DDL          ROW-LEVEL WRITE     TIME TRAVEL        CONCURRENCY
     │              │              │                   │                  │
  spark.sql.     CREATE...      MERGE/UPDATE/       VERSION AS OF      optimistic:
  catalog.*      USING iceberg  DELETE              TIMESTAMP AS OF    ghi trước,
  hadoop=lab     format-        COW: rewrite file   rollback_to_       CAS lúc commit
  hive/rest/     version=2      (đọc rẻ)            snapshot           thua → check
  glue=prod      PARTITIONED BY MOR: delete file    branch/tag         conflict →
  tên 3 phần     (transform)    (ghi rẻ, nợ đọc)    → WAP: write,      retry hoặc
  cat.db.table                  dedupe nguồn!       audit, publish     ValidationExc
```

### Checklist trước khi gõ "Continue"

- [ ] Gõ lại được block config catalog (packages, extensions, catalog type/warehouse) không nhìn tài liệu.
- [ ] Giải thích được COW vs MOR và chọn đúng cho 2 tình huống: bảng BI, bảng CDC.
- [ ] Viết được MERGE INTO xử cả 3 nhánh matched-update / matched-delete / not-matched-insert.
- [ ] Làm được chuỗi cứu hộ: phát hiện qua `.snapshots` → time travel xác nhận → rollback.
- [ ] Chạy được WAP: branch → audit → fast_forward/drop.
- [ ] Giải thích được optimistic concurrency và khi nào job phải fail thật.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 32 — Table maintenance: compaction, snapshot expiration.**

Hôm nay bạn đã thấy mặt trái của phép màu: mỗi MERGE, mỗi micro-batch, mỗi lần "không sửa gì tại chỗ, chỉ thêm mới" đều để lại di sản — snapshot chồng chất, file nhỏ li ti, delete file chờ được áp, metadata phình. Một bảng streaming bỏ bê 3 tháng có thể chậm gấp 10 lần dù dữ liệu chẳng thêm bao nhiêu. Lesson 32 dạy nghề "bảo trì": compaction gộp file, expire snapshot giải phóng storage, dọn orphan file, rewrite manifest — kèm lịch chạy chuẩn production và cách tự động hóa bằng Airflow. Đây là phần tách biệt người *dùng* Iceberg với người *vận hành* được Iceberg.

> Gõ **"Continue"** khi sẵn sàng.
