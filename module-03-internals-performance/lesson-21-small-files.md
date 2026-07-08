# Lesson 21 — Small Files Problem & File Layout: căn bệnh mãn tính của data lake

> Module 3 · Internals & Performance Tuning · Tuần 11 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao **small files giết performance**: listing metadata, mỗi file ≥ 1 task, scheduler overhead, memory driver.
- Small files **từ đâu ra**: streaming ghi liên tục, partition folder quá mịn, shuffle partitions cao lúc ghi.
- Đo đạc tận tay: 1000 file nhỏ vs 10 file lớn — cùng dữ liệu, tốc độ khác một trời một vực.
- Bộ vũ khí writer: `repartition`/`coalesce` trước khi ghi, `maxRecordsPerFile`, **compaction** định kỳ (Iceberg `rewrite_data_files`).
- Nguyên tắc thiết kế **file layout** hoàn chỉnh: partition folder + cỡ file 128 MB–1 GB + sort trong partition.

Sau bài này bạn phải làm được:

- Nhìn một bảng chậm và trả lời trong 2 phút: "bao nhiêu file, cỡ trung bình bao nhiêu, có bệnh không" — bằng lệnh đếm file, không đoán.
- Viết đúng đoạn `write` cuối pipeline: partition gì, mấy file, cắt file thế nào — có lý do cho từng lựa chọn.
- Thiết kế lịch compaction cho bảng streaming mà không gây downtime.

Kiến thức dùng trong thực tế: small files là bệnh **tích luỹ âm thầm** — hôm nay ghi thêm 500 file nhỏ chẳng ai thấy gì, 6 tháng sau cả data lake lết. Đây cũng là 1 trong các "quả bom" được cài trong Project 3 tuần này.

---

## 2. Why

### Câu chuyện: bảng events 50 GB mà count mất 25 phút

Team A ghi events từ streaming job, trigger mỗi 1 phút, mỗi lần ghi ~20 partition folder × vài file. Sau 6 tháng:

```
s3://lake/events/
  ├── date=2025-01-01/hour=00/  part-0001.parquet (85 KB)
  │                             part-0002.parquet (112 KB)
  │                             ... 240 file nữa ...
  ├── ... × 180 ngày × 24 giờ ...
  └── TỔNG: 1,900,000 file, trung bình 27 KB/file, tổng 50 GB
```

`SELECT count(*)` mất 25 phút. Cùng 50 GB đó nếu nằm trong ~400 file 128 MB: **dưới 1 phút**. Dữ liệu y hệt nhau. Khác biệt duy nhất: **cách xếp file**.

### 4 lý do small files giết performance

1. **Listing metadata** — trước khi đọc byte dữ liệu ĐẦU TIÊN, driver phải liệt kê toàn bộ file (tên, size, vị trí). Trên S3/object storage, list là API call phân trang ~1000 entry/call, có latency mỗi call. 1.9 triệu file = hàng nghìn call TUẦN TỰ theo từng folder. Bạn trả 10 phút "im lặng" trước khi job làm việc thật — nhìn UI thấy chưa có job nào chạy, mọi người tưởng treo.
2. **Mỗi file ≥ 1 task** — Spark gộp được nhiều file nhỏ vào 1 task (theo `maxPartitionBytes` + `openCostInBytes`, xem Internal), nhưng gộp có giới hạn và vẫn phải MỞ từng file: mỗi lần mở là 1 lượt bắt tay với storage, đọc footer Parquet, dựng reader. 1.9 triệu lần mở file so với 400 lần — chi phí cố định nuốt sạch throughput.
3. **Scheduler overhead** — nhiều task nhỏ = driver phải serialize, gửi, theo dõi, thu kết quả từng task (~ms mỗi task nhưng nhân trăm nghìn lần). Task làm việc 50 ms mà chi phí vòng đời task 20 ms là lỗ 40%.
4. **Memory driver** — danh sách file + metadata (path, size, block location, partition values) nằm TRONG HEAP DRIVER. Hàng triệu file = hàng GB metadata → driver GC liên miên, thậm chí OOM trước khi executor kịp làm gì.

Và một nạn nhân thầm lặng thứ 5: **Parquet mất tác dụng**. Parquet mạnh nhờ row group lớn, statistics, encoding theo cột — file 27 KB thì mỗi file một mẩu row group còi, min/max statistics vô nghĩa, nén kém.

> **Analogy chuyển nhà**: cùng 5 tấn đồ đạc — đóng vào 400 thùng carton chuẩn thì 2 chuyến xe tải là xong; chia vào 1.9 triệu túi nylon thì đội bốc vác chết vì THAO TÁC (nhặt, buộc, đếm, ghi sổ từng túi), không phải vì cân nặng. Small files là bệnh chết vì thao tác.

### Trade-off (không phải file càng to càng tốt)

| File quá nhỏ (< vài MB) | File quá to (> vài GB) |
|---|---|
| Listing + open + scheduler overhead | Ít file quá → thiếu parallelism khi đọc (dù Parquet splittable, vẫn kém linh hoạt) |
| Driver memory phình | 1 file hỏng = mất nhiều dữ liệu hơn, retry đắt hơn |
| Parquet statistics vô dụng | Ghi chậm (ít writer song song), khó update từng phần |

Vùng vàng: **128 MB – 1 GB mỗi file** — đủ to để chi phí mở file không đáng kể, đủ nhỏ để song song hoá và vá lỗi.

---

## 3. Theory

### 3.1. Thuật ngữ nền

| Thuật ngữ | Nghĩa |
|---|---|
| **File listing** | Bước liệt kê file trong path trước khi đọc — driver làm, tuần tự theo folder. |
| **Split** | Khúc dữ liệu 1 task đọc. 1 file to có thể nhiều split; nhiều file nhỏ có thể gộp 1 split. |
| **Open cost** | Chi phí cố định mở 1 file (handshake, đọc footer/schema) — Spark mô hình hoá bằng `openCostInBytes`. |
| **Compaction** | Gộp nhiều file nhỏ thành ít file to, dữ liệu không đổi. |
| **Partition folder** | Thư mục `col=value/` do `partitionBy` sinh ra — đơn vị pruning khi đọc. Đừng nhầm với partition in-memory! |
| **Row group** | Khối dữ liệu bên trong Parquet (mặc định ~128 MB) — đơn vị đọc/skip của Parquet reader. |

### 3.2. Small files từ đâu ra? Ba nguồn kinh điển

**Nguồn 1 — Streaming/ingest ghi liên tục:**

```
Trigger mỗi 1 phút, mỗi micro-batch vài nghìn dòng:
  phút 1: part-00000 (80 KB)     ← mỗi lần ghi TẠO FILE MỚI,
  phút 2: part-00000 (95 KB)        không append vào file cũ
  phút 3: part-00000 (71 KB)        (object storage không append được)
  ...
  1 ngày = 1440 file/folder × số folder. Tự nhiên như hơi thở.
```

**Nguồn 2 — Partition folder quá mịn (over-partitioning):**

```
.partitionBy("date", "hour", "customer_state", "category")
→ 365 × 24 × 27 × 74 ≈ 17.5 TRIỆU tổ hợp folder
→ dữ liệu 50 GB rắc vào đó = bụi. Mỗi folder vài KB.

Quy tắc: cardinality partition column phải sao cho MỖI partition folder ≥ vài trăm MB.
Cột lọc phổ biến (date) → partition. Cột lọc hiếm/cardinality cao → KHÔNG (để sort lo, 3.4).
```

**Nguồn 3 — Shuffle partitions cao ngay trước khi ghi:**

```
df.groupBy(...).agg(...)      ← shuffle ra 200 partition (mặc định)
  .write.parquet(path)        ← MỖI partition in-memory = ≥1 file
→ 200 file dù kết quả chỉ 40 MB. Bật thêm partitionBy nữa thì
  mỗi task ghi vào MỌI folder nó có dữ liệu → 200 × folders file!
```

Nguồn 3 chính là công thức thảm hoạ: `số file ≈ số task ghi × số folder mỗi task chạm vào`. AQE coalesce (lesson 20) đỡ được phần nào ở đây — nhưng chỉ khi có shuffle ngay trước write, và advisory 64 MB vẫn nhỏ hơn cỡ file lý tưởng.

### 3.3. Công thức số file khi ghi — thuộc lòng

```
KHÔNG partitionBy:
  số file = số partition in-memory lúc write (mỗi task 1 file)

CÓ partitionBy(col):
  số file = Σ (mỗi task ghi 1 file cho MỖI giá trị col nó cầm)
  worst case = numTasks × numFolders

CÓ maxRecordsPerFile:
  file bị CẮT thêm khi vượt ngưỡng dòng → chống file quá TO
  (không chống file nhỏ!)
```

Muốn mỗi folder đúng 1 file cỡ đẹp: `df.repartition(col)` trước `partitionBy(col)` — mọi dòng cùng value về 1 task → 1 file/folder; folder to thì thêm `maxRecordsPerFile` để cắt, hoặc salting writer (lesson 19 §5.6).

### 3.4. File layout hoàn chỉnh — 3 tầng quyết định tốc độ đọc

```
Tầng 1: PARTITION FOLDER  → skip cả THƯ MỤC       (partition pruning)
        date=2025-07-01/       WHERE date = ... không đụng folder khác

Tầng 2: CỠ FILE 128MB–1GB → ít overhead, đủ song song

Tầng 3: SORT TRONG FILE   → skip ROW GROUP        (min/max statistics)
        sortWithinPartitions("customer_state")
        → row group 1: states A–C, row group 2: D–G, ...
        WHERE customer_state='SP' → đọc đúng row group chứa SP, skip 90% file
```

Ba tầng = ba cấp độ "né đọc": né folder → né file → né row group. Bảng được thiết kế đủ 3 tầng có thể nhanh hơn bảng vứt bừa 100× trên CÙNG một engine — không cần thêm một executor nào.

### 3.5. Compaction — trả nợ định kỳ

Streaming không thể ngừng đẻ file nhỏ (bản chất trigger). Giải pháp không phải chặn đẻ, mà là **dọn định kỳ**: job compaction đọc N file nhỏ → ghi lại M file to → swap. Với Parquet trần (không table format): phải ghi ra path mới rồi đổi trỏ — kẹt chuyện reader đang đọc giữa chừng. Với **Iceberg** (module 5): `rewrite_data_files` chạy NGAY trên bảng đang phục vụ đọc/ghi, nhờ snapshot isolation — reader cũ vẫn thấy snapshot cũ, commit compaction là atomic. Đây là một trong những lý do table format tồn tại.

---

## 4. Internal

Spark thực sự làm gì khi bạn `spark.read.parquet(path)` trên 1 triệu file:

```
① DRIVER: liệt kê file (InMemoryFileIndex)
     - duyệt cây thư mục, mỗi folder ≥1 list call tới storage
     - nhiều folder → có thể chạy job listing song song trên executor
       (spark.sql.sources.parallelPartitionDiscovery.threshold, mặc định 32 path)
     - kết quả: danh sách [path, size, ...] NẰM TRONG HEAP DRIVER
        │
② DRIVER: đọc schema (footer 1 hoặc vài file; mergeSchema thì đọc footer TẤT CẢ — thảm hoạ)
        │
③ DRIVER: cắt SPLIT — gộp file nhỏ vào cùng partition theo công thức:

   maxSplitBytes = min( maxPartitionBytes,                     ← mặc định 128MB
                        max( openCostInBytes,                  ← mặc định 4MB
                             totalBytes+files×openCost / defaultParallelism ))

   - mỗi file nhỏ bị TÍNH PHỤ THÊM openCostInBytes (4MB) vào size khi xếp bin
     → 1000 file 100KB "nặng" như 1000 × 4.1MB → chia ra NHIỀU partition
     hơn bạn tưởng, mỗi partition vẫn phải mở hàng chục file
        │
④ EXECUTOR: mỗi task nhận danh sách khúc file của mình
     mỗi file: mở connection → đọc footer → dựng reader → đọc data → đóng
     file 100KB: chi phí mở ≈ 10–50ms, đọc data ≈ 1ms  → hiệu suất ~2–10%
     file 256MB: chi phí mở như trên, đọc ≈ giây        → hiệu suất ~99%
```

Ba điểm rút ra:

1. Spark ĐÃ cố cứu bạn (gộp file vào split, tính open cost) — nên 1000 file nhỏ không tạo đúng 1000 task. Nhưng **chi phí mở từng file thì không ai gộp hộ được** — nó nằm trong ④.
2. Bệnh nặng nhất nằm ở ① và ②: pha "im lặng trước job đầu tiên" toàn bộ do driver gánh. Đây là lúc mọi người tưởng cluster treo và đi restart lung tung.
3. Table format (Iceberg/Delta) né hẳn ①: danh sách file nằm trong **manifest/metadata của bảng**, đọc vài file metadata là biết hết — không list storage. Một lý do lớn nữa để module 5 tồn tại.

---

## 5. API

### 5.1. Chẩn đoán — đếm file trước, chữa sau

```bash
# Local/Docker: đếm file + phân bố size của một bảng
find output/events -name '*.parquet' | wc -l
du -sh output/events
find output/events -name '*.parquet' -printf '%s\n' | sort -n | awk \
  'BEGIN{c=0} {a[c++]=$1; s+=$1} END{print "files:",c," total:",s/1024/1024,"MB",
   " median:",a[int(c/2)]/1024,"KB"," avg:",s/c/1024,"KB"}'
```
- **Khi dùng**: NGHI NGỜ bảng bệnh — luôn đo trước. Median size < vài MB = có bệnh.

### 5.2. `coalesce(n)` / `repartition(n)` trước khi ghi

```python
target_files = max(1, int(df_size_bytes / (256 * 1024 * 1024)))   # nhắm ~256MB/file

df.coalesce(target_files).write.parquet(path)      # gộp KHÔNG shuffle — rẻ
df.repartition(target_files).write.parquet(path)   # shuffle — đắt hơn nhưng chia ĐỀU
```
- **`coalesce`**: gộp partition không shuffle (lesson 16) — rẻ, nhưng gộp lệch (partition to nhỏ tuỳ duyên) và có thể kéo tụt parallelism của cả các bước TRƯỚC nó (coalesce(1) làm cả pipeline chạy 1 task!).
- **`repartition`**: tốn 1 shuffle nhưng file đều tăm tắp; bắt buộc dùng khi cần chia lại theo cột (`repartition(F.col("date"))` trước `partitionBy("date")`).
- **Pitfall kinh điển**: `coalesce(1)` để "ra 1 file cho gọn" trên dữ liệu chục GB → 1 task ghi tuần tự cả chục GB, chậm + OOM. File to có giới hạn của nó.

### 5.3. `maxRecordsPerFile` — chống file quá TO

```python
(df.repartition("order_month")
   .write
   .option("maxRecordsPerFile", 2_000_000)      # hoặc spark.sql.files.maxRecordsPerFile
   .partitionBy("order_month")
   .mode("overwrite")
   .parquet("output/orders"))
```
- **Ý nghĩa**: task ghi tự cắt file mới mỗi khi vượt N dòng → folder to (tháng sale!) không thành 1 file 8 GB.
- **Pitfall**: đơn vị là DÒNG, không phải byte — phải ước lượng bytes/dòng để quy đổi (đo bằng size hiện tại / count). Và nó KHÔNG gộp file nhỏ — chỉ cắt file to. Chống 2 bệnh khác nhau, cần cả 5.2 lẫn 5.3.

### 5.4. Tuning phía ĐỌC khi lỡ dính bảng bệnh (giảm đau, không chữa)

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")  # gộp nhiều file nhỏ hơn vào 1 task
spark.conf.set("spark.sql.files.openCostInBytes", "8m")      # coi phí mở file đắt hơn → gộp mạnh tay hơn
```
- **Khi dùng**: phải đọc bảng small-files của người khác, chưa compaction được ngay.
- **Pitfall**: đây là thuốc giảm đau — chi phí mở từng file và listing vẫn nguyên. Chữa tận gốc = compaction.

### 5.5. Compaction thủ công (Parquet trần)

```python
# Đọc bảng bệnh → ghi lại path mới với layout chuẩn → swap path (hoặc overwrite)
df = spark.read.parquet("output/events_smallfiles")
(df.repartition(F.col("event_date"))
   .sortWithinPartitions("customer_state")            # tầng 3: sort trong partition
   .write
   .option("maxRecordsPerFile", 2_000_000)
   .partitionBy("event_date")
   .mode("overwrite")
   .parquet("output/events_compacted"))
```
- **Pitfall**: `mode("overwrite")` lên CHÍNH path đang có reader → reader đang chạy chết giữa chừng, và nếu job compaction fail giữa chừng thì mất luôn bảng. Parquet trần: ghi path mới + đổi con trỏ (view/synonym/biến config). Muốn tử tế → Iceberg.

### 5.6. Compaction chuẩn production — Iceberg `rewrite_data_files` (nếm trước module 5)

```sql
CALL catalog.system.rewrite_data_files(
  table => 'lake.events',
  options => map('target-file-size-bytes', '268435456',   -- 256MB
                 'min-input-files', '5')
);
```
- **Ý nghĩa**: gộp file nhỏ NGAY trên bảng đang đọc/ghi — snapshot isolation lo an toàn, commit atomic, không downtime. Chạy scheduled (Airflow) hàng ngày/giờ tuỳ tốc độ đẻ file.
- **Pitfall**: quên chạy kèm `expire_snapshots` → file cũ không bao giờ được xoá vật lý, storage phình gấp đôi. Compaction là cặp bài: rewrite + expire.

---

## 6. Demo nhỏ

```
Input:  CÙNG một dataset ~2M dòng, ghi 2 kiểu: 1000 file vs 10 file
   ↓    đọc lại + aggregate mỗi kiểu, đo giây
Output: bảng so sánh — cùng bytes, khác trời vực
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (SparkSession.builder.appName("demo21-smallfiles").master("local[2]")
         .getOrCreate())

df = (spark.range(2_000_000)
      .withColumn("cat", (F.col("id") % 500).cast("string"))
      .withColumn("val", F.rand()))

df.repartition(1000).write.mode("overwrite").parquet("/tmp/demo21/small")  # 1000 file còi
df.repartition(10).write.mode("overwrite").parquet("/tmp/demo21/big")      # 10 file chuẩn

def bench(path):
    t0 = time.time()
    r = spark.read.parquet(path).groupBy("cat").agg(F.sum("val")).count()
    return time.time() - t0

for name, path in [("1000 small", "/tmp/demo21/small"), ("10 big", "/tmp/demo21/big")]:
    print(f"{name}: {bench(path):.1f}s")

input(">>> UI :4040 → Jobs: so 2 job đọc — số task stage scan, thời gian. Enter...")
spark.stop()
```

Chạy xong tự hỏi: stage scan mỗi bên mấy task (Spark có gộp file nhỏ vào task không — soi Internal ④)? Chênh lệch chủ yếu nằm ở listing, open cost hay compute? Local filesystem đã "nhân từ" hơn S3 bao nhiêu (list local ~free!) — trên cloud khoảng cách còn tàn khốc hơn.

---

## 7. Production Example

Kiến trúc pipeline Olist-CDC của bạn (lesson 1) nhìn dưới lăng kính file layout:

```
Kafka → Spark Streaming ghi bronze (trigger 1 phút)   ← MÁY ĐẺ small files chạy 24/7
      → Spark batch silver/gold                        ← nạn nhân: đọc bronze chậm dần
      → Iceberg                                        ← thuốc chữa: compaction không downtime
      → Trino BI                                       ← nạn nhân thứ 2: query interactive lết
```

Cách các team lakehouse trưởng thành vận hành:

1. **Chấp nhận bronze đẻ file nhỏ** — không chặn được (latency ingest quan trọng hơn). Nhưng bronze là Iceberg, và Airflow chạy `rewrite_data_files` mỗi 6 giờ + `expire_snapshots` hằng đêm. File nhỏ chỉ sống tối đa 6 giờ.
2. **Silver/gold ghi chuẩn ngay từ đầu**: batch job có `repartition(cột partition)` + `maxRecordsPerFile` trong writer template chung — không ai tự viết writer tay.
3. **Layout gold theo truy vấn BI**: partition theo `order_date` (mọi dashboard lọc theo ngày), sort trong partition theo `customer_state` (bộ lọc phổ biến thứ 2). Trino skip folder theo ngày, skip row group theo state.
4. **Giám sát như metric hạng nhất**: dashboard theo dõi `avg_file_size` và `file_count` mỗi bảng (Iceberg metadata table `table$files` cho số liệu miễn phí). Cảnh báo khi avg < 32 MB. Bệnh này phải bắt ở giai đoạn ủ, không đợi phát.

Bài học: small files không phải "sự cố" mà là **dòng chảy tự nhiên của mọi hệ thống ingest** — kiến trúc trưởng thành thiết kế sẵn vòng dọn dẹp, như thành phố thiết kế sẵn hệ thống thu gom rác.

---

## 8. Hands-on Lab

**Mục tiêu**: tự gây bệnh small files trên Olist phóng to, đo 3 tầng thiệt hại, rồi compaction và đo lại.

### Bước 0 — chuẩn bị

```bash
make up      # master :8080, app UI :4040
```

Dataset tại `data/olist/*.csv` (container: `/workspace/data/olist/`). Worker 1 GB/1 core — overhead task nhỏ hiện rõ hơn cluster xịn.

### Bước 1 — `labs/lab21/make_smallfiles.py` (gây bệnh)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab21-make").getOrCreate()

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)
# Phóng to ~20× cho đủ đô (~2.2M dòng)
big = items.crossJoin(spark.range(20).select(F.lit(1).alias("d"))).drop("d")

# Bệnh nhân: 1000 file nhỏ
big.repartition(1000).write.mode("overwrite") \
   .parquet("/workspace/labs/lab21/items_small")

# Đối chứng: 8 file chuẩn
big.repartition(8).write.mode("overwrite") \
   .parquet("/workspace/labs/lab21/items_big")
spark.stop()
```

Sau khi chạy, đếm bệnh bằng lệnh ở §5.1 (chạy trên host, path `labs/lab21/...`). Ghi lại: số file, tổng MB, median KB.

### Bước 2 — `labs/lab21/read_bench.py` (đo thiệt hại)

Đọc lần lượt 2 path, mỗi path đo: (a) thời gian `spark.read.parquet(...).count()` — pha listing + scan; (b) thời gian query aggregate `groupBy("seller_id").sum("price")`. In bảng 2×2. Chạy `make run F=labs/lab21/read_bench.py` — lưu số.

### Bước 3 — `labs/lab21/compact.py` (chữa bệnh)

Compaction bảng bệnh theo pattern §5.5: repartition theo cột + `sortWithinPartitions("seller_id")` + `maxRecordsPerFile=500_000`, ghi ra `items_compacted`. Đếm file kết quả, chạy lại bench của Bước 2 trên bảng đã compact.

### Bước 4 — thí nghiệm over-partitioning (tự thiết kế)

Ghi `items_big` với `.partitionBy("seller_id")` (3000+ seller = 3000+ folder!). Đo: thời gian GHI, số folder, thời gian đọc `count()`. So với partition theo cột thô hơn (tự tạo `order_month` từ bảng orders join vào, hoặc dùng `product_id % 12` làm cột giả). Kết luận: cardinality bao nhiêu là quá mịn cho dữ liệu cỡ này?

### Bước 5 — ghi nhận

`labs/lab21/NOTES.md`: bảng (small / big / compacted / over-partitioned) × (số file, median size, thời gian count, thời gian agg, thời gian ghi). Kèm 3 dòng: thiệt hại nặng nhất nằm ở pha nào? compaction lấy lại được bao nhiêu %? nguyên tắc chọn cột partitionBy bạn rút ra?

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Kể 4 lý do small files làm chậm — phân loại: cái nào đánh vào driver, cái nào vào executor, cái nào vào storage API?
2. Ba nguồn sinh small files kinh điển? Với mỗi nguồn, một câu phòng bệnh.
3. Vì sao file 5 GB cũng không tốt? Vùng vàng là bao nhiêu và tại sao?

**Medium** — Công thức số file: cho `df` 80 partition in-memory, có cột `date` 30 giá trị. Dự đoán số file khi: (a) `write.parquet` thẳng; (b) `partitionBy("date")` thẳng (worst case?); (c) `repartition("date")` rồi `partitionBy("date")`; (d) thêm `maxRecordsPerFile` vào (c). Viết code kiểm chứng cả 4 trên dữ liệu lab, so dự đoán vs thực tế, giải thích chênh lệch.

**Hard** — Streaming compaction không downtime: bảng Parquet trần đang có reader đọc mỗi 5 phút, writer streaming ghi mỗi 1 phút. Thiết kế quy trình compaction KHÔNG làm reader fail: vẽ sơ đồ các bước (ghi path mới → swap thế nào? file đang được ghi dở xử lý ra sao? xoá file cũ khi nào an toàn?). Chỉ ra chỗ nào race condition không thể xử triệt để với Parquet trần — và Iceberg giải quyết chỗ đó bằng cơ chế gì (đọc trước docs `rewrite_data_files` + snapshot).

**Production Challenge** — Viết `labs/lab21/file_health.py`: nhận path, in "phiếu khám sức khoẻ": tổng file, tổng size, min/median/avg/max size, số file < 8 MB (%), số folder, folder lệch nhất (max/median size folder — nhớ lesson 19: skew khi ghi!), và 1 dòng chẩn đoán tự động (KHOẺ / BỆNH NHẸ / BỆNH NẶNG kèm lý do). Chạy trên cả 4 bảng của lab. Đây là tool bạn sẽ dùng thật trong Project 3.

> Nộp bài bằng cách paste code + số đo + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Chi phí với small files | Với file chuẩn 128MB–1GB |
|---|---|---|
| Listing (driver, trước job đầu) | Phút → chục phút trên object storage | Giây |
| Mở file (executor, mỗi file) | Chiếm 90%+ thời gian task | < 1% thời gian task |
| Scheduler | Trăm nghìn task tí hon | Trăm task đầy đặn |
| Parquet statistics / row group skip | Vô dụng (file bé hơn 1 row group chuẩn) | Skip được 50–95% I/O nếu có sort |
| Driver memory | GB metadata, GC storm | Không đáng kể |

Ba con số thuộc lòng:

- **Cỡ file mục tiêu: 128 MB – 1 GB** (thoả hiệp mở-file vs song-song; trùng "vùng đẹp" của row group Parquet và block HDFS).
- **Mỗi partition folder ≥ vài trăm MB** — folder nhỏ hơn thế nghĩa là cột partition quá mịn.
- **avg file size < 32 MB = báo động** — ngưỡng cảnh báo dashboard hợp lý cho bảng lake.

Câu tự vấn trước mỗi lệnh `write` từ nay: *"lệnh này sẽ ra bao nhiêu file, mỗi file bao nhiêu MB, và 6 tháng nữa bảng này có bao nhiêu file?"*

---

## 11. Spark UI

Chữ ký của small files trên UI — nhận diện trong 1 phút:

**Trước cả Jobs tab**: khoảng "im lặng" dài sau khi submit — UI đã lên nhưng chưa có job nào, hoặc có job tên `Listing leaf files and directories` (chính là driver nhờ executor list song song). Thấy job này chạy lâu = bệnh viện xác nhận.

**Tab Jobs / Stages**:
- Stage scan có số task lớn bất thường so với GB dữ liệu (nghìn task cho vài GB).
- Duration mỗi task rất ngắn (< 100 ms) — bảng Summary Metrics: median duration tí hon trong khi tổng thời gian stage dài → chết vì overhead, không phải vì compute.
- So cột **Input Size / Records** của task: mỗi task đọc vài trăm KB = task còi ăn không đủ no.

**Tab SQL**: node `Scan parquet` → metrics `number of files read`, `size of files read` — chia nhau ra avg size ngay tại chỗ. Đây là con số đưa vào báo cáo chẩn đoán Project 3.

**Tab Executors**: thời gian **Task Time** tổng cao nhưng **Input** thấp — cluster bận rộn "thao tác" chứ không xử lý dữ liệu (nhớ analogy túi nylon).

---

## 12. Common Mistakes

1. **`coalesce(1)` cho "ra 1 file đẹp"** trên dữ liệu lớn → 1 task ghi tuần tự, chậm + có thể kéo cả pipeline trước đó về 1 task. Chỉ dùng cho kết quả THẬT nhỏ (báo cáo vài MB).
2. **`partitionBy` theo cột cardinality cao** (user_id, order_id...) → triệu folder bụi. Partition theo cột LỌC PHỔ BIẾN + cardinality thấp; cột cardinality cao để `sortWithinPartitions` lo.
3. **Ghi thẳng sau groupBy với shuffle.partitions mặc định** → 200 file/lần ghi × chạy hourly = 4800 file/ngày. Luôn có bước chỉnh số partition (repartition/coalesce/AQE advisory) trước writer.
4. **Nghĩ `maxRecordsPerFile` gộp file nhỏ** — không, nó chỉ CẮT file to. Chống nhỏ = repartition/coalesce; chống to = maxRecordsPerFile. Hai thuốc, hai bệnh.
5. **Compaction bằng overwrite lên chính path đang có reader** → reader fail giữa chừng, job compaction fail thì mất bảng. Path mới + swap, hoặc dùng table format.
6. **Chỉ compaction mà quên dọn snapshot/file cũ** (Iceberg: expire_snapshots) → storage ×2, ×3 mà không hiểu tiền cloud đi đâu.
7. **Không có metric file count/size trên dashboard** — bệnh tích luỹ 6 tháng mới phát, lúc đó compaction lần đầu mất cả ngày. Đo từ ngày đầu, cảnh báo theo ngưỡng.

---

## 13. Interview

**Junior:**

1. *Small files problem là gì, tại sao chậm?* — Bảng gồm quá nhiều file quá nhỏ (KB–vài MB). Chậm vì: driver phải list hàng triệu entry metadata (lâu, tốn heap); executor trả chi phí mở/đọc footer cho TỪNG file trong khi dữ liệu mỗi file chẳng bao nhiêu; scheduler gánh trăm nghìn task tí hon; Parquet statistics/row group mất tác dụng.
2. *Small files từ đâu ra?* — (a) Streaming/ingest ghi liên tục, mỗi micro-batch tạo file mới; (b) partitionBy cột quá mịn/cardinality cao → triệu folder bụi; (c) số partition in-memory (shuffle partitions) cao ngay trước write → mỗi task 1+ file.
3. *Cỡ file lý tưởng cho data lake?* — 128 MB – 1 GB: đủ to để chi phí mở file không đáng kể và Parquet row group/statistics hiệu quả, đủ nhỏ để đọc song song và retry rẻ.
4. *repartition và coalesce trước khi ghi khác gì nhau?* — coalesce gộp partition không shuffle: rẻ nhưng phân bố lệch và có thể giảm parallelism các bước trước; repartition shuffle: đắt hơn nhưng chia đều và chia lại được theo cột. Cần file đều/ghi theo partition column → repartition; chỉ cần giảm nhẹ số file cuối → coalesce.

**Mid:**

5. *`partitionBy("date")` mà ra hàng nghìn file mỗi folder — vì sao và fix thế nào?* — Mỗi task ghi 1 file cho mỗi giá trị date nó cầm; dữ liệu date trộn lẫn khắp N task → mỗi folder nhận tới N file. Fix: `repartition(col("date"))` trước để gom mỗi date về 1 task → 1 file/folder; folder to thì thêm maxRecordsPerFile hoặc repartition(col, salt) để cắt.
6. *Spark có cơ chế gì giảm đau khi ĐỌC bảng small files?* — Gộp nhiều file nhỏ vào 1 split theo `maxPartitionBytes`, và cộng phạt `openCostInBytes` (~4MB) cho mỗi file khi xếp bin để tránh task ôm quá nhiều file. Tăng 2 config này gộp mạnh tay hơn. Nhưng chỉ giảm số TASK — chi phí listing và mở từng file vẫn nguyên, chữa gốc phải compaction.
7. *Thiết kế file layout cho bảng fact lớn — nêu 3 tầng.* — (1) Partition folder theo cột lọc phổ biến + cardinality thấp (thường date) → pruning cấp thư mục; (2) cỡ file 128 MB–1 GB qua repartition + maxRecordsPerFile; (3) sortWithinPartitions theo cột lọc thứ cấp → min/max statistics skip row group. Ba tầng: né folder, né file, né row group.
8. *Vì sao table format (Iceberg) đọc nhanh hơn Parquet trần trên cùng dữ liệu nhiều file?* — Iceberg giữ danh sách file + statistics trong manifest metadata: đọc vài file metadata thay vì list toàn bộ object storage; pruning bằng metadata thay vì mở footer từng file; và hỗ trợ compaction/rewrite an toàn ngay trên bảng đang chạy.

**Senior:**

9. *Bảng streaming đẻ 100k file nhỏ/ngày, BI than chậm — chiến lược tổng thể của anh/chị?* — (a) Đo: file count, avg size, phân bố theo folder (tool file_health); xác nhận bệnh và mức độ. (b) Ngắn hạn: tăng maxPartitionBytes/openCostInBytes phía đọc để giảm đau. (c) Trung hạn: compaction định kỳ — nếu Iceberg thì rewrite_data_files + expire_snapshots theo lịch, tần suất theo tốc độ đẻ file; nếu Parquet trần thì compact sang path mới + swap atomic qua con trỏ. (d) Gốc rễ: xem lại trigger interval (1 phút có cần không?), số partition khi ghi, cột partitionBy; cân nhắc bronze latency vs file size. (e) Phòng bệnh: metric file size lên dashboard + alert; writer template chuẩn cho cả team.
10. *Trade-off khi chọn tần suất compaction cho bảng streaming?* — Compact dày (mỗi giờ): reader luôn khoẻ, nhưng tốn compute lặp lại (file có thể bị rewrite nhiều lần — write amplification), tranh tài nguyên với pipeline chính, nhiều snapshot cần expire. Compact thưa (mỗi ngày): rẻ compute nhưng reader chịu file nhỏ lâu hơn, lần compact to hơn, rủi ro job compact fail giữa chừng lớn hơn. Quyết theo: SLA độ trễ query, tốc độ đẻ file, giờ thấp điểm cluster; pattern phổ biến: compact incremental thường xuyên cho partition nóng (hôm nay), compact sâu định kỳ cho partition nguội.

---

## 14. Summary

### Mindmap

```
                        SMALL FILES & FILE LAYOUT (L21)
                                    │
     ┌───────────────┬──────────────┴──────────────┬───────────────────┐
     ▼               ▼                             ▼                   ▼
  TẠI SAO CHẬM     NGUỒN BỆNH                  CHỮA & PHÒNG        LAYOUT 3 TẦNG
     │               │                             │                   │
  listing (driver) streaming trigger           repartition/         1. partition folder
  open cost/file   partitionBy quá mịn           coalesce trước ghi    (lọc phổ biến,
  scheduler        shuffle.partitions cao      maxRecordsPerFile       cardinality thấp)
  overhead           khi ghi                     (chống file TO)    2. file 128MB–1GB
  driver heap      công thức:                  compaction định kỳ  3. sortWithinPartitions
  parquet stats    files ≈ tasks × folders       Iceberg rewrite      → skip row group
  vô dụng                                        + expire_snapshots
                                               đo file health, alert <32MB
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 4 lý do small files chậm và pha nào của job gánh từng lý do.
- [ ] Thuộc công thức số file khi ghi (có/không partitionBy) và cách ép 1 file/folder.
- [ ] Phân biệt 2 thuốc: repartition/coalesce (chống nhỏ) vs maxRecordsPerFile (chống to).
- [ ] Đã đo tận tay 1000 file vs 10 file và compaction lấy lại tốc độ.
- [ ] Nói được layout 3 tầng và lý do từng tầng.
- [ ] Biết vì sao compaction trên Parquet trần khó an toàn và Iceberg giải quyết bằng gì.
- [ ] Trả lời 10 câu interview không nhìn đáp án.

---

## 15. Next Lesson

**Lesson 22 — Quy trình tuning tổng hợp: Checklist Senior.**

Bạn đã có đủ đồ nghề rời rạc: shuffle (15), partitioning (16), memory & spill (17), cache (18), skew (19), AQE (20), file layout (21). Nhưng 3 giờ sáng, on-call gọi "job chậm gấp 8 lần", bạn không thể thử ngẫu nhiên 7 thứ. Cái thiếu là **thứ tự ra đòn**: nhìn gì trước, ngưỡng nào thì nghi ngờ gì, hành động nào đổi 1 thứ một lần. Lesson 22 đóng gói toàn bộ module thành playbook 10 bước + bảng 15 config quan trọng nhất kèm giá trị khởi điểm + runbook on-call — chính là quy trình bạn sẽ dùng để cứu pipeline trong Project 3 ngay sau đó.

> Gõ **"Continue"** khi sẵn sàng.
