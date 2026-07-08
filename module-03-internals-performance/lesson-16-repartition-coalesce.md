# Lesson 16 — Partitioning chiến lược: repartition vs coalesce

> Module 3 · Internals & Performance Tuning · Tuần 8 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **`repartition`**: full shuffle, cân bằng lại dữ liệu, tăng hoặc giảm số partition — và cái giá của nó.
- **`coalesce`**: gộp partition không cần shuffle, chỉ giảm — và **cái bẫy `coalesce(1)`** làm sập parallelism của cả job mà 90% người dùng không biết.
- `repartition(col)` và `repartitionByRange` — sắp xếp dữ liệu THEO NỘI DUNG trước khi ghi, vũ khí chống "nghìn file rác" khi ghi partitioned table.
- Khống chế **số lượng và kích thước file output** — kỹ năng ghi lakehouse hàng ngày.
- Bảng quyết định: tình huống nào dùng công cụ nào.

Sau bài này bạn phải làm được:

- Giải thích cho đồng nghiệp tại sao `coalesce(1)` trước khi ghi có thể làm cả job chạy trên 1 core.
- Trước khi ghi một bảng partitioned, viết đúng câu `repartition` để mỗi partition ra số file mong muốn.
- Nhìn `explain()` phân biệt được `Exchange` (repartition) và `Coalesce` trong physical plan.

Kiến thức dùng trong thực tế: **mỗi lần ghi bảng** — tức là hàng ngày. Bảng ghi ẩu hôm nay = small files problem (lesson 21) + query chậm cho mọi người dùng downstream trong nhiều tháng.

---

## 2. Why

### Số partition sai — hai thái cực đều chết

Lesson 15 cho bạn thấy: partition quá ít → task to → spill/OOM. Nhưng chiều ngược lại cũng tệ không kém. Câu chuyện kinh điển ở mọi data team:

```
Pipeline ghi bảng orders_daily, chạy với spark.sql.shuffle.partitions = 200:

  df.write.partitionBy("dt").parquet(...)   # 30 ngày dữ liệu

Kết quả trên storage:
  dt=2026-06-01/  part-00000.parquet (1.2 MB), part-00001.parquet (0.9 MB), ... 200 file
  dt=2026-06-02/  ... 200 file
  ...
  → 30 ngày × 200 task = 6.000 file bé tí cho 5 GB dữ liệu!
```

Sáu tháng sau, bảng có 1 triệu file, mỗi lần `spark.read` mất 5 phút chỉ để LIỆT KÊ file. Ai đó "sửa" bằng `coalesce(1)` — giờ mỗi ngày 1 file đẹp, nhưng job từ 10 phút thành 3 tiếng vì toàn bộ pipeline co về **một task duy nhất**. Cả hai tai nạn đều do không hiểu bài hôm nay.

### Analogy: chuyển nhà vs dồn phòng

Công ty bạn có 1000 nhân viên ngồi rải rác 100 phòng, muốn dồn về 10 phòng:

- **`repartition(10)`** = tổng tái bố trí: TẤT CẢ 1000 người đứng dậy, bốc thăm phòng mới, di chuyển (full shuffle). Đắt, ồn ào — nhưng kết quả 10 phòng **đều tăm tắp** 100 người/phòng. Và vì là tái bố trí toàn diện, nó tăng phòng cũng được (10 → 300 phòng).
- **`coalesce(10)`** = sáp nhập tại chỗ: phòng 1–10 giữ nguyên chỗ ngồi, người phòng 11–100 chỉ *được ghi danh* vào 10 phòng đầu — thực chất mỗi "phòng mới" là chồng ghép của ~10 phòng cũ **trong cùng tòa nhà (executor)**, không ai di chuyển qua tòa khác. Rẻ, nhanh — nhưng phòng to phòng nhỏ tùy may rủi, và không thể "tách" phòng (không tăng được).

Và cái bẫy: nếu bạn ra lệnh "dồn về **1 phòng**" ngay từ đầu ngày làm việc, thì mọi việc trong ngày — kể cả những việc lẽ ra 100 phòng làm song song — bị dồn cho đúng nhóm người của 1 phòng đó làm. Đó chính là `coalesce(1)` lan ngược lên upstream.

### Trade-off (Senior phải thuộc)

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle? | **CÓ** — full shuffle (Exchange) | **KHÔNG** — narrow, gộp tại chỗ |
| Tăng số partition? | Được | Không (yêu cầu tăng sẽ bị lờ đi) |
| Phân bố dữ liệu | Đều (round-robin/hash) | Có thể lệch (gộp cơ học) |
| Chi phí | Serialize + disk + network (lesson 15) | Gần như miễn phí |
| Tác dụng phụ | Cắt stage mới — upstream giữ nguyên parallelism | **Lan ngược**: giảm parallelism của cả stage upstream |

---

## 3. Theory

### 3.1. `repartition(n)` — full shuffle, chia đều

```
TRƯỚC: 4 partition lệch          repartition(3) = SHUFFLE          SAU: 3 partition đều
┌──────────┐                                                   ┌────────┐
│ P0: 900MB │──┐    mỗi record được chia round-robin      ┌──▶│ P0'~400│
├──────────┤  │    (hoặc hash nếu có cột) sang partition  │   ├────────┤
│ P1:  50MB │──┼──▶ đích, đi qua đủ 4 khâu phí shuffle ───┼──▶│ P1'~400│
├──────────┤  │    (serialize+disk+network+deserialize)   │   ├────────┤
│ P2:  30MB │──┤                                          └──▶│ P2'~400│
├──────────┤  │                                               └────────┘
│ P3: 220MB │──┘         Exchange RoundRobinPartitioning(3)
└──────────┘
```

- `repartition(n)` không cột → **RoundRobinPartitioning**: chia đều tuyệt đối, không quan tâm nội dung. Dùng khi mục tiêu là *cân bằng tải* (sau filter làm lệch, trước một phép tính nặng).
- `repartition(col)` / `repartition(n, col)` → **HashPartitioning**: record cùng giá trị `col` về CÙNG partition. Dùng khi mục tiêu là *gom theo nội dung* — đặc biệt trước khi ghi `partitionBy(col)` (mục 3.4).
- `repartitionByRange(n, col)` → **RangePartitioning**: Spark *sample* dữ liệu để tìm các khoảng chia, record được xếp theo khoảng giá trị (partition 1: A–F, partition 2: G–M...). Dữ liệu ra vừa gom vừa **có thứ tự toàn cục theo khoảng** → file output sort tốt, nén tốt, range query đọc ít file. Lưu ý: vì phải sample nên tốn thêm 1 job nhỏ.

### 3.2. `coalesce(n)` — gộp tại chỗ, không shuffle

```
TRƯỚC: 8 partition trên 2 executor          coalesce(2): gộp CÙNG executor, không network

Executor A: [P0][P1][P2][P3]   ──────▶   Executor A: [P0+P1+P2+P3]  = P0'
Executor B: [P4][P5][P6][P7]   ──────▶   Executor B: [P4+P5+P6+P7]  = P1'

KHÔNG serialize, KHÔNG network — task mới chỉ đọc lần lượt nhiều partition cha.
```

- `coalesce` tạo **narrow dependency**: mỗi partition mới "trỏ" tới vài partition cũ, ưu tiên gộp những partition nằm cùng executor để không di chuyển dữ liệu.
- Chỉ **giảm** được. `coalesce(1000)` trên DataFrame 100 partition → vẫn 100.
- Kết quả có thể **lệch**: gộp cơ học 10 partition lệch thành 2 thì partition to vẫn to.

### 3.3. Cái bẫy `coalesce(1)` — lan ngược lên upstream

Đây là pitfall nổi tiếng nhất của bài. Vì `coalesce` là narrow transformation, nó **không cắt stage mới** — nó chỉ ghi đè "số task" của chính stage đang chứa nó, và hiệu lực đó lan ngược đến tận ranh giới shuffle gần nhất phía trước:

```
df.filter(...).withColumn(...).coalesce(1).write.parquet(...)

Bạn TƯỞNG:   [100 task đọc + filter + transform song song] → [1 task gộp & ghi]
THỰC TẾ:     [═══════════ 1 TASK làm TẤT CẢ: đọc, filter, transform, ghi ═══════════]
              vì coalesce(1) nằm cùng stage với filter/withColumn
              → cả stage chỉ còn 1 task → 1 core cày 100% job.
```

- Muốn "xử lý song song rồi mới gộp về 1 file" → dùng `repartition(1)`: shuffle cắt stage riêng, stage trước vẫn 100 task song song, chỉ stage ghi là 1 task. Đắt hơn coalesce một lần shuffle, nhưng nhanh hơn tổng thể **rất nhiều** khi phần transform nặng.
- Quy tắc nhớ nhanh: **coalesce rẻ ở phép gộp nhưng có thể đắt ở cả job; repartition đắt ở phép chia nhưng bảo toàn parallelism phía trước.**
- (Nếu ngay trước `coalesce` đã có sẵn một shuffle — ví dụ `groupBy` — thì hiệu ứng lan ngược dừng ở shuffle đó, thiệt hại nhỏ hơn. Nhưng đừng đánh cược: hãy `explain()` và nhìn số task từng stage trên UI.)

### 3.4. `repartition(col)` trước khi ghi partitioned table — pattern quan trọng nhất bài

Khi ghi `df.write.partitionBy("dt")`, **mỗi task ghi 1 file cho MỖI giá trị dt mà nó cầm**. Nếu dữ liệu chưa gom theo `dt`:

```
KHÔNG gom trước:  200 task × 30 giá trị dt   = tối đa 6.000 file (nhỏ li ti)

df.repartition("dt").write.partitionBy("dt"):
   record cùng dt về cùng task              = mỗi dt đúng 1 file (nhưng dt to → file to quá!)

df.repartition(F.col("dt"), F.expr("crc32(cast(order_id as string)) % 4"))
   .write.partitionBy("dt"):                = mỗi dt đúng 4 file — vừa đẹp
   (thêm "chìa khóa phụ" để tách mỗi dt ra 4 task)
```

Đây là pattern bạn sẽ dùng cả sự nghiệp: **gom dữ liệu theo cột partition (± khóa phụ) NGAY TRƯỚC khi ghi** để kiểm soát chính xác số file mỗi partition thư mục.

### 3.5. Khống chế kích thước file output

Mục tiêu chuẩn ngành: file Parquet **128 MB – 1 GB** (đủ to để scan hiệu quả, đủ nhỏ để song song). Công cụ:

| Công cụ | Cách hoạt động |
|---|---|
| `repartition(n)` / `repartition(n, col)` | n quyết định số file: `n ≈ tổng dung lượng / 256MB` |
| `spark.sql.files.maxRecordsPerFile` | Chặn trần: task cầm quá nhiều record thì tự cắt thêm file — chống file khổng lồ, không chống file bé |
| AQE coalesce partitions (lesson 20) | Spark 3 tự gom shuffle partition bé sau shuffle — đỡ được nhiều case, nhưng không thay được repartition(col) trước partitionBy |

### 3.6. Bảng quyết định (in ra dán cạnh màn hình)

| Tình huống | Dùng | Lý do |
|---|---|---|
| Sau `filter` bỏ 90% dữ liệu, còn nhiều partition rỗng/bé, phía sau còn tính toán nặng | `repartition(n nhỏ hơn)` | Cần cân bằng lại thật sự |
| Cuối job, chỉ muốn bớt số file output, transform phía trước nhẹ | `coalesce(n)` | Tránh trả giá 1 shuffle chỉ để ghi |
| Muốn ra đúng 1 file mà transform phía trước nặng | `repartition(1)` | Bảo toàn parallelism upstream |
| 1000 → 100 partition giữa pipeline | Thường `coalesce(100)`; nếu data lệch nặng thì `repartition(100)` | Rẻ trước, cân bằng sau |
| Tăng 50 → 400 partition (task đang quá to) | `repartition(400)` — coalesce không tăng được | |
| Trước `write.partitionBy(dt)` | `repartition(F.col("dt"))` ± khóa phụ | Kiểm soát số file/partition |
| Ghi bảng lớn hay bị range query (theo ngày, theo id) | `repartitionByRange` | File có thứ tự, nén tốt, skip tốt |

---

## 4. Internal

Nhìn dưới nắp capo để hiểu tại sao hai lệnh hành xử khác nhau:

```
repartition(3):                                coalesce(3):

== Physical Plan ==                            == Physical Plan ==
Exchange RoundRobinPartitioning(3)             Coalesce 3
+- ... (stage TRƯỚC kết thúc ở đây,            +- ... (KHÔNG có Exchange —
       shuffle write ra disk,                         cùng stage với operator trước,
       stage SAU đọc vào 3 task)                      chỉ đổi cách "gom" partition cha)
```

- **`repartition`** chèn một `ShuffleExchange` — mọi cơ chế của lesson 15 kích hoạt: map side write, shuffle files, reduce side read. Chi tiết tinh tế của Spark 3: trước round-robin, Spark **sort local từng partition** (`spark.sql.execution.sortBeforeRepartition=true`) để nếu task retry thì record vẫn được chia y hệt lần trước — đảm bảo tính đúng đắn khi có retry, đổi bằng chút CPU.
- **`coalesce`** tạo `CoalescedRDD` với narrow dependency: partition mới = danh sách partition cha. Bộ gộp (`DefaultPartitionCoalescer`) cố xếp các partition cha **cùng vị trí (executor/node)** vào một nhóm để giữ data locality. Vì là narrow, DAG scheduler KHÔNG cắt stage → số task của cả chuỗi narrow phía trước = n của coalesce — đây chính là cơ chế của cái bẫy 3.3.
- **`repartition(col)`** sinh `Exchange hashpartitioning(col, n)` với `n = spark.sql.shuffle.partitions` nếu không chỉ định. Lưu ý phỏng vấn hay hỏi: sau `repartition("dt")` với 30 giá trị dt và n=200 → **170 partition RỖNG** (mỗi dt vào đúng 1 trong 200 rổ theo hash). Partition rỗng gần như vô hại lúc chạy nhưng cho thấy hash partition ≠ "mỗi giá trị một partition".
- Khi `write.partitionBy(dt)`: mỗi task mở một writer cho từng giá trị `dt` gặp phải. Task cầm 30 dt = 30 file writer mở đồng thời = 30 file output + áp lực memory (mỗi Parquet writer giữ buffer/row group trong RAM — đây là nguồn OOM kín đáo khi ghi bảng nhiều partition mà quên repartition trước).

---

## 5. API

### `df.repartition(numPartitions, *cols)`

```python
df.repartition(64)                    # round-robin, cân bằng tuyệt đối
df.repartition("dt")                  # hash theo dt, n = spark.sql.shuffle.partitions
df.repartition(8, "dt")               # hash theo dt vào đúng 8 partition
```
- **Khi dùng**: cần cân bằng, cần tăng partition, cần gom theo cột trước khi ghi.
- **Pitfall**: `repartition("dt")` quên rằng n mặc định = 200 → vẫn có thể ra nhiều partition rỗng + số task ghi không như bạn tưởng. Chỉ định n tường minh khi ghi.

### `df.coalesce(numPartitions)`

```python
df.coalesce(16)     # gộp không shuffle; chỉ GIẢM
```
- **Khi dùng**: bước cuối trước khi ghi, transform phía trước nhẹ, chỉ cần bớt file.
- **Pitfall**: `coalesce(1)` lan ngược giết parallelism (mục 3.3). Kiểm tra bằng UI: stage ghi chỉ có 1 task VÀ kéo dài bất thường → dính bẫy.

### `df.repartitionByRange(numPartitions, *cols)`

```python
df.repartitionByRange(32, "order_purchase_timestamp")
```
- **Khi dùng**: ghi bảng lớn phục vụ range query; cần output có thứ tự theo khoảng.
- **Pitfall**: tốn 1 job sample nhỏ trước; cột nhiều giá trị trùng (low cardinality) có thể chia lệch.

### `df.rdd.getNumPartitions()` — nhiệt kế

```python
print(df.rdd.getNumPartitions())     # đo TRƯỚC và SAU mỗi thao tác khi debug
```

### `spark.sql.files.maxRecordsPerFile`

```python
spark.conf.set("spark.sql.files.maxRecordsPerFile", 1_000_000)  # 0 = không giới hạn
```
- **Khi dùng**: chặn trần kích thước file khi một task cầm quá nhiều dữ liệu.
- **Pitfall**: chỉ CẮT file to, không GỘP file bé — không thay thế được repartition/coalesce.

---

## 6. Demo nhỏ

```
Input:  DataFrame 8 partition tạo bằng range
   ↓    nhánh A: repartition(2)  → explain thấy Exchange
   ↓    nhánh B: coalesce(2)     → explain thấy Coalesce, KHÔNG Exchange
Output: đếm partition + đếm stage của mỗi nhánh trên UI
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("demo16")
         .master("local[4]").getOrCreate())

df = spark.range(0, 8_000_000, numPartitions=8).withColumn("v", F.rand())

a = df.repartition(2)
b = df.coalesce(2)

print("repartition:", a.rdd.getNumPartitions())   # 2
print("coalesce   :", b.rdd.getNumPartitions())   # 2
a.explain()   # có: Exchange RoundRobinPartitioning(2)
b.explain()   # có: Coalesce 2 — KHÔNG có Exchange

a.count()     # job này 2 stage (8 task + 2 task)
b.count()     # job này 1 stage (2 task — toàn bộ đọc+đếm chỉ 2 task!)

input("Mở http://localhost:4040 → Jobs: so số stage & số task 2 job cuối. Enter...")
spark.stop()
```

Cùng ra "2 partition" nhưng job A đọc bằng 8 task rồi mới gộp, job B đọc bằng đúng 2 task từ đầu — bạn vừa nhìn thấy "lan ngược upstream" bằng mắt thường.

---

## 7. Production Example

Pipeline gold layer chuẩn của một sàn TMĐT (mô hình y hệt kiến trúc kafka-flink của bạn): mỗi đêm ghi bảng `fact_order_items` partitioned theo ngày xuống Iceberg/Parquet cho Trino đọc.

```python
# ❌ Phiên bản gây sự cố (đã từng chạy thật ở nhiều công ty)
(fact.write.mode("overwrite")
     .partitionBy("dt")
     .parquet(path))
# 200 shuffle task × 90 ngày backfill = ~18.000 file 2MB → Trino list file lâu hơn query!

# ✅ Phiên bản production
target_files_per_day = 4       # ~ dung_lượng_1_ngày / 256MB
(fact
   .repartition(F.col("dt"),
                (F.crc32(F.col("order_id").cast("string")) % target_files_per_day))
   .sortWithinPartitions("dt", "seller_id")     # record cùng seller nằm gần nhau → nén + skip tốt
   .write.mode("overwrite")
   .partitionBy("dt")
   .parquet(path))
# 90 ngày × 4 file ~256MB — Trino scan nhanh, metadata gọn
```

Tại sao doanh nghiệp quan tâm đến từng dòng này:

1. **`repartition(dt, khóa_phụ)`**: gom mỗi ngày về đúng 4 task → 4 file/ngày, không nhiều không ít. Khóa phụ crc32 chia đều trong ngày, tránh 1 task ôm nguyên ngày Black Friday (mầm skew — lesson 19).
2. **`sortWithinPartitions`**: sắp trong file để Parquet nén tốt hơn (cột lặp giá trị) và min/max statistics hữu dụng → engine skip được row group.
3. Số file kiểm soát được → job compaction (lesson 32) nhẹ, catalog metadata gọn, cost storage/API giảm — trên S3, mỗi lần LIST/GET hàng chục nghìn file bé là tiền thật.

---

## 8. Hands-on Lab

**Mục tiêu**: chứng kiến bẫy `coalesce(1)` bằng đồng hồ, và luyện pattern `repartition(col)` trước `partitionBy`.

### Bước 0 — bật cluster

```bash
make up
```

### Bước 1 — viết `labs/lab16/coalesce_trap.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab16-coalesce-trap")
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)

# Thổi phồng + một transform "nặng CPU" (sha2 lặp) để thấy rõ mất parallelism
big = (items.withColumn("n", F.explode(F.sequence(F.lit(1), F.lit(30))))
            .repartition(8)                       # đảm bảo 8 partition đầu vào
            .withColumn("h", F.sha2(F.concat_ws("-", "order_id", "n"), 256)))

def timed(name, df_out, path):
    t0 = time.time()
    df_out.write.mode("overwrite").parquet(path)
    print(f"{name:<16} {time.time() - t0:6.1f}s")

timed("baseline(8)",   big,                 "/workspace/data/olist/output/lab16_base")
timed("coalesce(1)",   big.coalesce(1),     "/workspace/data/olist/output/lab16_coal")
timed("repartition(1)", big.repartition(1), "/workspace/data/olist/output/lab16_repa")

input(">>> Mở http://localhost:4040 → Jobs: so số task từng stage của 3 job ghi. Enter.")
spark.stop()
```

```bash
make run F=labs/lab16/coalesce_trap.py
```

Dự đoán TRƯỚC khi chạy (ghi ra giấy): job nào chậm nhất? `coalesce(1)` hay `repartition(1)`? Lưu ý cluster của ta chỉ có 1 core — parallelism thật là 1, nhưng bạn vẫn thấy khác biệt về **cấu trúc stage/task** trên UI; muốn thấy khác biệt thời gian rõ rệt, chạy thêm bằng `make run-local F=...` (local[2] — 2 core).

### Bước 2 — viết `labs/lab16/write_partitioned.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab16-write-partitioned")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())

orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
orders = orders.withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))

# Cách 1 — ngây thơ: không gom trước khi ghi
(orders.repartition(16)                       # giả lập df ra khỏi 1 shuffle 16 partition
       .write.mode("overwrite").partitionBy("month")
       .parquet("/workspace/data/olist/output/lab16_naive"))

# Cách 2 — production: gom theo month trước
(orders.repartition(F.col("month"))
       .write.mode("overwrite").partitionBy("month")
       .parquet("/workspace/data/olist/output/lab16_smart"))

spark.stop()
```

### Bước 3 — đếm file

```bash
make run F=labs/lab16/write_partitioned.py
echo "naive:"; find data/olist/output/lab16_naive -name "*.parquet" | wc -l
echo "smart:"; find data/olist/output/lab16_smart -name "*.parquet" | wc -l
find data/olist/output/lab16_naive/month=2018-01 -name "*.parquet" | wc -l   # file/1 tháng?
```

### Bước 4 — ghi nhận vào `labs/lab16/NOTES.md`

1. Số file naive vs smart, số file trong 1 thư mục tháng của mỗi cách.
2. Từ bước 1: số task của stage ghi và stage transform trong 3 biến thể — biến thể nào làm transform mất parallelism?
3. Một câu kết luận của riêng bạn: "quy tắc chọn repartition/coalesce khi ghi của tôi là ..."

---

## 9. Assignment

**Easy** — Lọc sớm để giảm shuffle: viết 2 phiên bản tính doanh thu theo tháng chỉ cho đơn `delivered`: (a) join items với FULL orders rồi mới filter; (b) filter orders trước rồi join. So sánh Shuffle Write trên UI của 2 bản. Liên hệ: partition/predicate pruning giúp gì thêm nếu bảng nguồn là Parquet partitioned theo status?

**Medium** — 1000 → 100: bạn có DataFrame 1000 partition (kết quả một shuffle trước đó, phân bố tương đối đều) và cần 100 partition trước bước xử lý tiếp theo. Dùng `repartition(100)` hay `coalesce(100)`? Trả lời cho 3 biến thể: (a) phân bố đều, bước sau nhẹ; (b) phân bố lệch nặng; (c) bước sau là một aggregation nặng cần cân bằng tải. Với mỗi biến thể, nêu lệnh chọn + 1 câu lý do. Viết code kiểm chứng biến thể (a) trên dữ liệu Olist thổi phồng.

**Hard** — Thiết kế partitioning cho bảng Iceberg 100 GB `fact_order_items` (grain: order item, truy vấn chủ đạo: theo khoảng ngày + theo seller): (a) chọn cột partition thư mục và giải thích tại sao KHÔNG partition theo `seller_id` (gợi ý: cardinality ~3.000 seller → nghìn thư mục bé); (b) tính số file/ngày nếu 100 GB/365 ngày, file đích 256 MB; (c) viết câu `repartition(...) + sortWithinPartitions(...) + write` hoàn chỉnh; (d) điều gì xảy ra với thiết kế của bạn vào ngày sale doanh số gấp 20 lần?

**Production Challenge** — Săn `coalesce(1)` trong tự nhiên: tìm trên GitHub/StackOverflow 2 đoạn code thật dùng `coalesce(1)` hoặc `repartition(1)` để "xuất 1 file CSV". Với từng đoạn: chẩn đoán nó có dính bẫy lan ngược không (dựa vào vị trí trong pipeline), đề xuất bản sửa, và viết 5 dòng "guideline xuất file" cho team của bạn (khi nào chấp nhận 1 file, khi nào phải từ chối yêu cầu "cho em 1 file Excel" của business).

> Nộp bài bằng cách paste code + số liệu + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Chi phí | Khi nào đáng |
|---|---|---|
| `coalesce(n)` | ~0 (narrow) | Gộp cuối pipeline, transform trước nhẹ |
| `repartition(n)` | 1 full shuffle | Cân bằng lại tải, tăng partition, cứu spill |
| `repartition(col)` trước partitionBy | 1 full shuffle | GẦN NHƯ LUÔN đáng khi ghi bảng partitioned |
| `repartitionByRange` | 1 shuffle + 1 job sample | Bảng phục vụ range query |
| `coalesce(1)` giữa pipeline | Cả job co về 1 task | Gần như không bao giờ |

Ba con số cần nhớ:

1. **Task lý tưởng nhai 100–200 MB** (sau giải nén). Số partition = tổng dữ liệu / 128MB, làm tròn lên bội của tổng core.
2. **File output lý tưởng 128 MB – 1 GB.** Số file = dung lượng / 256MB.
3. **Partition thư mục (partitionBy) lý tưởng: cardinality thấp–vừa** (ngày, tháng, quốc gia) — nghìn giá trị trở lên thì cân nhắc bucketing/hidden partitioning (lesson 33).

Và một phản xạ: sau mọi `filter` bỏ >80% dữ liệu giữa pipeline dài, hỏi "số partition giờ còn hợp lý không?" — 1000 partition mà 900 cái gần rỗng là 900 task overhead.

---

## 11. Spark UI

Bài này luyện đọc **số task per stage** — dấu vân tay của repartition/coalesce:

**Tab Jobs → 1 job → DAG Visualization:**
- `repartition` hiện thành ranh giới 2 stage (khối Exchange). `coalesce` KHÔNG tạo ranh giới — nó "tàng hình" trong stage.
- Nghi dính bẫy coalesce? So **số task của stage** với số partition bạn kỳ vọng: stage lẽ ra 100 task mà chỉ thấy 1 → coalesce đã lan ngược.

**Tab Stages:**
- Cột Tasks: `8/8` `2/2`... đối chiếu với `getNumPartitions()` bạn in ra.
- Vào stage ghi file: **Input Size / Records per task** — task ghi có đều nhau không? Lệch nặng sau `repartition(col)` = cột partition bị skew (một ngày/một khách khổng lồ) → lesson 19.

**Tab SQL/DataFrame** (Spark 3): click query → xem plan đồ họa — node `Exchange` và `Coalesce` hiện tường minh kèm số row đi qua. Đây là cách nhanh nhất kiểm tra "job tôi có mấy shuffle, coalesce nằm stage nào".

---

## 12. Common Mistakes

1. **`coalesce(1)` để "ra 1 file cho gọn"** ngay sau chuỗi transform nặng → cả job chạy 1 task. Nếu thật sự cần 1 file: `repartition(1)`, hoặc tốt hơn — hỏi lại tại sao downstream cần đúng 1 file.
2. **Quên `repartition(col)` trước `write.partitionBy(col)`** → số file = số task × số giá trị partition, đẻ ra nghìn file bé + OOM âm thầm do mỗi task mở hàng chục Parquet writer.
3. **Dùng `repartition` khi `coalesce` đủ** — trả giá nguyên một shuffle (serialize + disk + network) chỉ để giảm số file cuối job trong khi dữ liệu đã phân bố ổn.
4. **Tưởng `coalesce(500)` tăng được partition** từ 100 lên 500 — lệnh bị lờ đi im lặng, không lỗi, không cảnh báo; job vẫn 100 task và bạn không hiểu vì sao "tăng partition rồi mà vẫn chậm".
5. **`repartition("dt")` rồi đinh ninh mỗi dt một partition** — thực tế hash vào `spark.sql.shuffle.partitions` rổ; và 2 giá trị dt có thể hash VÀO CÙNG 1 rổ → 1 task ghi 2 ngày. Muốn kiểm soát chặt: chỉ định n + khóa phụ.
6. **Chỉnh partition mà không đo lại** — mọi thay đổi repartition/coalesce phải kèm 3 con số trước/sau: thời gian job, số file output, spill. Không số liệu = không phải tuning, là cầu may.

---

## 13. Interview

**Junior:**

1. *Khác biệt cốt lõi giữa repartition và coalesce?* — `repartition` gây full shuffle, chia lại dữ liệu đều (round-robin/hash), tăng hoặc giảm được số partition. `coalesce` gộp partition cùng executor, không shuffle, chỉ giảm được, phân bố có thể lệch.
2. *Tại sao coalesce nhanh hơn repartition?* — Vì là narrow transformation: partition mới chỉ là "chồng ghép" các partition cha trên cùng executor, không serialize, không ghi shuffle file, không network. Repartition trả đủ 4 khâu phí của shuffle.
3. *Muốn tăng 100 partition lên 400 thì dùng gì? Tại sao không dùng coalesce?* — `repartition(400)`. Coalesce chỉ gộp (narrow dependency không thể "tách" 1 partition cha ra nhiều partition con); gọi `coalesce(400)` sẽ bị bỏ qua im lặng.
4. *Ghi DataFrame ra ít file hơn thì làm thế nào?* — Cuối pipeline dùng `coalesce(n)` (rẻ) hoặc `repartition(n)` (khi cần giữ parallelism phía trước hoặc cần chia đều); với bảng partitioned thì `repartition(col_partition, ...)` trước `partitionBy`.

**Mid:**

5. *Giải thích tại sao `df.filter(nặng).coalesce(1).write...` chậm hơn hẳn `df.filter(nặng).repartition(1).write...` dù coalesce "rẻ hơn"?* — Coalesce là narrow nên không cắt stage: nó kéo số task của CẢ stage (gồm cả filter) xuống 1 → mất toàn bộ parallelism upstream. Repartition(1) chèn shuffle: stage filter vẫn chạy N task song song, chỉ stage ghi là 1 task. Chi phí 1 shuffle nhỏ hơn nhiều chi phí mất parallelism.
6. *`df.repartition("date")` với 30 ngày dữ liệu cho ra bao nhiêu partition?* — Bằng `spark.sql.shuffle.partitions` (mặc định 200), trong đó tối đa 30 partition có dữ liệu, còn lại rỗng; và có thể ít hơn 30 partition có data nếu 2 ngày hash trùng rổ. Muốn kiểm soát: `repartition(30, "date")` hoặc thêm khóa phụ.
7. *Trước `write.partitionBy("dt")` vì sao nên `repartition(F.col("dt"))`? Rủi ro còn lại là gì?* — Để record cùng dt về cùng task → mỗi thư mục dt ra đúng 1 file thay vì (số task) file. Rủi ro: dt lớn (ngày sale) dồn vào 1 task → task khổng lồ + file khổng lồ → thêm khóa phụ (hash % k) để tách mỗi dt ra k file, hoặc dùng maxRecordsPerFile chặn trần.
8. *repartitionByRange khác repartition(col) chỗ nào, khi nào chọn?* — repartition(col) dùng hash: cùng key cùng partition, nhưng các partition không có thứ tự. repartitionByRange sample dữ liệu rồi chia theo KHOẢNG giá trị → output có thứ tự toàn cục theo khoảng, file nén tốt và min/max statistics hiệu quả → chọn khi ghi bảng phục vụ range query (thời gian, id tăng dần). Giá: thêm 1 job sample.

**Senior:**

9. *Thiết kế bước ghi cho bảng fact 500 GB/ngày, partitionBy dt, đích file 512 MB, có ngày sale gấp 10. Trình bày.* — Số file thường: 500GB/512MB ≈ 1000 file/ngày → `repartition(F.col("dt"), hash(key) % 1000)` + `sortWithinPartitions` theo cột hay lọc; `maxRecordsPerFile` làm lưới an toàn chặn file quá to. Ngày sale: k=1000 tĩnh sẽ cho file 5GB → hoặc tính k động theo dung lượng ước tính từng dt (adaptive: đếm trước rồi đặt k), hoặc để AQE + maxRecordsPerFile cắt bớt, và giám sát kích thước file như một metric của pipeline. Nêu được "k phải động theo dữ liệu" là điểm ăn tiền senior.
10. *AQE (Spark 3) tự coalesce shuffle partition — vậy còn cần repartition/coalesce thủ công không?* — Vẫn cần. AQE chỉ gom shuffle partition NHỎ sau một shuffle sẵn có, giải quyết "200 partition mặc định quá nhiều"; nó không (a) gom dữ liệu theo cột trước partitionBy, (b) không thay repartitionByRange, (c) không cứu bẫy coalesce(1), (d) không tăng partition khi task quá to trước một phép nặng do bạn chủ đích. Câu trả lời thể hiện hiểu ranh giới của tự động hóa — đúng chất senior.

---

## 14. Summary

### Mindmap

```
                 PARTITIONING CHIẾN LƯỢC (L16)
                            │
    ┌──────────────┬────────┴────────┬─────────────────────┐
    ▼              ▼                 ▼                     ▼
 REPARTITION    COALESCE         GHI BẢNG              QUY TẮC SỐ
    │              │                 │                     │
 full shuffle   narrow, ~0 phí    repartition(col)      task ~128MB
 đều tăm tắp    chỉ GIẢM          trước partitionBy     file 128MB–1GB
 tăng/giảm      có thể lệch       ± khóa phụ % k        partition thư mục:
 (n) round-robin BẪY: coalesce(1) sortWithinPartitions  cardinality thấp
 (col) hash     lan ngược →       maxRecordsPerFile     đo lại sau mỗi
 ByRange: sample cả stage 1 task  = chặn trần           thay đổi!
```

### Checklist trước khi gõ "Continue"

- [ ] Nói được 4 khác biệt repartition vs coalesce (shuffle, tăng/giảm, phân bố, chi phí) không nhìn tài liệu.
- [ ] Giải thích được cơ chế bẫy `coalesce(1)` lan ngược bằng khái niệm narrow dependency + stage.
- [ ] Đã chạy lab: thấy job coalesce(1) chỉ 1 task cho cả stage transform trên UI.
- [ ] Viết được pattern `repartition(F.col("dt"), khóa_phụ)` trước `partitionBy` và giải thích từng phần.
- [ ] Thuộc 3 con số: task ~128MB, file 128MB–1GB, partitionBy cần cardinality thấp.
- [ ] Biết `repartition("dt")` KHÔNG cho "mỗi dt một partition".
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 17 — Spark Memory Model: unified memory, execution vs storage.**

Hai bài liền bạn nghe đi nghe lại "execution memory hết thì spill", "task to quá thì OOM" — nhưng execution memory chính xác là bao nhiêu MB, nằm chỗ nào trong heap, ai chia cho ai? Lesson 17 mở nắp hộp đen: heap = reserved + unified (execution/storage mượn qua mượn lại) + user memory, cộng thêm overhead ngoài heap mà YARN/K8s dùng để "xử tử" container của bạn. Học xong bạn sẽ tính được chính xác mỗi task trên cluster 1G của ta có bao nhiêu MB để làm việc — và hiểu tại sao tăng shuffle partitions nhiều khi cứu OOM tốt hơn tăng RAM.

Đây là bài lý thuyết "nặng đô" nhất Module 3 — nhưng cluster 1GB của chúng ta là phòng thí nghiệm hoàn hảo để nhìn OOM tận mắt mà không tốn một xu cloud.

> Gõ **"Continue"** khi sẵn sàng.
