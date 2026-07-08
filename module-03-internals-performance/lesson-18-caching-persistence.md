# Lesson 18 — Caching & persistence: khi nào cache giúp

> Module 3 · Internals & Performance Tuning · Tuần 9 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- `cache()` và `persist()` thực chất làm gì — và sự thật hay bị viết sai: với DataFrame trên Spark 3, **`cache()` = `persist(MEMORY_AND_DISK)`**, không phải MEMORY_ONLY.
- Bảng so sánh các **StorageLevel**: MEMORY_ONLY / MEMORY_AND_DISK / DISK_ONLY / các bản `_SER` / các bản `_2`.
- **Lazy caching**: `cache()` không cache gì cả — action đầu tiên mới materialize, và materialize TỪNG PHẦN.
- Đọc **Storage tab** trên Spark UI — đặc biệt cột **Fraction Cached** (cache nửa vời = hiểm họa âm thầm).
- Khung quyết định: khi cache **giúp** (dùng lại ≥2 lần, iterative) vs khi cache **hại** (dùng 1 lần, chiếm memory gây spill/eviction — nối thẳng vào memory model lesson 17).
- So găng ba anh em: **cache vs checkpoint vs ghi Parquet trung gian**.

Sau bài này bạn phải làm được:

- Quyết định có cache hay không cho một DataFrame bất kỳ trong 30 giây, kèm lý do bằng số (số lần dùng lại × chi phí tính lại vs chi phí memory).
- Nhìn Storage tab nói được: cache chiếm bao nhiêu, nằm memory hay disk, có bị cache nửa vời không.
- Giải thích tại sao thêm một dòng `cache()` có thể làm job CHẬM ĐI — bằng ngôn ngữ unified memory.

Kiến thức dùng trong thực tế: `cache()` là API bị lạm dụng nhất Spark. Code review nào cũng gặp cache rắc như muối — người biết đặt (và biết XÓA) cache đúng chỗ tiết kiệm hàng giờ compute mỗi ngày.

---

## 2. Why

### Lazy evaluation có một hóa đơn giấu kín

Từ lesson 2 bạn biết: DataFrame không chứa dữ liệu, nó chứa **công thức** (lineage). Mỗi action chạy lại công thức từ đầu:

```python
silver = (spark.read.csv(...)          # đọc 50GB
          .filter(...).join(...)       # làm sạch + join — 20 phút compute
          .withColumn(...))

gold_1 = silver.groupBy("seller_id").agg(...)   # action → chạy 20 phút
gold_2 = silver.groupBy("month").agg(...)       # action → chạy LẠI 20 phút
gold_3 = silver.groupBy("state").agg(...)       # action → chạy LẠI 20 phút nữa
# Tổng: 60 phút, trong đó 40 phút là TÍNH LẠI thứ đã tính
```

Ba nhánh cùng mọc từ `silver` — không cache thì thân cây bị trồng lại ba lần. `silver.cache()` biến 60 phút thành ~22 phút. Đó là mặt GIÚP.

### Nhưng cache là con dao hai lưỡi cắm thẳng vào lesson 17

Nhớ ao unified: **cache sống trong storage memory — cùng ao với execution**. Cache một bảng to trong khi job còn shuffle nặng nghĩa là bạn lấy bàn làm việc ra chứa hồ sơ:

```
Không cache:  execution dùng cả ao → sort/join thoải mái, không spill
Cache 70% ao: execution còn 30% + đuổi dần cache (nếu đuổi được)
              → spill tăng, cache bị evict từng phần → vừa chậm vừa chẳng còn cache
```

Và vì eviction âm thầm (không lỗi, không cảnh báo — chỉ một dòng INFO trong log), job "lúc nhanh lúc chậm" mà không ai hiểu tại sao. Cache sai chỗ không phải vô hại — nó **trả tiền memory thật để mua một món đồ có thể đã bị vứt đi**.

### Analogy: nồi nước dùng

Quán phở nấu nước dùng 6 tiếng (DataFrame `silver` đắt đỏ):

- Bán 200 bát/ngày từ cùng nồi → **giữ nồi trên bếp** (cache) là hiển nhiên.
- Món chỉ bán 1 bát/ngày → giữ nguyên một nồi chiếm bếp cả ngày (memory) là dại — nấu lúc cần (recompute) hoặc **trữ đông** (ghi Parquet trung gian: chậm lấy ra hơn, nhưng không chiếm bếp và mất điện không hỏng).
- Bếp chật (executor memory bé) mà cứ tham giữ nhiều nồi → không còn chỗ nấu món mới (execution) → mọi món đều chậm.

### Trade-off (Senior phải thuộc)

| Được | Mất |
|---|---|
| Nhánh dùng lại không phải tính lại từ lineage | Chiếm storage memory — giành ao với execution (spill!) |
| Nhanh hơn đọc lại nguồn (nhất là nguồn chậm: JDBC, CSV) | Materialize lần đầu TỐN THÊM thời gian + memory |
| Che chắn nguồn không ổn định trong phiên interactive | Có thể bị evict âm thầm → hiệu quả không đảm bảo |
| Đơn giản, một dòng code | KHÔNG cắt lineage (checkpoint mới cắt), không sống qua application |

---

## 3. Theory

### 3.1. `cache()` / `persist()` — hợp đồng chính xác

```python
df.cache()                          # = df.persist() = persist(MEMORY_AND_DISK) với DataFrame
df.persist(StorageLevel.DISK_ONLY)  # chọn level tường minh
df.unpersist()                      # trả lại memory (nhớ gọi! không có "hết hạn tự động")
```

Ba điều khoản trong hợp đồng:

1. **LƯỜI**: `cache()` chỉ ĐÁNH DẤU "kết quả của plan này đáng giữ lại". Chưa có byte nào được cache cho đến **action đầu tiên** chạy qua nó. Vì thế `df.cache()` xong `df.count()` là idiom "ép materialize ngay" (dùng có chủ đích, đừng rải bừa).
2. **Đơn vị là partition**, không phải bảng: action đầu chỉ đụng 3/100 partition (ví dụ `show()`)? Chỉ 3 partition được cache → sinh ra **Fraction Cached 3%** trên Storage tab. Memory hết giữa chừng? Cache được một phần, phần còn lại tính lại mỗi lần dùng — **cache nửa vời**.
3. **DataFrame khác RDD**: `rdd.cache()` = MEMORY_ONLY (hết memory là bỏ phần thừa), còn `df.cache()` = **MEMORY_AND_DISK** (hết memory thì tràn xuống disk). Câu "cache mặc định MEMORY_ONLY" đúng với RDD và SAI với DataFrame — bẫy phỏng vấn kinh điển. Ngoài ra dữ liệu DataFrame được cache ở dạng **cột, nén** (columnar batches) nên thường nhỏ hơn bạn nghĩ.

### 3.2. Bảng StorageLevel

| StorageLevel | Nằm đâu | Serialize? | Hết chỗ thì | Dùng khi |
|---|---|---|---|---|
| `MEMORY_ONLY` | RAM | Không (object)* | **Bỏ partition thừa** → tính lại mỗi lần dùng | Data bé chắc chắn vừa RAM, cần nhanh nhất |
| `MEMORY_AND_DISK` ⭐ mặc định DF | RAM, tràn → disk | Không*/disk có | Tràn disk, không mất | Mặc định hợp lý cho hầu hết ca |
| `DISK_ONLY` | Disk local | Có | — | Bảng to, tính lại RẤT đắt, RAM cần cho execution |
| `MEMORY_ONLY_SER` (RDD) | RAM | Có (bytes) | Bỏ phần thừa | Tiết kiệm RAM 2–5×, trả CPU deserialize |
| `MEMORY_AND_DISK_SER` (RDD) | RAM+disk | Có | Tràn disk | Như trên, an toàn hơn |
| `..._2` (vd MEMORY_AND_DISK_2) | 2 bản trên 2 executor | tùy | — | Streaming/SLA cao: executor chết không phải tính lại. Trả giá ×2 memory + network |
| `OFF_HEAP` | Off-heap (Tungsten) | Có | — | Nâng cao: né GC, cần bật offHeap (lesson 17) |

\* Với DataFrame, "deserialized" thực tế là columnar batch nén — vốn đã gọn; các bản `_SER` chủ yếu có ý nghĩa với RDD. Chọn nhanh: **mặc định cứ MEMORY_AND_DISK; tính-lại-rẻ và thiếu RAM → đừng cache; tính-lại-đắt và RAM quý → DISK_ONLY.**

### 3.3. Cache giúp khi nào — công thức 1 dòng

```
CACHE ĐÁNG GIÁ  ⇔  (số lần dùng lại − 1) × chi_phí_tính_lại  >  chi_phí_cache
                                                                (materialize + memory bị chiếm
                                                                 + spill/eviction gây ra cho phần còn lại)
```

Các ca **GIÚP** rõ rệt:

- DataFrame được rẽ **≥2 nhánh** action (ví dụ silver → nhiều bảng gold).
- **Iterative**: ML training, thuật toán đồ thị, vòng lặp while đọc cùng df.
- Khám phá **interactive** (notebook): làm sạch một lần, query đi query lại.
- Nguồn đọc lại đắt/không ổn định: JDBC production, API, CSV khổng lồ + parse phức tạp.

Các ca **HẠI** rõ rệt:

- **Dùng đúng 1 lần** — cache chỉ tốn thêm materialize + memory, không thu về gì (lỗi phổ biến số 1).
- Bảng cache to chiếm ao unified trong khi pipeline còn shuffle/sort nặng → execution thiếu đất → spill (lesson 17). *Cache một thứ ít dùng có thể làm chậm mọi thứ khác.*
- Cache TRƯỚC filter/select — cache 50GB thô trong khi phía sau chỉ dùng 2GB đã lọc. Cache điểm HẸP nhất của pipeline (sau lọc, trước rẽ nhánh).
- Dữ liệu dùng lại giữa **các application khác nhau** — cache chết theo application; ca này là đất của Parquet trung gian (3.4).

### 3.4. Cache vs checkpoint vs ghi Parquet trung gian

| | `cache()/persist()` | `checkpoint()` | Ghi Parquet/Iceberg trung gian |
|---|---|---|---|
| Nằm đâu | Memory/disk local của executor | Thư mục checkpoint (HDFS/S3) | Storage chính thức |
| Cắt lineage? | **Không** — plan vẫn dài, chỉ đánh dấu tái sử dụng | **Có** — plan mới bắt đầu từ checkpoint | Có — đọc lại là plan mới |
| Sống qua application? | Không | Không (mục đích chính là trong-app) | **Có** — job khác, engine khác (Trino!) đọc được |
| Executor chết | Mất phần đó (tính lại từ lineage) | Còn (trên reliable storage) | Còn |
| Chi phí | Thấp nhất | Ghi + **tính 2 lần** (trừ khi cache trước khi checkpoint) | Ghi + đọc lại, thêm bước quản lý |
| Dùng khi | Tái sử dụng trong 1 job | Lineage quá dài/đệ quy (vòng lặp dài, streaming state) làm planner nghẹt hoặc stack overflow | Ranh giới bronze/silver/gold; debug; chia sẻ giữa pipeline |

> Kinh nghiệm production: trong pipeline batch nhiều tầng, **ghi bảng trung gian thắng cache** ở đa số ranh giới lớn — vì nó vừa là "cache bền" vừa là điểm restart khi job fail nửa chừng. Cache dành cho tái sử dụng NGẮN HẠN bên trong một job. `localCheckpoint()` là bản lai (cắt lineage, lưu trên executor — nhanh nhưng không bền), tiện cho notebook.

### 3.5. `unpersist()` — nửa còn lại của kỹ năng

Cache không tự hết hạn. Pipeline dài mà không `unpersist()` thì các bảng cache cũ ngồi chiếm ao unified đến hết application (Spark chỉ evict khi bị ép — và evict theo LRU, chưa chắc đúng bảng bạn muốn bỏ):

```python
silver.cache()
gold_1 = build_gold_1(silver); gold_1.write.parquet(...)
gold_2 = build_gold_2(silver); gold_2.write.parquet(...)
silver.unpersist()          # ← xong việc là trả đất NGAY cho execution
```

Quy tắc: **ai cache người đó unpersist** — cache trong hàm thì unpersist trước khi return (hoặc ghi rõ hợp đồng cho caller).

---

## 4. Internal

Chuyện gì xảy ra dưới nắp capo khi bạn gọi `df.cache()` rồi chạy action:

```
① df.cache() → CacheManager (trên driver) ghi sổ:
   "logical plan P ↔ InMemoryRelation" (chưa chạy gì)
        │
② Action tới → Catalyst thay mọi chỗ khớp plan P bằng InMemoryRelation
   → physical plan xuất hiện node InMemoryTableScan (thấy được qua explain()!)
        │
③ Lần chạy ĐẦU: task tính partition như thường → đưa các row vào
   columnar batch (nén per-column) → BlockManager cất block:
   • MEMORY_AND_DISK: thử MemoryStore (storage pool của unified memory);
     thiếu chỗ → xin đuổi block LRU khác; vẫn thiếu → ghi DiskStore
   • block được đăng ký với driver (BlockManagerMaster) để lần sau tìm thấy
        │
④ Lần chạy SAU: task cần partition → hỏi BlockManager
   • có local → đọc thẳng (nhanh nhất)
   • có trên executor khác → kéo qua network (vẫn rẻ hơn tính lại, thường thế)
   • đã bị evict/mất executor → TÍNH LẠI từ lineage (vì thế cache không cắt lineage!)
```

Chi tiết đáng giá:

- **Khớp plan, không khớp biến**: `df.filter(x > 5)` và một DataFrame khác build ra plan y hệt sẽ CÙNG hưởng cache (CacheManager so sánh plan). Ngược lại, đổi một chút plan (thêm cột) là KHÔNG khớp — tưởng có cache mà không dùng được.
- **Eviction là per-block, LRU, âm thầm** — log executor hiện `INFO MemoryStore: ... dropped block rdd_42_7 from memory`. Đây là nơi "Fraction Cached 63%" ra đời.
- Cache DataFrame dạng columnar + nén (điều khiển bởi `spark.sql.inMemoryColumnarStorage.compressed=true`, `batchSize=10000`) — nên **Size in Memory trên Storage tab thường NHỎ hơn dữ liệu gốc**, đừng ngạc nhiên.
- Vì sao cache không cắt lineage còn checkpoint cắt: cache là tối ưu *cơ hội* (mất thì tính lại — cần giữ công thức), checkpoint là *cam kết* (dữ liệu đã nằm nơi bền vững — công thức cũ vứt được). Hiểu câu này là hiểu cả bài.

---

## 5. API

### `df.cache()` / `df.persist(level)`

```python
from pyspark.storagelevel import StorageLevel

df.cache()                                    # MEMORY_AND_DISK (DataFrame, Spark 3)
df.persist(StorageLevel.DISK_ONLY)            # chọn level
df.persist(StorageLevel.MEMORY_AND_DISK_2)    # 2 bản sao — cho SLA cao
```
- **Pitfall**: gọi `persist` ĐỔI level trên df đã cache → `Cannot change storage level ... after it was already assigned` — phải `unpersist()` trước.

### `df.unpersist(blocking=False)`

```python
df.unpersist()              # async — thường đủ
df.unpersist(blocking=True) # chờ xóa xong — dùng trong benchmark để đo sạch
```

### `df.count()` sau cache — ép materialize toàn phần

```python
df.cache()
df.count()      # materialize 100% partition (show() chỉ vài partition!)
```
- **Pitfall**: materialize bằng `show()` rồi thắc mắc Fraction Cached 5%.

### `spark.catalog` — dọn dẹp & kiểm tra

```python
spark.catalog.clearCache()          # xóa MỌI cache của session — dùng khi benchmark
spark.catalog.isCached("my_view")   # với temp view đã cache qua spark.sql("CACHE TABLE ...")
```

### `df.checkpoint()` / `df.localCheckpoint()`

```python
spark.sparkContext.setCheckpointDir("/workspace/data/olist/output/_checkpoints")
df2 = df.checkpoint()        # GHI CHÚ: trả về DataFrame MỚI — phải hứng biến!
df3 = df.localCheckpoint()   # cắt lineage, lưu trên executor (nhanh, không bền)
```
- **Pitfall**: gọi `df.checkpoint()` mà không gán `df = df.checkpoint()` → vô dụng, lineage cũ vẫn nguyên.

### `explain()` — bằng chứng cache được dùng

```python
df_cached.groupBy("x").count().explain()
# tìm: InMemoryTableScan / InMemoryRelation [storageLevel=Disk Memory Deserialized...]
```

---

## 6. Demo nhỏ

```
Input:  DataFrame có transform "đắt" giả lập
   ↓    đo 2 action KHÔNG cache (tính lại 2 lần)
   ↓    cache + materialize, đo lại 2 action
Output: chênh lệch thời gian + bằng chứng InMemoryTableScan
```

```python
import time
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("demo18").master("local[2]").getOrCreate()

# sha2 lặp 3 lần = transform đắt giả lập
df = (spark.range(0, 2_000_000)
      .withColumn("h", F.sha2(F.col("id").cast("string"), 512))
      .withColumn("h", F.sha2("h", 512))
      .withColumn("h", F.sha2("h", 512)))

def timed(label, fn):
    t0 = time.time(); fn(); print(f"{label:<28}{time.time()-t0:6.2f}s")

timed("no-cache: action 1", lambda: df.count())
timed("no-cache: action 2", lambda: df.filter(F.col("h").startswith("a")).count())

df.cache()
timed("cache: materialize (count)", lambda: df.count())          # trả phí 1 lần
timed("cache: action 2", lambda: df.filter(F.col("h").startswith("a")).count())  # gặt hái

df.filter(F.col("h").startswith("a")).explain()   # tìm InMemoryTableScan
input(">>> Mở http://localhost:4040 → Storage: xem Size in Memory, Fraction Cached. Enter...")
df.unpersist()
spark.stop()
```

Đọc kết quả: action 2 sau cache nhanh hơn nhiều lần vì sha2 ×3 không phải tính lại. Đồng thời để ý: "materialize" CHẬM HƠN action 1 no-cache một chút — đó là phí xây cache. Dùng 1 lần thì phí này không bao giờ hoàn vốn.

---

## 7. Production Example

Pipeline gold layer thật (đúng mô-hình Project 1 của bạn): từ `silver_order_items` (đã join + làm sạch, đắt) rẽ ra 4 bảng gold — doanh thu theo seller, theo tháng, theo bang, và bảng RFM khách hàng.

```python
silver = build_silver(spark)          # join 4 bảng + dedup + QC — 15 phút compute

# Quyết định cache — đi qua checklist:
# 1. Dùng lại mấy lần? 4 nhánh → CÓ, vượt ngưỡng ≥2.
# 2. Cache ở điểm hẹp chưa? silver đã lọc/chọn cột — ~3GB. OK.
# 3. Cluster còn ao không? executors 10×4GB, unified ~22GB, shuffle các nhánh nhẹ. OK.
# 4. Level? Bảng 3GB, tính lại 15 phút → MEMORY_AND_DISK (mặc định) là đúng.
silver = silver.cache()
silver.count()                        # materialize 1 lần, có kiểm soát

for name, builder in gold_builders.items():        # 4 nhánh
    builder(silver).write.mode("overwrite").parquet(f"{base}/{name}")

silver.unpersist()                    # trả ao trước phần việc sau
```

Vì sao đội này KHÔNG cache mà GHI bảng silver trung gian trong bản chạy đêm chính thức: bản scheduled cần **điểm restart** (job fail ở gold thứ 3 lúc 3h sáng → chạy lại chỉ phần gold, đọc silver từ Parquet, không nấu lại 15 phút) và silver còn được team khác + Trino dùng. Họ chỉ dùng `cache()` như trên trong bản **backfill/notebook** — chạy nhiều biến thể gold liên tiếp trong một session. Một quyết định — hai ngữ cảnh — hai công cụ khác nhau: đó chính là tư duy senior của bài này.

---

## 8. Hands-on Lab

**Mục tiêu**: đo lợi ích cache trên pipeline nhiều nhánh; sau đó dùng executor 512MB để tận mắt thấy **cache nửa vời (Fraction Cached < 100%)** và **cache gây spill**.

### Bước 0

```bash
make up
```

### Bước 1 — `labs/lab18/cache_benefit.py`: cache GIÚP

```python
import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab18-cache-benefit").getOrCreate())

orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)

# "silver" đắt: join + explode ×20 + hash
silver = (orders.filter(F.col("order_status") == "delivered")
          .join(items, "order_id")
          .withColumn("n", F.explode(F.sequence(F.lit(1), F.lit(20))))
          .withColumn("h", F.sha2(F.concat_ws("-", "order_id", "n"), 256))
          .withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM")))

def five_actions(df, label):
    t0 = time.time()
    df.groupBy("month").count().collect()
    df.groupBy("seller_id").agg(F.sum("price")).count()
    df.filter(F.col("price") > 100).count()
    df.select(F.countDistinct("order_id")).collect()
    df.groupBy("product_id").count().count()
    print(f"{label:<12}{time.time()-t0:6.1f}s")

five_actions(silver, "no-cache")
silver.cache(); silver.count()          # materialize
five_actions(silver, "cached")

input(">>> UI :4040 → Storage: Size in Memory? Fraction Cached? On Disk? Enter...")
silver.unpersist()
spark.stop()
```

```bash
make run F=labs/lab18/cache_benefit.py
```

### Bước 2 — `labs/lab18/cache_harm.py`: cache HẠI (điểm hay nhất của lab)

```python
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel

spark = (SparkSession.builder.appName("lab18-cache-harm")
         .config("spark.executor.memory", "512m")        # unified ≈ 127MB — ao tí hon
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)
big = (items.withColumn("n", F.explode(F.sequence(F.lit(1), F.lit(40))))
            .withColumn("h", F.sha2(F.concat_ws("-", "order_id", "n"), 512)))

def heavy_shuffle(label):
    t0 = time.time()
    big.groupBy("order_id").agg(F.collect_list("h")).count()   # shuffle cần nhiều execution mem
    print(f"{label:<22}{time.time()-t0:6.1f}s")

heavy_shuffle("no-cache")                        # baseline

big.persist(StorageLevel.MEMORY_ONLY)            # cố nhét bảng to vào ao 127MB
big.count()                                      # materialize — chắc chắn không vừa
input(">>> UI → Storage: Fraction Cached bao nhiêu %? (cache nửa vời!) Enter để đo tiếp...")

heavy_shuffle("with MEMORY_ONLY")                # cache chiếm ao + nửa vời → dự đoán?
big.unpersist(blocking=True)

big.persist(StorageLevel.DISK_ONLY)
big.count()
heavy_shuffle("with DISK_ONLY")                  # không giành ao memory — so sánh!

input(">>> UI → Storage (On Disk) + Stages (Spill của từng lần). Enter để thoát.")
spark.stop()
```

```bash
make run F=labs/lab18/cache_harm.py
```

### Bước 3 — quan sát & ghi `labs/lab18/NOTES.md`

1. Bước 1: no-cache vs cached — nhanh hơn mấy lần? Storage tab: Size in Memory so với bạn đoán (nhớ: columnar + nén).
2. Bước 2: **Fraction Cached** của MEMORY_ONLY là bao nhiêu %? Bảng thời gian 3 lần đo — MEMORY_ONLY có khi nào CHẬM hơn no-cache không? Giải thích bằng 2 khái niệm: eviction + giành ao unified (lesson 17).
3. Một câu quy tắc của riêng bạn: "trước khi gõ .cache() tôi sẽ tự hỏi ___".

---

## 9. Assignment

**Easy** — Chọn StorageLevel (kèm 1 câu lý do mỗi ca): (a) bảng dimension 200MB dùng lại 10 lần trong job, cluster RAM dư dả; (b) DataFrame 40GB tính lại mất 30 phút, executor tổng storage chỉ ~20GB, các bước sau còn shuffle nặng; (c) lookup table trong job streaming SLA khắt khe, executor thỉnh thoảng bị thu hồi (spot instance).

**Medium** — Cache bao nhiêu là hợp lý: dựa trên lesson 17, lập luận vì sao "tổng cache nên chừa lại phần lớn ao unified cho execution ở pipeline shuffle-nặng". Với cluster 10 executor × 4g (unified ≈ 2.27GB/executor, `storageFraction=0.5`): tổng cache tối đa trước khi CHẮC CHẮN bắt đầu evict là bao nhiêu nếu execution đang cần cả phần của mình? Kiểm chứng khái niệm bằng lab bước 2 (số Fraction Cached bạn đo được so với unified 127MB).

**Hard** — Cache gây OOM/hại — hồ sơ đầy đủ: từ lab bước 2, viết phân tích 1 trang: (a) chuỗi nhân quả cache → chiếm storage → execution thiếu → spill (kèm số đo của bạn); (b) tại sao MEMORY_ONLY nửa vời có thể TỆ hơn không cache (gợi ý: partition bị bỏ phải tính lại TỪNG LẦN dùng, mà vẫn mất công materialize + giành ao); (c) thí nghiệm bổ sung: thêm nhánh thứ 2 dùng lại `big` — DISK_ONLY thắng hay thua recompute trên cluster của ta? Số liệu quyết định.

**Production Challenge** — Chính sách cache cho team: viết `caching-policy.md` nửa trang gồm: checklist 4 câu trước khi cache (dùng lại ≥2? điểm hẹp nhất? ao còn chỗ? level nào?), quy tắc unpersist (ai cache người đó dọn), khi nào PHẢI dùng bảng Parquet trung gian thay cache (scheduled job cần restart point / chia sẻ cross-app / lineage quá dài thì checkpoint), và 1 quy ước code review ("mọi PR có .cache() phải kèm comment số lần dùng lại"). So chính sách của bạn với quyết định ở mục 7.

> Nộp bài bằng cách paste code + số liệu + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Tình huống | Cache? | Ghi chú |
|---|---|---|
| Dùng lại ≥2 lần, tính lại đắt, ao còn chỗ | ✅ MEMORY_AND_DISK | Ca kinh điển — nhớ materialize bằng count() và unpersist khi xong |
| Dùng 1 lần | ❌ | Cache chỉ thêm phí materialize + chiếm ao |
| Dùng lại nhưng tính lại RẺ (đọc Parquet + filter nhẹ) | ❌ thường | Parquet đọc lại đã nhanh; cache chưa chắc hoàn vốn |
| Bảng to + pipeline còn shuffle nặng | ⚠️ DISK_ONLY hoặc ❌ | Đừng giành ao với execution |
| Notebook khám phá 30 phút trên cùng df | ✅ | Kèm clearCache() khi đổi hướng phân tích |
| Cần dùng ở application/job khác | ❌ → ghi Parquet/Iceberg | Cache chết theo app |
| Lineage dài trăm bước / vòng lặp | → checkpoint | Vấn đề là planner, không phải compute |

Con số nên nhớ:

1. Cache DataFrame là **columnar + nén** — 1GB CSV thô có thể chỉ ~200–400MB trong cache. Đo bằng Storage tab, đừng đoán.
2. Phí xây cache ≈ chạy action đầu chậm hơn **10–30%** (ghi block, nén). Hoàn vốn từ lần dùng thứ 2.
3. **Fraction Cached < 100% kéo dài** = đang trả phí cache mà nhận hàng thiếu — hoặc tăng level xuống disk, hoặc thu hẹp df, hoặc bỏ cache.

---

## 11. Spark UI

Bài này mở khóa tab **Storage** — chỉ xuất hiện khi có cache được materialize:

- Mỗi dòng = một cached DataFrame/RDD (`InMemoryRelation`). Cột quan trọng:
  - **Storage Level**: đối chiếu với code (Disk Memory Deserialized 1x Replicated = MEMORY_AND_DISK).
  - **Fraction Cached**: **cột quan trọng nhất** — < 100% nghĩa là cache nửa vời (materialize chưa đủ action, hoặc bị evict, hoặc không vừa MEMORY_ONLY). 100% mới là cache tử tế.
  - **Size in Memory / Size on Disk**: bảng nằm đâu; MEMORY_AND_DISK mà 90% on-disk → RAM không đủ, cân nhắc DISK_ONLY cho đỡ giành ao.
- Click vào tên → phân bố block theo executor: cache lệch (1 executor ôm 80% block) là mầm chậm.
- Đối chiếu chéo: **Executors tab** cột Storage Memory used tăng đúng bằng Size in Memory; **Stages tab** — sau khi cache mà job vẫn có stage đọc CSV/scan nguồn → cache KHÔNG được dùng (plan không khớp!) → kiểm tra bằng `explain()` tìm `InMemoryTableScan`.
- Sau `unpersist()`: dòng biến mất khỏi Storage tab — thói quen kiểm tra "đã trả ao chưa" sau mỗi pipeline.

---

## 12. Common Mistakes

1. **Cache thứ dùng 1 lần** — rải `.cache()` như gia vị "cho chắc". Mỗi cái là một khoản phí materialize + memory không bao giờ hoàn vốn.
2. **Cache mà không materialize có kiểm soát** — `cache()` rồi `show()` (cache vài partition) hoặc không action nào cả, rồi tin rằng "đã cache". Idiom đúng: `df.cache(); df.count()`.
3. **Không bao giờ `unpersist()`** — pipeline 10 bước cache 5 bảng, bảng cũ chiếm ao đến hết app, execution spill dài dài. Cache là vay — unpersist là trả nợ.
4. **Cache TRƯỚC filter** — cache 50GB thô để rồi mọi nhánh chỉ dùng 2GB sau lọc. Luôn cache ở điểm hẹp nhất có tái sử dụng.
5. **Tưởng cache cắt lineage / bền vững** — executor chết là partition cache đó tính lại từ đầu; application tắt là mất sạch. Cần bền → checkpoint hoặc ghi bảng.
6. **Tin `cache()` là MEMORY_ONLY (đọc blog cũ)** rồi sợ OOM không dám cache, hoặc ngược lại dùng RDD tưởng có disk-fallback. Nhớ: DataFrame = MEMORY_AND_DISK, RDD = MEMORY_ONLY.
7. **Benchmark có cache mà quên `clearCache()`** giữa các lần đo → "tối ưu ảo": phiên bản sau nhanh hơn nhờ cache của phiên bản trước, không phải nhờ code bạn sửa.

---

## 13. Interview

**Junior:**

1. *cache() và persist() khác nhau gì? Mặc định của DataFrame là gì?* — `cache()` = `persist()` không tham số. Với DataFrame (Spark 3): MEMORY_AND_DISK — hết RAM thì tràn xuống disk local; với RDD: MEMORY_ONLY — hết RAM thì bỏ partition thừa, dùng đến đâu tính lại đến đó.
2. *Gọi df.cache() xong dữ liệu đã nằm trong memory chưa?* — Chưa. Cache lười: chỉ đánh dấu plan đáng giữ. Action đầu tiên chạy qua mới materialize, và chỉ materialize những partition mà action đó đụng tới. Muốn cache đủ 100%: chạy `count()` sau khi cache.
3. *Khi nào NÊN cache?* — DataFrame được dùng lại ≥2 lần mà tính lại đắt: rẽ nhiều nhánh aggregate, vòng lặp/ML iterative, notebook khám phá, nguồn đọc lại chậm (JDBC). Dùng 1 lần thì đừng — chỉ tốn phí.
4. *unpersist() để làm gì, quan trọng không?* — Trả lại storage memory cho ao unified ngay khi hết cần, thay vì chờ LRU eviction bị động. Quan trọng trong pipeline dài: cache cũ không dọn sẽ chèn ép execution của các bước sau (spill).

**Mid:**

5. *Fraction Cached trên Storage tab là 60% — chuyện gì đang xảy ra, hậu quả?* — Chỉ 60% partition nằm trong cache: do materialize bằng action không quét hết (show/take), do memory thiếu (MEMORY_ONLY bỏ phần thừa), hoặc block bị evict khi execution đòi đất. Hậu quả: 40% partition tính lại từ lineage MỖI lần dùng — trả phí cache mà nhận hàng thiếu; job chập chờn khó dự đoán.
6. *Tại sao thêm cache() có thể làm job CHẬM ĐI? Trình bày bằng memory model.* — Cache sống trong storage pool của ao unified — chung ao với execution. Cache bảng to: (a) phí materialize; (b) execution mất phần ao mượn được → sort/join/shuffle spill sớm hơn; (c) khi execution đòi đất, cache bị evict → vừa mất cache vừa đã trả phí. Dùng-1-lần hoặc pipeline shuffle-nặng là hai ca cache phản tác dụng điển hình.
7. *Cache có cắt lineage không? Vì sao? Cái gì cắt?* — Không. Cache là tối ưu cơ hội: block có thể bị evict/mất theo executor bất cứ lúc nào, Spark phải giữ nguyên công thức (lineage) để tính lại. `checkpoint()` mới cắt — vì dữ liệu đã nằm trên reliable storage, kế hoạch cũ không cần nữa. Hệ quả thực dụng: lineage quá dài gây nghẹt planner thì cache không cứu được, phải checkpoint.
8. *MEMORY_ONLY vs MEMORY_AND_DISK vs DISK_ONLY — chọn thế nào?* — MEMORY_ONLY: nhanh nhất nhưng thiếu RAM là bỏ partition (tính lại mỗi lần dùng) — chỉ khi chắc chắn vừa. MEMORY_AND_DISK: mặc định cân bằng — tràn xuống disk, không mất. DISK_ONLY: khi bảng to + tính lại đắt + muốn nhường toàn bộ RAM cho execution; đọc disk local vẫn thường rẻ hơn recompute cả lineage. Các bản _2 nhân đôi bản sao cho fault tolerance — trả ×2 memory, dành cho streaming/SLA cao.

**Senior:**

9. *So sánh cache vs checkpoint vs ghi Parquet trung gian — anh chọn gì cho pipeline batch scheduled nhiều tầng, tại sao?* — Cache: tái sử dụng ngắn hạn trong 1 job, rẻ nhất, không bền, không cắt lineage. Checkpoint: cắt lineage, bền trong phạm vi app — dành cho lineage quá dài/iterative/streaming state. Ghi bảng trung gian: bền qua application, thành restart point khi job fail giữa chừng, chia sẻ được cho pipeline/engine khác (Trino), quan sát được — nên là lựa chọn mặc định ở ranh giới tầng (bronze/silver/gold) của batch scheduled; cache chỉ dùng bên trong một tầng có rẽ nhánh. Trả lời hay phải nêu tiêu chí chọn (bền? restart? chia sẻ? chi phí?) chứ không phán một công cụ đúng mọi nơi.
10. *Thiết kế quy tắc caching cho team 10 DE dùng chung một cluster — anh đặt những luật nào?* — (1) Cache phải qua checklist: tái sử dụng ≥2, đặt ở điểm hẹp nhất, ước lượng size so với ao unified còn trống; (2) bắt buộc materialize tường minh (count) + unpersist khi xong — "ai vay người đó trả"; (3) pipeline scheduled ưu tiên bảng trung gian, cache chỉ trong phạm vi 1 job/notebook; (4) cấm MEMORY_ONLY cho bảng không chắc vừa (cache nửa vời); (5) giám sát: Storage tab/metrics — cache tồn tại > X giờ hoặc Fraction Cached < 100% thì cảnh báo; (6) code review: mọi `.cache()` phải kèm comment số nhánh dùng lại. Câu này đo tư duy vận hành đa người dùng — điểm senior nằm ở luật unpersist và giám sát, không phải ở việc biết API.

---

## 14. Summary

### Mindmap

```
                    CACHING & PERSISTENCE (L18)
                               │
   ┌───────────────┬───────────┴────────────┬────────────────────┐
   ▼               ▼                        ▼                    ▼
 HỢP ĐỒNG        STORAGE LEVEL           GIÚP vs HẠI          BỀN VỮNG
   │               │                        │                    │
 lười — action   DF cache() =            GIÚP: ≥2 lần dùng,   cache: mất theo
 đầu mới chạy    MEMORY_AND_DISK         iterative, notebook  executor/app,
 đơn vị =        (RDD = MEMORY_ONLY!)    HẠI: dùng 1 lần,     KHÔNG cắt lineage
 partition       _SER: đổi CPU lấy RAM   giành ao unified     checkpoint: cắt
 (Fraction       _2: ×2 bản sao          → spill/evict        lineage, bền in-app
 Cached!)        DISK_ONLY: nhường       cache điểm HẸP       Parquet trung gian:
 unpersist =     RAM cho execution       nhất + unpersist     bền cross-app,
 trả nợ ao                               materialize = count  restart point
```

### Checklist trước khi gõ "Continue"

- [ ] Nói đúng: DataFrame `cache()` = MEMORY_AND_DISK, RDD = MEMORY_ONLY, và cache là LƯỜI.
- [ ] Kể được bảng StorageLevel và tiêu chí chọn trong 4 ca khác nhau.
- [ ] Giải thích cache gây hại qua ao unified (lesson 17): materialize phí + giành đất + eviction.
- [ ] Đã thấy Fraction Cached < 100% trên Storage tab bằng chính tay mình (lab bước 2).
- [ ] Thuộc idiom: `cache()` → `count()` → dùng → `unpersist()`.
- [ ] Phân xử được cache vs checkpoint vs Parquet trung gian cho 3 ngữ cảnh khác nhau.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 19 — Data Skew: phát hiện và xử lý.**

Suốt 4 bài của Module 3, một bóng ma cứ thấp thoáng: "trừ khi dữ liệu LỆCH". Spill dồn vào 1 task (lesson 15), repartition(col) đẻ ra 1 task khổng lồ ngày Black Friday (lesson 16), OOM một executor trong khi 9 cái còn lại ngồi chơi (lesson 17) — tất cả cùng một thủ phạm: **data skew**, căn bệnh phổ biến nhất và khó chịu nhất của mọi hệ phân tán. Lesson 19 dạy bạn bắt bệnh trong 2 phút bằng Summary Metrics (max vs median), rồi cả kho vũ khí: salting, broadcast, tách hot key, và AQE skew join của Spark 3. Đây là bài mà kinh nghiệm thật của bạn bắt đầu khác biệt với người chỉ đọc docs.

> Gõ **"Continue"** khi sẵn sàng.
