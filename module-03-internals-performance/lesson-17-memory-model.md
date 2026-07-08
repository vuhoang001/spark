# Lesson 17 — Spark Memory Model: unified memory, execution vs storage

> Module 3 · Internals & Performance Tuning · Tuần 9 · Thời lượng: 5–6 giờ (lý thuyết 3h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Bản đồ memory của một executor**: heap = reserved 300MB + unified memory (`spark.memory.fraction` 0.6, chia execution/storage) + user memory 0.4 — và phần NGOÀI heap: memoryOverhead, off-heap.
- **Unified memory**: execution mượn storage, storage mượn execution — và luật eviction bất đối xứng (storage bị đuổi, execution thì không).
- Phân loại **OOM executor vs OOM driver**: nhận diện qua stack trace, nguyên nhân từng loại, cách xử lý đúng thứ tự.
- Công thức **memory per task** — con số quyết định spill hay không spill, sống hay OOM.
- Tại sao **tăng `spark.sql.shuffle.partitions` nhiều khi cứu OOM tốt hơn tăng RAM** — nghịch lý mà bài này giải thích tận gốc.

Sau bài này bạn phải làm được:

- Cho `spark.executor.memory=4g`, 4 core: tính ra từng vùng memory bằng MB và memory mỗi task được chia.
- Nhận một stack trace OOM và trả lời trong 1 phút: driver hay executor, nghi phạm số 1 là gì.
- Đọc cột Storage Memory / Peak JVM Memory trên tab Executors mà không đoán mò.

Kiến thức dùng trong thực tế: OOM là lỗi số 1 của Spark production. Người debug OOM bằng cách "tăng RAM gấp đôi rồi cầu nguyện" và người tính được từng vùng memory — khác nhau đúng một bậc lương.

---

## 2. Why

### "Cho executor 8GB mà vẫn OOM là sao?"

Câu hỏi này xuất hiện ở mọi team Spark. Junior nghĩ memory executor là một khối 8GB muốn dùng gì thì dùng. Sự thật: 8GB đó bị chia năm xẻ bảy, và phần thực sự dành cho việc "xử lý dữ liệu" (execution) chỉ khoảng **2.3GB** — chia tiếp cho 4 task đang chạy song song, mỗi task còn **~580MB**. Task của bạn cần sort 2GB? Spill. Cần giữ một bảng băm 3GB không spill được? OOM. 8GB "biến mất" như thế đấy.

Tệ hơn: có loại OOM xảy ra **ngoài heap** — executor bị YARN/Kubernetes xử tử vì process vượt trần container, dù heap còn trống. Tăng `spark.executor.memory` cho loại này còn làm bệnh NẶNG THÊM (heap to hơn nhưng overhead vẫn thiếu). Không có bản đồ memory thì mọi lần chỉnh config là một lần gieo xúc xắc.

### Analogy: tòa nhà văn phòng

Executor 4GB là một tòa nhà:

- **Reserved 300MB** = phòng kỹ thuật điện nước: của Spark, không ai được đụng.
- **Unified memory (~60% phần còn lại)** = không gian làm việc chung, gồm 2 khu:
  - **Execution** = bàn làm việc: nơi sort, join, aggregate diễn ra.
  - **Storage** = tủ hồ sơ: nơi cất cache, broadcast.
  - Tường giữa 2 khu là **tường di động**: bàn thiếu chỗ thì dẹp bớt tủ (đuổi cache — cache đuổi được vì tính lại được). Nhưng tủ KHÔNG đuổi được người đang ngồi làm (execution đang giữ dữ liệu dở dang — đuổi là sai kết quả).
- **User memory (~40%)** = hành lang, pantry: object Python/Scala của code bạn, buffer đọc Parquet... Spark không kiểm soát khu này — tiệc pizza to quá (UDF giữ list khổng lồ) thì tràn ra và cháy cả tòa nhà (OOM) dù khu làm việc còn trống.
- **memoryOverhead** = bãi xe + sân ngoài trời: NGOÀI tòa nhà nhưng vẫn tính vào diện tích lô đất mà chủ đất (YARN/K8s) cho thuê. Xe đậu tràn lố ranh giới → chủ đất hủy hợp đồng ngay lập tức (container killed).

### Nếu không hiểu memory model thì sao?

- Mọi OOM đều xử bằng "tăng RAM" → chi phí cluster tăng 2–4 lần mà lỗi vẫn quay lại khi dữ liệu tăng.
- Không giải thích được tại sao cache một bảng làm job spill nặng hơn (cache chiếm chỗ của execution — lesson 18).
- Phỏng vấn mid/senior gần như chắc chắn có câu memory model — trả lời "execution với storage gì đó" là rớt.

### Trade-off của unified memory (Senior phải thuộc)

| Được | Mất |
|---|---|
| Execution/storage mượn qua lại → tận dụng RAM tối đa, không lãng phí vùng cố định như Spark cũ (<1.6) | Hành vi khó dự đoán hơn: cùng job, có cache hay không cache làm spill khác hẳn |
| Cache tự bị đuổi thay vì job chết | Cache "bốc hơi" âm thầm → job chậm bí ẩn (recompute) |
| Ít config phải chỉnh (fraction hợp lý sẵn) | Hiểu sai `storageFraction` (tưởng là trần, thật ra là sàn) → tuning ngược |

---

## 3. Theory

### 3.1. Bản đồ toàn cảnh: executor 4GB, 4 core

```
┌─ Container / Pod (thứ YARN/K8s giám sát) ─────────────── 4g + 400m ≈ 4.4GB ─┐
│                                                                              │
│  ┌─ JVM HEAP (spark.executor.memory = 4g = 4096MB) ──────────────────────┐  │
│  │                                                                        │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐ │  │
│  │  │ RESERVED — 300MB cứng                                            │ │  │
│  │  │ (Spark giữ cho object nội bộ, không config được)                 │ │  │
│  │  ├──────────────────────────────────────────────────────────────────┤ │  │
│  │  │ UNIFIED MEMORY = (4096 − 300) × spark.memory.fraction (0.6)      │ │  │
│  │  │               = 3796 × 0.6 ≈ 2278MB                              │ │  │
│  │  │  ┌───────────────────────────┬────────────────────────────────┐  │ │  │
│  │  │  │ EXECUTION                 │ STORAGE                        │  │ │  │
│  │  │  │ sort / hash join /        │ cache / persist /              │  │ │  │
│  │  │  │ aggregate / shuffle buffer│ broadcast variables            │  │ │  │
│  │  │  └──────────── ▲ ───────────┴────────────────────────────────┘  │ │  │
│  │  │        ranh giới MỀM — spark.memory.storageFraction (0.5)       │ │  │
│  │  │        = mức storage được BẢO HỘ khỏi bị đuổi ≈ 1139MB          │ │  │
│  │  ├──────────────────────────────────────────────────────────────────┤ │  │
│  │  │ USER MEMORY = (4096 − 300) × 0.4 ≈ 1518MB                        │ │  │
│  │  │ (object trong code bạn, metadata, buffer thư viện —              │ │  │
│  │  │  Spark KHÔNG theo dõi vùng này)                                  │ │  │
│  │  └──────────────────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  NGOÀI HEAP:                                                                 │
│  • memoryOverhead = max(384MB, 10% × 4g) ≈ 410MB                             │
│    (Python worker của PySpark! + Netty buffer + thread stack + JVM metaspace)│
│  • spark.memory.offHeap.size (nếu bật offHeap.enabled — mặc định TẮT)        │
└──────────────────────────────────────────────────────────────────────────────┘
```

Với cluster Docker của ta (`spark.executor.memory=1g`): unified = (1024 − 300) × 0.6 ≈ **434MB**. Đó chính là con số bạn sẽ thấy ở cột "Storage Memory" tab Executors — giờ bạn biết nó từ đâu ra.

### 3.2. Luật mượn – trả – đuổi (eviction)

Unified memory là một cái ao chung, execution và storage cùng múc:

```
Luật 1: STORAGE mượn đất EXECUTION đang trống      → OK
Luật 2: EXECUTION mượn đất STORAGE đang trống      → OK
Luật 3: EXECUTION cần lại đất storage đang mượn    → ĐUỔI cache (evict block,
        tính lại sau nếu cần) — nhưng chỉ đuổi đến mức storageFraction (sàn bảo hộ)
Luật 4: STORAGE cần lại đất execution đang mượn    → CHỜ. Không bao giờ được đuổi
        execution (dữ liệu sort/hash dở dang, đuổi là sai kết quả) —
        cache mới sẽ ghi xuống disk hoặc bỏ qua, tùy StorageLevel
```

Tính bất đối xứng này là câu phỏng vấn kinh điển: **execution ưu tiên hơn storage**, vì block cache có thể tính lại từ lineage (mất thì chậm), còn trang execution đang dùng dở mà mất là SAI (hoặc crash). `spark.memory.storageFraction=0.5` là cái **SÀN** cho storage (phần không bị đuổi), không phải trần — storage vẫn chiếm 100% ao nếu execution không dùng.

### 3.3. Memory per task — con số quyết định

Executor N core chạy N task song song, chia nhau execution memory một cách **động**: mỗi task được đảm bảo tối thiểu `1/(2N)` và tối đa `1/N` ao execution (khi task khác chưa dùng). Ước lượng thực dụng:

```
memory_per_task ≈ (executor_memory − 300MB) × 0.6 / số_core_executor

Ví dụ 4g/4core:  ≈ 2278 / 4 ≈ 570MB/task
Cluster lab 1g/1core: ≈ 434MB/task
```

So sánh với **kích thước dữ liệu 1 task phải nhai** (shuffle read / số partition, nhớ nhân "hệ số nở" 2–5× khi deserialize thành object):

- Dữ liệu task < memory per task → êm.
- Lớn hơn → spill (với operator biết spill: sort, hash agg...).
- Lớn hơn mà operator KHÔNG spill được đủ (bảng băm broadcast, một record khổng lồ, collect_list một key…) → **OOM**.

### 3.4. Tại sao tăng shuffle partitions cứu OOM

Bây giờ nghịch lý ở lesson 15 sáng tỏ hoàn toàn:

```
Shuffle 100GB, executor 4g/4core → memory_per_task ≈ 570MB

partitions = 200:  100GB/200  = 500MB/task (×2–5 khi nở) → spill nặng, chực OOM
partitions = 2000: 100GB/2000 =  50MB/task               → nằm gọn trong 570MB

KHÔNG thêm một GB RAM nào — chỉ thái dữ liệu mỏng hơn.
```

Tăng RAM là scale **dọc** (có trần, tốn tiền); tăng partitions là thái nhỏ bài toán — đúng triết lý Spark từ lesson 1. Chỉ khi task đã mỏng mà vẫn thiếu (record đơn lẻ quá to, aggregate giữ state lớn) mới đến lượt tăng memory hoặc giảm core/executor.

### 3.5. OOM executor vs OOM driver

| | OOM EXECUTOR | OOM DRIVER |
|---|---|---|
| Nhận diện | Log executor: `java.lang.OutOfMemoryError: Java heap space`; hoặc `ExecutorLostFailure ... Container killed by YARN for exceeding memory limits` (loại NGOÀI heap) | Log ở chính process submit: OOM trong stack trace có `collect`/`toPandas`/broadcast; hoặc driver treo rồi chết |
| Nguyên nhân 1 | Task quá to: ít partition / skew → sort, hash agg không đủ chỗ và không spill kịp | `collect()` / `toPandas()` kéo cả bảng về driver |
| Nguyên nhân 2 | `groupBy` + `collect_list`/window trên key khổng lồ — một key phải nằm trọn trong 1 task | `broadcast()` bảng quá to (bảng broadcast được build trên driver trước) |
| Nguyên nhân 3 | Cache MEMORY_ONLY chèn ép + UDF giữ object to (user memory) | Quá nhiều partition/file → metadata, task status ngập driver (job hàng trăm nghìn task) |
| Nguyên nhân 4 (ngoài heap) | PySpark UDF: Python worker ăn memoryOverhead → container killed dù heap trống | Notebook giữ tham chiếu hàng chục DataFrame + kết quả cũ |
| Xử lý theo thứ tự | ① tăng shuffle partitions / fix skew ② bỏ collect_list kiểu gom-cả-thế-giới ③ đổi cache sang SER/DISK ④ tăng memoryOverhead (nếu container killed + PySpark) ⑤ cuối cùng: tăng executor memory hoặc giảm core | ① thay collect bằng write/show/take ② bỏ broadcast bảng to (để sort-merge join) ③ giảm số file bé (bớt task) ④ cuối cùng: tăng driver memory |

> Quy tắc vàng đọc OOM: **nhìn stack trace xem process nào chết trước, đừng nhìn message chung chung.** "Job failed" không nói lên gì; dòng `at org.apache.spark.sql.Dataset.collect` nói lên tất cả.

---

## 4. Internal

Đường đi của một lần "xin memory" bên trong executor:

```
① Task bắt đầu → TaskMemoryManager của task được tạo
        │
② Operator (vd ExternalSorter của shuffle) là một MemoryConsumer:
   "cho tôi 64MB execution memory"
        │
③ ExecutionMemoryPool kiểm tra:
   • Ao execution còn trống? → cấp.
   • Hết? Ao storage có phần trống hoặc phần mượn quá sàn storageFraction?
     → ĐUỔI block cache (evict LRU), lấy đất cấp cho task.
   • Vẫn không đủ VÀ task đang giữ dưới mức tối thiểu 1/(2N)?
     → task KHÁC bị ép spill để nhường.
   • Không xoay được nữa → consumer này tự SPILL, hoặc nếu không spill được → OOM.
        │
④ Task kết thúc → trả toàn bộ memory về ao.
```

Những chi tiết đáng giá khi debug:

- **Vì sao có spill rồi mà vẫn OOM?** Vì không phải mọi thứ spill được: (a) user memory (object trong UDF của bạn) nằm ngoài quyền quản của TaskMemoryManager; (b) một record/一 key đơn lẻ phải nằm trọn trong memory (row 500MB do explode/collect_list thì chịu); (c) bảng băm broadcast join phải nguyên vẹn trong memory.
- **`Container killed by YARN for exceeding memory limits. 4.5 GB of 4.4 GB physical memory used`** — câu thần chú production: process TỔNG (heap + overhead + Python worker) vượt trần container. Chữa bằng `spark.executor.memoryOverhead` (hoặc `spark.executor.pyspark.memory` để quản riêng Python), KHÔNG phải bằng executor.memory.
- **PySpark đặc thù**: UDF thường chạy trong **Python worker process riêng** — ăn RAM ngoài heap JVM, tức ăn vào overhead. Job toàn UDF + pandas thì overhead 10% mặc định gần như chắc chắn thiếu.
- **GC**: heap càng to, full GC càng lâu (hàng chục giây với heap >32–64GB). Đây là lý do người ta chuộng nhiều executor vừa (4–8 core, 8–32GB) hơn một executor khổng lồ — và là một lý do nữa để "tăng RAM" không phải thuốc tiên.
- Driver có cấu trúc heap y hệt executor (cũng reserved/unified/user) — nhưng "khách hàng" của nó là kết quả collect, broadcast build, task metadata thay vì sort/join.

---

## 5. API

Memory điều khiển bằng config — đặt lúc submit, KHÔNG đổi được giữa chừng:

### Bộ tứ chính

```bash
spark-submit \
  --conf spark.executor.memory=4g \            # heap executor
  --conf spark.executor.memoryOverhead=1g \    # ngoài heap (mặc định max(384m, 10%))
  --conf spark.driver.memory=2g \              # heap driver
  --conf spark.executor.cores=4 \              # số task song song → chia execution memory
  app.py
```
- **Pitfall lớn nhất**: đặt `spark.driver.memory` trong `SparkSession.builder.config(...)` ở **client mode** là VÔ DỤNG — JVM driver đã khởi động trước khi dòng code đó chạy. Phải đặt qua spark-submit/`spark-defaults.conf`. (`spark.executor.memory` thì đặt trong builder được, vì executor khởi động sau.)

### Bộ điều chỉnh tỷ lệ (hiếm khi cần đụng)

```bash
--conf spark.memory.fraction=0.6          # phần unified trong (heap − 300MB)
--conf spark.memory.storageFraction=0.5   # SÀN bảo hộ storage trong unified
```
- **Pitfall**: giảm `memory.fraction` để "cho user memory nhiều hơn" là chữa triệu chứng UDF ngốn RAM — thuốc đúng là sửa UDF (lesson 12).

### Off-heap (chủ đề nâng cao — biết để đọc tài liệu)

```bash
--conf spark.memory.offHeap.enabled=true
--conf spark.memory.offHeap.size=2g       # Tungsten quản lý ngoài heap, né GC
```

### PySpark riêng

```bash
--conf spark.executor.pyspark.memory=1g   # trần riêng cho Python worker
```

### Quan sát từ code

```python
sc = spark.sparkContext
print(sc._conf.get("spark.executor.memory"))
# Executors tab là nguồn chân lý: Storage Memory (ao unified), Peak JVM/Execution/Storage Memory (Spark 3)
```

---

## 6. Demo nhỏ

```
Input:  không cần dữ liệu — demo này TÍNH và ĐỐI CHIẾU
   ↓    tính unified memory bằng tay cho executor 1g
Output: so với cột Storage Memory trên tab Executors
```

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("demo17")
         .master("local[2]")
         .config("spark.driver.memory", "1g")   # local mode: driver = executor, set được ở đây
         .getOrCreate())                        #  vì JVM local chưa khởi động... khi chạy spark-submit
                                                #  thì vẫn phải set từ CLI — thử cả 2 để thấy khác biệt!
heap_mb = 1024
reserved = 300
unified = (heap_mb - reserved) * 0.6
print(f"Unified (execution+storage) dự đoán ≈ {unified:.0f}MB")
print(f"User memory ≈ {(heap_mb - reserved) * 0.4:.0f}MB")
print(f"Memory/task (2 core) ≈ {unified / 2:.0f}MB")

input(">>> Mở http://localhost:4040 → Executors → cột 'Storage Memory'. "
      "Con số tổng có ≈ dự đoán không? (Chênh nhẹ do JVM trừ hao heap thật.) Enter...")
spark.stop()
```

Bài học của demo: cột "Storage Memory" trên UI thực chất hiển thị **cả ao unified** (dành được cho storage tối đa), không phải "phần đang cache". Rất nhiều người đọc sai cột này.

---

## 7. Production Example

Sự cố thật (mô-típ lặp lại ở mọi công ty dùng PySpark trên YARN/K8s):

**Hiện tượng**: pipeline scoring chạy ổn 6 tháng, một sáng chết hàng loạt: `ExecutorLostFailure: Container killed by YARN for exceeding memory limits. 5.8 GB of 5.5 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead`.

**Phản xạ sai của team (và cái giá)**: tăng `spark.executor.memory` 5g → 8g. Container request thành 8.8GB, cluster chứa được ít executor hơn (job chậm đi), và... vẫn chết. Vì sao? Thủ phạm ăn memory là **Python worker của một pandas UDF mới thêm tuần trước** — nó ăn phần NGOÀI heap. Tăng heap chỉ làm phần "được phép" của JVM to ra, phần Python vẫn giẫm vạch như cũ.

**Chẩn đoán đúng** (dùng đúng bài hôm nay):

1. Message nói "physical memory of container" chứ không phải "Java heap space" → lỗi NGOÀI heap → nghi phạm: overhead/Python.
2. `git log` tuần trước: có pandas UDF mới xử lý batch lớn (`spark.sql.execution.arrow.maxRecordsPerBatch` mặc định 10.000 record/batch × record to).
3. Fix ba tầng: giảm `maxRecordsPerBatch` xuống 2.000; đặt `spark.executor.pyspark.memory=2g` (trần tường minh cho Python); tăng `memoryOverhead` lên 1.5g. Heap GIỮ NGUYÊN 5g.

**Kết quả**: job sống, container 5g+1.5g nhỏ hơn phương án 8g+0.8g thất bại — vừa rẻ hơn vừa đúng bệnh. Bài học được viết vào runbook của team: *"Container killed ≠ thiếu heap. Đọc kỹ message trước khi đụng config."*

---

## 8. Hands-on Lab

**Mục tiêu**: trên cluster 1G/1core, tự tay gây (a) task chật memory → spill, (b) cứu nó bằng shuffle partitions, (c) một driver OOM có kiểm soát. Memory thấp của lab là đạo cụ hoàn hảo — trên laptop 16GB bạn không bao giờ "được" thấy OOM.

### Bước 0 — bật cluster và tính trước

```bash
make up
```

Điền trước vào `labs/lab17/NOTES.md`: executor 512m → unified ≈ ((512−300)×0.6) = ? MB; 1 core → memory/task = ? MB. (Bạn sẽ đối chiếu số này với hành vi thật.)

### Bước 1 — `labs/lab17/executor_pressure.py`: chật → spill → cứu bằng partitions

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab17-executor-pressure")
         .config("spark.executor.memory", "512m")       # unified ≈ 127MB — chật chội cố ý
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)
big = (items.withColumn("n", F.explode(F.sequence(F.lit(1), F.lit(60))))
            .withColumn("payload", F.sha2(F.concat_ws("-", "order_id", "n"), 512)))

for parts in [2, 8, 64]:
    spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    t0 = time.time()
    cnt = (big.groupBy("order_id")
              .agg(F.collect_list("payload").alias("p"))   # agg giữ state to, khó gom
              .count())
    print(f"partitions={parts:<4} groups={cnt:,}  time={time.time()-t0:6.1f}s")

input(">>> UI :4040 → Stages: so Spill (Disk) của 3 vòng; Executors → Peak Memory. Enter.")
spark.stop()
```

```bash
make run F=labs/lab17/executor_pressure.py
```

Ghi bảng: partitions | thời gian | Spill (Disk) | task lâu nhất. Nếu vòng `parts=2` chết hẳn với `java.lang.OutOfMemoryError` — chúc mừng, bạn vừa chứng kiến executor OOM thật; đọc log rồi giảm `explode` xuống 40 để chạy tiếp các vòng sau.

### Bước 2 — `labs/lab17/driver_oom.py`: driver OOM có kiểm soát

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab17-driver-oom").getOrCreate()

big = (spark.range(0, 30_000_000)
       .withColumn("payload", F.sha2(F.col("id").cast("string"), 512)))

print("Executor đếm bình thường:", big.count())      # executor làm — êm
rows = big.collect()                                  # kéo TẤT CẢ về driver — BÙM (driver ~1g)
print(len(rows))
```

Chạy với driver bị siết (chú ý: driver memory phải đặt từ CLI, không đặt được trong builder — chính là pitfall mục 5):

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  labs/lab17/driver_oom.py
```

Đọc kỹ stack trace: OOM xuất hiện quanh `collectToPython`/`Dataset.collect` — chữ ký của driver OOM. So với stack trace (nếu có) ở bước 1: khác nhau thế nào?

### Bước 3 — tổng kết vào `labs/lab17/NOTES.md`

1. Bảng số liệu bước 1 + kết luận: partitions tăng thì spill và thời gian đổi ra sao, khớp công thức memory/task không?
2. 2 stack trace (executor vs driver) — dán dòng "đắt giá" nhất của mỗi cái và 1 câu nhận diện.
3. Trả lời: nếu chỉ được đổi MỘT config để cứu bước 1 vòng `parts=2`, bạn đổi gì và tại sao?

---

## 9. Assignment

**Easy** — Tính heap cần thiết: job có shuffle stage nặng nhất đọc 60 GB, chạy trên executor 4 core, muốn mỗi task xử lý trong memory không spill với hệ số nở dữ liệu ×3, dùng `spark.sql.shuffle.partitions=600`. Tính: dữ liệu/task, memory/task cần, từ đó suy ra `spark.executor.memory` tối thiểu (đảo ngược công thức unified). Đáp số kèm từng bước biến đổi.

**Medium** — Phân loại 4 ca OOM (nói rõ: driver hay executor, heap hay overhead, và MỘT hành động sửa đầu tiên): (a) `java.lang.OutOfMemoryError: Java heap space` tại `org.apache.spark.util.collection.ExternalSorter`; (b) `Container killed by YARN ... 6.1 GB of 6.0 GB physical memory used`, job dùng nhiều pandas UDF; (c) notebook chết ở dòng `df.toPandas()`; (d) `OutOfMemoryError` khi Spark build `BroadcastHashJoin` cho bảng bạn vừa hint broadcast.

**Hard** — "Tăng shuffle partitions hay tăng executor memory?": viết một trang quyết định luận có cây quyết định (ASCII), trả lời: khi nào partitions thắng (đa số), khi nào BẮT BUỘC memory (kể tối thiểu 2 ca: một key khổng lồ sau groupBy vì một key không chia được cho nhiều task; broadcast bảng lớn), và tại sao "giảm số core mỗi executor" là chiêu tăng memory/task không tốn thêm RAM. Kiểm chứng 1 nhánh bằng lab bước 1.

**Production Challenge** — Viết `memory-audit.md` cho cluster Docker của khóa học: với worker 1G/1core và executor mặc định 1g, tính đầy đủ các vùng (reserved/unified/user/overhead — overhead của standalone tính vào đâu?), chỉ ra tổng process có thể vượt 1G worker memory không và hệ quả; đề xuất config submit chuẩn cho các lab tiếp theo của Module 3 (executor.memory bao nhiêu để chừa overhead an toàn?). Đây là dạng tài liệu bạn sẽ viết thật khi vận hành cluster công ty.

> Nộp bài bằng cách paste code + số liệu + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Triệu chứng | Nghi phạm memory | Chữa theo thứ tự |
|---|---|---|
| Spill (Disk) > 0 đều các task | Task to hơn memory/task | ① filter/prune sớm ② tăng shuffle partitions ③ giảm core/executor ④ tăng memory |
| Spill dồn 1–2 task | Skew, không phải thiếu RAM | Lesson 19 — tăng RAM cả cụm để chữa 1 task là đốt tiền |
| Executor OOM heap | Task to + operator không spill được | Như spill, cộng: bỏ collect_list/window trên key to |
| Container killed (physical memory) | Overhead/Python, NGOÀI heap | memoryOverhead / pyspark.memory / sửa UDF — KHÔNG tăng heap |
| Job chậm dần khi thêm cache | Cache chiếm ao unified → execution hết chỗ mượn | unpersist / đổi StorageLevel (lesson 18) |
| Full GC dài (thấy trong Executors → GC Time) | Heap quá to hoặc churn object (UDF) | Nhiều executor nhỏ hơn; bỏ UDF; xem off-heap |

Ba nguyên tắc chốt:

1. **Memory là để tính toán, không phải để chứa bài toán.** Bài toán to thì thái nhỏ (partitions), đừng nong bụng (RAM).
2. **GC Time / Task Time > 10%** (tab Executors) = heap đang oằn mình — tín hiệu sớm trước cả OOM.
3. Mọi thay đổi memory config phải kèm số trước/sau: thời gian, spill, peak memory. Không số = không tuning.

---

## 11. Spark UI

Bài này mở khóa tab **Executors** — trạm y tế memory:

- **Storage Memory (used/total)**: total = ao unified (đúng con số bạn tính tay ở demo); used = phần cache/broadcast đang chiếm. Used sát total mà job còn shuffle nặng → execution và storage sắp giẫm chân nhau.
- **Peak JVM Memory / Peak Execution Memory / Peak Storage Memory** (Spark 3, bật qua nút "Show Additional Metrics"): trả lời "job này THẬT SỰ cần bao nhiêu" — nền tảng để right-size thay vì đoán. Peak execution 400MB trên executor 4g? Bạn đang trả tiền thừa 8 lần.
- **GC Time**: so với Task Time; >10% là đèn vàng.
- Executor biến mất khỏi danh sách + tab Jobs hiện `ExecutorLostFailure` → đi tìm log executor (Master UI :8080 → worker → stderr) đọc nguyên nhân thật.

Kết hợp tab Stages (lesson 15): **Stages nói task nào thiếu memory (spill), Executors nói process nào ốm (GC, peak, lost)** — hai tab này là cặp bài trùng chẩn đoán mọi vụ OOM.

---

## 12. Common Mistakes

1. **OOM là tăng `spark.executor.memory` — vô điều kiện.** Đúng bệnh chỉ ~30% ca; sai bệnh với container-killed (ngoài heap) và skew (1 task). Luôn đọc stack trace + UI trước.
2. **Đặt `spark.driver.memory` trong `SparkSession.builder` ở client mode** rồi tin là nó ăn. JVM driver đã chạy trước dòng code đó — config bị lờ im lặng. Đặt qua spark-submit.
3. **Đọc cột Storage Memory (total) là "RAM còn trống của executor"** — thực ra là kích thước ao unified. Đánh giá sai sức chứa → sizing sai.
4. **Nghĩ `storageFraction=0.5` là "cache tối đa 50%".** Nó là SÀN bảo hộ, không phải trần — cache có thể chiếm cả ao khi execution rảnh, và bị đuổi về sàn khi execution cần.
5. **Quên Python worker ăn RAM ngoài heap.** Job PySpark nhiều UDF để overhead mặc định 10% → container killed định kỳ, đổ oan cho "cluster không ổn định".
6. **Cache tràn lan để "cho nhanh"** rồi ngạc nhiên job spill nặng hơn — cache chiếm ao chung với execution. (Bài sau mổ xẻ đúng chỗ này.)

---

## 13. Interview

**Junior:**

1. *Heap của executor được chia thành những vùng nào?* — Reserved 300MB cố định; unified memory = (heap − 300MB) × `spark.memory.fraction` (0.6) gồm execution (sort/join/agg/shuffle) và storage (cache/broadcast) dùng chung; phần còn lại ~0.4 là user memory cho object của code người dùng. Ngoài heap còn memoryOverhead và off-heap (nếu bật).
2. *Execution memory và storage memory khác nhau gì?* — Execution: memory tạm cho tính toán (sort, hash join, aggregate, shuffle buffer), giải phóng khi task xong. Storage: memory giữ lâu dài cho cache và broadcast. Từ Spark 1.6 hai vùng dùng chung một ao (unified) và mượn qua lại được.
3. *OOM driver thường do gì?* — `collect()`/`toPandas()` kéo dữ liệu lớn về driver; broadcast bảng quá to (build trên driver); job có quá nhiều task/file khiến metadata ngập driver. Xử lý: thay collect bằng write/take, bỏ broadcast bảng to, gộp file bé, cuối cùng mới tăng driver memory.
4. *memoryOverhead là gì, mặc định bao nhiêu?* — Phần memory NGOÀI heap của container: Python worker (PySpark), Netty buffer, thread stack, metaspace. Mặc định max(384MB, 10% executor memory). Thiếu nó thì container bị YARN/K8s kill dù heap còn trống.

**Mid:**

5. *Luật mượn giữa execution và storage — chiều nào được đuổi chiều nào?* — Hai bên mượn phần trống của nhau. Execution đòi lại được: block cache bị evict (LRU) xuống tới sàn `storageFraction`. Storage KHÔNG đuổi được execution — phải chờ hoặc cache xuống disk/bỏ qua. Bất đối xứng vì cache tính lại được từ lineage còn dữ liệu execution dở dang mất là sai kết quả.
6. *Executor 8g, 4 core: một task được bao nhiêu memory để sort?* — Unified = (8192−300)×0.6 ≈ 4735MB; chia động cho 4 task: đảm bảo tối thiểu 1/8 (~590MB), tối đa 1/4 (~1180MB) khi các task khác chưa dùng. Trả lời có cả cận dưới 1/(2N) và cận trên 1/N là điểm cộng.
7. *"Container killed by YARN for exceeding memory limits" — chẩn đoán và xử lý?* — Process tổng vượt trần container: vấn đề NGOÀI heap (overhead, Python worker, off-heap netty), không phải heap đầy. Xử: tăng memoryOverhead, đặt spark.executor.pyspark.memory, giảm batch pandas UDF; tăng executor.memory thường vô ích thậm chí phản tác dụng.
8. *Tại sao tăng spark.sql.shuffle.partitions có thể cứu OOM?* — OOM/spill xảy ra khi dữ liệu 1 task vượt memory/task (~unified/số core). Tăng partitions thái shuffle mỏng hơn → mỗi task ôm ít hơn → nằm vừa memory. Chữa bằng cấu trúc bài toán thay vì tài nguyên — rẻ, scale được, nên thử trước khi tăng RAM.

**Senior:**

9. *Có spill để chống OOM rồi, tại sao executor VẪN OOM được? Kể 3 con đường.* — (a) User memory: object trong UDF/thư viện nằm ngoài kiểm soát của TaskMemoryManager, không spill được; (b) đơn vị không chia được: một record/một key khổng lồ (collect_list một key hot, row nở to sau explode) phải trọn trong memory; bảng băm broadcast join phải nguyên khối; (c) ngoài heap: Python worker/netty vượt overhead → container bị kill trước cả khi JVM kịp OOM. Bonus: spill cần đủ memory tối thiểu để vận hành buffer — bị ép đến mức không còn buffer thì vẫn chết.
10. *Bạn sizing memory cho một job mới từ con số nào, quy trình ra sao?* — Chạy thử trên mẫu dữ liệu, đọc Peak Execution Memory + Spill + GC Time trên UI; ước lượng dữ liệu/task ở stage nặng nhất (shuffle read / partitions × hệ số nở 2–5); chọn partitions để dữ liệu/task ~100–200MB; suy ngược executor.memory từ công thức unified với biên an toàn ~30%; overhead cộng riêng theo tỷ lệ UDF/Python; driver theo kích thước kết quả collect/broadcast. Sau đó chạy full, so peak thật với dự tính, điều chỉnh. Điểm senior: nói rõ "sizing là vòng lặp đo–chỉnh, không phải công thức một phát ăn ngay".

---

## 14. Summary

### Mindmap

```
                      SPARK MEMORY MODEL (L17)
                               │
   ┌───────────────┬───────────┴───────────────┬─────────────────┐
   ▼               ▼                           ▼                 ▼
 BẢN ĐỒ HEAP     LUẬT AO CHUNG             MEMORY/TASK         OOM HỌC
   │               │                           │                 │
 reserved 300MB  execution ↔ storage        ≈ unified/core     EXECUTOR: heap
 unified 0.6     mượn phần trống            cận 1/(2N)..1/N    (task to, key to)
 (exec+storage)  execution ĐUỔI cache       so với data/task   hoặc container
 user 0.4        (tới sàn storageFraction)  → spill/OOM        killed (ngoài heap,
 ngoài heap:     storage KHÔNG đuổi         TĂNG PARTITIONS    Python!)
 overhead 10%    được execution             = thái nhỏ         DRIVER: collect,
 (Python ở đây!) (cache tính lại được)      bài toán           broadcast, toPandas
                                                               → đọc STACK TRACE
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại bản đồ memory executor 4g từ trí nhớ, điền số MB từng vùng.
- [ ] Giải thích luật mượn–đuổi bất đối xứng và tại sao (lineage recompute).
- [ ] Nói đúng `storageFraction` là sàn, không phải trần.
- [ ] Tính memory/task cho cluster lab (512m/1core) và đối chiếu với spill quan sát được.
- [ ] Phân biệt 3 họ OOM: executor heap / container killed (ngoài heap) / driver — mỗi họ 1 cách chữa đầu tiên.
- [ ] Giải thích trôi chảy tại sao tăng shuffle partitions cứu được OOM.
- [ ] Biết `spark.driver.memory` phải đặt từ spark-submit ở client mode.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 18 — Caching & persistence: khi nào cache giúp.**

Hôm nay bạn đã thấy storage memory — cái tủ hồ sơ trong ao unified — và biết cache bị đuổi khi execution cần đất. Bài sau trả lời trọn vẹn: `cache()` thực chất làm gì (spoiler: nó LƯỜI, và mặc định của DataFrame là MEMORY_AND_DISK chứ không phải MEMORY_ONLY như nhiều blog viết), chọn StorageLevel nào cho tình huống nào, đọc "Fraction Cached" trên Storage tab để phát hiện cache nửa vời, và — quan trọng nhất — khi nào cache GIÚP, khi nào cache HẠI (dùng một lần mà cũng cache là tự bắn vào chân, vì chính bài học ao chung hôm nay). Kèm màn so găng: cache vs checkpoint vs ghi Parquet trung gian.

Memory model là lý thuyết nền — caching là quyết định hàng ngày xây trên nền đó.

> Gõ **"Continue"** khi sẵn sàng.
