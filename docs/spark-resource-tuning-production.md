# Cấu hình tài nguyên Spark trong Production — tư duy của Senior Data Engineer

> **Mục tiêu tài liệu:** không phải liệt kê tham số (tra docs là ra), mà là **cách ra quyết định**.
> Mỗi con số ở đây phải trả lời được câu: *"vì sao là số đó, và khi nào tôi được phép phá luật?"*

## Bảng mức độ tin cậy — đọc trước

Ngành Spark đầy "best practice" được chép lại suốt 10 năm mà không ai kiểm chứng lại. Tài liệu này đánh dấu rõ:

| Ký hiệu | Nghĩa |
|---|---|
| 📗 **CHẮC** | Đọc được trong mã nguồn Spark, hoặc là hằng số/mặc định kiểm chứng được |
| 📘 **CÓ NGUỒN** | Từ blog kỹ thuật công khai của Netflix/Uber/LinkedIn/Cloudera/Databricks, hoặc SPARK ticket |
| 📙 **TRUYỀN MIỆNG** | Quy tắc cộng đồng dùng rộng rãi nhưng **không ai công bố số liệu**. Dùng làm điểm khởi đầu, không phải chân lý |
| 📕 **SUY LUẬN** | Tôi suy ra từ nguyên lý. Hãy tự đo lại |

---

# 1. Nguyên tắc — và lý do đằng sau

## 1.1 Vì sao KHÔNG dùng hết CPU/RAM của cluster?

Vì **Spark không phải phần mềm duy nhất chạy trên máy đó.** Mỗi node còn nuôi:

| Thứ ăn tài nguyên | Ai hay quên |
|---|---|
| Kernel, systemd, sshd, monitoring agent (Datadog/Prometheus node exporter) | ai cũng quên |
| **NodeManager (YARN) / kubelet (K8s)** — chính cái daemon cấp tài nguyên cho bạn | người mới |
| **Page cache của OS** — Spark đọc Parquet qua page cache; bóp hết RAM = mọi lần đọc đều đi xuống đĩa | gần như tất cả |
| **JVM non-heap**: metaspace, code cache, thread stacks, Netty direct buffers | tất cả |
| **Tiến trình Python** (PySpark) — nằm HOÀN TOÀN ngoài heap | tất cả |

📗 **Sự thật cứng:** `-Xmx` chỉ chặn **heap**. Một executor JVM xin 20 GB heap thường chiếm **23–26 GB RSS thật**. Nếu bạn cấp đủ 100% RAM node cho heap, kernel sẽ **OOM-kill** executor — và bạn sẽ thấy `ExecutorLostFailure (exit code 137)` mà không hiểu vì sao, vì Spark UI báo memory used còn thấp.

📘 Đây là lý do YARN có `yarn.nodemanager.resource.memory-mb` < RAM vật lý, và K8s có `requests`/`limits` — cả hai đều là cơ chế **chừa phần cho hệ thống**.

📙 Quy tắc thực dụng: **chừa 1 core + 1 GB mỗi node cho OS/daemon**, và với node ≥ 64 GB thì chừa **8–10% RAM**.

> **Chi phí của việc vắt kiệt không phải "chậm hơn một chút" — mà là job CHẾT.** Đó là lý do người có kinh nghiệm luôn để lại đệm, kể cả khi CFO hỏi sao không dùng hết máy.

## 1.2 Vì sao "2–5 core mỗi executor"? — và vì sao quy tắc này ĐÃ LỖI THỜI

📘 **Nguồn gốc thật:** Sandy Ryza, blog Cloudera *"How-to: Tune Your Apache Spark Jobs (Part 2)"* (2015). Lập luận nguyên văn: **HDFS client** bị suy giảm throughput khi có quá nhiều thread đọc đồng thời trong một tiến trình; đo đạc cho thấy **~5 task đồng thời/executor** là điểm bão hoà.

**Đây là một tuyên bố về HDFS, năm 2015.** Không phải định luật vật lý.

**Chặn dưới (vì sao ≥ 2)** — cái này thì vẫn đúng mãi mãi, vì nó là số học:
- 📗 Mỗi executor trả **300 MB reserved** cố định (hằng số `RESERVED_SYSTEM_MEMORY_BYTES` trong `UnifiedMemoryManager`). Chia nhỏ = trả nhiều lần.
- 📗 Mỗi executor giữ **một bản sao riêng** của mọi broadcast variable. 100 executor × broadcast 500 MB = **50 GB** RAM bốc hơi toàn cluster.
- 📗 Shuffle sinh **M × R** kết nối. Nhiều executor nhỏ → nhiều mapper → bùng nổ số file/kết nối shuffle.
- 📗 Mỗi executor = một JVM = metaspace, code cache, thread pool riêng (~300–500 MB overhead).

**Chặn trên (vì sao ≤ 5) — chỗ cần nghi ngờ:**

| Bối cảnh | Trần thực tế | Vì sao |
|---|---|---|
| HDFS (2015, on-prem) | ~5 core | 📘 Đúng như Ryza đo — HDFS client thread contention |
| **S3/ADLS/GCS (đa số production 2025)** | **8–16 core vẫn ổn** | 📕 Nút thắt không còn là HDFS client mà là **connection pool** (`fs.s3a.connection.maximum`, mặc định 96) và băng thông mạng. Tăng pool lên là đi tiếp được |
| GC | 📙 khoảng 8–10 core | Nhiều core = nhiều object cùng lúc = áp lực GC. Đây mới là trần thật sự ở môi trường cloud |
| **PySpark** | 📕 **thấp hơn, 2–4** | Mỗi core = một tiến trình `python3` riêng ngoài heap. 8 core = 8 tiến trình Python ăn RAM không ai quản |

> **Cách nói đúng của năm 2025:** *"4–5 core là mặc định an toàn vì nó cân bằng GC và overhead-mỗi-executor. Nó KHÔNG phải vì HDFS — trừ khi bạn thật sự chạy trên HDFS. Trên S3, hãy đo lại trước khi tin."*

## 1.3 Nhiều executor nhỏ vs ít executor lớn

| | **Nhiều executor NHỎ** | **Ít executor LỚN** |
|---|---|---|
| Ví dụ | 20 × (2 core, 4 GB) | 8 × (5 core, 20 GB) |
| ✅ Được | Lỗi 1 executor mất ít việc; scheduler xếp dễ vào node vụn (bin-packing); GC pause ngắn | Ít trả reserved/broadcast; **join/agg trong-executor không qua mạng**; ít kết nối shuffle |
| ❌ Mất | Reserved × N; broadcast × N; shuffle M×R bùng nổ; nhiều JVM = nhiều overhead | GC pause dài (stop-the-world); mất cả executor = mất nhiều task; khó xếp vào node còn ít chỗ |
| 📙 Dùng khi | Job nhẹ RAM, nhiều task ngắn, cluster chia sẻ đông người, **spot/preemptible instance** | Shuffle lớn, join lớn, **cache nhiều**, ML |

📘 **Thực tế các công ty:** Netflix và các shop chạy nặng trên **spot instance** nghiêng về executor **vừa phải** — vì mất một executor 60 GB do spot bị thu hồi là mất rất nhiều việc phải tính lại. Đây là ví dụ điển hình của việc **hạ tầng quyết định config**, không phải lý thuyết Spark.

## 1.4 GC, JVM overhead và scheduling

### GC — kẻ thù thầm lặng

📗 **Ngưỡng 32 GB (compressed oops):** JVM nén con trỏ object xuống 4 byte khi heap ≤ ~32 GB. **Vượt 32 GB, con trỏ phình lên 8 byte** — bạn xin 40 GB nhưng **chứa được ít object hơn** 32 GB. Đây là hố sập kinh điển: tăng RAM mà chậm đi.

📙 **Luật thực dụng:** giữ executor heap trong **8–32 GB**. Cần nhiều hơn → **thêm executor, đừng phình executor**.

📗 **G1GC** nên bật khi heap > 8 GB (Java 11+ mặc định G1). ParallelGC tốt cho heap nhỏ, throughput cao.

**Đọc GC ở đâu:** Spark UI → tab Executors → cột **GC Time**.
📙 **GC Time > 10% Task Time = có vấn đề.** > 20% = job đang chết dần.

### Scheduling — khái niệm "wave"

📗 Nếu stage có 1000 task và cluster có 200 core → chạy **5 wave**. Wave cuối chỉ dùng một phần core → **cluster idle ở đuôi job**.

📙 Vì thế: **số task nên là bội số của tổng core** (2–4×). Không phải để "nhiều task hơn cho nhanh", mà để **wave cuối không bỏ trống máy**.

📕 Task quá ngắn (< 100 ms) thì chi phí lập lịch (~vài chục ms/task) ăn hết lợi ích song song — đây là mặt trái ít người nói của "chia nhỏ partition".

---

# 2. Quy trình thiết kế — 7 bước

> Đây là thứ tự mà người có kinh nghiệm thực sự nghĩ. Để ý: **số executor là bước GẦN CUỐI**, không phải bước đầu.

### Bước 1 — Kiểm kê phần cứng THẬT
Không phải `nproc`. Hỏi: bao nhiêu **nhân vật lý**? Hyper-threading chỉ cho thêm ~20–30%, không phải gấp đôi.
```
Ví dụ: 10 node × (16 vCPU / 8 nhân vật lý, 64 GB RAM)
```

### Bước 2 — Trừ phần cho hệ thống
```
mỗi node:  16 vCPU − 1 (OS + NodeManager/kubelet)  = 15 vCPU khả dụng
           64 GB   − 6 GB (OS + page cache + daemon) = 58 GB khả dụng
```
📙 Chừa 1 core + 8–10% RAM.

### Bước 3 — Chọn **KÍCH CỠ MỘT EXECUTOR** (không phải số lượng!)
Đây là quyết định thật sự. Mọi thứ khác là hệ quả.
```
executor.cores = 5     (mặc định an toàn; PySpark nặng thì 3)
```

### Bước 4 — Số executor **RƠI RA** từ phép chia
```
executor/node = 15 ÷ 5 = 3
tổng executor = 3 × 10 node = 30
   (YARN: trừ 1 cho ApplicationMaster → 29)
```
📗 **Bạn không chọn số executor. Bạn chọn kích cỡ, phép chia quyết định số lượng.** Đây là chỗ người mới hiểu ngược.

### Bước 5 — RAM mỗi executor
```
RAM/executor (gộp) = 58 GB ÷ 3 = 19.3 GB
```

### Bước 6 — **Tách overhead ra khỏi heap** (bước bị bỏ quên nhiều nhất)
📗 `container = spark.executor.memory (heap) + spark.executor.memoryOverhead (off-heap)`
📗 Mặc định overhead = `max(384 MB, 0.1 × heap)` — Spark 3.3+ dùng `spark.executor.memoryOverheadFactor` (mặc định `0.1`).
```
heap     = 19.3 / 1.1 ≈ 17 GB   →  spark.executor.memory   = 17g
overhead = 10% × 17  ≈ 1.7 GB   →  spark.executor.memoryOverhead = 2g
tổng container ≈ 19 GB  ✓ vừa 19.3
```
⚠️ **PySpark:** hệ số 0.1 là **quá ít**. Tiến trình Python nằm ngoài heap.
📙 Cộng đồng dùng **0.2–0.4** cho PySpark nặng, hoặc set `spark.executor.pyspark.memory` để chặn cứng.

### Bước 7 — Suy ra partition, rồi ĐO và sửa
```
tổng core = 29 × 5 = 145
spark.sql.shuffle.partitions = 145 × 2..3 ≈ 290–435  → chọn 300
```
📗 **Rồi mở Spark UI.** Mọi con số trên đây là **giả thuyết**, không phải kết luận. Xem mục 8.

---

# 3. Từng tham số — ý nghĩa thật và cái bẫy

## `spark.executor.instances`
📗 Số executor. **Bị vô hiệu khi bật `dynamicAllocation`** (trở thành giá trị khởi tạo).
🪤 Bẫy: set cái này trên Databricks/EMR có autoscaling → thường bị ghi đè, bạn tưởng đã tune nhưng không.

## `spark.executor.cores`
📗 Số task chạy song song **trong một** executor. Cũng là số thread, số tiến trình Python (PySpark).
🪤 Bẫy: **Standalone mode — không set thì executor ăn HẾT core của worker.** Đây là mặc định gây sốc nhất, và là điều bạn đã gặp ở lab A1.

## `spark.executor.memory`
📗 **Chỉ là JVM heap.** KHÔNG bao gồm overhead, KHÔNG bao gồm Python.
📗 Công thức RAM dùng được cho dữ liệu:
```
maxMemory = (heap − 300 MB) × spark.memory.fraction
```
🪤 Bẫy chí mạng: **xin 2 GB → chỉ có 1049 MB cho dữ liệu (51%).** Xin 512 MB → chỉ 127 MB (**25%**). Trần tuyệt đối là **60%**, không bao giờ vượt.
📗 Heap < 450 MB → Spark **từ chối khởi động** (`System memory must be at least 450MB`).

## `spark.driver.memory`
📗 RAM cho driver: giữ kết quả `collect()`, **bảng broadcast**, metadata Parquet, kế hoạch query.
🪤 **Bẫy lớn nhất trong toàn bộ Spark:** ở **client mode**, set từ `SparkSession.builder` là **VÔ HIỆU HOÀN TOÀN** — JVM driver đã khởi động trước khi code Python chạy. **Bắt buộc** dùng `--driver-memory` / `spark-defaults.conf`. Spark **không báo lỗi**, chỉ im lặng bỏ qua.
📙 Khi nào cần driver to: nhiều broadcast join, `toPandas()`, hàng chục nghìn partition (metadata), query plan khổng lồ.
📗 Kèm theo: `spark.driver.maxResultSize` (mặc định **1g**) — vượt là job chết dù driver còn RAM.

## `spark.driver.cores`
📗 Chỉ có tác dụng ở **cluster deploy-mode** (driver chạy trong container). Client mode: driver dùng CPU của máy submit.
📙 1–4 core. Driver không tính toán dữ liệu, nhưng **lập plan Catalyst** với query lớn thì có ăn CPU.

## `spark.default.parallelism`
📗 Chỉ áp cho **RDD API** (`parallelize`, `reduceByKey`). **DataFrame/SQL KHÔNG dùng nó** — dùng `shuffle.partitions`.
🪤 Bẫy: set cái này rồi tưởng đã tune SQL. Không hề.
📗 Mặc định = tổng core của executor (cluster) hoặc N trong `local[N]`.

## `spark.sql.shuffle.partitions`
📗 Mặc định **200** — con số cố định từ 2015, **không liên quan gì đến cluster của bạn**.
📙 Hai cách chọn:
- **Theo core:** `2–4 × tổng core`
- **Theo kích thước** (tốt hơn): nhắm **100–200 MB dữ liệu shuffle mỗi partition** → `partitions = tổng_shuffle_bytes / 150 MB`
📗 **AQE (Spark 3.2+, bật mặc định) tự gộp partition sau shuffle** → con số 200 bớt tai hại. Nhưng **map side vẫn ghi đủ 200 mảnh** — AQE không cứu được phần đó.

## `spark.memory.fraction` (0.6) và `storageFraction` (0.5)
📗 `fraction`: bao nhiêu phần của (heap − 300 MB) được dùng cho **execution + storage**. 40% còn lại là **user memory** (object của bạn, UDF).
📗 `storageFraction`: phần trong pool đó được **bảo đảm** cho cache — execution không đuổi được. Hai bên **mượn nhau tự do** ngoài phần bảo đảm.
📙 **Khi nào đụng vào:** hầu như không bao giờ. Nếu OOM ở user code (UDF giữ list to) → **giảm** `fraction` xuống 0.5. Nếu job toàn cache và không shuffle → tăng `storageFraction`. Sửa mù hai số này là dấu hiệu chưa hiểu vấn đề thật.

## `spark.executor.memoryOverhead`
📗 RAM **off-heap** của container: Netty direct buffer, thread stack, metaspace, **tiến trình Python**.
📗 Mặc định `max(384 MB, 0.1 × heap)`.
🪤 **Bẫy theo môi trường:** ở **YARN/K8s**, vượt overhead → **container bị GIẾT** (`exit 137`). Ở **Standalone**, nó **hoàn toàn không được thực thi** — chỉ là con số kế toán, không ai chặn. Đây là lý do standalone trên laptop có thể làm **đơ cả máy** thay vì chết job.
📙 PySpark/pandas UDF: nâng lên **20–40%** heap.

## `spark.dynamicAllocation.enabled`
📗 Spark tự thêm/bớt executor theo hàng đợi task.
📗 Yêu cầu: **external shuffle service** (YARN), hoặc `spark.dynamicAllocation.shuffleTracking.enabled=true` (K8s, Spark 3.0+) — nếu không, thu hồi executor sẽ **mất dữ liệu shuffle** của nó.
📗 Tham số: `minExecutors`, `maxExecutors`, `initialExecutors`, `executorIdleTimeout` (60s), `schedulerBacklogTimeout` (1s).
📘 **Bật mặc định trên EMR và Dataproc.** Đây là lý do bạn set `executor.instances` mà thấy không ăn thua.
🪤 **Streaming: cân nhắc TẮT.** Batch micro liên tục làm scale-up/down giật liên hồi; thu hồi executor giữa chừng gây trễ. Structured Streaming có `spark.streaming.dynamicAllocation` riêng.

---

# 4. Theo loại workload

| Workload | executor.cores | heap | Điểm mấu chốt |
|---|---|---|---|
| **ETL Batch** | 4–5 | 8–32 GB | Mặc định. Bật dynamic allocation. `shuffle.partitions` theo kích thước dữ liệu |
| **Streaming** | 2–4 | 4–16 GB | 📙 **TẮT dynamic allocation** (hoặc rất thận trọng). `shuffle.partitions` = **cố định, ≈ tổng core** — đổi số này giữa chừng làm **hỏng checkpoint**. Ưu tiên độ trễ ổn định hơn throughput |
| **Machine Learning** | 4–8 | **lớn, 32–64 GB** | Cache dataset nhiều lần lặp → tăng `storageFraction`. Ít executor lớn (dữ liệu ở gần nhau). GPU: thường **1 executor/GPU** |
| **SQL Analytics** | 4–5 | 16–32 GB | AQE là vua. Broadcast join → **driver phải to**. `autoBroadcastJoinThreshold` (mặc định 10 MB) thường nên nâng lên 100–200 MB nếu driver đủ RAM |
| **Large Shuffle** | 5 | to, + **overhead to** | 📘 Đây là chỗ các công ty lớn phải **tự viết shuffle service**: LinkedIn **Magnet** (push-based shuffle, vào Spark 3.2 — `spark.shuffle.push.enabled`), Uber **RSS**, Facebook **Cosco**. Ở quy mô đó, shuffle qua đĩa local là nút thắt kiến trúc, không tune bằng config được nữa |
| **Small Files** | — | — | Vấn đề **không nằm ở executor**. Sửa ở tầng ghi: `repartition()` trước `write`, hoặc **compaction job định kỳ**. 📗 Mỗi file Parquet có footer riêng → 10.000 file nhỏ = 10.000 lần mở file + đọc metadata. Driver cũng ngộp vì phải liệt kê file |
| **Iceberg** | 4–5 | 8–32 GB | 📗 Metadata nằm trong **manifest**, không phải liệt kê thư mục → planning nhanh hơn Hive rất nhiều, **driver nhẹ gánh hơn**. Dùng `rewrite_data_files` để compaction. Tuning dịch chuyển sang **table maintenance**, không phải executor |
| **Delta Lake** | 4–5 | 8–32 GB | 📗 `OPTIMIZE` + `ZORDER` thay cho tự tune layout. `optimizeWrite`/`autoCompact` xử lý small-files tự động (Databricks). Z-ORDER ăn shuffle nặng → cần executor RAM khá |

> **Điểm chung của Iceberg/Delta:** chúng **chuyển bài toán tuning từ "config executor" sang "bảo trì bảng"**. Đây là thay đổi tư duy lớn nhất của lakehouse: bạn tune *layout dữ liệu*, không tune *cụm máy*.

---

# 5. Theo môi trường triển khai

| | Ai quyết tài nguyên | Overhead có bị ép? | Đặc thù |
|---|---|---|---|
| **Local** | `local[N]` | Không | Không có executor. Driver kiêm tất cả. `--driver-memory` là **thứ duy nhất** có nghĩa |
| **Standalone** | Worker tự khai (`SPARK_WORKER_CORES/MEMORY`) | ❌ **KHÔNG** | 🪤 Không set `executor.cores` → **1 executor ăn hết worker**. Overhead không được thực thi → tràn RAM host thay vì chết job. **Không ai dùng cho production lakehouse** |
| **YARN** | ResourceManager | ✅ Có (giết container) | `--num-executors`, AM chiếm 1 container. Cần External Shuffle Service cho dynamic allocation. 📘 Nền tảng của Uber/LinkedIn thời kỳ đầu |
| **Kubernetes** | Scheduler K8s | ✅ Có (OOMKilled 137) | Mỗi executor = **1 Pod**. `spark.kubernetes.executor.request.cores` vs `limit.cores` (CPU throttling!). Dynamic allocation cần `shuffleTracking`. 📘 Hướng đi của hầu hết công ty tự host hiện nay |
| **Databricks** | **Bạn chọn instance type, không chọn executor** | ✅ Có | 📘 Triết lý khác hẳn: bạn chọn *loại máy* + autoscaling, Databricks **tự suy** executor config. Photon (engine C++) làm nhiều tham số JVM thành vô nghĩa. Tuning dịch sang: chọn instance, autoscale range, Delta OPTIMIZE |
| **EMR / Dataproc** | Managed, dynamic allocation **bật sẵn** | ✅ Có | 🪤 `maximizeResourceAllocation=true` (EMR) → **1 executor khổng lồ/node**. Nghe hấp dẫn, thường là **bẫy**: GC pause dài, mất 1 executor = mất nhiều |

📕 **Nhận xét xuyên suốt:** càng lên managed platform, **bạn càng ít tune executor và càng nhiều tune dữ liệu**. Kỹ năng dịch chuyển từ "sizing JVM" sang "layout bảng + chọn instance + đọc query plan".

---

# 6. Ví dụ theo quy mô

> Giả định: cloud, S3, PySpark vừa phải, node 16 vCPU / 64 GB.

### Cluster 8 core (1 node — dev/CI)
```
executor.cores = 2 ;  executor.memory = 4g ;  overhead = 1g
→ 3 executor (chừa 1-2 core cho OS + driver)
shuffle.partitions = 16 ;  driver.memory = 2g
```
**Vì sao:** ở quy mô này **overhead cluster lớn hơn lợi ích song song** — như lab A3 của bạn đã chứng minh (cluster thua `local[*]` 1.8×). Đây là môi trường **học và test đúng đắn**, không phải test hiệu năng.

### Cluster 32 core (2 node)
```
executor.cores = 4 ;  executor.memory = 12g ;  overhead = 2g
→ 3 executor/node × 2 = 6 executor,  24 core
shuffle.partitions = 96  (4× core)  ;  driver.memory = 4g
```
**Vì sao:** 4 core để chia hết 15 core khả dụng thành 3 executor gọn. Bắt đầu có lợi ích thật từ cluster.

### Cluster 64 core (4 node)
```
executor.cores = 5 ;  executor.memory = 17g ;  overhead = 2g
→ 3 executor/node × 4 = 12 executor,  60 core
shuffle.partitions = 200  (mặc định, tình cờ vừa!) ;  driver.memory = 8g
dynamicAllocation: min=4  max=12
```
**Vì sao:** đây là "vùng cổ điển" mà quy tắc 5-core sinh ra. Chú ý 200 mặc định **tình cờ hợp lý** ở đúng quy mô này — và đó là lý do nó tồn tại được 10 năm mà ít ai thắc mắc.

### Cluster 128 core (8 node)
```
executor.cores = 5 ;  executor.memory = 17g ;  overhead = 2g
→ 24 executor,  120 core
shuffle.partitions: theo KÍCH THƯỚC, không theo core nữa
   1 TB shuffle / 150 MB ≈ 7000 partition   (không phải 120×3=360!)
driver.memory = 16g   ← metadata của 7000 partition là thật
AQE: bật (coalesce sẽ tự gộp phần đuôi)
```
**Vì sao đổi cách tính:** 📕 Ở quy mô này, quy tắc "2–4× core" **gãy**. Nếu 1 TB shuffle chia 360 partition → mỗi partition **2.8 GB** → **spill ra đĩa chắc chắn**. Kích thước partition mới là ràng buộc, số core chỉ quyết định *bao nhiêu wave*.

### Cluster 500+ core
```
Không còn là bài toán config nữa.
```
📘 Ở quy mô này, các công ty lớn **thay đổi kiến trúc**, không sửa tham số:
- **Shuffle service riêng**: LinkedIn Magnet (push-based, đã vào Spark 3.2), Uber RSS, Facebook Cosco. Vì shuffle qua đĩa local trở thành nút thắt vật lý.
- **Tách compute khỏi shuffle storage** (shuffle lên S3/dịch vụ riêng) → cho phép dùng spot instance mà không sợ mất shuffle.
- **Auto-tuning**: 📘 LinkedIn viết **Dr. Elephant**, Qubole viết **Sparklens** — công cụ đọc history server và **tự đề xuất config**. Vì không ai tune tay nổi 10.000 job/ngày.
- **Chia nhỏ job.** Một job 500 core thường nên là 10 job 50 core.

> 📕 **Bài học quy mô:** dưới 100 core, bạn tune config. Trên 500 core, bạn tune **kiến trúc**. Người mới hay cố mang tư duy đầu sang bài toán thứ hai.

---

# 7. Sai lầm phổ biến

| Sai lầm | Triệu chứng | Vì sao xảy ra |
|---|---|---|
| **Executor khổng lồ** (1/node, all cores) | GC time > 20%, task đứng hình | 📗 Vượt 32 GB → mất compressed oops. EMR `maximizeResourceAllocation` dụ người ta vào đây |
| **Executor tí hon** (1 core) | Job chậm bất thường, RAM "biến mất" | 📗 300 MB reserved × N. Ở heap 512 MB chỉ còn **25%** dùng được |
| **Quên overhead** | `ExecutorLostFailure`, `exit code 137` | 📗 `-Xmx` chỉ chặn heap. RSS thật cao hơn 15–30%. **PySpark là thủ phạm số 1** |
| **Quá ít partition** | Spill ra đĩa, OOM, 1 task chạy mãi | Partition > RAM/core → spill. Kiểm: Spark UI cột **Spill (Disk)** |
| **Quá nhiều partition** | Task < 100 ms, cluster bận mà chẳng làm gì | Chi phí lập lịch ăn hết. Cũng gây **small files** khi ghi |
| **Để nguyên 200** | Vừa spill (data lớn) vừa lãng phí (data nhỏ) | 200 là số cố định năm 2015. AQE cứu một phần, không cứu hết |
| **`collect()` / `toPandas()`** | Driver OOM, `maxResultSize exceeded` | Kéo toàn bộ về **1 máy**. Đây là lỗi #1 của người mới |
| **`coalesce(1)` khi ghi** | Job treo ở stage cuối | `coalesce` **không shuffle** → dồn hết vào 1 task, **1 core làm việc, còn lại ngồi chơi**. Cần `repartition(1)` (có shuffle) hoặc chấp nhận nhiều file |
| **Data Skew** | 199/200 task xong trong 5s, task cuối chạy 40 phút | 📗 Một key chiếm phần lớn dữ liệu. **AQE `skewJoin` xử lý được join skew**, nhưng **không cứu được groupBy skew** — cái đó cần salting |
| **`print()` để debug** | "Sao không thấy gì?" | Chạy trên executor, log ở máy khác (bạn đã học ở A2) |

---

# 8. Quy trình tuning — cách Senior thật sự làm

> **Nguyên tắc số 1: KHÔNG tune trước khi đo.** Mọi con số ở mục 2 là *giả thuyết khởi đầu*.

## Thứ tự đọc Spark UI (đúng thứ tự này, đừng nhảy cóc)

**1. Tab Jobs → tìm stage LÂU NHẤT.** Chỉ tune stage đó. 📙 Quy tắc 80/20: một stage thường chiếm phần lớn thời gian.

**2. Trong stage đó → bảng phân bố Task (Summary Metrics).** Nhìn cột **Max** và **Median**:
```
📙 Max / Median > 3  →  CÓ SKEW.  Đây là thứ đầu tiên phải kiểm, trước mọi tham số.
```
Vì sao trước? Vì **skew làm mọi tuning khác vô nghĩa** — bạn thêm 100 core cũng vẫn phải chờ đúng một task đó.

**3. Cột Spill (Memory) / Spill (Disk).**
```
📗 Spill > 0  →  partition quá to so với RAM/core.
   Sửa: TĂNG shuffle.partitions (chia nhỏ ra), hoặc tăng executor.memory.
   📕 Tăng partition thường rẻ hơn tăng RAM.
```

**4. Shuffle Read / Write size.**
```
📙 Nhắm 100–200 MB shuffle mỗi partition.
   shuffle.partitions = tổng_shuffle_bytes ÷ 150 MB
```
📕 Đây là **cách tính đúng**, còn "2–4× core" chỉ là xấp xỉ dùng khi chưa biết kích thước dữ liệu.

**5. Tab Executors → cột GC Time.**
```
📙 GC > 10% Task Time  →  heap quá lớn, hoặc quá nhiều object.
   Sửa: GIẢM executor.memory (nghe phản trực giác!), hoặc chuyển sang G1GC,
        hoặc giảm executor.cores (ít task đồng thời = ít object sống cùng lúc).
```

**6. CPU Utilization.** Cluster đủ core mà CPU thấp → **không phải thiếu máy**, mà là:
- ít task hơn số core (thiếu partition), hoặc
- đang chờ I/O (mạng/đĩa), hoặc
- **skew** (chỉ 1 core làm việc)

**7. Tab SQL → đọc plan.** `explain(mode="formatted")`. Tìm:
- `PartitionFilters` — partition pruning có chạy không?
- `PushedFilters` — filter có đẩy xuống tầng đọc file không?
- `ReadSchema` — có đọc thừa cột không?
- `Exchange` — mỗi cái là một lần shuffle. Đếm chúng.
- `number of files read` / `size of files read` — 📕 **con số trung thực nhất trong toàn bộ Spark UI**, vì bytes không nói dối như giây.

## AQE — thứ đã thay đổi cuộc chơi

📗 Bật mặc định từ **Spark 3.2**. Ba thứ nó làm, **sau khi đã thấy dữ liệu thật**:

| Tính năng | Làm gì | Config |
|---|---|---|
| **Coalesce partitions** | Gộp partition nhỏ sau shuffle → con số 200 bớt tai hại | `advisoryPartitionSizeInBytes` (mặc định **64 MB**) |
| **Skew join** | Tự phát hiện partition khổng lồ, **chẻ nhỏ nó ra** | `skewedPartitionFactor` (5.0), `skewedPartitionThresholdInBytes` (256 MB) |
| **Dynamic join switch** | Sort-merge join → broadcast join khi thấy bảng thật ra nhỏ | tự động |

📕 **Hệ quả tư duy:** AQE làm **rất nhiều tuning tay trở nên thừa**. Người học Spark từ tài liệu 2017 vẫn đang tune những thứ AQE lo hộ từ lâu.
🪤 **Nhưng đừng ỷ lại:** AQE chỉ sửa **sau shuffle**. Nó **không cứu** map-side (vẫn ghi đủ 200 mảnh), **không cứu** groupBy skew, **không cứu** small files khi ghi.

---

# 9. Rule of thumb — và khi nào PHÁ chúng

| Quy tắc | Mức | **Phá khi nào** |
|---|---|---|
| 4–5 core/executor | 📙 | **Trên S3** (không phải HDFS) → 8+ vẫn ổn. **PySpark nặng** → giảm còn 2–3 |
| Heap 8–32 GB | 📗 | ML cache lớn → chấp nhận 48–64 GB **và trả giá GC**, sau khi đã đo |
| Overhead 10% | 📗 | **PySpark → 20–40%**. Đây là ngoại lệ phổ biến nhất, và bị bỏ qua nhiều nhất |
| `shuffle.partitions = 2–4 × core` | 📙 | **Dữ liệu > vài trăm GB** → tính theo **kích thước** (150 MB/partition), không theo core |
| Partition 128 MB | 📙 | Task quá ngắn (< 100 ms) → gộp lớn hơn. Chỉ số thật cần nhìn là **task duration**, không phải MB |
| Chừa 1 core + 10% RAM cho OS | 📙 | Không bao giờ phá. Phá = job **chết**, không phải chậm |
| Bật dynamic allocation | 📘 | **Streaming → tắt.** Job SLA chặt → tắt (scale-up có độ trễ) |
| Bật AQE | 📗 | Gần như không bao giờ tắt. Trừ khi đang **debug** để thấy plan gốc |
| Task 1–10 giây | 📙 | Ngắn hơn → overhead lập lịch. Dài hơn → mất mát lớn khi task fail/retry |
| GC < 10% | 📙 | Không phá. Đây là chỉ báo sức khoẻ |

## Ba câu hỏi Senior tự hỏi trước khi đụng vào config

1. **"Đây có phải là vấn đề tài nguyên không?"**
   📕 Phần lớn job chậm **không phải** vì thiếu executor. Nó chậm vì **skew**, vì **đọc thừa dữ liệu** (không pruning), vì **shuffle không cần thiết**, hoặc vì **small files**. Thêm máy vào một job skew = **đốt tiền mà không nhanh hơn một giây nào**.

2. **"Tôi đang tối ưu cho cái gì?"**
   Throughput? Độ trễ? **Chi phí?** Ba mục tiêu này **mâu thuẫn nhau**. Cluster nhanh nhất hiếm khi rẻ nhất. Không trả lời được câu này thì mọi tuning là mò mẫm.

3. **"Con số này đến từ đâu?"**
   📕 Nếu câu trả lời là *"tôi chép trên mạng"* — bạn đang **cargo-cult**. Đây chính là cách quy tắc "5 core" (một tuyên bố về **HDFS năm 2015**) vẫn được áp cho cluster S3 năm 2025 mà **không ai đo lại**.

---

## Nguồn tham khảo có thật

- **Sandy Ryza (Cloudera, 2015)** — *How-to: Tune Your Apache Spark Jobs, Part 1 & 2*. Cội nguồn của quy tắc 5-core và cách tính executor. Đọc để hiểu **bối cảnh** nó ra đời (HDFS, on-prem, YARN).
- **LinkedIn Magnet** — SPARK-30602, push-based shuffle, vào Spark 3.2. Ví dụ điển hình: quy mô lớn thì phải sửa **kiến trúc shuffle**, không sửa config.
- **LinkedIn Dr. Elephant** — công cụ auto-tuning mã nguồn mở. Bằng chứng cho thấy tune tay không scale.
- **Uber Remote Shuffle Service (RSS)**, **Facebook Cosco** — cùng bài toán, cùng hướng giải.
- **Netflix** — Iceberg (Ryan Blue) và S3 committers. Chứng minh luận điểm "lakehouse chuyển tuning từ cluster sang table format".
- **Mã nguồn Spark** — `UnifiedMemoryManager.scala` (hằng số 300 MB), `FilePartition.scala` (công thức chia partition khi đọc file). 📕 **Khi tài liệu và mã nguồn mâu thuẫn, tin mã nguồn.**

---

> ## Câu tổng kết
>
> **Người mới hỏi: "config nào đúng?"**
> **Senior hỏi: "nút thắt của tôi ở đâu?"**
>
> Config chỉ là *hệ quả* của câu trả lời thứ hai. Đó là lý do tài liệu này dành 1 mục cho tham số và 3 mục cho **cách nhìn**.
