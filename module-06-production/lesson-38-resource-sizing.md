# Lesson 38 — Resource sizing: executor/core/memory calculation

> Module 6 · Production Engineering · Tuần 20 · Thời lượng: 4–5 giờ (lý thuyết 2.5h, lab 2h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Bài toán kinh điển**: cluster 10 node × 16 core × 64 GB — cấu hình executor thế nào? Giải từng bước như lời giải mẫu, để bạn giải lại được với MỌI cỡ cluster.
- Vì sao **5 core/executor** là con số vàng (và nó đến từ đâu — HDFS client throughput).
- **Fat executor vs tiny executor** — hai thái cực đều thua, bảng trade-off.
- **memoryOverhead**: phần RAM "tàng hình" ngoài heap — và vì sao PySpark phải tăng nó.
- Khớp **số partition với tổng core** (nguyên tắc 2–3× wave).
- Sizing cho dataset cụ thể (ví dụ 100 GB), dynamic allocation min/max, và vì sao **streaming sizing khác batch**.
- Một trang **cheat sheet công thức** dùng cả đời.

Sau bài này bạn phải làm được:

- Giải bài "10 node × 16 core × 64 GB" trong 5 phút trên bảng trắng, giải thích từng phép trừ.
- Nhìn lệnh spark-submit của đồng nghiệp và chỉ ra: "executor này fat quá / memoryOverhead thiếu cho PySpark".
- Trả lời "job chậm thì thêm executor hay thêm core?" bằng lập luận, không đoán.

Kiến thức dùng trong thực tế: **mọi job production và mọi buổi phỏng vấn**. Đây là câu hỏi sizing xuất hiện trong ~70% phỏng vấn Spark từ mid trở lên.

---

## 2. Why

### Chuyện gì xảy ra khi sizing sai

Bạn có job xử lý 100 GB. Bạn đoán đại `--executor-memory 4g --executor-cores 2`:

- Quá ít memory → shuffle spill xuống disk (chậm 10×), hoặc thẳng tay `OutOfMemoryError`.
- Quá nhiều memory một executor (fat executor 60 GB) → GC pause hàng chục giây, job "thi thoảng khựng" không rõ lý do.
- Quá ít core tổng → 800 partition xếp hàng chờ trên 8 slot, job chạy 100 wave.
- Quá nhiều executor nhỏ (tiny) → broadcast join phải copy dữ liệu ra 100 chỗ thay vì 20, mất luôn khả năng chạy nhiều task chung một JVM.

Và điều đau nhất: **cluster manager không sửa sai giúp bạn**. YARN/K8s cấp đúng cái bạn xin. Xin ngu thì nhận ngu. Standalone còn tệ hơn: xin quá tay thì app treo WAITING vô hạn (bạn sẽ tự gây ra điều này trong lab hôm nay).

### Vì sao không có nút "auto"?

Có các nỗ lực (Databricks autoscaling, YARN elastic) nhưng chúng chỉ co giãn SỐ executor — còn **hình dáng** một executor (bao nhiêu core, bao nhiêu GB, bao nhiêu overhead) vẫn là quyết định của bạn, vì nó phụ thuộc bản chất job: shuffle nhiều hay ít, có Python worker không, có cache không. Máy không biết những điều đó — bạn biết.

### Trade-off tổng quát

| Được (khi sizing đúng) | Mất (chi phí của việc sizing) |
|---|---|
| Tận dụng ~90% tài nguyên trả tiền | Phải hiểu memory model (may là lesson 21 học rồi) |
| Job ổn định, không OOM ngẫu nhiên | Con số đúng thay đổi theo job — không copy mù được |
| GC lành mạnh, không executor "khựng" | Phải đo lại khi dữ liệu tăng trưởng |

> Bài học Senior: sizing không phải tìm con số HOÀN HẢO — nó là **loại trừ các vùng chắc chắn sai** (heap quá to cho GC, core quá nhiều cho I/O, thiếu overhead cho Python) rồi tinh chỉnh bằng số liệu từ Spark UI. Công thức cho điểm xuất phát tốt; UI cho điểm kết thúc.

---

## 3. Theory

### 3.1. Nhắc lại bản đồ memory (từ lesson 21, giờ nhìn từ góc sizing)

```
┌─ RAM một node 64 GB ──────────────────────────────────────────┐
│  OS + daemon (NodeManager/kubelet...)           ~1 GB  ← CHỪA │
│  ┌─ Container/pod của 1 executor ─────────────────────┐       │
│  │  spark.executor.memoryOverhead  ← NGOÀI heap:      │       │
│  │   (max(384MB, 10% heap) mặc định)                  │       │
│  │   - buffer network/shuffle (Netty)                 │       │
│  │   - PYTHON WORKER của PySpark UDF sống Ở ĐÂY!      │       │
│  │  ┌─ spark.executor.memory (JVM heap) ────────┐     │       │
│  │  │  reserved 300MB                            │     │       │
│  │  │  unified memory (execution + storage) 60%  │     │       │
│  │  │  user memory 40%                           │     │       │
│  │  └─────────────────────────────────────────────┘    │       │
│  └────────────────────────────────────────────────────┘       │
│  (×số executor trên node)                                     │
└───────────────────────────────────────────────────────────────┘
Container thật = executor.memory + memoryOverhead  ← YARN/K8s tính TIỀN theo cái này
```

Sai lầm phổ biến nhất: nghĩ `--executor-memory 8g` nghĩa là executor dùng 8 GB. Không — nó dùng 8 GB heap **+ overhead** (~800 MB nữa). Xin container theo heap mà quên overhead → YARN kill container với thông báo huyền thoại *"Container killed by YARN for exceeding memory limits"*.

### 3.2. BÀI TOÁN KINH ĐIỂN — lời giải mẫu từng bước

> **Đề**: Cluster 10 node, mỗi node 16 core, 64 GB RAM (YARN). Hãy đề xuất `--num-executors`, `--executor-cores`, `--executor-memory`.

**Bước 1 — Chừa phần cho OS/daemon trên MỖI node.**
Mỗi node phải nuôi OS, NodeManager/kubelet, datanode... Chuẩn mực: chừa **1 core + 1 GB** (cluster lớn/nhiều daemon thì chừa hơn).

```
Khả dụng mỗi node: 16 - 1 = 15 core;  64 - 1 = 63 GB
```

**Bước 2 — Chọn core mỗi executor = 5.**
Vì sao không 15 (fat) hay 1 (tiny)? Kinh nghiệm được kiểm chứng rộng rãi (từ benchmark của Cloudera): **quá ~5 task ghi đồng thời, throughput của HDFS client trong một JVM suy giảm** (tranh chấp connection/buffer khi nhiều luồng cùng ghi qua một client). 5 core giữ được concurrency tốt mà chưa chạm trần I/O. Con số này cũng cho GC một heap vừa phải (bước 4).

```
executor-cores = 5
```

**Bước 3 — Số executor mỗi node, và tổng.**

```
executor/node = floor(15 / 5) = 3
tổng executor thô = 3 × 10 node = 30
```

**Bước 4 — Memory mỗi executor, TRỪ overhead.**
Mỗi node 63 GB chia 3 executor → mỗi **container** được 21 GB. Nhưng container = heap + overhead, overhead mặc định = max(384 MB, 10% heap) ⇒ heap ≈ container / 1.10:

```
container/executor = 63 / 3 = 21 GB
heap = 21 / 1.10 ≈ 19 GB  → --executor-memory 19g
overhead ≈ 2 GB           (mặc định tự tính; PySpark thì set tay cao hơn — 3.4)
```

**Bước 5 — Trừ suất của DRIVER.**
Chạy cluster mode thì driver cũng sống trong cluster, chiếm chỗ tương đương 1 executor:

```
num-executors = 30 - 1 = 29
```

**Đáp số:**

```bash
spark-submit --master yarn --deploy-mode cluster \
  --num-executors 29 \
  --executor-cores 5 \
  --executor-memory 19g \
  --driver-memory 19g --driver-cores 5 \
  app.py
# Tổng parallelism = 29 × 5 = 145 task đồng thời
```

Trên bảng trắng, 5 dòng là đủ: `15 core/node → 5 core/exec → 3 exec/node → 30 exec → 21GB/exec = 19g heap + 2g overhead → trừ driver còn 29`.

> **Analogy đội thợ xây**: node là khu nhà trọ 15 giường. Executor là một TỔ thợ thuê chung phòng. Tổ 15 người (fat) — cãi nhau khi cùng dùng một cầu thang (GC, HDFS client). Tổ 1 người (tiny) — ai cũng phải tự mua riêng bộ đồ nghề (broadcast copy), không ai san việc cho nhau được (không chia sẻ memory trong JVM). Tổ 5 người — vừa xinh.

### 3.3. Fat executor vs Tiny executor — bảng trade-off

| | Tiny (1 core/exec, 63 exec... ) | **Vàng (~5 core)** | Fat (15 core/exec, 10 exec) |
|---|---|---|---|
| GC | heap nhỏ, GC nhanh | heap ~20 GB, GC ổn | heap ~60 GB → **GC pause dài**, executor "đơ" từng đợt |
| Broadcast | copy ra 63 JVM — lãng phí RAM & network | copy ra ~30 JVM | copy ra 10 JVM — tốt nhất |
| Chia sẻ memory giữa task cùng JVM | không (1 task/JVM) | tốt | tốt |
| HDFS/S3 write throughput | ổn | ổn | **suy giảm** (>5 luồng ghi/JVM) |
| Chịu lỗi | mất 1 executor = mất 1 task đang chạy | mất 5 task + shuffle file vừa phải | mất 1 executor = mất 15 task + đống shuffle file to |
| Overhead cố định | 63 × (JVM + overhead tối thiểu 384MB) — phí chồng phí | cân bằng | ít suất overhead nhất |

Kết luận: hai thái cực đều thua vì những lý do KHÁC NHAU — interviewer muốn nghe bạn kể được cả hai phía.

### 3.4. memoryOverhead — khi nào phải tăng tay?

`spark.executor.memoryOverhead` mặc định `max(384MB, 0.10 × heap)`. Tăng tay khi:

1. **PySpark có UDF / pandas UDF / RDD API** — LÝ DO SỐ 1: mỗi core sinh một **python worker process**, và RAM của Python nằm **ngoài heap JVM**, tức là ăn vào overhead. Job Python nặng mà để overhead 10% → YARN/K8s kill container dù heap còn trống. Chuẩn PySpark: overhead 15–25%, hoặc Spark 3+ dùng riêng `spark.executor.pyspark.memory` để khoanh vùng phần Python.
2. Đọc/ghi nhiều **off-heap buffer**: Kafka client, Netty shuffle lớn, Arrow (`toPandas`/pandas UDF dùng Arrow — lại là PySpark!).
3. Dùng `spark.memory.offHeap.enabled=true` — phần off-heap đó cũng phải nằm trong container limit.

Triệu chứng nhận diện: container bị **KILL bởi YARN/K8s (exit 137, OOMKilled)** trong khi UI không thấy heap đầy → thiếu overhead, không phải thiếu heap. (Heap đầy thật thì thấy `java.lang.OutOfMemoryError` trong log executor — lesson 40 mổ xẻ.)

### 3.5. Số partition khớp tổng core — nguyên tắc wave

```
Tổng slot = num-executors × executor-cores = 29 × 5 = 145

partition = 145?   → 1 wave: đẹp trên giấy, thực tế task lệch nhau
                     → task chậm nhất giữ cả job (straggler hết chỗ trốn)
partition = 300–450 (2–3× slot) → 2–3 wave: task nhỏ hơn, slot nào xong
                     nhận việc tiếp → tự cân bằng, straggler ít đau
partition = 50.000 → task 5ms, overhead lên lịch > thời gian làm việc
```

Quy tắc: **`spark.sql.shuffle.partitions` ≈ 2–3 × tổng core**, đồng thời mỗi partition nhắm ~128 MB (lesson 22). Hai ràng buộc mâu thuẫn thì ưu tiên cỡ partition (đừng để partition 2 GB chỉ vì muốn đúng 2× core). Spark 3 AQE (`spark.sql.adaptive.enabled=true`, mặc định bật từ 3.2) tự gộp partition nhỏ sau shuffle — nhưng AQE chỉ GỘP xuống được, con số ban đầu vẫn nên hợp lý.

### 3.6. Ví dụ sizing theo dataset: job đọc 100 GB

Cluster như 3.2 (29 exec × 5 core × 19 GB). Job: đọc 100 GB Parquet, join + groupBy, ghi ra lake.

1. **Partition đầu vào**: 100 GB / 128 MB ≈ **800 partition** — Spark tự cắt khi đọc, khỏi can thiệp.
2. **Slot**: 145 → 800 task chạy ~5–6 wave. Chấp nhận được (wave nhiều hơn 3 một chút không phải tội — đừng vì thế mà xin gấp đôi cluster).
3. **Memory kiểm tra nhanh**: mỗi task cầm ~128 MB đầu vào; 5 task/executor ≈ 640 MB dữ liệu thô cùng lúc, nở ra khi decompress + hash table của join/groupBy (nhân 3–5×) ≈ 2–3 GB execution memory. Executor có ~19 GB heap × 60% unified ≈ 11 GB → dư địa thoải mái, kể cả skew nhẹ.
4. **Shuffle partitions**: set 400 (≈ 2.7× slot) hoặc để AQE lo với target 128 MB.
5. **Có cần cả 29 executor không?** 100 GB là job vừa. Nếu cluster dùng chung → dynamic allocation min 5 / max 29, để job khác còn thở.

Nguyên tắc rút gọn: **memory theo cỡ PARTITION (không phải cỡ dataset!), số core theo deadline**. Dataset 1 TB không cần executor to hơn — chỉ cần nhiều wave hơn hoặc nhiều executor hơn.

### 3.7. Dynamic allocation min/max — đặt sàn/trần thế nào

```
minExecutors  = đủ cho phần "xương sống" luôn cần (đọc metadata, ghi cuối) — thường 2–5
maxExecutors  = trần THEO TÍNH TOÁN SIZING (không phải ∞!) — vd 29 ở trên
initialExecutors = ước lượng pha đầu để khỏi chờ scale-up — vd = max nếu job ngắn
executorIdleTimeout = 60s là hợp lý cho batch
```

Bẫy: để `maxExecutors` mặc định (vô hạn) trên cluster chung → một job skew xin điên cuồng, chiếm cả cluster. Trần là cam kết công dân tốt.

### 3.8. Sizing STREAMING khác batch — ổn định > burst

Batch: chạy nhanh nhất có thể rồi trả máy → sizing cho THROUGHPUT đỉnh.
Streaming: chạy 24/7 → sizing cho **độ ổn định ở tải trung bình + chịu được đỉnh**, vì:

| Khía cạnh | Batch | Streaming |
|---|---|---|
| Mục tiêu | tổng thời gian ngắn | **batch duration < trigger interval**, mọi batch |
| Dữ liệu mỗi lượt | cả dataset | micro-batch nhỏ → executor ÍT & NHỎ hơn nhiều |
| Memory phải cộng thêm | hash/join tạm | **STATE** (aggregation window, dedup, stream-stream join) sống lâu dài trong executor |
| Dynamic allocation | rất hợp | thận trọng: co giãn làm rung batch time, state phải load lại; thường size TĨNH theo p95 tải |
| GC | pause thi thoảng, chịu được | pause = batch trễ = lag dây chuyền → heap NHỎ càng quý |

Cách làm thực tế: đo `processedRowsPerSecond` một executor chịu được (chạy thử 1 executor), lấy `input rate p95 / rate một executor` + 30% dự phòng. Và nhớ: nguồn Kafka bị chặn trên bởi **số partition Kafka** — 8 partition Kafka thì core thứ 9 ngồi không (lesson 27).

---

## 4. Internal

Điều gì thật sự xảy ra với con số bạn xin — đường đi của `--executor-memory 19g` trên YARN:

```
① spark-submit gửi yêu cầu: container 19g heap
        │
② Spark cộng overhead: 19g × 1.10 ≈ 20.9g → làm tròn LÊN theo
   yarn.scheduler.minimum-allocation-mb (thường 1g) → xin container ~21g
        │
③ YARN kiểm tra: node còn ≥21g trong yarn.nodemanager.resource.memory-mb?
   Còn → cấp. Không → chờ (app PENDING một phần)
        │
④ Executor JVM khởi động với -Xmx19g bên trong container 21g
        │
⑤ Từ đây 2 "cảnh sát" khác nhau canh 2 vùng:
   - JVM canh heap: vượt 19g → java.lang.OutOfMemoryError (executor tự chết)
   - YARN/K8s canh CẢ container: process tree vượt 21g
     → KILL từ bên ngoài (exit code 137 / "killed by YARN")
     ← python worker của PySpark ăn RAM là vượt kiểu NÀY
```

Hiểu bước ⑤ là hiểu 90% ca "OOM bí ẩn": stacktrace Java OOM = thiếu heap (xem lại execution memory, skew); bị kill exit 137 không stacktrace = thiếu **overhead** (tăng memoryOverhead, nghi ngờ Python/Arrow/off-heap).

Còn trên **standalone** (cluster Docker của bạn): worker chỉ có `SPARK_WORKER_MEMORY=1G, CORES=1`. Master xếp app theo kiểu "đủ thì cấp, không đủ thì để app **WAITING**" — không lỗi, không cảnh báo ồn ào, chỉ im lặng chờ. Lab hôm nay bạn sẽ tự giăng bẫy này để nhận diện nó cả đời không quên.

---

## 5. API

### Bộ tứ flag sizing

```bash
--num-executors 29        # YARN/K8s. Standalone KHÔNG có flag này →
                          #   dùng --conf spark.cores.max=145 (tổng core toàn app)
--executor-cores 5        # task đồng thời mỗi executor
--executor-memory 19g     # HEAP của executor JVM
--driver-memory 19g       # heap driver — to khi collect/broadcast nhiều
```

- **Pitfall**: trên standalone, `--num-executors` bị lờ đi; số executor = f(cores.max, worker). Đây là khác biệt hay bị hỏi.

### `spark.executor.memoryOverhead` / `spark.executor.pyspark.memory`

```bash
--conf spark.executor.memoryOverhead=4g          # PySpark UDF nặng: 15–25% thay vì 10%
--conf spark.executor.pyspark.memory=3g          # Spark 3: khoanh riêng phần python worker
```

- **Khi dùng**: mọi job PySpark có UDF/pandas UDF/toPandas; job đọc Kafka/Arrow nhiều.
- **Pitfall**: tăng overhead thì phải GIẢM heap tương ứng nếu container size cố định — tổng không tự nở ra.

### `spark.sql.shuffle.partitions` + AQE

```bash
--conf spark.sql.shuffle.partitions=400 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true
```

- **Pitfall**: mặc định 200 — quá to cho cluster học tập (task tí hon), quá nhỏ cho cluster trăm core (partition 2 GB). Không có job production nghiêm túc nào nên chạy với con số mặc định chưa suy nghĩ.

### Dynamic allocation

```bash
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=29 \
--conf spark.dynamicAllocation.executorIdleTimeout=60s
```

- **Pitfall**: quên điều kiện shuffle (external shuffle service / shuffleTracking — lesson 37) → executor bị thu mang theo shuffle file, stage sau fetch fail, retry bão táp.

---

## 6. Demo nhỏ

Chứng minh "xin quá tay = WAITING im lặng" trên cluster Docker (worker chỉ có 1 core/1G):

```
Input:  app xin executor 2 GB trên worker 1 GB
   ↓    master không đủ hàng để cấp
Output: app treo WAITING — nhìn thấy trên :8080
```

```bash
# Terminal 1 — xin quá khả năng worker:
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  /opt/spark/examples/src/main/python/pi.py 10
# → treo, log lặp lại: "Initial job has not accepted any resources;
#    check your cluster UI to ensure that workers are registered
#    and have sufficient resources"
```

Mở `http://localhost:8080`: app ở trạng thái **WAITING**, cột Cores/Memory cho thấy worker 1 core/1G không gánh nổi yêu cầu 2G. Ctrl+C, chạy lại với `--executor-memory 512m` → chạy ngay. Thông báo *"Initial job has not accepted any resources"* là một trong những dòng log đáng thuộc lòng nhất của Spark — từ nay bạn biết nó nghĩa là "sizing lệch với thực tế cluster", không phải bug code.

---

## 7. Production Example

Một team DE vận hành cluster YARN dùng chung 10 node × 16 core × 64 GB, chạy 3 loại job — cùng cluster, ba "đơn thuốc" khác nhau:

```
┌─ Job A: ETL đêm 500 GB (batch nặng, một mình một cõi 2h–4h sáng) ──┐
│  29 exec × 5 core × 19g (bài giải 3.2) — ăn trọn cluster           │
│  shuffle.partitions=450, AQE on                                     │
├─ Job B: PySpark scoring có pandas UDF, ban ngày, cluster chung ────┤
│  dynamicAllocation min=2 max=10                                     │
│  executor: 5 core, HEAP 14g + OVERHEAD 5g   ← Python ăn off-heap!  │
│  spark.executor.pyspark.memory=4g                                   │
├─ Job C: Structured Streaming Kafka 24/7 (12 partition Kafka) ──────┤
│  TĨNH: 3 exec × 4 core × 6g (12 slot = 12 partition Kafka)         │
│  KHÔNG dynamic allocation — giữ batch time phẳng, state tại chỗ     │
│  heap nhỏ chủ đích: GC pause ngắn hơn trigger 30s                   │
└─────────────────────────────────────────────────────────────────────┘
```

Ba bài học doanh nghiệp trong hình:
1. **Không có cấu hình chung cho mọi job** — sizing là thuộc tính CỦA JOB, không của cluster.
2. Job B minh họa nguyên tắc PySpark: tổng container vẫn 19–21g, nhưng **tỷ lệ heap/overhead xoay lại** cho Python.
3. Job C minh họa slot = partition Kafka, tĩnh, heap nhỏ — ổn định thắng tốc độ đỉnh.

---

## 8. Hands-on Lab

**Mục tiêu**: giải bài sizing trên giấy + đối chứng hành vi cấp phát trên cluster Docker thật.

### Bước 0 — chuẩn bị

```bash
make up
mkdir -p labs/lab38
```

### Bước 1 — `labs/lab38/sizing.py`: máy tính sizing của riêng bạn

```python
# labs/lab38/sizing.py — chạy bằng python thường, không cần Spark
import math, sys

nodes, cores, ram_gb = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
cores_avail, ram_avail = cores - 1, ram_gb - 1          # chừa OS/daemon
cores_per_exec = 5 if cores_avail >= 5 else cores_avail
exec_per_node  = cores_avail // cores_per_exec
container_gb   = ram_avail / exec_per_node
heap_gb        = math.floor(container_gb / 1.10)         # trừ overhead 10%
overhead_gb    = round(container_gb - heap_gb, 1)
total_exec     = exec_per_node * nodes - 1               # trừ driver

print(f"executor/node={exec_per_node}  num-executors={total_exec}")
print(f"executor-cores={cores_per_exec}  executor-memory={heap_gb}g  overhead~{overhead_gb}g")
print(f"tổng slot={total_exec * cores_per_exec}  → shuffle.partitions gợi ý="
      f"{2 * total_exec * cores_per_exec}–{3 * total_exec * cores_per_exec}")
```

```bash
python3 labs/lab38/sizing.py 10 16 64    # kiểm tra khớp lời giải mẫu 3.2
python3 labs/lab38/sizing.py 20 32 128   # cluster khác — giải lại tức thì
```

### Bước 2 — quan sát cấp phát trên standalone

```python
# labs/lab38/observe_alloc.py
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("lab38-alloc").getOrCreate()
sc = spark.sparkContext
print(">>> defaultParallelism =", sc.defaultParallelism)
df = spark.range(0, 10_000_000, numPartitions=12)
out = df.withColumn("k", F.col("id") % 100).groupBy("k").count()
out.write.mode("overwrite").parquet("/workspace/labs/lab38/out")
input(">>> Mở :4040 tab Executors + Stages, :8080 xem app. Enter để thoát...")
spark.stop()
```

Chạy 3 kịch bản, mỗi lần ghi lại (Executors tab: mấy executor, mấy core; Stages tab: mấy task, mấy wave):

```bash
# (a) mặc định — ăn hết những gì worker có (1 core):
make run F=labs/lab38/observe_alloc.py

# (b) giới hạn tường minh:
docker exec -it spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 512m --conf spark.cores.max=1 \
  --conf spark.sql.shuffle.partitions=4 \
  /workspace/labs/lab38/observe_alloc.py

# (c) xin quá tay (demo mục 6) — executor-memory 2g → WAITING, chụp màn hình :8080
```

### Bước 3 — nhìn thấy wave

Với 1 core mà stage có 12 task → UI Stages/Timeline cho thấy 12 wave chạy nối đuôi. Đổi `numPartitions=3` chạy lại → 3 wave. Đây chính là quan hệ partition/slot bằng hình ảnh.

### Bước 4 — (tùy chọn, máy khỏe) thêm worker để tăng slot

```bash
docker compose -f docker-compose.spark.yaml up -d --scale spark-worker=2
```

Chạy lại (a): :8080 hiện 2 worker, app nhận 2 executor, stage 12 task còn 6 wave. Ghi vào `labs/lab38/NOTES.md`: *"thêm executor giảm số wave — với điều kiện có đủ partition để chia"*.

---

## 9. Assignment

**Easy** — (bám ROADMAP) Điền và giải thích: tổng parallelism = `num-executors × executor-cores`. Cluster 29 exec × 5 core, stage có 800 partition → mấy wave? Nếu partition = 100 thì chuyện gì xảy ra với 45 slot thừa? Từ đó phát biểu quan hệ lý tưởng giữa số partition và tổng slot bằng lời của bạn.

**Medium** — (bám ROADMAP) Job PySpark cần giữ ~50 GB dữ liệu trong executor memory (cache + shuffle), driver cần 2 GB. Cụm YARN cho container tối đa 24 GB/node. Thiết kế: bao nhiêu executor, heap/overhead mỗi executor bao nhiêu để tổng UNIFIED memory ≥ 50 GB (nhớ: heap × 0.6 mới là unified — lesson 21), và vì sao "1 executor 50 GB" là đáp án sai dù toán học cộng đủ.

**Hard** — (bám ROADMAP) Job đang chạy 29 exec × 5 core, chậm hơn deadline 2×. Phân tích 3 phương án: (a) tăng num-executors lên 58; (b) tăng executor-cores lên 10; (c) không tăng gì, sửa số partition. Với MỖI phương án: khi nào nó giúp, khi nào vô dụng, nhìn metric nào trên Spark UI để quyết định (gợi ý: task time có đều không? slot có idle không? GC time? spill?). Kết luận bằng cây quyết định 6–8 dòng.

**Production Challenge** — Viết "sizing review checklist" 10 mục cho team (dạng bảng: câu hỏi → cách kiểm tra → ngưỡng đỏ), dùng được khi review PR có thay đổi spark-submit config. Bắt buộc bao gồm: overhead cho PySpark, partition vs slot, fat executor, dynamic allocation trần, khác biệt batch/streaming. Sau đó dùng chính checklist chấm cấu hình Job B ở mục 7 — nó pass hết không?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Sizing quan hệ trực tiếp với mọi trục performance đã học:

| Quyết định sizing | Hệ quả performance | Nhìn thấy ở đâu |
|---|---|---|
| Heap quá nhỏ so với cỡ partition | Shuffle/aggregation SPILL xuống disk — chậm 5–10× | Stages tab: cột Spill (Memory/Disk) |
| Heap quá to (fat executor) | GC pause dài, task thi thoảng đứng hình | Executors tab: GC Time đỏ (>10% task time) |
| Thiếu overhead (PySpark) | Container bị kill exit 137, retry liên tục | Log YARN/K8s, KHÔNG có Java stacktrace |
| Partition << slot | Slot idle, trả tiền máy đứng nhìn | Executors tab: active tasks < cores |
| Partition >> hợp lý (task <100ms) | Overhead lên lịch nuốt thời gian thật | Stages: task duration median vài chục ms |
| Streaming heap to + GC | batch duration nhọn răng cưa vượt trigger | Structured Streaming tab (lesson 39 alert) |

Chu trình đúng: **công thức → chạy → mở UI đọc spill/GC/idle → chỉnh một biến → lặp**. Sizing một lần rồi quên là sizing sai từ tháng thứ hai (dữ liệu lớn lên mỗi ngày).

---

## 11. Spark UI

Ba chỗ trên UI trở thành "đồng hồ đo sizing" từ hôm nay:

**Tab Executors** — bảng tổng kết tài nguyên:
- Cột **Cores / Active Tasks**: active thường xuyên < cores → thừa slot (partition ít hoặc job tuần tự).
- Cột **GC Time** so với Task Time: > ~10% là heap có vấn đề (thường là fat executor hoặc cache tràn).
- Cột **Storage Memory** (đã dùng/tổng): tổng chính là unified pool — đối chiếu với con số bạn TÍNH (heap × 0.6) để kiểm tra mình hiểu đúng.

**Tab Stages** — chi tiết một stage:
- **Tasks total** vs tổng slot → đếm wave.
- **Summary metrics**: min/median/max duration — max >> median là skew (lesson 23), đừng chữa skew bằng cách đổ thêm memory cho cả cluster.
- Cột **Spill (Disk)**: khác 0 nghĩa là execution memory không đủ cho task này — tăng heap/giảm cỡ partition.

**Master UI :8080** (standalone): cột Memory/Cores của worker vs yêu cầu app — nơi chẩn đoán WAITING trong 5 giây.

---

## 12. Common Mistakes

1. **Copy cấu hình job khác/blog mà không tính lại** — sizing phụ thuộc node size, loại job, PySpark hay không. Con số đúng của người khác là con số sai của bạn.
2. **Quên memoryOverhead khi tính chia node** → 3 executor × (19g heap + 2g lén lút) = 63g... vừa khít trên giấy, tràn ngoài đời vì bạn chia 63 cho heap chứ không cho container.
3. **PySpark UDF nặng + overhead mặc định 10%** → chuỗi container killed exit 137 "bí ẩn". Python worker sống NGOÀI heap — phải nuôi nó bằng overhead/pyspark.memory.
4. **"Job chậm → tăng gấp đôi memory"** theo phản xạ — memory chỉ chữa spill/OOM; chậm vì thiếu slot, skew, hay shuffle to thì thêm RAM là đốt tiền. Đọc UI trước, chỉnh sau.
5. **1 wave "cho đẹp"** (partition = slot) → task chậm nhất định đoạt cả stage. 2–3 wave cho scheduler chỗ để cân bằng.
6. **Fat executor 1 node = 1 executor** vì "đỡ overhead" → GC pause chục giây + HDFS throughput tụt + một executor chết là thảm họa. Trên 5–8 core/executor phải có lý do rất tốt.
7. **Streaming dùng nguyên đơn thuốc batch** (executor to, dynamic allocation hăng hái) → batch time răng cưa, state di cư, lag. Streaming ưu tiên ổn định: tĩnh, nhỏ, heap gọn.
8. **Không chừa gì cho OS/daemon** → node "đủ 16 core cho Spark" nhưng kubelet/NodeManager đói → node flapping, executor lost hàng loạt — lỗi nhìn như hardware nhưng là toán trừ.

---

## 13. Interview

**Junior:**

1. *Cluster 10 node × 16 core × 64 GB, cấu hình executor thế nào?* — Chừa 1 core + 1 GB/node cho OS → 15 core, 63 GB. 5 core/executor → 3 executor/node → 30 tổng. 63/3 = 21 GB container, trừ ~10% overhead → 19 GB heap. Trừ 1 suất driver → `--num-executors 29 --executor-cores 5 --executor-memory 19g`.
2. *Vì sao chừa tài nguyên cho OS?* — Node còn chạy OS, NodeManager/kubelet, monitoring agent... Không chừa thì các daemon đói tài nguyên → node bất ổn, executor lost, lỗi tưởng hardware nhưng do cấp phát.
3. *`--executor-memory` có phải toàn bộ RAM executor dùng?* — Không, đó chỉ là heap JVM. Cộng thêm memoryOverhead (mặc định max(384MB, 10%)) cho off-heap: network buffer, python worker... Container thật = heap + overhead, và cluster manager kill theo container.
4. *Tổng số task chạy đồng thời tính thế nào?* — num-executors × executor-cores. Ví dụ 29 × 5 = 145. Partition nhiều hơn thì xếp wave; ít hơn thì slot idle.

**Mid:**

5. *Vì sao 5 core/executor mà không phải 15 hay 1?* — >~5 luồng ghi HDFS đồng thời qua một JVM client thì throughput suy giảm; heap của executor 15 core cũng quá to → GC pause dài. Còn 1 core/executor mất chia sẻ memory/broadcast trong JVM và nhân bản overhead cố định. 5 là điểm cân bằng được kiểm chứng rộng rãi.
6. *Fat vs tiny executor — trade-off?* — Fat: ít suất overhead, broadcast ít bản copy, NHƯNG GC heap to đau, HDFS throughput giảm, một executor chết mất nhiều task + shuffle file. Tiny: GC nhẹ, cô lập lỗi tốt, NHƯNG broadcast nhân bản ra quá nhiều JVM, không chia sẻ memory, tổng overhead lớn. Chọn giữa: ~5 core, heap 10–20 GB.
7. *Khi nào tăng memoryOverhead? Nhận diện thiếu overhead bằng gì?* — PySpark UDF/pandas UDF (python worker ngoài heap), Arrow, Kafka buffer, off-heap. Nhận diện: container bị YARN/K8s kill (exit 137/OOMKilled) mà KHÔNG có java.lang.OutOfMemoryError, heap trên UI chưa đầy. Java OOM stacktrace thì ngược lại — thiếu heap.
8. *Chọn `spark.sql.shuffle.partitions` thế nào?* — Điểm xuất phát: 2–3× tổng core để có vài wave tự cân bằng, đồng thời giữ mỗi partition ~128 MB; ưu tiên ràng buộc cỡ partition khi mâu thuẫn. Spark 3 bật AQE để tự gộp partition nhỏ sau shuffle, nhưng vẫn cần con số ban đầu hợp lý.

**Senior:**

9. *Job chậm gấp đôi deadline — thêm executor hay thêm core mỗi executor? Trình bày cách quyết định.* — Không quyết trước khi đọc UI. (a) Slot đang idle (active < cores) hoặc ít partition → thêm tài nguyên VÔ ÍCH, sửa partition trước. (b) Task đều và slot bận kín → thiếu parallelism thật: thêm EXECUTOR (scale ngang) an toàn hơn thêm core (5→10 core đụng trần I/O + heap phải to theo). (c) Max task >> median → skew: thêm gì cũng gần vô ích, xử lý skew (salting/AQE skew join). (d) GC/spill cao → vấn đề memory chứ không phải compute. Câu trả lời đúng là một CÂY QUYẾT ĐỊNH dựa trên metric, không phải con số.
10. *Sizing job streaming 24/7 khác gì batch, cụ thể bạn làm gì khác?* — Mục tiêu đổi từ throughput đỉnh sang batch duration < trigger interval Ổn ĐỊNH: (1) executor ít + heap nhỏ chủ đích để GC pause ngắn hơn trigger; (2) cộng memory cho STATE sống lâu (window/dedup/join state) chứ không chỉ dữ liệu batch; (3) slot khớp số partition Kafka — thừa core là vô ích; (4) tránh/dè dặt dynamic allocation vì co giãn gây rung batch time; size tĩnh theo p95 input rate + ~30% dự phòng; (5) benchmark một executor để có đơn vị đo trước khi nhân.

---

## 14. Summary

### Mindmap

```
                      RESOURCE SIZING (L38)
                             │
   ┌───────────────┬─────────┴────────┬───────────────────┐
   ▼               ▼                  ▼                   ▼
CÔNG THỨC GỐC    MEMORY MODEL      PARTITION/SLOT      BIẾN THỂ
   │               │                  │                   │
 -1 core -1GB    container =        slot = exec×core    100GB: mem theo
  cho OS          heap + OVERHEAD   partition = 2–3×     PARTITION, không
 5 core/exec      overhead 10%       slot (wave)          theo dataset
 (HDFS I/O)       PYSPARK → 15-25%  ~128MB/partition    dyn.alloc: có TRẦN
 3 exec/node      (python worker     AQE gộp hộ         streaming: TĨNH,
 heap = /1.1       ngoài heap!)     1 wave = bẫy         nhỏ, ổn định,
 -1 cho driver    137 vs Java OOM    straggler            + state, GC ngắn
```

### Cheat sheet (in ra dán màn hình)

```
cores_avail = cores/node - 1            ram_avail = ram/node - 1GB
executor-cores = 5                      exec/node = cores_avail // 5
container = ram_avail / exec_per_node   heap = container / 1.10
num-executors = exec/node × nodes - 1   overhead = container - heap
slots = num-executors × 5               shuffle.partitions = 2–3 × slots
PySpark UDF? overhead → 15–25% (hoặc pyspark.memory riêng)
partition đầu vào ~128MB | spill>0 → thiếu heap | exit137 → thiếu overhead
GC>10% → heap to/cache tràn | streaming: tĩnh, nhỏ, slot=Kafka partitions
```

### Checklist trước khi gõ "Continue"

- [ ] Giải lại bài 10×16×64 trên giấy không nhìn tài liệu, đủ 5 bước.
- [ ] Giải thích được nguồn gốc con số 5 core/executor.
- [ ] Phân biệt Java OOM (thiếu heap) vs exit 137 (thiếu overhead) và thuốc cho từng bệnh.
- [ ] Nói được vì sao PySpark cần overhead cao hơn.
- [ ] Chọn shuffle.partitions cho cluster bất kỳ kèm lập luận wave.
- [ ] Kể 3 khác biệt sizing streaming vs batch.
- [ ] Đã tự gây ra và chẩn đoán trạng thái WAITING trên :8080.

---

## 15. Next Lesson

**Lesson 39 — Monitoring & alerting: metrics, event log, history server.**

Bạn đã deploy đúng mode (L37), cấp tài nguyên đúng toán (L38). Nhưng production có quy luật sắt: **thứ không được giám sát sẽ hỏng vào lúc bạn nghỉ phép**. Lesson 39 xây hệ thần kinh cho pipeline: event log + History Server để "người chết kể chuyện" (job fail 3h sáng, 9h sáng bạn vẫn mở được UI đầy đủ của nó), metrics sink sang Prometheus/Grafana, và quan trọng nhất — chọn ĐÚNG vài metric đáng đánh thức người ta dậy, thay vì 500 dashboard không ai nhìn.

Spark UI bạn dùng suốt 38 bài là công cụ NHÌN — giờ ta dạy hệ thống tự nhìn thay mình.

> Gõ **"Continue"** khi sẵn sàng.
