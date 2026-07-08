# Lesson 40 — Debugging playbook: OOM, stragglers, stuck jobs

> Module 6 · Production Engineering · Tuần 21 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Phương pháp debug có hệ thống**: triệu chứng → giả thuyết → bằng chứng (UI/log) → fix. Không đoán mò, không "tăng memory lên xem sao".
- **Catalog lỗi kinh điển** của Spark production: executor OOM, driver OOM, container killed by YARN, PicklingError, executor lost, straggler, job stuck, FetchFailedException, small tasks.
- **Quy trình đọc log 4 tầng**: driver log → executor log → event log → GC log. Mỗi tầng trả lời một loại câu hỏi khác nhau.
- Cách **phân biệt skew với hardware issue** — hai bệnh cùng triệu chứng (straggler), thuốc hoàn toàn khác nhau.

Sau bài này bạn phải làm được:

- Nhận một stack trace bất kỳ, nói được trong 30 giây: lỗi ở driver hay executor, thuộc họ lỗi nào, mở tab nào của Spark UI để xác nhận.
- Tự tay gây ra 3 lỗi kinh điển trên cluster (lab), rồi chẩn đoán chúng CHỈ bằng log và UI — như thể bạn không biết trước nguyên nhân.
- Viết được runbook debug cho team: on-call 3 giờ sáng, pipeline chết, làm gì trước làm gì sau.

Kiến thức dùng trong thực tế: đây là bài **ăn lương** nhất khóa học. Viết job Spark thì ai cũng viết được sau 6 tháng; chẩn đoán job chết trong 15 phút thay vì 2 ngày mới là thứ phân biệt mid với senior. Interviewer hỏi "kể một lần bạn debug Spark job" ở MỌI vòng phỏng vấn senior DE.

---

## 2. Why

### Vấn đề: Spark chết rất... khó hiểu

Một chương trình Python thường chết với traceback chỉ thẳng dòng code lỗi. Spark thì không, vì:

1. **Lỗi xảy ra xa nơi gây ra nó.** Bạn viết `join` sai ở dòng 40, nhưng lazy evaluation dồn mọi thứ lại — stack trace nổ ở dòng 80 chỗ `write()`, trên một executor ở máy khác, sau 45 phút chạy.
2. **Lỗi bị bọc nhiều lớp.** Một PicklingError của Python bị bọc trong `Py4JJavaError`, bọc trong `SparkException: Job aborted`, kèm 200 dòng stack trace JVM không liên quan. Junior đọc từ trên xuống và lạc lối; senior biết tìm dòng `Caused by:` cuối cùng.
3. **Lỗi hiển thị không phải lỗi gốc.** `FetchFailedException` trông như lỗi network, nhưng 80% trường hợp nguyên nhân gốc là một executor ĐÃ CHẾT vì OOM từ 2 phút trước. Fix network thì vô ích.
4. **Có những "lỗi" không có exception nào cả**: job đứng im 40 phút, không fail, không progress. Không có stack trace để google.

### Nếu không có playbook thì sao?

Bạn sẽ debug kiểu "voodoo": tăng `executor.memory` gấp đôi → vẫn chết → tăng tiếp → hết quota cluster → đổ lỗi cho infra. Tôi đã thấy team tăng executor từ 8 GB lên 64 GB để "fix" một lỗi mà nguyên nhân thật là skew trên 1 key — 64 GB vẫn chết, vì partition skew đó 90 GB. Playbook tồn tại để thay thói quen đoán bằng thói quen **tìm bằng chứng**.

> **Analogy bác sĩ cấp cứu**: bệnh nhân đau bụng (triệu chứng). Bác sĩ giỏi không mổ ngay — họ đặt giả thuyết (ruột thừa? dạ dày?), yêu cầu xét nghiệm (siêu âm = Spark UI, xét nghiệm máu = log), rồi mới kê thuốc. Bác sĩ dở kê kháng sinh liều cao cho mọi bệnh — giống DE dở tăng memory cho mọi lỗi.

### Vòng lặp debug chuẩn — dán lên màn hình

```
┌──────────────────────────────────────────────────────────────┐
│  ① TRIỆU CHỨNG      job fail? chậm? đứng im? kết quả sai?    │
│        │             ghi lại NGUYÊN VĂN message + thời điểm   │
│        ▼                                                      │
│  ② GIẢ THUYẾT       lỗi này thuộc HỌ nào? (OOM/skew/serde/   │
│        │             network/config) — dùng catalog §3        │
│        ▼                                                      │
│  ③ BẰNG CHỨNG       mở UI tab nào? grep log nào? tìm gì?     │
│        │             bằng chứng phải XÁC NHẬN hoặc BÁC BỎ     │
│        │             giả thuyết — không được "chắc là vậy"    │
│        ▼                                                      │
│  ④ FIX NHỎ NHẤT     đổi ĐÚNG 1 thứ, chạy lại, so sánh.       │
│        │             Fix ăn → ghi vào runbook.                │
│        └──── không ăn → quay lại ② với giả thuyết mới ────────┘
└──────────────────────────────────────────────────────────────┘
```

Quy tắc vàng: **mỗi lần chỉ đổi 1 biến**. Đổi 3 config cùng lúc mà job chạy được, bạn không học được gì — lần sau lại mò từ đầu.

---

## 3. Theory — Catalog lỗi kinh điển

Mỗi lỗi theo format: **[Triệu chứng / Stack trace mẫu / Nguyên nhân / Fix]**. Đây là phần để tra cứu suốt sự nghiệp — bookmark lại.

### 3.1. Executor OOM

**Triệu chứng**: task fail rồi retry, vài executor biến mất, job chết sau 4 lần retry. UI tab Executors có dòng đỏ "Dead".

**Stack trace mẫu**:
```
ExecutorLostFailure (executor 7 exited caused by one of the running tasks)
Reason: java.lang.OutOfMemoryError: Java heap space
    at org.apache.spark.util.collection.ExternalAppendOnlyMap.growTable(...)
```

**Nguyên nhân** — 4 thủ phạm phổ biến, phân biệt bằng bằng chứng:

| Thủ phạm | Bằng chứng trên UI/log |
|---|---|
| **Shuffle quá lớn** — hash map của aggregate/join không vừa execution memory | Stages tab: Shuffle Read của stage fail rất lớn; log có `ExternalAppendOnlyMap`/`spill` trước khi chết |
| **Cache tham lam** — persist bảng to bằng MEMORY_ONLY chiếm hết storage memory | Storage tab: Fraction Cached cao; Executors tab: Storage Memory kịch trần |
| **Skew** — 1 partition khổng lồ dồn vào 1 task | Stages tab → Summary Metrics: max Shuffle Read gấp 10–100× median. CHỈ task đó chết, executor khác sống khỏe |
| **Quá nhiều core/executor** — 8 core = 8 task chia nhau 1 cục heap; mỗi task được quá ít | Config: `executor.cores` cao mà `executor.memory` thấp (vd 8 core / 8 GB = 1 GB/task) |

**Fix theo đúng thủ phạm**: shuffle to → tăng `spark.sql.shuffle.partitions` (mỗi partition nhỏ đi) hoặc bật AQE; cache → hạ xuống `MEMORY_AND_DISK` hoặc unpersist; skew → salting/AQE skew join (lesson 19); nhiều core → giảm `executor.cores` xuống 4–5. Tăng `executor.memory` là fix ĐÚNG chỉ khi dữ liệu mỗi task thật sự cần nhiều hơn — và là fix cuối cùng, không phải đầu tiên.

### 3.2. Driver OOM

**Triệu chứng**: application chết CẢ CỤM một phát (driver chết = mọi thứ chết — lesson 1), hoặc driver đơ dần rồi mất liên lạc. Spark UI (do driver phục vụ) cũng chết theo — dấu hiệu nhận dạng quan trọng.

**Stack trace mẫu** (nằm ở DRIVER log, không phải executor):
```
java.lang.OutOfMemoryError: Java heap space
    at java.util.Arrays.copyOf(...)
    at org.apache.spark.sql.Dataset.collectToPython(...)
```

**Nguyên nhân** — 3 thủ phạm:

1. **`collect()`/`toPandas()` bảng to** — kéo cả bảng về RAM driver. Bằng chứng: stack trace có `collectToPython`/`collectAsList`.
2. **Broadcast bảng lớn** — broadcast join phải build bảng TRÊN DRIVER trước khi phát đi. Broadcast bảng 4 GB với driver 2 GB = chết. Bằng chứng: stack trace có `TorrentBroadcast`/`broadcastExchange`.
3. **Quá nhiều task metadata** — job có 500.000 task (đọc triệu small files, shuffle partitions quá cao): driver phải giữ trạng thái từng task + accumulator + listener event. Bằng chứng: không có collect/broadcast nào, nhưng số task khổng lồ trong UI, driver chết từ từ kèm GC liên tục.

**Fix**: (1) bỏ collect, dùng `write`/`show`/`take`; (2) hạ `spark.sql.autoBroadcastJoinThreshold` hoặc bỏ hint broadcast; (3) giảm số partition/task — compact small files (lesson 21), hạ `shuffle.partitions`. Tăng `driver.memory` chỉ khi kết quả cần collect thật sự nhỏ-nhưng-không-nhỏ-lắm (vài trăm MB) và bạn hiểu tại sao.

### 3.3. Container killed by YARN (vượt memory limit)

**Triệu chứng**: executor chết nhưng KHÔNG có `OutOfMemoryError` nào trong log của nó — nó bị giết từ bên ngoài.

**Stack trace mẫu**:
```
Container killed by YARN for exceeding memory limits. 5.5 GB of 5.5 GB
physical memory used. Consider boosting spark.yarn.executor.memoryOverhead.
```

**Nguyên nhân**: đây KHÔNG phải heap OOM. Tổng memory một container = heap (`executor.memory`) + **overhead** (off-heap: buffer network, thread stack, và đặc biệt **PYTHON WORKER PROCESS** của PySpark). YARN đo tổng vật lý; vượt là bắn bỏ, không hỏi han. PySpark bị nhiều nhất vì Python UDF/pandas UDF chạy trong process Python riêng, ăn memory NGOÀI heap JVM — heap còn trống mà container vẫn bị giết.

**Fix**: tăng `spark.executor.memoryOverhead` (mặc định chỉ max(10% executor memory, 384 MB) — quá ít cho PySpark nặng UDF; thực tế PySpark thường cần 15–25%). Spark 3.x có riêng `spark.executor.pyspark.memory` để giới hạn phần Python. Fix gốc rễ hơn: bỏ UDF, dùng built-in functions (lesson 12). Lưu ý bài học chuyển vị: trên Kubernetes, hiện tượng tương đương là pod bị **OOMKilled** (exit code 137) — cùng bệnh, khác giấy khai tử.

### 3.4. Task serialization error — PicklingError

**Triệu chứng**: job chết NGAY LẬP TỨC khi action đầu tiên chạy — chưa xử lý byte dữ liệu nào. Chết-ngay-lập-tức là chữ ký của họ lỗi này.

**Stack trace mẫu** (log driver, phần Python):
```
_pickle.PicklingError: Could not serialize object: TypeError:
cannot pickle '_thread.lock' object
    at pyspark/serializers.py ... in dumps
```

**Nguyên nhân**: hàm bạn đưa vào UDF/`foreach`/RDD map phải được **pickle** gửi đến executor. Closure của hàm vô tình "bắt" (capture) một object không serialize được: connection database, client Kafka, file handle, logger, hoặc cả... `SparkSession` (`spark` là object driver-side, không bao giờ pickle được). Python pickle cả những gì hàm tham chiếu, không chỉ thân hàm.

```python
conn = psycopg2.connect(...)          # object driver-side, có socket bên trong

@F.udf("string")
def lookup(x):
    return conn.query(x)              # ← closure bắt `conn` → PicklingError
```

**Fix**: tạo tài nguyên **bên trong** executor thay vì gửi từ driver — với `foreachPartition`, mở connection ở đầu partition, đóng ở cuối (1 connection/partition thay vì 1/dòng); hoặc dùng biến module-level lazy-init. Với lookup data thuần: broadcast variable. Tổng quát: hỏi "hàm này chạy Ở ĐÂU?" trước khi hỏi "hàm này làm gì?".

### 3.5. Executor lost / heartbeat timeout

**Triệu chứng & stack trace mẫu**:
```
ExecutorLostFailure (executor 3 exited unrelated to the running tasks)
Reason: Executor heartbeat timed out after 128331 ms
```

**Nguyên nhân** (theo thứ tự xác suất): (1) **GC storm** — executor bận full GC hàng chục giây, không kịp gửi heartbeat, driver tưởng chết. Bản chất vẫn là memory gần cạn; (2) executor thật sự chết vì OOM/bị YARN-K8s giết (đọc §3.1/§3.3 — kiểm tra log CỦA executor đó); (3) node hỏng/network phân vùng; (4) **spot instance bị thu hồi** (lesson 42).

**Fix**: xem GC Time trong Executors tab — GC > 15% task time là bệnh memory, quay về §3.1. Nếu do mạng chập chờn thật, tăng `spark.network.timeout` (mặc định 120s) là thuốc giảm đau hợp lệ, nhưng đừng dùng nó để che bệnh GC.

### 3.6. Straggler — skew hay hardware?

**Triệu chứng**: stage có 200 task, 199 task xong trong 10s, 1 task chạy 900s. Progress bar `199/200` treo mãi.

Hai nguyên nhân khác nhau một trời một vực:

| | **Skew** (bệnh dữ liệu) | **Hardware/environment** (bệnh máy) |
|---|---|---|
| Bằng chứng UI | Task chậm có Shuffle Read/Input **lớn hơn hẳn** các task khác (Stages → sort theo Duration, so cột Input/Shuffle Read) | Task chậm có input **bằng** task khác nhưng vẫn chậm; các task khác trên CÙNG executor đó cũng chậm |
| Test quyết định | **Rerun job: VẪN chậm ở cùng key/partition đó** — dữ liệu đi theo job | Rerun: chậm ở executor/node KHÁC hoặc hết chậm — bệnh đi theo máy |
| Fix | Salting, AQE skew join, broadcast bảng nhỏ (lesson 19–20) | Bật speculative execution (`spark.speculation=true` — chạy bản sao task rùa trên máy khác, lấy bản xong trước); decommission node bệnh |

Câu "rerun cùng chậm = skew" đáng giá một câu hỏi phỏng vấn senior nguyên con.

### 3.7. Job stuck — không fail, không progress

**Triệu chứng**: không exception, progress đứng im 30+ phút. Khó nhất vì không có gì để google.

**3 giả thuyết + cách lấy bằng chứng**:

1. **Deadlock tài nguyên**: job chưa từng bắt đầu — chờ mãi không được cấp executor. Bằng chứng: Executors tab trống trơn (chỉ có driver); Master UI (:8080) cho thấy cluster hết core/memory hoặc app khác chiếm hết. Log driver lặp lại `Initial job has not accepted any resources...`. Fix: đòi ít tài nguyên hơn, hoặc giải phóng app đang chiếm.
2. **Chờ shuffle fetch**: task đang chạy nhưng ì ạch kéo shuffle block từ executor quá tải/sắp chết. Bằng chứng: Stages tab → task đang RUNNING có Shuffle Read Blocked Time cao.
3. **GC storm**: executor sống dở chết dở, 90% thời gian full GC. Bằng chứng: Executors tab → cột GC Time đỏ rực.

**Vũ khí chuyên dụng — thread dump**: Executors tab → click "Thread Dump" của executor nghi vấn (đọc 2–3 lần cách nhau 30s). Thread `Executor task launch worker` đang RUNNABLE ở frame nào? — `sun.nio.ch...read` lặp lại = chờ network/fetch; frame liên quan GC/alloc = memory; WAITING trên lock lạ = deadlock code (hay gặp với UDF gọi service ngoài không timeout). Thread dump là "chụp X-quang" duy nhất cho job stuck — senior nào cũng phải đọc được.

### 3.8. FetchFailedException

**Stack trace mẫu**:
```
org.apache.spark.shuffle.FetchFailedException:
Failed to connect to spark-worker-3/10.0.1.7:7337
```

**Nguyên nhân**: reducer (stage sau) đến lấy shuffle block mà mapper (stage trước) đã ghi — nhưng executor giữ block **không còn ở đó**. 80% trường hợp: executor đó ĐÃ chết vì OOM/bị giết → đây là **lỗi thứ cấp**. Nguyên nhân gốc nằm ở chỗ khác, xảy ra TRƯỚC đó.

**Fix**: đừng chữa message này. Tìm trong log/UI xem executor nguồn chết **vì sao** (thường quay về §3.1/§3.3/§3.5), chữa bệnh đó. Spark cũng tự chữa một phần: stage trước được chạy lại để tái tạo shuffle data (bạn thấy stage "resubmitted" trong UI — dấu hiệu nhận biết). Phòng ngừa hạ tầng: external shuffle service / node decommissioning để shuffle data sống sót khi executor chết.

### 3.9. Quá nhiều small task

**Triệu chứng**: job không chết nhưng chậm vô lý; UI cho thấy stage có 50.000 task, mỗi task chạy 30ms nhưng scheduler delay + deserialize còn lâu hơn chạy thật; driver ăn CPU cao.

**Nguyên nhân**: triệu small files ở nguồn (lesson 21), hoặc `shuffle.partitions` để 2000 cho dữ liệu 200 MB. Chi phí điều phối một task (~vài ms–chục ms mỗi task, cộng dồn ở driver) vượt chi phí xử lý. Cũng chính là thủ phạm số 3 của driver OOM (§3.2).

**Fix**: compact files nguồn; `coalesce` sau filter mạnh; bật AQE (`spark.sql.adaptive.coalescePartitions.enabled=true` — Spark 3 tự gộp partition nhỏ sau shuffle). Nhắm mốc partition 100–200 MB.

---

## 4. Internal — Quy trình đọc log 4 tầng

Spark có 4 nguồn log, mỗi tầng trả lời một loại câu hỏi. Đọc sai tầng = mất hàng giờ.

```
┌─ TẦNG 1: DRIVER LOG ────────────────────────────────────────┐
│ Ở đâu: stdout/stderr của spark-submit (client mode);         │
│        YARN AM log / K8s driver pod log (cluster mode)       │
│ Trả lời: application chết VÌ SAO (mọi lỗi cuối cùng nổi lên  │
│ đây), job/stage nào fail, lỗi Python driver-side             │
│ Kỹ thuật: đọc NGƯỢC từ cuối; tìm "Caused by:" CUỐI CÙNG      │
│ trong chuỗi — đó là lỗi gốc, các lớp trên chỉ là giấy gói    │
└──────────────────────────────────────────────────────────────┘
┌─ TẦNG 2: EXECUTOR LOG ──────────────────────────────────────┐
│ Ở đâu: standalone: work dir trên worker (UI :8081 → stderr); │
│        YARN: `yarn logs -applicationId <id>`; K8s: pod logs  │
│ Trả lời: executor CỤ THỂ chết vì sao (OOM thật? bị kill?),   │
│ task error đầy đủ, log Python worker/UDF, spill messages     │
│ Kỹ thuật: driver log nói "executor 7 lost" → mở đúng log     │
│ executor 7, nhìn những dòng CUỐI trước khi im lặng           │
└──────────────────────────────────────────────────────────────┘
┌─ TẦNG 3: EVENT LOG (History Server — lesson 39) ────────────┐
│ Ở đâu: spark.eventLog.dir (JSON từng event)                  │
│ Trả lời: job ĐÃ CHẾT/qua đêm — dựng lại toàn bộ Spark UI     │
│ sau khi app kết thúc; so sánh run hôm nay vs hôm qua          │
│ (task metrics, shuffle size) — trend chính là bằng chứng     │
└──────────────────────────────────────────────────────────────┘
┌─ TẦNG 4: GC LOG ────────────────────────────────────────────┐
│ Bật: spark.executor.extraJavaOptions=-Xlog:gc*:stdout        │
│      (JDK11+; JDK8: -XX:+PrintGCDetails)                     │
│ Trả lời: nghi án memory mà heap chưa nổ — "Full GC" dày đặc, │
│ heap sau GC vẫn ~kịch trần = memory pressure thật sự;        │
│ pause dài giải thích heartbeat timeout (§3.5)                │
└──────────────────────────────────────────────────────────────┘
```

Thứ tự thực chiến khi có sự cố: **UI trước, log sau** (UI là bản đồ, log là hiện trường) → driver log tìm `Caused by:` cuối → khoanh executor nghi vấn → executor log của đúng nó → nếu job đã chết mất UI thì History Server → nếu nghi memory âm ỉ thì GC log. Job stuck (không có lỗi để đọc) → thread dump (§3.7).

---

## 5. API

Debug không có "API" theo nghĩa DataFrame, nhưng có bộ đồ nghề config + lệnh:

### Config chẩn đoán nên bật sẵn ở production

```python
spark = (SparkSession.builder
    .appName("prod-job")
    # Event log: không có nó, job chết lúc 3h sáng = mất sạch hiện trường
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/workspace/labs/lab40/event-logs")
    # Thuốc đặc trị straggler do máy (KHÔNG trị skew!)
    .config("spark.speculation", "true")
    .getOrCreate())
```

- **Pitfall**: `spark.speculation` với sink không idempotent (ghi JDBC, gọi API) có thể ghi ĐÚP — task sao chép cũng chạy thật. Chỉ an toàn với write atomic (file output committer, Iceberg).

### `spark.sparkContext.setJobDescription(desc)`

```python
spark.sparkContext.setJobDescription("silver: dedup orders")
```
- **Ý nghĩa**: đặt tên job trên UI. Pipeline 20 job đều tên `save at NativeMethodAccessorImpl` thì debug như tìm người trong đám đông không ai đeo bảng tên.

### Retry knobs — hiểu để đọc log, đừng vội chỉnh

- `spark.task.maxFailures` (mặc định 4): task fail 4 lần → cả job chết. Vì thế 1 lỗi trong log xuất hiện ~4 lần — bạn đang xem RETRY, không phải 4 lỗi khác nhau.
- `spark.stage.maxConsecutiveAttempts` (4): số lần resubmit stage khi FetchFailed. Thấy stage chạy đi chạy lại = có FetchFailed ở đâu đó (§3.8).
- **Pitfall**: tăng maxFailures để job "sống lâu hơn" chỉ trì hoãn cái chết và làm log dài gấp đôi.

### Lệnh soi container/process (môi trường Docker của khóa)

```bash
docker stats                                   # memory/CPU từng container thời gian thực
docker logs spark-mastery-spark-worker-1       # log worker daemon (thấy executor bị kill)
docker exec <container> jps                    # liệt kê JVM process (executor còn sống?)
docker exec <container> jstack <pid>           # thread dump bằng tay (khi UI không mở được)
```

---

## 6. Demo nhỏ

Đọc một stack trace thật theo đúng vòng lặp §2. Job fail với đoạn log (rút gọn):

```
ERROR TaskSetManager: Task 17 in stage 3.0 failed 4 times; aborting job
org.apache.spark.SparkException: Job aborted due to stage failure:
Task 17 in stage 3.0 failed 4 times, most recent failure:
Lost task 17.3 in stage 3.0 (TID 214) (10.0.1.7 executor 2):
ExecutorLostFailure (executor 2 exited caused by one of the running tasks)
Reason: Container killed by YARN for exceeding memory limits.
5.6 GB of 5.5 GB physical memory used.
Consider boosting spark.yarn.executor.memoryOverhead.
```

Mổ xẻ từng dòng:

1. `failed 4 times` → maxFailures=4 đã cạn; `17.3` = lần retry thứ 3 của task 17. Đây là MỘT lỗi lặp lại, không phải bốn.
2. `executor 2` tại `10.0.1.7` → biết chính xác phải lấy executor log nào.
3. `Container killed by YARN` → catalog §3.3: KHÔNG phải heap OOM (không có chữ `java.lang.OutOfMemoryError`), mà bị giết từ ngoài vì tổng memory vật lý.
4. `5.6 GB of 5.5 GB` → vượt đúng ~100 MB — kiểu vượt "sát nút" kinh điển của Python worker ăn off-heap.
5. Giả thuyết: job PySpark có UDF, overhead mặc định không đủ. Bằng chứng cần thêm: stage 3 có UDF không (SQL tab → plan có `BatchEvalPython`)? Fix nhỏ nhất: `spark.executor.memoryOverhead=1g`, chạy lại, so sánh.

Chú ý cái bẫy tâm lý: message gợi ý sẵn fix (`Consider boosting...`) — lần này gợi ý đúng, nhưng nếu nguyên nhân là UDF ngốn vô hạn thì tăng overhead chỉ dời vạch đích. Luôn đối chiếu catalog trước khi tin lời an ủi của framework.

---

## 7. Production Example

Sự cố có thật (đã đổi tên) — pipeline gold layer kiểu Olist ở một công ty e-commerce:

**3:05 AM** — PagerDuty: job `gold_seller_daily` fail. On-call mở driver log: `FetchFailedException: Failed to connect to worker-14`.

**Đường đúng mà on-call đã đi** (tổng 25 phút):

1. Nhớ catalog §3.8: FetchFailed = lỗi thứ cấp trong 80% trường hợp. KHÔNG ping mạng, không restart cluster vội.
2. History Server (job chết rồi, UI live không còn): executor 41 trên worker-14 chết lúc 3:01, TRƯỚC FetchFailed 3 phút. → nạn nhân đầu tiên là executor 41.
3. `yarn logs` của executor 41: `Container killed by YARN... 8.2 GB of 8 GB`. → §3.3.
4. Câu hỏi then chốt: **hôm qua vẫn chạy, sao hôm nay chết?** So event log 2 ngày: shuffle read stage join tăng 4× — team marketing vừa chạy campaign, seller lớn nhất tăng đơn 40× → **skew mới sinh** đẩy 1 task vượt memory.
5. Fix đêm: bật AQE skew join + tăng overhead, rerun, xong lúc 3:40, SLA 6 AM an toàn. Fix gốc (tuần sau): salting cho key seller + alert khi shuffle size lệch baseline 2× (lesson 39).

Bài học: chuỗi nhân quả thật là **skew → container killed → FetchFailed**. Người debug theo message cuối cùng (FetchFailed) sẽ đi chữa network — sai hai tầng nhân quả. Người có playbook đi ngược dòng thời gian về nạn nhân đầu tiên.

---

## 8. Hands-on Lab

**Mục tiêu**: tự gây 3 lỗi kinh điển trên cluster thật (worker chỉ có 1 GB — "phòng thí nghiệm OOM" hoàn hảo), rồi chẩn đoán bằng log/UI như thể chưa biết nguyên nhân.

### Bước 0 — chuẩn bị

```bash
make up
mkdir -p labs/lab40
docker ps   # đủ 3 container: master, worker, submit
```

Mở sẵn 2 tab trình duyệt: Master UI `http://localhost:8080`, và `http://localhost:4040` (chỉ sống khi app đang chạy).

### Bước 1 — Ca bệnh 1: driver OOM bằng collect — `labs/lab40/bug1_driver_oom.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("bug1-driver-oom")
         # driver bé để chết nhanh — đừng làm thế ở production nhé
         .config("spark.driver.memory", "512m")
         .getOrCreate())

# Nhân bản orders lên ~3M dòng rồi kéo TẤT CẢ về driver 512MB
orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True)
big = orders.crossJoin(spark.range(30))   # ~100k × 30 = ~3M dòng

rows = big.collect()                       # ← hiện trường vụ án
print(len(rows))
```

```bash
make run F=labs/lab40/bug1_driver_oom.py
```

**Chẩn đoán** (ghi vào NOTES.md): stack trace có `java.lang.OutOfMemoryError` kèm frame nào (`collectToPython`?)? Executor có lỗi gì không (mở worker log — dự đoán: executor vô tội, thậm chí đã làm xong việc)? Vì sao UI :4040 cũng tắt ngúm? Đối chiếu §3.2.

### Bước 2 — Ca bệnh 2: skew straggler — `labs/lab40/bug2_skew_straggler.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("bug2-skew")
         .config("spark.sql.shuffle.partitions", "8")
         # Tắt hai "bác sĩ tự động" để bệnh lộ nguyên hình
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .getOrCreate())

# 95% dòng dồn vào key 'HOT', phần còn lại chia đều 10k key
skewed = (spark.range(2_000_000)
    .withColumn("key", F.when(F.rand() < 0.95, F.lit("HOT"))
                        .otherwise(F.concat(F.lit("k"), (F.rand()*10000).cast("int"))))
    .withColumn("payload", F.sha2(F.col("id").cast("string"), 256)))

dim = spark.range(10001).withColumn(
    "key", F.when(F.col("id") == 0, F.lit("HOT"))
            .otherwise(F.concat(F.lit("k"), F.col("id") - 1))).drop("id")

out = skewed.join(dim, "key").groupBy("key").agg(F.count("*").alias("n"))
out.orderBy(F.desc("n")).show(5)
input(">>> GIỮ app sống. Mở http://localhost:4040 → Stages. Enter để thoát.")
```

```bash
make run F=labs/lab40/bug2_skew_straggler.py
```

**Chẩn đoán** trong lúc `input()` giữ UI sống: Stages → stage join → Summary Metrics — Duration và Shuffle Read của task max gấp mấy lần median? Đây là chữ ký skew (§3.6). **Test quyết định**: chạy lại lần 2 — vẫn đúng partition đó chậm chứ? Sau đó đổi `adaptive.enabled` thành `true`, chạy lại lần 3: AQE có cứu được không, số partition sau shuffle thay đổi ra sao?

### Bước 3 — Ca bệnh 3: PicklingError — `labs/lab40/bug3_pickling.py`

```python
import threading
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("bug3-pickling").getOrCreate()

class FakeDbClient:
    """Mô phỏng connection/client — chứa lock nên KHÔNG pickle được."""
    def __init__(self):
        self._lock = threading.Lock()
    def status_label(self, s):
        return f"[{s.upper()}]"

client = FakeDbClient()                    # sống ở DRIVER

@F.udf(StringType())
def label_status(s):
    return client.status_label(s)          # ← closure bắt client → BOOM

orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True)
orders.select(label_status("order_status")).show(5)
```

```bash
make run F=labs/lab40/bug3_pickling.py
```

**Chẩn đoán**: job chết trước hay sau khi task chạy (nhìn UI/log — có job nào kịp sinh ra không)? Tìm dòng `PicklingError` — nó nêu tên object nào (`_thread.lock`)? **Fix tại chỗ** rồi chạy lại cho pass: tạo client BÊN TRONG udf (mỗi lần gọi — chậm nhưng chạy), rồi phiên bản tốt hơn: `mapInPandas` hoặc biến module-level khởi tạo lười. Ghi lại cả hai cách.

### Bước 4 — Tổng kết hiện trường

Viết `labs/lab40/NOTES.md`: bảng 3 ca bệnh × (triệu chứng nguyên văn / dòng log quyết định / tab UI đã dùng / fix). Đây chính là trang đầu runbook cá nhân của bạn.

---

## 9. Assignment

**Easy** — Không chạy code, chỉ dùng catalog §3. Cho 3 mẩu log, gọi tên họ lỗi + nêu bằng chứng cần lấy thêm:
1. `Reason: Executor heartbeat timed out after 130 seconds` (Executors tab: GC Time = 48%)
2. `_pickle.PicklingError: Could not serialize object: TypeError: cannot pickle 'socket' object`
3. `FetchFailedException` xuất hiện lúc 02:14, và Executors tab có executor Dead từ 02:11.

**Medium** — Straggler kép: lab bug2 cho bạn straggler do skew. Hãy thiết kế thí nghiệm gây straggler do "máy chậm" GIẢ trên cluster này (gợi ý: UDF `time.sleep` có điều kiện theo `TaskContext.get().partitionId()` — ngủ ở partition ngẫu nhiên mỗi lần chạy). Chạy cả hai loại, chứng minh bằng UI rằng tiêu chí phân biệt §3.6 (input size + tính lặp lại khi rerun) tách được chúng.

**Hard** — Job stuck thật: viết job có UDF gọi `time.sleep(3600)` ở đúng 1 dòng dữ liệu. Job sẽ treo. Không kill vội — lấy thread dump qua UI (Executors → Thread Dump) VÀ qua `docker exec ... jstack`, tìm đúng thread đang ngủ, chụp bằng chứng. Sau đó trả lời: nếu đây là production và UDF này gọi API bên thứ ba, bạn đề xuất 2 lớp phòng thủ nào? (gợi ý: timeout trong UDF + speculation? speculation có rủi ro gì với API call?)

**Production Challenge** — Viết `RUNBOOK-spark-oncall.md` (1 trang) cho team tưởng tượng của bạn: nhận alert lúc 3h sáng → 5 bước đầu tiên làm gì (theo thứ tự, mỗi bước ghi rõ MỞ GÌ và TÌM GÌ) → bảng quyết định "họ lỗi → hành động tạm thời được phép làm ngay" → tiêu chí khi nào được rerun, khi nào phải gọi người. Dựa hoàn toàn trên catalog §3 và quy trình 4 tầng log §4.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Debug và performance là hai mặt một đồng xu — lỗi hôm nay là job-chậm ngày mai chưa nổ:

| Quan sát | Ý nghĩa performance | Ngưỡng hành động |
|---|---|---|
| GC Time / Task Time (Executors tab) | Memory pressure âm ỉ — tiền OOM | > 10–15% → điều tra memory ngay, đừng đợi nổ |
| Max vs median task duration (Stages) | Skew đang lớn dần theo dữ liệu | max > 3–5× median → lên lịch fix trước khi thành sự cố |
| Spill (Memory/Disk) trong Stages tab | Execution memory không đủ, đang trả bằng disk I/O | Spill xuất hiện đều đặn → tăng partitions hoặc memory |
| Task duration median < 100ms, số task hàng chục nghìn | Overhead scheduling > công việc thật (§3.9) | Gộp partition, compact files |
| Stage "resubmitted" trong UI | Có FetchFailed — đang trả giá chạy lại cả stage | Truy nguyên executor chết (§3.8) |

Thói quen senior: các chỉ số trên phải nằm trong **báo cáo sức khỏe hàng tuần** của pipeline (lesson 39 metrics), không phải thứ chỉ mở ra xem khi cháy nhà. Sự cố mục 7 hoàn toàn phát hiện được TRƯỚC 1 ngày nếu có alert shuffle-size-lệch-baseline.

---

## 11. Spark UI

Bài này dùng UI như phòng xét nghiệm — bảng tra "triệu chứng → tab":

| Nghi vấn | Tab | Nhìn cột/mục nào |
|---|---|---|
| Executor OOM | Executors | Dead executors; GC Time; Storage Memory kịch trần |
| Skew | Stages → stage nghi vấn | Summary Metrics: min/median/max của Duration + Shuffle Read; Event Timeline thấy 1 thanh dài lêu nghêu |
| Job stuck | Executors | **Thread Dump** (chụp 2–3 lần, so sánh); Active Tasks đứng im |
| Driver OOM | (UI chết theo driver!) | Bản thân việc UI biến mất là bằng chứng; dùng driver log + History Server |
| Shuffle bệnh | Stages | Shuffle Read Blocked Time; Spill (Memory/Disk) |
| UDF nghi phạm | SQL/DataFrame | Plan có node `BatchEvalPython` = có Python UDF trong đường đạn |
| Nhiều app giành tài nguyên | Master UI :8080 (standalone) | Cores/Memory in use; app WAITING |

Mẹo môi trường lab: UI :4040 chết theo app — với job đã fail, dùng event log (đã bật ở §5) + History Server (lesson 39), hoặc thêm `input()` chặn cuối script khi thí nghiệm.

---

## 12. Common Mistakes

1. **Chữa message cuối cùng thay vì lỗi gốc** — FetchFailed → đi tăng network timeout, trong khi executor chết vì OOM từ trước. Luôn hỏi: "cái gì chết ĐẦU TIÊN?"
2. **Tăng memory như phản xạ đầu gối.** 4 thủ phạm executor OOM (§3.1) thì 3 cái không chữa được bằng memory. Skew 1 key 90 GB — tăng kiểu gì?
3. **Đổi 3 config một lúc.** Job chạy được nhưng không biết vì đâu = chưa debug xong, chỉ là hên. Lần sau tái phát lại mò từ đầu.
4. **Đọc stack trace từ trên xuống rồi google dòng đầu tiên.** Dòng đầu là giấy gói (`Job aborted...`). Lỗi gốc ở `Caused by:` cuối cùng.
5. **Không bật event log ở production.** Job chết lúc 3h sáng, UI chết theo, không event log = hiện trường bị dọn sạch, chẩn đoán bằng... tưởng tượng.
6. **Nhầm 4 lần retry là 4 lỗi khác nhau** — hoảng loạn vì "log đầy lỗi" trong khi chỉ có 1 task fail × maxFailures=4.
7. **Bật speculation để "trị skew".** Speculation chạy bản sao task chậm trên máy khác — với skew, bản sao xử lý CÙNG partition to đó, chậm y hệt, tốn gấp đôi tài nguyên. Speculation trị bệnh MÁY, không trị bệnh DỮ LIỆU.

---

## 13. Interview

**Junior:**

1. *Executor OOM và driver OOM khác nhau thế nào về triệu chứng và nguyên nhân điển hình?* — Executor OOM: từng task/executor chết, job retry rồi mới chết, app và UI còn sống một lúc; nguyên nhân: shuffle lớn, cache, skew, quá nhiều core/executor. Driver OOM: cả application sập một phát, Spark UI chết theo; nguyên nhân: collect/toPandas bảng to, broadcast bảng lớn, quá nhiều task metadata.
2. *Đọc một stack trace Spark dài 200 dòng, bắt đầu từ đâu?* — Từ dưới lên: tìm `Caused by:` cuối cùng — đó là lỗi gốc; các lớp ngoài (`SparkException: Job aborted`) chỉ là wrapper. Ghi nhận executor id + stage/task id trong message để biết lấy log nào tiếp.
3. *`collect()` gây lỗi gì, thay bằng gì?* — Kéo toàn bộ dữ liệu về heap driver → driver OOM. Thay bằng `show`/`take` để xem mẫu, `write` ra storage cho kết quả đầy đủ, chỉ collect sau aggregate khi chắc chắn kết quả nhỏ.
4. *Task fail một lần thì job chết ngay không?* — Không, driver retry đến `spark.task.maxFailures` (mặc định 4) lần, có thể trên executor khác; quá số đó stage fail → job chết. Vì thế một lỗi thường xuất hiện ~4 lần trong log.

**Mid:**

5. *"Container killed by YARN for exceeding memory limits" — có phải heap OOM không? Fix gì?* — Không. Heap còn sống nhưng TỔNG memory vật lý (heap + off-heap + Python worker) vượt limit container nên YARN kill từ bên ngoài. Đặc biệt hay gặp ở PySpark vì UDF chạy trong process Python ngoài heap. Fix: tăng `spark.executor.memoryOverhead` (PySpark thường cần 15–25% thay vì 10% mặc định), giới hạn bằng `spark.executor.pyspark.memory`, hoặc bỏ UDF dùng built-in. Trên K8s hiện tượng tương đương là OOMKilled/exit 137.
6. *PicklingError xảy ra khi nào, vì sao, fix ra sao?* — Khi hàm gửi đến executor (UDF/foreach/map) có closure bắt object không serialize được: DB connection, Kafka client, lock, SparkSession. Python pickle cả môi trường tham chiếu của hàm chứ không chỉ thân hàm. Dấu hiệu: job chết ngay lập tức trước khi task nào chạy. Fix: tạo tài nguyên bên trong executor (foreachPartition mở/đóng connection theo partition, lazy module-level init), dữ liệu lookup thì broadcast.
7. *Straggler: làm sao biết skew hay hardware?* — Ba bằng chứng: (1) input/shuffle read của task chậm — to hơn hẳn là skew, bằng nhau là máy; (2) rerun job — vẫn chậm ở cùng key/partition là skew (bệnh theo dữ liệu), chậm chỗ khác/hết chậm là máy; (3) các task khác trên cùng executor cũng chậm → máy. Fix skew: salting/AQE/broadcast; fix máy: speculation, loại node.
8. *FetchFailedException nghĩa là gì và vì sao không nên chữa trực tiếp nó?* — Reducer không lấy được shuffle block vì executor giữ block đã biến mất — thường do executor đó chết trước đó vì OOM/bị kill. Là lỗi THỨ CẤP: phải truy nạn nhân đầu tiên trong timeline và chữa bệnh của nó. Spark tự resubmit stage trước để tái tạo shuffle data; hạ tầng phòng ngừa: external shuffle service/decommissioning.

**Senior:**

9. *Job đứng im 40 phút, không lỗi, không progress — quy trình của bạn?* — (1) Executors tab: có executor nào không? Không có → deadlock tài nguyên, kiểm tra cluster manager UI xem ai chiếm (log driver có "Initial job has not accepted any resources"). (2) Có executor, có active task → thread dump 2–3 lần cách 30s: thread task worker kẹt ở network read = chờ shuffle fetch từ executor quá tải; frame GC = GC storm (đối chiếu GC Time); WAITING trên lock = deadlock trong code/UDF gọi ngoài không timeout. (3) Stages tab: Shuffle Read Blocked Time. Điểm mấu chốt interviewer chờ: nhắc đến THREAD DUMP — công cụ duy nhất nhìn vào trong một process sống mà im lặng.
10. *Thiết kế "debuggability" cho pipeline Spark mới ở công ty — bạn chuẩn bị gì TRƯỚC khi có sự cố?* — (1) Event log + History Server bắt buộc mọi môi trường — hiện trường phải được bảo tồn; (2) `setJobDescription` cho mọi action — UI đọc được bằng tên nghiệp vụ; (3) log 4 tầng tập trung (driver/executor về log system, GC log bật sẵn ở prod); (4) baseline metrics theo run (duration, shuffle size, spill, GC%) + alert khi lệch ×2 — phát hiện skew/small-files TRƯỚC khi nổ; (5) runbook: catalog lỗi → tab UI → hành động tạm thời được phép; (6) config an toàn: speculation chỉ cho sink idempotent, maxFailures mặc định. Tư duy cần thể hiện: sự cố là chuyện "khi nào" chứ không phải "nếu", nên đầu tư vào khả năng chẩn đoán rẻ hơn nhiều lần đầu tư vào việc không bao giờ lỗi.

---

## 14. Summary

### Mindmap

```
                       DEBUGGING PLAYBOOK
                              │
    ┌──────────────┬──────────┴──────────────┬──────────────────┐
    ▼              ▼                         ▼                  ▼
 PHƯƠNG PHÁP    CATALOG LỖI               LOG 4 TẦNG         PHÂN BIỆT KHÓ
    │              │                         │                  │
 triệu chứng    OOM executor (4 thủ phạm)  ① driver: vì sao   skew vs máy:
 → giả thuyết   OOM driver (collect/        chết, Caused by    rerun cùng chậm
 → bằng chứng    broadcast/task metadata)  ② executor: ai      = skew
 → fix 1 thứ    YARN kill (overhead—        chết, chết sao    FetchFailed =
 mỗi lần         PySpark!)                 ③ event log:        lỗi thứ cấp,
                PicklingError (closure)     job đã chết        tìm nạn nhân
 stuck job →    heartbeat (GC storm)      ④ GC log: memory    đầu tiên
 THREAD DUMP    straggler / stuck           âm ỉ
                FetchFailed / small task
```

### Checklist trước khi gõ "Continue"

- [ ] Thuộc vòng lặp: triệu chứng → giả thuyết → bằng chứng → fix nhỏ nhất, mỗi lần 1 biến.
- [ ] Kể được 4 thủ phạm executor OOM và 3 thủ phạm driver OOM kèm bằng chứng phân biệt.
- [ ] Giải thích được vì sao PySpark hay bị "Container killed" dù heap còn trống.
- [ ] Biết test quyết định skew vs hardware: rerun cùng chậm = skew.
- [ ] Biết FetchFailed là lỗi thứ cấp — truy executor chết trước đó.
- [ ] Đã tự gây và tự chẩn đoán 3 ca bệnh trong lab, có NOTES.md làm bằng chứng.
- [ ] Đọc được thread dump ở mức: thread nào đang làm gì, kẹt ở đâu.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 41 — CI/CD cho Spark: test PySpark, packaging.**

Bài hôm nay dạy bạn chữa cháy. Bài sau dạy bạn **phòng cháy**: phần lớn sự cố 3 giờ sáng thực ra là bug logic hoàn toàn bắt được bằng một unit test 2 giây chạy trên laptop — nếu code được cấu trúc để test được. Ta sẽ trả lời: tại sao test Spark khó (và fixture nào làm nó dễ), cấu trúc project thế nào để transformation thành pure function, đóng gói `.whl` hay Docker image, và một pipeline GitHub Actions hoàn chỉnh từ lint đến canary deploy. Debug giỏi là kỹ năng đáng nể; khiến team KHÔNG PHẢI debug mới là kỹ năng đáng tiền.

> Gõ **"Continue"** khi sẵn sàng.
