# Lesson 42 — Cost & capacity: spot instances, autoscaling, khi nào KHÔNG dùng Spark

> Module 6 · Production Engineering · Tuần 22 · Thời lượng: 4–5 giờ (lý thuyết 2.5h, lab 1.5–2h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Công thức chi phí Spark**: compute (cluster-hours) + storage + network egress — và đơn vị đo lường quan trọng nhất: **core-hours per job**.
- **Spot/preemptible instances**: rẻ 60–90%, nhưng bị thu hồi bất kỳ lúc nào — kiến trúc "executor trên spot, driver trên on-demand" + graceful decommissioning.
- **Autoscaling**: dynamic allocation của Spark bắt tay cluster autoscaler của hạ tầng; đặt min/max thế nào, scale theo cái gì.
- **Right-sizing từ metrics thực**: job dùng 20% memory được cấp → cắt, không phải đoán.
- **Serverless options** (EMR Serverless, Databricks, Dataproc Serverless) — bảng trade-off.
- Và bài học senior nhất của cả khóa: **khi nào KHÔNG dùng Spark** — bảng quyết định DuckDB/Polars/Trino/Flink/OLTP.

Sau bài này bạn phải làm được:

- Tính core-hours và ước lượng tiền cho một job bất kỳ từ Spark UI/event log.
- Thiết kế cluster cho workload có spot instance mà không sợ mất shuffle data giữa chừng.
- Nhìn Executors tab và nói: "job này đang xin thừa X% memory, cắt được Y$/tháng".
- Đứng trước một yêu cầu mới và trả lời có căn cứ: "cái này không cần Spark" — kèm giải pháp thay thế.

Kiến thức dùng trong thực tế: đây là kỹ năng biến bạn từ "người viết job" thành "người chịu trách nhiệm nền tảng". Cloud bill của data team thường là khoản lớn nhất sau lương; người chỉ ra được 40% bill là lãng phí có tiếng nói khác hẳn trong phòng họp. Và câu "khi nào không dùng Spark" xuất hiện trong hầu hết vòng system design senior.

---

## 2. Why

### Vấn đề: Spark rất giỏi... đốt tiền một cách vô hình

Mọi thứ bạn học 41 bài qua đều làm job CHẠY ĐƯỢC và CHẠY NHANH. Nhưng không gì trong Spark tự làm job CHẠY RẺ. Ba kiểu đốt tiền phổ biến nhất:

1. **Cluster ngồi chơi**: cluster 20 node bật 24/7 "cho sẵn sàng", tổng job thực chạy 3 giờ/ngày → utilization ~12%, nghĩa là ~88% bill trả cho máy ngồi thở. Đây là khoản lãng phí lớn nhất và dễ cắt nhất.
2. **Xin thừa tài nguyên**: "cứ xin 16 GB/executor cho chắc" — job thực dùng đỉnh 3 GB. Nhân với 50 executor × 300 job/tháng, khoản "cho chắc" này mua được thêm một kỹ sư.
3. **Dùng dao mổ trâu giết gà**: bảng 2 GB, chạy Spark cluster 10 node vì "team mình chuẩn hóa trên Spark". DuckDB trên 1 máy xong trong 10 giây, chi phí gần bằng 0.

### Nếu không ai quan tâm cost thì sao?

Kịch bản lặp lại ở rất nhiều công ty: năm 1 — "cứ ship đi, tiền tính sau"; năm 2 — cloud bill × 5, CFO hỏi; năm 3 — chỉ thị "cắt 30% chi phí data" rơi xuống đầu đúng team DE, giờ phải làm trong 1 quý điều lẽ ra là thói quen hằng ngày. Engineer có "cost sense" không bao giờ rơi vào thế bị động này — với họ, cost chỉ là một metric nữa bên cạnh latency và correctness, được theo dõi từ ngày đầu.

> **Analogy tiền điện**: cluster như máy lạnh. Job tối ưu = máy lạnh inverter (lesson 15–22 đã dạy). Nhưng inverter xịn đến đâu mà **quên tắt khi ra khỏi phòng** thì hóa đơn vẫn khổng lồ. Bài này dạy phần "tắt máy lạnh": bật đúng lúc (autoscaling), thuê điện giờ thấp điểm (spot), phòng nhỏ thì dùng quạt (DuckDB).

### Chân lý quan trọng nhất: tối ưu cost = tối ưu performance

Trên cloud, compute tính tiền theo **tài nguyên × thời gian**. Job nhanh gấp 5 (cùng cluster) = giữ cluster 1/5 thời gian = **rẻ gấp 5**. Mọi thứ module 3 dạy — diệt skew, tránh shuffle thừa, bỏ UDF, compact small files — đều là kỹ thuật giảm bill trực tiếp. Vụ "cứu pipeline chậm" ở Project 3 (1h → 10min) không chỉ là chiến tích kỹ thuật, nó là **83% chi phí compute của pipeline đó**. Hãy tập nói cả hai thứ tiếng: với đồng nghiệp nói "giảm 6× duration", với sếp nói "tiết kiệm N triệu/tháng" — cùng một việc.

---

## 3. Theory

### 3.1. Chi phí Spark gồm những gì

```
TỔNG BILL = COMPUTE  +  STORAGE  +  NETWORK
            (80-90%)    (5-15%)     (dễ quên nhất)

COMPUTE  = Σ (số node × giá node/giờ × số giờ cluster SỐNG)
           ← chú ý: giờ cluster SỐNG, không phải giờ job CHẠY.
             Khoảng chênh chính là utilization gap — kẻ thù số 1.

STORAGE  = data lake (S3/GCS/MinIO: rẻ/GB nhưng phình vô hạn nếu
           không expire snapshot — lesson 32) + shuffle/scratch disk

NETWORK  = egress ra ngoài region/cloud (đắt bất ngờ: cross-region
           replication, gửi data cho vendor, download về office);
           trong cùng AZ thường free — thiết kế data locality có giá bằng tiền
```

### 3.2. Đo cost per job: core-hours

Đơn vị chuẩn để so sánh job với job, tuần với tuần, **độc lập với giá máy**:

```
core-hours = Σ (số core cấp cho executor i × thời gian executor i sống)
           ≈ (executors × cores/executor × duration_giờ)   [nếu cấp tĩnh]

Ví dụ: 10 executor × 4 core × 0.5h  = 20 core-hours
Tiền  ≈ core-hours × giá 1 core-giờ (suy từ giá instance:
        m5.2xlarge 8 core ~$0.384/h → ~$0.048/core-hour on-demand)
→ job trên ~ $0.96/run × 30 ngày ≈ $29/tháng. Nhân 200 job là thấy chuyện.
```

Lấy số ở đâu: Executors tab (số executor × core × uptime), hoặc event log (lesson 39) để tính tự động cho MỌI job mỗi ngày — đội platform trưởng thành đều có dashboard "cost per pipeline per day" dựng từ chính event log này. Metric phái sinh đáng giá: **cost per GB processed** và **cost per run so với baseline** — run hôm nay đắt gấp đôi hôm qua là alert, y như duration (lesson 39–40: skew mới sinh thường lộ ở cost trước khi lộ ở failure).

### 3.3. Spot / preemptible instances

Cloud bán công suất thừa giá rẻ **60–90%** (AWS Spot, GCP Spot/Preemptible, Azure Spot) với một điều khoản: **bị thu hồi bất kỳ lúc nào**, báo trước ~30–120 giây. Câu hỏi không phải "dám dùng không" mà "đặt cái gì lên spot".

```
                 KIẾN TRÚC CHUẨN: TÁCH VAI THEO ĐỘ ĐAU KHI CHẾT

   ┌─────────────────────────┐        ┌─────────────────────────────┐
   │   ON-DEMAND (ổn định)   │        │      SPOT (rẻ 60-90%)       │
   │                         │        │                             │
   │  • DRIVER               │        │  • EXECUTORS (toàn bộ hoặc  │
   │    chết = cả app chết   │        │    phần lớn)                │
   │    (lesson 1) → không   │        │    chết = task retry, dữ    │
   │    bao giờ đặt lên spot │        │    liệu tính lại được       │
   │  • Master/AM, core node │        │    (fault tolerance lo)     │
   │    giữ HDFS/shuffle svc │        │  • Nỗi đau còn lại: SHUFFLE │
   │                         │        │    data trên đĩa nó → mất   │
   └─────────────────────────┘        │    là FetchFailed (lesson   │
                                      │    40 §3.8), stage chạy lại │
                                      └─────────────────────────────┘
```

Giải pháp cho nỗi đau shuffle — **graceful decommissioning** (Spark 3.1+):

```
spark.decommission.enabled=true
spark.storage.decommission.enabled=true
spark.storage.decommission.shuffleBlocks.enabled=true
spark.storage.decommission.rddBlocks.enabled=true
```

Cơ chế: nhận tín hiệu thu hồi (cloud báo trước 30–120s) → executor chuyển trạng thái decommissioning: **không nhận task mới, di tản shuffle block + RDD block sang executor còn sống** (hoặc fallback lên object storage qua `spark.storage.decommission.fallbackStorage.path`) → chết trong danh dự. Kết quả: mất node ≠ mất công sức, stage không phải chạy lại. Bổ trợ: external shuffle service (YARN) hoặc remote shuffle service (Celeborn, EMR/Databricks có bản riêng) — shuffle data sống ngoài executor ngay từ đầu.

Chiến lược spot thực dụng: workload batch retry-được → spot tối đa; đa dạng hóa instance type (pool nào bị thu hồi thì lấy pool khác); SLA chặt → mix 30% on-demand làm "sàn" + 70% spot làm "tăng tốc". Streaming state lớn thì cân nhắc kỹ — recovery đắt hơn batch.

### 3.4. Autoscaling — hai tầng phải bắt tay nhau

```
TẦNG 1 — SPARK (dynamic allocation): app tự xin/trả EXECUTOR
  spark.dynamicAllocation.enabled=true
  spark.dynamicAllocation.minExecutors=2      ← sàn: giữ ấm cho micro-burst
  spark.dynamicAllocation.maxExecutors=50     ← trần: chặn job skew điên xin vô hạn
  spark.dynamicAllocation.executorIdleTimeout=60s    ← ngồi chơi 60s là trả máy
  # tín hiệu scale: SỐ TASK ĐANG XẾP HÀNG (schedulerBacklogTimeout=1s
  # → có backlog 1s là xin thêm, xin kiểu mũ: 1, 2, 4, 8...)
  # điều kiện đi kèm: shuffle tracking hoặc external shuffle service
  spark.dynamicAllocation.shuffleTracking.enabled=true
  # (executor giữ shuffle data chưa được trả — không thì FetchFailed)

TẦNG 2 — HẠ TẦNG (cluster autoscaler): cụm tự thêm/bớt NODE
  K8s cluster autoscaler / EMR managed scaling / Dataproc autoscaling:
  thấy pod/container pending không đủ chỗ → mua node; node rỗng → trả.
```

Hai tầng PHẢI cùng bật: dynamic allocation không có cluster autoscaler = xin executor mà không có máy để đặt (pending mãi — chính là "job stuck chờ tài nguyên" lesson 40 §3.7); cluster autoscaler không có dynamic allocation = app ôm khư khư executor cấp tĩnh, node không bao giờ rỗng để trả. Đặt min/max từ số liệu: min = mức phục vụ job nhỏ thường trực (hoặc 0 cho cluster job-scoped); max = trần ngân sách + trần mà storage/DB downstream chịu nổi. Với hàng đợi nhiều job (Airflow đổ 40 job lúc 2AM): scale theo **queue depth** ở tầng orchestrator — mở rộng cụm TRƯỚC giờ cao điểm theo lịch (scheduled scaling) rẻ và mượt hơn rượt đuổi backlog.

### 3.5. Right-sizing từ metrics thực

Quy trình 4 bước, chạy mỗi quý cho mọi pipeline lớn:

1. **Đo**: Executors tab / event log / history server — peak memory thực (Peak JVM/Execution/Storage memory, Spark 3.0+ đo sẵn), CPU utilization, GC time, spill.
2. **So**: cấp 16 GB, peak dùng 3.2 GB (20%) → thừa ~4–5×.
3. **Cắt có đệm**: cấp mới = peak × 1.3–1.5 (đệm cho ngày dữ liệu phình + overhead PySpark lesson 40 §3.3). 16 GB → 5 GB.
4. **Kiểm**: chạy 1 tuần, canh đúng các chỉ số lesson 40 (GC% tăng? spill xuất hiện? OOM?). Êm → chốt; ho → nới một nấc.

Chiều ngược lại cũng là right-sizing: job spill liên tục + GC 20% mà "tiết kiệm" memory → chậm gấp 3 → ĐẮT hơn (nhớ §2: thời gian là tiền). Right-size nghĩa là ĐÚNG cỡ, không phải cỡ nhỏ nhất. Dấu hiệu cụm chung đáng right-size nhất: cluster utilization <30% kéo dài — gộp job, thu cụm, hoặc chuyển mô hình job-scoped cluster (mỗi job một cụm ngắn hạn, chết theo job — utilization ~100% theo định nghĩa).

### 3.6. Serverless — trả tiền theo job, không theo cluster

Ý tưởng: bạn nộp job, nhà cung cấp lo cluster; tính tiền theo tài nguyên job THỰC dùng × thời gian chạy. Cluster ngồi chơi = 0 đồng — giải quyết tận gốc "kẻ thù số 1" §3.1.

| Tiêu chí | Tự quản (EC2/GKE + Spark) | EMR Serverless / Dataproc Serverless | Databricks (serverless/jobs) |
|---|---|---|---|
| Giá đơn vị compute | Rẻ nhất (nhất là spot) | Đắt hơn EC2 ~1.5–2× | Đắt nhất (DBU + hạ tầng) |
| Idle cost | CÓ — bạn tự chịu | Không (scale về 0) | Không (job cluster) |
| Vận hành (tuning, vá, autoscale) | Bạn làm hết | Gần như 0 | Gần như 0 + tooling xịn (notebook, Photon, Unity) |
| Kiểm soát version/config sâu | Toàn quyền | Giới hạn lựa chọn | Giới hạn theo runtime |
| Cold start | Cluster có sẵn thì 0 | 1–3 phút (warm pool cấu hình được) | 1–5 phút tùy loại |
| Hợp với | Đội platform mạnh, workload dày đặc đều đặn, cần ép cost tối đa | Workload thưa/lồi lõm, đội mỏng | Đội cần velocity, chấp nhận premium |

Quy tắc ngón tay cái: **utilization thấp hoặc thất thường → serverless thắng dù giá đơn vị đắt hơn** (trả 2× đơn giá cho 3h thực dùng vẫn rẻ hơn 1× đơn giá cho 24h bật máy). Workload dày đặc 24/7 đều đặn → tự quản + spot + autoscaling rẻ hơn. Nhiều công ty dùng cả hai: pipeline lõi trên cụm tự quản, long-tail ad-hoc trên serverless.

### 3.7. Khi nào KHÔNG dùng Spark — bảng quyết định

Spark trả **thuế phân tán** cho mọi job: khởi động JVM/cluster, điều phối task, shuffle qua mạng. Thuế đó chỉ đáng khi dữ liệu vượt sức một máy. Mà máy đơn 2026 rất khỏe (hàng trăm GB RAM thuê theo giờ) — ngưỡng "cần Spark" cao hơn nhiều người nghĩ.

| Bài toán | Đừng dùng Spark, hãy dùng | Vì sao |
|---|---|---|
| Batch transform, dữ liệu **< ~100 GB** | **DuckDB / Polars** (1 máy, chạy ngay trong Python/Airflow worker) | Không thuế cluster; nhanh hơn Spark ở cỡ này (vectorized, zero điều phối); đọc/ghi Parquet, S3, cả Iceberg |
| **Ad-hoc SQL / BI** trên lakehouse | **Trino** (đã ở kiến trúc lesson 1!) | Latency giây, MPP thường trực cho query ngắn; analyst không phải chờ Spark khởi động, không giành tài nguyên pipeline |
| Streaming **latency sub-second**, event-at-a-time | **Flink / Kafka Streams** | Spark micro-batch thực tế ~giây; per-event state machine, CEP là đất của Flink (lesson 23 đã hứa, giờ chốt) |
| **Point lookup / transactional** (`WHERE id=?`, UPDATE từng dòng, phục vụ app) | **PostgreSQL / DynamoDB / OLTP bất kỳ** | Spark là engine SCAN, không có index cho point read; câu query đáng 1ms sẽ tốn 30s + một cụm máy |
| ML train trên feature set vừa RAM | pandas/sklearn/XGBoost 1 máy to | Đơn giản hơn, debug dễ hơn; Spark chỉ để CHUẨN BỊ feature từ dữ liệu lớn |
| Job < vài phút mà chạy rất thường xuyên trên data nhỏ | DuckDB trong container của orchestrator | Overhead submit + khởi động > thời gian xử lý |

Dùng Spark khi: dữ liệu thật sự lớn (100s GB → TB+), join/shuffle nặng đa nguồn, cần một engine thống nhất batch + streaming + lakehouse maintenance (Iceberg compaction chẳng hạn), và tổ chức đã có nền tảng vận hành nó.

> Bài học senior khép lại 42 bài: **engineer giỏi không phải người dựng được cluster to nhất, mà là người biết khi nào TẮT cluster** — và khi nào không dựng nó ngay từ đầu. Lesson 1 đã cảnh báo "đừng vác dao mổ trâu giết gà"; sau 41 bài học cách mài dao, bạn đủ tư cách nói câu đó với sức nặng của người BIẾT dùng dao chứ không phải người sợ dao. Đó là hai câu rất khác nhau trong phòng phỏng vấn và phòng họp.

---

## 4. Internal

### Chuyện gì xảy ra khi một spot executor bị thu hồi (có decommissioning)

```
① Cloud quyết định lấy lại máy → bắn cảnh báo (AWS: rebalance
   recommendation / 2-min interruption notice; GCP: ~30s)
        │
② Node manager / K8s nhận tín hiệu → gửi SIGPWR / taint node
   → Spark đánh dấu executor: DECOMMISSIONING
        │
③ Executor lập tức: KHÔNG nhận task mới; task đang chạy được
   chạy nốt nếu kịp (không kịp → driver reschedule chỗ khác)
        │
④ Block migration (điểm ăn tiền): shuffle block + cached RDD
   block được ĐẨY sang executor còn sống, hoặc fallback lên
   object storage (spark.storage.decommission.fallbackStorage.path)
        │
⑤ Driver cập nhật MapOutputTracker: "shuffle block X giờ ở
   executor khác/fallback" → reducer stage sau fetch đúng chỗ mới
        │
⑥ Máy bị lấy. KHÔNG có FetchFailed, KHÔNG stage nào chạy lại.
   (So với không decommissioning: reducer đến gõ cửa nhà hoang
   → FetchFailed → resubmit cả stage trước — lesson 40 §3.8.)
```

### Dynamic allocation quyết định xin/trả thế nào

Vòng lặp trong driver (`ExecutorAllocationManager`): mỗi nhịp, đếm **task pending + running** → số executor "đáng có" = ⌈tasks / cores per executor⌉ (chặn bởi max). Có backlog quá `schedulerBacklogTimeout` (1s) → xin thêm theo cấp số nhân 1, 2, 4, 8 (thăm dò nhẹ, ramp nhanh). Executor idle quá `executorIdleTimeout` (60s) → trả; NHƯNG executor còn giữ shuffle data cần thiết thì được giữ lại theo `shuffleTracking.timeout` — đây là chỗ dynamic allocation "bắt tay" với chuyện shuffle: trả máy sớm quá thì mất shuffle → FetchFailed → tính lại → đắt hơn cả tiền máy vừa tiết kiệm. Mọi nút chỉnh cost đều xoay quanh việc đừng vứt đi thứ đắt hơn thứ tiết kiệm được.

---

## 5. API

### Bộ config spot-safe (Spark 3.1+, dán vào job production trên spot)

```python
spark = (SparkSession.builder
    .appName("batch-on-spot")
    .config("spark.decommission.enabled", "true")
    .config("spark.storage.decommission.enabled", "true")
    .config("spark.storage.decommission.shuffleBlocks.enabled", "true")
    .config("spark.storage.decommission.rddBlocks.enabled", "true")
    # tùy chọn: bến đỗ cuối cho block khi không còn executor nào nhận
    # .config("spark.storage.decommission.fallbackStorage.path", "s3a://bucket/fallback/")
    .getOrCreate())
```
- **Pitfall**: driver KHÔNG có cơ chế này cứu — driver chết là hết phim (lesson 1). Driver luôn on-demand; đây là quyết định hạ tầng (node group/instance fleet), không phải config Spark.

### Bộ config dynamic allocation

```python
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "50")
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
```
- **Khi dùng**: cluster chia sẻ nhiều job, workload lồi lõm. Job-scoped cluster cấp tĩnh đúng cỡ thì KHÔNG cần — cluster chết theo job rồi.
- **Pitfall 1**: quên shuffleTracking (hoặc external shuffle service) → executor bị trả kéo theo shuffle data → FetchFailedException tự gây.
- **Pitfall 2**: maxExecutors mặc định là **vô hạn** — một job skew có thể nuốt cả cluster của mọi người. Luôn đặt trần.

### Đọc "hóa đơn" một job từ event log

```python
# labs/lab42/core_hours.py — máy tính tiền mini từ event log (JSON lines)
import json, sys

adds, removes, app_end = {}, {}, 0
for line in open(sys.argv[1]):
    e = json.loads(line)
    if e["Event"] == "SparkListenerExecutorAdded":
        adds[e["Executor ID"]] = (e["Timestamp"], e["Executor Info"]["Total Cores"])
    elif e["Event"] == "SparkListenerExecutorRemoved":
        removes[e["Executor ID"]] = e["Timestamp"]
    elif e["Event"] == "SparkListenerApplicationEnd":
        app_end = e["Timestamp"]

total = sum(cores * ((removes.get(eid, app_end) - t0) / 3.6e6)
            for eid, (t0, cores) in adds.items())
print(f"core-hours: {total:.4f}  (~${total * 0.05:.4f} @ $0.05/core-hour)")
```
- **Ý nghĩa**: chính là cách các platform team dựng dashboard cost-per-pipeline — không cần tool trả phí nào.

### `spark.sparkContext.uiWebUrl` / Executors REST API

```bash
curl -s http://localhost:4040/api/v1/applications/<app-id>/executors | jq \
  '.[] | {id, totalCores, maxMemory, memoryUsed, peakMemoryMetrics}'
```
- **Ý nghĩa**: số liệu right-sizing (§3.5) lấy bằng API để tự động hóa thay vì nhìn UI bằng mắt.

---

## 6. Demo nhỏ

Tính tiền một job cụ thể — job revenue của lab01, phóng chiếu lên production:

```
Giả định production: dữ liệu gấp 1000× (~120 GB), cluster mượn từ
lesson 38: 10 executor × 4 core × 16 GB trên m5.2xlarge.

Đo được (History Server): duration 24 phút, executor sống suốt job.

① core-hours   = 10 × 4 × (24/60)            = 16 core-hours
② tiền/run     = 16 × $0.048 (on-demand)     ≈ $0.77
③ tháng (daily)= $0.77 × 30                  ≈ $23/tháng — có vẻ hiền?

NHƯNG nhìn cả cụm: cluster này bật 24/7 phục vụ 12 job như thế,
tổng busy 6h/ngày:
④ bill cụm     = 10 node × $0.384 × 24 × 30  ≈ $2,765/tháng
⑤ phần hữu ích = 6/24                        = 25%  → ~$2,074/tháng trả cho KHÔNG KHÍ

Ba đòn theo bài học hôm nay:
  spot cho executor (giả sử -70%)      : $2,765 → ~$1,000
  autoscale/job-scoped (bỏ 75% idle)   : ~$1,000 → ~$300–400
  right-size nếu peak memory chỉ 20%    : còn cắt tiếp 30–40% nữa
→ cùng workload, ~$2,765 → ~$250. KHÔNG đổi một dòng logic nào.
```

Đó là điểm mấu chốt của demo: 10× tiết kiệm nằm ở **vận hành**, chưa cần đụng tới code. (Và nếu đụng code kiểu Project 3 — nhanh 6× — thì nhân tiếp.)

---

## 7. Production Example

Bài toán thật (mô típ gặp ở nhiều công ty cỡ vừa): nền tảng data ~40 pipeline, bill compute $38k/tháng, chỉ thị giảm 30% trong một quý. Đội platform làm đúng trình tự ưu tiên "cắt to trước, cắt tinh sau":

**Tuần 1–2 — Đo trước, cắt sau**: dựng dashboard core-hours per pipeline từ event log (script như §5). Phát hiện phân bố 80/20 kinh điển: 7 pipeline ăn 71% bill; cluster utilization trung bình 22%; và một bất ngờ — pipeline đắt thứ 3 là job "tạm thời" của một analyst đã nghỉ việc, chạy daily 11 tháng, **output không ai đọc**. Tắt: -$2.9k/tháng, một dòng diff Airflow. (Bài học nhớ đời: audit "job mồ côi" trước khi tối ưu bất cứ gì.)

**Tuần 3–6 — Kiến trúc tiền**: chuyển batch executor sang spot + decommissioning (driver on-demand) → compute batch -55%, tỷ lệ job phải retry vì thu hồi ~3%, chấp nhận được với batch. Cụm 24/7 tách đôi: pipeline lõi sang mô hình job-scoped cluster; long-tail 20+ job lặt vặt sang serverless — đơn giá đắt hơn nhưng tổng rẻ hơn hẳn vì hết idle.

**Tuần 7–10 — Right-size + hạ cấp công cụ**: quét peak memory toàn bộ: 60% job dùng <35% memory xin → cắt theo quy trình §3.5. Và mạnh dạn nhất: 9 pipeline input <30 GB được viết lại sang DuckDB chạy thẳng trong Airflow worker — nhanh hơn bản Spark (không thuế khởi động), chi phí gần 0, và dev trẻ debug dễ hơn hẳn.

**Kết quả quý**: $38k → $21k (-45%, vượt chỉ tiêu), p95 SLA không suy suyển, kèm hai "luật" mới: job mới phải khai ước lượng core-hours khi xin lên lịch, và alert cost-per-run lệch baseline 2× (nối thẳng vào hệ alert lesson 39). Điểm đáng học nhất: khoản cắt lớn nhất đến từ việc **không chạy** (job mồ côi, idle cluster, job không đáng dùng Spark) — đúng tinh thần "engineer giỏi là người biết tắt".

---

## 8. Hands-on Lab

**Mục tiêu**: tự tính core-hours từ event log thật; mô phỏng spot revocation bằng cách giết worker giữa job; và trận "chung kết" DuckDB/Polars vs Spark trên dữ liệu nhỏ.

```bash
mkdir -p labs/lab42/event-logs && make up
```

### Bước 1 — Đo core-hours của một job thật

Viết `labs/lab42/job_with_eventlog.py`: job revenue quen thuộc (đọc `/workspace/data/olist/` orders + items, join, groupBy month) nhưng bật event log:

```python
spark = (SparkSession.builder.appName("lab42-metered")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/workspace/labs/lab42/event-logs")
    .getOrCreate())
```

```bash
make run F=labs/lab42/job_with_eventlog.py
ls labs/lab42/event-logs/          # xuất hiện file app-XXXX
```

Chép script `core_hours.py` từ §5 vào `labs/lab42/`, chạy bằng venv local:

```bash
source venv/bin/activate
python labs/lab42/core_hours.py labs/lab42/event-logs/app-*
```

Ghi vào NOTES.md: core-hours của job, quy ra tiền với $0.05/core-hour, rồi phóng chiếu: nếu job này chạy mỗi giờ trong 1 năm thì bao nhiêu? Spot -70% tiết kiệm được bao nhiêu?

### Bước 2 — Mô phỏng spot revocation

Cluster của khóa có 1 worker — "spot instance" của chúng ta. Viết `labs/lab42/long_job.py`: job chạy đủ lâu (~1–2 phút) để kịp ra tay:

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("lab42-spot-victim").getOrCreate()
df = (spark.range(30_000_000)
      .withColumn("k", (F.col("id") % 1000))
      .withColumn("h", F.sha2(F.col("id").cast("string"), 256)))
print(df.groupBy("k").agg(F.count("*"), F.max("h")).count())
```

Terminal 1: `make run F=labs/lab42/long_job.py`. Terminal 2, khi job đang giữa chừng (nhìn :4040 thấy stage đang chạy):

```bash
docker stop spark-mastery-spark-worker-1     # ← cloud "thu hồi" node
```

Quan sát terminal 1 + Master UI :8080: executor lost hiện thế nào? Driver log in gì (`ExecutorLostFailure`? app chờ tài nguyên?)? Bây giờ "cloud cấp máy mới":

```bash
docker start spark-mastery-spark-worker-1
```

Job có tự hồi và chạy tiếp không? Stage nào phải chạy lại (dấu hiệu mất shuffle — lesson 40 §3.8 bằng xương thịt)? Ghi timeline đầy đủ vào NOTES.md. Câu hỏi ngẫm: với `spark.decommission.enabled` + tín hiệu báo trước (mà `docker stop` mô phỏng phần nào qua SIGTERM), kịch bản này khác đi thế nào? Vì sao driver của bạn sống sót suốt thí nghiệm (nó nằm ở container nào — soi lại docker-compose)?

### Bước 3 — Chung kết: đúng cỡ công cụ

Câu hỏi: doanh thu theo tháng trên Olist (~120 MB) — Spark cluster hay 1 máy? Cài đối thủ vào venv: `pip install duckdb`. Viết `labs/lab42/duckdb_revenue.py`:

```python
import duckdb, time
t0 = time.time()
print(duckdb.sql("""
    SELECT strftime(o.order_purchase_timestamp, '%Y-%m') AS month,
           ROUND(SUM(i.price), 2) AS revenue
    FROM 'data/olist/olist_orders_dataset.csv' o
    JOIN 'data/olist/olist_order_items_dataset.csv' i USING (order_id)
    WHERE o.order_status = 'delivered'
    GROUP BY month ORDER BY month
"""))
print(f"DuckDB: {time.time() - t0:.2f}s")
```

Đo 3 đấu thủ trên CÙNG câu hỏi: (a) `python labs/lab42/duckdb_revenue.py` (venv), (b) `make run-local F=labs/lab42/job_with_eventlog.py`, (c) `make run F=...` (cluster). Tính cả **thời gian end-to-end** (gồm khởi động). Lập bảng: thời gian / tài nguyên chiếm / độ phức tạp vận hành. Viết kết luận 5 dòng: tại điểm dữ liệu NÀO (thử nhân bản dữ liệu ước lượng) Spark bắt đầu đáng thuế phân tán của nó?

### Bước 4 — Đề xuất right-sizing

Mở lại event log/History Server (hoặc REST API §5) của job bước 1: peak memory thực so với memory cấp (worker 1G)? Nếu đây là production 16 GB/executor, bạn đề xuất con số nào theo quy trình 4 bước §3.5? Viết thành một "cost review" 10 dòng đúng giọng gửi team lead — deliverable cuối cùng của lab, và là văn bản đầu tiên trong sự nghiệp "người gác tiền" của bạn.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Vì sao "tối ưu cost = tối ưu performance" trên cloud? Cho ví dụ số cụ thể.
2. Vì sao driver không bao giờ đặt lên spot còn executor thì được? Điều gì bảo vệ executor?
3. Kể 3 bài toán KHÔNG nên dùng Spark và công cụ thay thế cho từng cái.

**Medium** — Bài toán sizing + tiền: pipeline nightly xử lý 500 GB, hiện chạy cụm tĩnh 20 node (8 core, 32 GB, $0.40/h/node) bật 24/7, job chạy 2h/đêm. Tính: (a) bill tháng hiện tại; (b) bill nếu chuyển job-scoped cluster (cụm sống 2.5h/đêm kể cả khởi động); (c) chồng thêm spot -70% cho 18/20 node; (d) nếu tuning kiểu Project 3 rút job còn 40 phút thì (b) và (c) thành bao nhiêu? Trình bày thành bảng, mỗi dòng ghi rõ giả định.

**Hard** — Thiết kế autoscaling cho ca khó: cụm chia sẻ chạy 3 loại workload — batch 2AM nặng (40 job đổ cùng lúc từ Airflow), streaming 24/7 vừa phải, ad-hoc lác đác giờ hành chính. Viết design doc 1 trang: min/max executors cho từng loại (và VÌ SAO), loại nào spot loại nào on-demand, dynamic allocation config nào khác nhau giữa batch và streaming (gợi ý: streaming có nên idle-timeout hung hãn không? state thì sao?), scheduled scaling đặt lúc mấy giờ, và 2 metric bạn sẽ alert để biết autoscaling đang phản tác dụng.

**Production Challenge** — "Kiểm toán viên tập sự": lấy 3 job bất kỳ bạn đã viết trong khóa (lab cũ nào cũng được), bật event log chạy lại cả 3 trên cluster, dùng `core_hours.py` lập bảng cost. Sau đó phán quyết từng job theo bảng §3.7: job này Ở PRODUCTION nên chạy bằng gì (Spark cluster / serverless / DuckDB / Trino)? Job nào bạn sẽ "tắt cluster"? Viết như một cost review thật sự gửi mentor — đây là bài tập tổng kết tư duy của cả module 6.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Bảng quy đổi "kỹ thuật module 3 → tiền" — mang đi thuyết phục sếp đầu tư thời gian tuning:

| Kỹ thuật đã học | Lesson | Tác động cost điển hình |
|---|---|---|
| Diệt skew (salting/AQE) | 19–20 | Stage 60 phút → 8 phút = cắt ~85% core-hours của stage đắt nhất |
| Bỏ Python UDF → built-in | 12 | CPU-bound nhanh 3–10× = rẻ 3–10×; kèm bớt memoryOverhead (§ lesson 40) |
| Compact small files | 21 | Bớt vạn task lãng phí + driver nhỏ lại; scan nhanh = giờ cluster ngắn |
| Partition pruning / predicate pushdown | 5, 33 | Đọc 1/30 dữ liệu = trả 1/30 compute cho scan + bớt egress |
| Cache đúng chỗ (và unpersist!) | 18 | Đọc lại 5 lần → 1 lần; nhưng cache thừa = memory thừa = node thừa |
| Snapshot expiration Iceberg | 32 | Storage không phình vô hạn — bill S3 đi ngang thay vì tuyến tính |

Và bảng ngược — "tiết kiệm" giả làm TĂNG bill: cắt memory tới mức spill/GC storm (chậm 3× = đắt 3×); executor bé li ti quá nhiều (overhead JVM/executor nhân bản); spot cho job SLA chặt không decommissioning (retry cả stage nhiều lần đắt hơn khoản giảm giá); idle-timeout quá hung hãn trên cluster autoscaler chậm (trả máy xong 30s sau xin lại — "thrashing", vừa chậm vừa tốn). Nguyên tắc chốt: **đo trước — cắt sau — canh sau khi cắt** (metrics lesson 39 + playbook lesson 40 chính là lưới an toàn cho mọi quyết định cost).

---

## 11. Spark UI

Đọc UI bằng con mắt kế toán — vẫn những tab cũ, câu hỏi mới:

**Executors tab** — trung tâm của right-sizing:
- **Cores × uptime từng executor** = hóa đơn thô. Executor sống 2h nhưng Task Time cộng lại 10 phút? — 92% tiền trả cho chỗ ngồi trống.
- **Peak memory** (Spark 3: On Heap/Off Heap Peak) so với **Storage Memory total được cấp** — tỷ lệ <30–40% kéo dài là bằng chứng cắt được.
- Với dynamic allocation: danh sách executor xuất hiện/biến mất (Removed có lý do) — xem allocation có "thở" theo tải không hay min quá cao nên chẳng bao giờ thu.

**Jobs/Stages timeline** — soi utilization: khoảng TRẮNG giữa các job trên timeline = cluster giữ máy mà không làm gì (đọc chậm từ nguồn? driver bận Python? chờ lịch?). Job 30 phút nhưng tổng task time chỉ tương đương 5 phút × total cores → parallelism hụt (partition ít hơn core? — lesson 4) = trả tiền N core dùng N/6.

**Environment tab** — kiểm tra "giấy tờ tiền bạc" của run: decommission flags, dynamicAllocation min/max, eventLog có bật không. Run production thiếu các dòng này = chưa sẵn sàng nói chuyện cost.

Môi trường lab: cluster 1 worker 1 core nên con số tuyệt đối nhỏ, nhưng **kỹ năng đọc y hệt** khi bạn đứng trước cụm 200 node — khác mỗi số 0.

---

## 12. Common Mistakes

1. **Tối ưu code từng job mà không nhìn utilization cả cụm.** Job nhanh 2× trên cluster idle 80% = tiết kiệm 2% bill. Cắt to trước (idle, job mồ côi), cắt tinh sau.
2. **Đặt driver lên spot.** Executor chết có người đỡ (retry/lineage/decommissioning); driver chết là cả app chết + mất luôn hiện trường. Lesson 1 dạy điều này ở ngày đầu tiên — nó vẫn đúng ở bài cuối.
3. **Bật spot mà quên decommissioning / remote shuffle** → mỗi lần thu hồi là FetchFailed + chạy lại stage — có khi đắt hơn khoản giảm giá. Spot rẻ chỉ khi mất node là chuyện "không đau".
4. **Dynamic allocation không đặt maxExecutors** (mặc định vô hạn) → một job skew nuốt cả cluster, bill nhảy vọt và mọi job khác chết đói — hai tai nạn giá một config.
5. **Right-size bằng cách đoán** ("chắc 8 GB đủ") thay vì đo peak memory. Đoán thấp → OOM lesson 40; đoán cao → trả tiền không khí. Số liệu có sẵn trong Executors tab/event log — không đo là lười, không phải thiếu tool.
6. **So serverless với tự quản chỉ bằng đơn giá.** Đơn giá serverless đắt 1.5–2× nhưng idle = 0; workload thưa thì tổng rẻ hơn nhiều. So sánh đúng: tổng bill tháng cho workload CỦA BẠN + chi phí người vận hành.
7. **Mặc định mọi bài toán data = Spark.** Bảng 5 GB lên cluster 10 node; point lookup bằng full scan. Câu hỏi đầu tiên của mọi thiết kế: "dữ liệu có thật sự vượt một máy không?" — hỏi sau khi đã dựng cluster là quá muộn.
8. **Coi cost là việc của finance/DevOps.** Người quyết định 90% bill là người viết `.config("spark.executor.instances", ...)` — tức là bạn. FinOps cho data bắt đầu từ chính DE.

---

## 13. Interview

**Junior:**

1. *Chi phí một hệ thống Spark trên cloud gồm những phần nào?* — Compute (lớn nhất: node × giá × giờ CLUSTER SỐNG — không phải giờ job chạy), storage (data lake + shuffle disk; phình nếu không expire snapshot), network egress (cross-region/ra internet — hay bị quên). Kẻ thù số 1: utilization gap — cluster bật mà không làm gì.
2. *Spot instance là gì, rẻ bao nhiêu, rủi ro gì?* — Công suất thừa cloud bán rẻ 60–90%, đổi lại bị thu hồi bất kỳ lúc nào với cảnh báo ~30–120 giây. Rủi ro với Spark: mất executor giữa job → task phải chạy lại, và tệ hơn là mất shuffle data trên đĩa node đó → FetchFailed, chạy lại cả stage.
3. *Core-hour là gì, vì sao đo cost bằng nó?* — Tổng (số core × thời gian sống) của các executor một job. Độc lập với giá instance nên so sánh được job-với-job, tuần-với-tuần; nhân đơn giá là ra tiền. Lấy từ Executors tab hoặc event log.
4. *"Job nhanh gấp 5 thì rẻ gấp 5" — đúng khi nào?* — Khi compute trả theo thời gian và cluster giải phóng được sau job (job-scoped/autoscaling/serverless). Cluster tĩnh 24/7 thì job nhanh hơn KHÔNG giảm bill — phải kèm thu nhỏ/tắt cụm; đó là lý do tối ưu code phải đi đôi với tối ưu vận hành.

**Mid:**

5. *Thiết kế cluster dùng spot cho batch pipeline — đặt gì lên spot, đặt gì không, config gì?* — Driver + master/AM + node giữ dịch vụ trạng thái: on-demand (driver chết = app chết, không cứu được). Executor: spot, đa dạng instance pool. Bật `spark.decommission.enabled` + `storage.decommission.*` để executor bị thu hồi di tản shuffle/RDD block sang nơi khác (hoặc fallback storage) trong cửa sổ cảnh báo — tránh FetchFailed. SLA chặt: mix sàn on-demand 20–30%.
6. *Dynamic allocation hoạt động thế nào và cần điều kiện gì để trả executor an toàn?* — Driver theo dõi task backlog: có task chờ quá schedulerBacklogTimeout → xin executor theo cấp số nhân đến max; executor idle quá timeout → trả. Điều kiện an toàn: shuffle data của executor bị trả phải sống sót — external shuffle service hoặc `shuffleTracking.enabled` (giữ executor còn shuffle đang cần). Phải đặt maxExecutors (mặc định vô hạn) và cần cluster autoscaler tầng hạ tầng bắt tay, không thì xin executor mà không có node để đặt.
7. *Right-sizing một pipeline đang chạy — quy trình của bạn?* — Đo peak memory/CPU/GC/spill thực từ Executors tab hoặc event log qua nhiều run (lấy cả ngày dữ liệu to); cấp mới = peak × 1.3–1.5 (đệm growth + overhead PySpark); triển khai rồi CANH một tuần các chỉ số sức khỏe (GC%, spill, OOM) trước khi chốt. Hai chiều: cắt chỗ thừa VÀ nới chỗ đang spill/GC storm — vì chậm cũng là đắt.
8. *EMR Serverless / Dataproc Serverless vs cluster tự quản — chọn theo tiêu chí gì?* — Utilization là tiêu chí số 1: workload thưa/lồi lõm → serverless thắng dù đơn giá cao hơn 1.5–2× vì idle = 0; workload dày đặc đều đặn 24/7 → tự quản + spot + autoscaling rẻ hơn. Tiêu chí phụ: năng lực đội vận hành, nhu cầu kiểm soát version/config sâu, cold start có chấp nhận được với SLA không. Nhiều tổ chức mix cả hai.

**Senior:**

9. *CFO yêu cầu giảm 30% chi phí data platform trong một quý mà không vỡ SLA — kế hoạch của bạn?* — (1) Đo trước: dashboard core-hours per pipeline từ event log, tìm phân bố 80/20 + audit job mồ côi (output không ai dùng — thường có, tắt là tiền tươi); (2) cắt to: utilization — job-scoped cluster/autoscaling/serverless cho long-tail, spot + decommissioning cho batch executor; (3) cắt tinh: right-size từ peak metrics, tuning các stage đắt nhất (skew/UDF/small files); (4) hạ cấp công cụ: pipeline <100 GB chuyển DuckDB/Polars; (5) thể chế hóa: cost-per-run vào hệ alert cạnh duration, ước lượng core-hours khi xin lên lịch job mới. Trình bày kèm số: mỗi biện pháp ước lượng $ và rủi ro. Điểm ăn tiền: "đo trước cắt sau" và hiểu khoản lớn nhất thường là những gì KHÔNG cần chạy.
10. *Khi nào bạn khuyên KHÔNG dùng Spark, và nói sao với một team đã "chuẩn hóa mọi thứ trên Spark"?* — Bảng quyết định: <~100 GB batch → DuckDB/Polars (1 máy, không thuế phân tán, thường nhanh hơn ở cỡ này); ad-hoc SQL/BI → Trino (latency giây, không giành tài nguyên pipeline); sub-second per-event streaming → Flink/Kafka Streams; point lookup/transactional → OLTP database (Spark là engine scan, không có index). Với team: không đối đầu ý thức hệ — đề xuất POC đo đếm (thời gian end-to-end + tiền + độ phức tạp vận hành trên 1–2 pipeline nhỏ), giữ Spark cho đúng chỗ mạnh của nó (TB-scale, engine thống nhất batch/streaming/lakehouse), và nhấn: chuẩn hóa nên ở TABLE FORMAT (Iceberg — mọi engine cùng đọc) chứ không phải ở compute engine. Câu chốt thể hiện đẳng cấp: sức mạnh của senior không nằm ở việc dùng được công cụ to nhất, mà ở việc biết khi nào tắt nó.

---

## 14. Summary

### Mindmap

```
                        COST & CAPACITY
                              │
   ┌──────────────┬───────────┴───────────┬──────────────────────┐
   ▼              ▼                       ▼                      ▼
 ĐO TIỀN       MUA RẺ                  VỪA VẶN               ĐÚNG CÔNG CỤ
   │              │                       │                      │
 compute +     spot -60-90%            dynamic allocation     <100GB → DuckDB/
 storage +     executor: spot          (min/max! backlog →    Polars
 egress        driver: on-demand       xin, idle → trả)       ad-hoc → Trino
 core-hours    decommissioning:        + cluster autoscaler   sub-second →
 từ event log  di tản shuffle/RDD      right-size từ PEAK     Flink
 utilization   block trước khi chết    metrics, đệm ×1.3-1.5  point lookup →
 gap = kẻ thù  (không FetchFailed)     serverless khi         OLTP DB
 số 1          nhanh 5× = rẻ 5×        utilization thấp       "biết tắt cluster"
```

### Checklist trước khi gõ "Continue"

- [ ] Tính được core-hours và quy ra tiền cho job bất kỳ từ event log (đã làm trong lab).
- [ ] Vẽ được kiến trúc spot: cái gì on-demand, cái gì spot, decommissioning cứu cái gì.
- [ ] Giải thích được vì sao dynamic allocation cần shuffle tracking và maxExecutors.
- [ ] Thuộc quy trình right-sizing 4 bước: đo → so → cắt có đệm → canh.
- [ ] Nói được khi nào serverless thắng tự quản dù đơn giá đắt hơn.
- [ ] Đọc thuộc bảng "khi nào KHÔNG dùng Spark" và bảo vệ được từng dòng.
- [ ] Đã thắng/thua tâm phục trong trận DuckDB vs Spark ở lab và rút ra ngưỡng dữ liệu.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Project 4 — Capstone: Real-time Fraud Detection.**

42 bài học khép lại — giờ là trận chung kết kéo dài 2 tuần (tuần 23–24). Bạn sẽ dựng hệ thống phát hiện gian lận thời gian thực hoàn chỉnh: transaction stream đổ vào Kafka → Spark Structured Streaming chấm điểm rủi ro (rule-based + stream-static join với user profile + stateful history 24h) → giao dịch rủi ro cao bắn sang alert topic, toàn bộ ghi audit log vào Iceberg kèm risk score → metrics đẩy Prometheus, state TTL 30 ngày, deploy Kubernetes có HPA. Mọi thứ bạn học đều ra trận: streaming (module 4), Iceberg (module 5), sizing–monitoring–debugging–testing–cost (module 6). Đây chính là project bạn sẽ kể trong vòng phỏng vấn system design — lần này không phải kể lại tutorial, mà kể thứ tự tay bạn dựng, tự tay bạn debug, và tự tay bạn... tính tiền.

> Gõ **"Continue"** khi sẵn sàng.
