# Lesson 22 — Quy trình tuning tổng hợp: Checklist Senior

> Module 3 · Internals & Performance Tuning · Tuần 11 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Playbook 10 bước có thứ tự** để tuning bất kỳ job Spark nào — từ đo baseline đến resource sizing. Mỗi bước: nhìn gì, ngưỡng nào, hành động gì.
- Bảng **15 config quan trọng nhất** kèm giá trị khởi điểm — bộ số bạn mang theo cả sự nghiệp.
- **Runbook "job chậm lúc 3h sáng"** — phiên bản rút gọn cho on-call, khi não chỉ còn 30%.
- Hai nguyên tắc bất di bất dịch: **đo trước sửa sau** và **đổi 1 thứ mỗi lần**.

Sau bài này bạn phải làm được:

- Nhận một job chậm CHƯA TỪNG THẤY và điều tra theo đúng trình tự, không mò mẫm.
- Nói "không" với đề nghị "tăng memory lên là xong" khi chưa có số liệu.
- Viết tuning notes mà 6 tháng sau người khác (hoặc chính bạn) đọc lại vẫn tái hiện được.

Kiến thức dùng trong thực tế: đây là bài **tổng kết toàn bộ Module 3** — không có khái niệm mới, chỉ có TRÌNH TỰ. Sự khác biệt giữa mid và senior không nằm ở biết nhiều chiêu hơn, mà ở chỗ senior ra đòn **đúng thứ tự và dừng đúng lúc**. Project 3 ngay sau bài này là bài thi thực hành của cả playbook.

---

## 2. Why

### Câu chuyện: hai kỹ sư, một job chậm

Job daily chạy 55 phút, SLA 30 phút. Hai người vào cuộc:

**Kỹ sư A (mò mẫm)**: "Chắc thiếu memory" → tăng executor memory ×2 → 53 phút. "Chắc thiếu core" → thêm 5 executor → 51 phút. "Hay tại shuffle partitions" → đổi 200 thành 800 → 58 phút, tệ hơn! → đổi lại, thêm cache lung tung → 3 ngày trôi qua, cluster tốn gấp đôi tiền, job vẫn 50 phút, và giờ KHÔNG AI biết config nào đang có tác dụng gì.

**Kỹ sư B (playbook)**: Mở Spark UI → job có 9 job con nhưng 1 job chiếm 44/55 phút → trong đó 1 stage chiếm 41 phút → Summary Metrics: duration max 39 phút vs median 40 giây → **skew**, shuffle read max 6 GB vs median 90 MB → soi key: `device_id = 'unknown'` chiếm 45% → tách hot key + bật lại AQE threshold đúng cỡ → 12 phút. Một buổi sáng, một thay đổi, có số liệu before/after.

Khác biệt không phải IQ — là **quy trình**. A đoán nguyên nhân rồi tìm bằng chứng; B tìm bằng chứng rồi mới gọi tên nguyên nhân.

### Hai nguyên tắc nền (dán lên màn hình)

1. **Đo trước, sửa sau** — chưa có số baseline và chưa chỉ được ĐÍCH DANH stage/task nào chậm thì chưa được đổi bất cứ gì. Cảm giác ("chắc là...") không phải bằng chứng.
2. **Đổi 1 thứ mỗi lần** — đổi 3 config cùng lúc mà nhanh lên thì bạn học được gì? Không gì cả. Tệ hơn: 1 config tốt +40%, 2 config xấu −25%, bạn thấy +5% và giữ cả 3. Mỗi thay đổi = 1 lần chạy = 1 dòng trong bảng đo.

> **Analogy bác sĩ**: bệnh nhân kêu mệt. Bác sĩ tồi kê luôn 5 loại thuốc "cho chắc". Bác sĩ giỏi: hỏi bệnh sử (baseline) → khám tổng quát (UI) → chỉ định xét nghiệm đúng chỗ nghi (stage metrics) → chẩn đoán → MỘT phác đồ → tái khám đo lại. Spark UI là máy xét nghiệm; playbook là trình tự khám. Kê thuốc trước khi khám là tội ác — với bệnh nhân lẫn với job.

### Trade-off của việc tuning (điều senior hiểu mà mid quên)

| Được | Mất |
|---|---|
| Job nhanh hơn, tiền cluster ít hơn | Thời gian kỹ sư — đắt hơn máy! Tuning 2 tuần để tiết kiệm 5 phút/ngày = lỗ |
| Hiểu sâu hệ thống | Config càng chỉnh nhiều càng khó bảo trì — mỗi số phải có lý do ghi lại |
| SLA an toàn | Tuning quá khít = fragile: dữ liệu tăng 20% là vỡ. Chừa headroom |

Câu hỏi số 0 của playbook, trước cả bước 1: **"job này CÓ ĐÁNG tuning không?"** — chạy 1 lần/tháng, chậm 1 giờ, chẳng ai chờ → để yên đi làm việc khác.

---

## 3. Theory

### PLAYBOOK 10 BƯỚC — trái tim của bài

```
 ĐO & KHOANH VÙNG              CHẨN ĐOÁN THEO TRIỆU CHỨNG           TÁI CẤU TRÚC & TÀI NGUYÊN
┌────────────────────┐      ┌────────────────────────────────┐      ┌──────────────────────┐
│ 1. Baseline & SLA  │  →   │ 3. Shuffle read/write?          │  →  │ 7. Partition & files │
│ 2. UI: job/stage   │      │ 4. Skew?                        │      │ 8. Join & broadcast  │
│    nào tốn nhất?   │      │ 5. Spill?                       │      │ 9. Cache đúng chỗ    │
└────────────────────┘      │ 6. UDF?                         │      │ 10. Resource sizing  │
                            └────────────────────────────────┘      └──────────────────────┘
 KHÔNG BAO GIỜ                Đi từ trên xuống, DỪNG khi              Chỉ đến bước 10 khi
 nhảy cóc 1–2                 đạt SLA — đừng tuning thừa              1–9 đã sạch
```

**Bước 1 — Đo baseline & xác định SLA.**
- *Nhìn gì*: tổng thời gian chạy (3–5 lần để biết variance), input size, output size, tài nguyên đang cấp. SLA là bao nhiêu — hỏi người dùng nếu chưa ai định nghĩa.
- *Ngưỡng*: không có ngưỡng — bước này tạo ra thước đo. Không có baseline = mọi kết luận sau đều vô nghĩa.
- *Hành động*: ghi bảng baseline (thời gian, cấu hình, ngày, cỡ dữ liệu). Chốt mục tiêu ("55 phút → dưới 30").

**Bước 2 — Spark UI: tìm job/stage tốn nhất.**
- *Nhìn gì*: tab Jobs sort theo Duration → vào job nặng nhất → tab Stages của nó → stage nặng nhất. Định luật Pareto luôn đúng: 1–2 stage chiếm 80%+ thời gian.
- *Ngưỡng*: stage nào ≥ 30% tổng thời gian là nghi phạm chính.
- *Hành động*: KHOANH VÙNG — từ giờ chỉ điều tra stage đó. Map stage về dòng code (SQL tab → plan → tên cột/bảng). Mọi bước 3–9 đều thực hiện TRÊN STAGE NÀY.

**Bước 3 — Kiểm tra shuffle read/write.**
- *Nhìn gì*: cột Shuffle Read/Write của stage nghi phạm; SQL tab đếm số Exchange trong plan.
- *Ngưỡng*: shuffle write cỡ ~ input size trở lên = đang xáo gần như toàn bộ dữ liệu — hỏi ngay "có cần không?". Nhiều Exchange liên tiếp trên cùng key = shuffle lặp vô ích.
- *Hành động*: giảm dữ liệu TRƯỚC shuffle (filter sớm, select đúng cột, aggregate sớm); gộp các phép cùng key để dùng chung 1 lần shuffle; join bảng nhỏ → chuyển broadcast (bước 8). Shuffle là chi phí #1 của Spark — bước này thường ăn điểm to nhất.

**Bước 4 — Skew?** (lesson 19)
- *Nhìn gì*: Summary Metrics của stage: duration max vs median; shuffle read max vs median; Event Timeline có thanh cô đơn dài.
- *Ngưỡng*: max > 4× median duration, hoặc max > 5–10× median shuffle read.
- *Hành động*: theo thứ tự — broadcast nếu 1 bảng nhỏ → AQE skew join (chỉnh threshold theo cỡ partition thật) → salting → tách hot key. Kiểm tra NULL key trước tiên.

**Bước 5 — Spill?** (lesson 15, 17)
- *Nhìn gì*: cột Spill (Memory/Disk) trong stage metrics; Executors tab xem GC time.
- *Ngưỡng*: spill > 0 đã đáng ghi nhận; spill cỡ vài lần shuffle size = task nghẹt thở. GC time > 10% task time = memory áp lực.
- *Hành động*: (a) TĂNG SỐ PARTITION để mỗi task ôm ít dữ liệu hơn — rẻ nhất, thử trước; (b) xem lại phép toán phàm ăn (collect_list, window to); (c) tăng memory executor — sau cùng, vì đắt. Spill cục bộ ở 1 task = quay lại bước 4, đó là skew.

**Bước 6 — UDF?** (lesson 12)
- *Nhìn gì*: plan có `BatchEvalPython` / `ArrowEvalPython`; stage chậm mà shuffle ít, spill không, CPU executor cao.
- *Ngưỡng*: bất kỳ UDF Python nào trên đường nóng (hot path) của stage nặng đều là nghi phạm — serialize từng dòng Python↔JVM đắt 10–100× built-in.
- *Hành động*: thay bằng built-in functions (`F.*` cover 95% nhu cầu); không được thì pandas UDF (vectorized, Arrow); UDF cũng chặn mọi pushdown/codegen của Catalyst — gỡ được 1 UDF đôi khi ăn cả chục lần tốc độ.

**Bước 7 — Partition count & file layout?** (lesson 16, 21)
- *Nhìn gì*: số task mỗi stage vs tổng core; input: số file & avg size (SQL tab, node Scan); output: đếm file sau khi ghi.
- *Ngưỡng*: task < 2× tổng core (thiếu song song) hoặc task hàng chục nghìn với duration < 100ms (vụn); file nguồn avg < 32 MB = bệnh small files; partition in-memory nhắm 100–200 MB/task.
- *Hành động*: chỉnh initialPartitionNum/AQE advisory; nguồn small files → compaction (lesson 21); writer thiếu repartition/maxRecordsPerFile → thêm.

**Bước 8 — Join strategy & broadcast?** (lesson 11, 19, 20)
- *Nhìn gì*: SQL tab final plan: SortMergeJoin hay BroadcastHashJoin? Bảng bên nhỏ THẬT SỰ bao nhiêu MB lúc runtime?
- *Ngưỡng*: bảng < vài trăm MB (tuỳ memory executor) mà vẫn sort-merge = cơ hội bỏ lỡ; cross join / join nổ dòng (output records >> input) = thiết kế sai.
- *Hành động*: hint `broadcast()`, nâng autoBroadcastJoinThreshold có cân nhắc memory; xác nhận AQE không tự đổi được vì lý do gì (estimate? đã materialize?); xét lại thứ tự join (join bảng lọc mạnh trước).

**Bước 9 — Cache đúng chỗ?** (lesson 18)
- *Nhìn gì*: Storage tab: cache gì, Fraction Cached bao nhiêu, có bị evict không; DAG các job có tính lại cùng nhánh nhiều lần không (nhiều job cùng đọc/xử lý một nguồn).
- *Ngưỡng*: một DataFrame dùng ≥ 2 action mà không cache = tính lại phí; cache mà Fraction < 100% hoặc chỉ dùng 1 lần = cache hại (chiếm memory, thêm bước ghi).
- *Hành động*: cache đúng nhánh dùng lại nhiều lần, SAU bước lọc/thu gọn; `unpersist()` khi xong; cân nhắc MEMORY_AND_DISK khi executor bé; đôi khi checkpoint/ghi tạm parquet tốt hơn cache (cắt lineage).

**Bước 10 — Resource sizing.**
- *Nhìn gì*: Executors tab: CPU dùng thật, memory dùng thật, GC time; tổng core vs số task song song cần.
- *Ngưỡng*: mọi bước 1–9 đã sạch mà vẫn thiếu SLA; executor luôn bận 100% và không còn gì để bớt việc → lúc này MỚI thêm tài nguyên. Kinh nghiệm cỡ executor: 4–5 core/executor, 4–8 GB/core là vùng cân bằng phổ biến (quá nhiều core/executor → nghẽn HDFS client & GC; quá ít → phí overhead JVM).
- *Hành động*: scale số executor (ngang) trước, cỡ executor (dọc) sau; bật dynamic allocation cho workload thất thường; TÍNH TIỀN: mỗi lần tăng phải kèm % cải thiện đo được.

Vì sao thứ tự này? **Bước 1–2 là la bàn** (không có thì mọi bước sau đi lạc). **Bước 3–6 là bệnh trong code/dữ liệu** — sửa được bằng chất xám, miễn phí. **Bước 7–9 là cấu trúc dữ liệu & plan** — rẻ. **Bước 10 là tiền** — vì thế nó đứng CUỐI, trong khi 90% người đời làm nó đầu tiên.

---

## 4. Internal

Không có machinery mới hôm nay — thay vào đó là bản đồ nối TRIỆU CHỨNG trên UI về CƠ CHẾ bên trong đã học, để bạn tra ngược khi điều tra:

```
TRIỆU CHỨNG (UI)                    CƠ CHẾ GÂY RA (lesson)                 BƯỚC PLAYBOOK
─────────────────────────────────────────────────────────────────────────────────────────
Im lặng dài trước job đầu        →  driver listing files (21)           →  7
Job nhiều hơn số action          →  AQE query stages (20), inferSchema  →  2 (đọc UI đúng)
Stage cắt tại đâu                →  shuffle boundary (15)               →  3
Shuffle write khổng lồ           →  wide transform trên data chưa lọc   →  3
Duration max >> median           →  hash partition + hot key (19)       →  4
Spill chỉ ở 1–2 task             →  skew ép 1 task quá tải (19+17)      →  4 (không phải 5!)
Spill đều mọi task               →  partition to quá / memory thiếu(17) →  5
GC time cao, executor đỏ         →  heap áp lực: cache thừa, task to    →  5, 9
Stage chậm, ít shuffle, CPU cao  →  BatchEvalPython — UDF (12)          →  6
Nghìn task < 100ms               →  partition vụn / small files (16,21) →  7
SortMergeJoin với bảng bé        →  estimate sai, AQE chưa với tới (20) →  8
Cùng nhánh DAG chạy lại N job    →  lazy evaluation, thiếu cache (2,18) →  9
Task ít hơn core, cluster nhàn   →  thiếu partition / coalesce sớm (16) →  7, 10
```

Một mẹo nội công khi đọc UI tổng thể: thời gian job = **Σ(stage tuần tự)**, mỗi stage = **max(task trong stage) × số wave**. Vậy chỉ có 3 cách nhanh lên về mặt toán học: (a) bớt stage (bớt shuffle), (b) hạ max task (trị skew/spill/UDF), (c) bớt wave (thêm song song — partition & core). Mọi bước 3–10 đều là biến thể của 3 con đường này. Nhìn mọi đề xuất tuning và tự hỏi "nó đánh vào a, b hay c?" — không trả lời được thì đề xuất đó vô nghĩa.

---

## 5. API

### 5.1. Bảng 15 config quan trọng nhất — kèm giá trị khởi điểm

Đây là "bàn dao mổ" — giá trị khởi điểm cho cluster tầm trung (executor 4 core/16 GB), kèm lý do. KHÔNG copy mù: mỗi số phải đo lại trên workload của bạn.

| # | Config | Khởi điểm | Vì sao / khi nào chỉnh |
|---|---|---|---|
| 1 | `spark.executor.memory` | 12–16g | Memory cho JVM executor. Chỉnh khi bước 5/10, sau khi hết cách rẻ. |
| 2 | `spark.executor.cores` | 4–5 | Quá cao → nghẽn I/O + GC; quá thấp → phí overhead JVM. |
| 3 | `spark.executor.instances` (hoặc dynamic) | theo data | Scale ngang. Ưu tiên trước scale dọc. |
| 4 | `spark.driver.memory` | 4–8g | Tăng khi: collect kết quả to, listing triệu file, broadcast to. |
| 5 | `spark.sql.shuffle.partitions` | 200→cao hào phóng | Với AQE: là TRẦN, set theo job lớn nhất (vd 800–2000). |
| 6 | `spark.sql.adaptive.enabled` | true | Đừng tắt trừ khi debug plan. |
| 7 | `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64–128m | Cỡ partition sau coalesce; tăng khi task quá vụn. |
| 8 | `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256m → hạ theo cụm | Cluster/partition nhỏ phải hạ, không AQE "ngó lơ" skew. |
| 9 | `spark.sql.autoBroadcastJoinThreshold` | 10m → 64–256m | Nâng khi dim vài chục MB mà cứ sort-merge; nhớ trần memory. |
| 10 | `spark.sql.files.maxPartitionBytes` | 128m → 256m | Tăng để gộp file nhỏ nhiều hơn vào 1 task khi đọc lake bệnh. |
| 11 | `spark.sql.files.maxRecordsPerFile` | 0 → đặt theo bytes/dòng | Chống file to khi ghi partition lệch. |
| 12 | `spark.memory.fraction` | 0.6 (để yên) | Chỉ đụng khi hiểu lesson 17 và có bằng chứng; đa số ca không cần. |
| 13 | `spark.serializer` | KryoSerializer | Nhanh gọn hơn Java serializer cho shuffle/cache RDD. |
| 14 | `spark.dynamicAllocation.enabled` (+shuffle tracking) | true cho ad-hoc | Cluster share nhiều job; batch cố định giờ chạy thì để static dễ đoán. |
| 15 | `spark.sql.parquet.filterPushdown` | true (mặc định) | Kiểm tra nó CÒN hoạt động — UDF/cast trong filter làm mất pushdown. |

- **Pitfall meta**: config đúng của người khác là config sai của bạn. Bảng này là ĐIỂM XUẤT PHÁT của thí nghiệm, không phải đáp án. Số nào bạn giữ trong repo phải kèm comment "vì sao + đo được gì".

### 5.2. Đo baseline có kỷ luật

```python
import time, json

def timed_run(label, fn, spark):
    spark.sparkContext.setJobGroup(label, label)   # UI: gom job theo nhóm, dễ đối chiếu
    t0 = time.time()
    result = fn()
    dt = time.time() - t0
    conf_snapshot = {k: spark.conf.get(k, "unset") for k in [
        "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold", "spark.executor.memory"]}
    print(json.dumps({"label": label, "seconds": round(dt, 1), "conf": conf_snapshot}))
    return result
```
- **Ý nghĩa**: mỗi lần chạy in ra 1 dòng JSON (thời gian + config đang hiệu lực) → bảng before/after tự viết ra log, hết cãi nhau "lúc đó config gì".
- **Pitfall**: đo lần chạy ĐẦU sau khi bật cluster gồm cả chi phí warm-up (JVM, connection); chạy ≥ 3 lần lấy median.

### 5.3. Chụp plan để so before/after

```python
plan_before = df._jdf.queryExecution().explainString(
    spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted"))
# hoặc đơn giản, đủ dùng cho báo cáo:
df.explain("formatted")     # sau action → final plan (lesson 20)
```
- **Khi dùng**: mọi optimization về join/AQE/pushdown — bằng chứng nằm ở plan, không ở cảm giác. Deliverable Project 3 yêu cầu đúng cái này.

### 5.4. Khung tuning notes (bắt buộc mỗi thay đổi 1 dòng)

```
| # | Thay đổi (MỘT thứ)            | Giả thuyết         | Trước | Sau  | Giữ? |
|---|-------------------------------|--------------------|-------|------|------|
| 1 | broadcast(dim_sellers)        | né shuffle 4GB     | 55m   | 31m  | ✔    |
| 2 | shuffle.partitions 200→800    | task 400MB quá to  | 31m   | 29m  | ✔    |
| 3 | cache(orders_raw)             | dùng lại 3 lần(?)  | 29m   | 33m  | ✘ rollback — chỉ dùng 1 lần |
```
- Cột "Giữ?" với dòng rollback là cột giá trị nhất — thất bại được ghi lại là tri thức, thất bại không ghi là lãng phí lặp lại.

---

## 6. Demo nhỏ

```
Input:  1 query cố tình chậm (UDF + không broadcast) trên dữ liệu tự sinh
   ↓    áp playbook: đo → khoanh vùng UI → bước 6 (UDF) → bước 8 (broadcast)
Output: 3 số đo — baseline, sau fix 1, sau fix 2 — mỗi lần MỘT thay đổi
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, broadcast
from pyspark.sql.types import StringType
import time

spark = (SparkSession.builder.appName("demo22").master("local[2]")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate())

big = spark.range(3_000_000).withColumn("code", (F.col("id") % 1000).cast("int"))
dim = spark.range(1000).withColumnRenamed("id", "code").withColumn("name", F.lit("x"))

@udf(StringType())
def fmt(c): return f"C-{c:04d}"                      # tội đồ 1: UDF

def run(q, label):
    t0 = time.time(); q.count(); print(f"{label}: {time.time()-t0:.1f}s")

run(big.withColumn("k", fmt("code")).join(dim, "code"), "baseline (UDF + SMJ)")
run(big.withColumn("k", F.format_string("C-%04d", "code")).join(dim, "code"),
    "fix 1: built-in thay UDF")                       # MỘT thay đổi
run(big.withColumn("k", F.format_string("C-%04d", "code")).join(broadcast(dim), "code"),
    "fix 2: + broadcast dim")                         # thêm MỘT thay đổi nữa
spark.stop()
```

Tự hỏi: nếu làm fix 1 + fix 2 CÙNG LÚC, bạn có biết mỗi cái đóng góp bao nhiêu không? Đó chính là lý do nguyên tắc "đổi 1 thứ mỗi lần" tồn tại — demo này là playbook thu nhỏ.

---

## 7. Production Example

### Runbook "job chậm lúc 3h sáng" — bản dán cạnh giường on-call

Playbook 10 bước là cho ban ngày, đầu óc tỉnh táo. 3h sáng bạn cần bản RÚT GỌN — mục tiêu KHÁC: không phải tối ưu, mà là **cứu SLA đêm nay rồi ngủ tiếp, tuning tử tế để mai**.

```
┌─ RUNBOOK 3H SÁNG ──────────────────────────────────────────────────────────┐
│ 0. ĐỪNG restart vội. UI/log đang chứa bằng chứng — restart là đốt hiện trường│
│                                                                              │
│ 1. CÁI GÌ ĐỔI? (90% sự cố đêm là do THAY ĐỔI, không phải do code tự hư)     │
│    □ Dữ liệu hôm nay to bất thường? (ngày sale? backfill ai đó bơm vào?)     │
│    □ Deploy mới? config mới? Spark version mới?                              │
│    □ Cluster: node chết? hàng xóm chiếm tài nguyên? (Executors tab: số        │
│      executor có đủ như mọi ngày không, có executor dead/lost không?)        │
│    → Tìm ra thay đổi = xử thay đổi đó. Xong. Đi ngủ.                          │
│                                                                              │
│ 2. JOB ĐANG TREO HAY ĐANG CHẬM? (UI → Stages đang chạy)                      │
│    □ Task vẫn nhích đều → chậm: xem bước 3                                   │
│    □ 1–2 task đứng im hàng giờ → straggler: soi task đó (skew? node bệnh?)   │
│      → thử kill task/executor đó cho retry chỗ khác (speculative execution)  │
│    □ 0 task chạy, job "pending" → thiếu tài nguyên: cluster manager UI (:8080)│
│      xem ai đang chiếm; xin/giành lại tài nguyên                              │
│                                                                              │
│ 3. CHẬM ĐỀU → 1 phút chẩn nhanh trên stage nặng nhất:                        │
│    □ Duration max >> median?     → skew: đêm nay TÁCH HOT KEY thô bạo         │
│      (filter riêng xử riêng) hoặc hạ AQE skew threshold — mai làm đẹp        │
│    □ Spill to?                   → tăng partition (nhanh nhất) / memory       │
│    □ Executor chết đi sống lại?  → OOM: xem log executor, tăng memory        │
│      hoặc GIẢM cỡ task (tăng partition) — cách 2 thường nhanh hơn             │
│                                                                              │
│ 4. CỨU SLA: còn kịp không nếu để chạy tiếp? Không kịp →                       │
│    □ Chạy lại chỉ phần dữ liệu thiếu (theo partition ngày) thay vì full       │
│    □ Hạ mức: bảng downstream nào chấp nhận trễ, báo trước cho consumer        │
│                                                                              │
│ 5. TRƯỚC KHI NGỦ: chụp UI (Stages + Summary Metrics), copy applicationId,    │
│    ghi 3 dòng vào incident note. Mai đọc lại + chạy playbook 10 bước đầy đủ. │
└──────────────────────────────────────────────────────────────────────────────┘
```

Điểm khác biệt văn hoá ở team trưởng thành: sau MỖI lần 3h sáng đều có post-mortem nhỏ trả lời "làm sao để lần sau nó tự lành / tự cảnh báo sớm?" — speculative execution cho straggler, alert khi input size lệch 50% so với trung bình 7 ngày, AQE threshold đặt đúng từ đầu. Runbook tốt nhất là runbook ngày càng ÍT phải dùng.

---

## 8. Hands-on Lab

**Mục tiêu**: áp playbook 10 bước lên một job chậm "lạ mặt" — tập dượt đúng quy trình trước Project 3.

### Bước 0 — chuẩn bị

```bash
make up      # master :8080, app UI :4040
```

### Bước 1 — nhận bệnh nhân: `labs/lab22/mystery_job.py`

Tự viết job này theo mô tả (đây là một phần bài tập — bạn xây bệnh nhân, rồi GIẢ VỜ quên và chẩn đoán như người ngoài):

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

spark = (SparkSession.builder.appName("lab22-mystery")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .getOrCreate())

items    = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                          header=True, inferSchema=True)
products = spark.read.csv("/workspace/data/olist/olist_products_dataset.csv",
                          header=True, inferSchema=True)

# phóng to + tạo skew nhẹ
big = items.crossJoin(spark.range(30).select(F.lit(1).alias("d"))).drop("d")

@udf(DoubleType())
def tax(p):                       # UDF không cần thiết
    return float(p) * 1.1 if p else 0.0

result = (big.withColumn("price_tax", tax("price"))
             .join(products, "product_id")
             .groupBy("product_category_name")
             .agg(F.sum("price_tax").alias("gmv"),
                  F.count("*").alias("cnt")))
result.write.mode("overwrite").parquet("/workspace/labs/lab22/out")
spark.stop()
```

### Bước 2 — chạy playbook, ghi từng bước

```bash
make run F=labs/lab22/mystery_job.py
```

Tạo `labs/lab22/PLAYBOOK_RUN.md` với đúng 10 mục. Mỗi mục 2–4 dòng: **nhìn thấy gì (số liệu!) → kết luận → hành động/bỏ qua**. Bước nào "sạch" cũng phải ghi "sạch, bằng chứng: ..." — kỷ luật nằm ở chỗ không nhảy cóc.

### Bước 3 — sửa theo thứ tự, mỗi lần một thứ

Tạo `labs/lab22/fixed_v1.py`, `fixed_v2.py`, ... — mỗi version chỉ khác version trước ĐÚNG MỘT thay đổi. Sau mỗi version: chạy, điền 1 dòng vào bảng tuning notes (khung §5.4). Gợi ý các nghi phạm có mặt: UDF, thiếu broadcast, AQE tắt, inferSchema, số partition khi ghi.

### Bước 4 — nghiệm thu

`labs/lab22/NOTES.md`: bảng tuning notes hoàn chỉnh + tổng cải thiện (mục tiêu ≥ 3× cho lab này) + trả lời: thay đổi nào ăn nhiều nhất? thay đổi nào bạn TƯỞNG ăn mà không ăn? nếu chỉ được làm MỘT thay đổi, chọn gì?

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Vì sao bước "resource sizing" đứng THỨ 10 chứ không phải thứ 1? Kể 2 tác hại của việc tăng tài nguyên trước khi chẩn đoán.
2. "Đổi 1 thứ mỗi lần" — giải thích bằng ví dụ 2 thay đổi triệt tiêu nhau.
3. Spill đều mọi task vs spill dồn 1 task — dẫn về bước playbook nào, vì sao khác nhau?

**Medium** — Viết "bản đồ triệu chứng" của riêng bạn: 10 triệu chứng UI (khác bảng section 4 càng tốt) → cơ chế → bước playbook. Sau đó lấy lab của lesson 19 hoặc 21 chạy lại và đánh dấu: những triệu chứng nào bạn THẤY TẬN MẮT rồi, cái nào mới chỉ học chay — cái học chay thì thiết kế thí nghiệm nhỏ để thấy nó.

**Hard** — Giới hạn của playbook: thiết kế một job chậm mà playbook 10 bước KHÔNG bắt được thủ phạm trực tiếp (gợi ý hướng: chậm do nguồn JDBC đọc 1 luồng; do driver làm việc tuần tự ngoài Spark — vòng for gọi action; do lineage quá dài re-computation). Chạy thử, mô tả playbook "trượt" ở bước nào và bạn phát hiện bằng cách gì ngoài UI (log driver, py-spy, đồng hồ đo từng đoạn code). Đề xuất "bước 11" cho playbook của riêng bạn.

**Production Challenge** — Viết `docs/runbook-spark-slow.md` cho "công ty" của bạn: phối runbook 3h sáng (section 7) với đặc thù cụm Docker lab này (worker 1G/1core, master UI :8080, app UI :4040, lệnh `make ps`, `make logs`). Yêu cầu: dưới 60 dòng, một junior chưa học module 3 cầm nó lúc nửa đêm vẫn thao tác được từng bước. Test thật: đưa cho một người bạn (hoặc tự đọc với tư cách junior) và sửa chỗ họ vấp.

> Nộp bài bằng cách paste code + số đo + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

Bảng "đòn nào ăn bao nhiêu" — kỳ vọng thực tế để bạn ưu tiên (số liệu điển hình, không phải cam kết):

| Optimization | Cải thiện điển hình | Chi phí thực hiện |
|---|---|---|
| Gỡ UDF Python → built-in | 2–20× cho stage đó | Thấp (đổi vài dòng) |
| Sort-merge → broadcast join | 2–10× cho join đó | Rất thấp (1 hint) |
| Trị skew nặng | 3–50× cho stage đó | Trung bình (salting) → thấp (AQE) |
| Filter/select sớm trước shuffle | 1.5–5× | Thấp |
| Compaction small files nguồn | 2–20× pha đọc | Trung bình (job định kỳ) |
| Hết spill (partition/memory) | 1.5–3× | Thấp |
| Cache đúng chỗ | ~N× cho N lần dùng lại | Thấp — nhưng dễ âm điểm nếu sai |
| Tăng tài nguyên ×2 | thường < 2× (Amdahl!) | TIỀN, mỗi tháng, mãi mãi |

Hai định luật khắc cốt:

- **Amdahl cho Spark**: tăng gấp đôi core chỉ tăng tốc phần SONG SONG ĐƯỢC. Stage skew (1 task quyết định), driver listing, collect về driver — thêm core vô nghĩa. Vì thế các fix "hình dạng" (3–9) thường thắng fix "tài nguyên" (10).
- **Định luật dừng**: đạt SLA + headroom (~30%) thì DỪNG. Mỗi giờ tuning sau đó là giờ không xây pipeline mới. Perfect is the enemy of shipped.

---

## 11. Spark UI

Bài này không mở tab mới — nó đóng gói **lộ trình di chuyển giữa các tab** thành thói quen. Lộ trình chuẩn của một phiên điều tra:

```
① Jobs (sort Duration)        "job nào?"          — 30 giây
② → job → Stages              "stage nào?"        — 30 giây
③ → stage → Summary Metrics   "triệu chứng gì?"   — 2 phút: duration/shuffle/spill
      + Event Timeline           max vs median      percentiles là mỏ vàng
④ → SQL tab → query → plan    "code nào? plan gì?" — map về dòng code,
      (AdaptiveSparkPlan,        đếm Exchange, tìm BatchEvalPython,
       AQEShuffleRead metrics)   xem join strategy final
⑤ → Executors                 "tài nguyên thở không?" — GC time, dead executors,
                                 task time vs input (bận thật hay bận thao tác)
⑥ → Storage (nếu có cache)    "cache sống khoẻ không?" — fraction cached, size
```

Ba thói quen senior:

- **Percentiles trước, average sau** — average che giấu mọi tội lỗi (1 task 40 phút + 199 task 30s có average đẹp long lanh).
- **Chụp màn hình TRƯỚC khi sửa** — bằng chứng before là thứ không quay lại lấy được sau khi job đã nhanh.
- **Đọc số theo cặp đối chiếu**: shuffle write stage này vs input của nó; task time vs input size; số task vs số core. Con số đơn lẻ không nói gì — TỈ LỆ mới nói.

---

## 12. Common Mistakes

1. **Tăng tài nguyên đầu tiên** — đắt, thường vô hiệu (Amdahl), và che triệu chứng khiến bệnh nặng hơn về sau. Bước 10 là bước 10.
2. **Đổi nhiều config một lần** — không quy được công/tội cho từng thay đổi; cluster thành nồi lẩu config không ai dám đụng.
3. **Tuning không baseline** — "hình như nhanh hơn" không phải kết quả. Không có số before thì mọi số after vô nghĩa.
4. **Copy config từ blog/Stack Overflow** — số của cluster người ta, workload người ta. Dùng làm điểm xuất phát thí nghiệm thì được, dán vào production mù quáng thì không.
5. **Tối ưu stage không đáng tối ưu** — stage 2 phút trong job 60 phút, có nhanh 10× cũng chỉ cứu được 1.8 phút. Luôn bắt đầu từ Pareto (bước 2).
6. **Quên điều kiện dừng** — đạt SLA rồi vẫn tuning tiếp vì "cuốn". Thời gian kỹ sư đắt hơn máy.
7. **Không ghi tuning notes** — 6 tháng sau chính bạn hỏi "cái `shuffle.partitions=847` này ở đâu ra?" và không ai dám sửa. Mỗi số một lý do, mỗi thay đổi một dòng.
8. **Restart job lúc sự cố trước khi chụp bằng chứng** — đốt hiện trường. UI của application chết là mất (trừ khi có history server); chụp trước, restart sau.

---

## 13. Interview

**Junior:**

1. *Job Spark chậm — việc ĐẦU TIÊN bạn làm là gì?* — Không đổi gì cả: đo baseline (thời gian, cỡ dữ liệu, tài nguyên) và mở Spark UI tìm job/stage chiếm nhiều thời gian nhất. Xác định ĐÍCH DANH chỗ chậm rồi mới chẩn đoán — đo trước, sửa sau.
2. *Vì sao không nên đổi nhiều config cùng lúc?* — Không tách được tác động từng thay đổi: cái tốt và cái xấu triệt tiêu nhau, kết quả ròng gây hiểu lầm; và về sau không biết config nào đang gánh hệ thống. Mỗi lần một thay đổi, đo, ghi lại, quyết giữ/rollback.
3. *Kể 5 nguyên nhân phổ biến làm job chậm.* — Shuffle quá nhiều/quá to; data skew (1 task rùa kéo stage); spill do thiếu memory cho cỡ task; UDF Python thay vì built-in; small files/partition không hợp lý (quá vụn hoặc quá to). (Cộng thêm: join strategy sai, cache thiếu/thừa, thiếu tài nguyên thật.)
4. *Nhìn đâu trên UI để biết stage nào là thủ phạm?* — Tab Jobs sort theo duration → vào job nặng nhất → Stages: stage chiếm phần lớn thời gian. Rồi Summary Metrics của stage đó (percentiles duration, shuffle read/write, spill) cho biết triệu chứng cụ thể.

**Mid:**

5. *Trình bày trình tự tuning của bạn.* — (1) baseline & SLA; (2) UI khoanh vùng job/stage; (3) soi shuffle — giảm data trước shuffle, bớt shuffle; (4) skew — broadcast/AQE/salting; (5) spill — tăng partition trước, memory sau; (6) UDF — thay built-in/pandas UDF; (7) partition count & file layout; (8) join strategy & broadcast; (9) cache đúng chỗ; (10) cuối cùng mới resource sizing. Nguyên tắc xuyên suốt: sửa "hình dạng" trước, đổ tiền sau; mỗi lần một thay đổi.
6. *Spill và skew liên hệ thế nào, phân biệt trên UI ra sao?* — Spill xảy ra khi task xử lý nhiều dữ liệu hơn memory execution của nó. Nếu spill ĐỀU mọi task → partition to quá/memory thiếu → tăng số partition hoặc memory. Nếu spill CHỈ ở 1–2 task → chính là skew (hot partition) → xử theo hướng skew, tăng memory toàn cụm là lãng phí.
7. *Khi nào tăng `shuffle.partitions`, khi nào giảm?* — Tăng khi task quá to (spill, chậm đều, > ~200–300 MB/task) để mỗi task ôm ít hơn. Giảm (hoặc để AQE coalesce) khi task quá vụn (nghìn task < 100ms, overhead scheduler + small files khi ghi). Với AQE bật: set trần hào phóng + advisory size, để engine tự co.
8. *Sếp bảo "tăng gấp đôi cluster cho nhanh gấp đôi" — bạn trả lời sao?* — Chỉ đúng khi job song song hoàn hảo. Các phần không song song không nhanh lên: stage skew (chờ 1 task), driver listing/collect, số stage tuần tự (Amdahl). Đề nghị: cho tôi 1 buổi chạy playbook đo trước — thường fix hình dạng plan rẻ hơn và ăn hơn; nếu sau đó vẫn cần scale, ta scale với số liệu chứng minh.

**Senior:**

9. *Thiết kế quy trình/văn hoá tuning cho cả team, không chỉ cá nhân?* — (a) Chuẩn hoá: playbook viết thành doc, runbook on-call riêng bản rút gọn; writer template & bộ config mặc định có lý do từng dòng. (b) Đo lường: metrics job (duration, input size, shuffle, spill, file count) đẩy về dashboard, alert theo lệch so với lịch sử — bắt bệnh ở giai đoạn ủ. (c) Tri thức: tuning notes bắt buộc trong PR (thay đổi gì/vì sao/số before-after), post-mortem sau mỗi sự cố với action item "tự lành lần sau". (d) Kỷ luật ROI: ưu tiên tuning theo (tiền cluster + rủi ro SLA) chứ không theo độ thú vị kỹ thuật; định nghĩa điều kiện dừng.
10. *Job chậm mà Spark UI trông... hoàn toàn bình thường — hướng điều tra?* — UI đẹp nghĩa là thời gian trôi NGOÀI các stage: (a) driver làm việc tuần tự — vòng lặp Python gọi nhiều action nhỏ, listing triệu file, lập plan quá lâu (query cực phức tạp); đo bằng log timestamp/py-spy trên driver. (b) Ngoài Spark: chờ nguồn JDBC single-thread, API rate limit, chờ tài nguyên từ cluster manager (job pending — xem master UI). (c) Giữa các job: lineage dài re-compute, hoặc code thuần Python xử lý giữa 2 action. Kỹ thuật chung: đóng dấu thời gian từng đoạn code driver, so tổng thời gian stage (UI) với wall-clock — phần chênh chính là "thời gian tàng hình" cần soi.

---

## 14. Summary

### Mindmap

```
                        TUNING PLAYBOOK (L22)
                                │
   ┌──────────────┬─────────────┴─────────────┬────────────────────┐
   ▼              ▼                           ▼                    ▼
NGUYÊN TẮC     10 BƯỚC                    CÔNG CỤ              ON-CALL 3H SÁNG
   │              │                           │                    │
đo trước       1 baseline+SLA   6 UDF?     15 config          0 đừng restart vội
sửa sau        2 UI khoanh vùng 7 partition   khởi điểm       1 cái gì ĐỔI?
đổi 1 thứ      3 shuffle        + files    timed_run          2 treo hay chậm?
mỗi lần        4 skew?          8 join     explain formatted  3 chẩn 1 phút
Pareto: 1-2    5 spill?         9 cache    tuning notes       4 cứu SLA đã,
stage = 80%       ↑ chất xám    10 TIỀN      (bảng 5 cột)       tối ưu để mai
đạt SLA=DỪNG      miễn phí trước  (cuối!)                     5 chụp bằng chứng
```

### Checklist trước khi gõ "Continue"

- [ ] Đọc thuộc 10 bước theo đúng thứ tự — và giải thích được VÌ SAO thứ tự đó.
- [ ] Với mỗi bước 3–9: nói được nhìn metric nào, ngưỡng bao nhiêu, ra đòn gì.
- [ ] Kể được 8–10 config trong bảng 15 config kèm giá trị khởi điểm + lý do.
- [ ] Đã chạy lab: PLAYBOOK_RUN.md đủ 10 mục + bảng tuning notes mỗi dòng 1 thay đổi.
- [ ] Thuộc runbook 3h sáng — nhất là mục 0 (đừng đốt hiện trường) và mục 1 (cái gì đổi?).
- [ ] Giải thích được Amdahl áp vào Spark và vì sao bước 10 đứng cuối.
- [ ] Trả lời 10 câu interview không nhìn đáp án.

---

## 15. Next Lesson

**Project 3 (Tuần 11) — "Cứu pipeline chậm".**

Hết lý thuyết. Tuần này bạn nhận một pipeline PySpark viết tệ TOÀN DIỆN — UDF vô nghĩa, inferSchema, bảng nhỏ không broadcast, groupBy trên key skew, ghi ra hàng nghìn small files, collect() thừa, cache sai chỗ — chạy trên Olist phóng to, trên đúng cluster Docker 1G/1core này. Nhiệm vụ: profile tìm top 3 bottleneck, viết báo cáo chẩn đoán, tối ưu từng vấn đề theo đúng playbook (mỗi ngày một optimization, mỗi lần một thay đổi), và chứng minh cải thiện **≥ 5×** bằng before/after explain, UI screenshots và bảng metrics. Đây là bài kiểm tra tổng hợp cả Module 3 — làm nghiêm túc, nó chính là câu chuyện bạn kể trong buổi phỏng vấn Senior sau này.

> Gõ **"Continue"** khi sẵn sàng.
