# Lesson 39 — Monitoring & alerting: metrics, event log, history server

> Module 6 · Production Engineering · Tuần 21 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **3 nguồn quan sát** một Spark app: Spark UI live (:4040), **event log + History Server** (hồ sơ sau khi chết), và **metrics system** (sink sang Prometheus/JMX/Graphite — dòng số liệu thời gian thực cho máy đọc).
- Config `spark.eventLog.enabled` / `spark.eventLog.dir` và tự chạy một History Server.
- `metrics.properties` và **PrometheusServlet** (Spark 3) — đường ngắn nhất tới Grafana.
- **Metric nào đáng alert** (batch: duration vs baseline, failed tasks, GC ratio, spill; streaming: batch duration vs trigger, input vs process rate, state size, Kafka lag) — và metric nào chỉ là tiếng ồn.
- **Structured logging** trong PySpark job và **listener API** (SparkListener/StreamingQueryListener) để đẩy custom metrics.
- Layout dashboard Grafana gợi ý + **alert runbook mẫu** (điều kiện → ý nghĩa → hành động).

Sau bài này bạn phải làm được:

- Bật event log cho mọi job, dựng History Server, mở lại UI của job đã chết từ hôm qua.
- Kể tên 5 metric batch + 5 metric streaming đáng alert, kèm ngưỡng và hành động.
- Viết một StreamingQueryListener đẩy số liệu batch ra log/HTTP.
- Thiết kế dashboard 2 hàng cho một pipeline mà on-call nhìn 10 giây hiểu tình hình.

Kiến thức dùng trong thực tế: **mỗi ngày on-call**. Deploy (L37) và sizing (L38) là sinh con; monitoring là nuôi con. Không có bài này, mọi sự cố đều bắt đầu bằng tin nhắn của người dùng: "sao số liệu hôm nay không có?" — tức là bạn đã thua từ 6 tiếng trước.

---

## 2. Why

### Ba câu hỏi mà :4040 không trả lời được

Suốt 38 bài, công cụ quan sát của bạn là Spark UI :4040. Nhưng production đặt 3 câu hỏi nó bó tay:

1. **"Job chết 3h sáng, vì sao?"** — 4040 là của driver; driver chết là UI bốc hơi. Bạn cần hồ sơ tồn tại NGOÀI driver → **event log + History Server**.
2. **"Job hôm nay chậm hơn mọi ngày không?"** — UI cho snapshot một app, không cho chuỗi thời gian 30 ngày để so baseline → **metrics sink + Prometheus/Grafana**.
3. **"Ai đánh thức tôi TRƯỚC khi người dùng phát hiện?"** — UI là công cụ kéo (bạn phải mở), alert là công cụ đẩy → **alert rule trên metrics**.

```
                 ┌──────────────────────────────────────────────┐
                 │        3 NGUỒN QUAN SÁT MỘT SPARK APP        │
                 └──────────────────────────────────────────────┘
  ① SPARK UI :4040          ② EVENT LOG + HISTORY      ③ METRICS SYSTEM
  (live, của driver)           SERVER :18080              (sink liên tục)
  người NHÌN lúc đang chạy    driver ghi từng sự kiện    counter/gauge/timer
  chết theo driver            (JSON) ra HDFS/S3/dir      → Prometheus/JMX/
                              → History Server đọc lại     Graphite → GRAFANA
                              dựng NGUYÊN UI               → ALERT đánh thức
  "bây giờ ra sao?"           "hôm qua chuyện gì?"        "có bất thường không?"
```

### Nếu không có thì sao?

- Không event log: mọi post-mortem chỉ còn log text — mất DAG, mất task metrics, mất timeline. Debug bằng khảo cổ học.
- Không metrics: "hôm nay chậm" là cảm giác, không phải dữ kiện. Không baseline thì không có khái niệm "bất thường".
- Không alert: hệ thống của bạn được giám sát bởi... khách hàng.

### Trade-off

| Được | Mất |
|---|---|
| Post-mortem đầy đủ như đang mở UI live | Event log chiếm storage (job to → file GB; phải dọn định kỳ) |
| Baseline + alert chủ động | Nuôi thêm hạ tầng (history server, Prometheus, Grafana) |
| Một dashboard cho 50 job | Rủi ro nghịch lý: QUÁ nhiều metric/alert → mù vì nhiễu |

> Bài học Senior: monitoring tốt không phải là đo NHIỀU, mà là đo thứ **có người hành động khi nó lệch**. Mỗi alert phải trả lời trước 3 câu: điều kiện gì, nghĩa là gì, LÀM GÌ. Alert không kèm hành động là spam có tổ chức.

---

## 3. Theory

### 3.1. Nguồn ①: Spark UI live — điểm lại vai trò

Đã dùng 38 bài. Chốt lại đúng một điều mới: UI thực chất được dựng từ **cùng dòng sự kiện** mà event log ghi lại. UI = view bộ nhớ của event stream; event log = event stream ghi ra file. Hiểu vậy thì History Server hết bí ẩn: nó chỉ là "UI phát lại băng ghi hình".

### 3.2. Nguồn ②: Event log + History Server

**Bật ghi hình** (per-job, đặt ở tầng submit — đúng tinh thần L37):

```bash
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/workspace/spark-events        # production: hdfs:// hoặc s3a://
--conf spark.eventLog.rolling.enabled=true \             # job dài/streaming: cắt file
--conf spark.eventLog.rolling.maxFileSize=128m
```

Driver sẽ ghi từng sự kiện — `SparkListenerApplicationStart`, `SparkListenerTaskEnd` (kèm đủ metrics của task!), `SparkListenerStageCompleted`... — thành **file JSON mỗi dòng một event**. Vì là JSON, bạn parse được bằng 10 dòng Python (lab sẽ làm).

**Máy phát lại** — History Server:

```bash
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:/workspace/spark-events"
/opt/spark/sbin/start-history-server.sh     # UI tại :18080
```

Config đáng biết: `spark.history.fs.cleaner.enabled=true` + `maxAge=30d` (tự dọn hồ sơ cũ — không dọn thì S3 phình vô hạn); `spark.history.fs.update.interval` (bao lâu quét thư mục một lần — job mới xong đôi khi cần chờ vài giây mới hiện).

Hai lưu ý thực chiến:
- Event log **không chứa log text** (stdout/stderr của executor) — nó chứa SỰ KIỆN + METRICS. Log text vẫn phải gom riêng (yarn logs / kubectl logs / fluentd).
- App đang chạy hiện trong tab "Incomplete applications"; app crash không kịp ghi event cuối cũng nằm đó — bản thân điều này là manh mối (chết đột tử, thường là driver bị kill).

### 3.3. Nguồn ③: Metrics system — Spark tự nói về mình bằng số

Spark có metrics registry (thư viện Dropwizard) trong TỪNG component: driver, executor, master, worker. Mỗi component đẩy metrics của mình ra các **sink** cấu hình trong `metrics.properties`:

```properties
# conf/metrics.properties — cú pháp: <instance>.sink.<tên>.<option>
# instance: master|worker|driver|executor|* (tất cả)

# Sink 1: JMX — cho jmx_exporter/jconsole
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink

# Sink 2: Graphite/StatsD — hệ cũ nhưng còn phổ biến
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=graphite.cty.vn
*.sink.graphite.port=2003
*.sink.graphite.period=10

# Sink 3 (Spark 3, KHUYÊN DÙNG): PrometheusServlet — expose HTTP endpoint
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
```

Với PrometheusServlet, metrics xuất hiện ngay TRÊN UI có sẵn (không cần process phụ):

| Endpoint | Của ai |
|---|---|
| `http://driver:4040/metrics/prometheus` | driver (DAG scheduler, memory, JVM...) |
| `http://driver:4040/metrics/executors/prometheus` | tổng hợp executor (cần thêm `spark.ui.prometheus.enabled=true`) |
| `http://master:8080/metrics/master/prometheus` | standalone master |

Prometheus scrape các endpoint này định kỳ → Grafana vẽ → Alertmanager gọi bạn. Nhóm metric đáng nhớ: `jvm.heap.used`, `executor.filesystem.*`, `DAGScheduler.stage.failedStages`, `HiveExternalCatalog.*`, và với streaming: `spark.streaming.*` states qua listener (3.5).

**Phân vai 3 nguồn** — câu chốt: UI cho MẮT lúc sống, event log cho MẮT sau khi chết, metrics cho MÁY 24/7.

### 3.4. Metric nào ĐÁNG alert (và ngưỡng gợi ý)

**Batch job:**

| Metric | Điều kiện đáng ngờ | Vì sao |
|---|---|---|
| Job/stage duration | > baseline × 1.5–2 (so cùng job 7 ngày) | Dữ liệu phình, skew mới, cluster nghẽn |
| Failed tasks | > 0 kéo dài / retry rate tăng | Chớm bệnh: OOM lác đác, node hỏng — trước khi fail hẳn |
| App failed / không chạy đúng lịch | fail HOẶC "quá giờ mà chưa thấy start" | Alert "vắng mặt" quan trọng ngang alert "thất bại"! |
| GC time ratio | > 10% task time | Heap sai cỡ (L38), cache tràn |
| Shuffle spill (disk) | > 0 đáng kể và tăng dần | Execution memory hụt — điềm báo OOM tương lai |
| Executor lost | > 1–2 / giờ | Node bệnh, overhead thiếu (exit 137), spot bị thu |

**Streaming (Structured Streaming) — nghiêm ngặt hơn vì nợ chồng lãi:**

| Metric | Điều kiện | Vì sao |
|---|---|---|
| **Batch duration vs trigger interval** | duration > trigger, liên tiếp ≥3 batch | Định nghĩa của "không theo kịp" — lag chỉ còn tăng |
| **Input rate vs process rate** | inputRowsPerSecond > processedRowsPerSecond kéo dài | Cùng bệnh trên, nhìn từ phía tốc độ |
| Kafka consumer lag | tăng đơn điệu qua N phút | Thước đo nợ tuyệt đối; đo từ phía Kafka nên sống cả khi app CHẾT |
| State size | tăng không chặn (numRowsTotal của state store) | Quên watermark/TTL → OOM là chuyện thời gian |
| Batch duration răng cưa | p99 >> p50 | GC hoặc executor flapping — ổn định quan trọng hơn trung bình |
| Query terminated | sự kiện onQueryTerminated có exception | App streaming chết là P1 mặc định |

Nguyên tắc chọn: alert theo **triệu chứng người dùng cảm nhận được** (trễ, thiếu, sai) và vài **chỉ báo sớm** (spill, GC, lag) — không alert theo mọi con số nhúc nhích.

### 3.5. Tự đo: structured logging + Listener API

**Structured logging** — log JSON để máy parse được, mỗi dòng có ngữ cảnh đầy đủ:

```python
import json, logging, sys, time

logger = logging.getLogger("job")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(handler); logger.setLevel(logging.INFO)

def log_event(event: str, **kw):
    logger.info(json.dumps({"ts": time.time(), "event": event,
                            "app": "daily-revenue", **kw}))

log_event("read_done", rows=df.count(), source="s3a://raw/orders")   # đếm CÓ CHỦ ĐÍCH
log_event("write_done", partitions=out.rdd.getNumPartitions())
```

Log dạng này đổ vào Loki/ELK là query được `event="write_done" | avg(rows)` — log biến thành metric nghiệp vụ (số dòng ghi ra hôm nay = 0 là alert đắt giá nhất pipeline!).

**Listener API** — móc vào dòng sự kiện của Spark:

- `SparkListener` (batch): interface JVM — PySpark muốn dùng phải viết Scala/Java đóng jar rồi `--conf spark.extraListeners=com.cty.MyListener`. Biết là có, dùng khi cần sâu.
- `StreamingQueryListener` (streaming): **PySpark hỗ trợ thẳng từ 3.4** — đây là công cụ chính của bạn:

```python
from pyspark.sql.streaming import StreamingQueryListener

class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, e):  log_event("query_started", id=str(e.id))
    def onQueryProgress(self, e):
        p = e.progress
        log_event("batch_done",
                  batchId=p.batchId,
                  durationMs=p.durationMs.get("triggerExecution"),
                  inputRate=p.inputRowsPerSecond,
                  processRate=p.processedRowsPerSecond,
                  numInputRows=p.numInputRows)
        # hoặc: đẩy thẳng lên Prometheus Pushgateway/StatsD tại đây
    def onQueryTerminated(self, e):
        log_event("query_terminated", id=str(e.id), exception=str(e.exception))

spark.streams.addListener(MetricsListener())
```

`e.progress` chính là JSON bạn từng thấy ở `query.lastProgress` (lesson 26) — giờ nó tự chảy về hệ thống monitoring thay vì chờ bạn gõ tay.

### 3.6. Dashboard Grafana — layout gợi ý

```
┌─ HÀNG 1: SỐNG CÒN (on-call nhìn 10 giây) ─────────────────────────────┐
│ [App up?]  [Batch duration vs trigger]  [Kafka lag]   [Failed tasks]  │
│  xanh/đỏ     2 đường chồng nhau           1 đường       counter        │
├─ HÀNG 2: HIỆU NĂNG ───────────────────────────────────────────────────┤
│ [Input vs process rate]  [Job duration vs baseline 7d]  [GC ratio]    │
├─ HÀNG 3: TÀI NGUYÊN (nguyên liệu cho L38) ────────────────────────────┤
│ [Executor count (dyn.alloc)]  [Heap used/max]  [Spill bytes] [State]  │
├─ HÀNG 4: NGHIỆP VỤ (từ structured log) ───────────────────────────────┤
│ [Rows written/ngày]  [Freshness: giờ của dữ liệu mới nhất]            │
└────────────────────────────────────────────────────────────────────────┘
```

Nguyên tắc: hàng trên trả lời "có cháy không", hàng dưới trả lời "vì sao cháy". Hàng 4 hay bị quên nhất mà lại gần người dùng nhất — pipeline "xanh" nhưng ghi 0 dòng vẫn là pipeline chết.

---

## 4. Internal

Đường đi của một sự kiện, từ lúc task xong đến lúc điện thoại bạn rung:

```
① Executor hoàn thành task → gửi TaskEnd + metrics (bytes, spill, GC...)
   về driver qua heartbeat/RPC
        │
② Trong driver: LiveListenerBus phát sự kiện vào HÀNG ĐỢI cho các listener:
   ├─ AppStatusListener   → cập nhật bộ nhớ cho Spark UI :4040
   ├─ EventLoggingListener→ serialize JSON, GHI event log (nếu bật)
   └─ listener CỦA BẠN    → StreamingQueryListener/SparkListener
        │
   ⚠ hàng đợi có giới hạn (spark.scheduler.listenerbus.eventqueue.capacity,
     mặc định 10000) — listener CHẬM làm rơi event: UI thiếu số liệu,
     log có "Dropped events". Listener của bạn phải NHANH (đừng gọi HTTP
     đồng bộ 2s trong onQueryProgress!)
        │
③ Song song, metrics registry (Dropwizard) của driver/executor được sink
   đọc theo chu kỳ → PrometheusServlet trả qua HTTP khi bị scrape
        │
④ Prometheus scrape 15s/lần → lưu time series
⑤ Alert rule đánh giá liên tục, ví dụ:
   (batch_duration > trigger) trong 3 lần đánh giá → FIRING
⑥ Alertmanager định tuyến → Slack/PagerDuty → điện thoại rung
        │
⑦ Bạn mở History Server :18080 — đọc "băng ghi hình" do ② tạo ra,
   thấy đúng UI như lúc app còn sống → chẩn đoán → runbook → hành động
```

Vòng lặp khép kín: **event bus là trái tim** — cả UI, event log, lẫn listener của bạn đều là khách hàng của cùng một dòng sự kiện. Vì thế chúng nhất quán với nhau, và vì thế listener chậm là tội nặng (nghẽn tim).

---

## 5. API

### `spark.eventLog.*` — bật hồ sơ

```bash
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=s3a://logs/spark-events \
--conf spark.eventLog.rolling.enabled=true
```

- **Pitfall**: thư mục phải TỒN TẠI TRƯỚC (Spark không tự tạo → app fail ngay lúc start). Streaming không bật rolling → một file event log phình vô hạn.

### History Server + `spark.history.*`

```bash
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://logs/spark-events \
  -Dspark.history.fs.cleaner.enabled=true -Dspark.history.fs.cleaner.maxAge=30d" \
  /opt/spark/sbin/start-history-server.sh    # → :18080
```

- **Pitfall**: quên cleaner — 6 tháng sau bill S3/disk tăng bí ẩn; và history server load app to lần đầu khá chậm (phải phát lại cả file event) — không phải treo.

### `metrics.properties` / PrometheusServlet

```bash
--conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet \
--conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus \
--conf spark.ui.prometheus.enabled=true
```

(Set qua `--conf spark.metrics.conf.*` khỏi cần file — tiện cho per-job.)
- **Pitfall**: endpoint executor tổng hợp (`/metrics/executors/prometheus`) cần đúng flag `spark.ui.prometheus.enabled=true`, quên là 404; và các endpooint này nằm TRÊN 4040 — driver chết là mất, nên metric dùng để alert "app chết" phải đo từ NGOÀI app (Prometheus `up`, Kafka lag exporter).

### `spark.streams.addListener()` — như 3.5

- **Khi dùng**: mọi streaming job production — đây là nguồn số liệu batchDuration/rate/state chuẩn.
- **Pitfall**: code trong listener chạy trong driver và trên event bus — chậm là rơi event; exception trong listener làm log gào thét. Bọc try/except, đẩy async.

### `query.lastProgress` / `spark.sparkContext.statusTracker()`

- Cách "kéo" thủ công khi chưa có hệ metrics: expose 2 giá trị này ra healthcheck HTTP đơn giản cũng đã hơn 0.

---

## 6. Demo nhỏ

Xem PrometheusServlet nhả metrics — không cần cài gì thêm:

```
Input:  1 job bình thường + 3 flag metrics
   ↓    curl endpoint trên 4040 trong lúc chạy
Output: metrics text định dạng Prometheus
```

```bash
docker exec -d spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.ui.prometheus.enabled=true \
  --conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet \
  --conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus \
  /opt/spark/examples/src/main/python/pi.py 500     # chạy đủ lâu để kịp curl

sleep 10 && curl -s localhost:4040/metrics/prometheus | head -20
curl -s localhost:4040/metrics/executors/prometheus | head -20
```

Bạn sẽ thấy các dòng kiểu `metrics_app_..._driver_jvm_heap_used_Value{...} 12345`. Đúng định dạng Prometheus scrape — nghĩa là từ Spark tới Grafana chỉ còn là chuyện cắm ống nước, không còn là chuyện của Spark nữa.

---

## 7. Production Example

Hệ monitoring của một team chạy 40 job Spark trên K8s (nối tiếp ví dụ L37):

```
Spark driver/executor pods
  ├─ event log ──────────► s3a://datalake-logs/spark-events
  │                            ▲
  │                     History Server (1 deployment, CẢ 40 job dùng chung)
  │                     + cleaner 30d — post-mortem mọi job tại một URL
  ├─ PrometheusServlet ──► Prometheus (scrape qua pod annotation)
  ├─ stdout JSON logs ───► Fluent Bit → Loki (structured logging 3.5)
  └─ Kafka lag ──────────► kafka-lag-exporter (đo từ NGOÀI app!)
                               │
                         Grafana (dashboard layout 3.6)
                               │
                         Alertmanager → #data-oncall + PagerDuty
```

**Runbook mẫu** (trích 4 dòng thật — mỗi alert PHẢI có một dòng như vầy trong wiki):

| Alert (điều kiện) | Ý nghĩa | Hành động |
|---|---|---|
| `streaming_batch_duration > trigger 15 phút` | Không theo kịp input; lag đang tích | Mở Grafana xem input rate: spike tạm thời → theo dõi; bền vững → tăng maxOffsetsPerTrigger tạm, sau đó sizing lại (L38); kiểm tra skew ở History Server |
| `kafka_lag tăng đơn điệu 30 phút VÀ app up` | App sống nhưng xử lý hụt hơi (nếu app chết đã có alert khác) | Như trên + kiểm tra GC ratio; nếu state size cũng tăng → nghi watermark hỏng (điều tra ngay, đừng restart vô tội vạ — mất manh mối) |
| `job đêm không start sau 02:30` | Scheduler/Airflow hỏng, KHÔNG phải Spark | Kiểm tra Airflow trước tiên; đây là alert "vắng mặt" — rẻ nhất, cứu nhiều nhất |
| `rows_written == 0 (từ structured log)` | Pipeline "xanh" nhưng vô sinh — upstream rỗng hoặc filter sai | Chặn downstream tiêu thụ, kiểm tra upstream freshness, so schema (cột đổi tên làm filter loại 100%?) |

Điểm doanh nghiệp đáng học: **Kafka lag đo bằng exporter ngoài app** — vì metric từ trong app chết cùng app; giám sát viên không được ngủ chung giường với kẻ bị giám sát.

---

## 8. Hands-on Lab

**Mục tiêu**: bật event log ghi vào `/workspace/spark-events`, dựng History Server trong Docker, mở lại UI của app ĐÃ CHẾT, parse event log bằng Python, gắn StreamingQueryListener.

### Bước 0 — chuẩn bị

```bash
make up
mkdir -p labs/lab39 spark-events        # thư mục event log PHẢI có trước!
```

### Bước 1 — chạy job có event log rồi ĐỂ NÓ CHẾT

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/workspace/spark-events \
  /opt/spark/examples/src/main/python/pi.py 100
# app xong → driver chết → :4040 KHÔNG còn — đúng vấn đề cần giải
ls spark-events/    # → file app-2026....  (mỗi app một file JSON)
```

### Bước 2 — dựng History Server (container riêng, chung network + volume)

```bash
docker run -d --name spark-history \
  --network spark-mastery_default \
  -p 18080:18080 \
  -v "$PWD:/workspace" \
  -e SPARK_NO_DAEMONIZE=true \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:/workspace/spark-events" \
  apache/spark:3.4.1 /opt/spark/sbin/start-history-server.sh
```

Mở `http://localhost:18080` → thấy app pi vừa chết → click vào: **đầy đủ Jobs/Stages/Executors/SQL như đang sống**. Đây là khoảnh khắc "người chết kể chuyện" — mọi post-mortem từ nay bắt đầu tại đây.

### Bước 3 — parse event log bằng tay (hiểu thứ dưới nắp)

```python
# labs/lab39/parse_eventlog.py — chạy: python3 labs/lab39/parse_eventlog.py spark-events/<file>
import json, sys
from collections import Counter

events, durations = Counter(), []
for line in open(sys.argv[1]):
    e = json.loads(line)
    events[e["Event"]] += 1
    if e["Event"] == "SparkListenerTaskEnd":
        info = e["Task Info"]
        durations.append((info["Finish Time"] - info["Launch Time"], info["Executor ID"]))

print(events.most_common(10))
durations.sort(reverse=True)
print("5 task chậm nhất (ms, executor):", durations[:5])
if durations:
    med = durations[len(durations)//2][0]
    print("max/median =", round(durations[0][0]/max(med,1), 1), "→ >5 là dấu hiệu skew/straggler")
```

Chính script bé này là phôi thai của mọi công cụ kiểu "phân tích job tự động" — event log là API, UI chỉ là một client của nó.

### Bước 4 — StreamingQueryListener đẩy metrics

```python
# labs/lab39/streaming_metrics.py
import json, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener

class L(StreamingQueryListener):
    def onQueryStarted(self, e): print(json.dumps({"event": "started", "id": str(e.id)}))
    def onQueryProgress(self, e):
        p = e.progress
        print(json.dumps({"event": "batch", "batchId": p.batchId,
                          "durMs": p.durationMs.get("triggerExecution"),
                          "inRate": p.inputRowsPerSecond,
                          "procRate": p.processedRowsPerSecond}))
    def onQueryIdle(self, e): pass
    def onQueryTerminated(self, e): print(json.dumps({"event": "terminated"}))

spark = SparkSession.builder.appName("lab39-stream").getOrCreate()
spark.streams.addListener(L())

df = spark.readStream.format("rate").option("rowsPerSecond", 500).load()
q = (df.withColumn("k", F.col("value") % 10).groupBy("k").count()
       .writeStream.format("console").outputMode("complete")
       .trigger(processingTime="5 seconds")
       .option("checkpointLocation", "/workspace/labs/lab39/ckpt").start())
q.awaitTermination(40); q.stop(); spark.stop()
```

```bash
make run F=labs/lab39/streaming_metrics.py
```

Đọc các dòng JSON `"event": "batch"`: durMs có < 5000 (trigger) không? Tăng `rowsPerSecond` lên 50000 chạy lại — quan sát procRate đuối dần so với inRate: bạn vừa nhìn thấy TẬN MẮT hai metric alert quan trọng nhất của streaming.

### Bước 5 — tổng kết vào NOTES.md

Ghi: (1) URL của 3 nguồn quan sát trong setup này; (2) một app trên :18080 mà bạn đọc được số task/spill; (3) từ output bước 4, batch nào "không theo kịp" và căn cứ.

---

## 9. Assignment

**Easy** — (bám ROADMAP) Dùng `parse_eventlog.py` (mở rộng nếu cần) trên event log của một job lab cũ chạy lại với event log bật (gợi ý: job join Olist ở lab01): in tổng số task, task chậm nhất thuộc stage nào, tỉ lệ max/median duration. Đối chiếu con số với History Server UI — có khớp không?

**Medium** — (bám ROADMAP) Viết `labs/lab39/check_baseline.py`: nhận thư mục event log, nhóm các lần chạy theo `spark.app.name`, tính duration từng lần (ApplicationStart → ApplicationEnd), rồi in cảnh báo `ALERT` nếu lần chạy mới nhất > **baseline × 2** (baseline = median các lần trước). Chạy job pi 3 lần bình thường + 1 lần với tham số to gấp 10 để kích hoạt alert. Đây chính là alert-theo-baseline thu nhỏ.

**Hard** — (bám ROADMAP) Trace MỘT task xuyên suốt: chọn task chậm nhất từ event log, trích từ `SparkListenerTaskEnd` → `Task Metrics` đầy đủ chuỗi: `Executor Deserialize Time` → `Executor Run Time` → `JVM GC Time` → `Shuffle Read Metrics` (Fetch Wait Time, Remote Bytes Read) → `Memory Bytes Spilled`. Vẽ timeline text cho task đó và KẾT LUẬN nó chậm vì đâu (GC? chờ shuffle? spill?). Viết 5 dòng: metric nào trong chuỗi này đáng đưa lên dashboard cho MỌI job?

**Production Challenge** — Viết **alert runbook** hoàn chỉnh (markdown, 6–8 alert) cho pipeline capstone của bạn (batch Olist + streaming Kafka của module 4): mỗi alert gồm điều kiện đo được (kèm nguồn: event log / listener / lag exporter), mức độ (page ngay / giờ hành chính), ý nghĩa, 3 bước hành động đầu tiên, và tiêu chí đóng alert. Ràng buộc: ÍT NHẤT một alert "vắng mặt" (job không chạy) và một alert nghiệp vụ (rows_written). Mentor sẽ chấm như review runbook thật: alert nào thiếu hành động sẽ bị gạch.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Monitoring cũng có giá — đừng để máy đo làm chậm máy chạy:

| Thao tác | Chi phí | Khuyến nghị |
|---|---|---|
| Event log bật | Rất nhỏ (ghi tuần tự, async) | Bật CHO MỌI JOB production — mặc định, không bàn |
| Event log cho streaming 24/7 | File phình vô hạn | `rolling.enabled=true` + cleaner trên history server |
| Listener làm việc nặng (HTTP sync, count()!) | Nghẽn event bus → **rơi event**, UI thiếu số liệu | Listener chỉ format + đẩy async; tuyệt đối không gọi action Spark trong listener |
| `df.count()` để log số dòng | Cả một job quét lại dữ liệu! | Đếm bằng accumulator/observe API (`df.observe()`), hoặc lấy `numOutputRows` từ chính event log |
| Prometheus scrape 1s | Ồn + tải UI endpoint | 15–30s là đủ cho Spark |
| Giữ history 365 ngày | Storage + history server load chậm | 30–90 ngày; job đặc biệt thì export file event log ra nơi khác |

Viên ngọc ẩn đáng biết: `df.observe("stats", F.count(F.lit(1)).alias("rows"))` — đếm số dòng KHÔNG tốn thêm job, kết quả nhận qua `QueryExecutionListener`/progress. Đây là cách log số liệu nghiệp vụ chuẩn performance.

---

## 11. Spark UI

Bài này chính thức mở khóa **History Server :18080** — và một tab UI hay bị bỏ quên:

**History Server** — khác gì UI live:
- Trang chủ liệt kê app theo thời gian: cột Duration + Spark User + State — bản thân nó đã là "dashboard baseline" thô sơ (job hằng đêm đứng cạnh nhau, cái nào dài bất thường lộ ngay).
- "Incomplete applications": app đang chạy HOẶC chết đột tử không kịp ghi event cuối — thấy app cũ nằm đây mãi = manh mối driver bị kill cứng.
- Mọi tab quen thuộc hoạt động y nguyên; SQL tab vẫn mở được query plan của job đã chết từ tuần trước — hãy dùng nó cho bài Hard.

**Tab Structured Streaming** (trên UI live 4040 của app streaming): đồ thị Input Rate / Process Rate / Batch Duration theo thời gian — chính là phiên bản có sẵn của những gì listener bước 4 in ra. Nhìn hình răng cưa batch duration một lần bằng đọc mười trang lý thuyết GC.

**Executors tab** (góc nhìn mới): cột Task Time (GC Time) tô đỏ khi GC > 10% — Spark tự alert cho bạn trong UI từ lâu; giờ bạn chỉ chuyển ngưỡng đó ra Prometheus để nó chạy 24/7.

---

## 12. Common Mistakes

1. **Không bật event log vì "job đang chạy tốt"** → sự cố đầu tiên xảy ra là mất trắng hiện trường. Event log là dây an toàn: mặc định bật, không thương lượng.
2. **Quên tạo trước thư mục event log** → app fail ngay khi start với lỗi FileNotFound — deploy nửa đêm fail vì lý do ngớ ngẩn nhất có thể.
3. **Alert mọi thứ động đậy** (executor add/remove, mỗi task fail lẻ) → on-call mute channel sau 2 tuần → alert thật bị chôn trong nhiễu. Ít alert, alert nào cũng có runbook.
4. **Chỉ alert "job fail", quên alert "job KHÔNG CHẠY"** — scheduler hỏng thì không có ai fail cả, chỉ có sự im lặng. Alert vắng mặt (job chưa start sau giờ X) bắt được lớp lỗi này.
5. **Đo sức khỏe app bằng metric do chính app phát** — app chết là metric tắt, alert "app chết" không bao giờ bắn. Sức khỏe đo từ ngoài: Prometheus `up`, Kafka lag exporter, freshness của bảng đích.
6. **`count()` rải rác để logging** → mỗi count là một job thật (bài học lesson 1 quay lại đòi nợ). Dùng `observe()`/accumulator/event log.
7. **Listener nặng nề** gọi API đồng bộ trong `onQueryProgress` → event bus nghẽn, "Dropped events" trong log, UI thiếu dữ liệu — máy đo làm hỏng thứ nó đo.
8. **Dashboard 60 panel không có thứ tự** → 3h sáng không ai biết nhìn ô nào. Hàng 1 phải trả lời được "có cháy không" trong 10 giây (layout 3.6).

---

## 13. Interview

**Junior:**

1. *Kể 3 nguồn quan sát một Spark app và vai trò từng nguồn.* — (1) Spark UI :4040: nhìn live, do driver phục vụ, chết theo driver; (2) event log + History Server: driver ghi mọi sự kiện ra file, History Server phát lại nguyên UI cho app đã chết — công cụ post-mortem; (3) metrics system: sink Prometheus/JMX/Graphite đẩy số liệu liên tục cho dashboard + alert 24/7.
2. *Job chết rồi, xem lại Spark UI bằng cách nào?* — Phải bật từ TRƯỚC: `spark.eventLog.enabled=true` + `eventLog.dir` trỏ HDFS/S3/thư mục chung; chạy History Server trỏ cùng thư mục; mở :18080. Không bật trước thì không có cách nào — vì thế production luôn bật mặc định.
3. *Event log chứa gì và KHÔNG chứa gì?* — Chứa: sự kiện có cấu trúc (app/job/stage/task start-end) kèm đầy đủ task metrics (duration, shuffle, spill, GC) — đủ dựng lại UI. Không chứa: log text stdout/stderr của driver/executor — phần đó gom riêng (yarn logs, kubectl logs, fluentd).
4. *PrometheusServlet là gì?* — Sink metrics có sẵn từ Spark 3: expose metrics định dạng Prometheus ngay trên UI port có sẵn (driver 4040 `/metrics/prometheus`, executors `/metrics/executors/prometheus` với `spark.ui.prometheus.enabled=true`) — Prometheus scrape trực tiếp, không cần process phụ như jmx_exporter.

**Mid:**

5. *Với batch pipeline, bạn alert trên metric nào? Ngưỡng?* — Nhóm triệu chứng: app fail; app KHÔNG start đúng lịch (alert vắng mặt); duration > baseline × 1.5–2 (baseline theo lịch sử chính job đó). Nhóm chỉ báo sớm: failed task tăng, GC > 10% task time, spill disk tăng dần, executor lost lặp lại. Kèm alert nghiệp vụ: rows_written = 0, data freshness.
6. *Với streaming, metric nào quan trọng nhất? Vì sao?* — Batch duration vs trigger interval (và cặp input rate vs process rate — cùng bệnh hai góc nhìn): duration > trigger kéo dài nghĩa là không theo kịp, lag chỉ tăng — mọi thứ khác là hệ quả. Kèm: Kafka lag đo từ exporter ngoài app (sống cả khi app chết), state size tăng không chặn (watermark hỏng → OOM), sự kiện queryTerminated.
7. *StreamingQueryListener dùng làm gì, lưu ý gì?* — Móc vào vòng đời query: onQueryProgress nhận progress mỗi micro-batch (batchId, durationMs, rates, state metrics) → đẩy log JSON/Prometheus làm nguồn alert. Lưu ý: chạy trên event bus của driver — phải nhẹ và không ném exception; PySpark hỗ trợ trực tiếp từ 3.4.
8. *Làm sao log số dòng job ghi ra mà không làm chậm job?* — Không dùng `count()` (thêm một job quét lại). Dùng `df.observe()` + listener, accumulator, hoặc đọc `numOutputRows` từ event log/SQL metrics — các cách này đo ngay trên dòng chảy sẵn có, chi phí ~0.

**Senior:**

9. *Thiết kế hệ monitoring cho 50 job Spark (batch + streaming) trên K8s — kiến trúc và nguyên tắc?* — Một History Server dùng chung đọc event log tập trung trên S3 (+ cleaner); metrics qua PrometheusServlet + pod annotation cho Prometheus; log JSON có cấu trúc qua Fluent Bit → Loki; Kafka lag exporter độc lập. Nguyên tắc: (1) giám sát từ ngoài cho câu hỏi sống/chết — không tin app tự khai; (2) alert phải có runbook điều kiện→ý nghĩa→hành động, ưu tiên triệu chứng người dùng + alert vắng mặt; (3) baseline theo từng job thay vì ngưỡng tuyệt đối chung; (4) tầng nghiệp vụ (rows, freshness) bắt buộc — pipeline xanh mà bảng đích ế là vẫn cháy.
10. *Event log của job 8 tiếng nặng vài GB, History Server mở chậm — xử lý thế nào? Và điều gì làm event log mất tin cậy?* — Giảm nguồn: `spark.eventLog.rolling.enabled` cắt file, xem lại job có quá nhiều task tí hon (200k task = 200k cặp event — dấu hiệu partition sai từ L38); tăng heap history server, bật cleaner giới hạn tuổi. Mất tin cậy khi: event bus quá tải làm DROP event (listener chậm, queue capacity — log có "Dropped events", số liệu UI/event log thiếu), hoặc driver chết cứng không kịp flush — thấy app kẹt ở "incomplete" bất thường thì phải nghi ngờ chính hồ sơ. Người làm monitoring giỏi biết cả giới hạn của công cụ đo.

---

## 14. Summary

### Mindmap

```
                    MONITORING & ALERTING (L39)
                              │
   ┌──────────────┬───────────┴──────────┬─────────────────────┐
   ▼              ▼                      ▼                     ▼
3 NGUỒN        ALERT GÌ?             TỰ ĐO                 VẬN HÀNH
   │              │                      │                     │
UI :4040       batch: duration        structured log JSON   dashboard 4 hàng:
 (live, chết    vs baseline, fail,    observe() thay count   cháy?/vì sao/
 theo driver)   GC>10%, spill,        StreamingQuery-        tài nguyên/nghiệp vụ
event log +     KHÔNG-start           Listener (PySpark      runbook: điều kiện
HistoryServer  streaming: batch>       3.4+): durMs, rates    → ý nghĩa → HÀNH ĐỘNG
 :18080         trigger, in>proc      SparkListener (JVM)    alert vắng mặt +
 (post-mortem)  rate, state size,     listener phải NHẸ      nghiệp vụ (rows=0)
metrics sink    Kafka lag (đo từ      (event bus chung       đo sống/chết từ
 Prometheus     NGOÀI app)             với UI!)               NGOÀI app
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ được sơ đồ 3 nguồn quan sát và nói đúng câu hỏi mỗi nguồn trả lời.
- [ ] Đã bật event log, dựng History Server bằng Docker, mở UI của app đã chết.
- [ ] Parse được event log bằng Python, chỉ ra task chậm nhất.
- [ ] Kể 5 metric batch + 5 metric streaming đáng alert, kèm hành động.
- [ ] Viết được StreamingQueryListener và giải thích vì sao nó phải nhẹ.
- [ ] Biết vì sao "app chết" phải đo từ ngoài app, và vì sao cần alert vắng mặt.
- [ ] Có một runbook mẫu điều kiện → ý nghĩa → hành động của chính bạn.

---

## 15. Next Lesson

**Lesson 40 — Debugging playbook: OOM, stragglers, stuck jobs.**

Monitoring vừa reo: job fail lúc 3h07. Bạn mở History Server, thấy stage 4 chết với `ExecutorLostFailure`... rồi sao nữa? Lesson 40 là cuốn cẩm nang trong túi on-call: phân loại OOM (driver hay executor? heap hay overhead? — L38 đã cho bạn nửa câu trả lời), truy straggler (skew hay node bệnh?), và xử lý loại quái đản nhất — job STUCK không chết không tiến. Ta sẽ cố tình làm hỏng job theo 5 kiểu kinh điển rồi lần theo dấu vết bằng đúng bộ công cụ của L39 — vì cách duy nhất để bình tĩnh lúc 3h sáng là đã từng thấy đám cháy đó vào ban ngày.

> Gõ **"Continue"** khi sẵn sàng.
