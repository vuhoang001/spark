# Project 4 (CAPSTONE) — Real-time Fraud Detection

> Module 6 · Production Engineering · Tuần 23–24 · Thời lượng: 2 tuần (16–24 giờ)

---

## 1. Đây là gì và tại sao nó là capstone

Đây là project cuối cùng của khóa — và là project duy nhất được viết như một **design spec production thật**, kiểu tài liệu bạn sẽ nhận (hoặc tự viết) khi vào team Data Platform của một công ty fintech. Không còn "lab từng bước cầm tay". Bạn được cho: yêu cầu nghiệp vụ, SLA, kiến trúc đích, 8 checkpoint và tiêu chí nghiệm thu. Cách bạn lấp khoảng trống giữa chúng chính là thứ được chấm.

Project này ép bạn dùng lại **gần như toàn bộ khóa học**:

| Kỹ năng | Học ở |
|---|---|
| Schema tường minh, JSON parsing | Lesson 5, 11 |
| Transformations, `when/otherwise` | Lesson 7 |
| Stream-static join, broadcast | Lesson 9, 28 |
| Kafka source/sink, checkpoint, trigger | Lesson 24 |
| Watermark, event time | Lesson 25 |
| Stateful processing, state store, TTL | Lesson 26 |
| Exactly-once, foreachBatch, multi-sink | Lesson 27 |
| Iceberg write, maintenance | Lesson 31, 32 |
| Monitoring, metrics, lag | Lesson 39 |
| Deployment, resource sizing, Airflow | Lesson 36, 37, 38 |

Nếu có checkpoint nào bạn không biết bắt đầu từ đâu — đó là tín hiệu quay lại lesson tương ứng, không phải tín hiệu bỏ cuộc.

---

## 2. Bối cảnh nghiệp vụ & yêu cầu

### Bối cảnh

Bạn là Data Engineer của một công ty thanh toán. Team Risk cần một hệ thống **chấm điểm rủi ro giao dịch thẻ theo thời gian thực**: mỗi giao dịch đi qua phải được gán `risk_score`; giao dịch rủi ro cao phải đẩy alert cho hệ thống chặn giao dịch (downstream của team khác — bạn chỉ cần bắn vào Kafka topic); **mọi** giao dịch phải được ghi audit log bất biến để compliance tra soát về sau.

Đây là bài toán kinh điển: **Flink hay Spark?** Team chọn Spark Structured Streaming vì: (a) tổ chức đã có Spark cho batch, một engine dễ vận hành hơn hai; (b) SLA latency là **giây, không phải mili-giây per-event** — hệ chặn giao dịch thật (sub-100ms, synchronous) là một service khác; tầng Spark này là tầng **near-real-time scoring + audit**, chấp nhận trễ vài giây. Nói được câu này trong buổi bảo vệ project = điểm Senior đầu tiên.

### Yêu cầu phi chức năng (đóng khung, treo lên tường)

| Hạng mục | Yêu cầu | Ghi chú |
|---|---|---|
| Throughput | 1.000 events/sec sustained (lab), thiết kế chịu được 10.000/sec | Generator ở mục 5 bơm được cả hai mức |
| Latency end-to-end | P99 < 10 giây (event vào Kafka → alert ra topic) | Trigger 5s → budget còn ~5s cho xử lý |
| SLA availability | Job chết phải tự phục hồi, **không mất và không double-alert khi restart** | Checkpoint + idempotent sink |
| Data retention | Audit log giữ vĩnh viễn (Iceberg); state user giữ 30 ngày | Checkpoint 6 |
| Observability | Lag, error rate, alerts/min, avg risk_score có trên Prometheus | Checkpoint 5, 7 |
| Đúng đắn | Restart giữa chừng → audit log không duplicate (exactly-once) | Test plan mục 6 |

> Lưu ý mentor: P99 latency trong ROADMAP ghi "< 100ms" — đó là SLA của tầng **chặn giao dịch synchronous**, ngoài phạm vi Spark. Với micro-batch, latency sàn = trigger interval. Phân biệt được hai tầng này là một câu hỏi bảo vệ (mục 8).

---

## 3. Kiến trúc end-to-end

```
┌──────────────────┐
│  txn_generator.py │  (Python, mục 5 — bơm 1k–10k events/sec,
│  (máy bạn)        │   trộn ~2% fraud pattern có chủ đích)
└────────┬─────────┘
         │ JSON
         ▼
┌────────────────────────┐
│  KAFKA  topic:          │   (repo ../kafka-flink)
│  `transactions`         │   6 partitions — trần parallelism của source
└────────┬───────────────┘
         │ readStream (maxOffsetsPerTrigger để chống ngộp khi backfill)
         ▼
┌─────────────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING  (job duy nhất)          │
│                                                                  │
│  ① parse JSON theo schema tường minh + watermark 10 phút        │
│  ② ENRICH: stream-static join với `user_profiles`               │
│     (Iceberg, reload mỗi ngày — checkpoint 3)                    │
│  ③ RULE SCORING: amount > $1000, country != home_country, ...   │
│  ④ STATEFUL: applyInPandasWithState — lịch sử 10 txn / 24h      │
│     per user, TTL 30 ngày (checkpoint 4, 6)                      │
│  ⑤ tổng hợp risk_score, gán risk_level                          │
│                                                                  │
│  foreachBatch (checkpoint 5):                                    │
│    ├─► risk_level = HIGH  → Kafka topic `fraud_alerts`          │
│    ├─► TẤT CẢ rows        → Iceberg `lake.fraud.txn_audit`      │
│    └─► counters           → Prometheus (alerts/min, avg score)  │
└──────────────┬──────────────────────────────────────────────────┘
               │ checkpointLocation (offset + state) — linh hồn exactly-once
               ▼
┌───────────────────────┐   ┌───────────────────────┐   ┌──────────────────┐
│ Kafka `fraud_alerts`  │   │ Iceberg audit log      │   │ Prometheus        │
│ → hệ chặn giao dịch   │   │ → Trino / compliance   │   │ → Grafana, alert  │
└───────────────────────┘   └───────────────────────┘   └──────────────────┘

Vòng ngoài (checkpoint 8):
  Airflow: reload profile 1AM, maintenance Iceberg, restart-on-failure
  Kubernetes: spark-submit cluster mode + dynamic allocation / HPA
```

Ba quyết định kiến trúc phải tự bảo vệ được (sẽ bị hỏi ở mục 8):

1. **Một streaming query hay nhiều?** Ta dùng **một query + foreachBatch fan-out 3 sink**, thay vì 3 query đọc riêng. Được: đọc Kafka 1 lần, state 1 nơi, dễ suy luận. Mất: 3 sink không atomic với nhau (Iceberg ghi xong mà Kafka alert fail thì sao? — lesson 27 Hard, xử lý ở checkpoint 5).
2. **Rule-based, không phải ML.** Scoring là `CASE WHEN` — vì project này chấm kỹ năng **data engineering** (đường ống, state, đúng đắn, vận hành), không chấm model. Kiến trúc này cắm ML model vào sau được (thay khối ③ bằng pandas UDF gọi model) — nói được điều đó là đủ.
3. **Audit ghi TẤT CẢ, không chỉ alert.** Compliance cần chứng minh "giao dịch X đã được chấm điểm Y lúc Z" kể cả khi điểm thấp. Audit là append-only, không UPDATE.

---

## 4. 8 Checkpoint

Mỗi checkpoint: mô tả → hướng dẫn kỹ thuật → skeleton → tiêu chí hoàn thành. Thư mục làm việc đề xuất: `labs/project4/`.

### Checkpoint 1 — Transaction event schema

**Mô tả:** Định nghĩa contract của event. Đây là bước bị junior xem thường nhất và là bước senior làm kỹ nhất — schema là API giữa các team.

**Kỹ thuật (lesson 5, 11):** schema tường minh bằng `StructType`, KHÔNG `inferSchema` (streaming cũng không cho phép). Timestamp là **event time** do generator sinh, kiểu ISO-8601 UTC. Mọi field bắt buộc phải non-null theo contract; field nào nullable phải ghi rõ.

```python
# labs/project4/schema.py
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

TXN_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    False),
    StructField("user_id",           StringType(),    False),
    StructField("card_id",           StringType(),    False),
    StructField("amount",            DoubleType(),    False),  # USD
    StructField("currency",          StringType(),    False),
    StructField("merchant_id",       StringType(),    False),
    StructField("merchant_category", StringType(),    True),
    StructField("country",           StringType(),    False),  # nơi quẹt thẻ
    StructField("city",              StringType(),    True),
    StructField("device_id",         StringType(),    True),
    StructField("event_time",        TimestampType(), False),
])
```

Parse từ Kafka (nhớ: Kafka trả `value` là binary):

```python
from pyspark.sql import functions as F

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "transactions")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 50_000)   # chống ngộp khi resume sau downtime
       .load())

txn = (raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"),
                  F.col("timestamp").alias("kafka_ts"))
          .select("t.*", "kafka_ts")
          .withWatermark("event_time", "10 minutes"))
```

**Tiêu chí hoàn thành:** (a) file `schema.py` + một trang markdown mô tả từng field, nghĩa, nullable, ví dụ; (b) chứng minh event rác (JSON hỏng) không giết job — `from_json` trả null, bạn tách dòng null vào nhánh dead-letter (đếm được); (c) trả lời: tại sao giữ cả `event_time` lẫn `kafka_ts`? (đo latency ingest, và debug late data).

### Checkpoint 2 — Rule-based scoring

**Mô tả:** Chấm điểm bằng luật. Bắt đầu với 2 luật của ROADMAP, cộng thêm ít nhất 1 luật tự nghĩ.

**Kỹ thuật (lesson 7):** thuần `when/otherwise` — KHÔNG viết Python UDF cho việc này (lesson 12: UDF mất Catalyst, tốn serialization; interviewer đọc code thấy UDF ở chỗ built-in làm được là trừ điểm ngay). Viết mỗi luật thành **một cột flag riêng** rồi cộng lại — audit cần biết *vì sao* điểm cao, không chỉ điểm.

```python
def apply_rules(df):
    return (df
        .withColumn("r_amount",  F.when(F.col("amount") > 1000, 40).otherwise(0))
        .withColumn("r_country", F.when(F.col("country") != F.col("home_country"), 30)
                                  .otherwise(0))
        # luật tự thêm — ví dụ: giao dịch đêm khuya theo giờ local của user
        .withColumn("r_night",   F.when(F.hour("event_time").between(1, 4), 10)
                                  .otherwise(0)))
# risk_score cộng dồn ở checkpoint 4 (thêm phần stateful) rồi mới gán risk_level
```

**Tiêu chí hoàn thành:** (a) ≥3 luật, mỗi luật một cột `r_*`, có bảng mô tả luật + trọng số; (b) không UDF; (c) unit test cho `apply_rules` bằng pytest + DataFrame nhỏ tạo tay (lesson 41 — hàm nhận df trả df thì test được không cần Kafka).

### Checkpoint 3 — Stream-static join: user profile, reload daily

**Mô tả:** Enrich mỗi giao dịch với profile user (`home_country`, `avg_amount_30d`, `risk_tier`) từ bảng Iceberg `lake.fraud.user_profiles`, được batch job cập nhật mỗi 1AM. Yêu cầu chết người: **streaming job không restart nhưng vẫn phải thấy profile mới**.

**Kỹ thuật (lesson 28 Hard):** stream-static join trong plan tĩnh sẽ **đóng băng** snapshot của bảng static tại thời điểm plan — profile hôm qua dùng mãi. Pattern chuẩn: đưa join vào **foreachBatch** và tự quản lý cache có hạn dùng:

```python
# labs/project4/profile_cache.py
import time

class ProfileCache:
    """Re-read user_profiles từ Iceberg khi cache quá hạn (TTL giây)."""
    def __init__(self, spark, table, ttl_sec=3600):
        self.spark, self.table, self.ttl = spark, table, ttl_sec
        self.df, self.loaded_at = None, 0.0

    def get(self):
        if self.df is None or time.time() - self.loaded_at > self.ttl:
            if self.df is not None:
                self.df.unpersist()
            self.df = self.spark.read.table(self.table).cache()
            self.df.count()                    # materialize cache ngay
            self.loaded_at = time.time()
        return self.df

profiles = ProfileCache(spark, "lake.fraud.user_profiles", ttl_sec=3600)

def process_batch(batch_df, batch_id):
    enriched = batch_df.join(F.broadcast(profiles.get()), "user_id", "left")
    ...
```

Điểm cần nhớ: (a) `broadcast` vì profile nhỏ (nghìn–triệu user) so với stream — tránh shuffle mỗi micro-batch (lesson 9); (b) join **left** — user mới chưa có profile không được làm rớt giao dịch; cột null thì `coalesce` về default và cộng thêm điểm rủi ro "user lạ"; (c) TTL 1h là trade-off: profile mới trễ tối đa 1h so với 1AM — chấp nhận được với nghiệp vụ này, và rẻ hơn nhiều so với re-read mỗi batch.

**Tiêu chí hoàn thành:** (a) tạo và seed bảng `user_profiles` (script batch từ dữ liệu generator, coi như output của pipeline batch 1AM); (b) demo: đổi `home_country` của 1 user trong Iceberg → không restart stream → sau khi cache hết hạn, scoring đổi theo (ghi log thời điểm reload); (c) giải thích được vì sao không dùng stream-static join "thẳng" trong plan.

### Checkpoint 4 — Stateful: lịch sử 10 txn / cửa sổ 24h per user

**Mô tả:** Luật mạnh nhất của fraud detection là **hành vi bất thường so với chính user đó**: 5 giao dịch trong 10 phút (velocity), amount gấp 5 lần trung bình 10 giao dịch gần nhất, 2 quốc gia khác nhau trong 1 giờ (impossible travel). Cần nhớ per-user: 10 txn gần nhất trong 24h.

**Kỹ thuật (lesson 26):** `applyInPandasWithState` (Spark 3.4) — arbitrary stateful processing với state schema tự định nghĩa. Đây là API khó nhất khóa học, nên skeleton cho gần đủ:

```python
# labs/project4/stateful.py
import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

STATE_SCHEMA  = ("amounts array<double>, ts array<long>, countries array<string>")
OUTPUT_SCHEMA = ("user_id string, transaction_id string, "
                 "txn_cnt_24h int, amount_avg_hist double, "
                 "r_velocity int, r_travel int, r_amount_anomaly int")

TTL_MS = 30 * 24 * 3600 * 1000        # checkpoint 6: 30 ngày

def track_history(key, pdf_iter, state: GroupState):
    if state.hasTimedOut:              # không thấy user 30 ngày → dọn state
        state.remove()
        return                          # generator rỗng — không emit gì
    user_id = key[0]
    amounts, ts, countries = (list(state.get) if state.exists else ([], [], []))
    rows = []
    for pdf in pdf_iter:
        pdf = pdf.sort_values("event_time")
        for r in pdf.itertuples():
            epoch = int(r.event_time.timestamp() * 1000)
            # (1) cắt cửa sổ 24h, (2) giữ tối đa 10 phần tử
            keep = [i for i, t in enumerate(ts) if epoch - t <= 24*3600*1000]
            amounts  = [amounts[i]   for i in keep][-9:]
            ts       = [ts[i]        for i in keep][-9:]
            countries= [countries[i] for i in keep][-9:]
            # === TỰ VIẾT: 3 luật dựa trên amounts/ts/countries ===
            r_velocity = ...       # ≥5 txn trong 10 phút gần nhất?
            r_travel   = ...       # country khác country trước đó < 1h?
            r_anomaly  = ...       # amount > 5 × mean(amounts)?
            amounts.append(r.amount); ts.append(epoch); countries.append(r.country)
            rows.append((user_id, r.transaction_id, len(ts),
                         float(pd.Series(amounts).mean()),
                         r_velocity, r_travel, r_anomaly))
    state.update((amounts, ts, countries))
    state.setTimeoutDuration(TTL_MS)
    yield pd.DataFrame(rows, columns=[c.split()[0] for c in OUTPUT_SCHEMA.split(", ")])

history = (txn.groupBy("user_id")
              .applyInPandasWithState(track_history, OUTPUT_SCHEMA, STATE_SCHEMA,
                                      "append", GroupStateTimeout.ProcessingTimeTimeout))
```

Điểm cần nhớ: (a) state store mặc định HDFS-backed in-memory — bật **RocksDB** (`spark.sql.streaming.stateStore.providerClass = ...RocksDBStateStoreProvider`) vì hàng trăm nghìn user × state là thứ làm executor OOM (lesson 26 Hard); (b) hàm này chạy trong **Python worker trên executor** — mọi bài học về serialization của lesson 12 áp dụng; (c) output của nhánh stateful phải **join ngược lại** với nhánh chính theo `transaction_id` trong foreachBatch, hoặc gọn hơn: cho toàn bộ cột đi xuyên qua stateful function (đưa hết vào OUTPUT_SCHEMA) — chọn và giải thích.

**Tiêu chí hoàn thành:** (a) 3 luật stateful chạy đúng với fraud pattern do generator bơm (mục 5 — pattern `velocity` phải làm `r_velocity` bật); (b) restart job → state sống lại từ checkpoint (đếm txn_cnt_24h không reset); (c) xem state size trong Spark UI tab Structured Streaming / metrics `stateOperators` và ghi nhận con số.

### Checkpoint 5 — Ba output: alert Kafka + audit Iceberg + metrics Prometheus

**Mô tả:** Fan-out kết quả trong `foreachBatch`. Tổng điểm → `risk_score`; `risk_score ≥ 70` → HIGH.

**Kỹ thuật (lesson 27):**

```python
from prometheus_client import Counter, Gauge, start_http_server
ALERTS  = Counter("fraud_alerts_total", "Số alert đã phát")
AVGRISK = Gauge("fraud_avg_risk_score", "Risk score trung bình batch gần nhất")

def process_batch(batch_df, batch_id):
    scored = enrich_and_score(batch_df)          # checkpoint 2+3+4 gộp lại
    scored.persist()                             # dùng 3 lần — không tính lại 3 lần!
    try:
        # ① audit: TẤT CẢ → Iceberg (append-only)
        (scored.withColumn("batch_id", F.lit(batch_id))
               .writeTo("lake.fraud.txn_audit").append())
        # ② alert: HIGH → Kafka
        (scored.filter("risk_level = 'HIGH'")
               .select(F.col("user_id").alias("key"),
                       F.to_json(F.struct("*")).alias("value"))
               .write.format("kafka")
               .option("kafka.bootstrap.servers", "kafka:9092")
               .option("topic", "fraud_alerts").save())
        # ③ metrics
        n_alert = scored.filter("risk_level = 'HIGH'").count()
        ALERTS.inc(n_alert)
        avg = scored.agg(F.avg("risk_score")).first()[0]
        if avg is not None: AVGRISK.set(avg)
    finally:
        scored.unpersist()

query = (history_joined.writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", "/workspace/labs/project4/_chk/main")
         .trigger(processingTime="5 seconds")
         .start())
start_http_server(9099)   # Prometheus scrape driver:9099
```

Điểm cần nhớ — thứ tự sink là quyết định có chủ đích: **audit trước, alert sau**. Nếu job chết giữa foreachBatch, batch replay lại toàn bộ. Iceberg append replay → duplicate? Có thể — nên audit mang cột `batch_id`: consumer/compaction dedup theo `(transaction_id, batch_id)` hoặc bạn dùng MERGE theo `transaction_id` (đắt hơn — nêu trade-off). Kafka sink là **at-least-once**: hệ chặn giao dịch nhận alert đúp phải tự idempotent theo `transaction_id` — ghi rõ điều đó vào contract của topic. Đây chính là câu trả lời cho lesson 27 Hard: *multi-sink không có distributed transaction; thiết kế để duplicate vô hại thay vì cố làm điều bất khả.*

**Tiêu chí hoàn thành:** (a) 3 sink cùng chạy; `kafka-console-consumer` thấy alert, Trino query được audit, `curl driver:9099/metrics` thấy 2 metric; (b) giải thích tại sao `persist()` trong foreachBatch (không có nó: 3 action = 3 lần tính lại cả enrich + stateful); (c) văn bản 5–10 dòng về semantics từng sink (exactly-once? at-least-once? vì sao?).

### Checkpoint 6 — State TTL 30 ngày

**Mô tả:** User không giao dịch 30 ngày → state bị dọn. Không có TTL, state phình vô hạn = quả bom OOM nổ chậm sau vài tháng chạy production (lesson 26 Hard).

**Kỹ thuật:** đã cài ở checkpoint 4 (`setTimeoutDuration` + `hasTimedOut` + `remove()`). Checkpoint này là **chứng minh nó chạy**: hạ TTL xuống 2 phút trong môi trường test, bơm 1 user rồi ngừng, quan sát metric `stateOperators[0].numRowsRemoved` và `numRowsTotal` giảm ở batch sau timeout.

Điểm cần nhớ: `ProcessingTimeTimeout` chỉ được kiểm tra **khi có micro-batch chạy** — stream không có data mới thì timeout không nổ (vì vậy production hay giữ trigger đều đặn thay vì `availableNow`). Phân biệt được TTL này với watermark (watermark dọn state của *windowed aggregation/dedup*, TTL này dọn state của *arbitrary stateful*) là điểm cộng.

**Tiêu chí hoàn thành:** log/screenshot chứng minh state row bị remove sau TTL + đoạn giải thích ProcessingTime vs EventTime timeout, tại sao ở đây chọn ProcessingTime (yêu cầu là "không thấy user 30 ngày" theo giờ hệ thống, không theo event time).

### Checkpoint 7 — Monitoring: lag, error rate, SLA

**Mô tả:** Job không có monitoring = job chưa xong. Ba câu hệ thống phải tự trả lời: *đang tụt hậu bao nhiêu?* (lag), *có đang lỗi không?* (error rate), *có đạt SLA không?* (latency).

**Kỹ thuật (lesson 39 + Project 2 checkpoint 6):** PySpark 3.4 có `StreamingQueryListener`:

```python
from pyspark.sql.streaming import StreamingQueryListener
import json

LAG      = Gauge("fraud_kafka_lag_rows", "Ước lượng backlog")
BATCHDUR = Gauge("fraud_batch_duration_ms", "Thời gian xử lý batch")
INPUTRPS = Gauge("fraud_input_rows_per_sec", "Tốc độ vào")

class Metrics(StreamingQueryListener):
    def onQueryStarted(self, e): pass
    def onQueryTerminated(self, e): pass   # production: bắn alert PagerDuty ở đây
    def onQueryIdle(self, e): pass
    def onQueryProgress(self, e):
        p = json.loads(e.progress.json)
        BATCHDUR.set(p["durationMs"]["triggerExecution"])
        if p["inputRowsPerSecond"]: INPUTRPS.set(p["inputRowsPerSecond"])
        # lag: processedRowsPerSecond < inputRowsPerSecond kéo dài = đang tụt

spark.streams.addListener(Metrics())
```

Định nghĩa SLA đo được: `batch_duration < trigger_interval` (nếu batch 5s mà xử lý mất 8s → tụt dần, không bao giờ đuổi kịp). Alert rule (viết dạng PromQL trong doc, không cần dựng Alertmanager thật): lag tăng đơn điệu 5 phút; `fraud_alerts_total` đứng im 10 phút khi input vẫn chảy (alert pipeline chết *im lặng* — loại lỗi nguy hiểm nhất); batch duration > 2× baseline.

**Tiêu chí hoàn thành:** (a) ≥5 metrics expose ra Prometheus endpoint; (b) bảng "metric → ngưỡng → hành động của on-call" (đây là runbook thu nhỏ — lesson 40); (c) demo một sự cố: tắt Kafka consumer... à không — *giảm* tài nguyên hoặc tăng input ×10, chỉ ra metric nào bắt được trước tiên.

### Checkpoint 8 — Airflow + Kubernetes deployment với HPA

**Mô tả:** Đóng gói vận hành. Checkpoint này chấp nhận **mức design + manifest** (hạ tầng K8s thật không bắt buộc trong lab), nhưng manifest phải viết như thật.

**Kỹ thuật (lesson 36, 37, 38, 42):**

- **Airflow** không "chạy" streaming job như batch — nó đóng 3 vai: (1) DAG `profile_refresh` 1AM build lại `user_profiles`; (2) DAG `iceberg_maintenance` daily: compaction + `expire_snapshots` cho `txn_audit` (streaming ghi 5s/lần = nhà máy small files — lesson 21/32); (3) DAG `stream_babysitter`: sensor kiểm tra query còn sống (qua metrics), chết thì `spark-submit` lại — hoặc để K8s restart policy lo và Airflow chỉ alert. Chọn và ghi lý do.
- **K8s:** spark-submit cluster mode, driver pod + executor pods. Sizing khởi điểm cho 1k events/sec: 2 executor × 2 core × 4GB (+ overhead ~10%), driver 2GB — kèm phép tính của bạn theo lesson 38, không copy số này.
- **HPA — sự thật senior:** HPA scale *Deployment*, còn executor của Spark không phải Deployment. Scaling đúng chuẩn Spark-on-K8s là **dynamic allocation** (`spark.dynamicAllocation.enabled=true` + `spark.dynamicAllocation.shuffleTracking.enabled=true`), min/max executors — Spark tự xin/trả pod theo backlog. HPA theo nghĩa đen chỉ áp dụng nếu bạn gói toàn job vào 1 Deployment (client mode trong pod) — làm được nhưng thô. Trong doc: trình bày cả hai, chọn dynamic allocation, và ghi chú caveat của streaming (scale theo CPU không nhạy bằng scale theo lag — nêu hướng custom metric).

**Tiêu chí hoàn thành:** (a) 2–3 Airflow DAG chạy được trên Airflow của repo kafka-flink (hoặc tối thiểu parse được + unit test cấu trúc DAG); (b) file YAML/spark-submit args cho K8s + bảng sizing có tính toán; (c) đoạn văn "chuyện gì xảy ra khi node chứa driver chết?" (gợi ý: cluster mode + restart policy + checkpoint → sống lại đúng chỗ cũ).

---

## 5. Transaction generator (chạy được ngay)

Lưu vào `labs/project4/txn_generator.py`. Cần `pip install kafka-python` trong venv của repo.

```python
#!/usr/bin/env python3
"""Transaction generator cho Project 4 — Real-time Fraud Detection.

Bơm giao dịch giả lập vào Kafka topic `transactions`, trộn fraud pattern
có chủ đích để test scoring:
  - big_amount : amount 1500–9000 USD
  - foreign    : country khác home_country của user
  - velocity   : bắn 6 giao dịch liên tiếp trong vài giây

Dùng:
  python txn_generator.py --rate 1000 --fraud-rate 0.02
  python txn_generator.py --rate 50 --label        # thêm ground-truth để debug
"""
import argparse, json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

COUNTRIES = ["VN", "VN", "VN", "VN", "SG", "TH", "US", "JP", "KR", "GB"]
CATEGORIES = ["grocery", "electronics", "fashion", "travel", "food",
              "gaming", "utilities", "health"]
FRAUD_PATTERNS = ["big_amount", "foreign", "velocity"]


def build_users(n):
    users = []
    for i in range(n):
        users.append({
            "user_id": f"u{i:05d}",
            "card_id": f"c{i:05d}",
            "home_country": random.choice(COUNTRIES),
            "avg_amount": round(random.lognormvariate(3.5, 0.8), 2),  # ~$33 median
            "device_id": f"d{uuid.uuid4().hex[:8]}",
        })
    return users


def make_txn(user, amount=None, country=None, label=None, with_label=False):
    txn = {
        "transaction_id": uuid.uuid4().hex,
        "user_id": user["user_id"],
        "card_id": user["card_id"],
        "amount": round(amount if amount is not None
                        else max(0.5, random.gauss(user["avg_amount"],
                                                   user["avg_amount"] * 0.5)), 2),
        "currency": "USD",
        "merchant_id": f"m{random.randint(0, 4999):04d}",
        "merchant_category": random.choice(CATEGORIES),
        "country": country or user["home_country"],
        "city": None,
        "device_id": user["device_id"],
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    if with_label:                      # CHỈ để debug/test — không dùng khi chấm
        txn["_injected"] = label or "normal"
    return txn


def gen_fraud(user, with_label):
    pattern = random.choice(FRAUD_PATTERNS)
    if pattern == "big_amount":
        return [make_txn(user, amount=round(random.uniform(1500, 9000), 2),
                         label=pattern, with_label=with_label)]
    if pattern == "foreign":
        far = random.choice([c for c in COUNTRIES if c != user["home_country"]])
        return [make_txn(user, country=far, label=pattern, with_label=with_label)]
    # velocity: 6 giao dịch dồn dập cùng user
    return [make_txn(user, label=pattern, with_label=with_label) for _ in range(6)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--rate", type=int, default=100, help="events/sec mục tiêu")
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--fraud-rate", type=float, default=0.02)
    ap.add_argument("--duration", type=int, default=0, help="giây; 0 = chạy mãi")
    ap.add_argument("--label", action="store_true",
                    help="gắn field _injected (ground truth) để debug")
    args = ap.parse_args()

    users = build_users(args.users)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),          # key = user_id
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20, acks="all",
    )
    print(f"Bơm ~{args.rate} events/sec vào `{args.topic}` "
          f"(fraud_rate={args.fraud_rate}, users={args.users})")
    sent, t0 = 0, time.time()
    try:
        while True:
            tick = time.time()
            for _ in range(args.rate):
                user = random.choice(users)
                batch = (gen_fraud(user, args.label)
                         if random.random() < args.fraud_rate
                         else [make_txn(user, with_label=args.label)])
                for txn in batch:
                    producer.send(args.topic, key=txn["user_id"], value=txn)
                    sent += 1
            producer.flush()
            if sent and sent % (args.rate * 10) < args.rate:
                print(f"  sent={sent:,}  elapsed={time.time()-t0:,.0f}s")
            if args.duration and time.time() - t0 >= args.duration:
                break
            time.sleep(max(0.0, 1.0 - (time.time() - tick)))   # pace 1 giây/vòng
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(); producer.close()
        print(f"Xong. Tổng {sent:,} events trong {time.time()-t0:,.0f}s.")


if __name__ == "__main__":
    main()
```

Chạy job Spark (nhớ 2 package quen thuộc — Kafka connector và Iceberg runtime):

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
  /workspace/labs/project4/main.py
# (config catalog Iceberg: dùng lại đúng bộ --conf spark.sql.catalog.* của Project 2 / module 5)
```

---

## 6. Test plan

Viết thành `labs/project4/TEST_PLAN.md`, chạy và ghi kết quả từng mục. Đây là phần rubric chấm nặng tay — production engineer được trả lương để *chứng minh* hệ thống đúng, không phải để *tin* nó đúng.

| # | Test | Cách làm | Đạt khi |
|---|---|---|---|
| T1 | **Kill giữa chừng → exactly-once audit?** | Bơm 10k events có `--label`, chờ chạy nửa chừng, `docker kill` container driver, start lại job (cùng checkpointLocation). | Đếm trong Iceberg: `COUNT(DISTINCT transaction_id) = 10.000` và ghi nhận có/không duplicate theo `(transaction_id)` — nếu có, cơ chế dedup của bạn (batch_id/MERGE) phải khử được khi đọc. Giải thích kết quả theo lesson 27. |
| T2 | **Fraud pattern → alert?** | Bơm với `--fraud-rate 0.05 --label`. Join topic `fraud_alerts` với audit theo `transaction_id`. | Recall theo từng pattern: `big_amount` ≥ 99%, `foreign` ≥ 99%, `velocity` ≥ 90% (giải thích vì sao velocity khó đạt 100% — event đầu của burst chưa có history). |
| T3 | **State sống qua restart** | User X bơm 8 txn → restart → bơm thêm 3 txn. | `txn_cnt_24h` của txn cuối = 10 (bị cắt trần), không phải 3. |
| T4 | **Profile reload không cần restart** | Đổi `home_country` user Y trong Iceberg, chờ quá TTL cache. | Txn mới của Y được chấm theo profile mới; log ghi thời điểm reload. |
| T5 | **TTL dọn state** | TTL test = 2 phút; bơm rồi ngừng user Z. | `numRowsRemoved` > 0 ở batch sau timeout. |
| T6 | **Chịu tải & lag** | Tăng `--rate` từ 100 → 1000 → (thử) 5000. | Ghi bảng: rate vs batch duration vs lag. Xác định điểm gãy của cluster lab và đề xuất sizing để chịu 10k/sec (phép tính, không cần chạy). |
| T7 | **Poison message** | Bơm tay 1 message JSON hỏng vào topic (`kafka-console-producer`). | Job không chết; counter dead-letter tăng 1. |

---

## 7. Deliverable

Nộp một thư mục `labs/project4/` gồm:

1. `DESIGN.md` — design doc ≤4 trang: kiến trúc (diagram), schema, luật scoring + trọng số, semantics từng sink, sizing, vận hành (runbook thu nhỏ). Viết cho người đọc là tech lead chưa xem code.
2. Code: `schema.py`, `rules.py`, `stateful.py`, `profile_cache.py`, `main.py`, `txn_generator.py`, `tests/`.
3. `TEST_PLAN.md` — kết quả 7 test kèm số liệu/screenshot Spark UI.
4. `airflow/` + `k8s/` — DAGs và manifests của checkpoint 8.
5. Log 1 phiên chạy ≥30 phút ở 1000 events/sec.

## Rubric (chấm theo chuẩn Senior — tổng 100)

| Hạng mục | Điểm | Đạt tối đa khi |
|---|---|---|
| Đúng đắn (T1–T5 pass) | 30 | Exactly-once có chứng minh, state đúng qua restart, không chết vì poison message |
| Thiết kế & trade-off | 20 | Mỗi quyết định (1 query vs nhiều, thứ tự sink, TTL cache profile, ProcessingTime timeout) có lý do và có nêu cái giá phải trả |
| Chất lượng code | 15 | Hàm thuần nhận df trả df, có unit test, không UDF vô cớ, config tách khỏi code |
| Vận hành | 15 | Metrics + ngưỡng + runbook; trả lời được "3h sáng lag tăng, làm gì trong 5 phút đầu?" |
| Hiệu năng | 10 | Bảng đo T6, biết điểm gãy ở đâu và vì sao (source partitions? state? sink?) |
| Deployment | 10 | Airflow DAG hợp lý, hiểu đúng dynamic allocation vs HPA, sizing có phép tính |

**Thang:** ≥85 = pass chuẩn Senior · 70–84 = pass, còn nợ vận hành · <70 = làm lại checkpoint hổng.

## Câu hỏi bảo vệ project (vấn đáp — chuẩn bị trước)

1. Tại sao chọn Spark chứ không Flink cho bài này? SLA nào thì bạn đổi câu trả lời?
2. Chứng minh cho tôi audit log là exactly-once. Sink Kafka thì sao? Nếu tôi yêu cầu alert cũng exactly-once, bạn làm gì và trả giá gì?
3. Nếu 1 user bị kẻ gian bơm 1 triệu giao dịch/giờ (state skew về 1 key), job của bạn chết ở đâu trước? Đỡ thế nào?
4. Watermark 10 phút của bạn dùng để làm gì trong pipeline này — và có ảnh hưởng gì đến state TTL không?
5. Muốn thêm luật ML model (XGBoost) vào scoring — sửa kiến trúc chỗ nào, latency budget thay đổi ra sao?
6. Checkpoint directory bị xóa mất. Chuyện gì xảy ra khi restart? Khôi phục thế nào để không double-alert?
7. Team Risk muốn đổi trọng số luật mà không restart job. Thiết kế lại chỗ nào? (gợi ý: luật cũng là data — bảng config, cùng pattern với profile cache)
8. Audit table sau 6 tháng có 3 triệu file nhỏ. Ai gây ra, và bạn đã phòng từ đầu bằng gì?

---

## 8. Next

Bạn vừa xây thứ mà rất nhiều JD "Senior Data Engineer" mô tả nguyên văn. Phần cuối cùng của khóa — **`interview-prep.md`** — sẽ nén 24 tuần thành: mindmap 6 module, 50 câu interview có đáp án, 3 bài system design (một trong số đó bạn vừa... làm xong), và checklist tự đánh giá trước khi bước vào phòng phỏng vấn. Project 4 chính là câu chuyện STAR đắt giá nhất của bạn — mang nó theo.

> Chúc mừng bạn đi đến cuối khóa. Gõ **"Continue"** nếu muốn ôn tập bất kỳ phần nào.
