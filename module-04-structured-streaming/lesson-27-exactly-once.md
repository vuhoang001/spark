# Lesson 27 — Exactly-once semantics: idempotent sink, foreachBatch

> Module 4 · Structured Streaming · Tuần 14 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Ba mức delivery semantics: **at-most-once, at-least-once, exactly-once** — và tại sao exactly-once là bài toán khó nhất của streaming.
- Công thức Spark đạt exactly-once: **replayable source + checkpoint (offset tracking) + idempotent/transactional sink**. Thiếu 1 trong 3 là sập.
- Tại sao console sink và Kafka sink của Spark chỉ đạt **at-least-once** — hiểu điều này là hiểu bản chất.
- Giải phẫu **`foreachBatch`**: `batch_id` là gì, vì sao nó là chìa khóa để tự xây sink idempotent.
- Ghi Iceberg/Delta từ stream: vì sao **atomic commit** của table format biến at-least-once thành exactly-once.
- Bẫy **multi-sink**: 2 sink trong 1 foreachBatch KHÔNG atomic với nhau — và 2 cách thoát.

Sau bài này bạn phải làm được:

- Kill một streaming job giữa chừng, restart, và **chứng minh bằng số liệu** rằng bảng đích không có duplicate.
- Nhìn bất kỳ pipeline streaming nào và chỉ ra: mắt xích nào phá vỡ exactly-once.
- Viết foreachBatch ghi Iceberg đúng chuẩn production.

Kiến thức dùng trong thực tế: đây là bài **ăn tiền** nhất module 4. Pipeline tính doanh thu mà đếm trùng 1 giao dịch = báo cáo sai = mất niềm tin cả team data. Interviewer senior gần như chắc chắn hỏi "exactly-once của Spark hoạt động thế nào?" — và 80% ứng viên trả lời sai vì tưởng checkpoint một mình là đủ.

---

## 2. Why

### Câu chuyện: 1 message, 2 lần tiền

Pipeline của bạn: Kafka topic `payments` → Spark Structured Streaming → bảng doanh thu. Nửa đêm, executor OOM, job chết **sau khi đã ghi output nhưng trước khi kịp ghi nhận "tôi đã xử lý đến offset 5000"**. Job restart, đọc lại từ offset đã ghi nhận lần cuối (4000), xử lý lại 1000 message... và **ghi lần thứ hai**. Sáng hôm sau doanh thu đội lên gấp rưỡi, CEO hỏi, và người phải giải thích là bạn.

Vấn đề gốc: trong hệ phân tán, **failure có thể xảy ra ở BẤT KỲ điểm nào giữa "xử lý" và "ghi nhận đã xử lý"**. Không tồn tại cách nào để hai hành động trên hai hệ thống khác nhau (ghi output vào DB, ghi offset vào checkpoint) xảy ra "cùng một khoảnh khắc" — trừ khi bạn thiết kế có chủ đích.

### Nếu không giải quyết thì sao?

- Chấp nhận **mất dữ liệu** (at-most-once): với log metrics thì tạm được, với tiền thì không bao giờ.
- Chấp nhận **trùng dữ liệu** (at-least-once): mọi consumer downstream phải tự dedup — đẩy độ khó cho cả chục team khác.
- Hoặc hiểu đúng và xây exactly-once **end-to-end** — nội dung hôm nay.

### Trade-off (Senior phải thuộc)

| Semantics | Được | Mất |
|---|---|---|
| At-most-once | Nhanh nhất, không state | Mất data khi fail |
| At-least-once | Không mất data, đơn giản | Duplicate khi fail |
| Exactly-once | Không mất, không trùng | Sink phải idempotent/transactional; latency cao hơn (commit theo batch); thiết kế phức tạp hơn |

> Bài học Senior: exactly-once **không phải là một config bật lên là có**. Nó là một **thuộc tính end-to-end** của cả 3 mắt xích source–engine–sink. Ai nói "tôi bật exactly-once trong Spark rồi" mà không nói sink là gì → chưa hiểu.

---

## 3. Theory

### 3.1. Ba mức delivery semantics

| | At-most-once | At-least-once | Exactly-once |
|---|---|---|---|
| Mỗi record được xử lý | 0 hoặc 1 lần | ≥ 1 lần | đúng 1 lần (về **kết quả**) |
| Khi fail | mất record | trùng record | không mất, không trùng |
| Cơ chế | fire & forget, commit offset TRƯỚC khi xử lý | retry + commit offset SAU khi xử lý | at-least-once + sink khử trùng |
| Ví dụ chấp nhận được | metrics giám sát, sampling log | đếm view (sai số nhỏ OK) | tiền, tồn kho, CDC |

Chú ý chữ **"về kết quả"**: exactly-once thực tế là *effectively-once* — record có thể được **xử lý lại** nhiều lần khi retry, nhưng **hiệu ứng lên đích chỉ tính 1 lần**. Không hệ thống nào đảm bảo "chạy code đúng 1 lần" trong môi trường có failure; chỉ đảm bảo được "kết quả như thể chạy 1 lần".

### 3.2. Diagram: failure rơi vào đâu thì chuyện gì xảy ra

```
Vòng đời 1 micro-batch:  ① đọc source → ② xử lý → ③ ghi sink → ④ ghi nhận offset

Kịch bản A — commit offset TRƯỚC khi ghi sink (at-most-once):
  ① đọc offset 4000→5000
  ④ ghi nhận "đã xong 5000"   ✔
  ③ ghi sink...  💥 CRASH
  → restart đọc từ 5000 → 1000 record ①-④ KHÔNG BAO GIỜ ra sink → MẤT DATA

Kịch bản B — commit offset SAU khi ghi sink (at-least-once):
  ① đọc offset 4000→5000
  ③ ghi sink                  ✔  (data đã nằm ở đích)
  ④ ghi nhận offset...  💥 CRASH
  → restart đọc lại từ 4000 → ghi sink LẦN 2 → DUPLICATE

Kịch bản C — exactly-once:
  giống B, NHƯNG sink có khả năng nhận ra "batch này tôi ghi rồi"
  (idempotent) hoặc ghi-sink-và-ghi-offset nằm trong 1 transaction
  → replay vô hại → KHÔNG MẤT, KHÔNG TRÙNG
```

Khắc cốt ghi tâm: **không có thứ tự ③④ nào tự nó cho exactly-once**. Phải có sự trợ giúp của sink.

### 3.3. Công thức exactly-once của Spark = 3 mảnh ghép

```
┌──────────────────┐   ┌────────────────────┐   ┌─────────────────────┐
│ ① REPLAYABLE      │   │ ② CHECKPOINT        │   │ ③ IDEMPOTENT /       │
│    SOURCE         │ + │    (offset tracking)│ + │    TRANSACTIONAL SINK│
│                  │   │                    │   │                     │
│ Kafka: đọc lại   │   │ WAL ghi "batch N =  │   │ Iceberg/Delta: commit│
│ được theo offset │   │ offset X→Y" TRƯỚC   │   │ atomic theo snapshot;│
│ bất kỳ lúc nào   │   │ khi xử lý; commits/ │   │ hoặc tự xây bằng     │
│ (retention)      │   │ ghi SAU khi xong    │   │ batch_id + MERGE     │
└──────────────────┘   └────────────────────┘   └─────────────────────┘
```

1. **Replayable source**: khi cần xử lý lại, Spark phải đọc lại được **đúng** đoạn dữ liệu đó. Kafka làm được (seek theo offset, trong thời gian retention). Socket source thì KHÔNG — data trôi qua là mất → socket không bao giờ cho exactly-once, thậm chí không cho at-least-once.
2. **Checkpoint**: Spark ghi vào checkpoint dir hai loại file — `offsets/N` ("batch N định xử lý offset X→Y", ghi **trước** khi chạy batch — đây là Write-Ahead Log) và `commits/N` ("batch N đã xong", ghi **sau**). Nhờ đó restart biết chính xác batch nào dang dở.
3. **Sink khử trùng**: batch dang dở sẽ được **chạy lại với cùng batch_id, cùng dải offset, cùng dữ liệu**. Sink phải làm cho lần ghi thứ 2 vô hại — bằng idempotence (ghi đè theo key/batch_id) hoặc transaction (commit atomic, lần 2 bị từ chối/skip).

### 3.4. Tại sao console sink và Kafka sink chỉ at-least-once?

- **Console sink**: in ra màn hình. In 2 lần thì... có 2 dòng trên màn hình, không ai "xóa dòng in lần 1" được → replay = duplicate → at-least-once (dùng để debug, không ai quan tâm).
- **Kafka sink**: Kafka là **append-only log**. Spark ghi batch N vào topic, crash trước khi ghi `commits/N`, restart ghi lại batch N → topic có 2 bản. Spark (3.4) **không dùng Kafka transactions** cho sink, nên không rollback được → at-least-once. Consumer downstream phải tự dedup (theo key nghiệp vụ) hoặc chấp nhận trùng.
- **File sink** (parquet trên writeStream): lại là exactly-once! Vì Spark duy trì **manifest** (`_spark_metadata/`) ghi nhận "file nào thuộc batch nào" một cách atomic — reader chỉ đọc file có trong manifest, file ghi thừa của batch replay bị lờ đi.

> Analogy: console = nói ra miệng (không rút lại được); Kafka sink = viết vào sổ bằng bút mực không tẩy; Iceberg = viết nháp thoải mái, chỉ khi **ký tên đóng dấu (atomic commit)** thì trang nháp mới thành chính thức — ký trùng batch thì phát hiện được và bỏ.

### 3.5. foreachBatch — cánh cửa tự do

`foreachBatch` cho bạn nhận từng micro-batch dưới dạng **DataFrame batch bình thường** + một số nguyên `batch_id`:

```python
def upsert(df, batch_id: int):
    # df: DataFrame TĨNH chứa data của đúng micro-batch này
    # batch_id: tăng dần 0,1,2,... — RESTART THÌ BATCH REPLAY GIỮ NGUYÊN batch_id
    ...

query = stream_df.writeStream.foreachBatch(upsert) \
        .option("checkpointLocation", "/ckpt/...").start()
```

Hai sự thật làm nên giá trị của `batch_id`:

1. **Cùng checkpoint → batch replay mang cùng batch_id và cùng dữ liệu** (vì dải offset đã chốt trong WAL). Đây là nền tảng để idempotent: "nếu tôi đã ghi batch_id này rồi thì bỏ qua / ghi đè".
2. Trong foreachBatch bạn dùng được **toàn bộ API batch**: MERGE INTO, ghi JDBC, ghi nhiều bảng — những thứ writeStream thuần không làm được.

Ba chiến lược sink với foreachBatch, từ thô đến mịn:

| Chiến lược | Cách làm | Đạt gì |
|---|---|---|
| Transactional theo batch | Ghi `(batch_id, data)` trong 1 transaction; trước khi ghi check "batch_id đã có chưa" | Exactly-once mức batch |
| Idempotent theo key | MERGE INTO / upsert theo primary key — ghi lại lần 2 cho cùng kết quả | Exactly-once mức row (mạnh nhất, hồi phục cả khi mất checkpoint) |
| Append + dedup downstream | Ghi kèm cột `batch_id`, view đích lọc trùng | Yếu, chỉ khi 2 cách trên bất khả thi |

### 3.6. Multi-sink trong foreachBatch — KHÔNG atomic

```python
def two_sinks(df, batch_id):
    df.write...  # ① ghi Iceberg      ✔ thành công
    df.write...  # ② ghi Kafka alert  💥 fail → cả batch fail → RETRY
    # retry: ① chạy LẠI LẦN 2 rồi mới đến ②
```

Hai lệnh ghi là hai hệ thống độc lập — **không có transaction bao trùm cả hai**. Batch retry làm sink ① bị ghi 2 lần. Giải pháp:

1. **Làm cả hai sink idempotent**: ① là MERGE theo key (replay vô hại), ② Kafka có key ổn định để consumer dedup. Đơn giản, đủ dùng 90% trường hợp.
2. **Outbox pattern**: chỉ ghi MỘT nơi atomic (Iceberg — data + bảng `outbox` chứa event cần phát, ghi trong cùng commit). Một job riêng đọc `outbox` phát sang Kafka rồi đánh dấu. Chuyển bài toán "2 sink atomic" thành "1 sink atomic + 1 relay at-least-once có dedup".
3. (Tệ) chấp nhận lệch và chạy job đối soát định kỳ.

Ngoài ra nếu bạn `df.persist()` đầu hàm rồi ghi 2 nơi — nhớ `df.unpersist()` cuối hàm, nếu không memory rò rỉ dần qua hàng nghìn batch.

---

## 4. Internal

Chuyện gì xảy ra trong checkpoint dir qua một vòng đời batch — mổ xẻ để hết mù mờ:

```
checkpoint/
├── metadata          ← id của query (đổi query khác dùng chung ckpt → lỗi)
├── offsets/          ← WRITE-AHEAD LOG
│   ├── 0             ← "batch 0 sẽ xử lý Kafka offset {p0: 0→812, p1: 0→790}"
│   ├── 1
│   └── 2             ← ghi TRƯỚC khi batch 2 chạy
├── commits/
│   ├── 0             ← "batch 0 đã hoàn tất" — ghi SAU khi sink xong
│   └── 1             ← ⚠ chưa có commits/2 → batch 2 dang dở!
├── sources/          ← trạng thái khởi tạo source
└── state/            ← state store (lesson 26) — agg/join/dedup state
```

Trình tự mỗi micro-batch:

```
① Driver hỏi Kafka: latest offset là bao nhiêu? → quyết định dải offset batch N
② Ghi offsets/N (WAL) — "chốt kèo" TRƯỚC khi làm
③ Executor đọc Kafka theo dải đã chốt, xử lý, gọi sink (hoặc foreachBatch)
④ Sink xong → ghi commits/N
⑤ Sang batch N+1
```

Khi restart:

```
Đọc offsets/ và commits/
  → offsets có N, commits có N     → batch N xong, chạy tiếp N+1
  → offsets có N, commits CHƯA có N → RE-RUN batch N: cùng dải offset
                                      (đã chốt trong WAL), cùng batch_id N
```

Đây là lý do batch replay **tất định** (deterministic về input): dữ liệu vào giống hệt. Nhưng lưu ý senior: nếu code của bạn **không tất định** (gọi `current_timestamp()`, `rand()`, gọi API ngoài), output lần 2 có thể khác lần 1 — idempotence theo key vẫn cứu được, idempotence theo "so sánh nội dung" thì không.

**Iceberg commit atomic thế nào?** Mọi file Parquet mới được ghi ra trước (chưa ai thấy), rồi Iceberg tạo snapshot mới và **swap con trỏ metadata bằng 1 thao tác atomic** (compare-and-swap trên catalog). Crash giữa chừng → con trỏ chưa swap → file rác nằm đó nhưng bảng không đổi → reader không bao giờ thấy nửa batch. Với streaming write, Iceberg/Delta còn lưu `(queryId, batchId)` của lần commit gần nhất trong snapshot — batch replay tới, thấy batchId đã commit → **skip**, không ghi lại. Đây chính là mảnh ghép ③ được làm sẵn cho bạn.

---

## 5. API

### `writeStream.foreachBatch(func)`

```python
def upsert_to_iceberg(batch_df, batch_id: int):
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql("""
        MERGE INTO iceberg.demo.accounts t
        USING updates s ON t.account_id = s.account_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (df.writeStream
           .foreachBatch(upsert_to_iceberg)
           .option("checkpointLocation", "/workspace/labs/lab27/ckpt")
           .trigger(processingTime="10 seconds")
           .start())
```

- **Ý nghĩa**: biến mỗi micro-batch thành DataFrame batch, bạn toàn quyền ghi đâu, ghi mấy nơi, chạy MERGE.
- **Khi dùng**: sink không có sẵn connector streaming; cần MERGE/upsert; cần ghi nhiều bảng.
- **Pitfall**: bên trong hàm dùng `batch_df.sparkSession`, đừng tham chiếu biến `spark` global một cách vô thức (chạy được ở client mode, vỡ ở một số môi trường serverless/Connect). Và **đừng quên checkpointLocation** — không có nó, restart là đọc lại từ đầu hoặc mất chỗ.

### `writeStream.format("iceberg")` — sink có sẵn

```python
query = (df.writeStream.format("iceberg")
           .outputMode("append")
           .option("checkpointLocation", "/workspace/labs/lab27/ckpt")
           .toTable("iceberg.demo.events"))
```

- **Ý nghĩa**: append thẳng vào Iceberg, exactly-once nhờ cơ chế skip batchId đã commit.
- **Khi dùng**: chỉ cần append (fact/event table). Cần MERGE → quay về foreachBatch.

### `query.stop()` / `query.awaitTermination()` / `query.lastProgress`

- `awaitTermination()`: block driver chờ stream (không có nó script kết thúc là stream chết).
- `lastProgress`: dict JSON tiến độ batch gần nhất — `numInputRows`, `batchId`, thời gian từng pha. Là nguyên liệu monitoring ở Project 2.
- **Pitfall**: dừng job bằng Ctrl+C/kill là an toàn (nhờ WAL), nhưng **xóa checkpoint dir rồi start lại** = job mới tinh, đọc lại theo `startingOffsets` → nguồn gốc của cả mất lẫn trùng data số 1 trong thực tế.

### Submit với packages (Spark 3.4.1 + Kafka + Iceberg)

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.iceberg:iceberg-aws-bundle:1.4.3 \
  --conf spark.jars.ivy=/workspace/.ivy2 \
  /workspace/labs/lab27/exactly_once_lab.py
```

`spark.jars.ivy=/workspace/.ivy2` để cache jar giữa các lần chạy (volume mount) — không tải lại mỗi lần.

---

## 6. Demo nhỏ

Chứng kiến batch_id replay bằng mắt — không cần Kafka, dùng `rate` source:

```
Input:  rate source (sinh số tăng dần, replayable)
   ↓    foreachBatch in batch_id + ghi log ra file
Output: kill giữa chừng → restart → batch_id dang dở XUẤT HIỆN LẠI
```

```python
# labs/lab27/demo_batch_id.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo27").master("local[2]").getOrCreate()

df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

def show_batch(batch_df, batch_id):
    cnt = batch_df.count()
    lo_hi = batch_df.selectExpr("min(value)", "max(value)").first()
    print(f">>> batch_id={batch_id} rows={cnt} value=[{lo_hi[0]}..{lo_hi[1]}]")

(df.writeStream.foreachBatch(show_batch)
   .option("checkpointLocation", "/tmp/ckpt_demo27")
   .trigger(processingTime="5 seconds")
   .start().awaitTermination())
```

Chạy `make run-local F=labs/lab27/demo_batch_id.py`, chờ đến `batch_id=4` thì **Ctrl+C**. Chạy lại: batch đầu tiên sau restart sẽ in **đúng batch_id dang dở** (4 hoặc 5) chứ không phải 0. Mở `/tmp/ckpt_demo27/offsets/` và `commits/` so số file — thấy ngay batch nào có offset mà thiếu commit.

---

## 7. Production Example

Pipeline thanh toán thực tế (mô hình bạn sẽ gặp ở fintech/e-commerce):

```
App payments ──► Kafka topic "payments" (retention 7 ngày = replayable 7 ngày)
                    │
                    ▼
     Spark Structured Streaming (trigger 30s)
       foreachBatch:
         ① dedup trong batch theo payment_id
         ② MERGE INTO iceberg.finance.payments (key = payment_id)   ← idempotent
         ③ ghi bảng iceberg.finance.payment_metrics (append, kèm batch_id)
                    │
                    ▼
     Iceberg trên S3/MinIO ──► Trino ──► dashboard doanh thu realtime
```

Quyết định thiết kế và lý do:

1. **MERGE theo payment_id** thay vì append: replay batch, hay thậm chí producer gửi trùng (Kafka producer mặc định cũng chỉ at-least-once!) đều vô hại. Idempotent theo key nghiệp vụ chống trùng ở **mọi tầng**, không chỉ tầng Spark.
2. **Checkpoint đặt trên object storage** (production) chứ không phải disk container — container chết là mất checkpoint = thảm họa.
3. **Retention Kafka ≥ thời gian sửa sự cố tối đa**: nếu job chết 3 ngày cuối tuần mà retention 1 ngày → offset trong checkpoint trỏ vào data đã bị xóa → mất data dù kiến trúc "đúng".
4. Alert khi **lag tăng** (Project 2 sẽ xây) — exactly-once không cứu được việc data đến trễ.

---

## 8. Hands-on Lab

**Mục tiêu**: stream Kafka → Iceberg qua foreachBatch + MERGE, kill job giữa chừng, restart, chứng minh không duplicate.

### Bước 0 — nối 2 cụm Docker

Hạ tầng Kafka/Iceberg dùng của repo `../kafka-flink` (đã có sẵn broker, MinIO, iceberg-rest):

```bash
cd ../kafka-flink && docker compose up -d broker minio minio-init iceberg-rest
cd ../spark-mastery && make up
# nối container spark vào network của kafka-flink (kiểm tra tên network: docker network ls)
docker network connect kafka-flink_confluent spark-mastery-spark-submit-1
docker network connect kafka-flink_confluent spark-mastery-spark-master-1
docker network connect kafka-flink_confluent spark-mastery-spark-worker-1
```

Từ trong container Spark, Kafka là `broker:29092`, Iceberg REST là `http://iceberg-rest:8181`, MinIO là `http://minio:9000`.

### Bước 1 — tạo topic + bơm data có chủ đích trùng

```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 \
  --create --topic lab27-payments --partitions 3 --replication-factor 1

# bơm 500 payment, trong đó cố tình lặp lại 50 payment_id (producer at-least-once giả lập)
docker exec broker bash -c 'for i in $(seq 1 500); do
  id=$((i % 450));  # 50 id cuối trùng với 50 id đầu
  echo "{\"payment_id\": \"P$id\", \"amount\": $((RANDOM % 900 + 100)), \"ts\": \"2026-07-08T10:00:00Z\"}";
done | kafka-console-producer --bootstrap-server broker:29092 --topic lab27-payments'
```

### Bước 2 — viết `labs/lab27/exactly_once_lab.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (SparkSession.builder.appName("lab27-exactly-once")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .getOrCreate())

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.lab27")
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.lab27.payments (
        payment_id string, amount double, ts string, batch_id long
    ) USING iceberg
""")

schema = StructType([
    StructField("payment_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("ts", StringType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "broker:29092")
       .option("subscribe", "lab27-payments")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 50)   # ép nhiều batch để dễ kill giữa chừng
       .load())

payments = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("p"))
               .select("p.*"))

def merge_batch(batch_df, batch_id):
    # ① dedup NỘI BỘ batch (MERGE cấm 2 source row khớp cùng 1 target row)
    deduped = batch_df.dropDuplicates(["payment_id"]) \
                      .withColumn("batch_id", F.lit(batch_id))
    deduped.createOrReplaceTempView("upd")
    # ② MERGE = idempotent theo key: replay bao nhiêu lần kết quả vẫn vậy
    deduped.sparkSession.sql("""
        MERGE INTO iceberg.lab27.payments t
        USING upd s ON t.payment_id = s.payment_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f">>> batch {batch_id}: merged {deduped.count()} rows")
    time.sleep(3)   # cố tình chậm để bạn kịp kill giữa batch

(payments.writeStream.foreachBatch(merge_batch)
    .option("checkpointLocation", "/workspace/labs/lab27/ckpt")
    .trigger(processingTime="5 seconds")
    .start().awaitTermination())
```

### Bước 3 — chạy, kill, restart

```bash
# chạy (dùng lệnh spark-submit đầy đủ ở Section 5 vì cần --packages)
# ... chờ in ">>> batch 3: ..." thì Ctrl+C (hoặc docker kill tiến trình)
# chạy lại CÙNG LỆNH — quan sát batch_id đầu tiên sau restart
```

### Bước 4 — chứng minh không duplicate

```python
# labs/lab27/verify.py — chạy bằng make run-local (kèm --packages Iceberg)
total    = spark.sql("SELECT count(*) c FROM iceberg.lab27.payments").first().c
distinct = spark.sql("SELECT count(DISTINCT payment_id) c FROM iceberg.lab27.payments").first().c
print(f"total={total} distinct={distinct}")   # PHẢI bằng nhau và = 450
spark.sql("""SELECT payment_id, count(*) FROM iceberg.lab27.payments
             GROUP BY payment_id HAVING count(*) > 1""").show()  # PHẢI rỗng
spark.sql("SELECT snapshot_id, committed_at, operation FROM iceberg.lab27.payments.snapshots").show(truncate=False)
```

### Bước 5 — phá để hiểu (quan trọng nhất)

Đổi `merge_batch` thành `deduped.writeTo("iceberg.lab27.payments_append").append()` (bảng mới, append thuần trong foreachBatch — **không** MERGE, **không** qua sink `format("iceberg")`). Lặp lại kill/restart. Đếm lại: lần này **CÓ duplicate** (batch dang dở ghi 2 lần). Ghi vào `labs/lab27/NOTES.md`: vì sao foreachBatch + append trần trụi mất exactly-once còn MERGE thì không.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Vẽ lại diagram kịch bản A/B/C (section 3.2) từ trí nhớ. Ghi rõ tại sao "commit offset sau khi ghi sink" vẫn chưa đủ cho exactly-once.
2. Idempotent write: nếu MERGE 2 lần cùng một batch data vào Iceberg, bảng thay đổi thế nào ở lần 2? Snapshot mới có được tạo không? (chạy thử rồi trả lời — xem bảng `.snapshots`)
3. Kể 3 sink at-least-once và 3 sink exactly-once trong hệ Spark.

**Medium** — Checkpoint offset và Iceberg write có atomic **với nhau** không? (Gợi ý: chúng là 2 hệ thống — checkpoint dir và Iceberg catalog. Trace lại: crash giữa "Iceberg đã commit" và "ghi commits/N" thì restart chuyện gì xảy ra? Cơ chế nào của Iceberg sink cứu tình huống này?) Viết 10–15 dòng giải thích, kèm thí nghiệm nếu làm được.

**Hard** — Multi-sink: trong foreachBatch, ghi Iceberg thành công nhưng ghi Kafka alert fail → toàn batch retry. Thiết kế giải pháp cho yêu cầu: "bảng Iceberg không được trùng, alert không được mất, alert trùng thì chấp nhận được". Viết code foreachBatch hoàn chỉnh + giải thích từng quyết định. Bonus: phác thảo phương án outbox và so sánh chi phí vận hành.

**Production Challenge** — Job production của bạn cần đổi logic transform (thêm cột). Checkpoint cũ còn đó. Trả lời: (a) thay đổi nào được phép giữ checkpoint, thay đổi nào phải bỏ? (tra docs "Recovery Semantics after Changes in a Streaming Query"); (b) nếu buộc phải bỏ checkpoint, quy trình nào để không mất/không trùng data? Viết runbook 10 bước.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| MERGE INTO mỗi micro-batch | Chậm hơn append đáng kể | MERGE = join target với source + rewrite file chứa row khớp. Batch 30s × bảng 100M rows → cân nhắc partition bảng đích để prune, hoặc MoR (lesson 31). |
| `maxOffsetsPerTrigger` quá to | Batch "bò" | 1 batch nuốt 10M message → xử lý 10 phút → latency 10 phút và fail là retry cả cục. Giữ batch vừa miếng. |
| Trigger quá dày + Iceberg | Small files + snapshot bloat | Mỗi batch 1 commit = 1 snapshot + N file nhỏ. Trigger 1s → 86k snapshot/ngày. Thực tế: 30s–5min + compaction định kỳ (lesson 32). |
| Checkpoint trên storage chậm | Mỗi batch cộng thêm vài trăm ms–vài s | Mỗi batch ít nhất 2 lần ghi file nhỏ (offsets, commits). Object storage có latency → đừng trigger 1s với checkpoint trên S3. |
| `persist()` trong foreachBatch khi ghi 2 nơi | Nhanh hơn | Không persist → 2 lần ghi = 2 lần đọc lại Kafka + transform lại. Nhớ unpersist. |

Câu tự vấn trước khi ship một streaming job: *"batch của tôi mất bao lâu so với trigger interval?"* — batch duration > trigger interval nghĩa là lag sẽ tăng vô hạn.

---

## 11. Spark UI

Streaming job mở khóa tab mới: **Structured Streaming** (UI :4040).

- Click vào query đang chạy → đồ thị **Input Rate vs Process Rate**: process < input kéo dài = lag phình → cảnh báo đỏ số 1.
- **Batch Duration**: so với trigger interval như câu tự vấn ở trên.
- Bảng batch: cột **Batch Id** — sau khi kill/restart ở lab, vào đây thấy batch_id tiếp nối chứ không reset về 0. Đối chiếu với file trong `ckpt/offsets/`.
- Với foreachBatch: mỗi action trong hàm (count, MERGE) hiện thành **job batch thường** ở tab Jobs/SQL — đọc query plan của MERGE tại tab SQL, thấy rõ nó join + rewrite gì (số liệu này giải thích vì sao MERGE chậm).
- Tab **SQL**: lần theo một batch cụ thể xem bao nhiêu file Iceberg được ghi.

---

## 12. Common Mistakes

1. **Xóa checkpoint dir cho "sạch" rồi restart** → đọc lại từ `startingOffsets` → trùng cả triệu record (hoặc mất, nếu `latest`). Checkpoint là ký ức của stream — xóa ký ức là job mới.
2. **Tưởng checkpoint một mình = exactly-once.** Checkpoint chỉ chống mất; chống trùng là việc của sink. Kafka sink + checkpoint vẫn trùng như thường.
3. **foreachBatch + append trần trụi** rồi ngạc nhiên vì duplicate — bạn đã tự tay tháo mảnh ghép ③. Trong foreachBatch, exactly-once là TRÁCH NHIỆM CỦA BẠN.
4. **Đổi logic query nhưng giữ nguyên checkpoint** khi thay đổi không tương thích (đổi agg, đổi source) → lỗi khó hiểu hoặc kết quả sai lặng lẽ.
5. **2 sink trong 1 foreachBatch, không sink nào idempotent** → sink 1 trùng mỗi khi sink 2 fail. Đã phân tích ở 3.6.
6. **Checkpoint trên disk cục bộ của container** → container recreate là mất sạch. Production: object storage/HDFS; lab này sống được nhờ volume mount `/workspace`.
7. **MERGE khi batch có 2 row cùng key mà quên dedup trước** → `MERGE ... multiple source rows matched` — lỗi runtime. Luôn dedup trong batch trước khi MERGE (lesson 29 dedup theo ts_ms + lsn).

---

## 13. Interview

**Junior:**

1. *Phân biệt at-most-once, at-least-once, exactly-once.* — At-most-once: ≤1 lần, fail là mất (commit offset trước, xử lý sau). At-least-once: ≥1 lần, fail là trùng (xử lý trước, commit offset sau). Exactly-once: kết quả như xử lý đúng 1 lần — cần sink hợp tác (idempotent/transactional), không chỉ engine.
2. *Checkpoint của Structured Streaming chứa gì?* — WAL offsets (dải offset từng batch, ghi trước khi chạy), commits (batch đã xong), metadata (query id), state store (cho stateful ops). Nhờ nó restart biết batch nào dang dở và đọc lại đúng đoạn dữ liệu.
3. *foreachBatch là gì, khi nào dùng?* — Sink tùy biến: nhận (DataFrame batch, batch_id) mỗi micro-batch, dùng được toàn bộ API batch. Dùng khi cần MERGE/upsert, sink không có connector streaming, hoặc ghi nhiều đích.
4. *Console sink có exactly-once không?* — Không, at-least-once: batch replay in lại lần nữa, không "rút lại" được output đã in. Chỉ dùng debug.

**Mid:**

5. *Ba điều kiện để Spark đạt exactly-once end-to-end?* — (a) source replayable (Kafka seek theo offset); (b) checkpoint WAL chốt dải offset mỗi batch → replay tất định; (c) sink idempotent hoặc transactional để lần ghi lặp vô hại. Thiếu một là rớt xuống at-least-once (hoặc tệ hơn).
6. *Vì sao Kafka sink của Spark chỉ at-least-once? Khắc phục?* — Kafka là append-only, Spark không dùng Kafka transaction cho sink → batch replay ghi bản thứ hai, không rollback được. Khắc phục: message có key nghiệp vụ để downstream dedup, hoặc downstream đọc idempotent (upsert theo key).
7. *batch_id trong foreachBatch giúp idempotent thế nào?* — Restart thì batch dang dở replay với **cùng batch_id và cùng dữ liệu** (offset chốt trong WAL). Sink có thể lưu batch_id đã ghi (trong cùng transaction với data) và skip khi gặp lại; hoặc ghi kèm batch_id để khử trùng sau.
8. *Vì sao file sink của Spark lại exactly-once dù chỉ ghi file?* — Nhờ manifest `_spark_metadata`: danh sách file hợp lệ của từng batch được commit atomic; reader chỉ tin manifest, file thừa do replay bị bỏ qua. Nguyên lý giống Iceberg snapshot — "atomic không nằm ở ghi file, nằm ở ghi metadata".

**Senior:**

9. *Iceberg sink đạt exactly-once bằng cơ chế gì, và có kẽ hở nào không?* — Ghi file trước, commit snapshot bằng CAS atomic trên catalog; snapshot lưu (queryId, batchId) gần nhất → batch replay bị skip. Kẽ hở: (a) chỉ bảo vệ trong cùng checkpoint/queryId — xóa checkpoint là mất bảo vệ, nên bảng quan trọng vẫn nên MERGE theo key nghiệp vụ; (b) foreachBatch tự viết thì cơ chế skip này KHÔNG tự có với append thuần — phải tự làm idempotent; (c) code không tất định (timestamp, API call) làm replay ra nội dung khác.
10. *Thiết kế foreachBatch ghi Iceberg + phát Kafka alert, yêu cầu Iceberg không trùng, alert không mất.* — Hai sink không atomic với nhau, chấp nhận at-least-once cho alert: Iceberg dùng MERGE theo key (replay vô hại), ghi Iceberg TRƯỚC alert SAU (alert fail → retry batch → MERGE vô hại → alert gửi lại, có thể trùng alert, consumer alert dedup theo key). Cần chặt hơn: outbox — alert ghi vào bảng Iceberg outbox cùng commit với data, relay riêng đọc outbox phát Kafka + đánh dấu. Trả lời tốt phải nêu được: "không tồn tại 2-sink atomic, chỉ có cách quy về 1 điểm atomic + phần còn lại idempotent".

---

## 14. Summary

### Mindmap

```
                       EXACTLY-ONCE (L27)
                             │
    ┌──────────────┬─────────┴──────────┬────────────────────┐
    ▼              ▼                    ▼                    ▼
 3 SEMANTICS   CÔNG THỨC 3 MẢNH     foreachBatch         BẪY & VŨ KHÍ
    │              │                    │                    │
 at-most:mất   ①replayable src      (df, batch_id)       console/Kafka sink
 at-least:trùng②checkpoint WAL      replay = cùng id      = at-least-once
 exactly =     ③sink idempotent/     + cùng data          multi-sink ≠ atomic
 "effectively"   transactional      MERGE theo key        → idempotent cả 2
 (kết quả 1 lần) thiếu 1 là sập     = idempotent          hoặc outbox
                                    Iceberg: atomic commit, skip batchId đã ghi
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ được 3 kịch bản failure A/B/C và chỉ ra vì sao thứ tự commit offset không tự cho exactly-once.
- [ ] Thuộc công thức 3 mảnh ghép và soi được pipeline bất kỳ xem gãy ở đâu.
- [ ] Giải thích được WAL offsets/ vs commits/ trong checkpoint dir, và batch replay vì sao tất định.
- [ ] Đã kill & restart lab, verify total = distinct, và làm bước "phá để hiểu".
- [ ] Nói được vì sao Kafka sink at-least-once còn file sink/Iceberg exactly-once.
- [ ] Biết 2 giải pháp cho multi-sink và khi nào dùng outbox.
- [ ] Trả lời 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Lesson 28 — Stream-stream & stream-static join.**

Bạn đã ghi được stream xuống bảng đích an toàn tuyệt đối. Nhưng stream thô ít khi đủ dùng — order stream cần enrich với bảng khách hàng (stream-static), và muốn bắt fraud "mua xong trả hàng trong 10 phút" thì phải join **hai stream với nhau** — bài toán buộc Spark giữ state cả hai phía và chỉ giải được nhờ watermark bạn học ở lesson 25. Join sai cách ở streaming không báo lỗi ngay — nó âm thầm phình state đến khi OOM. Lesson 28 dạy bạn join đúng.

> Gõ **"Continue"** khi sẵn sàng.
