# Lesson 23 — Streaming 101: micro-batch, unbounded table

> Module 4 · Structured Streaming · Tuần 12 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Mô hình tư duy của Structured Streaming: **stream = một cái bảng vô hạn (unbounded table)** — đây là ý tưởng quan trọng nhất của cả module 4.
- Cơ chế **micro-batch execution**: Spark không xử lý từng event, nó xử lý từng "đợt" event nhỏ.
- Giải phẫu `readStream` / `writeStream` — khác gì `read` / `write` bạn đã quen 11 tuần qua.
- 3 **output mode** (append / update / complete) và bảng "query nào được dùng mode nào".
- Các loại **trigger** và trade-off latency vs throughput.
- Giới thiệu **event time vs processing time** — nền cho lesson 25.
- Vị trí của Spark trong bản đồ streaming: micro-batch (Spark) vs per-event (Flink).

Sau bài này bạn phải làm được:

- Vẽ lại mô hình unbounded table từ trí nhớ và giải thích cho đồng nghiệp chỉ biết batch.
- Chạy streaming query đầu tiên với rate source + console sink, đọc tab Structured Streaming trên Spark UI.
- Trả lời: "batch mới đến, Spark tính lại từ đầu hay tính tiếp?" (câu này lộ ngay ai hiểu ai học vẹt).

Kiến thức dùng trong thực tế: mọi pipeline CDC, clickstream, fraud detection bạn sẽ xây ở Project 2, 3, 4 đều đứng trên nền bài này.

---

## 2. Why

### Vấn đề: batch có "khoảng mù"

11 tuần qua bạn làm batch: dữ liệu nằm yên trong file/bảng, job chạy lúc 2AM, xong là xong. Nhưng nghiệp vụ thật không chờ đến 2AM:

- Đơn hàng gian lận cần chặn **trong vài giây**, không phải sáng mai.
- Dashboard vận hành muốn thấy doanh thu **của 5 phút trước**, không phải của hôm qua.
- CDC từ PostgreSQL (repo `kafka-flink` của bạn) đổ vào Kafka **liên tục** — chờ gom đủ một ngày rồi mới xử lý nghĩa là lakehouse luôn trễ 24h.

Batch job chạy mỗi giờ? Được, nhưng bạn phải tự quản: lần trước đọc đến đâu, file nào xử lý rồi, job chết giữa chừng thì chạy lại có bị double không. Đó chính là đống việc bẩn mà Structured Streaming sinh ra để gánh hộ.

### Câu trả lời của Spark: đừng học API mới, hãy đổi cách nhìn dữ liệu

Đội Spark chọn một nước đi rất khôn (paper "Structured Streaming: A Declarative API..." — SIGMOD 2018): **không tạo API streaming riêng**. Bạn viết `filter`/`groupBy`/`join` y như batch, chỉ đổi `read` → `readStream`. Spark lo phần "dữ liệu đến liên tục" bằng cách coi stream là **một bảng được append vô hạn**.

So sánh với thế hệ trước — Spark Streaming (DStream, RDD-based): API riêng, không có Catalyst, không có event time tử tế. DStream đã **deprecated** — nếu tài liệu nào dạy bạn `StreamingContext`, đóng tab lại.

### Trade-off phải thuộc lòng

| Được | Mất |
|---|---|
| Một API cho batch + streaming (code silver layer dùng lại được) | Latency tối thiểu ~trăm ms đến vài giây (micro-batch), không phải sub-10ms |
| Exactly-once end-to-end (với sink phù hợp — lesson 27) | Cần checkpoint + sink hỗ trợ, không phải tự nhiên mà có |
| Catalyst + Tungsten tối ưu như batch | Một số phép batch bị cấm trên stream (sort tùy ý, limit, ...) |
| Chịu lỗi, tự resume từ offset | Vận hành: state store, watermark, lag — nhiều khái niệm mới |

> Bài học Senior: **chọn latency theo nghiệp vụ, không theo độ ngầu**. Dashboard 1 phút trễ? Micro-batch 30s là thừa. Chặn giao dịch <50ms? Đó là đất của Flink — và nói được điều này trong interview đáng giá hơn thuộc 100 config.

---

## 3. Theory

### 3.1. Unbounded table — mô hình tư duy số 1

Hãy tưởng tượng mọi event đến từ Kafka/socket được **append vào cuối một cái bảng không bao giờ đóng**:

```
  INPUT STREAM  =  UNBOUNDED TABLE (bảng vô hạn, chỉ có append)

  t=1s  event A ──┐        ┌───────────────┐
  t=2s  event B ──┼──────▶ │ A             │  ← bảng lúc t=1
  t=3s  event C ──┘        ├───────────────┤
                           │ A, B          │  ← bảng lúc t=2
                           ├───────────────┤
                           │ A, B, C       │  ← bảng lúc t=3
                           └───────────────┘
                             dòng mới chỉ được THÊM vào cuối,
                             không sửa, không xóa

  QUERY của bạn (filter/groupBy/...) chạy trên bảng này
       │
       ▼
  RESULT TABLE (bảng kết quả — cũng lớn dần / cập nhật dần)
       │
       ▼  output mode quyết định phần nào của result table được ghi ra
  SINK (console, Kafka, Iceberg, ...)
```

Điểm ăn tiền: về **ngữ nghĩa**, kết quả tại mọi thời điểm PHẢI giống hệt việc chạy batch query trên toàn bộ bảng tính đến lúc đó. Nhưng về **thực thi**, Spark không dại gì tính lại từ đầu — nó tính **incremental**: batch mới đến, chỉ xử lý phần mới + cộng dồn vào **state** (kết quả nhớ từ trước). Ngữ nghĩa là "query cả bảng", thực thi là "query phần chênh lệch". Tách được 2 tầng này trong đầu là bạn đã hiểu 50% module 4.

> **Analogy sổ thu chi**: bạn ghi chép chi tiêu cả đời vào một cuốn sổ (unbounded table). Câu hỏi "tổng chi theo tháng?" — bạn không cộng lại cả cuốn sổ mỗi lần có hóa đơn mới; bạn giữ một tờ tổng kết (result table + state) và mỗi hóa đơn mới chỉ cộng thêm vào đúng ô tháng đó. Structured Streaming chính là người giữ tờ tổng kết ấy hộ bạn.

### 3.2. Micro-batch execution

Spark không xử lý từng event một. Nó cắt dòng event thành các **micro-batch** — mỗi batch là một job Spark bình thường (có stage, task, shuffle như bạn đã học):

```
 events:   ● ● ●   ● ●     ● ● ● ●    ●        ●●
 time:   ──┬───────┬───────┬──────────┬─────────┬────▶
           │batch 0│batch 1│ batch 2  │ batch 3 │
           └──▶ job└──▶ job└──▶ job   └──▶ job
                (mỗi micro-batch = 1 job Spark, chạy xong
                 ghi kết quả + lưu tiến độ, rồi chờ batch sau)
```

Chu trình mỗi micro-batch (đơn giản hóa — chi tiết ở mục 4):

1. Hỏi source: "từ lần trước đến giờ có gì mới?" → chốt khoảng dữ liệu (ví dụ offset Kafka X→Y).
2. Chạy query trên khúc dữ liệu đó như một batch job, kết hợp với state cũ.
3. Ghi kết quả ra sink theo output mode, cập nhật state, ghi tiến độ vào checkpoint.
4. Lặp lại theo trigger.

Hệ quả trực tiếp: **latency tối thiểu ≈ thời gian chạy 1 micro-batch** (thường trăm ms → vài giây). Đổi lại throughput rất cao vì mỗi batch tận dụng toàn bộ máy móc tối ưu của Spark SQL.

### 3.3. Giải phẫu readStream / writeStream

```python
df = (spark.readStream          # DataStreamReader — thay vì spark.read
      .format("rate")           # source: rate / kafka / socket / file
      .option("rowsPerSecond", 10)
      .load())                  # trả về DataFrame STREAMING (df.isStreaming == True)

query = (df.writeStream         # DataStreamWriter — thay vì df.write
         .format("console")     # sink: console / kafka / parquet / memory / foreachBatch
         .outputMode("append")  # append / update / complete
         .trigger(processingTime="10 seconds")
         .option("checkpointLocation", "/workspace/data/chk/demo")  # sổ tiến độ
         .start())               # trả về StreamingQuery — chạy NỀN, non-blocking!

query.awaitTermination()        # giữ driver sống; không có dòng này script thoát ngay
```

Khác biệt chí mạng so với batch: `start()` **không chặn** — nó khởi động query chạy nền rồi trả về ngay. Quên `awaitTermination()` là script kết thúc, query chết theo, và bạn ngồi thắc mắc "sao không thấy output". Đây là lỗi số 1 của người mới.

DataFrame streaming trông y hệt DataFrame thường nhưng bị cấm một số phép: `count()`/`show()`/`collect()` trực tiếp (phải qua sink), sort không kèm aggregation, v.v. Spark sẽ ném `AnalysisException` nói thẳng phép nào không hỗ trợ.

### 3.4. Output modes — ghi phần nào của result table?

- **append**: chỉ ghi **dòng mới thêm** vào result table, dòng đã ghi không bao giờ đổi. Sink chỉ cần biết "nhận thêm dòng".
- **update**: ghi các dòng **mới hoặc vừa thay đổi** kể từ batch trước. Sink phải xử lý được chuyện một key xuất hiện nhiều lần với giá trị mới dần.
- **complete**: ghi **toàn bộ** result table mỗi batch. Chỉ khả thi khi result table nhỏ (bảng tổng hợp ít key).

Mode nào hợp lệ phụ thuộc query — Spark kiểm tra lúc `start()`:

| Loại query | append | update | complete |
|---|---|---|---|
| Chỉ select/filter/map (stateless) | ✅ mặc định | ✅ (giống append) | ❌ (result table vô hạn, ghi lại cả bảng là tự sát) |
| groupBy aggregation **không** watermark | ❌ (dòng nào cũng có thể còn đổi — chẳng dòng nào "chốt" được) | ✅ | ✅ |
| groupBy trên window **có** watermark | ✅ (window đóng mới emit — lesson 25) | ✅ | ✅ |
| dropDuplicates | ✅ | ✅ | ❌ |
| stream-stream join | ✅ | tùy loại | ❌ |

Đọc bảng này theo logic, đừng học thuộc: *append cần dòng "chốt sổ" không đổi nữa; complete cần result table đủ nhỏ để chép lại mỗi batch; update là lối thoát ở giữa.*

### 3.5. Trigger — nhịp tim của stream

| Trigger | Cú pháp (PySpark 3.4) | Hành vi | Dùng khi |
|---|---|---|---|
| Default | không khai gì | Batch xong là chạy batch tiếp ngay (nếu có data mới) | Muốn latency thấp nhất có thể của micro-batch |
| ProcessingTime | `.trigger(processingTime="30 seconds")` | Nhịp cố định 30s; batch chạy quá 30s thì batch sau chạy ngay khi xong | Phổ biến nhất production — nhịp ổn định, dễ tính tài nguyên |
| AvailableNow | `.trigger(availableNow=True)` | Xử lý HẾT dữ liệu đang có (chia nhiều micro-batch), xong thì **tự dừng** | Chạy streaming theo lịch Airflow — "batch job có checkpoint". Spark 3.3+ |
| Once | `.trigger(once=True)` | Nhét hết dữ liệu tồn vào **1 batch duy nhất** rồi dừng — **deprecated**, dễ OOM khi tồn nhiều | Đừng dùng nữa, thay bằng availableNow |
| Continuous | `.trigger(continuous="1 second")` | Chế độ per-event thật (~1ms latency) — **experimental**, chỉ map/filter, ít ai dùng production | Gần như không bao giờ. Biết để trả lời interview |

`AvailableNow` là viên ngọc ít người biết: bạn được **cả hai thế giới** — ngữ nghĩa streaming (offset tracking, checkpoint, exactly-once) nhưng vận hành như batch (chạy theo cron, không chiếm cluster 24/7). Rất nhiều pipeline "near-real-time mỗi 15 phút" trong thực tế chạy kiểu này.

### 3.6. Latency vs throughput

```
 trigger ngắn (1s):   |■|■|■|■|■|■|   latency thấp, NHƯNG overhead lập lịch/
                                      commit mỗi batch chiếm tỷ trọng lớn,
                                      sinh nhiều file nhỏ nếu sink là file
 trigger dài (2 min): |■■■■■■■■■■|    throughput/batch cao, file to đẹp,
                                      NHƯNG dữ liệu chờ đến 2 phút mới thấy
```

Không có đáp án đúng tuyệt đối — chỉ có đáp án đúng **cho SLA của bạn**. Quy tắc thô: latency yêu cầu phút → trigger 30s–1min; yêu cầu giây → default/vài giây và chuẩn bị tiền cluster; yêu cầu <100ms → cân nhắc Flink.

### 3.7. Event time vs processing time (giới thiệu — lesson 25 học sâu)

- **Event time**: thời điểm sự việc **xảy ra** — nằm trong dữ liệu (cột `order_purchase_timestamp`).
- **Processing time**: thời điểm Spark **nhìn thấy** event — đồng hồ trên tường của cluster.

Hai cái này lệch nhau vì mạng chậm, thiết bị offline, Kafka tồn đọng. Đơn hàng đặt lúc 09:59 có thể đến Spark lúc 10:03 — nó thuộc doanh thu khung 9h–10h hay 10h–11h? Nghiệp vụ nói: 9h–10h (event time). Muốn đúng theo event time thì phải trả lời được "chờ dữ liệu trễ đến bao giờ?" — đó chính là **watermark**, nhân vật chính của lesson 25. Hôm nay chỉ cần khắc cốt: **aggregation nghiêm túc luôn theo event time**.

### 3.8. Spark micro-batch vs Flink per-event

| | Spark Structured Streaming | Flink |
|---|---|---|
| Đơn vị xử lý | micro-batch (nhóm event) | từng event (true streaming) |
| Latency điển hình | trăm ms → giây | ms → chục ms |
| Throughput | rất cao (tối ưu batch) | cao |
| API | trùng batch API — team biết Spark là biết luôn | API riêng (DataStream/SQL) |
| Thế mạnh | ETL streaming vào lakehouse, unified batch+stream | low-latency event-driven app, CEP, state khổng lồ |

Trong kiến trúc repo `kafka-flink` của bạn, hai đứa này **không phải kẻ thù**: Flink có thể gánh phần latency ms (alerting), Spark gánh phần đổ dữ liệu vào Iceberg (throughput + hệ sinh thái lakehouse). Chọn theo bài toán, không theo tôn giáo.

---

## 4. Internal

Chuyện gì xảy ra bên trong một micro-batch, từ lúc `start()`:

```
① start() → tạo StreamExecution (MicroBatchExecution) chạy trên
   một thread riêng ở DRIVER — vòng lặp vô hạn theo trigger
        │
② ĐẦU BATCH: hỏi source "offset mới nhất là gì?"
   (Kafka: latest offset mỗi partition; rate: timestamp hiện tại)
        │
③ GHI Ý ĐỊNH vào checkpoint: file offsets/<batchId>
   "batch N sẽ xử lý từ offset X đến Y"
   → ghi TRƯỚC khi chạy (write-ahead log) — chết giữa chừng
     thì lúc dậy biết chính xác định làm gì
        │
④ Incrementalize: lấy logical plan bạn viết, thay source bằng
   khúc dữ liệu [X, Y), nối với STATE của các batch trước
   → Catalyst optimize → chạy như 1 job Spark bình thường
   (stage, task, shuffle — mọi thứ module 1–3 áp dụng nguyên si)
        │
⑤ Executor xử lý task; các phép stateful đọc/ghi state store
   ngay trên executor (lesson 26)
        │
⑥ Sink nhận kết quả theo output mode
        │
⑦ CUỐI BATCH: ghi file commits/<batchId> vào checkpoint
   → "batch N XONG rồi". Có offsets mà không có commits
     = batch dở dang → restart sẽ CHẠY LẠI đúng batch đó
        │
⑧ Cập nhật watermark, dọn state hết hạn, chờ trigger → quay lại ②
```

Ba điều rút ra:

- **Mỗi micro-batch là một job Spark thật** — nên mọi kỹ năng tuning module 3 (shuffle, skew, AQE, partition) vẫn là vũ khí của bạn, chỉ khác là giờ job chạy lặp mãi mãi.
- **Cặp offsets/commits là trái tim của fault tolerance** — lesson 24 sẽ mổ xẻ tận file.
- Vòng lặp điều phối nằm ở **driver** — driver chết là stream chết (nhưng restart với cùng checkpoint là sống lại đúng chỗ cũ).

---

## 5. API

### `spark.readStream` + rate source

```python
df = (spark.readStream.format("rate")
      .option("rowsPerSecond", 100)   # sinh 100 dòng/giây
      .load())
# Schema cố định: timestamp: timestamp, value: long (0,1,2,...)
```
- **Ý nghĩa**: source tự sinh dữ liệu — "máy phát nhịp" để học và benchmark, không cần Kafka.
- **Pitfall**: đừng đo throughput thật bằng rate source rồi kết luận về Kafka — bỏ qua toàn bộ chi phí network/deserialize.

### `df.writeStream` + console sink

```python
q = (df.writeStream.format("console")
     .option("truncate", "false")
     .outputMode("append")
     .start())
```
- **Ý nghĩa**: in kết quả mỗi batch ra stdout của **driver** — công cụ debug số 1.
- **Pitfall**: console sink kéo dữ liệu về driver — chỉ dùng để học/debug, cấm production.

### `.outputMode(...)` / `.trigger(...)`

```python
.outputMode("update")
.trigger(processingTime="10 seconds")   # hoặc availableNow=True
```
- **Pitfall**: sai cặp query↔mode là `AnalysisException` ngay lúc `start()` — quay lại bảng 3.4 thay vì thử mò.

### `StreamingQuery` — cần điều khiển sau `start()`

```python
q.status          # {'message': 'Waiting for data...', 'isDataAvailable': ...}
q.lastProgress    # dict metric batch gần nhất: inputRowsPerSecond, batchDuration...
q.stop()          # dừng êm
q.awaitTermination(timeout=120)   # chặn tối đa 120s (không timeout = chặn mãi)
```
- **Pitfall**: exception trong query nền không văng ra chỗ bạn gọi `start()` — nó văng ra ở `awaitTermination()`. Stream "im lặng" ≠ stream khỏe; xem `q.exception()` khi nghi ngờ.

### `.option("checkpointLocation", ...)`

- **Ý nghĩa**: thư mục lưu offset/commit/state — bộ nhớ tiến độ của query.
- **Pitfall**: console sink cho phép bỏ qua checkpoint (dùng temp) nhưng mọi sink nghiêm túc BẮT BUỘC có. Mỗi query một thư mục riêng — 2 query chung checkpoint là ăn corrupt.

---

## 6. Demo nhỏ

```
Input:  rate source, 5 dòng/giây (timestamp, value)
   ↓    withColumn tính value % 3 làm "khóa" (transformation — quen thuộc)
   ↓    groupBy khóa, count (stateful aggregation!)
Output: console, mode update, trigger 5s — xem count LỚN DẦN qua các batch
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo23").getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # log INFO của streaming rất ồn

stream = (spark.readStream.format("rate")
          .option("rowsPerSecond", 5).load())

counts = (stream
          .withColumn("key", (F.col("value") % 3).cast("string"))
          .groupBy("key").count())          # chưa chạy gì — lazy như batch

q = (counts.writeStream.format("console")
     .outputMode("update")                  # thử đổi "append" → AnalysisException!
     .trigger(processingTime="5 seconds")
     .start())

q.awaitTermination(60)                      # tự dừng sau ~60s
q.stop(); spark.stop()
```

Quan sát: Batch 0 có thể rỗng; các batch sau count của key 0/1/2 **tăng dần** — bằng chứng sống của state: Spark nhớ kết quả cũ và cộng tiếp, không đếm lại từ đầu. Đổi `update` thành `complete` xem khác gì (in cả 3 key mỗi batch dù không đổi).

---

## 7. Production Example

Nhìn lại kiến trúc repo `kafka-flink` — hôm nay bạn biết chính xác Structured Streaming ngồi ghế nào:

```
PostgreSQL ──(Debezium/CDC)──▶ Kafka ──▶ SPARK STRUCTURED STREAMING ──▶ Iceberg ──▶ Trino
                                              │
                                              ├─ trigger 1 min: bronze ingest (append)
                                              ├─ trigger 5 min: silver dedup + parse CDC
                                              └─ AvailableNow theo Airflow: gold aggregate
```

Cách các công ty thật cấu hình tầng này:

1. **Bronze — trigger ngắn (30s–1min), append mode**: chép nguyên liệu từ Kafka vào Iceberg càng sớm càng tốt, không biến đổi gì nặng. Latency thấp, logic mỏng.
2. **Silver — trigger vừa (5–15min) hoặc AvailableNow**: dedup, parse Debezium envelope, MERGE. Không ai cần silver latency 1 giây — trigger dài hơn = file to hơn, ít snapshot Iceberg rác hơn.
3. **Gold — thường là batch/AvailableNow theo Airflow**: metric tổng hợp cho BI, mỗi 30–60 phút là quá đủ.

Bài học kiến trúc: **cùng một công nghệ, ba nhịp trigger khác nhau theo SLA từng tầng** — đây chính là latency vs throughput trade-off (mục 3.6) hóa thân vào đời thật. Netflix/Uber/Grab đều xếp hình kiểu này, khác nhau mỗi nhãn hiệu storage.

---

## 8. Hands-on Lab

**Mục tiêu**: chạy 2 streaming query đầu tiên (rate + socket), quan sát micro-batch bằng mắt, mở tab Structured Streaming trên UI.

### Bước 1 — `labs/lab23/rate_console.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab23-rate").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stream = (spark.readStream.format("rate")
          .option("rowsPerSecond", 20).load())

agg = (stream.withColumn("bucket", (F.col("value") % 5).cast("string"))
       .groupBy("bucket")
       .agg(F.count("*").alias("cnt"), F.max("timestamp").alias("last_seen")))

q = (agg.writeStream.format("console")
     .outputMode("update")
     .trigger(processingTime="10 seconds")
     .option("truncate", "false")
     .start())
q.awaitTermination(180)   # chạy 3 phút rồi tự dừng
```

Chạy **local mode để thấy console ngay trên terminal**:

```bash
make run-local F=labs/lab23/rate_console.py
```

### Bước 2 — socket source (nc) — `labs/lab23/socket_wordcount.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab23-socket").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

lines = (spark.readStream.format("socket")
         .option("host", "localhost").option("port", 9999).load())

words = (lines.select(F.explode(F.split("value", " ")).alias("word"))
         .groupBy("word").count())

q = (words.writeStream.format("console")
     .outputMode("complete")     # bảng word-count nhỏ → complete xem toàn cảnh
     .start())
q.awaitTermination()
```

Socket source cần `nc` chạy **cùng network với Spark**. Cách đơn giản nhất — mở nc ngay trong container submit (terminal 1), rồi chạy job (terminal 2):

```bash
# Terminal 1: máy phát dữ liệu
docker exec -it spark-mastery-spark-submit-1 bash -c "apt-get install -y netcat 2>/dev/null; nc -lk 9999"
# (image không có nc? dùng: python3 -c "exec(open('/workspace/labs/lab23/tiny_server.py').read())"
#  hoặc bỏ qua bước socket, rate source là đủ cho mục tiêu bài này)

# Terminal 2:
make run-local F=labs/lab23/socket_wordcount.py
```

Gõ vài câu vào terminal 1, Enter — nhìn word count đổi ở terminal 2. Cảm giác "gõ phím thấy kết quả" này chính là streaming.

### Bước 3 — quan sát (phần quan trọng nhất)

Khi query đang chạy, mở `http://localhost:4040` → tab **Structured Streaming**:

1. Click vào run đang active → xem 4 đồ thị: **Input Rate**, **Process Rate**, **Batch Duration**, **Operation Duration**.
2. Tab **Jobs**: thấy job mới sinh ra ĐỀU ĐẶN theo trigger — mỗi micro-batch một (vài) job. Đối chiếu với mục 4.
3. Sửa `rowsPerSecond` từ 20 → 20000, chạy lại: Batch Duration thay đổi thế nào? Input Rate có đuổi kịp Process Rate không?

Ghi 3 quan sát vào `labs/lab23/NOTES.md`.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Unbounded table là gì? Vì sao nói "ngữ nghĩa là query cả bảng, thực thi là incremental"?
2. Vẽ timeline: event đến → nằm chờ → micro-batch trigger → output. Latency tổng gồm những khúc nào?
3. Vì sao aggregation không watermark bị cấm append mode? (dùng logic "dòng chốt sổ")

**Medium** — Lấy `rate_console.py`, chạy 3 lần với trigger `5 seconds` → `30 seconds` → `2 minutes` (giữ nguyên rowsPerSecond). Với mỗi lần, ghi lại từ tab Structured Streaming: batch duration, số dòng/batch, và latency tệ nhất một event phải chịu (ước lượng = trigger + batch duration). Viết 5 dòng kết luận về latency vs throughput.

**Hard** — Đọc docs `Trigger.Continuous`. Trả lời: (a) tại sao continuous mode không hỗ trợ aggregation? (gợi ý: shuffle cần chặn cả batch); (b) so sánh cơ chế checkpoint của continuous (epoch marker) vs micro-batch (offset log); (c) nếu bạn cần <10ms latency thật, bạn đề xuất gì với team — continuous mode hay công nghệ khác? Vì sao?

**Production Challenge** — Thiết kế trigger cho 3 pipeline sau, mỗi cái 3–5 dòng lý do: (1) bronze ingest CDC vào Iceberg, SLA dashboard 5 phút; (2) tính tổng doanh thu ngày cho báo cáo 8AM hôm sau; (3) đếm số lượt view video hiển thị ngay trên trang cho creator. Pipeline nào không nên dùng Spark Streaming luôn?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Trigger 1s với sink ra file | Chậm dần theo thời gian | Mỗi batch đẻ file nhỏ → small files problem (lesson 21) quay lại báo thù. Trigger dài hơn hoặc compaction. |
| Batch duration > trigger interval | Nguy hiểm | Batch sau dồn toa → lag tăng vô hạn. Đây là "job chậm" phiên bản streaming — xử bằng đúng playbook module 3. |
| `complete` mode với nhiều key | Chậm + phình | Chép lại toàn bộ result table mỗi batch. Nhiều key → dùng update. |
| Aggregation trên stream | Có shuffle mỗi batch | `spark.sql.shuffle.partitions` mặc định 200 — với micro-batch bé, 200 task tí hon toàn overhead. Giảm xuống (vd 8–16) là một trong những tuning lãi nhất của streaming. |

Câu tự vấn mới cho streaming: *"batch duration của tôi có nhỏ hơn trigger interval không, và nó có ỔN ĐỊNH không?"* — duration tăng dần đều là triệu chứng state phình (lesson 26 xử).

---

## 11. Spark UI

Bài này mở khóa tab mới: **Structured Streaming** (UI :4040 khi query chạy).

- **Input Rate vs Process Rate**: dòng vào/giây vs khả năng xử lý/giây. Input > Process kéo dài = lag phình — đồ thị quan trọng nhất tab này.
- **Batch Duration**: thời gian mỗi micro-batch. Phải < trigger interval và đi ngang; dốc lên đều = có gì đó tích tụ (thường là state).
- **Operation Duration**: mổ xẻ batch duration — addBatch (chạy job), getBatch, latestOffset, commitOffsets, walCommit. Batch chậm mà addBatch nhỏ? Vấn đề ở source/checkpoint I/O, không phải logic của bạn.

Tab **Jobs** vẫn hữu ích: mỗi micro-batch hiện thành job có mô tả `id = <query-id>, batch = N` — click vào là quay về thế giới stage/task quen thuộc của module 1–3.

---

## 12. Common Mistakes

1. **Quên `awaitTermination()`** → script thoát ngay sau `start()`, "sao không có output?". Lỗi vỡ lòng số 1.
2. **Gọi `df.show()`/`count()` trên streaming DataFrame** → `AnalysisException`. Muốn nhìn dữ liệu: console sink hoặc memory sink.
3. **Học DStream/`StreamingContext` từ tutorial cũ** — API deprecated, không Catalyst, không event time. Chỉ học Structured Streaming.
4. **Dùng complete mode "cho chắc"** với result table lớn → chậm và phình memory. Chọn mode theo bảng 3.4.
5. **Nghĩ streaming là công nghệ khác batch** — rồi không áp dụng kiến thức tuning đã học. Mỗi micro-batch là job Spark thường; skew vẫn là skew, shuffle vẫn đắt.
6. **Đòi real-time bằng mọi giá** khi nghiệp vụ chỉ cần 5 phút — trả tiền cluster 24/7 cho SLA không ai yêu cầu. `AvailableNow` + cron thường là câu trả lời trưởng thành hơn.

---

## 13. Interview

**Junior:**

1. *Structured Streaming là gì, khác Spark Streaming (DStream) chỗ nào?* — Engine streaming trên Spark SQL, coi stream như bảng vô hạn, dùng chung DataFrame API với batch, có Catalyst tối ưu + event time + exactly-once. DStream là thế hệ cũ RDD-based, API riêng, đã deprecated.
2. *Unbounded table là gì?* — Mô hình coi stream như bảng chỉ-append vô hạn; query của bạn về ngữ nghĩa chạy trên cả bảng, kết quả tại mọi thời điểm giống hệt batch query trên dữ liệu tính đến lúc đó; thực thi thì incremental trên phần mới + state.
3. *Micro-batch là gì, latency cỡ nào?* — Spark cắt stream thành các đợt nhỏ, mỗi đợt chạy như 1 job Spark. Latency tối thiểu ≈ thời gian 1 batch, thực tế trăm ms → vài giây.
4. *Ba output mode khác nhau thế nào?* — Append: chỉ dòng mới, không đổi nữa. Update: dòng mới hoặc vừa thay đổi. Complete: toàn bộ result table mỗi batch, chỉ hợp bảng kết quả nhỏ.

**Mid:**

5. *Vì sao groupBy không watermark không dùng được append mode?* — Append đòi hỏi dòng đã emit không bao giờ đổi; không watermark thì mọi key có thể còn nhận data mới bất kỳ lúc nào, không dòng nào "chốt" được → chỉ update/complete hợp lệ.
6. *Trigger AvailableNow dùng khi nào, hơn gì Trigger.Once?* — Chạy streaming như batch theo lịch: xử hết dữ liệu tồn rồi tự dừng, giữ nguyên checkpoint/exactly-once. Hơn Once ở chỗ chia dữ liệu tồn thành nhiều micro-batch (tôn trọng maxOffsetsPerTrigger) thay vì nhồi 1 batch khổng lồ dễ OOM — Once đã deprecated.
7. *Batch duration lớn hơn trigger interval thì chuyện gì xảy ra?* — Batch sau khởi động muộn (không chạy chồng), dữ liệu tích tụ ở source, lag tăng; nếu kéo dài là mất SLA. Xử: tuning như job batch chậm (shuffle partitions, skew, tài nguyên) hoặc nới trigger/giảm tải mỗi batch.
8. *Event time vs processing time — vì sao aggregation nên theo event time?* — Event time là lúc sự việc xảy ra (trong data), processing time là lúc Spark thấy nó. Data đến trễ/lệch thứ tự là chuyện thường; aggregate theo processing time cho kết quả phụ thuộc vào... tình trạng mạng, không phải nghiệp vụ.

**Senior:**

9. *Spark chọn micro-batch thay vì per-event — trade-off thiết kế là gì?* — Được: tái dùng toàn bộ engine batch (Catalyst/Tungsten/scheduler) → throughput cao, một API cho batch+stream, fault tolerance đơn giản bằng offset log theo batch. Mất: latency sàn cỡ trăm ms–giây, không phù hợp use case ms. Continuous mode tồn tại nhưng experimental và không hỗ trợ aggregation vì shuffle đòi ranh giới batch. Chọn Spark khi bài toán là ETL/analytics streaming; chọn Flink khi cần per-event latency hoặc CEP.
10. *Team đề xuất chạy stream 24/7 cho báo cáo cập nhật mỗi giờ — bạn phản biện thế nào?* — SLA 1 giờ không cần cluster thường trực: dùng Trigger.AvailableNow theo Airflow mỗi giờ — vẫn checkpoint/exactly-once, code không đổi, tiết kiệm ~90% chi phí compute, vận hành đơn giản hơn (không lo stream chết đêm). Streaming liên tục chỉ đáng khi SLA phút/giây. Câu trả lời thể hiện tư duy cost-aware — điểm cộng lớn với interviewer.

---

## 14. Summary

### Mindmap

```
                    STRUCTURED STREAMING 101
                              │
     ┌────────────────┬───────┴───────┬────────────────────┐
     ▼                ▼               ▼                    ▼
 MÔ HÌNH          THỰC THI        API                  TRADE-OFF
     │                │               │                    │
 unbounded        micro-batch     readStream/load      latency vs throughput
 table            = job Spark     writeStream/start    (trigger quyết định)
 (append-only)    lặp theo        outputMode:          micro-batch (Spark)
 result table     trigger         append/update/        vs per-event (Flink)
 incremental      offsets→run     complete             event time vs
 + state          →commits        awaitTermination!    processing time
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại sơ đồ unbounded table → result table → sink không nhìn tài liệu.
- [ ] Giải thích được micro-batch = job Spark bình thường chạy lặp, và vì sao điều đó nghĩa là kiến thức module 1–3 vẫn dùng nguyên.
- [ ] Thuộc bảng output mode × loại query — theo logic, không học vẹt.
- [ ] Nói được khi nào dùng ProcessingTime vs AvailableNow, vì sao Once deprecated.
- [ ] Đã chạy rate source, thấy count tăng dần qua các batch (bằng chứng của state).
- [ ] Đã mở tab Structured Streaming, chỉ được Input Rate / Process Rate / Batch Duration.
- [ ] Phát biểu được 1 câu vì sao chọn Spark hay Flink cho một use case cụ thể.

---

## 15. Next Lesson

**Lesson 24 — Kafka source/sink: offset, checkpoint, trigger.**

Rate source là bánh xe phụ — production đọc **Kafka**. Lesson 24 nối Spark vào Kafka trong repo `kafka-flink` của bạn: schema cố định key/value của Kafka source, `startingOffsets`, backpressure bằng `maxOffsetsPerTrigger`, và quan trọng nhất — **mổ xẻ thư mục checkpoint** (offsets/ commits/ sources/) để hiểu tận gốc vì sao kill job rồi restart mà không mất, không double dữ liệu. Bạn đã thấy checkpoint thoáng qua ở mục Internal hôm nay; mai ta mở từng file ra đọc.

Không hiểu checkpoint thì mọi lời hứa exactly-once ở lesson 27 đều là niềm tin mù quáng — nên ta giải phẫu nó ngay bài sau.

> Gõ **"Continue"** khi sẵn sàng.
