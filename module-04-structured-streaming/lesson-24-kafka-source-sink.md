# Lesson 24 — Kafka source/sink: offset, checkpoint, trigger

> Module 4 · Structured Streaming · Tuần 12 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Đọc Kafka bằng Structured Streaming: format `kafka`, **schema cố định 7 cột** và nghi thức cast `value` từ binary.
- `startingOffsets` (earliest / latest / JSON per-partition) — đọc từ đâu trong lần chạy ĐẦU TIÊN.
- **Backpressure** bằng `maxOffsetsPerTrigger` — van chống ngộp khi Kafka tồn hàng triệu message.
- **Giải phẫu thư mục checkpoint**: `offsets/`, `commits/`, `sources/` — mở từng file ra đọc.
- Cơ chế **resume sau restart**: vì sao kill -9 xong bật lại vẫn không mất, không double.
- Ghi ngược ra Kafka sink, `failOnDataLoss`, và sự thật ít ai biết: **consumer group của Spark không giống consumer group thường**.

Sau bài này bạn phải làm được:

- Nối Spark cluster của bạn vào Kafka của repo `../kafka-flink`, đọc topic ra console.
- Kill job giữa chừng, mở checkpoint chỉ ra batch nào dở dang, restart và chứng minh nó chạy tiếp đúng chỗ.
- Trả lời: "Spark commit offset vào Kafka à?" — (KHÔNG, và bạn phải giải thích được vì sao).

Kiến thức dùng trong thực tế: 90% pipeline streaming doanh nghiệp có Kafka ở đầu vào. Đây là bài "cơm áo gạo tiền" nhất module 4.

---

## 2. Why

### Vì sao cứ phải là Kafka?

Nguồn streaming thật (DB qua CDC, app events, IoT) không nói chuyện thẳng với Spark, vì cần một cái **đệm** ở giữa:

- **Tốc độ lệch pha**: nguồn bắn 50k msg/s lúc peak; Kafka giữ hàng theo retention (ngày/tuần), Spark đọc theo sức mình.
- **Replay**: bug ở silver layer, cần chạy lại 3 ngày dữ liệu? Kafka còn giữ → chỉnh offset đọc lại. Nguồn bắn thẳng vào Spark thì dữ liệu qua là qua luôn.
- **Nhiều consumer**: cùng topic `orders`, Spark đổ lakehouse, Flink bắn alert — không ai giẫm chân ai vì **mỗi consumer tự giữ vị trí đọc (offset) của mình**.

Ôn nhanh từ vựng Kafka (repo `kafka-flink` bạn đã dựng): **topic** = kênh; **partition** = topic chia khúc để song song; **offset** = số thứ tự tăng dần của message TRONG một partition. Bộ ba `(topic, partition, offset)` định danh duy nhất một message — nhớ kỹ, cả bài hôm nay xoay quanh nó.

### Vấn đề thật sự: nhớ mình đã đọc đến đâu

Đọc Kafka thì dễ. Cái khó là câu hỏi sống còn của mọi hệ thống streaming: **"nếu tôi chết giữa chừng, lúc sống lại tôi đọc tiếp từ đâu?"**

- Nhớ vị trí *trước khi* xử lý xong → chết giữa chừng là **mất dữ liệu** (at-most-once).
- Nhớ vị trí *sau khi* xử lý xong → chết sau xử lý, trước khi kịp nhớ → **double dữ liệu** (at-least-once).

Spark giải bằng **checkpoint hai pha** (offsets trước, commits sau) — và hôm nay ta mổ tận file để bạn không phải "tin", mà **thấy**.

> Bài học Senior: đừng bao giờ nhận một hệ thống streaming vào tay mà chưa biết checkpoint của nó nằm đâu và chứa gì. Ngày nó chết lúc 3AM (sẽ có ngày đó), thư mục checkpoint là hiện trường vụ án duy nhất bạn có.

---

## 3. Theory

### 3.1. Kafka source — schema cố định, value là binary

```python
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "orders")            # hoặc subscribePattern / assign
       .option("startingOffsets", "earliest")
       .load())
```

Bất kể topic chứa gì, DataFrame trả về LUÔN có đúng schema này:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `key` | binary | Key của message (quyết định message vào partition nào) |
| `value` | binary | **Payload — món chính của bạn** |
| `topic` | string | Tên topic (quan trọng khi subscribe nhiều topic) |
| `partition` | int | Partition chứa message |
| `offset` | long | Vị trí trong partition |
| `timestamp` | timestamp | Thời điểm message vào Kafka (thường dùng làm event time tạm) |
| `timestampType` | int | 0 = CreateTime, 1 = LogAppendTime |

Kafka không biết (và không quan tâm) payload của bạn là JSON hay Avro — với nó tất cả là **bytes**. Nghi thức bắt buộc: cast rồi parse:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([StructField("order_id", StringType()),
                     StructField("amount", DoubleType()),
                     StructField("created_at", TimestampType())])

orders = (raw
    .select(F.col("key").cast("string").alias("k"),
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
            "topic", "partition", "offset", "timestamp")
    .select("k", "data.*", "topic", "partition", "offset", "timestamp"))
```

Khai schema **tường minh** — người anh em của bài học `inferSchema` từ lesson 1: trên stream thậm chí không có "infer" mà xài, và JSON rác không khớp schema sẽ thành NULL (chứ không nổ) — hãy chủ động đếm NULL để bắt rác.

### 3.2. startingOffsets — chỉ có ý nghĩa ở LẦN CHẠY ĐẦU

| Giá trị | Nghĩa | Dùng khi |
|---|---|---|
| `earliest` | Đọc từ message cũ nhất còn trong retention | Backfill, dev muốn có data ngay |
| `latest` (mặc định) | Chỉ đọc message đến SAU khi query start | Chỉ quan tâm "từ giờ trở đi" |
| JSON: `{"orders":{"0":1500,"1":-2}}` | Chỉ định offset từng partition (-2=earliest, -1=latest) | Reprocess phẫu thuật từ một điểm cụ thể |

Chữ in hoa cần khắc vào não: **một khi checkpoint đã tồn tại, `startingOffsets` bị BỎ QUA hoàn toàn** — vị trí đọc lấy từ checkpoint. Đổi option này rồi restart mà mong nó đọc lại từ đầu là ảo tưởng phổ biến số 1 của bài này. Muốn đọc lại từ đầu thật: xóa (hoặc trỏ sang) checkpoint mới — và chấp nhận mọi hệ quả double ở downstream.

### 3.3. maxOffsetsPerTrigger — van backpressure

Tình huống kinh điển: stream chết từ đêm qua, Kafka tồn 50 triệu message. Restart với trigger default → Spark định nghĩa batch đầu tiên = "từ offset checkpoint đến latest" = **nguyên 50 triệu message trong MỘT batch** → OOM hoặc batch chạy 2 tiếng.

```python
.option("maxOffsetsPerTrigger", "100000")   # tổng tối đa 100k msg/batch, chia đều theo tỷ lệ tồn của các partition
```

```
Không có van:   [██████████████████ 50M ██████████████████] ← 1 batch tử thần
Có van 100k:    [100k][100k][100k][100k][100k]... ← nhai dần, batch duration ổn định,
                                                     cluster sống, lag giảm dần đều
```

Đây là option quan trọng nhất của Kafka source trong production. Chỉnh nó sao cho batch duration thoải mái dưới trigger interval (nhìn tab Structured Streaming). Người anh em: `minOffsetsPerTrigger` (Spark 3.3+) gom đủ hàng mới chạy batch, đỡ đẻ batch tí hon.

### 3.4. Giải phẫu thư mục checkpoint

Đây là phần đáng giá nhất bài hôm nay. Checkpoint của một query trông thế này:

```
/workspace/data/chk/orders_bronze/
├── metadata                  ← JSON 1 dòng: {"id":"<query-uuid>"} — căn cước của query.
│                                Đổi checkpoint = đổi căn cước = query "mới tinh"
├── offsets/                  ← WRITE-AHEAD LOG — "TÔI ĐỊNH LÀM GÌ"
│   ├── 0                     ← batch 0 sẽ đọc đến các offset này
│   ├── 1
│   └── 2                     ← mỗi file: vài dòng metadata (batchWatermarkMs,
│                                batchTimestampMs, conf) + JSON:
│                                {"orders":{"0":31500,"1":30998}}
│                                nghĩa: batch này đọc ĐẾN offset 31500 (partition 0)...
├── commits/                  ← "TÔI ĐÃ LÀM XONG GÌ"
│   ├── 0                     ← file gần rỗng ({"nextBatchWatermarkMs":...}) —
│   └── 1                        sự TỒN TẠI của nó = batch đó đã commit trọn vẹn
│                                (chú ý: có offsets/2 mà KHÔNG có commits/2!)
├── sources/
│   └── 0/0                   ← điểm xuất phát của source #0 lúc query chạy lần đầu
│                                (startingOffsets đã được "vật chất hóa" ở đây —
│                                 vì thế đổi option sau này vô tác dụng)
└── state/                    ← state store của stateful ops (lesson 26 mổ tiếp)
    └── 0/<partition>/...
```

Nhìn cây trên: `offsets/` có batch 2 nhưng `commits/` chỉ đến 1 → **batch 2 đang dở dang** (hoặc chết giữa chừng). Đây chính là cách bạn "đọc hiện trường" khi stream chết.

### 3.5. Cơ chế resume sau restart — thuật toán 3 dòng

Khi query start với checkpoint có sẵn:

```
1. Đọc offsets/ tìm batch ID lớn nhất  → N
2. commits/N tồn tại?
   ├─ CÓ  → batch N xong rồi. Chốt batch N+1: hỏi Kafka offset mới nhất,
   │        ghi offsets/N+1, chạy tiếp như thường.
   └─ KHÔNG → batch N dở dang → CHẠY LẠI batch N với ĐÚNG khoảng offset
              đã ghi trong offsets/N (không chốt lại!) — cùng input, cùng batch id.
```

Vì batch được chạy lại với **y nguyên khoảng offset cũ**, kết quả tính ra giống hệt lần trước. Phần "ghi sink lần nữa có double không?" phụ thuộc sink có idempotent không — đó là miếng ghép cuối của exactly-once, để dành lesson 27. Hôm nay chốt được: **phía đọc, Spark đảm bảo không mất, không lệch một offset nào**.

### 3.6. Kafka sink — ghi ngược ra Kafka

Sink Kafka đọc các cột theo QUY ƯỚC TÊN: bắt buộc có `value` (string/binary), tùy chọn `key` (nên có — giữ message cùng key vào cùng partition, giữ thứ tự), tùy chọn `topic` (hoặc set option `topic` cố định):

```python
out = orders.select(
    F.col("order_id").alias("key"),
    F.to_json(F.struct("order_id", "amount", "status")).alias("value"))

q = (out.writeStream
     .format("kafka")
     .option("kafka.bootstrap.servers", "kafka:9092")
     .option("topic", "orders_enriched")
     .option("checkpointLocation", "/workspace/data/chk/orders_enriched")
     .start())
```

Lưu ý trung thực: Kafka sink là **at-least-once** — batch chạy lại (mục 3.5) sẽ produce lại message của batch đó. Downstream phải chịu được duplicate (dedup theo key, hoặc consumer idempotent).

### 3.7. failOnDataLoss

Chuyện đời thật: stream chết 10 ngày, Kafka retention 7 ngày → offset trong checkpoint trỏ vào dữ liệu **đã bị Kafka xóa**. Restart:

- `failOnDataLoss=true` (mặc định): query fail ngay, báo "Offsets out of range". Đau nhưng TRUNG THỰC — bạn biết mình mất dữ liệu.
- `failOnDataLoss=false`: nhảy tới offset cũ nhất còn sống, chạy tiếp, **lặng lẽ nuốt lỗ hổng dữ liệu**.

Production để `true`, xử lý sự cố có ý thức (backfill từ nguồn khác nếu cần). Set `false` chỉ khi đã cân nhắc và chấp nhận mất — kèm comment giải thích lý do trong code.

### 3.8. Consumer group của Spark — KHÔNG như consumer group thường

Consumer thường (Python client, Kafka Connect...): join một `group.id`, Kafka **điều phối** chia partition cho các thành viên, offset **commit vào Kafka** (topic `__consumer_offsets`), rebalance khi thành viên vào/ra.

Spark thì khác hẳn:

| | Consumer thường | Spark Structured Streaming |
|---|---|---|
| Ai chia partition cho ai | Kafka group coordinator | **Driver của Spark tự chia** partition→task |
| Offset lưu ở đâu | Kafka (`__consumer_offsets`) | **Checkpoint của Spark** (HDFS/S3/local) |
| group.id | Bạn đặt, cố định | Spark tự sinh unique mỗi query (`spark-kafka-source-<uuid>...`) |
| Rebalance | Có | Không có khái niệm này |

Vì sao Spark "chảnh" vậy? Vì offset phải được commit **nguyên tử cùng nhịp với tiến độ xử lý và state** — mà giao dịch đó nằm trong checkpoint, không thể nhờ Kafka giữ hộ một nửa. Hệ quả thực tế: (a) tool giám sát consumer lag theo group (Burrow, `kafka-consumer-groups.sh`) **không thấy** lag của Spark một cách đáng tin — phải đo lag từ metrics của Spark; (b) hai query Spark cùng đọc một topic không chia việc cho nhau như 2 consumer cùng group — mỗi query đọc TOÀN BỘ topic độc lập.

---

## 4. Internal

Ghép Kafka vào vòng đời micro-batch (lesson 23 mục 4), phóng to phần source:

```
① Trigger đến. Driver hỏi Kafka (AdminClient): "latest offset
   của từng partition topic `orders` là bao nhiêu?"
        │
② Driver tính khoảng đọc batch N:
   từ = offset kết thúc của batch N-1 (trong checkpoint)
   đến = latest, NHƯNG bị cắt bởi maxOffsetsPerTrigger
   (chia hạn mức cho các partition theo tỷ lệ tồn đọng)
        │
③ Ghi offsets/N (write-ahead log) — Ý ĐỊNH được chốt trước
        │
④ Lập kế hoạch task: MỖI KAFKA PARTITION → 1 TASK
   (topic 4 partition = tối đa 4 task đọc song song —
    muốn parallelism đọc cao hơn phải thêm partition Kafka,
    hoặc option minPartitions để Spark tự chẻ nhỏ khoảng offset)
        │
⑤ Executor chạy task: KafkaConsumer trên executor fetch đúng
   khoảng [from, to) được giao — không thừa không thiếu.
   Consumer được CACHE lại trên executor để batch sau khỏi
   tốn tiền bắt tay TCP/metadata lại từ đầu
        │
⑥ Xử lý (parse, transform...) → sink ghi
        │
⑦ Ghi commits/N → batch chốt sổ. Quay lại ① chờ trigger
```

Hai insight đọc chậm lại:

- **Số partition Kafka là TRẦN của parallelism đọc.** Topic 2 partition trên cluster 32 core = 30 core ngồi chơi ở bước đọc. Thiết kế số partition topic là quyết định chung giữa team Kafka và team Spark — đây là câu chuyện "partition là đơn vị song song hóa" (lesson 4) tái xuất ở tầng hạ tầng.
- **Offset được chốt ở driver TRƯỚC khi task chạy** → batch là deterministic: chạy lại lần nào cũng đúng khúc dữ liệu đó. So sánh với consumer thường "đọc được gì xử nấy" — không bao giờ replay chính xác được. Tính deterministic này là móng của exactly-once.

---

## 5. API

### Nạp connector — `--packages`

Image `apache/spark:3.4.1` KHÔNG kèm Kafka connector. Phải khai khi submit (version connector khớp version Spark, Scala 2.12):

```bash
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /workspace/labs/lab24/kafka_console.py
```

- **Pitfall #1**: quên → `Failed to find data source: kafka`. Gặp lỗi này là biết ngay thiếu package, đừng google mất 30 phút.
- **Pitfall #2**: lần đầu chạy sẽ tải jar từ Maven (cần mạng, mất ~1 phút) — jar được cache cho các lần sau.

### `readStream.format("kafka")` — option đáng nhớ

```python
.option("kafka.bootstrap.servers", "kafka:9092")  # mọi option kafka.* đi thẳng xuống Kafka client
.option("subscribe", "orders,payments")           # nhiều topic cách nhau dấu phẩy
.option("subscribePattern", "cdc\\..*")           # regex — topic mới khớp pattern TỰ ĐỘNG được đọc
.option("startingOffsets", "earliest")
.option("maxOffsetsPerTrigger", "100000")
.option("failOnDataLoss", "true")
.option("minPartitions", "16")                    # chẻ khoảng offset thành ≥16 task khi cần
```

- **Pitfall**: `subscribePattern` tiện cho CDC nhiều bảng, nhưng topic rác khớp pattern cũng bị hút vào — đặt convention tên topic nghiêm túc.

### `writeStream.format("kafka")`

- Cột theo quy ước: `value` bắt buộc, `key`/`topic`/`headers` tùy chọn.
- **Pitfall**: quên cột `key` → message rải round-robin, thứ tự per-key mất — chết dở với CDC (update trước insert sau!). Luôn set key = khóa nghiệp vụ.

### `query.lastProgress` — đo lag không cần tool ngoài

```python
p = query.lastProgress
p["sources"][0]["startOffset"] / p["sources"][0]["endOffset"] / p["sources"][0]["latestOffset"]
p["numInputRows"], p["inputRowsPerSecond"], p["processedRowsPerSecond"]
# lag ≈ latestOffset - endOffset (cộng theo từng partition)
```

- **Ý nghĩa**: vì Spark không commit offset vào Kafka, đây là nguồn sự thật để giám sát lag.

---

## 6. Demo nhỏ

Chưa cần bật Kafka — demo này mô phỏng chính xác nghi thức parse (chạy được bằng `make run-local`):

```
Input:  rate source → giả làm Kafka: value là JSON dạng BINARY
   ↓    cast binary → string → from_json với schema tường minh
Output: console — thấy JSON rác biến thành NULL chứ không nổ
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.appName("demo24").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Giả lập cột value binary như Kafka trả về (dòng chia hết cho 7 là JSON rác)
fake_kafka = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .withColumn("value", F.when(F.col("value") % 7 == 0, F.lit('{"oops":')
        ).otherwise(F.format_string('{"order_id":"o-%d","amount":%d}',
                                    F.col("value"), F.col("value") % 100))
        .cast("binary")))                     # ← binary, y như Kafka

schema = StructType([StructField("order_id", StringType()),
                     StructField("amount", LongType())])

parsed = (fake_kafka
    .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("is_garbage", F.col("order_id").isNull()))

q = (parsed.writeStream.format("console").outputMode("append")
     .trigger(processingTime="5 seconds").option("truncate", "false").start())
q.awaitTermination(45); q.stop(); spark.stop()
```

Quan sát: dòng rác ra `order_id=null, is_garbage=true` — im lặng, không exception. Trong production bạn sẽ rẽ nhánh dòng rác này vào "dead letter" thay vì để nó lặng lẽ trôi.

---

## 7. Production Example

Bronze ingestion của Project 2 (CDC Lakehouse) — chính là job bạn sẽ viết ở tuần 15, nhìn trước bản phác:

```
PostgreSQL → Debezium → Kafka topic cdc.public.orders (JSON envelope)
                              │
              Spark Structured Streaming (job này)
              • subscribePattern "cdc\\.public\\..*"  → nuốt mọi bảng CDC
              • maxOffsetsPerTrigger 500k             → sáng thứ 2 tồn 20M msg vẫn bình tĩnh nhai
              • failOnDataLoss=true                   → mất data phải BIẾT, không im
              • checkpoint trên storage BỀN (S3/HDFS) → container chết, checkpoint còn
              • trigger 1 min, append mode
                              │
              Iceberg bảng bronze (raw envelope + topic/partition/offset/timestamp)
```

Ba quyết định "rất production" đáng học:

1. **Bronze giữ nguyên envelope + metadata Kafka** (topic/partition/offset): offset là "số căn cước" để dedup và điều tra sự cố về sau — xóa đi là tự bịt mắt mình.
2. **Checkpoint nằm trên storage bền, cùng vòng đời với bảng**: checkpoint trên local disk của container là quả bom hẹn giờ — container recreate là mất trí nhớ, stream đọc lại từ `startingOffsets` như query mới.
3. **Chia để trị**: mỗi nhóm bảng một query/checkpoint riêng thay vì một query ôm cả trăm topic — bảng nào hỏng thì restart bảng đó, không kéo sập cả pipeline.

---

## 8. Hands-on Lab

**Mục tiêu**: đọc Kafka thật từ repo `../kafka-flink`, ghi Kafka sink, kill job và giải phẫu checkpoint.

### Bước 0 — nối mạng Spark ↔ Kafka

Hai cụm compose khác nhau = hai network Docker khác nhau. Nối container Spark vào network của Kafka:

```bash
cd ../kafka-flink && docker compose up -d kafka && cd -
docker network ls                                    # tìm network của kafka-flink (vd: kafka-flink_default)
docker network connect kafka-flink_default spark-mastery-spark-submit-1
docker network connect kafka-flink_default spark-mastery-spark-worker-1   # worker cũng cần thấy Kafka!
# Kiểm tra: docker exec spark-mastery-spark-submit-1 getent hosts kafka
```

Từ đây bootstrap servers là `kafka:9092` (tên service trong network). *(Không dựng được Kafka? Vẫn học được 80% bài: làm demo mục 6 + Bước 3 dưới đây với file sink — checkpoint anatomy giống hệt.)*

Tạo topic + bắn vài message mồi:

```bash
docker exec -it <kafka-container> kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab24 --partitions 3
docker exec -it <kafka-container> bash -c \
  'for i in $(seq 1 100); do echo "{\"order_id\":\"o-$i\",\"amount\":$((RANDOM%500))}"; done | \
   kafka-console-producer.sh --bootstrap-server localhost:9092 --topic lab24'
```

### Bước 1 — `labs/lab24/kafka_console.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.appName("lab24-kafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([StructField("order_id", StringType()),
                     StructField("amount", LongType())])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "lab24")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", "50")      # cố tình bé để thấy NHIỀU batch
       .load())

parsed = (raw.select(
            F.col("partition"), F.col("offset"),
            F.from_json(F.col("value").cast("string"), schema).alias("d"))
          .select("partition", "offset", "d.*"))

q = (parsed.writeStream.format("console")
     .outputMode("append")
     .option("checkpointLocation", "/workspace/data/chk/lab24")
     .trigger(processingTime="5 seconds")
     .start())
q.awaitTermination()
```

Chạy (nhớ `--packages` — Makefile `make run` không có sẵn nên gọi thẳng):

```bash
docker exec -it spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /workspace/labs/lab24/kafka_console.py
```

100 message + van 50/batch → thấy 2 batch có dữ liệu. Mở terminal khác produce thêm, nhìn batch mới xuất hiện.

### Bước 2 — kill & giải phẫu checkpoint (trái tim của lab)

1. Đang chạy, bấm **Ctrl+C** (giả lập chết đột tử).
2. Giải phẫu:

```bash
find data/chk/lab24 -type f | sort                 # (đường dẫn host tương ứng /workspace/data/chk/lab24)
cat data/chk/lab24/offsets/$(ls data/chk/lab24/offsets | sort -n | tail -1)   # JSON offset — đối chiếu mục 3.4
ls data/chk/lab24/commits/
```

3. Trả lời vào NOTES: batch lớn nhất trong `offsets/` là bao nhiêu? `commits/` có theo kịp không? Suy ra batch nào sẽ chạy lại khi restart?
4. **Restart** đúng lệnh cũ. Đối chiếu console: batch đầu sau restart có đúng khoảng offset bạn dự đoán không? Đổi thử `startingOffsets` thành `latest` rồi restart — có gì thay đổi không? (Không! Vì sao? — mục 3.2.)

### Bước 3 — Kafka sink: `labs/lab24/kafka_sink.py`

Đọc `lab24`, lọc `amount > 250`, ghi sang topic `lab24-big` với `key = order_id`, checkpoint riêng `/workspace/data/chk/lab24_sink`. Xác minh bằng `kafka-console-consumer.sh --topic lab24-big --from-beginning --property print.key=true`. Sau đó chạy:

```bash
docker exec -it <kafka-container> kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

Thấy group tên `spark-kafka-source-<uuid>...` — nhưng describe nó sẽ không cho bạn lag đáng tin. Đó là minh chứng sống cho mục 3.8.

### Bước 4 — quan sát UI

Tab **Structured Streaming** khi produce liên tục: Input Rate vs Process Rate, Batch Duration khi chỉnh `maxOffsetsPerTrigger` 50 → 5000. Ghi nhận xét vào `labs/lab24/NOTES.md`.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Kafka source trả về những cột nào? Vì sao `value` là binary và bước xử lý bắt buộc là gì?
2. `offsets/` và `commits/` khác nhau thế nào? Vì sao phải ghi offsets TRƯỚC khi chạy batch?
3. Spark có commit offset vào Kafka không? Lưu ở đâu? Hệ quả gì cho việc giám sát lag?

**Medium** — Kịch bản restart: checkpoint có `offsets/{0,1,2,3}` và `commits/{0,1,2}`. (a) Restart thì chuyện gì xảy ra với batch 3 — khoảng offset được chốt lại hay giữ nguyên? (b) Trước khi restart, bạn sửa `startingOffsets` từ `latest` thành `earliest` — có tác dụng gì không, vì sao? (c) Làm thí nghiệm trên lab để chứng minh cả hai câu trả lời, đính kèm nội dung file offset.

**Hard** — Bài toán tồn đọng: stream chết cuối tuần, tồn 10M message; cluster của bạn xử được ~40k msg/s; SLA yêu cầu lag về 0 trong 2 giờ VÀ batch duration ≤ 60s (trigger 60s). (a) Tính `maxOffsetsPerTrigger` thỏa cả 2 ràng buộc (hay không tồn tại?). (b) Nếu topic chỉ có 2 partition thì con số 40k msg/s còn đạt được không — option nào cứu được phần đọc? (c) Nghiệm lại bằng lab: produce 100k message, đặt van, đo số batch để hết lag qua `lastProgress`.

**Production Challenge** — Viết `labs/lab24/lag_monitor.py`: chạy query đọc `lab24` với sink `memory` hoặc console, và một vòng lặp Python mỗi 10s đọc `query.lastProgress`, in một dòng: `batchId, numInputRows, inputRowsPerSecond, processedRowsPerSecond, lag_ước_tính (latestOffset - endOffset tổng các partition)`. Đây là phôi thai của hệ thống alerting bạn sẽ xây ở Project 2 checkpoint 6.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Không đặt `maxOffsetsPerTrigger`, restart sau downtime dài | Thảm họa | Batch đầu nuốt toàn bộ tồn đọng — OOM hoặc batch chạy hàng giờ. LUÔN đặt van trong production. |
| Topic ít partition, cluster nhiều core | Lãng phí | Parallelism đọc bị trần bởi số partition Kafka. Chữa tạm bằng `minPartitions`; chữa gốc bằng thiết kế topic. |
| Checkpoint trên S3 | Mỗi batch chậm thêm | Mỗi batch ≥2 lượt ghi metadata (offsets + commits); object store latency cao làm trigger ngắn bị "thuế" nặng — nhìn `walCommit`/`commitOffsets` trong Operation Duration. |
| `from_json` trên JSON to, phức tạp | CPU-bound | Parse JSON là việc nặng nhất của bronze. Schema gọn (chỉ field cần), hoặc cân nhắc Avro + Schema Registry. |

---

## 11. Spark UI

Tab **Structured Streaming** với Kafka source:

- **Input Rate vs Process Rate**: lag đang phình hay đang xẹp — nhìn 5 giây là biết stream khỏe hay ốm.
- **Batch Duration**: sau khi chỉnh `maxOffsetsPerTrigger`, đồ thị này cho biết van của bạn vừa hay chưa — mục tiêu: ổn định và dưới trigger interval.
- **Operation Duration**: `addBatch` (xử lý thật) vs `latestOffset`/`walCommit` (nói chuyện với Kafka/checkpoint). addBatch nhỏ mà tổng lớn → nghẽn ở metadata I/O, không phải logic — đổi storage checkpoint chứ đừng tối ưu code vô ích.

Click một micro-batch bên tab **Jobs** → **Stages**: số task của stage đọc = số partition Kafka (hoặc `minPartitions`) — kiểm chứng mục 4 bằng mắt. Task duration lệch nhau nhiều? Một partition Kafka đang bị dồn message — **skew phiên bản streaming**, thường do produce key lệch.

---

## 12. Common Mistakes

1. **Quên `--packages` connector** → `Failed to find data source: kafka`. Lỗi chào sân của 100% người mới.
2. **Đổi `startingOffsets` rồi restart, mong đọc lại từ đầu** — checkpoint đè lên option. Muốn reprocess: checkpoint mới + kế hoạch xử lý double ở sink.
3. **Xóa checkpoint "cho sạch" khi gặp lỗi** — tương đương đốt sổ kế toán vì thấy số xấu: mất vị trí đọc, mất state, stream thành query mới tinh. Chẩn đoán trước, xóa là phương án cuối cùng có tính toán.
4. **Không đặt `maxOffsetsPerTrigger`** — chạy dev êm ru (data ít), lên production chết ngay lần downtime đầu tiên. Lỗi này không lộ khi test — càng nguy hiểm.
5. **`failOnDataLoss=false` dán khắp nơi để "hết lỗi"** — lỗi biến mất, dữ liệu cũng lặng lẽ biến mất. Sếp hỏi vì sao doanh thu tháng thiếu 2% thì không trả lời được.
6. **Ghi Kafka sink không có `key`** — mất ordering per-key; sự kiện `update` đến trước `insert` ở consumer. Với CDC đây là bug ăn thịt người.
7. **Hai query dùng chung một checkpoint** — corrupt cả hai. Mỗi query một thư mục, đặt tên theo query, quản lý như quản lý schema.

---

## 13. Interview

**Junior:**

1. *Đọc Kafka trong Structured Streaming cần gì và DataFrame trả về trông thế nào?* — format `kafka` + option bootstrap servers/subscribe + package spark-sql-kafka. Schema cố định 7 cột: key, value (đều binary), topic, partition, offset, timestamp, timestampType; payload phải cast value và parse (from_json...).
2. *`startingOffsets` earliest và latest khác nhau gì?* — Lần chạy đầu: earliest đọc từ message cũ nhất còn retention, latest chỉ nhận message mới sau khi start. Từ lần hai trở đi checkpoint quyết định, option bị bỏ qua.
3. *Checkpoint của streaming query chứa gì?* — metadata (id query), offsets/ (khoảng dữ liệu từng batch — ghi trước khi chạy), commits/ (đánh dấu batch xong), sources/ (điểm xuất phát), state/ (dữ liệu stateful ops).
4. *`maxOffsetsPerTrigger` để làm gì?* — Chặn trần số message mỗi micro-batch (chia theo partition), tránh batch khổng lồ sau downtime; là cơ chế backpressure chính của Kafka source.

**Mid:**

5. *Trình bày cơ chế resume sau restart.* — Tìm batch N lớn nhất trong offsets/; nếu commits/N có → N xong, chốt batch N+1 mới; nếu thiếu → chạy lại batch N với đúng khoảng offset đã ghi (không chốt lại). Nhờ ghi ý định trước (WAL) + đánh dấu hoàn thành sau, việc đọc là không mất, không lệch.
6. *Vì sao Spark không commit offset vào Kafka như consumer thường?* — Offset phải tiến/lùi nguyên tử cùng batch progress và state trong checkpoint; commit vào Kafka là tách đôi nguồn sự thật, mất khả năng replay chính xác. Spark tự sinh group.id, tự gán partition→task ở driver, không dùng group coordination/rebalance của Kafka.
7. *Hệ quả của việc đó cho monitoring và scaling?* — Lag không xem được đáng tin qua kafka-consumer-groups/Burrow → phải lấy từ lastProgress/metrics của Spark. Hai query Spark không chia partition với nhau như 2 consumer cùng group — mỗi query đọc độc lập toàn topic.
8. *`failOnDataLoss` nên đặt gì trong production, vì sao?* — `true`: khi offset checkpoint đã bị retention xóa, fail to và rõ còn hơn lặng lẽ nhảy qua lỗ hổng dữ liệu. `false` chỉ khi chủ đích chấp nhận mất (vd log ít quan trọng), có ghi chú lý do.

**Senior:**

9. *Thiết kế ingest 200 bảng CDC từ Kafka vào lakehouse — một query hay nhiều query? Trần parallelism nằm đâu?* — Không ôm 200 topic vào 1 query: một bảng lỗi schema kéo sập tất; checkpoint/state khổng lồ; không scale độc lập. Nhóm theo độ quan trọng/tải (vd 5–10 query, subscribePattern theo nhóm), mỗi query checkpoint riêng, van riêng. Trần parallelism đọc = tổng partition Kafka của nhóm — thỏa thuận số partition với team Kafka từ lúc thiết kế; minPartitions chỉ là băng cứu thương phía đọc, không tăng ordering hay throughput produce.
10. *Stream chết 10 ngày, retention 7 ngày. Kế hoạch khôi phục?* — (a) Nhận diện: restart sẽ fail vì offset out of range (failOnDataLoss=true đang bảo vệ ta). (b) Đánh giá lỗ hổng: khoảng offset/timestamp bị mất, đối chiếu nguồn gốc (DB nguồn còn data → backfill batch từ DB/snapshot cho khoảng thủng). (c) Nối lại stream: checkpoint mới với startingOffsets JSON/theo timestamp từ mép còn sống, HOẶC failOnDataLoss=false một lần có kiểm soát. (d) Dedup/reconcile ở silver theo khóa nghiệp vụ. (e) Rút kinh nghiệm: alert lag/liveness (đáng lẽ biết sau 10 phút chứ không phải 10 ngày), retention ≥ RTO. Trả lời có cả recovery lẫn prevention là dấu hiệu Senior.

---

## 14. Summary

### Mindmap

```
                     KAFKA × STRUCTURED STREAMING
                                │
    ┌───────────────┬───────────┴──────────┬─────────────────────┐
    ▼               ▼                      ▼                     ▼
 SOURCE          CHECKPOINT             SINK                 VẬN HÀNH
    │               │                      │                     │
 schema 7 cột    offsets/ = ý định      cột value/key/topic   maxOffsetsPerTrigger
 value=binary    commits/ = đã xong     key giữ ordering      (van backpressure)
 → cast+parse    sources/ = xuất phát   at-least-once         failOnDataLoss=true
 startingOffsets state/   = lesson 26   (dedup ở lesson 27)   lag đo từ lastProgress
 (chỉ lần đầu!)  resume: chạy lại                             group.id ≠ group thường
                 batch dở, đúng offset                        1 partition = 1 task (trần)
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 7 cột của Kafka source và viết đúng nghi thức cast + from_json không nhìn tài liệu.
- [ ] Giải thích được vì sao `startingOffsets` vô tác dụng khi checkpoint tồn tại.
- [ ] Vẽ lại cây thư mục checkpoint và vai trò từng thư mục con.
- [ ] Trình bày thuật toán resume 3 dòng (offsets có, commits thiếu → chạy lại đúng khoảng cũ).
- [ ] Đã kill job, đọc file offsets bằng mắt, restart và kiểm chứng dự đoán.
- [ ] Giải thích được consumer group của Spark khác consumer group thường ở 3 điểm.
- [ ] Biết đặt `maxOffsetsPerTrigger` và `failOnDataLoss` theo tư duy production.

---

## 15. Next Lesson

**Lesson 25 — Event time, watermark, late data.**

Hôm nay dữ liệu của bạn có cột `timestamp` (lúc message VÀO Kafka) — nhưng nghiệp vụ hỏi "doanh thu khung 9–10h" theo lúc đơn hàng **được đặt**. Hai đồng hồ đó lệch nhau, và dữ liệu đến trễ là luật chứ không phải ngoại lệ. Lesson 25 trả lời bộ ba câu hỏi trung tâm của streaming analytics: gom event vào **cửa sổ thời gian** thế nào (tumbling/sliding/session), chờ dữ liệu trễ **đến bao giờ** (watermark), và dữ liệu trễ quá hạn thì **số phận ra sao**. Đây cũng là chìa khóa mở lại bảng output mode: vì sao có watermark thì aggregation mới được dùng append.

Không có watermark, state của aggregation sẽ phình đến chết — nên trước khi học stateful (lesson 26), phải học cách cho state một hạn sử dụng.

> Gõ **"Continue"** khi sẵn sàng.
