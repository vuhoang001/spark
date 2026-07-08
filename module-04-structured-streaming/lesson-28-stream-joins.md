# Lesson 28 — Stream-stream & stream-static join

> Module 4 · Structured Streaming · Tuần 14 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Stream-static join**: enrich stream với dimension table — join "một bên chảy, một bên đứng yên".
- Sự thật về static side: nó có được **đọc lại mỗi micro-batch không**? (câu trả lời tinh tế hơn bạn nghĩ) và pattern **rebroadcast dimension hàng ngày**.
- **Stream-stream join**: tại sao phải giữ **state cả hai phía**, và state đó sống chết ra sao.
- Vì sao inner join stream-stream **bắt buộc** watermark + **time range condition** nếu không muốn state phình vô hạn; vì sao outer join còn **khắt khe hơn**.
- Cơ chế **state cleanup**: watermark quét state cũ lúc nào, và hệ quả "kết quả outer join bị trễ".
- **Ma trận join nào được hỗ trợ** — thuộc bảng này là né được 90% lỗi `AnalysisException` streaming.

Sau bài này bạn phải làm được:

- Viết stream-static join enrich đơn hàng với bảng seller, giải thích dimension được refresh lúc nào.
- Viết stream-stream join phát hiện fraud (orders × returns) với watermark + time bound đúng, chứng minh state không phình bằng Spark UI.
- Trả lời không do dự: "left outer stream-stream join cần điều kiện gì?"

Kiến thức dùng trong thực tế: enrich stream là nhu cầu số 1 của mọi pipeline realtime (đơn hàng cần tên khách, click cần thông tin campaign). Stream-stream join là vũ khí của fraud detection, attribution (click → purchase), monitoring (request → response). Làm sai watermark ở đây = job chạy êm 2 tuần rồi OOM lúc 3 giờ sáng.

---

## 2. Why

### Vấn đề 1: stream thô thì "mù"

Kafka topic `orders` chỉ có `seller_id`, không có tên seller, thành phố, rating. Dashboard cần "doanh thu theo thành phố seller" → phải join stream với bảng dimension `sellers` (nằm trong Iceberg, cập nhật vài lần/ngày). Đây là **stream-static join** — nhu cầu chiếm ~80% các phép join streaming ngoài đời.

### Vấn đề 2: hai dòng sự kiện cần "gặp nhau"

Bài toán fraud: khách đặt hàng (topic `orders`) rồi yêu cầu trả hàng (topic `returns`) chỉ sau vài phút — dấu hiệu lừa đảo chiếm dụng khuyến mãi. Muốn bắt realtime, phải join `orders` × `returns` theo `order_id`. Nhưng nghĩ mà xem:

```
orders:   ... O17(10:00) ──────────────────────────►  (chảy mãi)
returns:  ................... R17(10:06) ──────────►  (chảy mãi)
```

Khi `O17` đến lúc 10:00, bản ghi return khớp với nó **chưa tồn tại**. Spark buộc phải **giữ O17 lại trong state** để chờ. Chờ đến bao giờ? Nếu không ai nói cho Spark biết "quá X phút thì thôi", nó phải giữ **mọi order từ thuở khai thiên lập địa** → state = toàn bộ lịch sử = OOM. Đó là lý do watermark + time range condition không phải "khuyến nghị" mà là **điều kiện sống còn**.

### Nếu làm sai thì sao?

| Sai lầm | Hậu quả |
|---|---|
| Stream-stream inner join không watermark/time bound | Chạy được (!), state phình vô hạn, OOM sau vài ngày |
| Tưởng static side tự refresh khi dimension đổi | Enrich bằng data cũ hàng tuần mà không ai biết |
| Outer join thiếu watermark | `AnalysisException` — Spark thẳng tay từ chối |
| Watermark quá ngắn | Cặp match đến trễ bị bỏ lỡ → fraud lọt lưới |
| Watermark quá dài | State to, kết quả outer join trễ theo |

> Bài học Senior: ở batch, join sai thì kết quả sai — thấy ngay. Ở streaming, join sai kiểu "thiếu bound" thì **kết quả vẫn đúng** trong nhiều ngày, chỉ có state lặng lẽ phình. Lỗi loại này không nằm ở output, nằm ở **tài nguyên** — phải nhìn Spark UI mới thấy.

---

## 3. Theory

### 3.1. Stream-static join — bên tĩnh không cần state

```
                 micro-batch N của stream orders
                 ┌─────────────────────────┐
 Kafka orders ──►│ O91, O92, O93 (vài trăm │
                 │ row của batch này)      │──┐
                 └─────────────────────────┘  │    join theo seller_id
                                              ├──────────► output enriched
                 ┌─────────────────────────┐  │
 Iceberg      ──►│ sellers (3,095 rows —   │──┘
 (static)        │ đọc như DataFrame batch)│
                 └─────────────────────────┘
```

- **Stateless**: mỗi micro-batch là một phép join batch bình thường giữa "miếng stream nhỏ" và bảng tĩnh. Không state, không watermark, không chờ đợi. Rẻ.
- Static side thường nhỏ → Spark hay tự chọn **broadcast join** (lesson 16): bảng dimension được phát cho mọi executor, khỏi shuffle stream side.

**Câu hỏi kinh điển: static side có được re-read mỗi batch không?**

Câu trả lời đúng: **không được cam kết như bạn tưởng — nó được đọc theo plan của bảng tĩnh, và plan đó chốt những gì tùy nguồn**:

- Nguồn **file thuần (parquet/csv path)**: danh sách file được chốt khi query start. File mới thêm vào thư mục **không được nhìn thấy** cho đến khi restart query. Nội dung file bị overwrite giữa chừng → hên xui, thậm chí lỗi file not found.
- Nguồn **table format (Iceberg/Delta) qua catalog**: mỗi micro-batch thực thi lại plan, snapshot **mới nhất tại thời điểm batch chạy** thường được đọc (Delta cam kết rõ điều này; Iceberg tùy version — đừng đặt cược đời pipeline vào đó).
- Nguồn **JDBC**: mỗi batch chạy lại query JDBC → thấy data mới, nhưng mỗi batch đấm một phát vào DB nguồn — cẩn thận.

Kết luận hành xử của Senior: **coi như static side KHÔNG tự refresh, và thiết kế sự refresh một cách tường minh.** Hai pattern chuẩn:

```
Pattern 1 — refresh trong foreachBatch (đọc dimension mỗi batch, chủ động):
    def process(batch_df, batch_id):
        dim = spark.read.table("iceberg.dim.sellers")    # đọc TƯỜNG MINH mỗi batch
        batch_df.join(broadcast(dim), "seller_id")...
    → luôn mới, đổi lại mỗi batch tốn 1 lần đọc dim (dim nhỏ thì rẻ)

Pattern 2 — rebroadcast theo lịch (dimension to, đổi chậm):
    Giữ dim trong biến, kèm timestamp lần load; đầu mỗi batch nếu quá TTL
    (ví dụ qua 1AM hàng ngày) thì unpersist bản cũ, đọc + persist bản mới.
    → tiết kiệm I/O, chấp nhận dim cũ tối đa TTL.
    (Cách "cục súc" mà nhiều nơi dùng thật: restart query mỗi đêm bằng scheduler
     — restart là re-plan, ăn luôn cả schema mới của dim.)
```

### 3.2. Stream-stream join — state hai phía

```
              STATE STORE (trên executor, checkpoint hóa)
             ┌───────────────────────────────────────────┐
 orders  ───►│  orders state:  O15, O16, O17, ...        │
             │        ▲ mỗi order đến: ① dò returns state │
             │        │ tìm match  ② tự nộp mình vào state│
             │        │ ngồi chờ match tương lai          │
             │        ▼                                   │
 returns ───►│  returns state: R09, R12, R17, ...        │
             └───────────────────────────────────────────┘
                       │ watermark tiến lên → quét bỏ
                       ▼ record quá hạn khỏi CẢ HAI state
             output: các cặp (order, return) khớp nhau
```

Mỗi record đến từ **một** phía phải: (a) dò phía kia tìm match đã đến trước, (b) **tự lưu mình vào state** để chờ match đến sau. Vậy nên state tồn tại **cả hai phía** — khác hẳn stream-static (không state) và khác aggregation (state một bảng tổng).

### 3.3. Điều kiện để state không bất tử: watermark + time range condition

Với **inner join**, Spark cho phép bạn viết không watermark — nhưng đó là cái bẫy lịch sự. Để Spark **được phép xóa** một record khỏi state, nó phải chứng minh được "record này không thể match với bất kỳ record tương lai nào". Muốn vậy cần đủ 2 thứ:

```python
orders  = orders.withWatermark("order_ts", "30 minutes")     # ① watermark 2 phía
returns = returns.withWatermark("return_ts", "30 minutes")

joined = orders.join(
    returns,
    F.expr("""
        o_order_id = r_order_id AND
        return_ts BETWEEN order_ts AND order_ts + interval 15 minutes
    """))                                                    # ② time range condition
```

- **① Watermark** trên event-time của cả hai stream: "data trễ hơn X thì coi như không đến nữa" (lesson 25).
- **② Time range condition** (hoặc window join): ràng buộc **thời gian hai phía cách nhau tối đa bao lâu**. Không có nó, một order 3 tuần trước về lý thuyết vẫn match được return hôm nay → Spark không dám xóa gì.

Từ 2 thứ này Spark **suy ra TTL của state từng phía**. Trực giác: return chỉ match order trong quá khứ ≤15 phút, watermark trễ 30 phút → order cần được giữ ~45 phút là an toàn tuyệt đối; return chỉ cần giữ ~30 phút (chỉ match order đến trước nó, cộng độ trễ watermark). Watermark tiến tới đâu, state cũ hơn ngưỡng bị quét tới đó.

### 3.4. Outer join — khắt khe hơn, và kết quả bị trễ có chủ đích

Inner join: thiếu watermark chỉ chết state. **Outer join: thiếu watermark + time bound là `AnalysisException` ngay** — vì outer join phải trả lời câu "record này CHẮC CHẮN không có match" để phát row kèm NULL. Trong stream, "chắc chắn không có match" chỉ tồn tại khi có deadline — deadline đó chính là watermark + time bound.

Hệ quả quan trọng mà junior hay ngã ngửa:

```
O17 đến lúc 10:00, không có return nào match.
Left outer join KHÔNG phát (O17, NULL) lúc 10:01.
Nó phát khi WATERMARK vượt qua 10:15 (hết time bound + độ trễ watermark)
→ tức khoảng 10:45+ theo đồng hồ event time, và chỉ khi CÓ DATA MỚI
  đẩy watermark tiến lên (stream im ắng = watermark đứng = kết quả treo).
```

Kết quả NULL-side của outer join **luôn trễ** = time bound + watermark delay. Đây không phải bug — là cái giá logic của "khẳng định điều không xảy ra". Đặt watermark càng dài, fraud càng khó lọt nhưng cảnh báo càng trễ — trade-off nghiệp vụ, không phải kỹ thuật.

### 3.5. Ma trận hỗ trợ join (Spark 3.4) — học thuộc

| Left \ Right | Static | Stream |
|---|---|---|
| **Static** | join batch thường | Inner ✔ · Left outer ✘ · **Right outer ✔** · Full ✘ |
| **Stream** | Inner ✔ · **Left outer ✔** · Right outer ✘ · Full ✘ (không cần watermark) | Inner ✔ (watermark khuyến nghị mạnh) · Left/Right outer ✔ (bắt buộc watermark + time bound) · Full outer ✔ (bắt buộc, từ 3.1) · **Left semi ✔** |

Quy tắc nhớ nhanh thay vì học vẹt: **phía outer (phía được giữ lại toàn bộ) phải là phía stream** — vì Spark không thể chờ "hết" một stream để biết static row nào không có match... nhưng ngược lại với static row cũng vậy: stream không bao giờ "hết", nên `stream LEFT OUTER static` được (mỗi record stream tự biết mình có match hay không ngay trong batch), còn `static LEFT OUTER stream` thì không (bao giờ mới dám kết luận một seller "không có order nào"?).

Cấm khác cần biết: sau join stream-stream chỉ được nối tiếp bằng các phép được hỗ trợ ở append mode; aggregation nối sau join có ràng buộc riêng (chuỗi stateful operators — Spark 3.4 mới nới lỏng một phần, kiểm tra kỹ trước khi thiết kế).

---

## 4. Internal

Stream-stream join chạy trên **StreamingSymmetricHashJoinExec** — mổ xẻ một micro-batch:

```
① Batch N có: 200 orders mới, 80 returns mới (đã chia partition
   theo hash(order_id) — CẢ HAI stream shuffle về CÙNG partitioning
   → cặp khớp nhau chắc chắn nằm cùng partition, cùng executor)
        │
② Trên mỗi partition, với MỖI order mới:
   - probe returns-state tìm match theo key + time range → phát output
   - append chính nó vào orders-state (kèm event time)
   Với MỖI return mới: đối xứng ngược lại (vì thế gọi là SYMMETRIC hash join)
        │
③ Tính watermark mới = min(watermark 2 stream)   ← chú ý: lấy MIN,
   stream nào tụt lại kéo lùi watermark chung của query
        │
④ State eviction: quét record có event time < ngưỡng suy ra từ
   watermark + time bound → xóa khỏi state
   (outer join: record bị evict mà chưa từng match → PHÁT (row, NULL) lúc này)
        │
⑤ Ghi state delta vào checkpoint (state store versioning — lesson 26)
```

Ba hệ quả thực chiến:

- **Shuffle mỗi batch**: 2 stream cùng shuffle theo key join. `spark.sql.shuffle.partitions` chốt số partition state **vĩnh viễn theo checkpoint** — đổi con số này giữa chừng là không được với query stateful. Chọn trước, chọn kỹ.
- **Watermark = min hai phía**: topic returns ế ẩm (ít message) → watermark của nó đứng im → watermark chung đứng im → state orders không được dọn dù orders chảy ào ào. Thực tế người ta khắc phục bằng cách đảm bảo topic thưa vẫn có nhịp event (hoặc chấp nhận cấu hình idle timeout ở tầng nghiệp vụ).
- **State nằm trong executor memory** (HDFSBackedStateStore mặc định) — state to thì GC khóc. RocksDB state store (lesson 26) đẩy xuống disk, là lựa chọn mặc định đúng cho join state lớn.

---

## 5. API

### `stream.join(static_df, on, how)` — stream-static

```python
sellers = spark.read.table("iceberg.dim.sellers")          # static
enriched = orders_stream.join(F.broadcast(sellers), "seller_id", "inner")
```

- **Ý nghĩa**: join stateless mỗi micro-batch; `broadcast()` gợi ý phát dimension đi mọi executor.
- **Pitfall**: nhớ ma trận — `orders_stream.join(sellers, ..., "left")` được, nhưng `sellers.join(orders_stream, ..., "left")` (static bên trái giữ toàn bộ) thì KHÔNG.

### `withWatermark(eventTimeCol, delay)` — trên CẢ HAI stream trước khi join

```python
orders  = orders.withWatermark("order_ts", "30 minutes")
returns = returns.withWatermark("return_ts", "30 minutes")
```

- **Pitfall**: watermark phải khai trên **cột event time xuất hiện trong time range condition**. Khai xong mà điều kiện join không ràng buộc thời gian → inner join vẫn không dọn được state.

### Time range condition trong `join(..., F.expr(...))`

```python
joined = orders.alias("o").join(
    returns.alias("r"),
    F.expr("""
        o.order_id = r.order_id
        AND r.return_ts BETWEEN o.order_ts AND o.order_ts + interval 15 minutes
    """),
    "leftOuter")
```

- **Ý nghĩa**: ràng buộc khoảng cách event time hai phía — nguồn suy ra TTL state.
- **Pitfall**: viết điều kiện thời gian bằng cột **processing time hoặc cột không watermark** → không có tác dụng dọn state. Và interval phản ánh **nghiệp vụ** (return hợp lệ trong 15 phút?) chứ không phải con số bốc thuốc.

### Đọc 2 topic Kafka

```python
def read_topic(topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest").load())
```

Mỗi stream một `readStream` riêng — đừng `subscribe` 2 topic vào 1 stream rồi filter tách (được về logic nhưng schema value khác nhau sẽ hành bạn, và mất khả năng tune riêng từng source).

---

## 6. Demo nhỏ

Xem inner join stream-stream giữ record chờ match bằng 2 rate stream:

```
Input:  rate stream A (5 rows/s), rate stream B (5 rows/s, làm trễ 10s)
   ↓    join A.value = B.value trong khoảng 30s
Output: cặp (value, tsA, tsB) — value của A phát ra chỉ khi B "đuổi kịp"
```

```python
# labs/lab28/demo_ss_join.py
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("demo28").master("local[2]").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "4")

a = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
     .select(F.col("value").alias("a_val"), F.col("timestamp").alias("a_ts"))
     .withWatermark("a_ts", "10 seconds"))

b = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
     .select((F.col("value")).alias("b_val"),
             (F.col("timestamp") - F.expr("interval 10 seconds")).alias("b_ts"))
     .withWatermark("b_ts", "10 seconds"))

j = a.join(b, F.expr("""
        a_val = b_val AND b_ts BETWEEN a_ts - interval 30 seconds
                                   AND a_ts + interval 30 seconds"""))

q = (j.writeStream.format("console").option("truncate", False)
     .option("checkpointLocation", "/tmp/ckpt_demo28")
     .trigger(processingTime="5 seconds").start())
q.awaitTermination()
```

Chạy `make run-local F=labs/lab28/demo_ss_join.py`. Quan sát: batch đầu ít cặp (B "trễ" 10s nên A phải ngồi state chờ), sau đó nhịp nhàng. Mở UI :4040 → tab Structured Streaming → **Aggregated Number Of Total State Rows**: state tăng rồi **đi ngang** (watermark dọn kịp). Thử xóa `withWatermark` cả 2 phía + bỏ time bound → state rows thành đường thẳng đi lên mãi — chụp lại 2 đồ thị để so.

---

## 7. Production Example

Hệ thống chống gian lận khuyến mãi của sàn TMĐT (mô phỏng theo kiến trúc repo `kafka-flink`):

```
 app orders ──► Kafka "orders" ──┐
                                 ├─► Spark Structured Streaming
 app returns ─► Kafka "returns" ─┘        │
                                          ├─ orders JOIN returns (stream-stream,
                                          │   return trong vòng 15' sau order,
                                          │   watermark 30')  → nghi án fraud
                                          ├─ JOIN dim.customers (stream-static,
                                          │   rebroadcast 1AM) → gắn risk_score
                                          ▼
                       foreachBatch → MERGE INTO iceberg.risk.fraud_alerts
                                          │
                                          ▼
                              Trino → dashboard đội risk (SLA: alert < 5 phút)
```

Quyết định thiết kế:

1. **Inner join cho luồng alert chính** (chỉ quan tâm cặp khớp) — state gọn, latency thấp. **Left outer chạy song song** đổ ra bảng khác cho bài toán "order lớn KHÔNG có return" phục vụ phân tích — chấp nhận kết quả trễ ~45 phút, vì bản chất outer là phải chờ hết hy vọng match.
2. Watermark 30 phút được chọn từ **số liệu đo thực tế** p99 độ trễ event, không bốc thuốc. Trade-off được ghi thành văn: "return đến trễ hơn 30' sẽ lọt — đội risk chấp nhận, có batch job đối soát cuối ngày quét lại."
3. Sink là **MERGE theo alert key** — bài lesson 27 áp dụng nguyên con: stream-stream join + kill/restart vẫn không nhân đôi alert.
4. RocksDB state store vì join state ~ hàng triệu order đang "mở".

---

## 8. Hands-on Lab

**Mục tiêu**: orders × returns tìm fraud (stream-stream), rồi enrich seller (stream-static). Hạ tầng: như lab 27 (đã nối network với `kafka-flink`).

### Bước 1 — tạo topic + generator

```bash
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic lab28-orders  --partitions 3 --replication-factor 1
docker exec broker kafka-topics --bootstrap-server broker:29092 --create --topic lab28-returns --partitions 3 --replication-factor 1
```

Viết `labs/lab28/gen_events.py` (chạy bằng python thường trên máy host, cần `pip install kafka-python`, bootstrap `localhost:9092`): mỗi giây phát 1 order `{"order_id","seller_id","amount","order_ts"}`; với xác suất 20% phát thêm return cho order đó sau 5–600 giây `{"order_id","reason","return_ts"}` — trong đó ~5% return đến trong vòng 60 giây (fraud giả lập). Tự viết — đây cũng là bài tập.

### Bước 2 — `labs/lab28/fraud_join.py`

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (SparkSession.builder.appName("lab28-fraud")
         .config("spark.sql.shuffle.partitions", "6")   # chốt TRƯỚC khi có checkpoint!
         .getOrCreate())

order_schema = StructType([
    StructField("order_id", StringType()), StructField("seller_id", StringType()),
    StructField("amount", DoubleType()),   StructField("order_ts", TimestampType())])
return_schema = StructType([
    StructField("order_id", StringType()), StructField("reason", StringType()),
    StructField("return_ts", TimestampType())])

def read_json_topic(topic, schema, alias):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "broker:29092")
           .option("subscribe", topic).option("startingOffsets", "earliest").load())
    return raw.select(F.from_json(F.col("value").cast("string"), schema).alias(alias)) \
              .select(f"{alias}.*")

orders  = read_json_topic("lab28-orders", order_schema, "o") \
              .withWatermark("order_ts", "10 minutes")
returns = read_json_topic("lab28-returns", return_schema, "r") \
              .withColumnRenamed("order_id", "r_order_id") \
              .withWatermark("return_ts", "10 minutes")

# FRAUD = return đến trong vòng 2 phút sau order
frauds = orders.join(
    returns,
    F.expr("""
        order_id = r_order_id
        AND return_ts BETWEEN order_ts AND order_ts + interval 2 minutes
    """))

q = (frauds.select("order_id", "seller_id", "amount", "order_ts", "return_ts", "reason")
     .writeStream.format("console").option("truncate", False)
     .option("checkpointLocation", "/workspace/labs/lab28/ckpt_fraud")
     .trigger(processingTime="10 seconds").start())
q.awaitTermination()
```

Chạy bằng spark-submit với `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1` (xem lesson 27 section 5).

### Bước 3 — thêm tầng stream-static

Nâng cấp: join `frauds` với bảng sellers. Nếu chưa có bảng Iceberg dimension, tạo nhanh từ CSV Olist:

```python
# labs/lab28/build_dim.py — chạy 1 lần (cần --packages Iceberg như lab 27)
sellers = spark.read.csv("/workspace/../data/olist_sellers_dataset.csv", header=True)
sellers.writeTo("iceberg.lab28.dim_sellers").createOrReplace()
```

Trong `fraud_join.py`, đổi sink thành foreachBatch và enrich **bên trong** (Pattern 1 — dimension luôn mới):

```python
def enrich_and_write(batch_df, batch_id):
    dim = batch_df.sparkSession.read.table("iceberg.lab28.dim_sellers")
    out = batch_df.join(F.broadcast(dim), "seller_id", "left")
    out.write.format("console").save()   # hoặc MERGE INTO bảng alerts (bài tập)
```

### Bước 4 — quan sát state (phần quan trọng nhất)

1. UI :4040 → **Structured Streaming** → query → đồ thị **Total State Rows** và **Rows Dropped By Watermark**: state phải đi ngang sau khi ổn định.
2. Sửa watermark 10 phút → `10 hours`, chạy lại với checkpoint MỚI, so đồ thị state rows.
3. Tắt generator returns (chỉ bơm orders) 5 phút: watermark chung đứng im (min 2 phía!) → state orders ngừng được dọn. Bật lại generator → quan sát cú "xả" state. Ghi nhận xét vào `labs/lab28/NOTES.md`.

---

## 9. Assignment

**Easy** —
1. Ma trận join: không nhìn tài liệu, viết lại bảng support (static×stream, stream×stream, các kiểu inner/left/right/full). Giải thích bằng lời vì sao `static LEFT OUTER stream` bị cấm còn `stream LEFT OUTER static` thì không.
2. Trong lab, inner join phía nào giữ state? (Cả hai — giải thích vì sao mỗi phía đều phải chờ phía kia.)

**Medium** — State cleanup: với watermark 10 phút và time bound `return_ts BETWEEN order_ts AND order_ts + 2 minutes`, tính (trên giấy) một order được giữ trong state **tối đa bao lâu** và một return tối đa bao lâu. Kiểm chứng bằng đồ thị state rows. Sau đó thiết kế cho yêu cầu mới "join trong 7 ngày, dọn state cũ hơn 7 ngày": watermark/time bound đặt bao nhiêu, state ước tính bao nhiêu row nếu 1M order/ngày, và cấu hình state store nào phù hợp?

**Hard** — Stream-static với dimension đổi mỗi ngày: cài đặt Pattern 2 (rebroadcast daily 1AM) hoàn chỉnh trong foreachBatch — biến toàn cục giữ `(dim_df, loaded_date)`, đầu batch kiểm tra sang ngày mới thì unpersist + reload + persist. Chứng minh nó hoạt động: UPDATE một seller city trong bảng dim khi stream đang chạy, cho thấy enrich chỉ đổi sau "1AM" giả lập (hạ TTL xuống 2 phút để test). So sánh chi phí với Pattern 1 bằng thời gian batch trên UI.

**Production Challenge** — Left outer join orders × returns để tìm "order KHÔNG bị return trong 2 phút" (khách hàng tốt). Chạy và trả lời: (a) row NULL-side xuất hiện sau bao lâu so với order_ts? đo thực tế; (b) điều gì xảy ra với các row NULL-side nếu stream ngừng nhận data mới? (c) từ đó viết 5 dòng cảnh báo cho team về việc dùng outer join stream-stream trong luồng có SLA.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Stream-static + broadcast dim nhỏ | Nhanh | Không shuffle stream side, không state. Đây là lý do luôn cân nhắc "nghiệp vụ này có thật sự cần stream-stream không, hay static đủ?" |
| Stream-stream join | Đắt kép | Shuffle CẢ HAI stream mỗi batch + đọc/ghi state + checkpoint state. Chi phí ~ agg × 2. |
| Watermark dài (giờ/ngày) | State to | TTL state tỷ lệ thuận watermark + time bound. 7 ngày × 1M order/ngày = 7M row state → bắt buộc RocksDB. |
| `spark.sql.shuffle.partitions` mặc định 200 với state bé | Lãng phí | 200 state store instance, mỗi cái vài chục row + overhead checkpoint từng cái. Chốt con số hợp lý TRƯỚC lần chạy đầu (không đổi được sau khi có checkpoint). |
| Dimension đọc lại mỗi batch (Pattern 1) | Cộng chi phí đọc/batch | Dim 5k row trên Iceberg: không đáng kể. Dim 50M row: chuyển Pattern 2 hoặc đưa dim vào state (mapGroupsWithState — ngoài phạm vi bài). |
| Join rồi mới filter thời gian ở output | Chậm + state to | Đưa ràng buộc thời gian VÀO điều kiện join để Spark dùng nó dọn state — filter sau join không giúp state. |

---

## 11. Spark UI

Tab **Structured Streaming** → click query — hôm nay tập trung 3 đồ thị:

- **Aggregated Number Of Total State Rows**: chỉ số sống còn của join stream-stream. Đi ngang = watermark dọn kịp; dốc lên đều = thiếu bound/watermark hỏng → OOM đếm ngược.
- **Aggregated Number Of Rows Dropped By Watermark**: event trễ bị loại. Tăng bất thường = watermark quá chặt so với độ trễ thực → đang MẤT data lặng lẽ.
- **Operation Duration**: thời gian từng pha, để mắt phần state (commit/get) — RocksDB vs memory khác nhau rõ ở đây.

Tab **SQL** → mở plan một batch: tìm node `StreamingSymmetricHashJoin` — xem số output rows, số state rows từng phía. Tab **Stages**: thấy 2 nhánh shuffle (mỗi stream một exchange) hội tụ vào join — đúng như internal section 4.

Watermark hiện tại của query: xem `query.lastProgress["eventTime"]["watermark"]` — in nó ra log định kỳ là trick monitoring rẻ tiền mà hiệu quả.

---

## 12. Common Mistakes

1. **Inner join stream-stream không watermark/time bound** — chạy êm, đúng kết quả, và là quả bom hẹn giờ OOM. Spark không chặn bạn (inner join hợp lệ mà) — tự bạn phải chặn mình.
2. **Chỉ watermark một phía** rồi tưởng đủ — watermark chung = min hai phía, phía thiếu coi như âm vô cùng → không dọn được gì.
3. **Quên rằng một stream ế làm watermark cả query đứng im** — topic returns không có message mới = state orders không được dọn + outer join không phát NULL-side. Không phải bug của Spark, là tính chất của min().
4. **Đặt điều kiện thời gian ngoài join** (`.filter` sau join) — về kết quả tương đương, về state là vô dụng. Ràng buộc phải nằm TRONG join condition.
5. **`static.join(stream, "left")`** — AnalysisException. Đọc lại quy tắc "phía outer phải là stream" (với static-stream: outer side phải là stream side).
6. **Kỳ vọng outer join phát NULL-side ngay** — nó phát khi watermark vượt deadline, trễ hàng chục phút là bình thường. Thiết kế SLA phải cộng độ trễ này.
7. **Đổi `spark.sql.shuffle.partitions` sau khi query stateful đã có checkpoint** — state đã phân mảnh theo số cũ, đổi số là lỗi/kết quả sai. Muốn đổi: checkpoint mới + chiến lược backfill.
8. **Tin rằng static parquet path tự thấy file mới** — không. Refresh dimension phải tường minh (Pattern 1/2) hoặc restart.

---

## 13. Interview

**Junior:**

1. *Stream-static join khác stream-stream join thế nào?* — Stream-static: mỗi micro-batch join miếng stream với bảng tĩnh, stateless, không watermark. Stream-stream: hai phía đều chảy, record phải chờ match tương lai → state cả hai phía, cần watermark + time bound để dọn.
2. *Vì sao stream-stream join cần giữ state cả hai phía?* — Record phía A đến trước match phía B (và ngược lại) — mỗi record vừa dò phía kia vừa tự lưu mình chờ. Không giữ một phía = mất các cặp mà phía đó đến trước.
3. *Watermark trong join để làm gì?* — Cho Spark cơ sở khẳng định "record cũ hơn ngưỡng không thể có match mới" → xóa khỏi state (chống phình) và, với outer join, dám phát row NULL-side.
4. *Enrich stream đơn hàng với bảng khách hàng dùng join gì?* — Stream-static (stream trái, static phải), thường broadcast dimension. Chỉ dùng stream-stream khi cả hai nguồn đều là dòng sự kiện.

**Mid:**

5. *Inner join stream-stream không watermark có chạy được không? Hậu quả?* — Chạy được, kết quả đúng, nhưng Spark phải giữ toàn bộ lịch sử hai stream trong state → memory tăng vô hạn. Là lỗi tài nguyên chứ không phải lỗi kết quả — chỉ lộ trên Spark UI (state rows tăng đều) hoặc khi OOM.
6. *Time range condition đóng vai trò gì bên cạnh watermark?* — Watermark nói "data trễ tối đa bao nhiêu", time bound nói "hai phía match cách nhau tối đa bao nhiêu". Spark kết hợp cả hai để suy TTL từng phía state. Thiếu time bound thì dù có watermark, một record vẫn có thể match record tương lai xa bất kỳ → không dọn được.
7. *Left outer stream-stream: khi nào row (left, NULL) được phát?* — Khi watermark vượt qua thời hạn match tối đa của record đó (event time + time bound + watermark delay) mà chưa có match — tức bị evict khỏi state. Hệ quả: NULL-side luôn trễ, và stream im ắng thì watermark đứng, kết quả treo.
8. *Static side trong stream-static join có được refresh không?* — Không cam kết chung: file path chốt danh sách file lúc start; Delta/Iceberg qua catalog thường đọc snapshot mới mỗi batch nhưng tùy connector/version; JDBC query lại mỗi batch. Chuẩn production: refresh tường minh — đọc dim trong foreachBatch, rebroadcast theo TTL, hoặc restart theo lịch.

**Senior:**

9. *Thiết kế join orders × returns window 7 ngày, 5M orders/ngày. Phân tích state và lựa chọn của bạn.* — State orders ~ 5M × 7 = 35M row (+ returns nhỏ hơn) → HDFSBackedStateStore (heap) không gánh nổi → RocksDB state store. Shuffle partitions chốt từ đầu đủ lớn (state/partition hợp lý). Cân nhắc kiến trúc thay thế: window 7 ngày có thật cần stream-stream không — hay stream returns join với BẢNG orders đã ghi xuống Iceberg (stream-static, orders là static side được cập nhật liên tục bởi pipeline khác) → state gần như 0, đổi lấy độ trễ dữ liệu orders. Senior phải nêu được phương án số 2 — không phải bài toán nào trông giống stream-stream cũng nên giải bằng stream-stream.
10. *Watermark chung của query lấy min các stream — hệ quả vận hành và cách xử lý?* — Một nguồn thưa/tắc kéo watermark cả query đứng: state không dọn, outer/agg không phát kết quả. Xử lý: giám sát watermark qua lastProgress và alert khi nó tụt xa processing time; đảm bảo mọi topic có nhịp sự kiện (heartbeat event từ producer — chính là trick heartbeat.interval.ms của Debezium); thiết kế chấp nhận trễ cho nguồn thưa; đường cùng là tách query. Trả lời tốt phải nói được "đây là lựa chọn đúng của Spark về correctness (thà trễ còn hơn sai), gánh nặng chuyển sang vận hành".

---

## 14. Summary

### Mindmap

```
                        STREAM JOINS (L28)
                              │
      ┌───────────────┬──────┴─────────┬──────────────────┐
      ▼               ▼                ▼                  ▼
 STREAM-STATIC   STREAM-STREAM     ĐIỀU KIỆN SỐNG     MA TRẬN SUPPORT
      │               │                │                  │
 stateless        state 2 PHÍA     watermark 2 stream  outer side phải
 join mỗi batch   (symmetric       + time range         là stream
 broadcast dim     hash join)        trong JOIN cond   full outer: chỉ
 KHÔNG tự refresh watermark=min    → suy ra TTL state   stream-stream
 → Pattern 1:      2 phía          outer join: bắt      (có watermark)
   đọc trong      1 stream ế =     buộc + kết quả      static LEFT
   foreachBatch    cả query kẹt    NULL-side TRỄ        stream = cấm
 → Pattern 2:
   rebroadcast TTL
```

### Checklist trước khi gõ "Continue"

- [ ] Giải thích được vì sao stream-stream join cần state hai phía, còn stream-static thì không.
- [ ] Viết được join có watermark + time range condition và TÍNH được TTL state từ hai tham số đó.
- [ ] Trả lời được "static side có tự refresh không" đúng sắc thái, kèm 2 pattern refresh.
- [ ] Thuộc ma trận join support và quy tắc "phía outer phải là stream".
- [ ] Đã nhìn đồ thị state rows đi ngang vs dốc lên trên UI, và tận mắt thấy 1 stream ế làm watermark kẹt.
- [ ] Hiểu vì sao kết quả outer join bị trễ có chủ đích.
- [ ] Trả lời 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Lesson 29 — CDC pattern hoàn chỉnh: Debezium envelope, MERGE INTO.**

Bạn đã có đủ binh khí: đọc Kafka (L24), watermark (L25), state (L26), exactly-once + foreachBatch + MERGE (L27), join & enrich (L28). Giờ là lúc ghép tất cả vào bài toán "ăn tiền" nhất của Data Engineer hiện đại: **đồng bộ database production về lakehouse theo thời gian thực**. Lesson 29 mổ xẻ Debezium envelope đến từng field (before/after/op/lsn), dạy bạn dedup đúng thứ tự sự kiện, và viết câu MERGE INTO xử lý cả insert/update/delete trong một nhát — nền móng trực tiếp cho Project 2.

> Gõ **"Continue"** khi sẵn sàng.
