# Lesson 25 — Event time, watermark, late data

> Module 4 · Structured Streaming · Tuần 13 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Event time vs processing time** — hai đồng hồ luôn lệch nhau, và vì sao aggregation nghiêm túc phải theo event time.
- **Windowed aggregation**: tumbling / sliding / session window với `F.window` và `F.session_window`.
- **Watermark** — hợp đồng "chờ dữ liệu trễ đến bao giờ": công thức `watermark = max event time đã thấy − threshold`, và cách nó **giải phóng state**.
- Vị trí BẮT BUỘC của `withWatermark` (trước groupBy), số phận của late data quá hạn.
- Chọn **allowed lateness** cho từng nghiệp vụ: IoT sensor / user click / payment.
- Vì sao watermark mở khóa **append mode** cho aggregation (trả món nợ từ lesson 23).

Sau bài này bạn phải làm được:

- Vẽ timeline watermark di chuyển và chỉ ra event nào được nhận, event nào bị bỏ.
- Viết windowed aggregation 10 phút + watermark 15 phút đúng cú pháp, đúng thứ tự.
- Trả lời: "watermark 10 phút nghĩa là kết quả trễ 10 phút à?" — (KHÔNG, và cắt nghĩa được).

Kiến thức dùng trong thực tế: mọi câu hỏi dạng "X theo khung 5 phút" — doanh thu, lượt click, sensor trung bình — đều là bài này. Project 3 (Clickstream) và Project 4 (Fraud) đứng trên nó.

---

## 2. Why

### Hai đồng hồ, một sự thật

Mỗi event có hai mốc thời gian:

- **Event time**: lúc sự việc **xảy ra** — nằm trong payload (`order_purchase_timestamp`, lúc sensor đo, lúc user bấm).
- **Processing time**: lúc Spark **nhìn thấy** nó — đồng hồ của cluster.

Và chúng lệch nhau vì đời không hoàn hảo:

```
 EVENT TIME (lúc xảy ra)                PROCESSING TIME (lúc Spark thấy)
 ───────────────────────               ─────────────────────────────────
 09:58  đơn A đặt  ────────────────▶   09:58:20   (mạng ngon, đến ngay)
 09:59  đơn B đặt  ──── 3G chập ───▶   10:04:00   (đến TRỄ 5 phút)
 10:01  đơn C đặt  ────────────────▶   10:01:15
 09:57  đơn D đặt  ─ app offline ──▶   10:30:00   (điện thoại vào hầm gửi xe,
                                                   online lại mới bắn — trễ 33 phút!)
```

Câu hỏi nghiệp vụ: *"doanh thu khung 9–10h là bao nhiêu?"* Đơn B và D **thuộc khung 9–10h** dù Spark thấy chúng sau 10h. Aggregate theo processing time thì con số của bạn phụ thuộc vào... chất lượng sóng 3G của khách — chạy lại pipeline vào ngày khác ra số khác. Aggregate theo event time thì kết quả đúng và **tái lập được**.

### Nhưng event time đẻ ra một câu hỏi không có đáp án hoàn hảo

Nếu chấp nhận dữ liệu trễ, thì khung 9–10h **chờ đến bao giờ mới chốt sổ**? Chờ mãi thì: (a) không bao giờ có kết quả cuối, (b) Spark phải giữ state của MỌI khung giờ từ thuở khai thiên — state phình đến OOM.

**Watermark** chính là câu trả lời của Spark: một hợp đồng do BẠN ký — *"tôi chấp nhận chờ dữ liệu trễ tối đa T; trễ hơn nữa, tôi bỏ để đổi lấy việc chốt sổ được và state được dọn."* Đây là trade-off **đúng-đủ vs đúng-hạn**, không framework nào né được (Flink cũng có watermark, cùng một triết lý).

| Được (nhờ watermark) | Mất |
|---|---|
| State được dọn → stream sống khỏe vô hạn | Event trễ quá T bị bỏ (aggregation) — mất mát CÓ KIỂM SOÁT |
| Append mode khả dụng → window "chốt sổ" phát 1 lần, sạch cho downstream | Kết quả cuối của window đến muộn thêm ~T |
| Kết quả có tính tái lập theo event time | Bạn phải HIỂU nghiệp vụ để chọn T — không có số đúng vạn năng |

> Bài học Senior: watermark không phải config kỹ thuật, nó là **quyết định nghiệp vụ được dịch sang code**. "Chờ bao lâu" phải hỏi product owner, không hỏi Stack Overflow.

---

## 3. Theory

### 3.1. Windowed aggregation — cắt trục event time thành cửa sổ

**Tumbling window** — khít nhau, không chồng, mỗi event thuộc ĐÚNG 1 cửa sổ:

```
event time ──▶
[09:00 ─ 09:10)[09:10 ─ 09:20)[09:20 ─ 09:30)
      ●●  ●         ●●●            ●
```

```python
from pyspark.sql import functions as F
revenue = (orders
    .groupBy(F.window("created_at", "10 minutes"), "city")
    .agg(F.sum("amount").alias("revenue")))
# cột window là struct: window.start, window.end
```

**Sliding window** — chồng lên nhau, 1 event rơi vào NHIỀU cửa sổ (đắt hơn tumbling bấy nhiêu lần):

```
[09:00 ──────── 09:10)
      [09:05 ──────── 09:15)          window 10 phút, trượt mỗi 5 phút
            [09:10 ──────── 09:20)    → mỗi event thuộc 2 cửa sổ
```

```python
F.window("created_at", "10 minutes", "5 minutes")   # (cột, độ dài, bước trượt)
```

**Session window** — không có lịch cố định; cửa sổ MỌC theo hoạt động, ĐÓNG khi user im lặng đủ lâu (gap):

```
user A:  ●─●──●          (im ắng > 30 phút)          ●──●
         └── session 1 ──┘                    └─ session 2 ─┘
```

```python
F.session_window("event_time", "30 minutes")   # Spark 3.2+, gap 30 phút
```

Tumbling cho báo cáo định kỳ, sliding cho "trung bình trượt/phát hiện xu hướng", session cho phân tích hành vi (chính là sessionization của Project 3).

### 3.2. Watermark — định nghĩa chính xác

```python
withWatermark("created_at", "10 minutes")
```

nghĩa là, tại mọi thời điểm:

```
watermark = MAX(event time đã thấy trên toàn stream) − 10 phút
```

Đọc to lên 3 lần: watermark tính theo **max event time đã thấy**, KHÔNG theo đồng hồ trên tường. Stream không có dữ liệu mới → watermark **đứng yên** (hệ quả thi vị: nguồn tắt thì không window nào chốt sổ nữa). Watermark chỉ tiến, không bao giờ lùi, và được cập nhật ở **ranh giới micro-batch** (cuối batch tính max, đầu batch sau dùng — chi tiết mục 4).

### 3.3. Timeline watermark di chuyển — đọc chậm, đây là hình quan trọng nhất bài

Window tumbling 10 phút, watermark threshold 10 phút:

```
BATCH 1 nhận: ●(ev 09:02) ●(ev 09:08) ●(ev 09:11)
  max event time = 09:11 → watermark = 09:01
  state đang giữ: [09:00–09:10): 2 event │ [09:10–09:20): 1 event
  09:01 < 09:10 → chưa window nào đóng. (Update mode: emit kết quả tạm;
                                          Append mode: CHƯA emit gì)

BATCH 2 nhận: ●(ev 09:07 — TRỄ nhưng còn hạn!) ●(ev 09:25)
  09:07 > watermark 09:01 → ĐƯỢC NHẬN, cộng đúng vào [09:00–09:10) ✅
  max = 09:25 → watermark = 09:15
  09:15 ≥ 09:10 → window [09:00–09:10) CHỐT SỔ:
      • Append mode: EMIT kết quả cuối của nó — đúng 1 lần
      • state của nó bị DROP — trả lại memory  ← lý do tồn tại của watermark
  state còn: [09:10–09:20), [09:20–09:30)

BATCH 3 nhận: ●(ev 09:05 — trễ QUÁ HẠN)
  09:05 < watermark 09:15, window [09:00–09:10) đã khai tử
  → event bị BỎ QUA trong im lặng. Không lỗi. Không log WARN. Biến mất. ⚠️
```

Ba định luật rút ra:

1. **Watermark là lưỡi dao dọn state**: window có `end ≤ watermark` bị chốt và drop. Không watermark → không gì bị drop → state bất tử → OOM từ từ.
2. **Trễ trong hạn thì vẫn ĐÚNG**: event 09:07 đến muộn vẫn vào đúng cửa sổ của nó — đây là sức mạnh của event time.
3. **Trễ quá hạn chết không kèn trống** — theo dõi qua metric `numRowsDroppedByWatermark` (mục 11), đừng đợi kế toán phát hiện thiếu số.

### 3.4. `withWatermark` đặt TRƯỚC groupBy — luật cứng

```python
# ✅ ĐÚNG: watermark khai trên cột event time, TRƯỚC phép aggregate dùng cột đó
agg = (orders
       .withWatermark("created_at", "10 minutes")
       .groupBy(F.window("created_at", "10 minutes"), "city")
       .agg(F.sum("amount").alias("revenue")))

# ❌ SAI: đặt sau groupBy — watermark không gắn vào aggregation,
# Spark coi như aggregation KHÔNG watermark → append mode bị từ chối,
# state không bao giờ được dọn
agg_sai = (orders
           .groupBy(F.window("created_at", "10 minutes"), "city")
           .agg(F.sum("amount").alias("revenue"))
           .withWatermark("window", "10 minutes"))
```

Lý do: watermark là thuộc tính của **cột event time đầu vào** mà planner dùng khi dựng aggregation. Thêm luật phụ: cột trong `withWatermark` phải **chính là** cột dùng trong `F.window(...)` (đổi tên/biến đổi cột sau khi khai watermark là mất hiệu lực).

### 3.5. Watermark × output mode — trả món nợ lesson 23

| Mode | Hành vi với windowed agg + watermark | Hệ quả |
|---|---|---|
| **append** | Window CHỈ được emit khi đã chốt sổ (end ≤ watermark) — đúng 1 lần, giá trị cuối | Sạch tuyệt đối cho downstream (file/Iceberg), nhưng kết quả đến muộn thêm ~threshold |
| **update** | Mỗi batch emit các window vừa thay đổi (kết quả tạm, đổi dần) | Thấy số sớm, nhưng downstream phải chịu được upsert |
| **complete** | Emit cả result table mỗi batch; watermark KHÔNG drop state (phải giữ tất cả để in lại!) | Gần như vô hiệu hóa lợi ích dọn state — tránh với window |

Giờ bạn hiểu tận gốc câu bảng 3.4 lesson 23: aggregation không watermark bị cấm append vì không dòng nào chốt được; có watermark thì "chốt sổ" được định nghĩa → append hợp lệ. Chọn nhanh: dashboard nội bộ muốn số nhảy sớm → **update**; ghi xuống lakehouse cho BI → **append**.

### 3.6. Chọn allowed lateness theo nghiệp vụ

| Use case | Nguồn trễ điển hình | Gợi ý threshold | Lý do |
|---|---|---|---|
| IoT sensor (nhà máy, xe) | Thiết bị mất sóng, buffer rồi bắn bù — trễ hàng giờ là thường | 1–24 giờ (tùy đường truyền) | Bỏ sensor reading là bỏ sự thật vật lý; thà kết quả muộn còn hơn thiếu. Window thường to (giờ/ngày) nên chờ lâu vẫn ổn |
| User click / web event | Mạng di động, tab nền, batching SDK — đa số đến trong vài phút | 5–15 phút | Analytics chịu được thiếu 0.x% event đuôi; latency dashboard quan trọng hơn |
| Payment / giao dịch tiền | Hệ thống nguồn có SLA, trễ hiếm — nhưng MỌI bản ghi đều thiêng | Threshold rộng (giờ) + **lưới an toàn**: batch reconciliation đối soát cuối ngày với nguồn | Tiền không được phép "rơi im lặng" — streaming cho số nhanh, batch đối soát cho số ĐÚNG. Không bao giờ tin một mình watermark với tiền |

Quy trình chọn (làm thật ở lab): đo phân phối `processing_time − event_time` trên dữ liệu thật → lấy P99/P999 → cộng biên an toàn → thỏa thuận với chủ nghiệp vụ về tỷ lệ chấp nhận rơi. Threshold là con số **được đo và được ký**, không phải số đẹp bốc từ trên trời.

---

## 4. Internal

Watermark sống thế nào trong vòng đời micro-batch:

```
① BATCH N bắt đầu: lấy watermark W đã chốt từ CUỐI batch N-1
   (ghi trong file offsets/N của checkpoint — trường batchWatermarkMs.
    Mở file ra xem được! Lab bước 3)
        │
② Xử lý dữ liệu batch N:
   • event có event_time ≥ W → nhận, cập nhật state window tương ứng
   • event có event_time <  W mà window của nó đã bị dọn → DROP
     (đếm vào metric numRowsDroppedByWatermark)
        │
③ Aggregate với state (đọc/ghi state store trên executor — lesson 26)
        │
④ Emit theo output mode:
   • append: quét state, window nào end ≤ W → phát kết quả cuối
   • update: phát các window vừa thay đổi trong batch này
        │
⑤ DỌN STATE: xóa mọi window đã chốt (end ≤ W) khỏi state store
        │
⑥ CUỐI batch: tính max(event_time) thấy trong batch N
   → watermark mới W' = max(W, max_event_time − threshold)
   → ghi vào checkpoint cho batch N+1
```

Chi tiết thi cử hay hỏi:

- **Watermark trễ một nhịp batch**: event của batch N chỉ đẩy watermark cho batch N+1 — nên đừng ngạc nhiên khi window "đáng lẽ đóng rồi" mà phải chờ thêm 1 batch.
- **Watermark là MỘT con số toàn query** (không phải per-key): tính trên max event time của **mọi partition**. Nhiều source/nhiều cột watermark → mặc định lấy **min** (`spark.sql.streaming.multipleWatermarkPolicy=min`) — nguồn chậm nhất ghìm cả đoàn tàu.
- **Một producer đồng hồ sai** (event time năm 2099) đẩy watermark vọt xa → mọi event tử tế bỗng thành "quá hạn" và bị drop hàng loạt. Phòng thân: filter chặn event_time > hiện tại + biên độ ngay đầu pipeline.
- Watermark nằm trong **checkpoint** → restart không quên; đổi threshold rồi restart cùng checkpoint là được (giá trị mới áp dần), nhưng đổi cả cấu trúc aggregation thì không (state không tương thích — lesson 26).

---

## 5. API

### `F.window(timeColumn, windowDuration, [slideDuration])`

```python
F.window("created_at", "10 minutes")                 # tumbling
F.window("created_at", "10 minutes", "5 minutes")    # sliding
```
- **Ý nghĩa**: sinh cột struct `window{start, end}` để groupBy — dùng được cả trên batch DataFrame (thử ngay với Olist!).
- **Pitfall**: sliding với bước trượt quá nhỏ ("1 giờ" trượt "5 giây") = mỗi event nhân bản vào 720 window → state ×720. Nghĩ kỹ trước khi trượt mịn.

### `F.session_window(timeColumn, gapDuration)`

```python
F.session_window("event_time", "30 minutes")
# gap động theo dữ liệu cũng được: F.session_window("event_time", F.when(...).otherwise(...))
```
- **Pitfall**: session window BẮT BUỘC có watermark khi streaming — session không gap-out được nếu không ai nói cho Spark biết "đủ rồi, đừng chờ nữa".

### `df.withWatermark(eventTimeCol, delayThreshold)`

```python
df.withWatermark("created_at", "10 minutes")
```
- **Ý nghĩa**: khai hợp đồng lateness trên cột event time (kiểu timestamp).
- **Pitfall #1**: đặt SAU groupBy → vô hiệu (mục 3.4). **#2**: cột phải là TimestampType — dữ liệu Kafka ra string/epoch thì cast trước. **#3**: threshold là **event-time duration**, không phải "đợi 10 phút đồng hồ thật".

### Đọc kết quả window

```python
(agg.select(F.col("window.start").alias("ws"),
            F.col("window.end").alias("we"), "city", "revenue"))
```
- **Pitfall**: ghi xuống bảng mà giữ nguyên cột struct `window` → downstream SQL khó chịu; flatten start/end ra trước khi ghi.

### Đo tuổi trễ thực tế (chuẩn bị cho việc chọn threshold)

```python
df.withColumn("lateness_sec",
              F.col("kafka_timestamp").cast("long") - F.col("created_at").cast("long"))
# batch job: approxQuantile("lateness_sec", [0.5, 0.99, 0.999], 0.01) → P99 làm mốc threshold
```

---

## 6. Demo nhỏ

Rate source + tự chế độ trễ: dòng nào `value % 10 == 0` bị "gửi bưu điện" — event time bị lùi 45 giây (giả làm data trễ).

```
Input:  rate 10 dòng/s; 10% có event_time trễ 45s
   ↓    withWatermark 30s  →  trễ 45s là QUÁ HẠN → sẽ bị drop
   ↓    groupBy window 20s, count
Output: console, append mode — window chỉ hiện khi ĐÃ CHỐT SỔ
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo25").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
    .withColumn("event_time",
        F.when(F.col("value") % 10 == 0,
               F.col("timestamp") - F.expr("INTERVAL 45 SECONDS"))  # 10% trễ 45s
         .otherwise(F.col("timestamp"))))

agg = (events
       .withWatermark("event_time", "30 seconds")       # hợp đồng: chờ tối đa 30s
       .groupBy(F.window("event_time", "20 seconds"))
       .count())

q = (agg.select("window.start", "window.end", "count")
     .writeStream.format("console").outputMode("append")   # append: chỉ window ĐÃ ĐÓNG
     .option("truncate", "false")
     .trigger(processingTime="10 seconds")
     .start())
q.awaitTermination(120); q.stop(); spark.stop()
```

Quan sát: (1) vài batch đầu **trống trơn** — append phải chờ watermark vượt qua end của window đầu tiên; (2) mỗi window xuất hiện **đúng một lần**, không bao giờ sửa lại; (3) count các window ≈ 200 nhưng **thiếu vài đơn vị** — đó là các event trễ 45s bị watermark 30s loại. Đổi watermark thành `60 seconds` chạy lại: count đủ, nhưng window đóng muộn hơn — trade-off sờ được bằng tay.

---

## 7. Production Example

Dashboard doanh thu near-real-time cho sàn TMĐT (nguyên mẫu thu nhỏ của Project 3):

```
Kafka topic orders (event: order_id, amount, created_at, city)
        │
Spark Structured Streaming
  • đo lateness thật 1 tuần: P99 = 4 phút, P999 = 11 phút   ← BƯỚC BỊ BỎ QUA NHIỀU NHẤT
  • withWatermark("created_at", "15 minutes")               ← P999 + biên
  • Nhánh 1 → update mode → bảng serving (dashboard "số nhảy sớm",
              chấp nhận số tạm còn nhích trong 15 phút)
  • Nhánh 2 → append mode, window đã chốt → Iceberg gold
              (nguồn sự thật cho báo cáo — mỗi window đúng 1 dòng, bất biến)
        │
Batch reconciliation 2AM: đối soát tổng ngày với PostgreSQL nguồn
  → chênh > 0.1%? Alert. (lưới an toàn cho phần rơi ngoài watermark)
```

Ba bài học kiến trúc:

1. **Đo trước khi hứa**: threshold 15 phút đến từ phân phối lateness thật, không phải cảm hứng. Con số này nằm trong design doc, có chữ ký của product owner.
2. **Hai nhánh cho hai loại khách**: update mode phục vụ mắt người (cần sớm, tha thứ số tạm), append mode phục vụ bảng vàng (cần đúng và bất biến). Cùng một stream, hai hợp đồng.
3. **Watermark + reconciliation, không phải watermark một mình**: streaming trả lời "nhanh", batch đối soát trả lời "đủ". Với số liệu tiền bạc, mọi kiến trúc trưởng thành đều có tầng đối soát.

---

## 8. Hands-on Lab

**Mục tiêu**: tự tay bắn event trễ qua socket, nhìn watermark nhận/loại từng event, và soi watermark trong checkpoint.

### Bước 1 — máy bắn event có kịch bản trễ: `labs/lab25/feeder.py`

Socket cho ta quyền đạo diễn từng event — thứ rate source không làm được:

```python
# Chạy bằng python3 thường (không phải spark-submit) TRONG container submit
import socket, time, json
from datetime import datetime, timedelta

now = lambda: datetime.now()
fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S")
# Kịch bản: (chờ_giây, event_time_lùi_giây, nhãn)
script = [(0, 0, "on-time-1"), (2, 0, "on-time-2"),
          (2, 25, "late-25s-CON-HAN"),          # trễ 25s — trong hạn 30s
          (5, 0, "on-time-3"),                  # đẩy watermark tiến lên
          (10, 90, "late-90s-QUA-HAN"),         # trễ 90s — sẽ bị DROP
          (2, 0, "on-time-4")]

srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("0.0.0.0", 9999)); srv.listen(1)
print("Cho Spark ket noi vao :9999 ...")
conn, _ = srv.accept()
for wait, back, label in script * 10:           # lặp kịch bản 10 vòng
    time.sleep(wait)
    ev = {"event_time": fmt(now() - timedelta(seconds=back)), "label": label}
    conn.sendall((json.dumps(ev) + "\n").encode())
    print("sent:", ev)
```

### Bước 2 — query: `labs/lab25/watermark_lab.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("lab25-watermark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([StructField("event_time", StringType()),
                     StructField("label", StringType())])

events = (spark.readStream.format("socket")
          .option("host", "localhost").option("port", 9999).load()
          .select(F.from_json("value", schema).alias("d")).select("d.*")
          .withColumn("event_time", F.to_timestamp("event_time")))

agg = (events
       .withWatermark("event_time", "30 seconds")
       .groupBy(F.window("event_time", "20 seconds"))
       .agg(F.count("*").alias("cnt"), F.collect_list("label").alias("labels")))

q = (agg.select("window.start", "window.end", "cnt", "labels")
     .writeStream.format("console")
     .outputMode("append")
     .option("truncate", "false")
     .option("checkpointLocation", "/workspace/data/chk/lab25")
     .trigger(processingTime="10 seconds")
     .start())
q.awaitTermination()
```

Chạy 2 terminal:

```bash
# Terminal 1 — feeder
docker exec -it spark-mastery-spark-submit-1 python3 /workspace/labs/lab25/feeder.py
# Terminal 2 — query (local mode để thấy console)
make run-local F=labs/lab25/watermark_lab.py
```

### Bước 3 — quan sát & giải phẫu

1. Đối chiếu console: `late-25s-CON-HAN` có mặt trong `labels` của window cũ không? (Phải CÓ.) `late-90s-QUA-HAN` có xuất hiện ở BẤT KỲ window nào không? (Phải KHÔNG — nó đi đâu?)
2. Mở checkpoint xem watermark bằng mắt:
```bash
cat data/chk/lab25/offsets/$(ls data/chk/lab25/offsets | sort -n | tail -1) | head -2
# dòng 2 có {"batchWatermarkMs":169...} — đổi ms → giờ, đối chiếu max event time đã bắn − 30s
```
3. UI :4040 → tab Structured Streaming → click run → phần **Aggregated Number Of Rows Dropped By Watermark**: số event bị loại — khớp số lần `late-90s` đã bắn không?
4. Sửa `outputMode("update")` (đổi checkpoint mới!) chạy lại: giờ thấy window hiện SỚM và cnt NHÍCH DẦN — cảm nhận khác biệt append/update bằng mắt.

Ghi 4 quan sát vào `labs/lab25/NOTES.md`.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Vẽ timeline một event trễ: xảy ra 09:59 → Spark thấy 10:06. Với window 10 phút + watermark 10 phút, nó vào window nào? Được nhận hay bị bỏ (giả sử max event time đã thấy là 10:05)?
2. Vì sao watermark tính theo max event time đã thấy mà không theo đồng hồ cluster? Chuyện gì xảy ra với watermark khi stream không có dữ liệu 1 giờ?
3. Phân biệt tumbling / sliding / session window — mỗi loại một use case thật.

**Medium** — Window 5 phút, watermark 10 phút. Event có `event_time = 10:02` đến lúc Spark đã thấy max event time 10:16. (a) Watermark là bao nhiêu? (b) Window [10:00–10:05) còn sống không? (c) Event được nhận hay bị drop? (d) Nếu nó đến khi max mới là 10:11 thì sao? Sau khi trả lời trên giấy, DỰNG LẠI kịch bản này bằng feeder.py (sửa script) và chứng minh bằng console + metric dropped.

**Hard** — Bài "đồng hồ điên": thêm vào feeder một event có `event_time` = hiện tại **+ 2 giờ** (producer sai giờ). Chạy và mô tả thảm họa: watermark nhảy đi đâu, các event tử tế sau đó chịu số phận gì, metric nào tố cáo? Viết bản vá: 1 dòng `filter` chặn event tương lai + 3 dòng giải thích đặt filter ở đâu trong pipeline và vì sao phải TRƯỚC `withWatermark`.

**Production Challenge** — Bạn nhận yêu cầu: "dashboard click theo khung 5 phút, và số liệu payment theo khung 1 giờ". Viết đề xuất 15 dòng cho tech lead: threshold cho mỗi stream (kèm cách bạn sẽ ĐO để ra số), output mode mỗi nhánh, và lưới an toàn cho payment. Bonus: giải thích vì sao KHÔNG dùng chung một query cho cả hai.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Aggregation KHÔNG watermark (update mode) | Chậm dần → chết | Không gì được drop: state giữ mọi key/window vĩnh viễn. Batch duration dốc lên tuần này qua tuần khác — "memory leak hợp pháp". |
| Sliding window bước trượt mịn | State ×N | Window 1h trượt 1 phút = mỗi event nằm trong 60 window. Tự hỏi: có thật cần mịn vậy, hay tumbling 5 phút là đủ cho mắt người? |
| Watermark threshold quá rộng | State to, kết quả muộn | Chờ 24h cho data chỉ trễ tối đa 5 phút = giữ 24h state vô ích + append mode im lặng 24h. Đo rồi hãy chọn. |
| Window struct ghi thẳng xuống bảng | Phiền downstream | Flatten window.start/end trước khi ghi — rẻ mà đỡ một tầng SQL lồng. |
| Aggregate window + key cardinality cao (user_id) | State = #window × #key | Ước lượng trước: 1M user × 12 window sống × ~vài trăm byte = hàng GB state. Chuẩn bị đọc lesson 26 (RocksDB). |

Câu tự vấn mới: *"state của tôi có hạn sử dụng chưa, và ai là người ký hạn đó?"*

---

## 11. Spark UI

Tab **Structured Streaming**, click vào run đang chạy — bài này thêm 2 vũ khí mới:

- **Global Watermark Gap**: khoảng cách `đồng hồ hệ thống − watermark`. Gap ổn định ≈ threshold + nhịp batch là khỏe; gap PHÌNH DẦN nghĩa là watermark tụt lại (nguồn ngừng gửi, hoặc một partition Kafka im lặng ghìm min-watermark) — window không đóng, append mode câm lặng, state phình. Đây là đồ thị đầu tiên cần nhìn khi "stream chạy mà không ra kết quả".
- **Aggregated Number Of Rows Dropped By Watermark**: đếm event bị loại vì quá hạn. Số này > 0 đều đặn = threshold đang chém vào dữ liệu thật — đem con số này đi nói chuyện lại với product owner, đừng giấu.

Vẫn nhìn **Batch Duration** và **Operation Duration** như lesson 23–24; cộng thêm phần **stateOperators** trong `query.lastProgress` (numRowsTotal của state phải ĐI NGANG khi tải ổn định — dốc lên là watermark không dọn được, sang lesson 26 xử tiếp).

---

## 12. Common Mistakes

1. **Đặt `withWatermark` sau `groupBy`** → watermark vô hiệu, append bị từ chối hoặc state bất tử. Luật: watermark trước, aggregate sau, cùng một cột.
2. **Aggregate theo processing time cho tiện** (window trên cột `timestamp` của Kafka thay vì event time trong payload) → số liệu phụ thuộc độ trễ hạ tầng, chạy lại ra số khác, đối soát không khớp.
3. **Tưởng watermark 10 phút = "Spark đợi 10 phút đồng hồ"** — không, nó neo theo max event time ĐÃ THẤY. Nguồn ngừng gửi thì watermark đứng im và window cuối cùng không bao giờ đóng (gặp nhiều nhất khi demo/test với dữ liệu hữu hạn!).
4. **Append mode + ngạc nhiên "sao chưa có output"** — window phải chờ watermark vượt qua end mới emit. Kiên nhẫn, hoặc dùng update mode khi debug.
5. **Không theo dõi rows dropped by watermark** — dữ liệu rơi im lặng hàng tháng, đến kỳ đối soát mới tá hỏa. Metric này phải có alert.
6. **Threshold bốc thuốc** ("10 phút nghe hợp lý") — không đo phân phối lateness thật. P99 thật có khi là 40 phút (app mobile), khi đó bạn đang vứt 1% dữ liệu mỗi ngày.
7. **Quên event time cần cast TimestampType** — from_json ra string, window trên string → AnalysisException hoặc kết quả vô nghĩa.

---

## 13. Interview

**Junior:**

1. *Event time vs processing time?* — Event time: lúc sự việc xảy ra, nằm trong dữ liệu. Processing time: lúc engine xử lý nó. Lệch nhau do mạng/offline/tồn đọng; aggregation nghiệp vụ phải theo event time để kết quả đúng và tái lập được.
2. *Watermark là gì, công thức?* — Ngưỡng chấp nhận trễ: watermark = max event time đã thấy − threshold. Event/window cũ hơn watermark bị coi là quá hạn: window chốt sổ và state được dọn, event trễ hơn bị bỏ.
3. *Tumbling vs sliding vs session window?* — Tumbling: khít, không chồng, 1 event 1 window. Sliding: chồng lấp theo bước trượt, 1 event nhiều window. Session: theo hoạt động, đóng khi im lặng quá gap — không có lịch cố định.
4. *Late data trong hạn và quá hạn khác nhau thế nào?* — Trong hạn (event_time ≥ watermark): nhận và cộng đúng vào window cũ của nó. Quá hạn: bị bỏ qua im lặng (với aggregation), đếm vào metric dropped-by-watermark.

**Mid:**

5. *Vì sao watermark mở khóa append mode cho aggregation?* — Append cần dòng "không bao giờ đổi nữa"; watermark định nghĩa được thời điểm window chốt sổ (end ≤ watermark) → từ đó window emit đúng một lần với giá trị cuối. Không watermark thì mọi window mãi mãi có thể nhận thêm data → không gì chốt được.
6. *Watermark cập nhật lúc nào, và vì sao nói nó "trễ một nhịp"?* — Tính ở cuối mỗi micro-batch từ max event time của batch, áp dụng cho batch sau; lưu trong checkpoint (batchWatermarkMs). Nên event vừa đẩy max lên chưa làm window đóng ngay trong chính batch đó.
7. *Nguồn ngừng gửi dữ liệu — watermark và các window đang mở ra sao?* — Watermark neo theo max event time đã thấy nên ĐỨNG YÊN; window đang mở không bao giờ đạt điều kiện đóng, append mode không emit nữa, state giữ nguyên. Đây là lý do test với dữ liệu hữu hạn thấy "thiếu window cuối", và production cần liveness alert cho nguồn.
8. *Chọn watermark threshold thế nào cho chuẩn?* — Đo phân phối lateness thật (processing_time − event_time) trên vài tuần dữ liệu, lấy P99/P999 + biên; thỏa thuận tỷ lệ rơi chấp nhận được với chủ nghiệp vụ; giám sát rows-dropped để kiểm chứng liên tục. Threshold là quyết định nghiệp vụ đã được đo đạc.

**Senior:**

9. *Một producer bị sai đồng hồ (+3 giờ) bắn vào stream — phân tích tác động và thiết kế phòng thủ.* — Event tương lai đẩy max event time vọt lên → watermark nhảy +3h → mọi window hiện tại bị chốt non và DROP hàng loạt event tử tế; thiệt hại tiếp diễn đến khi watermark "thật" đuổi kịp (watermark không lùi được). Phòng thủ nhiều lớp: (a) validate/filter event_time > now + biên ngay đầu pipeline, TRƯỚC withWatermark; (b) alert trên đột biến rows-dropped và watermark gap; (c) quarantine event nghi vấn sang dead-letter thay vì thả vào aggregate; (d) nguồn nhiều partition/topic thì multipleWatermarkPolicy=min cũng không cứu được case này — chỉ validation cứu được.
10. *Pipeline payment: dùng watermark ngắn cho latency hay dài cho đủ dữ liệu? Trình bày thiết kế của bạn.* — Từ chối câu hỏi nhị phân: tách hai hợp đồng. Nhánh streaming watermark vừa phải (P999 đo được) + update mode cho vận hành nhìn sớm; nhánh append ghi bảng chuẩn; và BẮT BUỘC tầng batch reconciliation đối soát với hệ thống nguồn cuối ngày — với tiền, watermark là tối ưu latency, KHÔNG phải cơ chế đảm bảo đủ dữ liệu. Kèm alert dropped-rows ≈ 0 làm điều kiện sức khỏe. Câu trả lời thể hiện điều Senior hiểu: streaming và batch không thay thế nhau, chúng bảo hiểm cho nhau.

---

## 14. Summary

### Mindmap

```
                 EVENT TIME · WATERMARK · LATE DATA
                              │
   ┌──────────────┬───────────┴────────────┬──────────────────────┐
   ▼              ▼                        ▼                      ▼
 HAI ĐỒNG HỒ    WINDOW                  WATERMARK              VẬN HÀNH
   │              │                        │                      │
 event time     tumbling (khít)        = max event time seen    đo lateness P99
 (trong data)   sliding (chồng)          − threshold            rồi mới chọn T
 processing     session (theo gap)     withWatermark TRƯỚC      metric dropped
 time (đồng     F.window /             groupBy, cùng cột        watermark gap
 hồ cluster)    F.session_window       window end ≤ WM          IoT giờ / click
 agg PHẢI theo  cột struct               → chốt sổ + DROP state  phút / payment
 event time     start-end              trễ hơn WM → BỎ im lặng  + reconciliation
                                       mở khóa APPEND mode
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại timeline 3 batch của mục 3.3 từ trí nhớ: watermark tiến, window chốt, event quá hạn rơi.
- [ ] Viết đúng thứ tự: cast timestamp → withWatermark → groupBy(window) → agg.
- [ ] Giải thích được vì sao watermark neo theo max event time chứ không theo đồng hồ thật, và hệ quả khi nguồn im lặng.
- [ ] Nói được watermark mở khóa append mode như thế nào.
- [ ] Đã tự bắn event trễ qua socket, thấy event trong hạn được cộng đúng và event quá hạn biến mất.
- [ ] Đã đọc batchWatermarkMs trong checkpoint và metric dropped trên UI.
- [ ] Chọn được threshold cho IoT/click/payment kèm lý do — và biết payment cần thêm lưới đối soát.

---

## 15. Next Lesson

**Lesson 26 — Stateful operations: aggregation, dedup, state store.**

Cả bài hôm nay ta nói "state được giữ", "state bị drop" — nhưng state là CÁI GÌ, nằm CHỖ NÀO, to bao nhiêu thì vỡ? Lesson 26 mở nắp capo: state store trên executor (HDFSBackedStateStore vs RocksDB — và khi nào bắt buộc đổi sang RocksDB), `dropDuplicates` với watermark làm TTL, sessionization bằng `applyInPandasWithState`, và bộ kỹ năng chẩn đoán "state phình to" qua `query.lastProgress` — căn bệnh mạn tính số 1 của stream chạy lâu năm.

Watermark cho state một hạn sử dụng; lesson 26 cho bạn quyền quản lý cả kho. Xong bài đó, bạn đủ nội công cho exactly-once (lesson 27) và Project 2.

> Gõ **"Continue"** khi sẵn sàng.
