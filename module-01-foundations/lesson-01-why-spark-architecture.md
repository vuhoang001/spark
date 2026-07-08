# Lesson 1 — Tại sao Spark tồn tại: Distributed Computing & Kiến trúc tổng quan

> Module 1 · Foundations · Tuần 1 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Bài toán gốc mà Spark sinh ra để giải: **xử lý dữ liệu lớn hơn một máy**.
- Lịch sử: Hadoop MapReduce → Spark, và tại sao Spark thắng.
- Kiến trúc Spark: **Driver, Executor, Cluster Manager** — bộ ba bạn sẽ gặp mỗi ngày trong 10 năm tới.
- Chuỗi thực thi: Application → Job → Stage → Task.
- Chạy Spark application đầu tiên và mở Spark UI lần đầu.

Sau bài này bạn phải làm được:

- Vẽ lại kiến trúc Spark từ trí nhớ và giải thích từng thành phần cho một backend dev chưa biết gì về Spark.
- Trả lời: "code PySpark của tôi chạy Ở ĐÂU?" (câu này 90% junior trả lời sai).
- Chạy `pyspark`/`spark-submit`, đọc được tab Jobs trên Spark UI.

Kiến thức dùng trong thực tế: **mọi thứ**. Khi job OOM, bạn phải biết OOM ở driver hay executor. Khi job chậm, bạn phải biết task phân bổ ra sao. Không có bài nào nền tảng hơn bài này.

---

## 2. Why

### Vấn đề: một máy không đủ

Bạn có bảng transactions 2 TB. Máy bạn có 32 GB RAM, ổ đĩa đọc ~500 MB/s.

- Chỉ **đọc tuần tự** 2 TB đã mất ~70 phút, chưa xử lý gì.
- Muốn `GROUP BY user_id` toàn bộ? Không nhét vừa RAM.
- PostgreSQL của bạn? Được cho vài trăm GB, nhưng scale dọc (mua máy to hơn) có trần và giá tăng phi tuyến.

Giải pháp duy nhất còn lại: **chia dữ liệu cho nhiều máy xử lý song song** — distributed computing. Nhưng lập trình phân tán tay trần cực khổ: chia dữ liệu thế nào, máy chết giữa chừng thì sao, gom kết quả kiểu gì, network chậm thì ai chờ ai?

### Lịch sử: MapReduce → Spark

**2004** — Google công bố paper MapReduce: mô hình hóa mọi xử lý phân tán thành 2 bước `map` (biến đổi từng phần) và `reduce` (gom theo key). Framework lo hết chuyện chia việc, retry, gom kết quả. **2006** — Hadoop ra đời, là bản open-source của ý tưởng này.

Nhưng MapReduce có một điểm chết: **sau mỗi bước map/reduce, kết quả trung gian bị ghi xuống DISK (HDFS)**. Một pipeline 10 bước = 10 lần ghi đĩa + 10 lần đọc lại. Với iterative algorithm (chạy lặp trên cùng dữ liệu), điều này thảm họa.

```
MapReduce (pipeline 3 bước):
  đọc HDFS → map/reduce → GHI HDFS → đọc HDFS → map/reduce → GHI HDFS → đọc HDFS → map/reduce → ghi kết quả
                              ↑ disk I/O          ↑ disk I/O
                              (chậm hơn RAM ~100×)

Spark:
  đọc HDFS → transform → transform → transform → ghi kết quả
             └──── dữ liệu trung gian ở RAM/pipeline, chỉ chạm đĩa khi cần ────┘
```

**2009** — Matei Zaharia (UC Berkeley AMPLab) tạo Spark với ý tưởng cốt lõi: giữ dữ liệu trung gian **trong memory** khi có thể, và thay vì 2 phép map/reduce cứng nhắc, cho người dùng một **DAG (Directed Acyclic Graph — đồ thị có hướng không chu trình)** các phép biến đổi tùy ý. Kết quả benchmark thời đó: nhanh hơn MapReduce 10–100×. 2013 vào Apache, 2014 top-level project, và hiện là engine xử lý dữ liệu phổ biến nhất thế giới.

### Nếu không có Spark thì sao?

Bạn sẽ phải: tự viết code phân tán (khó sai vô hạn), hoặc dùng MapReduce (chậm, code Java dài dòng), hoặc bị giới hạn ở dữ liệu vừa một máy (DuckDB/Pandas/Postgres — tốt, nhưng có trần).

### Trade-off của Spark (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| Scale ngang gần như vô hạn | **Overhead khởi động + điều phối**: job Spark xử lý 100 MB có thể CHẬM hơn Pandas/DuckDB |
| Fault tolerance tự động | Phức tạp vận hành: cluster, memory tuning, shuffle |
| Một API cho batch + streaming + SQL | JVM-based: Python phải nói chuyện với JVM (có chi phí) |
| Ecosystem khổng lồ (Kafka, Iceberg, ...) | Latency không dành cho real-time <100ms (đó là đất của Flink) |

> Bài học Senior đầu tiên: **Spark là công cụ cho dữ liệu KHÔNG vừa một máy**. Dữ liệu 5 GB? DuckDB xong trong 10 giây. Đừng vác dao mổ trâu giết gà — interviewer rất thích hỏi câu này.

---

## 3. Theory

### 3.1. Thuật ngữ nền (đọc kỹ, mọi bài sau dùng lại)

| Thuật ngữ | Nghĩa |
|---|---|
| **Cluster** | Nhóm máy tính (node) làm việc cùng nhau như một hệ thống. |
| **Node** | Một máy (vật lý hoặc VM/container) trong cluster. |
| **Process** | Tiến trình hệ điều hành. Driver và Executor đều là các JVM process. |
| **JVM** | Java Virtual Machine — Spark viết bằng Scala, chạy trên JVM. PySpark là lớp vỏ Python điều khiển JVM. |
| **Parallelism** | Mức độ song song — bao nhiêu việc chạy đồng thời. |
| **Fault tolerance** | Khả năng sống sót khi một phần hệ thống chết (máy hỏng, process crash). |

### 3.2. Kiến trúc: 3 nhân vật chính

```
                        ┌────────────────────────────┐
                        │         DRIVER             │
                        │  (bộ não — 1 process duy   │
                        │   nhất cho mỗi application)│
                        │                            │
                        │  • Chạy main() của bạn     │
                        │  • Giữ SparkSession        │
                        │  • Dịch code → DAG → task  │
                        │  • Lên lịch, giao task     │
                        │  • Thu kết quả cuối        │
                        └────────────┬───────────────┘
                                     │ ① xin tài nguyên
                                     ▼
                        ┌────────────────────────────┐
                        │      CLUSTER MANAGER        │
                        │  (bên cho thuê máy)         │
                        │  Standalone / YARN /        │
                        │  Kubernetes / local          │
                        └────────────┬───────────────┘
                                     │ ② cấp executor trên các node
            ┌────────────────────────┼────────────────────────┐
            ▼                        ▼                        ▼
  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │   EXECUTOR 1     │    │   EXECUTOR 2     │    │   EXECUTOR 3     │
  │  (cơ bắp — JVM   │    │                  │    │                  │
  │   process trên   │    │  [task] [task]   │    │  [task] [task]   │
  │   worker node)   │    │                  │    │                  │
  │  • Chạy task     │    │  cache dữ liệu   │    │  cache dữ liệu   │
  │  • Giữ data      │    │  trong memory    │    │  trong memory    │
  └──────────────────┘    └──────────────────┘    └──────────────────┘
            ▲                        ▲                        ▲
            └──────── ③ driver giao task, executor trả kết quả ────────┘
```

**Driver** — bộ não:
- Là process chạy hàm `main()` của bạn. Khi bạn viết `spark = SparkSession.builder...getOrCreate()`, bạn đang ở trong driver.
- KHÔNG xử lý dữ liệu lớn. Nó dịch code thành kế hoạch, chia việc, giao việc, theo dõi.
- Driver chết = toàn bộ application chết. (Executor chết thì việc được giao lại cho executor khác — đó là fault tolerance.)
- Spark UI (port 4040) do driver phục vụ.

**Executor** — cơ bắp:
- JVM process trên worker node. Mỗi executor có số **core** (chạy được bấy nhiêu task đồng thời) và **memory** riêng.
- Nhận task từ driver, đọc phần dữ liệu của mình, xử lý, báo kết quả.
- Giữ dữ liệu cache trong memory nếu bạn yêu cầu.

**Cluster Manager** — bên cho thuê tài nguyên:
- Trả lời câu hỏi "cho tôi 10 executor, mỗi cái 4 core 8 GB được không?". Không quan tâm nội dung công việc.
- Các loại: `local` (giả lập trên 1 máy — dùng để học/dev), Standalone (của Spark), YARN (thế giới Hadoop), Kubernetes (hiện đại, xu hướng chính).

> **Analogy công trường**: Driver là kỹ sư trưởng đọc bản vẽ và chia việc. Cluster Manager là công ty cho thuê nhân công. Executor là các tổ thợ. Kỹ sư trưởng KHÔNG tự xây — nếu bạn bắt driver xử lý dữ liệu (như `collect()` cả bảng về driver), tức là bắt kỹ sư trưởng tự vác gạch → sập (OOM).

### 3.3. Đơn vị công việc: Application → Job → Stage → Task

```
Application  (1 lần spark-submit / 1 SparkSession)
   │
   ├── Job  (mỗi ACTION — count(), save(), collect() — sinh 1 job)
   │     │
   │     ├── Stage  (job bị cắt thành các stage tại ranh giới SHUFFLE —
   │     │           chỗ dữ liệu phải di chuyển giữa các executor)
   │     │     │
   │     │     ├── Task  (đơn vị nhỏ nhất: 1 task xử lý 1 partition dữ liệu,
   │     │     │          chạy trên 1 core của 1 executor)
   │     │     ├── Task
   │     │     └── Task   ← số task của stage = số partition
   │     └── Stage
   └── Job
```

Ba khái niệm mới xuất hiện — định nghĩa ngay, học sâu ở lesson 3–4:

- **Partition**: dữ liệu bị cắt thành các khúc. File 10 GB có thể thành 80 partition ~128 MB. Mỗi partition được 1 task xử lý → partition là đơn vị song song hóa.
- **Shuffle**: khi phép toán cần gom dữ liệu cùng key về một chỗ (`groupBy`, `join`), dữ liệu phải bay qua network giữa các executor. Đắt nhất trong Spark. Ranh giới shuffle = ranh giới stage.
- **Action vs Transformation**: transformation (`filter`, `select`) chỉ *mô tả* việc cần làm — lười, chưa chạy gì. Action (`count`, `save`) mới *kích hoạt* chạy thật. Lesson 2 mổ xẻ kỹ.

Ví dụ đếm nhanh: `spark.read.parquet(...).filter(...).groupBy("city").count().write.parquet(...)`
→ 1 action (`write`) = **1 job**; `groupBy` gây 1 shuffle = **2 stage**; mỗi stage N task theo số partition.

### 3.4. Deploy mode (biết sớm để đọc tài liệu không hoang mang)

| Mode | Driver chạy ở đâu | Dùng khi |
|---|---|---|
| `local[*]` | Tất cả trong 1 JVM trên máy bạn | Học, dev, unit test |
| client mode | Driver trên máy bạn submit, executor trên cluster | Notebook, debug tương tác |
| cluster mode | Driver cũng chạy trong cluster | **Production** — máy submit tắt cũng không sao |

---

## 4. Internal

Chuyện gì xảy ra từ lúc bạn gõ `spark-submit app.py` đến lúc có kết quả:

```
① spark-submit app.py
        │
② Driver process khởi động, chạy main(), tạo SparkSession
        │
③ Driver liên hệ Cluster Manager: "cho tôi N executor, mỗi cái X core, Y GB"
        │
④ Cluster Manager khởi động các Executor process trên worker nodes
   Executors đăng ký ngược lại (heartbeat) với Driver
        │
⑤ Code của bạn chạy trong driver, các transformation chỉ GHI CHÉP lại
   thành logical plan — chưa có dữ liệu nào được xử lý
        │
⑥ Gặp ACTION → Catalyst Optimizer tối ưu plan (lesson 13)
        │
⑦ DAG Scheduler cắt plan thành các STAGE tại ranh giới shuffle,
   xác định stage nào phụ thuộc stage nào
        │
⑧ Task Scheduler phát TASK của từng stage đến các executor —
   ưu tiên gửi task đến nơi dữ liệu đang nằm (data locality:
   xử lý tại chỗ rẻ hơn kéo dữ liệu qua network)
        │
⑨ Executor chạy task trên từng partition; task fail → driver
   giao lại cho executor khác (mặc định retry 4 lần)
        │
⑩ Kết quả: ghi ra storage (write) hoặc gửi về driver (collect/count)
```

Ghi nhớ 2 cái tên sẽ gặp lại suốt khóa:
- **DAG Scheduler**: cắt job → stages, quản lý phụ thuộc giữa stages.
- **Task Scheduler**: phát task → executor, retry khi fail, xử lý straggler (task rùa bò).

Và một sự thật quan trọng với PySpark: process Python của bạn chỉ là **remote control**. Mọi lệnh DataFrame API được chuyển qua **Py4J** (cầu nối Python↔JVM) cho JVM driver thực thi. Dữ liệu nằm trong JVM executor, KHÔNG nằm trong Python — trừ khi bạn dùng UDF hay `toPandas()` (và đó là lúc trả giá — lesson 12).

---

## 5. API

Bài này chỉ cần 4 API để khởi động:

### `SparkSession.builder`

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("lesson01")
         .master("local[4]")            # 4 core local; production KHÔNG hardcode master trong code
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
```
- **Ý nghĩa**: cửa ngõ duy nhất vào Spark từ 2.0+. Tạo (hoặc lấy lại) driver-side session.
- **Khi dùng**: mọi application, tạo đúng 1 lần.
- **Pitfall**: hardcode `master` và config cứng trong code → không đổi được khi deploy. Chuẩn production: để `spark-submit --master ... --conf ...` quyết định (xem `spark_session.py` trong repo của bạn — pattern factory là đúng hướng).

### `spark.read`

```python
df = spark.read.csv("data/olist_orders_dataset.csv", header=True, inferSchema=True)
```
- **Ý nghĩa**: DataFrameReader — đọc CSV/JSON/Parquet/JDBC/... thành DataFrame.
- **Pitfall**: `inferSchema=True` phải **đọc dữ liệu thêm một lần** để đoán kiểu → chậm và có thể đoán sai. Production: khai schema tường minh (lesson 5).

### `df.count()` — action

- **Ý nghĩa**: đếm số dòng. Kích hoạt job thật sự.
- **Performance**: phải quét toàn bộ (với Parquet có metadata thì rẻ hơn nhiều CSV).
- **Pitfall junior**: rắc `count()` khắp nơi để "xem thử" → mỗi lần là một job chạy lại từ đầu.

### `df.show(n)` / `df.collect()`

- `show(20)`: lấy 20 dòng về driver in ra — an toàn, chỉ kéo đủ số dòng cần.
- `collect()`: kéo **TOÀN BỘ** DataFrame về RAM driver. Bảng 100 GB + driver 4 GB = OOM chết tươi. Chỉ dùng khi kết quả chắc chắn nhỏ (sau aggregate còn vài trăm dòng).

---

## 6. Demo nhỏ

```
Input:  danh sách giao dịch nhỏ (tạo tay)
   ↓    filter số tiền > 100 (transformation — chưa chạy)
   ↓    groupBy city, sum (transformation — chưa chạy)
Output: show() (action — LÚC NÀY mới chạy)
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo01").master("local[2]").getOrCreate()

data = [("HN", 120.0), ("SG", 80.0), ("HN", 300.0), ("DN", 150.0), ("SG", 500.0)]
df = spark.createDataFrame(data, ["city", "amount"])

result = (df.filter(F.col("amount") > 100)     # chưa có gì chạy
            .groupBy("city")
            .agg(F.sum("amount").alias("total")))  # vẫn chưa có gì chạy

result.show()   # BÙM — job được sinh ra, chạy, in kết quả
# +----+-----+
# |city|total|
# +----+-----+
# |  HN|420.0|
# |  DN|150.0|
# |  SG|500.0|
# +----+-----+
input("Mở http://localhost:4040 xem Spark UI rồi Enter để thoát...")
spark.stop()
```

Chạy xong hãy tự hỏi: mấy job? (1 — do `show`). Mấy stage? (2 — `groupBy` gây shuffle). Mở UI kiểm chứng.

---

## 7. Production Example

Chính là kiến trúc repo `kafka-flink` bạn đang có:

```
PostgreSQL (HIS/Olist - nguồn)
   ↓  Debezium đọc WAL (CDC — không đụng vào query load của DB nguồn)
Kafka (buffer, replay được, chịu được downstream chết)
   ↓
Spark (batch + streaming: bronze → silver → gold)     ← BẠN Ở ĐÂY suốt khóa học
   ↓
Iceberg (table format trên object storage: ACID, time travel, schema evolution)
   ↓
Trino (query engine phục vụ ad-hoc + BI, không đụng tài nguyên của Spark)
   ↓
BI / Superset
```

Tại sao doanh nghiệp xếp hình như vậy — mỗi tầng trả lời một câu hỏi:

1. **Debezium+Kafka**: lấy dữ liệu mà không đè chết DB nguồn; downstream chết thì dữ liệu vẫn xếp hàng chờ trong Kafka.
2. **Spark ở giữa**: là tầng **compute** duy nhất biến dữ liệu thô thành dữ liệu sạch. Chọn Spark vì: xử lý được cả batch lẫn streaming cùng một API, scale theo dữ liệu, ecosystem connector đầy đủ.
3. **Iceberg**: tách **storage khỏi compute** — dữ liệu nằm một chỗ, Spark ghi, Trino đọc, không engine nào giữ độc quyền.
4. **Trino cho BI**: query interactive latency thấp; để analyst gõ SQL vào Spark cluster thì vừa chậm vừa giành tài nguyên với pipeline.

Đây là mẫu kiến trúc "compute tách storage" mà Netflix, Uber, Grab, Shopee đều dùng (khác nhau ở nhãn hiệu từng tầng).

---

## 8. Hands-on Lab

**Mục tiêu**: chạy Spark trong Docker, xử lý dataset Olist thật, mở Spark UI.

### Bước 0 — chuẩn bị

Dataset: dùng Olist CSV có sẵn trong repo bên cạnh (`../kafka-flink/data/`). Nếu chưa có đủ, tải "Brazilian E-Commerce Public Dataset by Olist" từ Kaggle (~120 MB, 9 file CSV, ~100k đơn hàng thật).

### Bước 1 — dựng Spark container

```bash
# chạy từ thư mục spark-mastery/
docker run -d --name spark-lab \
  -p 4040:4040 \
  -v "$PWD/../kafka-flink/data:/data:ro" \
  -v "$PWD/labs:/labs" \
  apache/spark:3.5.1-python3 \
  sleep infinity
```

### Bước 2 — viết `labs/lab01/explore_olist.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("lab01-olist-explore")
         .getOrCreate())

orders = spark.read.csv("/data/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
items  = spark.read.csv("/data/olist_order_items_dataset.csv",
                        header=True, inferSchema=True)

print(f"Orders: {orders.count():,} dòng | Items: {items.count():,} dòng")
orders.printSchema()

# Doanh thu theo tháng — có join + groupBy => có shuffle => nhiều stage
revenue = (orders
    .filter(F.col("order_status") == "delivered")
    .join(items, "order_id")
    .withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
    .groupBy("month")
    .agg(F.round(F.sum("price"), 2).alias("revenue"),
         F.countDistinct("order_id").alias("orders"))
    .orderBy("month"))

revenue.show(30, truncate=False)

input(">>> Mở http://localhost:4040 — đếm số Jobs, mở DAG của job cuối. Enter để thoát.")
spark.stop()
```

### Bước 3 — chạy

```bash
docker exec -it spark-lab /opt/spark/bin/spark-submit \
  --master 'local[4]' /labs/lab01/explore_olist.py
```

### Bước 4 — quan sát (phần quan trọng nhất)

Mở `http://localhost:4040` khi script đang dừng ở `input()`:

1. Tab **Jobs**: đếm số job. Đối chiếu: mỗi `count()`, `show()` là một job (và `inferSchema` cũng lén tạo job — bất ngờ chưa?).
2. Click job cuối (revenue) → xem **DAG Visualization**: thấy các khối stage nối nhau, tách nhau tại chỗ nào (đó là shuffle của `join`/`groupBy`).
3. Tab **Executors**: `local` mode chỉ có 1 "driver" gộp — ghi nhận điều này, tuần 20 chạy cluster thật sẽ thấy khác.
4. Tab **Stages**: xem cột số task mỗi stage.

Ghi 4 quan sát trên vào `labs/lab01/NOTES.md` — nộp cùng assignment.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn (không copy):
1. Driver làm gì, Executor làm gì? Tại sao driver chết thì application chết còn executor chết thì không?
2. Tại sao Spark nhanh hơn MapReduce? (gợi ý: dữ liệu trung gian nằm ở đâu)
3. Khi nào KHÔNG nên dùng Spark? Cho 2 ví dụ cụ thể.

**Medium** — Từ lab: đếm số đơn theo `order_status`, và top 10 seller theo tổng doanh thu (join `order_items` với `sellers`). Trước khi chạy, **dự đoán số job và số stage**, ghi ra giấy, chạy, so với Spark UI, giải thích chênh lệch.

**Hard** — Đọc cùng file orders 2 lần: một lần `inferSchema=True`, một lần khai `StructType` schema tường minh (tự tìm docs `pyspark.sql.types`). Đo thời gian mỗi cách (dùng `time.time()` quanh đoạn read + count). Mở Spark UI giải thích: inferSchema tạo thêm job nào? Viết 3–5 dòng kết luận khi nào chấp nhận được inferSchema.

**Production Challenge** — Đọc file `../kafka-flink/processing/spark/jobs/spark_session.py` trong repo của bạn. Viết review ngắn (10–15 dòng): pattern nào tốt? Config nào bạn chưa hiểu (liệt kê — đây là danh sách "nợ kiến thức" ta sẽ trả dần trong khóa)? Nếu là code reviewer, bạn hỏi tác giả câu gì?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Ngay ở bài 1 đã có bài học performance thật:

| Thao tác trong lab | Nhanh/Chậm | Tại sao |
|---|---|---|
| `inferSchema=True` trên CSV | Chậm | Phải đọc dữ liệu thêm một lượt chỉ để đoán kiểu. CSV 10 GB = trả phí đọc 2 lần. |
| 2 lần `count()` liên tiếp | Chậm ×2 | Không có cache thì mỗi action tính lại TỪ ĐẦU (lazy evaluation — lesson 2). |
| `show()` | Nhanh | Chỉ cần vài partition đầu đủ 20 dòng là dừng. |
| `groupBy` trên 100k dòng local | Nhanh, nhưng... | Dữ liệu bé thì shuffle bé. Cùng code đó với 2 TB, shuffle là thứ quyết định sống chết — module 3. |

Câu hỏi tự vấn từ nay về sau, trước mọi dòng code: *"lệnh này có kéo dữ liệu về driver không? có gây shuffle không? có đọc lại dữ liệu không?"*

---

## 11. Spark UI

Bài này làm quen 2 tab:

**Tab Jobs** — nhìn gì:
- Mỗi dòng = 1 job = 1 action. Cột Description cho biết action nào ở dòng code nào.
- Duration: job nào ngốn thời gian nhất → nơi bắt đầu điều tra khi chậm.
- Job nhiều bất thường so với số action bạn viết? → có action ẩn (inferSchema, một số phép ghi).

**DAG Visualization** (click vào 1 job) — đọc gì:
- Mỗi khối lớn = 1 stage. Mũi tên giữa các khối = ranh giới shuffle.
- Kết luận rút ra: "code của tôi gây mấy lần shuffle" — con số này về sau chính là chỉ số đầu tiên của chi phí job.

Các tab Stages / Executors / SQL / Storage sẽ được mở khóa dần ở lesson 3, 9, 13, 18.

---

## 12. Common Mistakes

1. **`collect()` bảng lớn về driver** → driver OOM. Sai lầm kinh điển số 1 của người mới. Dùng `show()`/`take(n)`, hoặc ghi ra storage.
2. **Nghĩ code Python của mình chạy trên executor.** Không — script của bạn chạy ở driver; chỉ *task* chạy trên executor. Hiểu sai điều này thì mọi lỗi về sau đều bí ẩn.
3. **Dùng Spark cho dữ liệu 200 MB** rồi kết luận "Spark chậm". Overhead điều phối > thời gian xử lý. Chọn đúng cỡ công cụ.
4. **`inferSchema=True` trong production job** → chậm + kiểu dữ liệu trôi nổi theo nội dung file (hôm nay cột là int, mai có chữ thành string → downstream vỡ).
5. **Rắc `count()`/`show()` debug khắp nơi rồi để nguyên đưa lên production** — mỗi cái là một job chạy lại từ đầu, pipeline chậm gấp vài lần mà không hiểu vì đâu.
6. **Không bao giờ mở Spark UI**, chỉ nhìn log. UI là công cụ chẩn đoán số 1 — thói quen mở UI phải hình thành từ hôm nay.

---

## 13. Interview

**Junior:**

1. *Spark là gì, giải quyết vấn đề gì?* — Engine xử lý dữ liệu phân tán: chia dữ liệu thành partition xử lý song song trên nhiều máy, che giấu độ phức tạp phân tán (chia việc, retry, gom kết quả). Giải bài toán dữ liệu vượt sức một máy.
2. *Driver và Executor khác nhau thế nào?* — Driver: 1 process/application, chạy main(), lập kế hoạch và điều phối. Executor: nhiều process trên worker node, thực thi task trên partition và giữ cache. Driver là não, executor là cơ bắp.
3. *Cluster Manager để làm gì? Kể tên vài loại.* — Cấp phát tài nguyên (node, CPU, memory) cho application; không quan tâm logic công việc. Standalone, YARN, Kubernetes, local (dev).
4. *Spark khác Hadoop MapReduce chỗ nào?* — (a) Dữ liệu trung gian ở memory/pipeline thay vì ghi HDFS sau mỗi bước; (b) DAG các phép biến đổi tùy ý thay vì map/reduce cứng; (c) một engine cho SQL/batch/streaming/ML. Kết quả nhanh hơn 10–100× cho workload nhiều bước/lặp.

**Mid:**

5. *Phân biệt Application, Job, Stage, Task.* — Application: 1 SparkSession/spark-submit. Job: sinh ra bởi mỗi action. Stage: job cắt tại ranh giới shuffle. Task: 1 stage × 1 partition, chạy trên 1 core executor. Số task của stage = số partition đầu vào stage đó.
6. *Điều gì quyết định số task chạy đồng thời?* — Tổng core của tất cả executor (ví dụ 5 executor × 4 core = 20 task song song). Số task nhiều hơn thì xếp hàng chờ theo đợt (wave).
7. *`collect()` nguy hiểm vì sao, thay bằng gì?* — Kéo toàn bộ dữ liệu từ executors về RAM driver → OOM với dữ liệu lớn. Thay bằng `show`/`take` khi xem mẫu, `write` khi cần kết quả đầy đủ, chỉ `collect` sau khi aggregate còn ít dòng.
8. *Client mode vs cluster mode?* — Vị trí driver: client mode driver ở máy submit (tốt cho notebook/debug, chết theo máy submit); cluster mode driver chạy trong cluster (chuẩn production, sống độc lập với máy submit).

**Senior:**

9. *Executor chết giữa job — chuyện gì xảy ra? Driver chết thì sao? Vì sao khác nhau?* — Executor chết: driver phát hiện qua heartbeat, reschedule task sang executor khác; partition dữ liệu nguồn đọc lại được, cache mất thì tính lại từ lineage → job tiếp tục. Driver chết: mất toàn bộ trạng thái điều phối (DAG, task states, kết quả đăng ký) → application chết. Vì trạng thái điều phối tập trung ở driver, còn công việc của executor là stateless-recomputable.
10. *Khi nào bạn khuyên KHÔNG dùng Spark?* — (a) Dữ liệu vừa một máy (< vài chục GB): DuckDB/Polars/Postgres rẻ và nhanh hơn vì không tốn overhead phân tán; (b) latency sub-second per-event: Flink/Kafka Streams; (c) OLTP/point lookup: database. Trả lời được câu này chứng tỏ hiểu trade-off, không phải fanboy công cụ.

---

## 14. Summary

### Mindmap

```
                         SPARK LESSON 1
                              │
      ┌───────────────┬───────┴────────┬──────────────────┐
      ▼               ▼                ▼                  ▼
   TẠI SAO         KIẾN TRÚC       THỰC THI           THỰC HÀNH
      │               │                │                  │
  1 máy không đủ   Driver (não)    Application          Spark UI :4040
  MapReduce chậm   Executor (cơ)     └ Job (mỗi action)  Jobs tab
  vì disk I/O      Cluster Mgr         └ Stage (cắt tại  DAG view
  Spark: memory    (cho thuê máy)        shuffle)        collect() = cấm
  + DAG            local/YARN/K8s        └ Task (1/partition)
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại kiến trúc Driver/Executor/Cluster Manager không nhìn tài liệu.
- [ ] Giải thích được: code PySpark của tôi chạy ở driver, task chạy ở executor.
- [ ] Nói được 2 lý do Spark nhanh hơn MapReduce.
- [ ] Phân biệt Application/Job/Stage/Task và biết cái gì sinh ra job, cái gì cắt stage.
- [ ] Đã chạy lab, mở Spark UI, đếm job và xem DAG.
- [ ] Biết vì sao `collect()` và `inferSchema` là red flag.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 2 — SparkSession, RDD → DataFrame → Dataset, và Lazy Evaluation.**

Hôm nay bạn đã thấy hiện tượng lạ: gọi `filter`/`groupBy` mà "chưa có gì chạy", đến `show()` mới bùng nổ job. Đó là **lazy evaluation** — quyết định thiết kế quan trọng nhất của Spark, và là lý do Catalyst có thể tối ưu code của bạn trước khi chạy. Lesson 2 trả lời: tại sao Spark cố tình lười? RDD là gì và tại sao 2025 rồi ta vẫn phải hiểu nó dù viết DataFrame? Transformation nào narrow, nào wide — và đó chính là chìa khóa để lesson 3 giải thích cách stage được cắt.

Không hiểu lazy evaluation thì mọi bài về performance sau này đều là học vẹt — nên ta học nó ngay bây giờ.

> Gõ **"Continue"** khi sẵn sàng.
