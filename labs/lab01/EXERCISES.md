# Lab 01 — Bộ bài tập đầy đủ: Từ chạy được đến hiểu cơ chế

> Dataset: `data/olist/olist_orders_dataset.csv` — 99.441 đơn hàng Olist (e-commerce Brazil).
> Mỗi bài tập có phần **Làm** và phần **Quan sát/Trả lời** — đừng bỏ qua phần quan sát,
> đó mới là chỗ bạn thực sự học.

---

## Phần 0 — Lệnh chạy chuẩn

```bash
# Bật cluster
docker compose -f docker-compose.spark.yaml up -d

# Chạy script trên cluster
docker exec -it spark-mastery-spark-submit-1 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /workspace/labs/lab01/lab01.py

# Mở PySpark shell tương tác (dùng cho hầu hết bài tập bên dưới)
docker exec -it spark-mastery-spark-submit-1 \
  /opt/spark/bin/pyspark \
  --master spark://spark-master:7077
```

UI:
- Master UI: http://localhost:8080 — cluster có gì, app nào đang chạy
- **Application UI: http://localhost:4040** — chỉ sống khi app đang chạy. Đây là công cụ học chính.

Mẹo: shell PySpark giữ application sống liên tục → UI 4040 luôn mở → vừa gõ lệnh vừa xem UI.

---

## Level 1 — Chạy được & đọc hiểu output (Cơ bản)

### Bài 1.1 — Chạy và đối chiếu
**Làm:** Chạy `lab01.py` trên cluster (lệnh ở Phần 0).
**Quan sát:**
- Mở http://localhost:8080 NGAY khi job đang chạy: thấy application "Lab 01 - Spark Session" ở mục Running Applications, sau đó chuyển xuống Completed.
- Application dùng bao nhiêu cores? Vì sao chỉ 1? (gợi ý: xem `SPARK_WORKER_CORES` trong docker-compose)

### Bài 1.2 — inferSchema đắt như thế nào
**Làm:** Trong PySpark shell:
```python
import time

t0 = time.time()
df1 = spark.read.option("header", True).csv("/workspace/data/olist/olist_orders_dataset.csv")
print("Không inferSchema:", time.time() - t0, "giây")

t0 = time.time()
df2 = spark.read.option("header", True).option("inferSchema", True).csv("/workspace/data/olist/olist_orders_dataset.csv")
print("Có inferSchema:", time.time() - t0, "giây")
```
**Trả lời:**
1. Cái nào nhanh hơn? Chênh bao nhiêu lần?
2. `df1.printSchema()` — mọi cột là kiểu gì? Tại sao?
3. Mở UI 4040 → tab Jobs: lệnh `spark.read` với inferSchema tạo ra job — tại sao "đọc file" lại là job trong khi `read` là transformation? (gợi ý: muốn đoán kiểu dữ liệu thì phải... đọc dữ liệu thật)

### Bài 1.3 — Schema tường minh (cách làm production)
**Làm:** Định nghĩa schema bằng tay thay vì inferSchema:
```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])
orders = spark.read.option("header", True).schema(schema).csv("/workspace/data/olist/olist_orders_dataset.csv")
```
**Trả lời:** Lệnh này có tạo job nào trên UI 4040 không? So với bài 1.2 rút ra kết luận gì?

---

## Level 2 — Lazy evaluation: transformation vs action (Cốt lõi)

### Bài 2.1 — Chứng minh Spark "lười"
**Làm:** Chạy từng dòng, sau MỖI dòng mở UI 4040 → tab Jobs xem có job mới không:
```python
a = orders.filter(orders.order_status == "delivered")   # có job mới không?
b = a.select("order_id", "customer_id")                 # có job mới không?
c = b.filter(orders.order_purchase_timestamp.isNotNull())  # có job mới không?
c.count()                                               # còn bây giờ?
```
**Trả lời:**
1. Dòng nào tạo job? Vì sao 3 dòng đầu chạy xong ngay lập tức dù dữ liệu 17MB?
2. Tự phân loại: `filter`, `select`, `count`, `show`, `printSchema`, `collect`, `write` — cái nào là transformation, cái nào là action?
3. Filter sai kiểu cố tình: `orders.filter(orders.khong_ton_tai == 1)` — lỗi văng ra NGAY hay đợi đến action? Điều đó nói gì về lúc nào Spark phân tích query?

### Bài 2.2 — Đọc execution plan
**Làm:**
```python
delivered = orders.filter(orders.order_status == "delivered").select("order_id", "order_status")
delivered.explain(True)   # in cả 4 tầng plan
```
**Trả lời:**
1. Tìm 4 phần: Parsed Logical Plan → Analyzed → Optimized → Physical Plan.
2. Trong Optimized/Physical plan, tìm chữ `PushedFilters` — filter được đẩy xuống tận tầng đọc file nghĩa là gì, lợi gì?
3. Đảo thứ tự: `orders.select("order_id", "order_status").filter(...)` rồi `explain(True)` — physical plan có khác không? Rút ra: bạn viết thứ tự nào cũng được, Catalyst tự tối ưu.

### Bài 2.3 — Một dòng code, mấy job?
**Làm:** `orders.show(5)` và `orders.count()` — xem UI 4040 sau mỗi lệnh.
**Trả lời:**
1. `show(5)` đọc bao nhiêu rows (xem tab SQL → click query → xem "number of output rows" ở node scan)? Tại sao không phải 99.441?
2. `count()` thì đọc bao nhiêu? Vì sao khác `show`?
3. Click vào job của `count()` → xem DAG có 2 stage. Tại sao đếm số dòng cần 2 stage? (gợi ý: mỗi partition đếm riêng → gom kết quả về 1 chỗ)

---

## Level 3 — Partition & phân tán (Hiểu cluster thật sự làm gì)

### Bài 3.1 — Dữ liệu bị chia thành mấy mảnh?
**Làm:**
```python
orders.rdd.getNumPartitions()
```
**Trả lời:**
1. Ra số mấy? File 17MB, mặc định `spark.sql.files.maxPartitionBytes` = 128MB — giải thích con số.
2. Chạy `orders.count()` rồi vào UI 4040 → job → stage đầu tiên: số **task** của stage đó bằng đúng số partition không? Rút ra quan hệ: 1 partition = 1 task.

### Bài 3.2 — Repartition và cái giá của shuffle
**Làm:**
```python
o8 = orders.repartition(8)
o8.count()          # xem job này trên UI
orders.coalesce(1).count()
```
**Trả lời:**
1. Job của `o8.count()` có thêm stage "Exchange" — mở DAG visualization xem. Shuffle Read/Shuffle Write ở tab Stages là bao nhiêu MB?
2. `repartition` vs `coalesce` khác nhau gì? Cái nào gây shuffle?
3. Khi nào cần repartition tăng (gợi ý: file ít mà cores nhiều), khi nào coalesce giảm (gợi ý: trước khi write để đỡ ra nhiều file nhỏ)?

### Bài 3.3 — shuffle.partitions = 8 để làm gì
**Làm:**
```python
spark.conf.get("spark.sql.shuffle.partitions")   # lab01 set = 8
by_status = orders.groupBy("order_status").count()
by_status.show()
```
Xem UI: stage sau shuffle có đúng 8 task không? Rồi thử:
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")   # mặc định của Spark
orders.groupBy("order_status").count().show()
```
**Trả lời:**
1. 200 task để xử lý vài dòng kết quả (chỉ có ~8 status khác nhau) — lãng phí chỗ nào? Nhìn cột Duration của các task: đa số task làm gì?
2. Vì sao lab để 8? Quy tắc ngón tay cái cho số shuffle partitions là gì?
(Set lại 8 sau khi xong: `spark.conf.set("spark.sql.shuffle.partitions", "8")`)

---

## Level 4 — Bài tập nghiệp vụ (Intermediate của README, làm kỹ hơn)

### Bài 4.1 — Filter & select
Đếm số đơn theo từng `order_status`, sắp xếp giảm dần theo số lượng. Bao nhiêu đơn `delivered`, bao nhiêu `canceled`?

### Bài 4.2 — Cột dẫn xuất
```python
from pyspark.sql import functions as F
```
- Tạo `order_year`, `order_month` từ `order_purchase_timestamp` (dùng `F.year`, `F.month`).
- Tạo `delivery_days` = số ngày từ lúc mua đến lúc giao (`F.datediff` giữa `order_delivered_customer_date` và `order_purchase_timestamp`).
- Tạo `is_late` = giao trễ hơn `order_estimated_delivery_date` (kiểu boolean).

### Bài 4.3 — Aggregation
- Số đơn theo từng năm — năm nào nhiều nhất?
- `delivery_days` trung bình, min, max theo năm (`F.avg`, `F.min`, `F.max` trong `.agg()`).
- Tỷ lệ giao trễ (%) theo tháng — tháng nào tệ nhất? (gợi ý: `F.avg(F.col("is_late").cast("int"))`)
- Null check: bao nhiêu đơn thiếu `order_delivered_customer_date`? Đối chiếu với `order_status` của chúng — có hợp lý không?

### Bài 4.4 — Ghi Parquet và so sánh
```python
result.write.mode("overwrite").parquet("/workspace/data/output/lab01/orders_by_year")
```
**Quan sát:**
1. `ls data/output/lab01/orders_by_year/` trên máy host — bao nhiêu file part? Con số đó từ đâu ra?
2. Đọc lại bằng `spark.read.parquet(...)` — `printSchema()` có cần inferSchema không? Vì sao Parquet không cần?
3. So kích thước: CSV 17MB vs thư mục Parquet — nhỏ hơn bao nhiêu lần? Vì sao (columnar + compression)?
4. Thử `.coalesce(1).write...` — giờ ra mấy file?

---

## Level 5 — Nâng cao / thử thách

### Bài 5.1 — local vs cluster, nhìn tận mắt
Chạy cùng script 2 lần:
```bash
# lần 1
spark-submit --master local[2] /workspace/labs/lab01/lab01.py
# lần 2
spark-submit --master spark://spark-master:7077 /workspace/labs/lab01/lab01.py
```
Xem tab Executors trên UI 4040 (hoặc log): local mode có executor tên gì? Cluster mode executor nằm ở host nào? Driver nằm ở container nào trong 2 trường hợp?

### Bài 5.2 — Cái bẫy collect()
`orders.collect()` kéo toàn bộ 99k rows về driver. Với data này thì sống, nhưng:
1. Giải thích tại sao `collect()` trên bảng 1TB sẽ giết driver trong khi `count()` thì không — dữ liệu đi đường nào trong mỗi trường hợp?
2. Khi nào `collect()` là hợp lệ? (gợi ý: sau khi aggregate còn vài chục dòng)

### Bài 5.3 — Cache
```python
delivered = orders.filter(orders.order_status == "delivered")
delivered.count()   # lần 1
delivered.count()   # lần 2 — có nhanh hơn không? Vì sao KHÔNG? (Spark không tự cache!)
delivered.cache()
delivered.count()   # lần này mới nạp cache
delivered.count()   # lần này mới nhanh
```
Xem tab Storage trên UI 4040: DataFrame chiếm bao nhiêu MB memory? So với 17MB file gốc, vì sao khác?

### Bài 5.4 — Mini-project chốt lab
Viết `labs/lab01/lab01_report.py` hoàn chỉnh, chạy được bằng spark-submit, làm:
1. Đọc orders với **schema tường minh** (không inferSchema).
2. Làm sạch: chỉ giữ đơn có `order_purchase_timestamp` không null.
3. Tính bảng report: theo (year, month) → tổng đơn, số đơn delivered, số đơn canceled, avg delivery_days, % giao trễ.
4. Ghi Parquet, partition theo năm: `.write.partitionBy("order_year").parquet(...)`.
5. Đọc lại Parquet, filter năm 2017 — xem `explain()` có dòng `PartitionFilters` không: đó là partition pruning, nền tảng của mọi data lake.

---

## Checklist tự kiểm tra trước khi sang Lab 02

Bạn phải trả lời trơn tru được các câu này (không nhìn tài liệu):

1. Driver làm gì, executor làm gì, master (cluster manager) làm gì? Cái nào chạy code Python của bạn?
2. Transformation vs action — vì sao Spark thiết kế lazy? Lợi ích cụ thể?
3. Job → Stage → Task: cái gì sinh ra job, cái gì cắt stage (shuffle!), task tương ứng với cái gì (partition!)?
4. Vì sao production không dùng inferSchema?
5. `spark.sql.shuffle.partitions` ảnh hưởng gì, để bao nhiêu là hợp lý với data nhỏ?
6. Vì sao Parquet tốt hơn CSV cho analytics?
