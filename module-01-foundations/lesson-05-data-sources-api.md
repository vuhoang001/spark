# Lesson 5 — Đọc/ghi dữ liệu: Data Sources API

> Module 1 · Foundations · Tuần 3 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3.5h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **DataFrameReader / DataFrameWriter** — hai cánh cửa duy nhất để dữ liệu ra/vào Spark, và anatomy của chúng.
- Ưu nhược của từng format: **CSV, JSON, Parquet, ORC, JDBC** — và khi nào chọn cái nào.
- Khai **schema tường minh bằng StructType** — kỹ năng phân biệt junior với người làm production.
- Ba **read mode** khi gặp dữ liệu bẩn: `PERMISSIVE` / `DROPMALFORMED` / `FAILFAST`, và cột `_corrupt_record`.
- Bốn **save mode** khi ghi: `append` / `overwrite` / `errorifexists` / `ignore` — chọn sai là mất dữ liệu thật.
- Ba phép tối ưu I/O nền tảng: **predicate pushdown, column pruning, partition pruning** — định nghĩa + demo bằng `explain()`.
- **JDBC partitioned read**: đọc database bằng nhiều connection song song.
- **`partitionBy` khi ghi**: đặt nền cho partition pruning ở downstream.

Sau bài này bạn phải làm được:

- Viết job đọc CSV bẩn mà không chết giữa chừng, và biết chính xác những dòng hỏng đi đâu.
- Nhìn `explain()` và chỉ ra: filter này có được đẩy xuống tầng đọc file không, cột nào bị cắt bỏ.
- Giải thích cho đồng nghiệp tại sao `overwrite` một bảng phân vùng có thể xóa sạch dữ liệu cũ — và cách phòng.

Kiến thức dùng trong thực tế: **mỗi ngày**. Một DE trung bình 70% thời gian là đọc chỗ này, biến đổi, ghi chỗ kia. Đọc/ghi sai là loại bug âm thầm nhất: job vẫn xanh, dashboard vẫn lên, nhưng số liệu sai.

---

## 2. Why

### Vấn đề: dữ liệu không bao giờ nằm sẵn trong Spark

Spark là **compute engine, không phải storage**. Nó không "chứa" dữ liệu — mọi job đều bắt đầu bằng đọc từ đâu đó (file, database, Kafka) và kết thúc bằng ghi ra đâu đó. Tầng đọc/ghi này quyết định:

1. **Tốc độ**: đọc 100 GB CSV mất 10 phút; cùng dữ liệu đó ở Parquet + chỉ lấy 3 cột + filter theo ngày có thể mất 10 giây. Chênh **60×** mà chưa cần tuning gì cả.
2. **Độ đúng**: CSV có dòng hỏng, JSON có field lúc có lúc không, database có NULL bất ngờ. Xử lý sai ở cửa vào thì mọi tầng sau đều sai.
3. **Chi phí**: trên cloud, I/O + scan là tiền thật (S3 request, BigQuery bytes scanned). Đọc thừa = đốt tiền.

### Analogy: nhập hàng vào kho

Hãy coi Spark job như một **kho hàng**:

- **DataFrameReader** là cổng nhập: kiểm tra giấy tờ (schema), quyết định làm gì với kiện hàng rách (read mode), chỉ nhận đúng loại hàng cần (column pruning), từ chối cả xe tải nếu không liên quan (partition pruning — xe không cần vào kho luôn).
- **DataFrameWriter** là cổng xuất: xếp hàng lên kệ theo khu (partitionBy), và có quy tắc rõ ràng khi kệ đã có hàng cũ (save mode).

Kho vận hành tốt hay không nằm ở hai cái cổng này trước tiên.

### Nếu làm ẩu thì sao?

- `inferSchema=True` production: hôm nay cột `price` toàn số → double; mai có một dòng ghi `"N/A"` → cả cột thành string → job downstream `sum(price)` nổ.
- Không khai read mode: mặc định `PERMISSIVE` lặng lẽ biến dòng hỏng thành NULL — bạn mất dữ liệu mà **không có một dòng log nào**.
- `mode("overwrite")` nhầm chỗ: xóa trắng 2 năm dữ liệu trong 1 lệnh. Chuyện này xảy ra ở công ty thật, người thật, thường vào chiều thứ Sáu.

### Trade-off tổng quát (Senior phải thuộc)

| Được | Mất |
|---|---|
| Một API thống nhất cho mọi nguồn (`spark.read.format(...)`) | Mỗi format có option riêng, hành vi mặc định riêng — phải học từng cái |
| Pushdown/pruning tự động với format thông minh (Parquet/ORC/JDBC) | Format "ngu" (CSV/JSON) không hưởng gì — chọn format là chọn tốc độ |
| Schema tường minh = hợp đồng dữ liệu ổn định | Tốn công viết StructType, phải cập nhật khi nguồn đổi (nhưng đó là feature: đổi schema PHẢI có người biết) |

---

## 3. Theory

### 3.1. Anatomy của DataFrameReader

Mọi lệnh đọc đều là chuỗi 4 mảnh này:

```
spark.read                     ← lấy DataFrameReader từ SparkSession
     .format("csv")            ← ĐỌC GÌ: csv/json/parquet/orc/jdbc/text/...
     .schema(schema)           ← HÌNH DẠNG: StructType tường minh (hoặc inferSchema)
     .option("header", True)   ← TÙY CHỌN: mỗi format một bộ option riêng
     .option("mode", "FAILFAST")
     .load("/path/to/data")    ← Ở ĐÂU: đường dẫn / bảng. Trả về DataFrame (lazy!)
```

- Các shortcut `spark.read.csv(path)`, `.parquet(path)`, `.json(path)` chỉ là đường tắt của `format(...).load(...)`.
- **`load()` là transformation, không phải action** — chưa đọc dữ liệu thật. Spark chỉ liệt kê file + lấy schema. Dữ liệu chỉ chảy khi gặp action.

### 3.2. Anatomy của DataFrameWriter

```
df.write                        ← lấy DataFrameWriter từ DataFrame
  .format("parquet")            ← GHI GÌ
  .mode("append")               ← KỆ ĐÃ CÓ HÀNG THÌ SAO: append/overwrite/errorifexists/ignore
  .partitionBy("order_date")    ← XẾP KHO THEO KHU NÀO (tùy chọn)
  .option("compression", "snappy")
  .save("/path/output")         ← ĐÂY LÀ ACTION — job chạy thật
```

### 3.3. So găng các format

| Format | Kiểu lưu | Schema | Splittable* | Pushdown/Pruning | Nên dùng khi |
|---|---|---|---|---|---|
| **CSV** | Row, text | Không có (đoán hoặc khai tay) | Có (uncompressed) | ❌ Không | Nhận dữ liệu từ bên ngoài, xuất cho Excel. **Chỉ ở rìa hệ thống** |
| **JSON** | Row, text | Tự mô tả từng dòng (đắt) | Có (JSON Lines) | ❌ Không | Dữ liệu nested từ API/Kafka/log. Cũng chỉ ở rìa |
| **Parquet** | **Columnar, binary** | Nằm trong file (footer) | Có | ✅ Đủ bộ | **Mặc định cho mọi tầng bên trong pipeline** |
| **ORC** | Columnar, binary | Trong file | Có | ✅ Đủ bộ | Hệ sinh thái Hive cũ; ngoài đó Parquet phổ biến hơn |
| **JDBC** | Bảng database | Lấy từ DB | Phải tự chia (3.8) | ✅ Filter thành WHERE | Đọc/ghi thẳng RDBMS lượng vừa phải |

\* *Splittable = một file lớn có thể cắt cho nhiều task đọc song song. Lưu ý bẫy kinh điển: **CSV/JSON nén gzip là KHÔNG splittable** — file `data.csv.gz` 10 GB sẽ do đúng 1 task đọc, 1 core cày, cluster 40 core ngồi nhìn.*

Điểm chết của CSV/JSON không chỉ là "to hơn": vì là **text theo dòng**, muốn lấy 1 cột vẫn phải đọc + parse **cả dòng**; muốn biết dòng có qua filter không cũng phải parse xong đã. Parquet thì khác — lesson 6 mổ xẻ tận xương.

### 3.4. Schema tường minh với StructType

```python
from pyspark.sql.types import (StructType, StructField,
                               StringType, TimestampType)

orders_schema = StructType([
    StructField("order_id",                      StringType(),    False),
    StructField("customer_id",                   StringType(),    False),
    StructField("order_status",                  StringType(),    True),
    StructField("order_purchase_timestamp",      TimestampType(), True),
    StructField("order_approved_at",             TimestampType(), True),
    StructField("order_delivered_carrier_date",  TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])
```

Ba lý do đây là chuẩn production (nhắc lại từ lesson 1, giờ có đủ ngữ cảnh):

1. **Nhanh**: bỏ được lượt đọc thứ hai của `inferSchema`.
2. **Ổn định**: kiểu dữ liệu là **hợp đồng**, không trôi theo nội dung file hôm nay.
3. **Phát hiện lỗi sớm**: dữ liệu không khớp schema → lộ ra ngay cửa vào (tùy read mode), thay vì nổ ở tầng gold ba ngày sau.

Lưu ý: tham số thứ ba (`nullable`) với file source chủ yếu mang tính **tài liệu/hợp đồng** — Spark không cưỡng chế chặn NULL khi đọc CSV. Muốn cưỡng chế thật thì tự viết quality check (lesson 14).

### 3.5. Read mode — dữ liệu bẩn đi đâu?

Áp dụng cho nguồn text (CSV/JSON) khi một dòng không parse được theo schema:

```
                     dòng hỏng xuất hiện
                            │
        ┌───────────────────┼────────────────────┐
        ▼                   ▼                    ▼
   PERMISSIVE          DROPMALFORMED         FAILFAST
   (mặc định)               │                    │
        │              lặng lẽ VỨT          ném exception
   giữ dòng, field     cả dòng              CHẾT NGAY
   hỏng → NULL;             │                    │
   nguyên văn dòng     "sạch" nhưng         "thà chết còn
   vào cột             mất dữ liệu          hơn sai" — hợp
   _corrupt_record     không dấu vết        pipeline tài chính
   (nếu khai)
```

| Mode | Triết lý | Rủi ro |
|---|---|---|
| `PERMISSIVE` | "Cho qua hết, đánh dấu chỗ hỏng" | Không ai nhìn `_corrupt_record` thì NULL lan âm thầm |
| `DROPMALFORMED` | "Vứt rác trước cửa" | **Mất dữ liệu không dấu vết** — gần như không bao giờ nên dùng ở production |
| `FAILFAST` | "Có rác là dừng nhà máy" | Một dòng hỏng chặn cả pipeline — cần quy trình xử lý khi nó nổ |

Muốn dùng `_corrupt_record` phải **khai nó trong schema** (StringType). Pattern production tử tế: `PERMISSIVE` + tách dòng hỏng ra bảng quarantine + alert khi số dòng hỏng vượt ngưỡng (demo ở mục 6).

### 3.6. Save mode — kệ đã có hàng thì sao?

| Mode | Hành vi khi đích đã tồn tại | Dùng khi |
|---|---|---|
| `errorifexists` (mặc định) | Ném lỗi, không ghi | Ghi một lần, muốn an toàn tuyệt đối |
| `append` | Ghi thêm file mới vào | Ingest theo batch/ngày. **Chạy lại = dữ liệu đôi** (không idempotent!) |
| `overwrite` | **XÓA đích rồi ghi mới** | Rebuild bảng. Đọc kỹ cảnh báo dưới |
| `ignore` | Đích tồn tại → lặng lẽ không làm gì | Hiếm; kiểu "ghi nếu chưa có". Nguy hiểm vì im lặng |

⚠️ **Cảnh báo trị giá một chức vụ**: với bảng phân vùng, `overwrite` mặc định (`spark.sql.sources.partitionOverwriteMode=static`) xóa **TOÀN BỘ thư mục đích**, kể cả các partition bạn không hề động tới. Ghi lại mỗi ngày hôm qua mà bay luôn 2 năm dữ liệu là vì đây. Chuyển sang `dynamic` để chỉ ghi đè những partition có mặt trong DataFrame:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

### 3.7. Bộ ba tối ưu I/O — định nghĩa chuẩn

Cả ba cùng một triết lý: **đừng đọc thứ không cần**. Khác nhau ở việc né *chiều nào* của dữ liệu:

```
                        bảng logic
              cột A   cột B   cột C   cột D
            ┌───────┬───────┬───────┬───────┐
 partition  │▓▓▓▓▓▓▓│▓▓▓▓▓▓▓│▓▓▓▓▓▓▓│▓▓▓▓▓▓▓│ ← partition pruning:
 dt=07-01   │▓▓▓▓▓▓▓│▓▓▓▓▓▓▓│▓▓▓▓▓▓▓│▓▓▓▓▓▓▓│   filter dt='07-02'
            ├───────┼───────┼───────┼───────┤   → CẢ THƯ MỤC 07-01
 partition  │       │░░░░░░░│       │░░░░░░░│     không được liệt kê
 dt=07-02   │       │░░░░░░░│       │░░░░░░░│
            └───────┴───────┴───────┴───────┘
                        ▲                ▲
              column pruning:       predicate pushdown:
              chỉ đọc cột B, D      trong phần còn lại, điều kiện
              (▓/░ = bị bỏ qua)     `price > 100` được đẩy xuống
                                    tầng đọc file để né tiếp
                                    từng khối dữ liệu (row group)
```

- **Column pruning** (cắt cột): bạn `select("b", "d")` → tầng scan chỉ đọc 2 cột đó từ file. Chỉ format columnar làm được — CSV muốn cột nào cũng phải parse cả dòng.
- **Predicate pushdown** (đẩy điều kiện lọc xuống): filter được đẩy từ tầng logic xuống **tầng data source**, để nguồn tự né dữ liệu: Parquet dùng min/max statistics bỏ qua cả row group (lesson 6); JDBC biến filter thành `WHERE` chạy trên database.
- **Partition pruning** (cắt phân vùng): dữ liệu ghi theo `partitionBy("dt")` nằm trong thư mục `dt=.../`. Filter trên cột partition → Spark **không thèm liệt kê** thư mục ngoài phạm vi. Rẻ nhất trong ba loại vì né từ tầng danh sách file.

Cách kiểm chứng duy nhất đáng tin: đọc **physical plan**. Trong `df.explain()`, nhìn node `FileScan`:

- `PushedFilters: [IsNotNull(price), GreaterThan(price,100.0)]` → pushdown hoạt động.
- `ReadSchema: struct<b:double,d:string>` → column pruning hoạt động.
- `PartitionFilters: [isnotnull(dt), (dt = 2018-07-02)]` → partition pruning hoạt động.

### 3.8. JDBC partitioned read — đọc database bằng N connection

Mặc định `spark.read.jdbc(...)` mở **đúng 1 connection**, đọc tuần tự → 1 partition, 1 task. Bảng 50 triệu dòng: một core cày, cả cluster ngồi chơi. Cách chia việc:

```python
df = (spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://postgres:5432/olist")
      .option("dbtable", "public.orders")
      .option("user", "spark").option("password", "***")
      .option("partitionColumn", "order_sk")   # cột numeric/date/timestamp
      .option("lowerBound", "1")
      .option("upperBound", "10000000")
      .option("numPartitions", "8")
      .load())
```

Spark cắt khoảng `[lowerBound, upperBound]` thành 8 dải, sinh **8 query song song**:

```
task 0: WHERE order_sk <  1250000  (hoặc IS NULL)
task 1: WHERE order_sk >= 1250000 AND order_sk < 2500000
...
task 7: WHERE order_sk >= 8750000            ← dải cuối hứng cả phần > upperBound
```

Ba điều junior hay hiểu nhầm:

1. `lowerBound`/`upperBound` **không lọc dữ liệu** — chỉ quyết định cách cắt dải. Dòng ngoài khoảng vẫn được đọc (rơi vào dải đầu/cuối).
2. Cột chia phải **phân bố đều**. Chia theo cột lệch (90% giá trị dồn một khúc) → một task ôm gần hết bảng: skew ngay từ cửa đọc.
3. `numPartitions=100` = 100 connection đồng thời **đấm vào database production**. DBA sẽ tìm bạn. Cân nhắc connection pool của DB, và tốt nhất đọc từ replica.

---

## 4. Internal

Chuyện gì xảy ra khi bạn gọi `spark.read.parquet(path).filter(...).select(...).count()`:

```
① spark.read...load(path)          [driver]
     Liệt kê file trong path (file listing) — path nhiều
     partition thì chỉ liệt kê phần qua được PartitionFilters.
     Lấy schema: Parquet đọc FOOTER vài file (KB) — không đọc data.
     (CSV + inferSchema: đọc CẢ dữ liệu một lượt → job ẩn bạn thấy ở lab01)
        │
② filter/select                    [driver]
     Chỉ ghi vào logical plan. Chưa có byte dữ liệu nào di chuyển.
        │
③ Gặp action count()               [driver]
     Catalyst tối ưu logical plan. Hai rule quan trọng hôm nay:
     • PushDownPredicate: kéo Filter xuống sát nguồn đọc
     • ColumnPruning:     cắt cột không ai dùng khỏi ReadSchema
     Physical plan sinh node FileScan mang theo:
     PushedFilters / PartitionFilters / ReadSchema
        │
④ Chia partition đầu vào           [driver]
     File lớn cắt thành split ~spark.sql.files.maxPartitionBytes (128 MB);
     file bé gộp lại. Mỗi split → 1 task.
        │
⑤ Task chạy trên executor          [executor]
     Mở file, đọc footer/metadata → dùng PushedFilters bỏ qua khối
     không thể chứa dòng thỏa mãn (Parquet: skip row group) →
     chỉ giải mã các cột trong ReadSchema → trả kết quả.
```

Còn phía ghi: mỗi task ghi **file riêng** (`part-00000-...`, `part-00001-...`) — vì thế "một lần ghi Parquet" thực ra là một **thư mục** nhiều file, số file ≈ số task cuối cùng. Với `partitionBy("dt")`, mỗi task ghi một file **cho mỗi giá trị dt nó gặp** → 200 task × 365 ngày = tiềm năng 73.000 file bé tí. Đây là cội nguồn của **small files problem** (lesson 21) — thuốc chữa tạm thời: `repartition("dt")` trước khi ghi.

Với JDBC: mỗi task giữ 1 connection, kéo dòng theo `fetchsize` (mặc định thấp, chỉnh lên 1000–10000 cho Postgres). Pushdown với JDBC nghĩa là filter thành mệnh đề `WHERE` trong query gửi xuống DB — còn aggregate (`groupBy`) thì **không** được đẩy xuống (trừ khi bạn tự viết subquery vào `dbtable`/option `query`).

---

## 5. API

### `spark.read.format(...).schema(...).options(...).load(path)`

```python
df = (spark.read.format("csv")
      .schema(orders_schema)
      .option("header", True)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("mode", "PERMISSIVE")
      .load("/workspace/data/olist/olist_orders_dataset.csv"))
```

- **Ý nghĩa**: cấu hình rồi tạo DataFrame lazy trỏ vào nguồn.
- **Pitfall**: option là **string-typed và không được validate tên** — gõ nhầm `.option("haeder", True)` Spark im lặng bỏ qua, và bạn ngồi 30 phút tự hỏi tại sao dòng header thành dữ liệu.

### `StructType` / `StructField`

- **Ý nghĩa**: định nghĩa schema tường minh (xem 3.4).
- **Khi dùng**: mọi nguồn text (CSV/JSON) ở production. Parquet/ORC không cần — schema nằm trong file.
- **Pitfall**: với CSV, Spark map cột **theo vị trí**, không theo tên — file đổi thứ tự cột mà schema không đổi theo là dữ liệu "lệch hàng" toàn bộ. Với JSON thì map theo tên. Khác biệt này từng gây sự cố thật ở nhiều nơi.
- **Mẹo**: cần viết nhanh schema dài? Chạy `inferSchema` MỘT LẦN ở máy dev, in `df.schema` ra, dán vào code thành tường minh. Đoán một lần, đóng đinh mãi mãi.

### `option("mode", ...)` + `_corrupt_record`

```python
schema_with_corrupt = StructType(orders_schema.fields + [
    StructField("_corrupt_record", StringType(), True)
])
```

- **Pitfall 1**: quên khai `_corrupt_record` trong schema → PERMISSIVE vẫn chạy nhưng bạn **không có cách nào** nhìn thấy dòng hỏng.
- **Pitfall 2** (Spark 2.3+): query **chỉ đụng tới** cột `_corrupt_record` (`df.filter(col("_corrupt_record").isNotNull()).count()`) sẽ ném `AnalysisException` — Spark từ chối vì kết quả không đáng tin khi các cột khác bị prune. Fix: `df.cache()` trước, hoặc select thêm cột khác.

### `df.write.mode(...).partitionBy(...).save(path)`

```python
(df.withColumn("order_date", F.to_date("order_purchase_timestamp"))
   .repartition("order_date")               # gom mỗi ngày về 1 task → né small files
   .write.mode("overwrite")
   .partitionBy("order_date")
   .parquet("/workspace/data/output/lesson05/orders_by_date"))
```

- **Ý nghĩa**: ghi thư mục con `order_date=2018-07-02/` cho từng giá trị. Cột partition **bị bỏ khỏi file dữ liệu** (nằm ở tên thư mục), khi đọc lại Spark tự dựng lại cột.
- **Pitfall 1**: partitionBy theo cột **high-cardinality** (user_id, order_id) → hàng triệu thư mục, mỗi cái vài KB → file listing giết cả pipeline. Quy tắc: partition theo cột dùng để filter thường xuyên, cardinality vài trăm đến vài nghìn (ngày, tháng, quốc gia).
- **Pitfall 2**: `overwrite` + static mode xóa cả thư mục — đọc lại 3.6 lần nữa. Nghiêm túc đấy.

### `spark.read.jdbc` với 4 option chia dải

- (xem 3.8) **Pitfall**: quên 4 option → 1 connection, 1 task, "Spark sao chậm thế". Mở Spark UI thấy stage đọc có đúng 1 task là bắt được bệnh ngay.

### `option("query", ...)` — đẩy hẳn SQL xuống DB

```python
.option("query", "SELECT id, total FROM orders WHERE created_at >= '2018-01-01'")
```

- **Ý nghĩa**: để database làm phần lọc/join nhỏ trước, Spark chỉ nhận kết quả.
- **Pitfall**: `query` **không dùng chung** được với `partitionColumn` (Spark 3.4 ném lỗi) — cần cả hai thì viết subquery vào `dbtable`: `.option("dbtable", "(SELECT ...) AS t")`.

---

## 6. Demo nhỏ

Pattern production: PERMISSIVE + quarantine dòng hỏng.

```
Input:  CSV 5 dòng, trong đó 1 dòng hỏng (chữ trong cột số)
   ↓    đọc PERMISSIVE + _corrupt_record
   ↓    tách: dòng sạch → xử lý tiếp | dòng hỏng → bảng quarantine
Output: 2 DataFrame + con số để alert
```

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("demo05").master("local[2]").getOrCreate()

csv_lines = [
    "order_id,city,amount",
    "o1,HN,120.5",
    "o2,SG,80.0",
    "o3,DN,not_a_number",      # ← dòng hỏng
    "o4,HN,300.0",
]
path = "/tmp/demo05_dirty.csv"
with open(path, "w") as f:
    f.write("\n".join(csv_lines))

schema = StructType([
    StructField("order_id", StringType()),
    StructField("city", StringType()),
    StructField("amount", DoubleType()),
    StructField("_corrupt_record", StringType()),   # bắt buộc khai!
])

df = (spark.read.schema(schema)
      .option("header", True).option("mode", "PERMISSIVE")
      .csv(path)).cache()                            # cache: né AnalysisException (mục 5)

good = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
bad  = df.filter(F.col("_corrupt_record").isNotNull())

print(f"sạch: {good.count()} | hỏng: {bad.count()}")  # sạch: 3 | hỏng: 1
bad.select("_corrupt_record").show(truncate=False)
# +---------------------+
# |o3,DN,not_a_number   |   ← nguyên văn dòng hỏng, đem đi điều tra được
# +---------------------+
spark.stop()
```

Đổi `PERMISSIVE` thành `DROPMALFORMED` chạy lại: `o3` biến mất **không kèn không trống**. Đổi thành `FAILFAST`: exception ngay từ action đầu tiên. Ba triết lý, bạn vừa chứng kiến cả ba.

---

## 7. Production Example

Nhìn lại pipeline `kafka-flink` của bạn dưới lăng kính Data Sources API — mỗi mũi tên là một quyết định đọc/ghi:

```
PostgreSQL ──(JDBC partitioned read, backfill lịch sử)──▶ Spark
Kafka      ──(streaming source, module 4)───────────────▶ Spark
                                                            │
                    bronze: append, giữ nguyên raw + _corrupt_record
                            partitionBy(ingestion_date)
                                                            │
                    silver: overwrite từng partition (dynamic mode!)
                            schema tường minh = data contract
                                                            ▼
                                              Parquet/Iceberg trên MinIO
                                                            ▼
                                              Trino đọc — hưởng trọn
                                              pruning + pushdown mà
                                              tầng ghi đã chuẩn bị
```

Ba bài học doanh nghiệp trả tiền để học:

1. **Backfill lịch sử** không đi qua Kafka mà JDBC đọc thẳng từ **replica** của Postgres, chia 8–16 partition theo primary key, chạy ban đêm — vừa nhanh vừa không đè DB phục vụ khách.
2. **Bronze luôn `append` + giữ dòng hỏng**: tầng bronze là "chứng cứ pháp lý", có sai ở silver thì replay lại được. Không ai `DROPMALFORMED` ở bronze cả.
3. **Silver ghi đè theo partition với dynamic mode**: chạy lại ngày hỏng chỉ đè đúng ngày đó — đây chính là idempotency mà Airflow re-run cần (lesson 36).

---

## 8. Hands-on Lab

**Mục tiêu**: đọc Olist với schema tường minh, chứng kiến pushdown/pruning bằng `explain()`, ghi Parquet phân vùng.

> Thư mục `labs/lab05/` đã có bài streaming cũ của bạn (`lab05.py`) — **đừng đụng vào**, ta tạo file mới.

### Bước 1 — `labs/lab05/lesson05_read_modes.py`

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               TimestampType)

spark = SparkSession.builder.appName("lesson05-read").getOrCreate()

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True),
])

orders = (spark.read.schema(orders_schema)
          .option("header", True).option("mode", "PERMISSIVE")
          .csv("/workspace/data/olist/olist_orders_dataset.csv")).cache()

bad = orders.filter(F.col("_corrupt_record").isNotNull())
print(f"Tổng: {orders.count():,} | hỏng: {bad.count():,}")
bad.select("_corrupt_record").show(5, truncate=False)

good = orders.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
good.write.mode("overwrite").parquet("/workspace/data/output/lesson05/orders_clean")
spark.stop()
```

Chạy: `make run-local F=labs/lab05/lesson05_read_modes.py`

### Bước 2 — `labs/lab05/lesson05_pushdown.py`

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lesson05-pushdown").getOrCreate()
pq = spark.read.parquet("/workspace/data/output/lesson05/orders_clean")

q = (pq.filter(F.col("order_status") == "delivered")
       .select("order_id", "order_purchase_timestamp"))
q.explain(mode="formatted")   # ← tìm PushedFilters + ReadSchema trong output

# Ghi phân vùng theo ngày
by_date = (pq.withColumn("order_date", F.to_date("order_purchase_timestamp"))
             .filter(F.col("order_date").isNotNull())
             .repartition("order_date"))
(by_date.write.mode("overwrite").partitionBy("order_date")
        .parquet("/workspace/data/output/lesson05/orders_by_date"))

# Đọc lại có filter cột partition → tìm PartitionFilters trong plan
pruned = (spark.read.parquet("/workspace/data/output/lesson05/orders_by_date")
          .filter(F.col("order_date") == "2018-07-02"))
pruned.explain(mode="formatted")
print(f"Đơn ngày 2018-07-02: {pruned.count()}")
spark.stop()
```

Chạy: `make run F=labs/lab05/lesson05_pushdown.py` (cluster mode — tiện thể xem master UI :8080).

### Bước 3 — quan sát (quan trọng nhất)

1. Trong output `explain` thứ nhất: chép lại dòng `PushedFilters: [...]` và `ReadSchema: ...`. Cột nào KHÔNG có trong ReadSchema? Đó là column pruning bằng xương bằng thịt.
2. Trong `explain` thứ hai: tìm `PartitionFilters: [... (order_date = 2018-07-02)]`. So sánh `number of files read` trong Spark UI (tab SQL / node Scan) giữa có filter và không filter.
3. Ngó thư mục output: `ls data/olist/../output/lesson05/orders_by_date | head` — thấy `order_date=.../`. Đếm số thư mục: `ls ... | wc -l` (~600 ngày). Mỗi thư mục mấy file? Nhờ `repartition("order_date")` nên là 1.
4. Ghi 3 quan sát vào `labs/lab05/NOTES-lesson05.md`.

### (Tùy chọn) Bước 4 — JDBC với Postgres từ repo kafka-flink

Nếu Postgres của `../kafka-flink` đang chạy chung network Docker: viết `lesson05_jdbc.py` đọc một bảng với `numPartitions=4` (cần JDBC driver: thêm `--jars postgresql-42.x.jar` hoặc `spark.jars.packages org.postgresql:postgresql:42.6.0`). Mở Spark UI xác nhận stage đọc có 4 task. Không có hạ tầng thì bỏ qua — Mini Project 1 không cần JDBC.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Phân biệt predicate pushdown, column pruning, partition pruning — mỗi cái né chiều nào của dữ liệu, xảy ra ở tầng nào?
2. Chọn format cho 3 tình huống, nêu lý do: (a) nhận file đối tác gửi qua SFTP hàng ngày; (b) bảng silver 500 GB được 5 team query; (c) log event nested từ mobile app đổ về.
3. Tại sao `DROPMALFORMED` gần như bị cấm ở production?

**Medium** — Đọc `olist_order_items_dataset.csv` bằng schema tường minh (tự viết StructType — chú ý `price`, `freight_value` là Double, `shipping_limit_date` là Timestamp). Đọc cùng file với `inferSchema=True`. So sánh `df.schema` hai bên — inferSchema đoán sai/khác chỗ nào? Đo thời gian hai cách đọc (bọc `time.time()` quanh read + count).

**Hard** — Ghi `order_items` ra 2 phiên bản Parquet: (a) không partition, (b) `partitionBy` theo tháng của `shipping_limit_date`. Chạy cùng query "tổng `price` của tháng 2018-03" trên cả hai, so `number of files read` + thời gian trong Spark UI. Sau đó cố tình dùng `mode("overwrite")` static ghi lại CHỈ tháng 2018-04 vào bản (b) — chuyện gì xảy ra với các tháng khác? Bật `partitionOverwriteMode=dynamic` làm lại. Viết 5 dòng kết luận.

**Production Challenge** — Thiết kế (chỉ viết code + giải thích, chưa cần chạy): job backfill đọc bảng `orders` 200 triệu dòng từ Postgres production. Quyết định và biện luận: đọc từ đâu (primary/replica), partitionColumn chọn cột gì, numPartitions bao nhiêu, fetchsize, chạy giờ nào, save mode gì ở đích. Liệt kê 3 thứ có thể sập và cách phòng.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Đọc Parquet, select 3/20 cột | Nhanh | Column pruning: chỉ giải mã 3 cột. CSV cùng nội dung: parse đủ 20 |
| Filter cột partition (`dt='...'`) | Rất nhanh | Pruning ở tầng liệt kê file — dữ liệu ngoài phạm vi còn không được mở |
| `.csv.gz` một file lớn | Rất chậm | gzip không splittable → 1 task đọc tất |
| JDBC không chia partition | Chậm | 1 connection tuần tự. Nhìn Spark UI: stage 1 task là thủ phạm |
| `partitionBy` cột triệu giá trị | Thảm họa | Triệu thư mục + triệu file bé → listing chậm hơn cả đọc dữ liệu |
| `inferSchema` trên CSV lớn | Chậm | Đọc 2 lượt (đã biết từ lesson 1 — giờ bạn có thuốc: StructType) |

Câu tự vấn của bài này, trước mọi lệnh đọc: *"filter và select của tôi có được đẩy xuống tầng đọc không — hay Spark đang bê cả bảng lên rồi mới lọc?"* Trả lời bằng `explain()`, không bằng niềm tin.

---

## 11. Spark UI

Bài này mở khóa **tab SQL / DataFrame** — từ nay là tab bạn mở đầu tiên khi điều tra:

- Click vào query → sơ đồ physical plan dạng khối. Tìm node **Scan parquet/csv**.
- Hover/expand node Scan, đọc các con số nói thật:
  - `number of files read` / `size of files read`: partition pruning có ăn không — filter đúng cột partition thì con số này phải tụt thảm hại.
  - `PushedFilters`, `ReadSchema`: hai bằng chứng của pushdown + pruning cột.
  - `rows output`: so với tổng số dòng — bao nhiêu bị lọc ngay tại scan.
- Tab **Jobs**: lệnh ghi `partitionBy` sinh job ghi; đối chiếu số task với số file output.
- Tab **Stages** với JDBC: số task của stage đọc = `numPartitions`. 1 task = bạn quên chia dải.

---

## 12. Common Mistakes

1. **`inferSchema=True` trong production** — chậm gấp đôi + kiểu trôi theo dữ liệu. Thuốc: StructType (và mẹo "đoán một lần, đóng đinh mãi mãi").
2. **Không biết mình đang ở PERMISSIVE** — dòng hỏng thành NULL âm thầm, số liệu hụt mà không log nào báo. Luôn khai `mode` + `_corrupt_record` một cách có chủ đích.
3. **`overwrite` bảng phân vùng ở static mode** khi chỉ định ghi lại 1 ngày → bay cả bảng. Bật `partitionOverwriteMode=dynamic` hoặc dùng table format có ACID (Iceberg — module 5).
4. **`append` không idempotent**: job fail giữa chừng, Airflow retry → dữ liệu đôi. Chạy lại phải cho ra cùng kết quả — nghĩ về điều này NGAY từ khi viết lệnh write.
5. **Filter bằng UDF/hàm phức tạp rồi thắc mắc sao không pushdown** — pushdown chỉ hoạt động với predicate đơn giản trên cột gốc (`=, >, <, IN, IS NULL...`). `filter(my_udf(col))` = Spark buộc phải đọc hết.
6. **`partitionBy` theo cột high-cardinality** — triệu thư mục con. Partition là dao mổ: ngày/tháng/region thì đẹp, user_id thì tự sát.
7. **JDBC đè chết database nguồn** — numPartitions to + không fetchsize + đọc từ primary giờ cao điểm. Spark khỏe, DB thì không.

---

## 13. Interview

**Junior:**

1. *Kể các cách xử lý dòng hỏng khi đọc CSV/JSON trong Spark.* — Ba read mode: `PERMISSIVE` (mặc định — giữ dòng, field hỏng thành NULL, nguyên văn vào `_corrupt_record` nếu khai trong schema), `DROPMALFORMED` (vứt dòng hỏng, mất dữ liệu không dấu vết), `FAILFAST` (ném exception dừng ngay). Production nên PERMISSIVE + tách quarantine, hoặc FAILFAST với dữ liệu tài chính.
2. *Vì sao nên khai schema tường minh thay vì inferSchema?* — inferSchema đọc dữ liệu thêm một lượt (chậm) và kiểu suy ra trôi theo nội dung file từng ngày (không ổn định). Schema tường minh nhanh hơn, là hợp đồng dữ liệu, và làm lỗi lộ ra ngay cửa vào.
3. *Các save mode của DataFrameWriter?* — `errorifexists` (mặc định, đích tồn tại thì lỗi), `append` (ghi thêm), `overwrite` (xóa đích ghi mới), `ignore` (đích tồn tại thì lặng lẽ bỏ qua). Điểm nhấn: overwrite trên bảng phân vùng ở static mode xóa cả thư mục đích.
4. *CSV và Parquet khác nhau căn bản chỗ nào khi Spark đọc?* — CSV: text theo dòng, không schema, muốn 1 cột vẫn parse cả dòng, không pushdown. Parquet: binary columnar, schema trong footer, đọc đúng cột cần, có statistics để bỏ qua khối dữ liệu → nhỏ hơn và nhanh hơn nhiều lần.

**Mid:**

5. *Giải thích predicate pushdown / column pruning / partition pruning và cách kiểm chứng.* — Ba tầng né đọc: partition pruning né cả thư mục từ khâu liệt kê file (filter trên cột partitionBy); column pruning chỉ đọc cột trong ReadSchema; predicate pushdown đẩy filter xuống data source để né khối dữ liệu (Parquet row group) hoặc thành WHERE (JDBC). Kiểm chứng bằng `explain()`: `PartitionFilters`, `ReadSchema`, `PushedFilters` trên node FileScan, và `number of files read` trong tab SQL.
6. *JDBC partitioned read hoạt động thế nào, cần option gì?* — `partitionColumn` (numeric/date), `lowerBound`, `upperBound`, `numPartitions`: Spark cắt khoảng thành N dải, sinh N query WHERE song song, mỗi dải 1 task/connection. Bound không lọc dữ liệu, chỉ chia dải; dải biên hứng cả giá trị ngoài khoảng. Rủi ro: cột lệch gây skew, N connection đè DB nguồn.
7. *File `.csv.gz` 10 GB đọc chậm bất thường — vì sao và fix thế nào?* — gzip không splittable nên toàn bộ file do 1 task đọc, không song song hóa được. Fix: nguồn xuất nhiều file nhỏ hơn, dùng codec splittable (bzip2 — chậm CPU), hoặc tốt nhất: convert sớm sang Parquet ngay tầng ingest rồi mọi bước sau đọc Parquet.
8. *Làm sao ghi đè lại đúng 1 ngày dữ liệu trong bảng Parquet phân vùng theo ngày?* — Bật `spark.sql.sources.partitionOverwriteMode=dynamic` rồi `mode("overwrite").partitionBy("dt")` với DataFrame chỉ chứa ngày đó — Spark chỉ đè các partition có mặt trong dữ liệu. Static mode (mặc định) sẽ xóa toàn bộ bảng. Giải pháp căn cơ hơn: table format ACID như Iceberg.

**Senior:**

9. *Thiết kế tầng ingest nhận CSV hàng ngày từ 20 đối tác, chất lượng dữ liệu không đảm bảo. Anh/chị xử lý thế nào?* — (a) Schema tường minh per nguồn, coi như data contract có version; (b) PERMISSIVE + `_corrupt_record`, tách dòng hỏng vào bảng quarantine kèm nguồn + ngày, alert khi tỉ lệ hỏng vượt ngưỡng (vd 1%) thay vì fail cả pipeline vì 1 dòng; (c) bronze append-only giữ nguyên raw để replay; (d) convert sang Parquet ngay sau validate, partition theo ingestion_date; (e) ghi idempotent (dynamic overwrite theo partition) để Airflow re-run an toàn. Điểm ăn tiền: nói được trade-off giữa FAILFAST (đúng tuyệt đối, dễ nghẽn) và PERMISSIVE+quarantine (chạy tiếp, cần giám sát).
10. *Khi nào predicate pushdown KHÔNG cứu được bạn?* — (a) Format không hỗ trợ (CSV/JSON); (b) predicate không đẩy được: UDF, hàm phức tạp trên cột, cast ngầm (`string_col > 100`); (c) Parquet nhưng dữ liệu không được sort/cluster theo cột filter → min/max của mọi row group đều phủ giá trị cần tìm, chẳng skip được gì (statistics vô dụng khi dữ liệu trộn đều); (d) JDBC nhưng phần nặng là aggregate — không được đẩy xuống. Ý senior: pushdown là hợp đồng hai chiều — engine phải hỗ trợ VÀ layout dữ liệu phải hợp tác; vì thế mới có chuyện sort-before-write và partition design (lesson 6, 33).

---

## 14. Summary

### Mindmap

```
                       DATA SOURCES API (LESSON 5)
                                 │
     ┌──────────────┬────────────┴───────────┬────────────────────┐
     ▼              ▼                        ▼                    ▼
  READER          FORMAT                  WRITER              TỐI ƯU I/O
     │              │                        │                    │
  format         CSV/JSON: rìa,          mode:                 partition pruning
  schema ←Struct  text, không pushdown    append (idempotent?)   (né thư mục)
  option         Parquet/ORC: ruột,      overwrite (dynamic!)  column pruning
  mode:           columnar, pushdown     errorifexists/ignore    (né cột)
   PERMISSIVE    JDBC: chia dải          partitionBy:          predicate pushdown
   +_corrupt      partitionColumn         low-cardinality        (né khối/WHERE)
   DROPMALFORMED  bounds/numPartitions    + repartition trước   kiểm chứng: explain()
   FAILFAST       (bound ≠ filter!)       ghi                    FileScan node
```

### Checklist trước khi gõ "Continue"

- [ ] Viết được lệnh đọc CSV với StructType + PERMISSIVE + `_corrupt_record`, tách được dòng hỏng.
- [ ] Thuộc bảng save mode và giải thích được tai nạn overwrite static trên bảng phân vùng.
- [ ] Định nghĩa rành mạch 3 phép pruning/pushdown và chỉ ra chúng trong `explain()`.
- [ ] Đã chạy lab, thấy `PushedFilters` / `PartitionFilters` / `ReadSchema` bằng mắt mình.
- [ ] Viết được JDBC read 8 partition và giải thích lowerBound/upperBound không lọc dữ liệu.
- [ ] Biết vì sao `partitionBy("user_id")` là tự sát còn `partitionBy("order_date")` là chuẩn.
- [ ] Trả lời được 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Lesson 6 — Parquet & columnar format: tại sao DE sống chết với Parquet.**

Hôm nay bạn đã dùng Parquet như hộp đen: "nó nhanh, nó có pushdown". Nhưng interviewer senior sẽ hỏi tiếp: *nhanh VÌ SAO? Row group là gì? Min/max statistics nằm ở đâu mà giúp skip dữ liệu? Snappy với zstd khác gì nhau?* Lesson 6 mổ banh một file Parquet ra xem từng lớp: row group, column chunk, page, footer — và bạn sẽ hiểu tại sao cùng một câu filter, Parquet đọc 2% dữ liệu trong khi CSV phải nhai 100%. Hiểu tầng này xong, mọi quyết định về file layout, compression, sort order về sau đều có căn cứ thay vì cầu may.

> Gõ **"Continue"** khi sẵn sàng.
