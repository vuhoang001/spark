# Lesson 2 — SparkSession, RDD → DataFrame → Dataset, và Lazy Evaluation

> Module 1 · Foundations · Tuần 1 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **SparkSession anatomy**: bên trong nó có gì (SparkContext, catalog, conf) và tại sao từ Spark 2.0 chỉ cần một cửa ngõ duy nhất.
- **RDD** — API gốc của Spark: nó là gì, **lineage** là gì, và tại sao 2026 rồi ta vẫn phải hiểu nó dù hầu như không viết nó.
- Tại sao **DataFrame thay thế RDD** trong 95% công việc: Catalyst và Tungsten chỉ tối ưu được khi *nhìn thấy cấu trúc*.
- **Dataset** — vì sao chỉ tồn tại ở Scala/Java, và PySpark đứng ở đâu trong bức tranh này.
- Bảng đầy đủ **transformation vs action**, và phân loại **narrow vs wide** — nền móng để lesson 3 giải thích cách cắt stage.
- **Lazy evaluation**: tại sao Spark cố tình "lười", và sự lười đó mua được những gì.
- **Fault tolerance qua recompute**: executor chết, dữ liệu cache mất — Spark tính lại từ lineage thay vì replicate.

Sau bài này bạn phải làm được:

- Nhìn bất kỳ dòng code PySpark nào và nói ngay: đây là transformation hay action, narrow hay wide.
- Giải thích cho một người mới: "tại sao tôi gọi `filter` mà Spark KHÔNG chạy gì cả — và đó là tính năng, không phải bug".
- Chọn đúng API: khi nào DataFrame, khi nào (hiếm hoi) phải rơi xuống RDD.

Kiến thức dùng trong thực tế: **mỗi ngày**. Mọi quyết định performance (lesson 15–22) đều đứng trên nền transformation/action và lazy evaluation. Hiểu sai bài này thì tuning về sau là học vẹt.

---

## 2. Why

### Hiện tượng lạ ở lesson 1

Bạn đã thấy: gọi `filter`, `groupBy` — im lặng tuyệt đối, chạy xong trong 0.01 giây. Gọi `show()` — bùm, job xuất hiện, CPU quay. Tại sao một engine "xử lý dữ liệu" lại *không xử lý gì* khi bạn bảo nó filter?

**Analogy đặt phở**: bạn vào quán, dặn "cho tô phở, không hành, thêm tái, ít bánh". Đầu bếp KHÔNG nấu ngay sau mỗi câu — ông ấy *ghi order*. Chỉ khi bạn nói "làm luôn đi anh" (action), ông mới nấu — và nấu **một lần duy nhất** theo toàn bộ order đã gom. Nếu nấu ngay từng bước: nấu tô phở → vớt hành ra → chần thêm tái → múc bớt bánh, thì vừa chậm vừa lãng phí. Spark là ông đầu bếp đó: transformation = ghi order, action = "làm luôn đi".

### Vấn đề mà DataFrame sinh ra để giải

Spark 1.x chỉ có RDD. RDD hoạt động, nhưng có 2 cái giá đắt:

1. **Spark mù logic của bạn.** Với RDD, bạn đưa cho Spark một hàm Python/Scala tùy ý (`rdd.map(lambda x: ...)`). Spark chỉ thấy "một cục hàm" — không biết bạn đang lấy cột nào, lọc điều kiện gì → **không tối ưu được gì cả**, chỉ biết chạy đúng thứ tự bạn viết.
2. **Với PySpark, RDD là thảm họa tốc độ.** Mỗi record phải serialize từ JVM → gửi sang Python process → chạy lambda → serialize ngược về JVM. Trả phí "hải quan" hai chiều trên *từng dòng dữ liệu*.

DataFrame lật ngược ván cờ: thay vì đưa Spark *hàm tùy ý*, bạn đưa Spark *mô tả có cấu trúc* (`F.col("price") > 100`). Bây giờ Spark **đọc hiểu được ý định** → Catalyst tối ưu plan, Tungsten sinh code chạy sát phần cứng, và PySpark không phải chuyển dữ liệu sang Python nữa (biểu thức được dịch sang JVM chạy thẳng).

### Trade-off ba tầng API

| API | Được | Mất |
|---|---|---|
| **RDD** | Kiểm soát tuyệt đối; xử lý được dữ liệu phi cấu trúc kỳ dị; API `mapPartitions` cấp thấp | Không Catalyst, không Tungsten; PySpark cực chậm (serialize từng record); tự chịu trách nhiệm tối ưu |
| **DataFrame** | Catalyst + Tungsten tối ưu hộ; nhanh ngang nhau mọi ngôn ngữ; API giống SQL dễ đọc | Untyped — lỗi sai tên cột chỉ nổ lúc **runtime** (analysis), không phải lúc compile |
| **Dataset** (Scala/Java) | Như DataFrame + **compile-time type safety** (sai kiểu là không build được) | Chỉ Scala/Java; dùng lambda typed là mất một phần tối ưu Catalyst |

> Bài học Senior: trong PySpark, **DataFrame là mặc định, RDD là ngoại lệ phải giải trình**. Nếu thấy `df.rdd.map(...)` trong code review, câu hỏi đầu tiên là: "tại sao không làm được bằng built-in function?" — 9/10 lần là làm được, và nhanh hơn 10–100×.

---

## 3. Theory

### 3.1. SparkSession anatomy — mổ xẻ cửa ngõ

```
┌───────────────────────────────────────────────────────────┐
│                     SparkSession                          │
│         (cửa ngõ duy nhất từ Spark 2.0, sống ở DRIVER)     │
│                                                           │
│   ┌─────────────────────┐   ┌──────────────────────────┐  │
│   │   SparkContext      │   │   Catalog                │  │
│   │  (trái tim cũ từ 1.x)│   │  (danh bạ: databases,    │  │
│   │  • kết nối Cluster  │   │   tables, views, funcs)  │  │
│   │    Manager          │   └──────────────────────────┘  │
│   │  • tạo/quản lý RDD  │   ┌──────────────────────────┐  │
│   │  • cấp phát executor│   │   RuntimeConfig (conf)   │  │
│   │  • sc.parallelize() │   │  spark.conf.get/set      │  │
│   └─────────────────────┘   └──────────────────────────┘  │
│                                                           │
│   spark.read → DataFrameReader                            │
│   spark.sql("...") → chạy SQL trả DataFrame               │
│   spark.udf → đăng ký UDF                                 │
└───────────────────────────────────────────────────────────┘
```

- Thời Spark 1.x, bạn phải tạo lổn nhổn: `SparkContext` (RDD), `SQLContext` (SQL), `HiveContext` (Hive), `StreamingContext`... Spark 2.0 gom tất cả vào **SparkSession**.
- `SparkContext` vẫn sống bên trong: `spark.sparkContext` (hay gặp dưới tên `sc`). Cần nó khi đụng RDD, `setLogLevel`, hay đọc `defaultParallelism`.
- **1 application = 1 SparkSession** (về mặt thực hành). `getOrCreate()` nghĩa là: có rồi thì lấy lại, chưa có thì tạo — nhờ vậy gọi 2 lần không chết, nhưng cũng nghĩa là config lần 2 có thể **bị lờ đi** nếu session đã tồn tại (pitfall kinh điển trong notebook).

### 3.2. RDD — Resilient Distributed Dataset

Định nghĩa từng chữ:

- **Distributed**: collection bị cắt thành các **partition** nằm rải trên các executor.
- **Dataset**: tập các phần tử (bất kỳ object Python/Java nào).
- **Resilient** (hồi phục được): mất partition thì **tính lại** được — nhờ *lineage*.

**Lineage (dòng dõi)** = mỗi RDD ghi nhớ *nó được sinh ra từ RDD nào, bằng phép biến đổi gì*:

```
file HDFS/S3 ──textFile──▶ RDD_A ──map──▶ RDD_B ──filter──▶ RDD_C
                              │
              executor giữ partition 3 của RDD_C bị CHẾT
                              │
              Driver nhìn lineage: "partition 3 của C = filter(map(đọc block 3 của file))"
                              ▶ giao executor khác TÍNH LẠI đúng partition 3. Xong.
```

Đây là nước đi thiên tài của Spark so với cách cũ (replicate dữ liệu trung gian 3 bản như HDFS): **không tốn RAM/disk lưu bản sao — chỉ lưu công thức**. Công thức nhẹ hơn dữ liệu hàng triệu lần. Trả giá: khi mất mát thì phải tốn CPU tính lại (và nếu lineage quá dài, tính lại rất đau — đó là lúc cần checkpoint, học ở module 4).

RDD **immutable** (bất biến): `map` không sửa RDD cũ mà sinh RDD mới. Nhờ bất biến, lineage mới đáng tin — công thức không bao giờ bị đổi ruột giữa chừng.

### 3.3. Tại sao DataFrame thay RDD — Catalyst nhìn thấy cấu trúc

DataFrame = RDD + **schema** (tên cột, kiểu dữ liệu) + ngôn ngữ biểu thức mà engine đọc hiểu được.

```
RDD:        rdd.filter(lambda x: x[2] > 100)
            Spark thấy: ██████████ (hộp đen — một hàm Python)
            → không biết bạn lọc gì → không tối ưu được

DataFrame:  df.filter(F.col("price") > 100)
            Spark thấy: Filter(price > 100) — một CẤU TRÚC
            → Catalyst: "à, lọc cột price — để tôi đẩy filter này
              xuống tận lúc đọc file, khỏi đọc dòng thừa"
```

Hai cỗ máy hưởng lợi từ cấu trúc:

- **Catalyst Optimizer** (lesson 13): tối ưu logical plan — đẩy filter xuống sớm (predicate pushdown), chỉ đọc cột cần (column pruning), gộp các phép chiếu, chọn chiến lược join...
- **Tungsten**: lưu dữ liệu dạng **binary UnsafeRow** ngoài mô hình object của JVM (đỡ tốn RAM, đỡ GC), và **whole-stage codegen** — sinh code Java chuyên biệt cho query của bạn thay vì đi qua các lớp trừu tượng.

Hệ quả vàng cho dân Python: vì bạn gửi *mô tả* chứ không gửi *hàm*, mọi tính toán DataFrame chạy **hoàn toàn trong JVM** — PySpark DataFrame nhanh ngang Scala DataFrame. Chỉ khi dùng Python UDF mới phải trả phí serialize (lesson 12).

### 3.4. Dataset — mảnh ghép Scala/Java

- `Dataset[T]`: như DataFrame nhưng mỗi dòng là object **typed** `T` (case class). Sai tên field → **lỗi lúc compile**, không đợi tới runtime.
- Về nội bộ, `DataFrame = Dataset[Row]` — DataFrame chỉ là Dataset với kiểu "Row chung chung".
- **Python không có Dataset** vì Python là ngôn ngữ động — không có compile-time type để mà kiểm. PySpark chỉ có DataFrame (và đó không phải thiệt thòi lớn: đa số team Scala cũng dùng DataFrame cho ETL).

### 3.5. Transformation vs Action — bảng tra cứu

**Transformation** = mô tả bước biến đổi, trả về DataFrame/RDD mới, **lười** (chỉ ghi vào plan).
**Action** = yêu cầu kết quả thật, **kích hoạt job**.

| Transformation (lười) | Loại | Action (kích hoạt job) |
|---|---|---|
| `select`, `withColumn`, `withColumnRenamed`, `drop`, `cast` | narrow | `count()` |
| `filter` / `where` | narrow | `show(n)`, `take(n)`, `first()`, `head()` |
| `union`, `sample`, `na.fill`, `na.drop` | narrow | `collect()` |
| `coalesce(n)` (giảm partition) | narrow | `write.parquet/csv/...`, `saveAsTable` |
| `map`, `flatMap`, `mapPartitions` (RDD) | narrow | `toPandas()` |
| `groupBy(...).agg(...)` | **wide** | `foreach`, `foreachPartition` |
| `join` (trừ broadcast join) | **wide** | `reduce` (RDD) |
| `distinct`, `dropDuplicates` | **wide** | `isEmpty()` |
| `orderBy` / `sort` | **wide** | |
| `repartition(n)`, `repartitionByRange` | **wide** | |

Lưu ý các ca dễ nhầm:
- `printSchema()`, `explain()`, `cache()` — **không phải action** (không đụng dữ liệu; `cache` chỉ đánh dấu, đến action kế mới materialize).
- `show()` là action nhưng "tiết kiệm": chỉ chạy đủ partition để gom 20 dòng.
- Broadcast join là wide về bản chất join nhưng **không shuffle bảng lớn** (lesson 9).

### 3.6. Narrow vs Wide — ranh giới sinh tử

```
NARROW (hẹp): mỗi partition con chỉ cần MỘT partition cha
              → dữ liệu ở yên tại chỗ, không qua network

  cha:  [P0]      [P1]      [P2]           ví dụ: filter, select,
          │         │         │             withColumn, union
          ▼         ▼         ▼
  con:  [P0']     [P1']     [P2']

WIDE (rộng): mỗi partition con cần dữ liệu từ NHIỀU partition cha
             → dữ liệu bay chéo qua network = SHUFFLE

  cha:  [P0]      [P1]      [P2]           ví dụ: groupBy, join,
          │╲      ╱│╲       ╱│              distinct, orderBy,
          │ ╲    ╱ │ ╲     ╱ │              repartition
          ▼  ╲  ╱  ▼  ╲   ╱  ▼
  con:  [K0]  ╳╳  [K1]  ╳╳  [K2]   ← gom "cùng key về một chỗ"
```

Trực giác: hỏi *"để tính một dòng output, tôi có cần nhìn dữ liệu nằm ở máy khác không?"* `filter` — không (nhìn từng dòng là đủ) → narrow. `groupBy("city")` — có (mọi dòng cùng city phải về một chỗ) → wide. **Wide transformation = shuffle = ranh giới stage** — lesson 3 xây toàn bộ trên câu này.

### 3.7. Lazy evaluation — tại sao Spark cố tình lười

Spark lười để mua 4 thứ:

1. **Nhìn toàn cục để tối ưu.** Nếu chạy ngay từng lệnh (eager như Pandas), Spark chỉ thấy từng bước cô lập. Lười → gom cả chuỗi thành plan → Catalyst nhìn toàn cảnh: "select 3 cột rồi mới filter à? Để tôi filter trước, đẩy cả filter lẫn select xuống tận file reader". Bạn viết code dở, Spark chạy plan hay.
2. **Không materialize kết quả trung gian.** Eager: sau `filter` phải có "bảng đã filter" nằm đâu đó (RAM/disk). Lazy: `filter → withColumn → select` được **pipeline** — mỗi dòng chảy xuyên qua cả 3 phép trong một lần chạm, không có bảng trung gian nào tồn tại.
3. **Chỉ tính cái được hỏi.** `df.transform_chain().show(5)` — Spark biết đích chỉ cần 5 dòng → đọc 1 partition, đủ là dừng. Eager thì đã tính hết từ đời nào.
4. **Lineage miễn phí.** Vì mọi thứ đều là "công thức chưa nấu", công thức đó chính là lineage — nền của fault tolerance (3.2).

Cái giá của lười: **debug khó hơn**. Lỗi dữ liệu ở dòng `filter` (dòng 10) nhưng stack trace nổ ở `count()` (dòng 50), vì dòng 10 chưa hề chạy lúc bạn viết nó. Thuộc lòng: *lỗi runtime của Spark luôn nổ tại action, nhưng thủ phạm thường là transformation phía trước*.

---

## 4. Internal

Chuyện gì xảy ra bên trong khi bạn gõ từng dòng:

```
① spark = SparkSession.builder...getOrCreate()
   → JVM driver khởi tạo SparkContext → liên hệ Cluster Manager
     xin executor (như lesson 1). Python giữ "remote control" qua Py4J.

② df = spark.read.csv("...", header=True)
   → CHƯA đọc dữ liệu (chỉ đọc vài dòng đầu lấy tên cột nếu header=True,
     và nếu inferSchema=True thì lén chạy 1 job quét dữ liệu — action ẩn!)
   → Sinh node đầu tiên của LOGICAL PLAN: Relation[csv]

③ df2 = df.filter(F.col("price") > 100)
   → KHÔNG có dữ liệu nào bị lọc. Py4J gửi biểu thức sang JVM,
     plan mọc thêm 1 tầng:  Filter(price > 100)
                              └─ Relation[csv]

④ df3 = df2.groupBy("city").count()
   → Plan mọc tiếp:  Aggregate(city, count)
                       └─ Filter(price > 100)
                            └─ Relation[csv]
   Đến đây: 0 byte dữ liệu được đọc, 0 task chạy. Chỉ có cây plan trong RAM driver.

⑤ df3.show()   ← ACTION. Bây giờ chuỗi domino đổ:
   a. Analyzer: đối chiếu catalog — cột "city" có thật không? kiểu gì?
      (sai tên cột nổ Ở ĐÂY → AnalysisException tại dòng action)
   b. Catalyst Optimizer: xoay cây plan — pushdown, pruning...
   c. Physical Planner: chọn cách thực thi (hash aggregate? kiểu join nào?)
   d. DAG Scheduler: cắt thành stage tại shuffle (groupBy) → 2 stage
   e. Task Scheduler: phát task đến executor → chạy → trả 20 dòng về driver
```

Với RDD, đường đi ngắn hơn nhưng "ngu" hơn: không có bước a–c (không analyzer, không optimizer) — DAG của RDD được chạy *đúng nguyên văn* bạn viết. Đây chính là lý do kỹ thuật khiến DataFrame thắng: **bước b–c là nơi Spark biến code thường thành code nhanh, và RDD không có nó.**

Muốn nhìn tận mắt cây plan: `df3.explain(True)` in đủ 4 tầng — Parsed → Analyzed → Optimized → Physical. Hãy chạy thử trong lab.

---

## 5. API

### `SparkSession.builder` + `spark.sparkContext`

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("lesson02")
         .getOrCreate())
sc = spark.sparkContext          # cánh cửa xuống thế giới RDD
sc.setLogLevel("WARN")           # bớt ồn log INFO
print(sc.defaultParallelism)     # tổng core mà scheduler thấy
```
- **Pitfall**: trong notebook, session sống dai — `.config(...)` gọi sau khi session đã tồn tại sẽ **bị bỏ qua im lặng**. Muốn đổi config kiểu builder phải `spark.stop()` trước.

### `sc.parallelize` / `df.rdd` — chạm vào RDD

```python
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=2)   # list → RDD 2 partition
doubled = rdd.map(lambda x: x * 2)                    # transformation (lười)
print(doubled.collect())                              # action → [2,4,6,8,10]

row_rdd = df.rdd                                      # DataFrame → RDD[Row]
```
- **Khi dùng**: dạy/học, hoặc logic không diễn đạt nổi bằng DataFrame API.
- **Pitfall**: `df.rdd` kéo dữ liệu ra khỏi thế giới Tungsten → mất tối ưu + trả phí serialize sang Python. Đây là red flag trong code review PySpark.

### `df.filter` / `df.withColumn` / `df.select` — transformation tiêu biểu

```python
from pyspark.sql import functions as F

out = (df.filter(F.col("order_status") == "delivered")
         .withColumn("year", F.year("order_purchase_timestamp"))
         .select("order_id", "year"))
```
- **Pitfall**: viết xong cả chuỗi thấy "chạy nhanh quá!" — chưa có gì chạy đâu. Tốc độ thật chỉ lộ diện ở action.

### `df.explain(extended=...)` — soi plan mà không chạy

```python
out.explain(True)          # 4 tầng plan; hoặc out.explain("formatted") cho dễ đọc
```
- **Ý nghĩa**: công cụ số 1 để kiểm tra Catalyst làm gì với code của bạn. Không tốn tài nguyên, không phải action.
- **Pitfall**: junior không bao giờ đọc explain, senior đọc trước khi hỏi "sao chậm".

### `df.cache()` — đánh dấu để tái sử dụng (mở màn, học sâu lesson 18)

```python
hot = df.filter(...).cache()   # chỉ ĐÁNH DẤU — chưa có gì vào memory
hot.count()                    # action đầu: vừa tính vừa nạp cache
hot.groupBy(...).count().show()# action sau: đọc từ cache, khỏi tính lại
```
- **Pitfall**: cache xong quên dùng lại (vô nghĩa, tốn RAM), hoặc tưởng `cache()` chạy ngay (nó lười như mọi transformation).

---

## 6. Demo nhỏ

```
Input:  DataFrame nhỏ tạo tay
   ↓    chuỗi transformation + đo thời gian (sẽ thấy ~0 giây)
   ↓    explain() xem plan
Output: action count() — lúc này job mới chạy thật
```

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo02").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data = [("HN", "book", 120.0), ("SG", "food", 80.0), ("HN", "food", 300.0),
        ("DN", "book", 150.0), ("SG", "book", 500.0)] * 40_000   # 200.000 dòng
df = spark.createDataFrame(data, ["city", "category", "amount"])

t0 = time.time()
pipeline = (df.filter(F.col("amount") > 100)          # lười
              .withColumn("amount_usd", F.col("amount") / 25_000)  # lười
              .groupBy("city").agg(F.sum("amount_usd").alias("usd")))  # vẫn lười
print(f"Xây 3 transformation trên 200.000 dòng mất: {time.time()-t0:.4f}s")  # ~0.05s!

pipeline.explain(True)      # soi plan: thấy Filter được đẩy xuống dưới cùng

t0 = time.time()
pipeline.show()             # ACTION — bây giờ 200.000 dòng mới thực sự chạy
print(f"Action show() mất: {time.time()-t0:.2f}s")
spark.stop()
```

Chạy xong tự hỏi: thời gian dồn hết vào đâu? Nếu Spark eager như Pandas, dòng `filter` đã ngốn thời gian ngay. Đọc phần Physical Plan trong output `explain`: bạn sẽ thấy `Filter` nằm sát `Scan` — Catalyst đã xếp lại hộ bạn.

---

## 7. Production Example

Pipeline bronze → silver điển hình (chính là dạng job bạn sẽ viết ở Project 1):

```python
# silver_orders.py — chạy hằng đêm bởi Airflow
orders_raw = spark.read.parquet("s3://lake/bronze/orders/")        # lười
valid = (orders_raw
         .filter(F.col("order_id").isNotNull())                    # lười
         .filter(F.col("order_status").isin(VALID_STATUSES))       # lười
         .withColumn("purchase_date",
                     F.to_date("order_purchase_timestamp"))        # lười
         .dropDuplicates(["order_id"]))                            # lười (wide!)
valid.write.mode("overwrite").parquet("s3://lake/silver/orders/")  # ACTION duy nhất
```

Tại sao dân production viết kiểu "dồn hết về một action":

1. **Một action = một job = một lần đọc dữ liệu.** Nếu chèn `valid.count()` để log số dòng ở giữa, bạn vừa tăng gấp đôi chi phí đọc bronze — vì lazy evaluation, action thứ hai tính lại **từ đầu** (không có cache).
2. **Catalyst gom cả chuỗi filter** thành một lần quét: 2 `filter` + 1 `withColumn` được pipeline trong cùng một stage, dữ liệu chảy qua một lượt.
3. Khi cần cả `count` lẫn `write` (log metric là nhu cầu chính đáng), senior sẽ: `cache()` trước rồi làm cả hai, hoặc `write` xong đọc lại file mà đếm (Parquet đếm bằng metadata, gần như miễn phí), hoặc dùng accumulator/Observation API. Không bao giờ vô tư chạy 2 action trên plan chưa cache.
4. **Fault tolerance miễn phí**: job này chết giữa chừng ở task nào, Spark tính lại đúng task đó từ lineage — người viết không phải code retry thủ công dòng nào.

---

## 8. Hands-on Lab

**Mục tiêu**: chứng kiến lazy evaluation trên dataset Olist thật, và đo RDD chậm hơn DataFrame bao nhiêu lần trên cùng một phép tính.

Môi trường: cluster Docker của repo (đã dựng ở lesson 1). Dataset Olist nằm ở `data/olist/*.csv`, được mount vào container tại `/workspace/data/olist/`. Thư mục `labs/lab02/` đã có sẵn bài làm cũ của bạn — **tạo file mới, đừng sửa file cũ**.

### Bước 1 — bật cluster

```bash
make up          # spark-master + spark-worker + spark-submit
make ps          # kiểm tra 3 container Up
```

### Bước 2 — viết file MỚI `labs/lab02/lesson02_lazy_rdd_df.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab02-lazy-rdd-df").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------- PHẦN A: lazy evaluation ----------
t0 = time.time()
items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)
orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
t_read = time.time() - t0

t0 = time.time()
revenue = (orders.filter(F.col("order_status") == "delivered")
                 .join(items, "order_id")
                 .groupBy(F.to_date("order_purchase_timestamp").alias("d"))
                 .agg(F.sum("price").alias("revenue")))
t_transform = time.time() - t0

t0 = time.time()
revenue.orderBy(F.desc("revenue")).show(5)
t_action = time.time() - t0

print(f"[A] read={t_read:.2f}s  transform={t_transform:.4f}s  action={t_action:.2f}s")
# Dự đoán trước khi chạy: cái nào gần 0? cái nào ngốn thời gian?
# (read KHÔNG gần 0 — thủ phạm là inferSchema. Đổi thành inferSchema=False chạy lại mà xem!)

# ---------- PHẦN B: RDD vs DataFrame, cùng một phép tính ----------
# Tổng price theo seller_id — cách 1: DataFrame (JVM thuần)
t0 = time.time()
df_top = (items.groupBy("seller_id").agg(F.sum("price").alias("total"))
               .orderBy(F.desc("total")))
df_result = df_top.take(5)
t_df = time.time() - t0

# cách 2: RDD (mỗi record đi vòng qua Python)
t0 = time.time()
rdd_result = (items.rdd
              .map(lambda r: (r["seller_id"], r["price"]))
              .reduceByKey(lambda a, b: a + b)
              .takeOrdered(5, key=lambda kv: -kv[1]))
t_rdd = time.time() - t0

print(f"[B] DataFrame={t_df:.2f}s | RDD={t_rdd:.2f}s | RDD chậm hơn ~{t_rdd/t_df:.1f}x")
print("DF :", [(r["seller_id"][:8], round(r["total"], 2)) for r in df_result])
print("RDD:", [(k[:8], round(v, 2)) for k, v in rdd_result])   # phải khớp nhau!

# ---------- PHẦN C: soi plan ----------
revenue.explain("formatted")

input(">>> Mở http://localhost:4040 xem Jobs, rồi Enter để thoát...")
spark.stop()
```

### Bước 3 — chạy

```bash
make run-local F=labs/lab02/lesson02_lazy_rdd_df.py   # local[2], dễ quan sát
# hoặc chạy trên cluster thật:
make run F=labs/lab02/lesson02_lazy_rdd_df.py         # spark://spark-master:7077
```

### Bước 4 — quan sát

Khi script dừng ở `input()`, mở `http://localhost:4040`:

1. Tab **Jobs**: đếm job. Đối chiếu: 2 job của `inferSchema` (mỗi lần read), 1 của `show`, 1 của `take` (DataFrame), 1–2 của RDD. Transformation nào có job riêng không? (Không — đúng như lý thuyết.)
2. Click job của phần B-RDD: Description ghi gì? (Các job RDD hiện tên như `takeOrdered at ...` — và trong stage detail, task chạy qua "Python worker".)
3. Tab **SQL/DataFrame**: chỉ các query DataFrame xuất hiện ở đây (RDD không có — vì RDD nằm ngoài thế giới Catalyst). Mở query của phần A, ngắm cây physical plan bằng hình.
4. Ghi lại tỉ lệ chậm của RDD vs DataFrame vào `labs/lab02/NOTES-lesson02.md` (file mới) cùng 3 quan sát trên.

---

## 9. Assignment

**Easy** — Viết file mới `labs/lab02/ex_easy_transforms.py`: dùng bảng `olist_products_dataset.csv`, viết **5 transformation khác nhau** (ví dụ: `filter` sản phẩm nặng >1kg, `withColumn` tính thể tích cm³, `select` + rename, `na.drop`, `distinct` trên category) nối thành 1 chuỗi, kết thúc bằng đúng **1 action**. Với mỗi transformation, comment ngay trên dòng đó: *narrow hay wide? tại sao?*

**Medium** — "Debug bằng action": lấy chuỗi phần A của lab, cố tình gây 1 lỗi dữ liệu/logic (ví dụ đổi tên cột thành `pricee`, hoặc `to_date` với format sai). Chạy — ghi lại lỗi nổ ở dòng nào. Sau đó chèn `.count()` (hoặc `df.limit(10).collect()`) sau **từng** transformation để khoanh vùng chính xác bước hỏng. Viết 5 dòng kết luận: kỹ thuật này giúp gì khi debug, và tại sao **phải gỡ hết các action debug** trước khi merge code (gợi ý: mỗi action = 1 job tính lại từ đầu).

**Hard** — Trả lời bằng bài viết ngắn (15–20 dòng, chữ của bạn): *Tại sao Spark không thực thi gì khi chỉ có transformation?* Yêu cầu chạm đủ 4 ý: nhìn toàn cục để tối ưu (kèm 1 ví dụ pushdown cụ thể từ `explain` bạn tự chạy), tránh materialize trung gian, chỉ tính cái được hỏi, lineage/fault tolerance. Bonus: nêu 1 cái giá phải trả của lazy (gợi ý: vị trí lỗi, và chuyện 2 action = tính 2 lần).

**Production Challenge** — Trong lab phần A, `read` với `inferSchema=True` không hề lười (nó lén chạy job). Hãy: (1) khai `StructType` tường minh cho `olist_order_items_dataset.csv`, (2) đo lại `t_read` — bây giờ read có lười đúng nghĩa chưa? (3) mở Spark UI xác nhận job inferSchema biến mất, (4) kết luận 3 dòng: trong production, `read` nên là transformation hay action, và làm sao đảm bảo điều đó. Nộp code + số đo + screenshot mô tả.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

Bài học performance rút được hôm nay:

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Chuỗi 10 transformation liên tiếp | ~0 giây | Chỉ là dựng cây plan trong RAM driver — chưa đụng dữ liệu. |
| `df.rdd.map(lambda...)` trong PySpark | Chậm 5–50× | Mỗi record serialize JVM→Python→JVM; mất Catalyst + Tungsten + codegen. |
| 2 action trên cùng plan (không cache) | Chậm ×2 | Lazy nghĩa là KHÔNG nhớ kết quả — action sau recompute từ nguồn. |
| `filter` đặt "muộn" trong code | Vẫn nhanh | Catalyst tự đẩy filter xuống sớm (pushdown). Nhưng đừng ỷ lại — filter sớm trong code vẫn là thói quen tốt, vì không phải mọi biểu thức đều push được (UDF thì chịu). |
| `dropDuplicates` toàn bảng | Đắt | Wide transformation → shuffle toàn bộ dữ liệu. Cân nhắc dedup theo key hẹp. |

Ba câu tự vấn mới, cộng vào câu của lesson 1: *"plan của tôi có bao nhiêu action? mỗi action tính lại những gì? có đoạn nào rơi xuống RDD/Python không?"*

---

## 11. Spark UI

Bài này mở khóa tab mới: **SQL / DataFrame**.

**Tab SQL/DataFrame** — nhìn gì:
- Mỗi dòng = 1 query (thường ứng với 1 action trên DataFrame). Click vào: thấy **physical plan dạng hình** — các hộp `Scan csv`, `Filter`, `HashAggregate`, `Exchange`, `SortMergeJoin`...
- Hộp `Exchange` = shuffle — đếm số `Exchange` là đếm số lần dữ liệu bay qua network.
- Hover từng hộp thấy metrics: số dòng đầu ra (`number of output rows`) — dùng để kiểm tra filter có "ăn" như kỳ vọng không (lọc xong còn 99% số dòng nghĩa là filter vô dụng).
- **Truy ngược**: từ query → click job id → về tab Jobs/Stages. Đây là đường điều tra chuẩn từ nay về sau.

**Đối chiếu ba tầng**: `explain()` trong code = chữ; tab SQL = hình; tab Jobs/Stages = thực thi. Cả ba là một — khi bạn đọc được cả ba và thấy chúng khớp nhau, bạn đã "nhìn xuyên" được Spark.

Chú ý: các job RDD **không xuất hiện** trong tab SQL — bằng chứng trực quan rằng RDD sống ngoài vòng pháp luật của Catalyst.

---

## 12. Common Mistakes

1. **Tưởng transformation chạy ngay** → viết `df.filter(...)` rồi thắc mắc "sao nhanh thế / sao chưa thấy lỗi". Lỗi tên cột chỉ nổ ở action (AnalysisException) — nhìn stack trace tại `count()` mà đi tìm thủ phạm ở `count()` là lạc đường.
2. **Quên gán kết quả**: `df.filter(...)` rồi dùng tiếp `df` — DataFrame bất biến, filter trả về DataFrame MỚI. Phải `df = df.filter(...)`. Sai lầm tuần-đầu kinh điển.
3. **Rắc nhiều action không cache** — `count()` để log, `show()` để ngó, `write` để ghi: 3 action = đọc và tính lại dữ liệu 3 lần. Hoá đơn cloud tăng ×3 mà output y nguyên.
4. **`df.rdd.map(lambda ...)` cho việc mà built-in làm được** — vừa chậm vừa mù Catalyst. Kiểm tra `pyspark.sql.functions` trước; 300+ hàm ở đó cover 95% nhu cầu.
5. **Config builder trong notebook không ăn** — session cũ còn sống nên `.config()` mới bị lờ. Triệu chứng: "tôi đổi shuffle.partitions rồi mà UI vẫn hiện 200". Fix: `spark.stop()` rồi tạo lại, hoặc dùng `spark.conf.set` cho config runtime.
6. **Nhầm `cache()` là action** — đánh dấu xong không có action nào theo sau, rồi kết luận "cache không giúp gì". Cache chỉ nạp ở action đầu tiên sau khi đánh dấu.
7. **Không phân biệt narrow/wide khi viết code** — vô tư `distinct()`, `orderBy()` giữa pipeline, mỗi cái là một shuffle toàn dữ liệu. Từ hôm nay, mỗi lần gõ một wide transformation, tay phải khựng lại nửa giây: "shuffle này có đáng không?"

---

## 13. Interview

**Junior:**

1. *Transformation và action khác nhau thế nào? Cho ví dụ.* — Transformation mô tả biến đổi, trả về DataFrame/RDD mới, lười (chỉ ghi vào plan): `filter`, `select`, `groupBy`. Action yêu cầu kết quả thật, kích hoạt job: `count`, `show`, `collect`, `write`. Nhớ mẹo: trả về DataFrame → thường là transformation; trả về số/list/None/ghi ra ngoài → action.
2. *Lazy evaluation là gì, lợi ích chính?* — Spark trì hoãn thực thi đến khi gặp action, gom mọi transformation thành plan. Lợi: (a) Catalyst nhìn toàn cục để tối ưu (pushdown, pruning), (b) pipeline không materialize trung gian, (c) chỉ tính phần được hỏi, (d) plan chính là lineage phục vụ fault tolerance.
3. *RDD là gì? Ba chữ cái nghĩa là gì?* — Resilient Distributed Dataset: collection bất biến, cắt thành partition phân tán trên executor (Distributed), phục hồi được khi mất partition nhờ tính lại từ lineage (Resilient). Là API gốc, nay chủ yếu nằm dưới nắp capo của DataFrame.
4. *SparkSession và SparkContext quan hệ ra sao?* — SparkSession (2.0+) là cửa ngõ hợp nhất, chứa SparkContext bên trong (`spark.sparkContext`). SparkContext lo kết nối cluster và thế giới RDD; SparkSession thêm catalog, conf, SQL, DataFrame API.

**Mid:**

5. *Tại sao DataFrame nhanh hơn RDD, nhất là trong PySpark?* — DataFrame có schema + biểu thức khai báo → Catalyst tối ưu plan, Tungsten dùng binary format + whole-stage codegen. Với PySpark: biểu thức DataFrame được dịch sang JVM chạy thẳng, còn RDD lambda phải serialize từng record qua Python process hai chiều — chậm hơn một bậc độ lớn.
6. *Narrow vs wide transformation? Vì sao ranh giới này quan trọng?* — Narrow: partition con phụ thuộc đúng 1 partition cha (filter, select) — dữ liệu tại chỗ. Wide: con cần nhiều cha (groupBy, join, distinct, orderBy, repartition) — phải shuffle qua network. Quan trọng vì wide = shuffle = ranh giới stage = chi phí lớn nhất của job.
7. *Gọi 2 action trên cùng một DataFrame thì chuyện gì xảy ra? Khắc phục?* — Không cache thì action thứ hai recompute toàn bộ lineage từ nguồn (lazy không lưu kết quả trung gian). Khắc phục: `cache()`/`persist()` nếu tái sử dụng nhiều lần và đáng RAM; hoặc thiết kế lại còn 1 action; hoặc ghi ra storage rồi đọc lại.
8. *Dataset khác DataFrame chỗ nào, sao Python không có?* — Dataset[T] typed theo class, bắt lỗi kiểu/tên field lúc compile; DataFrame = Dataset[Row] untyped, lỗi chỉ lộ lúc analysis (runtime). Python là dynamic language, không có compile-time type nên không thể có Dataset — PySpark dừng ở DataFrame.

**Senior:**

9. *Spark đạt fault tolerance bằng recompute từ lineage thay vì replication — trade-off của lựa chọn này?* — Được: không tốn RAM/disk/network cho bản sao dữ liệu trung gian (lineage chỉ là metadata siêu nhẹ), ghi nhanh hơn hệ replicate như HDFS-style. Mất: khi hỏng phải trả CPU tính lại; lineage dài (iterative/streaming) làm recovery đắt và stack sâu → cần `checkpoint()` cắt lineage, chấp nhận ghi ra storage. Chọn recompute hợp lý vì hỏng hóc là ngoại lệ — tối ưu cho đường hạnh phúc, trả giá khi sự cố.
10. *Khi nào bạn chấp nhận rơi xuống RDD trong một codebase DataFrame?* — Hiếm: (a) thuật toán per-partition đặc thù cần `mapPartitions` với state phức tạp mà built-in/pandas_udf không diễn đạt nổi; (b) dữ liệu đầu vào phi cấu trúc kỳ dị phải parse thủ công trước khi có schema; (c) tương tác API cũ chỉ nhận RDD. Kể cả khi đó: khoanh vùng RDD thật hẹp, vào–ra khỏi RDD càng sớm càng tốt, và ghi chú lý do trong code — vì mỗi dòng RDD là một dòng Catalyst không bảo kê được.

---

## 14. Summary

### Mindmap

```
                          SPARK LESSON 2
                               │
     ┌──────────────┬──────────┴───────────┬───────────────────┐
     ▼              ▼                      ▼                   ▼
 SPARKSESSION    3 TẦNG API           TRANSFORMATION        LAZY EVAL
     │              │                  vs ACTION               │
 chứa            RDD: hộp đen,           │                 gom plan → Catalyst
 SparkContext    lineage, resilient   transform = ghi order  không materialize
 catalog, conf   DF: schema →         action = nấu (job)    chỉ tính cái cần
 getOrCreate     Catalyst+Tungsten    narrow: tại chỗ       lineage = công thức
 (config lần 2   DS: typed,           wide: SHUFFLE          → recompute khi chết
  có thể bị lờ)  chỉ Scala/Java       = ranh giới stage     giá: lỗi nổ tại action
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại SparkSession anatomy: cái gì nằm trong nó, SparkContext ở đâu.
- [ ] Giải thích RDD lineage và vì sao mất partition không cần replicate vẫn hồi phục được.
- [ ] Nói được 2 lý do DataFrame nhanh hơn RDD (Catalyst nhìn thấy cấu trúc; PySpark khỏi serialize sang Python).
- [ ] Phân loại nhanh 10 hàm bất kỳ: transformation/action, narrow/wide, không nhìn bảng.
- [ ] Kể đủ 4 lợi ích của lazy evaluation + 1 cái giá phải trả.
- [ ] Đã chạy lab: thấy transform ~0s, thấy RDD chậm hơn DataFrame bao nhiêu lần, đã mở tab SQL.
- [ ] Biết vì sao 2 action không cache = tính 2 lần.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 3 — Job / Stage / Task: mô hình thực thi chi tiết.**

Hôm nay bạn đã nắm chìa khóa: *wide transformation = shuffle*. Lesson 3 sẽ dùng chìa khóa đó mở căn phòng quan trọng nhất của Spark internals: **DAG Scheduler cắt job thành stage tại đúng các điểm shuffle** như thế nào, ShuffleMapStage khác ResultStage ra sao, một task sống chết thế nào từ lúc được serialize đến lúc báo kết quả, và tại sao 200 task trên 8 core phải chạy thành nhiều "wave". Bạn sẽ luyện kỹ năng đặc sản của senior: **nhìn code đoán trước số stage** — rồi mở DAG Visualization trên Spark UI kiểm chứng từng dự đoán. Và bạn sẽ gặp một bất ngờ dễ chịu tên là *skipped stage*.

Từ giờ, mỗi lần thấy `groupBy` hay `join`, trong đầu bạn phải hiện lên vết cắt stage. Lesson 3 rèn phản xạ đó.

> Gõ **"Continue"** khi sẵn sàng.
