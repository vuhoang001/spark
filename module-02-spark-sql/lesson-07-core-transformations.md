# Lesson 7 — Transformations cốt lõi: select, filter, withColumn, when

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 4 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Column expression** — viên gạch nhỏ nhất của DataFrame API: `F.col("x") > 100` không phải phép so sánh, mà là một **cây biểu thức lười** chờ Catalyst dịch.
- Bộ transformation dùng 90% thời gian: `select`, `selectExpr`, `filter`/`where`, `withColumn`, `withColumnRenamed`, `when/otherwise`, `cast`, `alias`, `drop`, `distinct`/`dropDuplicates`.
- **DataFrame API vs Spark SQL** (`createOrReplaceTempView`) — hai cú pháp, MỘT Catalyst plan. Chọn cái nào, khi nào.
- **Early filtering** — bài học performance đầu tiên của Module 2: lọc sớm rẻ hơn lọc muộn.

Sau bài này bạn phải làm được:

- Giải thích cho đồng nghiệp vì sao `F.col("price") * 1.1` chạy được mà không cần dữ liệu.
- Viết cùng một logic bằng cả API lẫn SQL, `explain()` ra và chỉ vào chỗ chứng minh 2 plan giống nhau.
- Nhìn một chuỗi `withColumn` trong loop và nói ngay: "chỗ này sẽ làm phình plan".

Kiến thức dùng trong thực tế: **mỗi ngày**. Bronze → silver layer về bản chất là một chuỗi select/filter/withColumn/when. Viết đẹp thì Catalyst tối ưu giúp; viết ẩu thì bài 13 (Catalyst) bạn sẽ thấy plan của mình dài như sớ.

---

## 2. Why

### Vấn đề: bạn cần một ngôn ngữ biến đổi dữ liệu mà Spark HIỂU ĐƯỢC

Nhớ lại lesson 2: transformation là lười — Spark chỉ ghi chép "việc cần làm" thành logical plan. Nhưng muốn ghi chép được, Spark phải **hiểu cấu trúc** của phép biến đổi. So sánh:

```python
# Cách 1 — hàm Python mờ đục (RDD style / UDF style):
rdd.filter(lambda row: row.price > 100)     # Spark thấy 1 cục lambda đen kịt,
                                            # không biết bên trong làm gì

# Cách 2 — Column expression trong suốt:
df.filter(F.col("price") > 100)             # Spark thấy rõ: cột `price`, phép `>`, hằng 100
                                            # → có thể pushdown xuống Parquet, reorder, tối ưu
```

Cách 1: Catalyst mù, phải chạy từng dòng qua Python. Cách 2: Catalyst đọc được **ý định**, nên có thể đẩy filter xuống tận tầng đọc file (predicate pushdown — lesson 5 bạn đã thấy), gộp các projection, cắt cột không dùng. Toàn bộ DataFrame API tồn tại để bạn *khai báo ý định* thay vì *viết lệnh chạy*.

> **Analogy phiên dịch**: bạn nói chuyện với đầu bếp (executor JVM) qua phiên dịch viên (Catalyst). Nói bằng Column expression = nói bằng từ vựng chuẩn mà phiên dịch viên thuộc lòng — dịch nhanh, dịch hay, còn gợi ý món ngon hơn. Nhét một lambda Python vào = đưa mảnh giấy viết tiếng lóng — phiên dịch viên bó tay, chuyển nguyên mảnh giấy vào bếp cho một anh phụ bếp Python đọc từng chữ (lesson 12 sẽ đo cái giá này).

### Nếu không nắm chắc bài này thì sao?

- Bạn sẽ viết `withColumn` trong for-loop 200 vòng → plan phình, driver ngồi analyze plan lâu hơn chạy job.
- Bạn sẽ filter SAU join thay vì TRƯỚC join → shuffle cả đống dữ liệu rồi mới vứt đi.
- Bạn sẽ cãi nhau "SQL nhanh hơn API" hay ngược lại — trong khi chúng ra cùng một plan.

### Trade-off: DataFrame API vs Spark SQL

| Tiêu chí | DataFrame API | Spark SQL (temp view) |
|---|---|---|
| Performance | Cùng Catalyst plan | **Cùng Catalyst plan** — không ai nhanh hơn ai |
| Compose/tái sử dụng | Mạnh: hàm Python nhận df trả df, if/else, loop | Yếu: string SQL khó lắp ghép, dễ lỗi f-string |
| Bắt lỗi sớm | Lỗi tên cột nổ khi **định nghĩa** (analysis time) | Lỗi nằm im trong string đến khi chạy câu SQL |
| Ai đọc dễ | Data engineer | Analyst, người từ thế giới DWH |
| Logic dài, nhiều bước | Dễ chia hàm nhỏ, unit test từng bước | Query 300 dòng khó test từng khúc |
| Ad-hoc exploration | Dài dòng hơn | Gõ nhanh, quen tay |

> Kết luận Senior: **pipeline production → API** (testable, composable); **exploration & phần logic dạng báo cáo thuần → SQL** cũng tốt. Trộn cả hai trong một job là bình thường, không phải tội lỗi.

---

## 3. Theory

### 3.1. Column expression — nhân vật chính

`F.col("price")` trả về object kiểu `Column`. Nó **không chứa dữ liệu**. Nó là một node trong cây biểu thức:

```
   F.when(F.col("price") * 1.1 > 100, "high").otherwise("low")

                    CaseWhen
                   /        \
              điều kiện    "high" / "low"
                 │
                 >
               /   \
              *     100
            /   \
      col(price) 1.1
```

Mọi toán tử bạn áp lên Column (`+`, `>`, `&`, `|`, `.cast()`, `.alias()`, `F.upper(...)`) chỉ **đắp thêm node** vào cây. Cây này được nhét vào logical plan, và chỉ khi gặp action, Catalyst mới dịch nó thành code chạy trên executor (thậm chí codegen ra Java bytecode — lesson 13).

Ba cách viết tương đương, gặp cả ba ngoài đời:

```python
df.filter(F.col("price") > 100)   # chuẩn mực, dùng được mọi nơi — khuyên dùng
df.filter(df.price > 100)         # ngắn, nhưng kẹt khi tên cột trùng keyword/có ký tự lạ,
                                  # và nguy hiểm khi self-join (2 df cùng lineage)
df.filter("price > 100")          # string SQL expression — tiện, nhưng lỗi chỉ lộ lúc parse
```

**Lưu ý ký hiệu logic** (khác Python thường — lỗi kinh điển tuần đầu):

```python
# ĐÚNG: & | ~ và BẮT BUỘC đóng ngoặc từng vế (vì độ ưu tiên toán tử)
df.filter((F.col("price") > 100) & (F.col("order_status") == "delivered"))

# SAI: and/or/not → Python đòi giá trị bool ngay → nổ
# "ValueError: Cannot convert column into bool"
df.filter(F.col("price") > 100 and F.col("order_status") == "delivered")
```

### 3.2. Bộ transformation cốt lõi — bản đồ

| Phép | Làm gì | Ghi nhớ |
|---|---|---|
| `select` | Chọn/tính toán cột, **định hình lại** DataFrame | Projection — cắt cột sớm = đọc ít dữ liệu |
| `selectExpr` | `select` nhưng nhận string SQL expression | Nhanh tay cho biểu thức ngắn |
| `filter` / `where` | Giữ dòng thỏa điều kiện | **Alias 100% của nhau** — where cho người gốc SQL |
| `withColumn` | Thêm/ghi đè 1 cột | Mỗi lần gọi = 1 projection mới chồng lên plan |
| `withColumnRenamed` | Đổi tên cột | Không tính toán gì, chỉ đổi metadata |
| `when/otherwise` | CASE WHEN của SQL | Không `otherwise` → rơi vào **null**, không phải lỗi |
| `cast` | Đổi kiểu dữ liệu | Cast fail → **null lặng lẽ**, không exception |
| `alias` | Đặt tên cho biểu thức/DataFrame | Bắt buộc sau khi tính toán, nếu không tên cột thành `(price * 1.1)` |
| `drop` | Bỏ cột | Bỏ cột không tồn tại → im lặng, không lỗi |
| `distinct` | Bỏ dòng trùng (mọi cột) | **Gây shuffle** |
| `dropDuplicates([cols])` | Bỏ trùng theo tập cột | Giữ dòng "nào đó" — **không xác định dòng nào** nếu không sort |

`distinct` và `dropDuplicates` là 2 kẻ duy nhất trong bảng gây **shuffle** (phải gom các dòng giống nhau về cùng chỗ mới biết trùng). Còn lại toàn narrow transformation — rẻ, pipeline được trong 1 stage.

### 3.3. DataFrame API vs Spark SQL — cùng một đích đến

```python
orders.createOrReplaceTempView("orders")     # đăng ký tên bảng ảo trong session catalog
sql_df = spark.sql("SELECT order_id FROM orders WHERE order_status = 'delivered'")
api_df = orders.filter(F.col("order_status") == "delivered").select("order_id")
```

```
   Chuỗi API .filter().select()          String SQL "SELECT ... WHERE ..."
              │                                      │
              ▼                                      ▼ (parser)
        ┌──────────────── Unresolved Logical Plan ────────────────┐
        │                    (hội tụ tại đây)                     │
        └───────────────┬──────────────────────────────────────---┘
                        ▼  Analyzer (soi tên cột/bảng vào catalog)
                 Logical Plan  →  Catalyst Optimizer  →  Physical Plan  →  chạy
```

`createOrReplaceTempView` **không copy dữ liệu, không chạy gì** — chỉ ghi tên vào catalog của session, trỏ về logical plan của DataFrame. View sống theo SparkSession, session tắt là mất.

Kiểm chứng bằng `explain()`: hai plan giống nhau đến từng node. Từ nay ai nói "viết SQL cho nhanh hơn API", bạn có bằng chứng phản biện.

### 3.4. Early filtering — lọc sớm nhất có thể

```
TỆ:   đọc 100M dòng → join (shuffle 100M) → tính toán → filter còn 2M   😱 shuffle 98M dòng vô ích
TỐT:  đọc 100M dòng → filter còn 2M → join (shuffle 2M) → tính toán     ✅
```

Catalyst có rule **PushDownPredicate** tự kéo filter xuống sớm giúp bạn trong nhiều trường hợp — nhưng nó bó tay khi filter đứng sau UDF, sau aggregation phức tạp, hoặc điều kiện phụ thuộc cột vừa tính. Thói quen đúng: **tự tay viết filter sớm nhất có thể**, coi Catalyst là lưới an toàn chứ không phải người dọn rác.

---

## 4. Internal

### withColumn và chuyện phình plan

Mỗi `withColumn` tạo một **Project node mới** bọc quanh plan cũ:

```python
for c in df.columns:                       # giả sử 200 cột
    df = df.withColumn(c, F.trim(F.col(c)))
```

```
Project(trim c200)
  └─ Project(trim c199)
       └─ Project(trim c198)
            └─ ... 200 tầng Project lồng nhau ...
                 └─ Scan csv
```

Hệ quả: **analyzer/optimizer phải đi qua plan 200 tầng**, thời gian phân tích plan tăng phi tuyến theo số tầng; job của bạn "đứng hình" ở driver vài chục giây đến vài phút *trước khi executor làm bất cứ việc gì*. Catalyst có rule `CollapseProject` gộp các Project liền kề, nhưng bản thân việc gộp cũng phải duyệt cây, và nhiều trường hợp (biểu thức tham chiếu cột vừa tạo) không gộp được.

Cách chữa — gom về **một** projection:

```python
# Cách 1: select với list comprehension — 1 Project duy nhất
df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])

# Cách 2 (Spark 3.3+): withColumns nhận dict — cũng 1 Project
df = df.withColumns({c: F.trim(F.col(c)) for c in df.columns})
```

### Analysis time vs run time — lỗi nổ ở đâu?

```python
df.select("khong_ton_tai")    # nổ NGAY dòng này: AnalysisException
                              # (analyzer soi schema — không cần chạy job)
df.select(F.col("price").cast("int"))  # KHÔNG nổ dù price có chữ —
                                       # cast fail từng dòng → null lặng lẽ lúc RUN
```

Spark có 3 thời điểm lỗi: **parse time** (SQL sai cú pháp), **analysis time** (tên cột/bảng sai — nổ khi định nghĩa transformation, chưa cần action), **run time** (dữ liệu bẩn — chỉ lộ khi action chạy). Hiểu điều này giúp bạn đọc stack trace nhanh gấp 3.

### Immutability

DataFrame **bất biến**. `df.filter(...)` không sửa `df` — nó trả DataFrame MỚI trỏ về plan mới. Viết `df.filter(...)` mà không gán lại → dòng code vô nghĩa (lỗi thật, gặp thật ở junior code review, không đùa).

---

## 5. API

### `F.col(name)` / biểu thức Column

```python
from pyspark.sql import functions as F
expr = (F.col("price") + F.col("freight_value")).alias("total_cost")
```
- **Ý nghĩa**: tạo node tham chiếu cột; mọi toán tử đắp thêm cây biểu thức.
- **Pitfall**: `&`/`|`/`~` chứ không phải `and`/`or`/`not`; từng vế phải trong ngoặc. So sánh null: `== None` sai — dùng `F.col("x").isNull()`.

### `select(*cols)` / `selectExpr(*exprs)`

```python
df.select("order_id", F.col("price").cast("double"),
          (F.col("price") * 0.1).alias("tax"))
df.selectExpr("order_id", "price * 0.1 AS tax", "CAST(price AS double) AS price_d")
```
- **Ý nghĩa**: projection — kết quả CHỈ có những cột bạn liệt kê.
- **Khi dùng**: định hình schema đầu ra; cắt cột sớm để column pruning.
- **Pitfall**: quên `alias` → tên cột thành `(price * 0.1)`, downstream gọi không nổi. `select("*", new_col)` giữ mọi cột cũ và thêm cột mới — thay được nhiều withColumn.

### `filter(cond)` / `where(cond)`

```python
df.filter((F.col("order_status") == "delivered") & F.col("order_approved_at").isNotNull())
```
- **Ý nghĩa**: giữ dòng thỏa điều kiện. `where` là alias tuyệt đối của `filter`.
- **Pitfall**: điều kiện so sánh với **null luôn cho null** → dòng bị LOẠI. `filter(F.col("x") != "cancel")` sẽ loại cả dòng `x IS NULL` — three-valued logic, lesson 14 mổ kỹ. Muốn giữ null phải viết tường minh `(F.col("x") != "cancel") | F.col("x").isNull()`.

### `withColumn(name, col)` / `withColumnRenamed(old, new)` / `withColumns(dict)`

```python
df = df.withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
df = df.withColumnRenamed("order_purchase_timestamp", "purchased_at")
```
- **Ý nghĩa**: thêm cột mới hoặc **ghi đè** cột trùng tên; rename chỉ đổi metadata.
- **Pitfall số 1 của bài** (nhắc lần 2 vì gặp hoài): `withColumn` trong loop dài → plan phình → driver tê liệt ở khâu analyze. Gom về 1 `select`/`withColumns`.
- **Pitfall phụ**: ghi đè cột cùng tên khiến bạn mất bản gốc — đặt tên mới rồi `drop` cột cũ khi cần rõ ràng.

### `F.when(cond, val).when(...).otherwise(val)`

```python
df.withColumn("price_tier",
    F.when(F.col("price") >= 500, "premium")
     .when(F.col("price") >= 100, "standard")
     .otherwise("budget"))
```
- **Ý nghĩa**: CASE WHEN — đánh giá tuần tự, khớp điều kiện đầu tiên thì dừng.
- **Pitfall**: quên `otherwise` → dòng không khớp nhận **null** (im lặng, không cảnh báo). Thứ tự `when` quan trọng: điều kiện hẹp đặt trước.

### `cast(type)` / `alias(name)`

```python
F.col("price").cast("decimal(12,2)")
F.col("customer_zip_code_prefix").cast("string")
```
- **Ý nghĩa**: đổi kiểu; theo chuẩn SQL, cast không được thì trả **null** chứ không ném lỗi (khác `spark.sql.ansi.enabled=true` — chế độ ANSI sẽ ném lỗi, mặc định 3.4 vẫn off).
- **Pitfall**: cast string→int trên cột có chữ = cột null hàng loạt mà pipeline vẫn "xanh". Production: sau cast, đếm null tăng thêm để phát hiện (`F.count(F.when(F.col("x").isNull(), 1))`).

### `drop(*cols)` / `distinct()` / `dropDuplicates([cols])`

```python
df.drop("product_length_cm", "product_height_cm")
df.select("customer_state").distinct()
df.dropDuplicates(["order_id"])          # mỗi order giữ đúng 1 dòng
```
- **Pitfall `drop`**: gõ sai tên cột → không lỗi, cột "không bị bỏ" một cách bí ẩn.
- **Pitfall `dropDuplicates`**: giữ dòng NÀO trong nhóm trùng là **không xác định** (phụ thuộc partition/order thực thi). Cần "giữ dòng mới nhất" → dùng window `row_number` (lesson 10), đừng tin dropDuplicates.
- **Performance**: cả hai gây **shuffle** — thấy chúng trong code là thấy tiền.

### `createOrReplaceTempView(name)` + `spark.sql(query)`

```python
orders.createOrReplaceTempView("orders")
spark.sql("SELECT order_status, count(*) FROM orders GROUP BY order_status")
```
- **Ý nghĩa**: đăng ký alias trong session catalog; `spark.sql` trả DataFrame — trộn tiếp với API thoải mái.
- **Pitfall**: tên view toàn cục cho cả session — job lớn nhiều module dễ giẫm tên nhau (`createOrReplace` ghi đè im lặng). Đặt prefix theo module.

---

## 6. Demo nhỏ

```
Input:  5 đơn hàng tạo tay (price, status lẫn null và chữ bẩn)
   ↓    cast + when/otherwise + filter (toàn transformation — chưa chạy)
Output: show() với cả API lẫn SQL — so 2 explain
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo07").master("local[2]").getOrCreate()

data = [("o1", "120.5", "delivered"), ("o2", "abc", "delivered"),
        ("o3", "80", "canceled"),     ("o4", "610", None), ("o5", "45.9", "delivered")]
df = spark.createDataFrame(data, ["order_id", "price_raw", "status"])

clean = (df
    .withColumn("price", F.col("price_raw").cast("double"))       # "abc" → null lặng lẽ!
    .withColumn("tier",
        F.when(F.col("price") >= 500, "premium")
         .when(F.col("price") >= 100, "standard")
         .otherwise("budget"))                                    # price null → tier "budget"?? Không — null so sánh ra null → rơi vào otherwise. Bẫy!
    .filter(F.col("status") == "delivered")                       # o4 status null → BỊ LOẠI luôn
    .select("order_id", "price", "tier"))

clean.show()
# +--------+-----+--------+
# |order_id|price|    tier|
# +--------+-----+--------+
# |      o1|120.5|standard|
# |      o2| null|  budget|   ← cast fail thành null, rồi rơi otherwise: dữ liệu bẩn đội lốt "budget"
# |      o5| 45.9|  budget|
# +--------+-----+--------+

df.createOrReplaceTempView("raw_orders")
sql_clean = spark.sql("""
    SELECT order_id, CAST(price_raw AS double) AS price,
           CASE WHEN CAST(price_raw AS double) >= 500 THEN 'premium'
                WHEN CAST(price_raw AS double) >= 100 THEN 'standard'
                ELSE 'budget' END AS tier
    FROM raw_orders WHERE status = 'delivered'
""")

clean.explain()      # so sánh 2 physical plan —
sql_clean.explain()  # giống nhau: cùng Project + Filter + Scan
spark.stop()
```

Hai bẫy trong demo — nhìn tận mắt một lần, nhớ cả đời: (1) cast fail → null im lặng; (2) null rơi vào `otherwise` như thể nó là dữ liệu hợp lệ.

---

## 7. Production Example

Bronze → Silver cleaning cho bảng orders — pattern chuẩn mà mọi công ty chạy hàng đêm:

```python
def clean_orders(bronze: "DataFrame") -> "DataFrame":
    """Silver: chuẩn hóa kiểu, chuẩn hóa giá trị, lọc rác, thêm cột dẫn xuất.
    Nhận df trả df → unit test được bằng chispa, compose được trong pipeline."""
    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date",
               "order_estimated_delivery_date"]
    return (bronze
        # 1 projection duy nhất cho mọi cast — KHÔNG withColumn trong loop
        .select("order_id", "customer_id",
                F.lower(F.trim(F.col("order_status"))).alias("order_status"),
                *[F.to_timestamp(F.col(c)).alias(c) for c in ts_cols])
        # early filtering: loại rác trước mọi join/aggregate phía sau
        .filter(F.col("order_id").isNotNull() & F.col("order_purchase_timestamp").isNotNull())
        .withColumns({
            "purchase_date": F.to_date("order_purchase_timestamp"),
            "is_delivered":  (F.col("order_status") == "delivered"),
            "delivery_days": F.datediff("order_delivered_customer_date",
                                        "order_purchase_timestamp"),
            "delivery_status": F.when(F.col("order_delivered_customer_date").isNull(), "in_transit")
                .when(F.col("order_delivered_customer_date") <=
                      F.col("order_estimated_delivery_date"), "on_time")
                .otherwise("late"),
        })
        .dropDuplicates(["order_id"]))
```

Điểm Senior trong đoạn này: (1) hàm thuần nhận df trả df — test được không cần cluster; (2) mọi cast gom 1 projection; (3) filter đứng ngay sau chuẩn hóa, trước mọi phép đắt; (4) `when` xử lý null tường minh (check `isNull` TRƯỚC khi so sánh ngày); (5) logic nghiệp vụ (`on_time`/`late`) nằm ở MỘT chỗ có tên, không rải rác.

---

## 8. Hands-on Lab

**Mục tiêu**: xây 10 transformation trên Olist thật, chứng minh SQL và API cùng plan.

### Bước 1 — tạo `labs/lab07/transformations_olist.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab07-core-transformations").getOrCreate()
DATA = "/workspace/data/olist"

orders   = spark.read.csv(f"{DATA}/olist_orders_dataset.csv", header=True)
items    = spark.read.csv(f"{DATA}/olist_order_items_dataset.csv", header=True)
products = spark.read.csv(f"{DATA}/olist_products_dataset.csv", header=True)

# ── 10 transformations ──────────────────────────────────────────────
# 1. cast: CSV đọc không inferSchema → mọi cột là string, tự tay cast
items_t = items.select("order_id", "product_id", "seller_id",
                       F.col("price").cast("double").alias("price"),
                       F.col("freight_value").cast("double").alias("freight_value"))
# 2. withColumn dẫn xuất
items_t = items_t.withColumn("total_cost", F.col("price") + F.col("freight_value"))
# 3. when/otherwise phân tầng giá
items_t = items_t.withColumn("price_tier",
    F.when(F.col("price") >= 200, "premium")
     .when(F.col("price") >= 50, "standard").otherwise("budget"))
# 4. filter với null-safe logic
delivered = orders.filter((F.col("order_status") == "delivered")
                          & F.col("order_delivered_customer_date").isNotNull())
# 5. to_timestamp + to_date
delivered = delivered.withColumns({
    "purchased_at": F.to_timestamp("order_purchase_timestamp"),
    "purchase_date": F.to_date("order_purchase_timestamp")})
# 6. withColumnRenamed
products_t = products.withColumnRenamed("product_category_name", "category")
# 7. drop cột không dùng
products_t = products_t.drop("product_name_lenght", "product_description_lenght",
                             "product_photos_qty")
# 8. distinct
n_status = orders.select("order_status").distinct()
# 9. dropDuplicates theo key
one_seller_per_product = items_t.dropDuplicates(["product_id"])
# 10. selectExpr
summary = items_t.selectExpr("order_id", "round(total_cost, 2) AS total_cost",
                             "price_tier", "price > freight_value AS price_dominant")

print("Số order_status khác nhau:", n_status.count())
summary.show(5, truncate=False)

# ── SQL vs API: cùng một plan ───────────────────────────────────────
items_t.createOrReplaceTempView("items")
api_df = (items_t.filter(F.col("price_tier") == "premium")
                 .select("order_id", "price", "total_cost"))
sql_df = spark.sql("SELECT order_id, price, total_cost FROM items "
                   "WHERE price_tier = 'premium'")
print("=== API plan ==="); api_df.explain()
print("=== SQL plan ==="); sql_df.explain()

# ── withColumn loop vs 1 select: đo độ phình plan ──────────────────
import time
wide = items_t
t0 = time.time()
for i in range(150):
    wide = wide.withColumn(f"c{i}", F.col("price") * i)
wide.explain(mode="simple")          # ép analyze plan
print(f"withColumn x150: analyze mất {time.time()-t0:.2f}s")

t0 = time.time()
wide2 = items_t.select("*", *[(F.col("price") * i).alias(f"c{i}") for i in range(150)])
wide2.explain(mode="simple")
print(f"1 select 150 cột: analyze mất {time.time()-t0:.2f}s")

input(">>> Mở http://localhost:4040 tab SQL/DataFrame, xem plan. Enter để thoát.")
spark.stop()
```

### Bước 2 — chạy

```bash
make run F=labs/lab07/transformations_olist.py
# hoặc không qua cluster:
make run-local F=labs/lab07/transformations_olist.py
```

### Bước 3 — quan sát

1. Hai đoạn `explain()`: physical plan của API và SQL **giống nhau node-by-node**. Chụp lại làm bằng chứng.
2. Con số thời gian analyze của `withColumn x150` vs `1 select` — chênh bao nhiêu lần trên máy bạn?
3. Mở `http://localhost:4040` → tab **SQL / DataFrame**: click query cuối, nhìn cây plan render đẹp — đây là bản đồ bạn sẽ đọc suốt module này.
4. Ghi 3 quan sát vào `labs/lab07/NOTES.md`.

---

## 9. Assignment

**Easy** — Viết query "đếm số đơn theo `order_status`, chỉ tính đơn mua từ 2018" bằng **cả hai**: DataFrame API và Spark SQL (temp view). Hai kết quả phải khớp từng dòng (`subtract` cho kết quả rỗng theo cả 2 chiều). Dán 2 đoạn `explain()` và chỉ ra chúng giống nhau ở đâu.

**Medium** — Từ `order_items` + `products`, xây DataFrame gồm: `order_id`, `price`, `freight_value`, `freight_ratio` (= freight/price, xử lý chia 0 → null), `category`, và cột `shipping_bucket` (when/otherwise: `"free"` nếu freight = 0, `"cheap"` nếu ratio < 0.1, `"normal"` nếu < 0.3, còn lại `"expensive"`). Toàn bộ cast/dẫn xuất phải nằm trong **tối đa 2 projection** (đếm số Project trong explain để chứng minh).

**Hard** — Early filtering: viết 2 phiên bản job "tổng doanh thu các đơn delivered năm 2017 theo tháng" — bản A filter (`order_status`, năm) SAU khi join orders×items, bản B filter TRƯỚC join. So sánh trên Spark UI: shuffle write của stage join (Stages tab) mỗi bản là bao nhiêu MB? Sau đó `explain()` bản A — Catalyst có tự đẩy filter xuống trước join không? Kết luận 3–5 dòng: khi nào bạn phải tự tay early-filter dù đã có Catalyst.

**Production Challenge** — Viết module `labs/lab07/clean_olist.py` chứa 3 hàm thuần `clean_orders(df)`, `clean_items(df)`, `clean_products(df)` (nhận df trả df, không đọc file bên trong) chuẩn hóa: kiểu dữ liệu đúng, trim/lower text, loại dòng thiếu key, thêm 2 cột dẫn xuất có ý nghĩa nghiệp vụ mỗi bảng. Kèm `main()` đọc CSV → gọi 3 hàm → in schema + 5 dòng mỗi bảng. Ràng buộc: không `withColumn` nào nằm trong loop; mỗi hàm ≤ 1 lần filter (gom điều kiện). Đây chính là mầm của silver layer trong PROJECT 1 (tuần 7).

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| `select`/`filter`/`withColumn`/`when` | Nhanh (narrow) | Không shuffle; Catalyst pipeline nhiều phép vào 1 stage, codegen thành 1 vòng lặp |
| Filter sớm (trước join/groupBy) | Nhanh | Giảm số dòng đi vào shuffle — shuffle là tiền |
| `distinct`/`dropDuplicates` | Chậm | Shuffle toàn bộ cột so trùng; `dropDuplicates(["key"])` rẻ hơn `distinct()` full-row |
| `withColumn` × 200 trong loop | Chậm ở **driver** | Plan 200 tầng, analyze/optimize lâu — executor rảnh mà job vẫn ì |
| Select ít cột ngay sau read | Nhanh | Column pruning — với Parquet là đọc ít byte thật sự; CSV vẫn phải parse cả dòng |
| `selectExpr`/`spark.sql` vs API | Bằng nhau | Cùng Catalyst plan — đừng "tối ưu" bằng cách đổi cú pháp |

Câu tự vấn của Module 2, dán lên màn hình: *"phép này narrow hay wide? filter của tôi đã đứng sớm nhất chưa? plan của tôi có bao nhiêu tầng Project?"*

---

## 11. Spark UI

Bài này mở khóa tab mới: **SQL / DataFrame** (từ đây là tab bạn nhìn nhiều nhất trong Module 2).

- Mỗi dòng = 1 query (thường ứng với 1 action). Click vào → cây **physical plan dạng đồ họa**.
- Đọc từ **dưới lên**: `Scan csv` → `Filter` → `Project` → ... Mỗi node có metrics: `number of output rows` — nhìn số dòng SỐNG SÓT qua từng node, bạn thấy ngay early filtering hiệu quả cỡ nào (Scan ra 100k, Filter còn 96k, v.v.).
- Đối chiếu với `explain()` trong console: cùng nội dung, UI dễ đọc hơn, console nhanh hơn.
- Trong lab, so query `withColumn x150`: bạn sẽ thấy plan đã bị `CollapseProject` gộp lại — nhưng cái giá analyze ở driver thì bạn ĐÃ trả rồi (con số thời gian bạn đo).

Tab Jobs/Stages tiếp tục dùng như Module 1: bài này đa số narrow → job 1 stage; thấy stage thứ 2 mọc ra là do `distinct`/`dropDuplicates`.

---

## 12. Common Mistakes

1. **`and`/`or`/`not` thay vì `&`/`|`/`~`**, hoặc quên ngoặc từng vế → `ValueError: Cannot convert column into bool` hoặc filter sai lặng lẽ (do độ ưu tiên toán tử).
2. **`withColumn` trong loop dài** → plan lồng trăm tầng, driver analyze mãi không xong. Gom về 1 `select`/`withColumns`.
3. **Quên `otherwise`** → null im lặng; và **null rơi vào `otherwise`** như giá trị hợp lệ (demo mục 6). Với dữ liệu có null, viết nhánh `when(col.isNull(), ...)` tường minh ĐẦU TIÊN.
4. **`cast` fail thành null không ai biết** → cột doanh thu null 30% mà dashboard vẫn chạy, chỉ là... sai. Sau cast quan trọng, thêm bước đếm null.
5. **`filter(col != "x")` vô tình loại cả null** — three-valued logic. Đây là bug data quality phổ biến nhất trong các pipeline tôi từng review.
6. **Quên gán lại DataFrame** (`df.filter(...)` đứng một mình) — immutable, dòng code đó không làm gì cả.
7. **Tin `dropDuplicates` giữ "dòng mới nhất"** — nó giữ dòng không-xác-định. Cần deterministic → window function (lesson 10).
8. **Tranh cãi SQL vs API về performance** — cùng plan. Tranh cãi đúng phải về: testability, composability, ai maintain.

---

## 13. Interview

**Junior:**

1. *Column expression là gì? `F.col("price") > 100` trả về cái gì?* — Trả về object `Column` — một cây biểu thức mô tả phép so sánh, KHÔNG phải giá trị bool. Nó lười: chỉ được Catalyst dịch thành code khi action chạy. Nhờ trong suốt với Catalyst nên tối ưu được (pushdown, pruning) — khác lambda/UDF mờ đục.
2. *`filter` và `where` khác gì nhau?* — Không khác gì, alias 100%. `where` tồn tại cho người quen SQL. Tương tự `select` các cột bằng string hay `F.col` đều được.
3. *`withColumn` làm gì? Gọi nó có sửa DataFrame gốc không?* — Thêm hoặc ghi đè (nếu trùng tên) một cột, trả về DataFrame MỚI — DataFrame bất biến, phải gán lại. Bên trong nó tạo một Project node mới bọc plan cũ.
4. *`when/otherwise` — nếu quên `otherwise` thì dòng không khớp điều kiện nhận giá trị gì?* — `null`. Không lỗi, không cảnh báo. Và ngược lại: dòng có input null thường trôi xuống `otherwise` vì mọi so sánh với null cho null (không phải True).

**Mid:**

5. *DataFrame API và Spark SQL, cái nào nhanh hơn? Chứng minh thế nào?* — Bằng nhau: cả hai hội tụ về cùng unresolved logical plan, qua cùng Analyzer/Catalyst/physical planning. Chứng minh: `explain()` hai phiên bản, plan giống nhau. Chọn theo tiêu chí khác: API composable/testable cho pipeline; SQL thân thiện analyst, gọn cho logic báo cáo.
6. *`createOrReplaceTempView` có materialize dữ liệu không? View sống bao lâu?* — Không — chỉ đăng ký tên trong session catalog trỏ tới logical plan; mỗi lần query view là plan được thực thi lại (trừ khi df được cache). View sống theo SparkSession; `createGlobalTempView` sống theo application, truy cập qua `global_temp.<name>`.
7. *Tại sao gọi `withColumn` vài trăm lần trong loop làm job chậm dù dữ liệu nhỏ?* — Mỗi lần gọi thêm 1 tầng Project vào logical plan; analyzer/optimizer duyệt cây phình theo số tầng, chi phí ở DRIVER trước khi executor chạy. Fix: gom 1 `select` với list comprehension hoặc `withColumns` (3.3+). Điểm cộng: nêu được `CollapseProject` giúp nhưng không miễn phí và không phải mọi case.
8. *Early filtering là gì, Catalyst đã có predicate pushdown thì mình còn phải quan tâm không?* — Đặt filter trước các phép đắt (join/agg) để giảm dữ liệu vào shuffle. Catalyst tự đẩy filter trong nhiều case, nhưng bó tay khi filter sau UDF, sau agg, hoặc plan phức tạp/non-deterministic — nên vẫn tự viết filter sớm; coi Catalyst là lưới an toàn.

**Senior:**

9. *Phân biệt lỗi analysis-time và run-time trong Spark. Thiết kế pipeline tận dụng điều này thế nào?* — Analysis-time: sai tên cột/bảng, sai kiểu biểu thức — nổ ngay khi định nghĩa transformation, không cần action, rẻ. Run-time: dữ liệu bẩn (cast fail→null, div 0), chỉ lộ khi chạy, hoặc tệ hơn là KHÔNG lộ (null lặng lẽ). Thiết kế: khai schema tường minh khi đọc; ép "fail sớm" bằng cách định nghĩa toàn bộ plan trước action; thêm data quality check tường minh sau các điểm cast/join; cân nhắc `spark.sql.ansi.enabled=true` để cast fail ném lỗi thay vì null.
10. *Filter `col("status") != "canceled"` trên cột có null — chuyện gì xảy ra và vì sao? Hệ quả production?* — Dòng null bị loại: `null != 'canceled'` cho `null`, filter chỉ giữ `true`. Hệ quả: mất dữ liệu lặng lẽ, số liệu hụt mà không có lỗi nào — bug data quality khó truy nhất vì mọi thứ "chạy xanh". Fix: `(col != lit) | col.isNull()`, hoặc `eqNullSafe`/`<=>`, và data quality check đếm dòng trước/sau filter. Trả lời được kèm ví dụ thật là dấu hiệu đã từng bị đốt.

---

## 14. Summary

### Mindmap

```
                      LESSON 7 — CORE TRANSFORMATIONS
                                   │
     ┌──────────────┬──────────────┼────────────────────┬──────────────────┐
     ▼              ▼              ▼                    ▼                  ▼
 COLUMN EXPR     BỘ PHÉP        API vs SQL          INTERNAL           PERFORMANCE
     │              │              │                    │                  │
 cây biểu thức   select/filter  temp view = alias    withColumn =       early filtering
 lười, trong     withColumn(s)  cùng 1 Catalyst      1 tầng Project     distinct/dropDup
 suốt với        when/otherwise plan → cùng tốc độ   loop → plan phình  = shuffle
 Catalyst        cast→null!     chọn theo maintain   lỗi analysis vs    narrow còn lại
 & | ~ + ngoặc   drop/distinct                       run time           → 1 stage
```

### Checklist trước khi gõ "Continue"

- [ ] Giải thích được Column expression là cây biểu thức lười, vì sao Catalyst cần nó trong suốt.
- [ ] Thuộc bẫy: `&|~` + ngoặc; cast fail → null; quên otherwise → null; `!=` loại cả null.
- [ ] Chứng minh được (bằng explain) API và SQL cùng plan.
- [ ] Nói được vì sao withColumn trong loop hại, và fix bằng gì.
- [ ] Đã chạy lab, so thời gian analyze 2 cách, mở tab SQL/DataFrame đọc plan.
- [ ] Biết `distinct`/`dropDuplicates` gây shuffle, và dropDuplicates không deterministic.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 8 — Aggregations & groupBy: hash aggregate hoạt động ra sao.**

Hôm nay mọi phép đều narrow — dữ liệu đứng yên tại chỗ. Bài sau ta bước vào thế giới wide: `groupBy` buộc mọi dòng cùng key phải GẶP NHAU, nghĩa là shuffle, nghĩa là tiền. Nhưng Spark có một mánh đẹp: **partial aggregate** — đếm trước một phần ngay tại chỗ rồi mới gửi đi, giảm dữ liệu bay qua network hàng chục lần. Ta sẽ mổ `HashAggregateExec`, xem hash table trong memory đầy thì spill ra sao, và vì sao `countDistinct` đắt gấp nhiều lần `count` — còn `approx_count_distinct` rẻ bất ngờ nhờ một thuật toán tên HyperLogLog.

Aggregation là trái tim của mọi báo cáo. Hiểu nó chạy ra sao = biết vì sao dashboard của bạn tính 5 phút hay 5 giây.

> Gõ **"Continue"** khi sẵn sàng.
