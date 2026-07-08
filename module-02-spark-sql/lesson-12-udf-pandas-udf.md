# Lesson 12 — UDF vs built-in vs pandas UDF

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 6 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Python UDF thực sự chạy ở đâu** — và tại sao câu trả lời đó là bản án tử cho performance (serialize từng row, rời JVM, Catalyst mù).
- **pandas UDF (vectorized UDF)**: cách Arrow chuyển dữ liệu theo **batch columnar** thay vì từng dòng, và tại sao nó nhanh hơn UDF thường 3–100×.
- Benchmark tận tay: built-in vs Python UDF vs pandas UDF trên cùng một logic.
- Khi nào **bắt buộc** phải UDF (và cách giảm thiểu thiệt hại).
- `mapInPandas` / `applyInPandas` — hai API cho logic phức tạp mức partition/nhóm.
- Thứ tự ưu tiên khắc cốt ghi tâm: **built-in > SQL expression > pandas_udf > udf**.

Sau bài này bạn phải làm được:

- Nhìn một đoạn code có `@udf` và ước lượng được nó đắt cỡ nào, đề xuất được bản viết lại bằng built-in nếu có thể.
- Vẽ lại diagram JVM ↔ Python worker từ trí nhớ và chỉ ra đúng chỗ tiền bị đốt.
- Viết pandas UDF đúng chuẩn Spark 3.4 (type hint style) và giải thích Arrow đóng vai trò gì.

Kiến thức dùng trong thực tế: **đây là lỗi performance số 1 trong code PySpark của các team mới**. Trong Project 3 ("Cứu pipeline chậm"), một trong ba thủ phạm mentor cài sẵn chính là UDF. Interviewer Senior gần như chắc chắn hỏi "tại sao Python UDF chậm" — và câu trả lời hời hợt ("vì Python chậm") là câu trả lời trượt.

---

## 2. Why

### Vấn đề: built-in không phủ hết thế giới

Spark có hơn 300 built-in functions, nhưng logic nghiệp vụ thì vô hạn: chuẩn hóa địa chỉ tiếng Việt, tính khoảng cách haversine, gọi thư viện `phonenumbers` để validate SĐT, chạy model sklearn để scoring... Spark cho bạn lối thoát: **UDF (User-Defined Function)** — nhét hàm Python của bạn vào giữa pipeline.

Nghe tuyệt vời. Vấn đề: **PySpark chỉ là remote control** (bạn đã học ở lesson 1). Dữ liệu nằm trong **JVM executor**, còn hàm Python của bạn chỉ chạy được trong **process Python**. Muốn chạy UDF, Spark phải làm điều không tự nhiên: **bê dữ liệu ra khỏi JVM, đưa sang Python, rồi bê kết quả về**. Mỗi chuyến đi đó có giá, và với UDF thường, giá được tính **theo từng dòng**.

### Cái giá cụ thể (con số để kể trong phỏng vấn)

Cùng phép nhân đơn giản trên 10 triệu dòng (đo trên laptop, con số tương đối):

| Cách viết | Thời gian | So với built-in |
|---|---|---|
| `F.col("x") * 2` (built-in) | ~1s | 1× |
| pandas UDF | ~3–5s | 3–5× |
| Python UDF thường | ~30–60s | **30–60×** |

Logic càng nhẹ, phần trăm overhead càng thảm — vì thời gian thật nằm ở **vận chuyển**, không phải tính toán.

### Nếu không hiểu bài này thì sao?

Bạn sẽ viết pipeline "trông rất Python": vài chục `@udf` xinh xắn, chạy đúng, được merge... rồi 6 tháng sau dữ liệu tăng 10× và pipeline từ 20 phút thành 8 tiếng. Người được gọi vào cứu sẽ mở Spark UI, thấy `BatchEvalPython` rải khắp plan, và thở dài. Đừng là người viết — hãy là người cứu.

### Trade-off

| Được (UDF) | Mất (UDF) |
|---|---|
| Logic tùy ý, dùng cả hệ sinh thái Python | Serialize/deserialize từng dòng qua ranh giới JVM↔Python |
| Code quen tay với dev Python | **Catalyst mù hoàn toàn** — không pushdown, không codegen xuyên qua |
| Lối thoát khi built-in bó tay | Python worker ăn thêm memory ngoài JVM (dễ OOM cấp container) |

---

## 3. Theory

### 3.1. Tại sao Python UDF giết performance — mổ xẻ từng nhát

Khi executor gặp UDF thường trong plan, với **mỗi dòng** dữ liệu:

```
         EXECUTOR (1 worker node)
┌─────────────────────────────────────────────────────────────────┐
│   JVM (nơi dữ liệu thật sự sống)          PYTHON WORKER          │
│  ┌──────────────────────────┐            ┌─────────────────────┐ │
│  │ Tungsten InternalRow      │            │                     │ │
│  │ (binary, columnar, gọn)   │            │  hàm udf của bạn    │ │
│  │                           │            │                     │ │
│  │  row 1 ──① serialize────────pickle───▶ ② deserialize        │ │
│  │           (Pickler)       │  qua       │ ③ chạy hàm Python   │ │
│  │  row 1 ◀─⑤ deserialize──────socket◀─── ④ serialize kết quả  │ │
│  │                           │            │                     │ │
│  │  row 2 ──① ... lặp lại ──▶            │  ... lặp lại ...    │ │
│  │  row 3 ──① ... lặp lại ──▶            │  (×10 triệu lần)    │ │
│  └──────────────────────────┘            └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
   * Spark có pipeline theo batch nhỏ để giảm syscall, nhưng bản chất
     mỗi ROW vẫn phải pickle/unpickle riêng lẻ — chi phí per-row.
```

Bốn vết thương, cộng dồn:

1. **Serialization per-row**: mỗi dòng phải rời định dạng binary Tungsten → pickle → gửi qua local socket → unpickle thành Python object → (chiều về cũng vậy). Chi phí này thường LỚN HƠN chính logic của bạn.
2. **Mất Catalyst**: với optimizer, UDF là **black box** — một hàm không đọc được ruột. Filter nằm sau UDF không được đẩy xuống trước scan (không predicate pushdown xuyên UDF), không constant folding, và **whole-stage codegen bị cắt đứt** tại node UDF (lesson 13 sẽ thấy trong plan).
3. **Python interpreter chậm** cho vòng lặp per-row — yếu tố mọi người đổ lỗi đầu tiên nhưng thật ra thường chỉ xếp thứ ba.
4. **Memory kép**: Python worker là process riêng, ăn RAM ngoài heap JVM. Container bị giới hạn memory tổng → JVM tưởng còn dư mà container bị kill (lỗi kinh điển trên K8s/YARN: "Container killed by YARN for exceeding memory limits").

> **Analogy hải quan**: dữ liệu trong JVM là hàng trong kho nội địa. UDF thường = xuất khẩu **từng gói hàng một**: mỗi gói khai tờ hải quan riêng (pickle), qua cửa khẩu (socket), dỡ ra (unpickle), gia công, rồi làm thủ tục ngược lại. pandas UDF = đóng **nguyên container** hàng nghìn gói, một tờ khai chung (Arrow batch). Built-in = gia công ngay trong kho, khỏi xuất khẩu.

### 3.2. pandas UDF — vẫn qua cửa khẩu, nhưng đi container

pandas UDF (còn gọi vectorized UDF, từ Spark 2.3, chín muồi ở 3.x) thay đổi **đơn vị vận chuyển**:

```
         JVM                    Apache Arrow                PYTHON WORKER
┌──────────────────────┐   (định dạng columnar        ┌──────────────────────┐
│ 10.000 rows          │    chung, 2 bên đọc trực     │ pd.Series 10.000     │
│ (columnar Tungsten)  │──▶ tiếp, gần zero-copy)  ──▶ │ phần tử              │
│                      │         1 CHUYẾN             │ hàm của bạn chạy     │
│ nhận về 1 batch  ◀───│◀────────────────────────────│ VECTORIZED (numpy/   │
│                      │                              │ pandas C code)       │
└──────────────────────┘                              └──────────────────────┘
```

Hai cải tiến độc lập nhưng cộng hưởng:

1. **Arrow batch thay vì pickle per-row**: Arrow là định dạng columnar in-memory chuẩn chung — JVM ghi ra, Python đọc thẳng, gần như không tốn CPU chuyển đổi. Mặc định mỗi batch 10.000 dòng (`spark.sql.execution.arrow.maxRecordsPerBatch`). Chi phí vận chuyển chia cho 10.000 thay vì trả từng dòng.
2. **Vectorized execution**: hàm của bạn nhận `pd.Series` (cả cột), trả `pd.Series`. Phép toán pandas/numpy chạy bằng C loop, không phải Python loop.

Nhưng nhớ: pandas UDF **vẫn là black box với Catalyst** và **vẫn rời JVM**. Nó chỉ giảm chi phí vận chuyển + tính toán, không lấy lại được optimization. Vì thế built-in vẫn vô địch.

### 3.3. Thứ tự ưu tiên — quy tắc kim cương

```
① BUILT-IN functions (F.*)          ← 95% trường hợp có đường này, kể cả khi bạn tưởng không
② SQL EXPRESSION (F.expr)           ← built-in dạng SQL: CASE WHEN phức tạp, higher-order
                                       functions (transform/filter/aggregate trên array)
③ PANDAS_UDF                        ← khi thật sự cần thư viện Python (sklearn, phonenumbers...)
④ UDF THƯỜNG                        ← đường cùng: logic per-row không vector hóa được,
                                       hoặc kiểu dữ liệu Arrow không hỗ trợ
```

Trước khi viết UDF, tự vấn 3 câu: (1) tổ hợp `when/regexp_replace/split/transform...` có làm được không? (2) higher-order function trên array có làm được không? (3) logic có vector hóa bằng pandas được không? Trả lời hết "không" mới được mở `@udf`.

### 3.4. applyInPandas / mapInPandas — cho logic to hơn một cột

| API | Đơn vị nhận | Dùng khi |
|---|---|---|
| `pandas_udf` | 1+ cột (pd.Series) → 1 cột | Biến đổi cột, giữ nguyên số dòng |
| `mapInPandas(func, schema)` | iterator các pd.DataFrame (mỗi cái ~1 Arrow batch của partition) → iterator pd.DataFrame | Biến đổi cấp bảng: đổi cả schema, lọc, sinh nhiều/ít dòng hơn |
| `groupBy(...).applyInPandas(func, schema)` | **CẢ NHÓM** thành 1 pd.DataFrame → pd.DataFrame | Logic per-group: train model mỗi seller, interpolate chuỗi thời gian mỗi sensor |

⚠️ `applyInPandas` có hai chi phí giấu mặt: (a) **gây shuffle** (groupBy), (b) **cả nhóm phải vừa RAM Python worker** — nhóm 50M dòng = OOM. Đừng dùng nó thay cho `groupBy().agg()` thường.

---

## 4. Internal

### Vòng đời một Python worker

```
① Executor JVM khởi động, nhận task có UDF
② JVM fork/reuse python worker process (python.worker.reuse=true mặc định
   → không tốn phí spawn process mỗi task, nhưng worker chiếm RAM thường trú)
③ JVM đẩy: closure của hàm (pickle bởi cloudpickle, gửi 1 lần) + dữ liệu
   - UDF thường: stream các row đã pickle
   - pandas UDF: stream các Arrow record batch
④ Python worker chạy hàm, trả kết quả cùng kênh
⑤ JVM ghép kết quả về InternalRow, pipeline tiếp
```

Ba hệ quả thực chiến:

- **Closure phải pickle được**: hàm của bạn tham chiếu object không pickle được (connection DB, client Kafka, model chưa load...) → lỗi `PicklingError` ngay khi submit, hoặc tệ hơn: pickle được nhưng nặng, gửi lại mỗi lần. Pattern đúng: khởi tạo tài nguyên **bên trong** hàm/worker (lazy init, hoặc dùng iterator-style pandas UDF để init một lần mỗi partition).
- **Memory Python nằm ngoài JVM heap**: cấu hình `spark.executor.memoryOverhead` (hoặc `spark.executor.pyspark.memory`) phải nuôi được Python worker. Dùng pandas UDF với batch lớn + applyInPandas nhóm to → tăng overhead trước khi tăng heap.
- **Trong plan**: UDF thường hiện là **`BatchEvalPython`**, pandas UDF là **`ArrowEvalPython`** — thấy hai cái tên này trong `explain()` là biết dữ liệu đang xuất ngoại. Cả hai đều **chặt đôi whole-stage codegen** (mất dấu `*` quanh node — lesson 13).

### Vì sao Catalyst không thể tối ưu xuyên UDF

Catalyst tối ưu bằng cách **đọc hiểu expression tree**: nó biết `col("a") > 5` nghĩa là gì nên dám đẩy filter xuống trước scan Parquet. Còn `my_udf(col("a")) > 5`? Catalyst không biết `my_udf` làm gì — có thể có side effect, có thể trả kết quả khác nhau mỗi lần gọi. Nó buộc phải giữ nguyên vị trí, đọc đủ mọi cột hàm cần, chạy đủ mọi dòng. **Một UDF đặt sai chỗ có thể vô hiệu hóa predicate pushdown của cả query** — filter bằng built-in TRƯỚC rồi mới UDF trên phần còn lại.

---

## 5. API

### `F.udf` / `@udf` — chỉ khi đường cùng

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def mask_phone(phone):
    if phone is None:              # LUÔN tự xử lý None — Spark đưa NULL vào thẳng hàm bạn
        return None
    return phone[:3] + "****" + phone[-3:]

df.withColumn("phone_masked", mask_phone("phone"))
```
- **Pitfall 1**: quên xử lý `None` → `TypeError` nằm sâu trong stacktrace Python worker, khó truy.
- **Pitfall 2**: khai sai returnType → Spark **không kiểm tra**, giá trị lệch kiểu thành NULL âm thầm.
- **Pitfall 3**: UDF có thể bị gọi **nhiều lần hơn số dòng** (plan re-evaluate) → hàm phải thuần (không side effect, không counter).

### `F.pandas_udf` — chuẩn Spark 3.4 (type hint style)

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")                      # returnType bằng chuỗi DDL
def haversine_km(lat: pd.Series, lon: pd.Series) -> pd.Series:
    import numpy as np                     # import trong hàm: chạy ở worker
    R = 6371.0
    HN_LAT, HN_LON = np.radians(21.0278), np.radians(105.8342)
    la, lo = np.radians(lat), np.radians(lon)
    a = np.sin((la-HN_LAT)/2)**2 + np.cos(la)*np.cos(HN_LAT)*np.sin((lo-HN_LON)/2)**2
    return pd.Series(2*R*np.arcsin(np.sqrt(a)))

df.withColumn("dist_to_hanoi_km", haversine_km("lat", "lon"))
```
- **Pitfall 1**: viết Python loop `for x in series:` bên trong pandas UDF → mất sạch lợi ích vectorized, chỉ còn lợi Arrow. Phải dùng phép toán pandas/numpy trên cả Series.
- **Pitfall 2**: NULL đến dưới dạng `NaN`/`pd.NA` tùy kiểu — hành vi khác `None` của UDF thường, test kỹ với dữ liệu có NULL.
- **Pitfall 3**: kiểu nested phức tạp (map lồng sâu) có thể không được Arrow hỗ trợ đầy đủ → lỗi khó hiểu; kiểm tra sớm với mẫu nhỏ.

### `F.expr` — built-in đội lốt SQL, đừng quên nó

```python
# Higher-order function trên array — nhiều người tưởng phải UDF:
df.withColumn("total", F.expr("aggregate(items, 0D, (acc, x) -> acc + x.price * x.qty)"))
df.withColumn("big_items", F.expr("filter(items, x -> x.price > 100)"))
```
- **Ý nghĩa**: `transform/filter/aggregate/exists` trên array chạy **trong JVM**, Catalyst hiểu — đây là kẻ giết UDF thầm lặng, ưu tiên trước pandas_udf.

### `mapInPandas` / `applyInPandas`

```python
def enrich(batches):                      # iterator[pd.DataFrame] -> iterator[pd.DataFrame]
    model = load_model()                  # init 1 LẦN cho cả partition — pattern quan trọng
    for pdf in batches:
        pdf["score"] = model.predict(pdf[["amount", "qty"]])
        yield pdf

df.mapInPandas(enrich, schema="order_id string, amount double, qty int, score double")

def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["amount_z"] = (pdf["amount"] - pdf["amount"].mean()) / pdf["amount"].std()
    return pdf

df.groupBy("seller_id").applyInPandas(zscore, schema=df.schema.add("amount_z", "double"))
```
- **Pitfall**: `applyInPandas` tải **cả nhóm** vào RAM một Python worker — key skew (1 seller có 10M đơn) là án OOM treo sẵn. Kiểm tra phân bố kích thước nhóm trước khi dùng.

---

## 6. Demo nhỏ

```
Input:  10 triệu số
   ↓    cùng logic x*2+1 viết 3 cách
Output: 3 con số thời gian — tự thấy, tự nhớ
```

```python
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType
import pandas as pd

spark = SparkSession.builder.appName("demo12").master("local[2]").getOrCreate()
df = spark.range(10_000_000).withColumnRenamed("id", "x")

def bench(name, col_expr):
    t = time.time()
    df.withColumn("y", col_expr).agg(F.sum("y")).collect()   # sum để ép chạy hết mọi dòng
    print(f"{name:12s}: {time.time()-t:6.2f}s")

@F.udf(LongType())
def slow_udf(x):
    return x * 2 + 1

@F.pandas_udf("long")
def fast_pudf(x: pd.Series) -> pd.Series:
    return x * 2 + 1

bench("built-in",   F.col("x") * 2 + 1)
bench("pandas_udf", fast_pudf("x"))
bench("python_udf", slow_udf("x"))
# Kết quả điển hình local[2]:  built-in ~1-2s | pandas_udf ~4-8s | python_udf ~40-90s
spark.stop()
```

Chạy xong mở lại `df.withColumn("y", slow_udf("x")).explain()` — tìm chữ `BatchEvalPython`. Đổi sang pandas UDF — thành `ArrowEvalPython`. Built-in — chẳng có Python gì cả.

---

## 7. Production Example

Tình huống thật tại một công ty e-commerce (mô phỏng chính pipeline Olist của bạn):

**Bài toán**: chuẩn hóa và masking dữ liệu khách hàng ở tầng silver — validate zip code, masking city cho môi trường dev, tính khoảng cách seller→customer để ước lượng phí ship.

**Phiên bản Junior viết** (chạy đúng, chậm chết):

```python
@udf(StringType())
def clean_city(c): return c.strip().title() if c else None      # ← built-in làm được!

@udf(DoubleType())
def distance(lat1, lon1, lat2, lon2): ...                        # ← pandas_udf làm được!
```

**Phiên bản sau code review Senior**:

```
clean_city  → F.initcap(F.trim("city"))               built-in, ở lại JVM
zip check   → F.col("zip").rlike("^[0-9]{5}$")        built-in
distance    → pandas_udf haversine (numpy vectorized)  bắt buộc rời JVM nhưng đi Arrow
scoring ML  → mapInPandas, model load 1 lần/partition
```

Kết quả đo trên bảng 40M dòng, cluster 8 executor: **52 phút → 9 phút**, không đổi một dòng logic nghiệp vụ. Đây là kiểu win Senior mang lại — không phải viết thêm, mà **xóa UDF đi**. Quy trình review của team từ đó có rule: mọi PR chứa `@udf` phải kèm một dòng giải thích "tại sao built-in không làm được".

---

## 8. Hands-on Lab

**Mục tiêu**: benchmark 3 cách trên dữ liệu Olist thật, đọc plan, và viết một pandas UDF có ý nghĩa.

### Bước 1 — `labs/lab12/benchmark_udf.py`

```python
import time
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("lab12-benchmark").getOrCreate()

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)
# Nhân bản dữ liệu cho đủ nặng (~112k dòng × 64 ≈ 7.2M dòng)
big = items.select("order_id", "price", "freight_value")
for _ in range(6):
    big = big.union(big)
big = big.cache(); big.count()   # cache để 3 lần đo công bằng, không đọc lại CSV

# Logic: tổng chi phí = price + freight, phụ thu 10% nếu freight > 20
def bench(name, df_out):
    t = time.time()
    df_out.agg(F.sum("total")).collect()
    print(f"{name:12s}: {time.time()-t:6.2f}s")

expr_builtin = F.col("price") + F.when(F.col("freight_value") > 20,
                    F.col("freight_value") * 1.1).otherwise(F.col("freight_value"))

@F.udf(DoubleType())
def total_udf(price, freight):
    if price is None or freight is None: return None
    return price + (freight * 1.1 if freight > 20 else freight)

@F.pandas_udf("double")
def total_pudf(price: pd.Series, freight: pd.Series) -> pd.Series:
    return price + freight.where(freight <= 20, freight * 1.1)

bench("built-in",   big.withColumn("total", expr_builtin))
bench("pandas_udf", big.withColumn("total", total_pudf("price", "freight_value")))
bench("python_udf", big.withColumn("total", total_udf("price", "freight_value")))

# So plan:
big.withColumn("total", expr_builtin).explain()
big.withColumn("total", total_udf("price", "freight_value")).explain()
input(">>> Mở http://localhost:4040 tab SQL, so 3 query. Enter để thoát.")
spark.stop()
```

```bash
make run-local F=labs/lab12/benchmark_udf.py
```

### Bước 2 — chứng minh UDF chặn predicate pushdown: `labs/lab12/pushdown_killer.py`

```python
# Ghi Parquet, rồi so 2 query:
# (1) filter bằng built-in trên cột gốc  → PushedFilters có điều kiện
# (2) filter bằng UDF                    → PushedFilters trống rỗng
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.appName("lab12-pushdown").getOrCreate()
items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)
items.write.mode("overwrite").parquet("/workspace/labs/lab12/out/items_parquet")
pq = spark.read.parquet("/workspace/labs/lab12/out/items_parquet")

@F.udf(BooleanType())
def is_expensive(p): return p is not None and p > 100

pq.filter(F.col("price") > 100).explain()          # tìm dòng PushedFilters: [GreaterThan(price,100.0)]
pq.filter(is_expensive("price")).explain()          # PushedFilters: []  ← Spark đọc HẾT rồi mới lọc
spark.stop()
```

### Bước 3 — applyInPandas thực dụng: `labs/lab12/apply_in_pandas.py`

Tính z-score giá của từng sản phẩm **trong từng category** (chuẩn hóa nội nhóm — thứ `groupBy().agg()` không làm gọn được vì cần trả lại từng dòng):

```python
import pandas as pd
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab12-apply").getOrCreate()
items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv", header=True, inferSchema=True)
products = spark.read.csv("/workspace/data/olist/olist_products_dataset.csv", header=True, inferSchema=True)

df = items.join(products.select("product_id", "product_category_name"), "product_id") \
          .select("product_id", "product_category_name", "price").na.drop()

def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
    std = pdf["price"].std()
    pdf["price_z"] = 0.0 if (std is None or std == 0) else (pdf["price"] - pdf["price"].mean()) / std
    return pdf

out = df.groupBy("product_category_name").applyInPandas(
    zscore, schema="product_id string, product_category_name string, price double, price_z double")
out.orderBy(F.desc("price_z")).show(10)   # sản phẩm đắt bất thường so với category của nó
spark.stop()
```

### Bước 4 — quan sát

Ghi vào `labs/lab12/NOTES.md`: 3 con số benchmark; dòng `BatchEvalPython`/`ArrowEvalPython` trong plan; dòng `PushedFilters` của 2 query bước 2. (Câu hỏi thêm: câu z-score trên có làm được bằng window function của lesson 10 không? Có — `(price - avg over w) / stddev over w`. Ở production, bản window thắng. Lab dùng applyInPandas để bạn học API, và để nhớ rằng nó thường có đối thủ built-in.)

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Kể đủ 4 lý do Python UDF chậm (serialization đâu, Catalyst mất gì, memory sao).
2. pandas UDF sửa được lý do nào trong 4 lý do đó, KHÔNG sửa được lý do nào?
3. Viết thứ tự ưu tiên 4 bậc và cho ví dụ mỗi bậc.

**Medium** — Ba đoạn logic sau, quyết định bậc thấp nhất đủ dùng (kèm code): (a) chuẩn hóa `customer_city` thành Title Case bỏ khoảng trắng thừa; (b) tính tổng `price * 1.1` cho các phần tử array `items` (từ lab 11); (c) validate `customer_zip_code_prefix` bằng thư viện regex đặc thù chỉ có trên PyPI. Chạy và dán plan chứng minh (a), (b) không có `EvalPython` nào.

**Hard** — Profile chứng minh overhead: dùng benchmark bước 1 nhưng thay logic bằng hàm CỰC NHẸ (`x + 1`) rồi hàm NẶNG (vòng lặp tính toán ~1ms/dòng — ví dụ hash 1000 lần). Đo lại tỷ lệ built-in : pandas : udf ở cả hai trường hợp. Giải thích: tại sao logic càng nhẹ thì UDF thường càng thảm so với built-in, còn logic nặng thì khoảng cách co lại? Kết luận về bản chất chi phí.

**Production Challenge** — Grep repo `../kafka-flink` tìm mọi `@udf`/`udf(`/`pandas_udf` trong code Spark hiện có. Với mỗi cái tìm được: xếp loại (thay được bằng built-in? nâng được lên pandas_udf? bắt buộc giữ?). Nếu không có UDF nào — viết 5 dòng giải thích vì sao đó lại là dấu hiệu TỐT của codebase.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Built-in / F.expr | Nhanh nhất | Ở lại JVM, Tungsten codegen, Catalyst tối ưu hết cỡ. |
| Higher-order function trên array | Nhanh | Vẫn là built-in — nhiều người không biết nên viết UDF oan. |
| pandas UDF logic vectorized | Khá | Arrow batch + numpy C loop; vẫn mất codegen và tốn RAM Python. |
| pandas UDF nhưng viết `for` bên trong | Chậm | Tự tay vứt đi phần vectorized, chỉ còn lợi Arrow. |
| Python UDF thường | Chậm 10–100× | Pickle per-row + 2 chiều socket + Python loop + chặn optimization. |
| UDF đặt TRƯỚC filter built-in | Thảm họa | UDF chạy trên dòng lẽ ra bị lọc bỏ; filter không pushdown được. Lọc trước, UDF sau. |
| applyInPandas nhóm skew | Bẫy OOM | Cả nhóm vào RAM 1 Python worker. |

Câu tự vấn của bài này: *"logic này có thật sự cần rời JVM không — và nếu cần, nó đi container (Arrow) hay đi từng gói (pickle)?"*

---

## 11. Spark UI

Bài này dùng tab **SQL / DataFrame** ở mức sâu hơn:

- Mở query có UDF → tìm node **`BatchEvalPython`** (UDF thường) hoặc **`ArrowEvalPython`** (pandas UDF). Vị trí node trong cây cho biết dữ liệu xuất ngoại Ở ĐÂU — nếu nó nằm dưới (trước) filter, bạn đang trả phí cho dòng sắp vứt đi.
- So sánh 3 query benchmark: query built-in gọn, các node liền mạch trong 1 khối **WholeStageCodegen**; query UDF bị chẻ khúc tại node EvalPython (chi tiết dấu `*` học ở lesson 13).
- Chi tiết node scan Parquet: dòng **PushedFilters** — bằng chứng pushdown sống hay chết (bước 2 của lab).
- Tab **Executors**: khi chạy UDF nặng, để ý memory — phần Python worker không hiện trong "Storage Memory" của JVM, đó là phần chìm của tảng băng.

---

## 12. Common Mistakes

1. **Viết UDF cho việc built-in làm được** — lỗi số 1 tuyệt đối. `strip/title/concat/if-else/regex/date math` đều có built-in. Mở docs `pyspark.sql.functions` TRƯỚC khi mở `@udf`.
2. **Không xử lý None trong UDF** → TypeError chôn trong stacktrace của Python worker trên executor xa xôi, debug mất buổi sáng.
3. **Đặt UDF trước filter** → mất predicate pushdown + chạy UDF trên dữ liệu sắp bị vứt. Luôn filter bằng built-in trước.
4. **Viết vòng lặp Python bên trong pandas UDF** → tưởng đã "vectorized" mà thật ra chưa. Trong thân hàm chỉ được có phép toán pandas/numpy trên cả Series.
5. **Khai sai returnType** → không lỗi, chỉ NULL âm thầm. Kiểm tra bằng count NULL sau khi thêm cột.
6. **Init tài nguyên nặng (model, connection) ở ngoài hàm hoặc mỗi lần gọi** → hoặc PicklingError, hoặc load model 10 triệu lần. Pattern đúng: init trong hàm theo kiểu once-per-partition (iterator API / mapInPandas).
7. **applyInPandas trên nhóm không kiểm soát kích thước** → chạy đẹp trên dev data, OOM ở production khi gặp key khổng lồ.
8. **Quên rằng UDF có thể bị gọi nhiều lần cho cùng dòng** → UDF có side effect (ghi log đếm, gọi API) cho kết quả không xác định.

---

## 13. Interview

**Junior:**

1. *UDF là gì, khi nào cần?* — Hàm người dùng tự định nghĩa để chạy logic không có trong built-in functions. Chỉ nên dùng khi built-in, SQL expression (kể cả higher-order functions) và pandas UDF đều không đáp ứng — vì UDF là lựa chọn chậm nhất.
2. *Tại sao Python UDF chậm?* — Bốn lý do: (a) dữ liệu phải serialize (pickle) TỪNG DÒNG từ JVM sang process Python và ngược lại; (b) Catalyst coi UDF là black box → mất predicate pushdown, mất whole-stage codegen; (c) Python interpreter loop chậm; (d) Python worker ăn memory ngoài JVM. Trả lời chỉ "vì Python chậm" là thiếu 3/4.
3. *pandas UDF khác UDF thường chỗ nào?* — Vận chuyển bằng Apache Arrow theo batch columnar (mặc định 10k dòng/batch) thay vì pickle từng dòng, và hàm nhận/trả pd.Series nên tính toán vectorized bằng C. Nhanh hơn UDF thường 3–100× nhưng vẫn là black box với Catalyst.
4. *Thứ tự ưu tiên khi cần một phép biến đổi?* — built-in > SQL expr (F.expr, higher-order functions) > pandas_udf > udf thường. Càng xuống thấp càng rời xa JVM và Catalyst.

**Mid:**

5. *Arrow là gì và vai trò trong pandas UDF?* — Định dạng columnar in-memory chuẩn, cả JVM lẫn Python đọc/ghi trực tiếp gần zero-copy. Nó thay pickle per-row bằng chuyển batch nguyên khối → chi phí vượt biên JVM↔Python giảm hàng chục lần. Cũng dùng cho `toPandas()` khi bật `spark.sql.execution.arrow.pyspark.enabled`.
6. *UDF ảnh hưởng thế nào đến predicate pushdown? Cách giảm thiểu?* — Filter dựa trên kết quả UDF không đẩy xuống được data source (Catalyst không hiểu ruột UDF) → scan toàn bộ. Giảm thiểu: tách điều kiện — phần lọc được bằng built-in đặt trước để pushdown, UDF chỉ chạy trên tập đã thu nhỏ.
7. *Nhìn explain() làm sao biết có UDF và loại nào?* — Node `BatchEvalPython` = UDF thường, `ArrowEvalPython` = pandas UDF. Chúng cũng cắt đứt WholeStageCodegen tại vị trí đó. Kèm dấu hiệu PushedFilters rỗng nếu filter phụ thuộc UDF.
8. *applyInPandas dùng khi nào, rủi ro gì?* — Logic per-group cần trả về từng dòng hoặc cần thư viện pandas (train model mỗi nhóm, resample time series). Rủi ro: gây shuffle, và cả nhóm phải vừa RAM một Python worker → key skew là án OOM; nhiều trường hợp thay được bằng window function rẻ hơn.

**Senior:**

9. *Job PySpark bị YARN/K8s kill vì vượt memory limit dù heap JVM còn dư — nghi ngờ gì, xử lý sao?* — Nghi phạm hàng đầu: Python worker (UDF/pandas UDF/toPandas) ăn memory NGOÀI heap — container bị tính tổng JVM heap + overhead + Python. Xử lý: tăng `spark.executor.memoryOverhead` hoặc set `spark.executor.pyspark.memory`; giảm `arrow.maxRecordsPerBatch`; rà applyInPandas nhóm to; về dài hạn loại UDF không cần thiết để Python worker khỏi phải ôm dữ liệu.
10. *Team bạn có 30 UDF trong codebase, pipeline chậm. Chiến lược refactor?* — (a) Đo trước: rank UDF theo thời gian (Spark UI, thời lượng stage chứa EvalPython); (b) phân loại: thay được built-in/higher-order (thường 60-80%), nâng cấp pandas_udf (logic vectorize được), giữ lại (hiếm); (c) refactor từ UDF đắt nhất, mỗi PR kèm benchmark before/after; (d) chặn tái phát: quy ước review "PR có @udf phải giải trình", lint rule nếu được. Điểm ăn tiền: nói được rằng thứ tự làm dựa trên ĐO ĐẠC chứ không refactor dàn hàng ngang.

---

## 14. Summary

### Mindmap

```
                       UDF vs BUILT-IN vs PANDAS UDF
                                  │
    ┌──────────────────┬──────────┴─────────┬─────────────────────┐
    ▼                  ▼                    ▼                     ▼
 VÌ SAO UDF CHẬM    PANDAS UDF          API LỚN HƠN           QUY TẮC
    │                  │                    │                     │
 pickle TỪNG ROW    Arrow: batch        mapInPandas:          built-in
 JVM ↔ Python       columnar 10k        iterator pdf,         > F.expr (HOF!)
 qua socket         gần zero-copy       đổi schema được       > pandas_udf
 Catalyst mù:       pd.Series vào,      applyInPandas:        > udf
 mất pushdown,      numpy C loop        CẢ NHÓM vào RAM      ─────────────
 mất codegen        vẫn black box       → skew = OOM         filter built-in
 (BatchEvalPython)  (ArrowEvalPython)                         TRƯỚC UDF
 RAM ngoài JVM                                                None phải tự xử
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại diagram JVM ↔ Python worker và chỉ đúng chỗ chi phí per-row nằm ở đâu.
- [ ] Kể đủ 4 lý do UDF chậm, và nói được pandas UDF sửa 2 cái nào.
- [ ] Đã chạy benchmark, có 3 con số của riêng mình.
- [ ] Nhận diện được BatchEvalPython / ArrowEvalPython / PushedFilters rỗng trong explain().
- [ ] Đọc thuộc thứ tự: built-in > SQL expr > pandas_udf > udf, kể được ví dụ higher-order function.
- [ ] Biết vì sao applyInPandas + key skew = OOM.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 13 — Catalyst Optimizer: logical/physical plan, explain().**

Suốt hai bài nay ta nhắc đi nhắc lại "Catalyst mù với UDF", "Catalyst tối ưu built-in" — nhưng Catalyst thực sự LÀM GÌ với query của bạn? Lesson 13 mở nắp hộp đen: query đi qua 4 giai đoạn Parsed → Analyzed → Optimized → Physical Plan ra sao, predicate pushdown và column pruning được áp vào lúc nào, dấu `*` trong plan nghĩa là gì, và cách đọc `explain(mode="formatted")` như đọc một bản khai sức khỏe của job. Từ bài này trở đi, "chậm" không còn là cảm giác — nó là thứ bạn nhìn thấy được trong plan trước cả khi bấm chạy.

> Gõ **"Continue"** khi sẵn sàng.
