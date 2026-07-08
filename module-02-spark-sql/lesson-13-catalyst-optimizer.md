# Lesson 13 — Catalyst Optimizer: logical/physical plan, explain()

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 7 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Catalyst Optimizer** — bộ não tối ưu của Spark SQL: query của bạn đi qua **4 giai đoạn** Parsed → Analyzed → Optimized Logical → Physical Plan như thế nào.
- **Rule-based optimization**: predicate pushdown, column pruning, constant folding, combine filters — từng rule làm gì, nhìn thấy nó ở đâu trong plan.
- **Cost-based optimization (CBO)**: statistics, `ANALYZE TABLE`, khi nào Spark chọn plan theo chi phí.
- Đọc **`explain(mode="formatted")`** từng dòng — kỹ năng chẩn đoán quan trọng nhất của DE làm Spark.
- **Whole-stage codegen** và dấu `*` trong plan; **Tungsten** là gì.
- Khi Catalyst **bó tay**: UDF black box, và cách can thiệp bằng **hint**.

Sau bài này bạn phải làm được:

- Cầm một query bất kỳ, chạy explain, chỉ ra: join strategy nào được chọn, filter có được đẩy xuống scan không, cột nào bị đọc thừa.
- Giải thích tại sao "viết filter ở đâu trong code không quan trọng (thường thì) — Catalyst tự dời", và chỉ ra ngoại lệ.
- Dùng hint đúng lúc, và quan trọng hơn: biết khi nào KHÔNG cần hint.

Kiến thức dùng trong thực tế: **mỗi lần job chậm**. Người mở Spark UI xem plan trước, rồi mới sửa code — đó là Senior. Người sửa code mò rồi chạy thử — đó là junior may rủi. Bài này cũng là nền của lesson 20 (AQE — Catalyst phiên bản "tối ưu giữa trận").

---

## 2. Why

### Vấn đề: code bạn viết không phải code Spark chạy

Hai đoạn code này, đoạn nào nhanh hơn?

```python
# Cách A: filter trước, join sau
orders.filter(F.col("order_status") == "delivered").join(items, "order_id")

# Cách B: join trước, filter sau
orders.join(items, "order_id").filter(F.col("order_status") == "delivered")
```

Trả lời: **như nhau** — vì Spark không chạy code của bạn theo nghĩa đen. Nhờ lazy evaluation (lesson 2), đến khi action xảy ra Spark mới cầm TOÀN BỘ chuỗi transformation, đưa qua Catalyst, và Catalyst tự dời filter xuống trước join (rule PushDownPredicate). Bạn viết B, Spark chạy A.

Đây là món quà của declarative API: bạn khai **CÁI GÌ**, engine quyết **LÀM THẾ NÀO**. Cùng triết lý với query optimizer của PostgreSQL/Oracle — Catalyst chính là query optimizer của Spark, chỉ khác là nó tối ưu cho thế giới phân tán.

### Nếu không có Catalyst thì sao?

- Mỗi dev phải tự nhớ "filter trước join", "chỉ select cột cần", "hằng số tính trước"... — và người quên sẽ viết query chậm 100×.
- RDD API chính là thế giới không Catalyst: `rdd.map(lambda ...)` là black box y như UDF, Spark chạy đúng thứ tự bạn viết, không tối ưu gì. Đó là lý do lớn nhất DataFrame thay RDD làm API mặc định.

### Nhưng tại sao vẫn phải HỌC nó, nếu nó tự động?

Vì ba lẽ:

1. **Catalyst có điểm mù** (UDF, một số kiểu data source, thống kê sai) — bạn phải nhận ra khi nào nó bất lực để tự cứu.
2. **Đọc plan = chẩn đoán bệnh**: job chậm, câu hỏi đầu tiên là "plan trông thế nào" — SortMergeJoin hay BroadcastJoin? PushedFilters có gì? Đọc không được plan thì tuning là bói toán.
3. **Phỏng vấn**: "giải thích Catalyst" là câu sàng lọc mid/senior kinh điển.

### Trade-off

| Được | Mất |
|---|---|
| Dev viết tự nhiên, engine tự tối ưu | Một tầng trừu tượng cần học để debug |
| Plan tốt hơn phần lớn dev tự viết | Đôi khi optimizer đoán sai (thống kê cũ, skew) → cần hint/AQE |
| Nền tảng chung: SQL và DataFrame API ra CÙNG plan | Mọi thứ ngoài expression system (UDF) bị bỏ rơi |

---

## 3. Theory

### 3.1. Pipeline 4 giai đoạn — bức tranh phải thuộc lòng

```
   SQL string          DataFrame API
       │                    │
       └─────────┬──────────┘
                 ▼
 ┌─────────────────────────────┐
 │ ① PARSED (UNRESOLVED)       │  "Ngữ pháp đúng chưa?"
 │    LOGICAL PLAN             │  Cây cú pháp. Chưa biết bảng `orders`
 │                             │  có thật không, cột `price` kiểu gì.
 └──────────────┬──────────────┘
                ▼   + Catalog (danh bạ: bảng nào có thật, schema gì)
 ┌─────────────────────────────┐
 │ ② ANALYZED LOGICAL PLAN     │  "Tên nào cũng có chủ chưa?"
 │                             │  Resolve tên bảng/cột về schema thật,
 │                             │  gắn kiểu dữ liệu, bắt lỗi cột không
 │                             │  tồn tại (AnalysisException ném ở đây)
 └──────────────┬──────────────┘
                ▼   + Rule-based optimization (hàng chục rule chạy lặp
                │     đến khi plan hết thay đổi — "fixed point")
 ┌─────────────────────────────┐
 │ ③ OPTIMIZED LOGICAL PLAN    │  "Cách diễn đạt rẻ nhất là gì?"
 │                             │  Pushdown, pruning, folding, combine...
 │                             │  Vẫn LOGICAL: nói CÁI GÌ, chưa nói CÁCH
 └──────────────┬──────────────┘
                ▼   + Strategies (+ statistics nếu CBO bật)
 ┌─────────────────────────────┐
 │ ④ PHYSICAL PLAN             │  "Thi công bằng thuật toán nào?"
 │   (SPARK PLAN)              │  join → BroadcastHash? SortMerge?
 │                             │  aggregate → HashAggregate?
 │                             │  Sinh ra Exchange (shuffle) tường minh
 └──────────────┬──────────────┘
                ▼   + Whole-stage codegen (Tungsten)
         RDD của InternalRow — chạy trên executor
```

Analogy xây nhà: ① bản phác thảo của khách ("tôi muốn nhà 2 tầng có sân") → ② kiến trúc sư kiểm tra thực địa (đất có thật, giấy tờ hợp lệ) → ③ tối ưu thiết kế (dồn ống nước một trục cho rẻ) → ④ bản vẽ thi công (đổ bê tông mác nào, thợ nào làm) → codegen là đội thợ được huấn luyện riêng cho đúng công trình này.

**Điểm hay bị nhầm**: `AnalysisException: Column 'pricee' does not exist` ném ra **ngay khi khai transformation** (giai đoạn ②, chạy eager trên driver) — không cần đợi action. Còn lỗi dữ liệu (cast fail, chia 0...) thì đến action mới lộ, trên executor.

### 3.2. Rule-based optimization — 4 rule phải kể được trong phỏng vấn

Mỗi rule là một phép biến đổi cây → cây, áp dụng lặp đi lặp lại:

**① Predicate pushdown** — đẩy filter xuống sát nguồn dữ liệu nhất có thể:

```
TRƯỚC:  Filter(status='delivered')          SAU:   Join
           └─ Join                                   ├─ Filter(status='delivered')
                ├─ Scan orders                       │    └─ Scan orders  ← Parquet chỉ
                └─ Scan items                        └─ Scan items          đọc row group
                                                                            thỏa điều kiện
```
Với Parquet/JDBC/Iceberg, filter được đẩy tiếp VÀO data source (`PushedFilters` trong plan) → bớt I/O ngay từ lúc đọc, không chỉ bớt xử lý.

**② Column pruning** — chỉ đọc cột được dùng. Bạn `select("order_id", "price")` ở cuối query? Scan Parquet chỉ đọc 2 cột đó (ReadSchema trong plan). Đây là lý do #1 Parquet + Spark là cặp trời sinh.

**③ Constant folding** — tính trước biểu thức toàn hằng số: `F.lit(60) * 60 * 24` thành `86400` NGAY trong plan, không tính lại 10 triệu lần trên executor.

**④ Combine filters / collapse projects** — `filter(a).filter(b)` gộp thành `filter(a AND b)`; chuỗi `withColumn` liên tiếp gộp thành 1 Project. Vì thế cứ viết code cho DỄ ĐỌC — chia nhỏ filter thoải mái, Catalyst gộp giùm.

Rule khác hay gặp trong plan: `NullPropagation` (biểu thức chứa NULL literal → NULL luôn), `PushDownPredicate` qua join tùy loại join (inner đẩy được 2 phía; left join chỉ đẩy điều kiện bên phải xuống bên phải một cách hạn chế — liên quan NULL semantics lesson 14).

### 3.3. Cost-based optimization (CBO) — khi rule không đủ

Rule-based là "luật cứng" đúng trong mọi trường hợp. Nhưng vài quyết định cần biết **dữ liệu to nhỏ ra sao**: bảng này 10 MB hay 10 TB quyết định broadcast hay sort-merge; join 3 bảng thì thứ tự nào rẻ nhất?

- Spark lấy statistics từ: kích thước file (luôn có, thô), hoặc thống kê chi tiết sau khi bạn chạy:

```sql
ANALYZE TABLE orders COMPUTE STATISTICS;                        -- row count, size
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS order_id;   -- distinct, min/max, null count
```

- Bật `spark.sql.cbo.enabled=true` (mặc định **tắt**) → optimizer dùng thống kê để reorder join, ước lượng selectivity.
- Thực tế 2026: CBO tĩnh ít được dùng trực tiếp; vai chính thuộc về **AQE** (Adaptive Query Execution, mặc định bật từ Spark 3.2) — thay vì tin thống kê TRƯỚC khi chạy, AQE đo kích thước THẬT sau mỗi stage rồi replan: tự đổi sort-merge → broadcast, tự gộp partition nhỏ, tự xẻ partition skew. Lesson 20 dành riêng cho nó — hôm nay chỉ cần biết nó là "CBO phiên bản dùng số đo thật".

### 3.4. Tungsten & whole-stage codegen — tầng cơ bắp

**Tungsten** = dự án tối ưu execution engine (Spark 1.4+), 3 trụ:
1. **Binary format (UnsafeRow)**: dữ liệu lưu dạng binary tự quản lý, thoát khỏi object Java (một string Java tốn ~40 byte overhead; UnsafeRow gần như chỉ tốn payload) → ít GC, cache-friendly.
2. **Off-heap memory management**: Spark tự quản memory ngoài heap, né GC.
3. **Whole-stage codegen**: thay vì mỗi operator là một lớp gọi hàm lồng nhau (volcano model — mỗi dòng đi qua N lời gọi virtual function), Spark **sinh mã Java tại runtime** gộp cả chuỗi operator thành MỘT vòng lặp, compile bằng Janino rồi chạy.

```
Volcano model:                     Whole-stage codegen:
mỗi row: Scan.next()               // mã Java sinh tự động, 1 vòng lặp:
  → Filter.next()                  while (scan.hasNext()) {
    → Project.next()                 row = scan.next();
      → Agg.consume()                if (row.status == "delivered") {   // filter
(N virtual calls / row)                sum += row.price;                 // agg
                                   } }   // như code tay — nhanh gấp nhiều lần
```

Trong `explain()`, operator nằm trong codegen có **dấu `*` kèm số id**: `*(2) HashAggregate(...)` nghĩa là operator này thuộc khối codegen số 2. **Ranh giới khối = ranh giới không codegen được**: Exchange (shuffle), và... `BatchEvalPython` — thêm một tội của UDF (lesson 12).

### 3.5. Khi Catalyst bó tay & hint

Catalyst bất lực khi:
- **UDF/RDD lambda**: black box — không pushdown xuyên qua, không codegen.
- **Thống kê sai/thiếu**: bảng sau filter còn 1 MB nhưng Spark chỉ biết size file gốc 10 GB → không dám broadcast (AQE cứu được phần này).
- **Skew**: plan không nhìn thấy phân bố key (AQE cứu một phần).
- **Cast ngầm giết pushdown**: filter trên cột string so sánh với số → cast cả cột, pushdown yếu đi.

Công cụ can thiệp — **hint** (gợi ý, không phải mệnh lệnh tuyệt đối):

```python
from pyspark.sql.functions import broadcast
big.join(broadcast(small), "key")                 # ép broadcast join (lesson 9)
df.hint("merge")        # gợi ý sort-merge
df.hint("shuffle_hash") # gợi ý shuffle hash join
# SQL: SELECT /*+ BROADCAST(s) */ ... ;  /*+ REPARTITION(200, col) */ ...
```

Kỷ luật Senior về hint: **hint là thuốc kê đơn, không phải vitamin**. Chỉ hint khi (a) đã đọc plan và chỉ ra được quyết định sai của optimizer, (b) đã thử để AQE tự xử. Hint bừa hôm nay = bug performance ngày mai khi dữ liệu đổi cỡ (broadcast bảng đã phình lên 5 GB → OOM).

---

## 4. Internal

Đường đi chi tiết từ `df.filter(...).join(...).agg(...).write...` đến task:

```
① Mỗi transformation chỉ đắp thêm node vào cây LogicalPlan (trong JVM driver,
   qua Py4J). KHÔNG có dữ liệu nào được đụng đến.
② Riêng phần resolve (Analyzer) chạy ngay khi khai — nên sai tên cột là biết liền.
③ ACTION → QueryExecution kích hoạt:
     analyzed → withCachedData (thay subtree bằng cache nếu bạn đã cache — lesson 18)
              → optimizedPlan (RuleExecutor chạy các batch rule đến fixed point,
                 mỗi batch có max iterations để tránh lặp vô hạn)
              → sparkPlan (SparkStrategies: pattern-match từng node logical
                 → chọn node physical; ví dụ Join + size nhỏ hơn
                 spark.sql.autoBroadcastJoinThreshold (mặc định 10MB) → BroadcastHashJoin)
              → executedPlan (chèn Exchange đúng chỗ cần phân phối lại dữ liệu,
                 chèn WholeStageCodegen, chèn AQE wrapper nếu bật)
④ executedPlan.execute() → RDD[InternalRow] → DAG Scheduler cắt stage tại
   các Exchange → Task Scheduler phát task (đúng bộ máy lesson 1-3)
```

Hai chi tiết đáng tiền:

- **Exchange = shuffle = ranh giới stage.** Đếm số `Exchange` trong physical plan = đếm số lần shuffle = ước được số stage. Đây là cây cầu nối lesson 3 (stage) với lesson 13 (plan): trước đây bạn đếm stage trên UI SAU khi chạy, giờ bạn đếm Exchange trong plan TRƯỚC khi chạy.
- **AQE làm plan "động"**: khi AQE bật, explain trước khi chạy hiện `AdaptiveSparkPlan isFinalPlan=false` — plan chỉ là dự kiến; xem plan THẬT phải vào tab SQL của UI sau khi chạy (mục "AQE plan" / final plan). Đừng hoảng khi plan lúc chạy khác plan lúc explain — đó là AQE làm đúng việc.

---

## 5. API

### `df.explain(mode=...)` — công cụ số 1 của bài

```python
df.explain()                    # = mode="simple": chỉ physical plan
df.explain(mode="extended")     # cả 4 giai đoạn: parsed/analyzed/optimized/physical
df.explain(mode="formatted")    # physical plan dạng cây gọn + chú thích từng node — DỄ ĐỌC NHẤT
df.explain(mode="cost")         # kèm statistics ước lượng (sizeInBytes, rowCount nếu có)
df.explain(mode="codegen")      # xem cả mã Java được sinh — để thỏa trí tò mò
```
- **Pitfall**: `explain()` in plan chứ KHÔNG chạy query — nhưng với AQE, plan in ra chưa phải final. Đối chiếu UI sau khi chạy thật.

### Đọc `mode="formatted"` — giải phẫu mẫu

```
== Physical Plan ==
AdaptiveSparkPlan (7)
+- HashAggregate (6)              ← final aggregate (sau shuffle)
   +- Exchange (5)                ← SHUFFLE! ranh giới stage
      +- HashAggregate (4)        ← partial aggregate (trước shuffle — lesson 8)
         +- Project (3)           ← chỉ giữ cột cần (column pruning đã áp)
            +- Filter (2)         ← filter sau khi đẩy xuống
               +- Scan parquet (1)

(1) Scan parquet
    Output [3]: [order_id, order_status, price]     ← ReadSchema: đọc 3/50 cột — pruning sống
    PushedFilters: [IsNotNull(order_status),
                    EqualTo(order_status,delivered)] ← pushdown VÀO data source — sống
    ...
```

Checklist đọc plan (dán lên màn hình): **① Scan**: ReadSchema có gọn không, PushedFilters có điều kiện không? **② Exchange**: mấy cái, có cái nào thừa không? **③ Join node**: BroadcastHashJoin / SortMergeJoin / ShuffleHashJoin — đúng kỳ vọng chưa? **④ Có `BatchEvalPython` lạc vào không?** **⑤ Dấu `*`/WholeStageCodegen có phủ các khối nặng không?**

### `ANALYZE TABLE` (SQL)

```python
spark.sql("ANALYZE TABLE local.db.orders COMPUTE STATISTICS FOR ALL COLUMNS")
```
- **Ý nghĩa**: quét bảng, ghi thống kê vào catalog cho CBO/estimate.
- **Pitfall**: thống kê là ảnh chụp — bảng thay đổi mà không ANALYZE lại thì thống kê nói dối, optimizer quyết sai còn tệ hơn không có. Production: đưa vào lịch maintenance (Project 1 checkpoint 4).

### `broadcast()` / `df.hint(name, *params)`

```python
from pyspark.sql.functions import broadcast
result = orders.join(broadcast(dim_sellers), "seller_id")
```
- **Pitfall**: broadcast bảng to → nghẹn driver + OOM executor. Trước khi hint, hỏi: "tôi có chắc bảng này MÃI MÃI nhỏ không?"

### Config liên quan (đọc-hiểu, đừng vội chỉnh)

| Config | Mặc định | Vai trò |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Dưới ngưỡng → tự broadcast. `-1` để tắt. |
| `spark.sql.adaptive.enabled` | true (3.2+) | AQE — replan theo số đo runtime. |
| `spark.sql.cbo.enabled` | false | CBO tĩnh dùng ANALYZE stats. |
| `spark.sql.codegen.wholeStage` | true | Tắt chỉ để debug/so sánh. |

---

## 6. Demo nhỏ

```
Input:  bảng nhỏ tự tạo, query có hằng số + 2 filter rời + select hẹp
   ↓    explain(mode="extended")
Output: nhìn tận mắt 3 rule: constant folding, combine filters, column pruning
```

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("demo13").master("local[2]").getOrCreate()

df = spark.createDataFrame(
    [(i, f"s{i%7}", float(i % 100), i % 2) for i in range(1000)],
    ["id", "seller", "price", "flag"])
df.write.mode("overwrite").parquet("/tmp/demo13")     # qua Parquet để thấy pushdown
pq = spark.read.parquet("/tmp/demo13")

q = (pq.filter(F.col("price") > F.lit(10) * 2 + 5)    # hằng số: 10*2+5
       .filter(F.col("flag") == 1)                    # filter thứ 2, viết rời
       .select("seller", "price"))                    # chỉ 2 cột

q.explain(mode="extended")
# Soi phần Optimized Logical Plan, đối chiếu:
#  1. (10*2+5) đã biến thành 25.0                 → constant folding
#  2. hai Filter gộp làm một: (price > 25.0) AND (flag = 1)  → combine filters
#  3. Scan chỉ ReadSchema [seller, price, flag]   → column pruning (id biến mất)
#  4. PushedFilters: [GreaterThan(price,25.0), EqualTo(flag,1)] → predicate pushdown
spark.stop()
```

Bài tập 30 giây: viết lại query với filter đặt SAU select (`select` trước, `filter` sau) — chạy explain — plan **y hệt**. Đó là toàn bộ tinh thần của Catalyst trong một thí nghiệm.

---

## 7. Production Example

Chuyện có thật ở mọi công ty dùng lakehouse (và sẽ là chuyện của bạn với `../kafka-flink`):

**Hiện trường**: dashboard "doanh thu theo seller" đang 40 giây/query. Bảng fact 2 tỷ dòng (Iceberg/Parquet), join dim_sellers 30 nghìn dòng, filter 30 ngày gần nhất.

**Cách junior xử**: tăng executor. Tốn tiền gấp đôi, còn 25 giây. 

**Cách Senior xử — mở plan trước**:

```
1. Nhìn Scan fact:  PushedFilters: []  ← !!! filter ngày đâu?
   → Truy code: ngày được filter bằng UDF  parse_date_udf(col("dt_str")) >= ...
   → UDF black box, pushdown chết. Sửa: to_date built-in + filter trên cột partition.
   → Sau sửa: PushedFilters + partition pruning: chỉ đọc 30/1095 ngày. 40s → 6s.
2. Nhìn Join: SortMergeJoin  ← bảng dim 30k dòng ~ 3MB mà không broadcast?
   → Kiểm tra: autoBroadcastJoinThreshold bị ai đó set -1 từ một sự cố cũ.
   → Trả về mặc định + broadcast hint tường minh cho dim. 6s → 2.5s.
3. Đặt ANALYZE TABLE vào DAG maintenance hàng đêm để thống kê không nói dối.
```

Bài học: **hai lệnh explain tiết kiệm hơn một cụm máy**. Cùng một tài nguyên, query nhanh 16× — và mọi quyết định đều bắt đầu từ việc ĐỌC, không phải ĐOÁN. Đây chính là quy trình bạn sẽ diễn lại trong Project 3 "Cứu pipeline chậm".

---

## 8. Hands-on Lab

**Mục tiêu**: đọc plan của 6 query Olist từ dễ đến khó, bắt tận tay từng optimization.

### Bước 0 — chuẩn bị Parquet (pushdown cần data source columnar)

```python
# labs/lab13/prepare.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("lab13-prep").getOrCreate()
for name in ["orders", "order_items", "sellers", "products"]:
    (spark.read.csv(f"/workspace/data/olist/olist_{name}_dataset.csv",
                    header=True, inferSchema=True)
          .write.mode("overwrite").parquet(f"/workspace/labs/lab13/pq/{name}"))
spark.stop()
```

### Bước 1 — `labs/lab13/read_plans.py`

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.appName("lab13-plans").getOrCreate()
orders  = spark.read.parquet("/workspace/labs/lab13/pq/orders")
items   = spark.read.parquet("/workspace/labs/lab13/pq/order_items")
sellers = spark.read.parquet("/workspace/labs/lab13/pq/sellers")

def show(title, df):
    print(f"\n{'='*70}\nQUERY: {title}\n{'='*70}")
    df.explain(mode="formatted")

# Q1 — pushdown + pruning cơ bản
show("Q1 filter+select", orders.filter(F.col("order_status") == "delivered")
                               .select("order_id", "order_purchase_timestamp"))

# Q2 — combine filters + constant folding
show("Q2 folding", items.filter(F.col("price") > F.lit(50) * 2)
                        .filter(F.col("freight_value") < 100))

# Q3 — filter SAU join có được đẩy xuống trước join không?
show("Q3 filter after join", orders.join(items, "order_id")
                                   .filter(F.col("order_status") == "delivered"))

# Q4 — join strategy: sellers bé → mong đợi BroadcastHashJoin (tự động)
show("Q4 auto broadcast", items.join(sellers, "seller_id"))

# Q5 — tắt auto broadcast rồi ép bằng hint
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
show("Q5a no broadcast", items.join(sellers, "seller_id"))            # → SortMergeJoin
show("Q5b hint",         items.join(broadcast(sellers), "seller_id")) # → Broadcast trở lại
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# Q6 — UDF phá hoại
@F.udf(BooleanType())
def delivered(s): return s == "delivered"
show("Q6 udf filter", orders.filter(delivered("order_status"))
                            .select("order_id"))
spark.stop()
```

```bash
make run-local F=labs/lab13/prepare.py
make run-local F=labs/lab13/read_plans.py
```

### Bước 2 — phiếu ghi kết quả (điền vào `labs/lab13/NOTES.md`)

Cho TỪNG query Q1–Q6: (a) mấy Exchange? (b) PushedFilters chứa gì? (c) ReadSchema mấy cột? (d) join node tên gì? (e) node nào KHÔNG nằm trong WholeStageCodegen (không có `*`)? Q3 phải trả lời được: filter status nằm TRÊN hay DƯỚI join trong plan — và vì sao điều đó chứng minh PushDownPredicate hoạt động. Q6 phải chỉ ra: PushedFilters rỗng + BatchEvalPython + codegen bị cắt.

### Bước 3 — chạy 1 action và đối chiếu UI

Thêm `.count()` cho Q4, mở `http://localhost:4040` tab **SQL** → click query → so cây plan trên UI với explain trong console; để ý plan UI có số liệu THẬT (rows output từng node) và AQE final plan.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Kể tên và mô tả 1 câu cho mỗi giai đoạn trong 4 giai đoạn của Catalyst. Lỗi "column not found" ném ở giai đoạn nào?
2. Predicate pushdown và column pruning khác nhau thế nào? Mỗi cái tiết kiệm gì?
3. Dấu `*` trong physical plan nghĩa là gì? Kể 2 thứ cắt đứt nó.

**Medium** — Không chạy máy, chỉ nhìn code — dự đoán plan của query sau (số Exchange, join strategy, PushedFilters, ReadSchema), GHI RA GIẤY rồi mới chạy explain kiểm chứng:
```python
(orders.join(items, "order_id")
       .filter(F.col("price") > 100)
       .filter(F.col("order_status") == "delivered")
       .groupBy("seller_id").agg(F.sum("price").alias("rev"))
       .select("seller_id", "rev"))
```
Giải thích mỗi chỗ đoán sai — đó chính là lỗ hổng hiểu biết cần vá.

**Hard** — CBO thực nghiệm: tạo bảng managed từ orders (`saveAsTable`), chạy query join + filter và ghi lại `explain(mode="cost")` (sizeInBytes ước lượng). Chạy `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`, bật `spark.sql.cbo.enabled=true`, explain lại. So sánh con số thống kê trước/sau và chỉ ra quyết định nào của plan thay đổi (nếu không đổi — giải thích tại sao với dữ liệu cỡ này điều đó hợp lý).

**Production Challenge** — Chọn query nặng nhất bạn từng viết trong lab 7–12, chạy `explain(mode="formatted")`, viết bản "plan review" 15 dòng như một Senior review PR: 3 điều plan đang làm tốt, 1-2 rủi ro khi dữ liệu tăng 100× (broadcast còn ổn không? shuffle mấy lần?), 1 đề xuất cải tiến.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Hiện tượng trong plan | Chẩn đoán | Thuốc |
|---|---|---|
| `PushedFilters: []` dù query có filter | Filter qua UDF/cast ngầm/biểu thức data source không hiểu | Viết lại bằng built-in trên cột gốc |
| ReadSchema liệt kê 50 cột cho query cần 3 | `select("*")` hoặc cache toàn bảng ở giữa | Select hẹp sớm; kiểm tra chỗ nào "nở" cột |
| SortMergeJoin với bảng dim vài MB | Thiếu thống kê / threshold bị chỉnh / size ước lượng sai | broadcast hint sau khi xác nhận bảng nhỏ bền vững |
| Exchange nhiều hơn kỳ vọng | groupBy/join/distinct/repartition chồng chất, key khác nhau | Gom phép cùng key; xem lesson 15-16 |
| Plan có BatchEvalPython | UDF | Lesson 12 — thay built-in/pandas_udf |
| `AdaptiveSparkPlan isFinalPlan=false` | Chưa chạy nên chưa phải plan cuối | Xem final plan trên UI sau khi chạy |

Nguyên tắc: **plan đọc TRƯỚC khi chạy job đắt tiền** — 30 giây explain rẻ hơn 3 giờ cluster.

---

## 11. Spark UI

Bài này **mở khóa hẳn tab SQL / DataFrame**:

- Mỗi query (action trên DataFrame) là một dòng — click vào thấy **cây physical plan dạng đồ họa**, mỗi node kèm metrics THẬT: `number of output rows`, thời gian, size.
- Đọc từ DƯỚI lên (scan trước): số rows output của Scan so với của Filter cho biết filter loại được bao nhiêu % — filter loại 0.1% mà đặt sau join đắt thì phải xem lại thiết kế.
- Node **Exchange** hiện data size shuffle — con số này nối thẳng sang tab Stages (shuffle read/write) mà bạn quen từ lesson 8-9.
- Khối **WholeStageCodegen** được vẽ thành khung bao quanh nhóm operator — nhìn phát biết codegen phủ đến đâu, đứt ở đâu.
- Với AQE: mục plan có thể hiện các version — plan ban đầu và final plan sau khi AQE điều chỉnh. So hai bản là cách học AQE trực quan nhất (sẽ khai thác kỹ ở lesson 20).

---

## 12. Common Mistakes

1. **Tuning mò không đọc plan** — đổi config, tăng máy, đảo code... trong khi câu trả lời nằm sẵn trong explain. Quy trình đúng: đọc plan → lập giả thuyết → sửa → đọc lại plan → đo.
2. **Tưởng thứ tự viết code = thứ tự thực thi** — sợ không dám tách filter thành nhiều dòng cho dễ đọc, hoặc ngược lại: tin rằng "tôi đã filter sớm" trong khi filter đó là UDF nên chẳng được đẩy đi đâu.
3. **Hint bừa** — copy `broadcast()` từ Stack Overflow cho bảng "hiện đang nhỏ"; 6 tháng sau bảng 5 GB, job chết bí ẩn mỗi thứ Hai. Hint phải kèm comment lý do + điều kiện còn hiệu lực.
4. **Quên ANALYZE sau khi bảng thay đổi lớn** — CBO/estimate dùng thống kê thối → plan tệ hơn cả không có thống kê.
5. **Đọc explain trước khi chạy rồi kết luận, quên AQE** — plan final trên UI mới là sự thật với Spark 3.x.
6. **Kết luận "Catalyst lo hết, tôi khỏi nghĩ"** — Catalyst không cứu được: UDF, skew (một phần), thiết kế grain/partition sai, join thừa. Optimizer tối ưu CÁCH làm, không sửa được VIỆC sai.
7. **So sánh string với số trong filter** (`col("zip") == 12345` khi zip là string) — cast ngầm cả cột, pushdown suy yếu, còn dễ sai logic. Ép kiểu tường minh.

---

## 13. Interview

**Junior:**

1. *Catalyst Optimizer là gì?* — Query optimizer của Spark SQL: nhận logical plan từ DataFrame/SQL, biến đổi qua 4 giai đoạn (parsed → analyzed → optimized logical → physical) bằng rule và cost/statistics, cuối cùng sinh mã chạy trên executor. Nhờ nó code declarative của dev được thực thi theo cách tối ưu hơn cách viết.
2. *Logical plan vs physical plan?* — Logical: mô tả CÁI GÌ cần tính (quan hệ đại số: filter, join, aggregate) — chưa nói cách. Physical: quyết định THUẬT TOÁN cụ thể (BroadcastHashJoin vs SortMergeJoin, HashAggregate), chèn Exchange (shuffle), là thứ thật sự chạy.
3. *Predicate pushdown là gì, lợi ích?* — Rule dời filter xuống sát data source nhất; với Parquet/JDBC/Iceberg còn đẩy VÀO reader (PushedFilters) → bớt I/O từ lúc đọc chứ không chỉ bớt xử lý. Kiểm chứng qua dòng PushedFilters trong explain.
4. *explain() có những mode nào, dùng mode nào để đọc cho người?* — simple (mặc định, chỉ physical), extended (cả 4 giai đoạn), formatted (cây gọn + chú thích node — dễ đọc nhất), cost (kèm statistics), codegen (mã Java sinh ra).

**Mid:**

5. *Rule-based vs cost-based optimization?* — Rule-based: luật biến đổi đúng vô điều kiện (pushdown, pruning, folding), áp lặp đến fixed point, không cần biết dữ liệu. Cost-based: cần statistics (ANALYZE TABLE) để chọn giữa các phương án hợp lệ — join strategy, join order. Spark 3.x chuyển trọng tâm sang AQE: dùng số đo runtime thật thay vì thống kê tĩnh.
6. *Whole-stage codegen là gì, tại sao nhanh hơn?* — Thay volcano model (mỗi row đi qua chuỗi virtual call của từng operator), Spark sinh mã Java runtime gộp cả chuỗi operator thành một vòng lặp duy nhất rồi compile — như code viết tay: ít lời gọi hàm, biến nằm trong register, CPU-friendly. Nhận diện bằng dấu `*(id)` trong plan; bị cắt bởi Exchange và Python UDF.
7. *Những gì Catalyst KHÔNG tối ưu được?* — UDF/RDD lambda (black box → mất pushdown/codegen), quyết định phụ thuộc dữ liệu khi thống kê thiếu/sai (broadcast hay không), data skew ở mức phân bố key, và lỗi thiết kế của người viết (join thừa, grain sai, đọc format không columnar).
8. *AnalysisException 'column not found' được ném khi nào — lúc khai transformation hay lúc action? Giải thích.* — Lúc khai: Analyzer resolve tên cột chạy eager trên driver ngay khi build plan. Ngược lại lỗi DỮ LIỆU (parse/cast/null) chỉ lộ khi action chạy trên executor. Phân biệt được hai thời điểm này chứng tỏ hiểu lazy evaluation + pipeline Catalyst.

**Senior:**

9. *Query join fact 2 tỷ dòng với dim 3 MB nhưng plan ra SortMergeJoin — các nguyên nhân có thể và cách xử từng cái?* — (a) Ước lượng size sai: dim đọc qua format/nguồn không cho size tin cậy, hoặc dim là kết quả subquery phức tạp Spark không ước được → broadcast hint sau khi tự xác nhận size; (b) threshold bị chỉnh (-1 hoặc quá thấp) → soát config trước khi hint; (c) AQE tắt → bật, để nó convert sang broadcast lúc runtime dựa trên size thật; (d) dim tưởng nhỏ mà không nhỏ (nhiều cột to) → đo thật bằng cache + UI Storage. Điểm cộng: nói rõ hint phải kèm điều kiện bảo trì.
10. *Thiết kế quy trình làm việc với plan cho một team DE — bạn đặt chuẩn gì?* — (a) Mọi pipeline mới/PR đổi query lớn phải đính kèm explain(formatted) và 3 dòng nhận xét (Exchange count, join strategy, pushdown status); (b) chuẩn hóa checklist đọc plan (scan → exchange → join → EvalPython → codegen); (c) hint bắt buộc kèm comment lý do + ngày review lại; (d) ANALYZE TABLE nằm trong DAG maintenance định kỳ; (e) benchmark before/after là điều kiện merge cho PR tuning. Câu này đo tư duy đưa kiến thức cá nhân thành kỷ luật đội nhóm — đặc sản của Senior.

---

## 14. Summary

### Mindmap

```
                        CATALYST OPTIMIZER
                               │
   ┌───────────────┬───────────┴────────────┬────────────────────┐
   ▼               ▼                        ▼                    ▼
 4 GIAI ĐOẠN     RULE-BASED              PHYSICAL & TUNGSTEN   GIỚI HẠN & HINT
   │               │                        │                    │
 Parsed (cú pháp) pushdown → PushedFilters Strategies chọn:     UDF = black box
 Analyzed (resolve pruning → ReadSchema    Broadcast/SortMerge  stats sai → CBO/
  ← lỗi tên cột)  folding: 10*2+5→25      Exchange = shuffle    ANALYZE TABLE
 Optimized (rules combine filters          = ranh giới stage    AQE = đo thật
  đến fixed point) → cứ viết code          codegen: dấu *,      rồi replan
 Physical (chọn   DỄ ĐỌC, Catalyst        1 vòng lặp Java,     hint = thuốc
  thuật toán)     gộp giùm                 đứt tại Exchange/UDF  kê đơn, có hạn dùng
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại pipeline 4 giai đoạn và nói được mỗi giai đoạn trả lời câu hỏi gì.
- [ ] Kể + nhận diện trong plan: pushdown, pruning, folding, combine filters.
- [ ] Đọc được explain(formatted): Exchange, join strategy, PushedFilters, ReadSchema, dấu *.
- [ ] Giải thích được vì sao filter viết sau join vẫn chạy trước join — và ngoại lệ UDF.
- [ ] Biết ANALYZE TABLE để làm gì và vì sao thống kê thối nguy hiểm.
- [ ] Dự đoán plan trước khi chạy (assignment Medium) và vá được chỗ đoán sai.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 14 — Null handling & data quality patterns.**

Trong plan hôm nay bạn đã thấy Spark tự chèn `IsNotNull(...)` vào PushedFilters mà bạn không hề viết — vì sao nó phải làm thế? Câu trả lời dẫn vào vùng đất nguy hiểm nhất của SQL: **NULL và logic ba giá trị**. `NULL = NULL` không phải true. `col != 'x'` âm thầm vứt luôn các dòng NULL. Join key chứa NULL không bao giờ khớp. Đây là loại bug không ném exception, không đỏ log — chỉ lặng lẽ làm báo cáo doanh thu thiếu 3% và không ai biết trong 6 tháng. Lesson 14 vũ trang cho bạn: NULL semantics đầy đủ, và bộ pattern data quality để bug loại này bị chặn ở cổng thay vì lên dashboard — hành trang cuối cùng trước khi bước vào Project 1.

> Gõ **"Continue"** khi sẵn sàng.
