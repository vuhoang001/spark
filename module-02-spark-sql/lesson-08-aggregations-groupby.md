# Lesson 8 — Aggregations & groupBy: hash aggregate hoạt động ra sao

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 4 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- `groupBy` trả về cái gì (spoiler: **GroupedData**, chưa phải DataFrame) và `agg` với nhiều hàm cùng lúc.
- Con đường của một aggregation: **partial aggregate (map-side combine) → shuffle → final aggregate** — vì sao Spark đếm 2 lần mà lại NHANH hơn đếm 1 lần.
- Hai chiến binh vật lý: **HashAggregateExec** vs **SortAggregateExec** — khi nào Spark chọn cái nào.
- `countDistinct` vs `approx_count_distinct` (HyperLogLog) — chính xác tuyệt đối đắt cỡ nào, sai số 2% rẻ cỡ nào.
- `pivot`, `rollup`, `cube` — aggregation nhiều chiều.
- **Spill to disk** khi hash table đầy — lần đầu chạm vào chuyện memory của executor.

Sau bài này bạn phải làm được:

- Vẽ từ trí nhớ sơ đồ partial → shuffle → final và chỉ ra shuffle nhỏ đi bao nhiêu nhờ partial.
- Nhìn `explain()` thấy 2 tầng `HashAggregate` và giải thích cho junior tầng nào làm gì.
- Quyết định có dùng `approx_count_distinct` cho dashboard hay không, và bảo vệ quyết định đó.

Kiến thức dùng trong thực tế: gold layer = aggregation. Revenue per seller per day, số khách distinct per tháng — tất cả là bài này. Job aggregation chậm hay OOM chiếm phần lớn ticket on-call của DE.

---

## 2. Why

### Vấn đề: đếm khi dữ liệu nằm rải trên nhiều máy

`GROUP BY seller_id, SUM(price)` — nhưng các dòng của seller A đang nằm trên 3 executor khác nhau. Muốn cộng, chúng phải **gặp nhau** → shuffle. Câu hỏi ăn tiền: gửi CÁI GÌ qua network?

**Cách ngây thơ**: gửi nguyên các dòng thô của seller A về một chỗ rồi cộng. Bảng 1 tỷ dòng → shuffle 1 tỷ dòng. Network khóc.

**Cách của Spark**: mỗi executor **cộng trước phần của mình** (partial aggregate), rồi chỉ gửi kết quả tạm — mỗi executor gửi tối đa 1 dòng *per seller*. 1 tỷ dòng, 3.000 seller, 100 executor → shuffle tối đa 300.000 dòng thay vì 1 tỷ. **Giảm hàng nghìn lần.**

```
Cách ngây thơ:  shuffle 1.000.000.000 dòng thô        😱
Spark:          partial agg tại chỗ → shuffle ≤ (số executor × số key) dòng tạm   ✅
```

> **Analogy bầu cử**: kiểm phiếu toàn quốc không phải chở HẾT phiếu về Hà Nội đếm. Từng điểm bỏ phiếu đếm tại chỗ (partial), chỉ gửi con số tổng của điểm mình (shuffle bé tí), trung ương cộng các con số (final). Chở phiếu thô về trung ương = shuffle dòng thô = cách ngây thơ.

### Nếu không hiểu bài này thì sao?

- Bạn thấy `explain()` in 2 lần `HashAggregate` và tưởng Spark "chạy trùng", đi tối ưu bậy.
- Bạn dùng `countDistinct` cho 50 cột dashboard rồi thắc mắc vì sao job 3 tiếng.
- Bạn không hiểu vì sao `groupBy` cột high-cardinality (order_id) chậm hơn hẳn low-cardinality (state) dù cùng số dòng.
- Gặp OOM/spill trong aggregation, bạn tăng memory mù quáng thay vì hiểu hash table đang phình vì đâu.

### Trade-off trung tâm của bài

| Lựa chọn | Được | Mất |
|---|---|---|
| Partial aggregate (mặc định) | Shuffle nhỏ đi cực nhiều | Tốn CPU + memory hash table ở map side |
| `countDistinct` (exact) | Chính xác 100% | Không partial-combine gọn được → shuffle to, có thể expand nhiều tầng |
| `approx_count_distinct` (HLL) | Shuffle tí hon (sketch vài KB/key), 1 pass | Sai số ~2–5% (chỉnh được bằng tham số `rsd`) |
| HashAggregate | Nhanh (O(1) lookup) | Cần memory cho hash table; đầy thì spill |
| SortAggregate | Không cần hash table lớn | Phải sort — chậm hơn khi hash còn vừa memory |

---

## 3. Theory

### 3.1. `groupBy` trả về GroupedData — trạm trung chuyển

```python
gd = df.groupBy("seller_id")        # <class 'pyspark.sql.group.GroupedData'>
gd.count()                          # → DataFrame
gd.agg(F.sum("price"))              # → DataFrame
```

`GroupedData` KHÔNG phải DataFrame — nó là trạng thái dở dang "đã biết chia nhóm theo gì, chưa biết tính gì". Không `show()`, không `filter()` được trên nó. Chỉ khi gọi `agg`/`count`/`sum`... mới quay về DataFrame. Điều này giúp API tách bạch: *chia nhóm theo gì* vs *tính gì trên mỗi nhóm*.

`agg` nhận nhiều biểu thức cùng lúc — và đây là cách đúng (1 lần quét, 1 shuffle cho tất cả):

```python
df.groupBy("seller_id").agg(
    F.count("*").alias("n_items"),
    F.sum("price").alias("revenue"),
    F.avg("price").alias("avg_price"),
    F.min("shipping_limit_date").alias("first_ship"),
    F.max("price").alias("max_price"),
)
# KHÔNG viết: gd.sum(...) rồi lại gd.avg(...) rồi join lại — 2 job, 2 shuffle, tự hành mình
```

### 3.2. Partial → Shuffle → Final — sơ đồ phải thuộc lòng

`items.groupBy("seller_id").agg(F.sum("price"))` trên 2 executor:

```
 STAGE 1 (map side)                                STAGE 2 (reduce side)
┌─────────────────────────────┐
│ Executor 1                  │
│ partition 0:                │
│  (A,10) (B,5) (A,20)        │      shuffle
│    │ PARTIAL AGG            │   (hash seller_id
│    ▼ hash table:            │    → reducer nào)
│  {A: 30, B: 5}  ────────────┼──────┐
└─────────────────────────────┘      │        ┌──────────────────────────────┐
                                     ├───────▶│ Reducer partition "A,C..."   │
┌─────────────────────────────┐      │        │ nhận: {A:30} {A:45} {C:7}    │
│ Executor 2                  │      │        │ FINAL AGG → A: 75, C: 7      │
│ partition 1:                │      │        └──────────────────────────────┘
│  (A,45) (C,7) (B,1)         │      │        ┌──────────────────────────────┐
│    │ PARTIAL AGG            │      └───────▶│ Reducer partition "B..."     │
│    ▼ hash table:            │               │ nhận: {B:5} {B:1}            │
│  {A: 45, C: 7, B: 1} ───────┼──────────────▶│ FINAL AGG → B: 6             │
└─────────────────────────────┘               └──────────────────────────────┘

 Bay qua network: 5 dòng tạm (A:30,B:5,A:45,C:7,B:1) thay vì 6 dòng thô.
 Dữ liệu thật: tỷ dòng thô → vài trăm nghìn dòng tạm. Đó là phép màu map-side combine.
```

Trong `explain()` bạn thấy đúng cấu trúc này:

```
HashAggregate(keys=[seller_id], functions=[sum(price)])          ← FINAL
+- Exchange hashpartitioning(seller_id, 200)                     ← SHUFFLE
   +- HashAggregate(keys=[seller_id], functions=[partial_sum(price)])   ← PARTIAL
      +- Scan ...
```

Hai `HashAggregate` không phải chạy trùng — là 2 nửa của một chiến lược. `Exchange` = shuffle; số `200` = `spark.sql.shuffle.partitions` mặc định (bạn đã gặp ở Module 1).

Điều kiện để partial hiệu quả: hàm aggregate phải **gộp được từng phần** — sum, count, min, max, avg (giữ cặp sum+count) đều được. Cardinality của key quyết định độ lợi: `groupBy("customer_state")` (27 giá trị) partial ép cực gọn; `groupBy("order_id")` (mỗi key ~1 dòng) partial gần như vô dụng — shuffle vẫn to.

### 3.3. HashAggregateExec vs SortAggregateExec

**HashAggregateExec** (mặc định, ưu tiên): giữ hash table `key → buffer tạm` trong memory (vùng execution memory, off-heap-friendly dạng UnsafeRow). Mỗi dòng vào: hash key, tìm buffer, cập nhật. O(1) mỗi dòng, không cần sort.

**SortAggregateExec** (phương án B): sort dữ liệu theo key trước, rồi quét tuần tự — các dòng cùng key nằm cạnh nhau nên chỉ cần 1 buffer hiện hành. Không tốn hash table, nhưng trả giá bằng sort O(n log n).

Spark chọn SortAggregate khi: kiểu dữ liệu của **aggregation buffer không mutable/fixed-size** (ví dụ buffer chứa string/kiểu phức tạp như `collect_list`, `min/max` trên string ở một số dạng) — hash table dạng UnsafeRow cần buffer kích thước cố định. Còn sum/count/avg trên kiểu số → luôn HashAggregate.

### 3.4. Spill to disk — khi hash table đầy

Hash table sống trong **execution memory** của executor (chi tiết memory model ở lesson 17). GroupBy key cardinality cao → hash table phình. Khi xin thêm memory không được:

```
Hash table đầy → sort các entry hiện có theo key → GHI XUỐNG DISK (spill file)
→ làm tiếp với hash table rỗng → (có thể spill nhiều lần)
→ cuối cùng: merge các spill file (đã sort) + phần trong memory theo kiểu sort-merge
```

Nghĩa là HashAggregate khi hết memory **tự thoái hóa dần về sort-based** — job KHÔNG chết, chỉ chậm đi (disk I/O + sort). Trên Spark UI, cột **Spill (Memory/Disk)** trong Stages tab > 0 là dấu hiệu. Thấy spill nặng: tăng shuffle partitions (mỗi task ôm ít key hơn), tăng memory per task, hoặc giảm cardinality trước khi group.

### 3.5. countDistinct vs approx_count_distinct

`F.countDistinct("customer_id")`: để đếm distinct CHÍNH XÁC, Spark phải nhìn thấy mọi giá trị — không thể "cộng số đếm tạm" như sum (2 executor cùng thấy khách X, cộng thô là đếm đôi). Kế hoạch thực tế: khử trùng lặp theo (key, giá trị) trước — thêm tầng aggregate/expand — rồi mới đếm. Nhiều `countDistinct` trên nhiều cột trong 1 query → operator `Expand` nhân bản mỗi dòng theo số cột distinct → dữ liệu phình gấp N lần trước shuffle. Đắt là vì vậy.

`F.approx_count_distinct("customer_id", rsd=0.05)`: dùng **HyperLogLog** — mỗi giá trị được hash; thuật toán chỉ ghi nhớ "số bit 0 dẫn đầu dài nhất từng thấy" trong các bucket. Trực giác: hash mà hiếm hoi có 20 bit 0 dẫn đầu → đã gặp cỡ 2²⁰ giá trị khác nhau. Toàn bộ "trí nhớ" là một **sketch vài KB cố định**, các sketch **merge được** (lấy max từng bucket) → partial aggregate hoạt động hoàn hảo, shuffle vài KB mỗi key. Sai số chuẩn mặc định `rsd=0.05` (5%), giảm `rsd` thì sketch to lên.

Luật chọn: **billing/đối soát/tài chính → exact**; dashboard/monitoring/trend nghìn tỷ dòng → approx, sai 2% không ai chết, nhanh gấp chục lần.

### 3.6. pivot, rollup, cube

```python
# pivot: xoay giá trị của cột thành CỘT — báo cáo chéo
pay.groupBy("state").pivot("payment_type").agg(F.sum("payment_value"))
# → state | credit_card | boleto | voucher | debit_card

# rollup: tổng dần theo cấp bậc — có dòng subtotal + grand total
df.rollup("state", "city").agg(F.sum("price"))
# → (state, city) | (state, null=tổng theo state) | (null, null=tổng toàn cục)

# cube: MỌI tổ hợp chiều
df.cube("state", "payment_type").agg(F.sum("price"))
# → cả (null, payment_type) — thứ rollup không có
```

`rollup` sinh n+1 mức nhóm, `cube` sinh 2ⁿ — cube 4 cột = 16 mức nhóm, dữ liệu qua Expand nhân 16 lần. Cẩn thận. Phân biệt null "subtotal" với null dữ liệu thật bằng `F.grouping("col")` (1 = null do subtotal).

`pivot` pitfall lớn: không truyền danh sách giá trị → Spark phải chạy **một job phụ** đếm distinct giá trị trước (và trần `spark.sql.pivotMaxValues` = 10.000). Luôn truyền tường minh: `pivot("payment_type", ["credit_card", "boleto", "voucher", "debit_card"])` — nhanh hơn và schema ổn định (cột không mọc thêm khi có giá trị rác mới).

---

## 4. Internal

Đường đi của một dòng dữ liệu qua `groupBy("seller_id").agg(sum, count)`:

```
① Task map-side đọc partition của nó (đã qua filter/project — narrow, cùng stage)
        │
② HashAggregate (partial): hash(seller_id) → tra hash table trong execution memory
   - Có entry: cập nhật buffer (sum += price; count += 1)
   - Chưa có: xin memory, tạo entry {sum=price, count=1}
   - Hết memory: sort entries theo key → spill xuống disk → hash table mới
        │
③ Kết thúc input: (merge spill nếu có) → ghi SHUFFLE FILE, các dòng tạm được
   phân vùng theo hash(seller_id) % numShufflePartitions (mặc định 200)
        │
④ ── ranh giới stage ── reducer task kéo (fetch) đúng mảnh của mình
   từ TẤT CẢ mapper qua network
        │
⑤ HashAggregate (final): merge các buffer tạm cùng key
   (sum_final = Σ sum_partial; count_final = Σ count_partial;
    avg = sum_final / count_final — vì thế avg cần buffer 2 trường)
        │
⑥ Trả kết quả cho operator tiếp theo (write / sort / show)
```

Ghi chú sâu:
- Toàn bộ bước ②⑤ chạy trong **code sinh tự động** (whole-stage codegen) — Catalyst dịch plan thành 1 hàm Java lồng vòng lặp, không gọi hàm ảo từng dòng. Lesson 13 sẽ soi.
- Buffer của avg là ví dụ đẹp về "aggregate phải phân rã được": avg không tự merge được (avg của avg là SAI khi nhóm lệch size), nên Spark lưu (sum, count) và chỉ chia ở bước cuối.
- Số reducer = `spark.sql.shuffle.partitions` (200) bất kể key có 27 hay 3 triệu giá trị → quá to thì task lèo tèo overhead, quá nhỏ thì task ôm nhiều key dễ spill. AQE (lesson 20) tự gộp partition thừa.

---

## 5. API

### `groupBy(*cols)` → `GroupedData`

```python
df.groupBy("seller_id")                        # 1 cột
df.groupBy("seller_id", F.year("d").alias("y")) # theo biểu thức cũng được
```
- **Pitfall**: `groupBy()` không tham số + `agg` = aggregate toàn bảng về 1 nhóm — hợp lệ và hữu ích (`df.groupBy().agg(F.sum("price"))`), nhưng kết quả 1 dòng duy nhất → mọi dữ liệu tạm dồn về 1 reducer (với global agg thì partial đã ép nhỏ nên thường ổn).

### `agg(*exprs)`

```python
df.groupBy("seller_id").agg(
    F.sum("price").alias("revenue"),
    F.countDistinct("order_id").alias("n_orders"))
```
- **Ý nghĩa**: khai nhiều aggregation trong MỘT lượt — 1 shuffle cho tất cả.
- **Pitfall**: luôn `alias` — không thì tên cột thành `sum(price)` có ngoặc, gọi lại phải backtick. Kiểu dict `agg({"price": "sum"})` không alias được và không dùng 2 hàm cùng cột — tránh.

### Các hàm hay dùng trong `agg`

```python
F.count("*")           # đếm mọi dòng      | F.count("col") BỎ QUA null — khác nhau đấy!
F.sum, F.avg, F.min, F.max
F.countDistinct("c")                     # exact — đắt
F.approx_count_distinct("c", rsd=0.05)   # HLL — rẻ, sai số ~5%
F.collect_list("c"), F.collect_set("c")  # gom giá trị thành array
F.first("c", ignorenulls=True), F.last("c")
```
- **Pitfall `count`**: `count("col")` bỏ null, `count("*")` đếm hết — chênh nhau là số null, nguồn của "sao 2 báo cáo lệch nhau?".
- **Pitfall `collect_list`**: buffer phình theo dữ liệu (không phải kích thước cố định) → dễ đẩy sang SortAggregate + nguy cơ OOM khi 1 key có triệu phần tử. Nghĩ kỹ trước khi gom cả nhóm vào một array.
- **Pitfall `first/last`** trong groupBy: **không xác định** dòng nào là "first" — phụ thuộc thứ tự partition. Cần deterministic → window + row_number (lesson 10).

### `pivot(col, [values])`

```python
pay.groupBy("customer_state") \
   .pivot("payment_type", ["credit_card", "boleto", "voucher", "debit_card"]) \
   .agg(F.round(F.sum("payment_value"), 2))
```
- **Pitfall**: thiếu list values → job phụ dò distinct + schema bất ổn định. Ô không có dữ liệu → null (thường `fillna(0)` sau pivot).

### `rollup(*cols)` / `cube(*cols)` / `F.grouping(col)`

```python
(df.rollup("customer_state", "customer_city")
   .agg(F.sum("price").alias("rev"), F.grouping("customer_city").alias("is_subtotal")))
```
- **Pitfall**: null trong KẾT QUẢ có 2 nghĩa (subtotal vs null dữ liệu) — bắt buộc `grouping()` để phân biệt. Cube nhiều cột = Expand 2ⁿ — đo trước khi thêm chiều.

---

## 6. Demo nhỏ

```
Input:  8 dòng (seller, price) trên 2 partition
   ↓    groupBy(seller).agg(sum, avg)  — wide!
Output: show() + explain() soi 2 tầng HashAggregate
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo08").master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()

data = [("A", 10.0), ("B", 5.0), ("A", 20.0), ("C", 7.0),
        ("A", 45.0), ("B", 1.0), ("C", 3.0), ("A", 25.0)]
df = spark.createDataFrame(data, ["seller", "price"]).repartition(2)

result = df.groupBy("seller").agg(
    F.sum("price").alias("revenue"),
    F.avg("price").alias("avg_price"),
    F.count("*").alias("n"))

result.explain()
# == Physical Plan ==
# AdaptiveSparkPlan
# +- HashAggregate(keys=[seller], functions=[sum(price), avg(price), count(1)])   ← FINAL
#    +- Exchange hashpartitioning(seller, 4)                                       ← SHUFFLE
#       +- HashAggregate(keys=[seller], functions=[partial_sum(price), ...])       ← PARTIAL
#          +- Exchange RoundRobinPartitioning(2) ...                               ← do repartition(2)

result.show()
# +------+-------+---------+---+
# |seller|revenue|avg_price|  n|
# +------+-------+---------+---+
# |     A|  100.0|     25.0|  4|
# |     B|    6.0|      3.0|  2|
# |     C|   10.0|      5.0|  2|
# +------+-------+---------+---+
input("Mở http://localhost:4040 → SQL tab → xem 2 khối HashAggregate. Enter thoát...")
spark.stop()
```

Bài tập mắt: trong SQL tab, node HashAggregate partial có metric `number of output rows` NHỎ hơn input (8 dòng → ≤ 6 dòng tạm, vì mỗi partition tối đa 3 seller) — đó chính là map-side combine bằng số liệu thật.

---

## 7. Production Example

Gold layer kinh điển: **daily seller performance** — bảng nuôi dashboard của team business:

```python
def build_seller_daily(orders, items):
    """Gold: mỗi (seller, ngày) một dòng — revenue, đơn, khách, tỷ lệ ship đúng hạn."""
    base = (orders
        .filter(F.col("order_status") == "delivered")            # early filter trước join
        .select("order_id", "customer_id",
                F.to_date("order_purchase_timestamp").alias("d"))
        .join(items.select("order_id", "seller_id", "price", "freight_value"),
              "order_id"))
    return (base
        .groupBy("seller_id", "d")
        .agg(
            F.round(F.sum("price"), 2).alias("revenue"),
            F.round(F.sum("freight_value"), 2).alias("freight"),
            F.countDistinct("order_id").alias("n_orders"),        # exact: nuôi đối soát
            F.approx_count_distinct("customer_id").alias("n_customers_approx"),  # trend: approx đủ
            F.max("price").alias("max_item_price"),
        ))
```

Quyết định đáng chú ý: cùng một bảng, `n_orders` dùng exact (con số này đi vào đối soát doanh thu — sai 1 đơn cũng bị hỏi), còn `n_customers_approx` dùng HLL (chỉ để vẽ trend — đổi lấy shuffle nhẹ). **Chọn độ chính xác theo NGƯỜI DÙNG con số, không theo thói quen** — đó là tư duy Senior. Và để ý: filter + select cắt cột đứng TRƯỚC join, groupBy đứng SAU join — thứ tự này là xương sống của mọi job gold.

---

## 8. Hands-on Lab

**Mục tiêu**: chạy aggregation trên Olist thật, soi partial/final trong plan và UI, đo exact vs approx.

### Bước 1 — tạo `labs/lab08/aggregations_olist.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab08-aggregations").getOrCreate()
DATA = "/workspace/data/olist"

items = (spark.read.csv(f"{DATA}/olist_order_items_dataset.csv", header=True)
         .select("order_id", "seller_id", "product_id",
                 F.col("price").cast("double").alias("price"),
                 F.col("freight_value").cast("double").alias("freight_value")))
orders = (spark.read.csv(f"{DATA}/olist_orders_dataset.csv", header=True)
          .select("order_id", "customer_id", "order_status",
                  F.to_date("order_purchase_timestamp").alias("d")))
pay = (spark.read.csv(f"{DATA}/olist_order_payments_dataset.csv", header=True)
       .select("order_id", "payment_type",
               F.col("payment_value").cast("double").alias("payment_value")))

# ── 1. Multi-agg một lượt: revenue theo seller ─────────────────────
seller_stats = items.groupBy("seller_id").agg(
    F.count("*").alias("n_items"),
    F.round(F.sum("price"), 2).alias("revenue"),
    F.round(F.avg("price"), 2).alias("avg_price"),
    F.max("price").alias("max_price"))
seller_stats.explain()                       # tìm 2 tầng HashAggregate + Exchange
seller_stats.orderBy(F.desc("revenue")).show(10)

# ── 2. Cardinality thấp vs cao: xem partial ép được bao nhiêu ─────
orders.groupBy("order_status").count().show()          # ~8 key — partial ép cực mạnh
items.groupBy("order_id").count().count()              # ~99k key — partial gần vô dụng
# → so shuffle write của 2 job này trong Stages tab!

# ── 3. Exact vs approx count distinct ──────────────────────────────
t0 = time.time()
exact = items.select(F.countDistinct("product_id")).collect()[0][0]
t_exact = time.time() - t0
t0 = time.time()
approx = items.select(F.approx_count_distinct("product_id", 0.02)).collect()[0][0]
t_approx = time.time() - t0
err = abs(exact - approx) / exact * 100
print(f"exact={exact} ({t_exact:.2f}s) | approx={approx} ({t_approx:.2f}s) | sai số={err:.2f}%")

# ── 4. Pivot: doanh số payment_type theo state ─────────────────────
cust = spark.read.csv(f"{DATA}/olist_customers_dataset.csv", header=True) \
            .select("customer_id", "customer_state")
pivoted = (orders.join(cust, "customer_id").join(pay, "order_id")
    .groupBy("customer_state")
    .pivot("payment_type", ["credit_card", "boleto", "voucher", "debit_card"])
    .agg(F.round(F.sum("payment_value"), 0))
    .fillna(0))
pivoted.orderBy(F.desc("credit_card")).show(10)

# ── 5. Rollup: subtotal theo state → city ──────────────────────────
rolled = (orders.join(cust, "customer_id").join(items, "order_id")
    .rollup("customer_state", "customer_city")
    .agg(F.round(F.sum("price"), 0).alias("rev"),
         F.grouping("customer_city").alias("is_state_total"))
    .filter(F.col("customer_state") == "SP"))
rolled.orderBy(F.desc("rev")).show(10)

input(">>> Mở http://localhost:4040: SQL tab (HashAggregate x2) + Stages tab (Shuffle Write). Enter thoát.")
spark.stop()
```

### Bước 2 — chạy

```bash
make run F=labs/lab08/aggregations_olist.py
```

### Bước 3 — quan sát (quan trọng nhất)

1. **SQL tab** → query seller_stats: node HashAggregate dưới (partial) output bao nhiêu dòng, node trên (final) output bao nhiêu? Exchange ở giữa "data size" bao nhiêu?
2. **Stages tab**: so **Shuffle Write** của job `groupBy(order_status)` vs `groupBy(order_id)` — cùng bảng, khác cardinality, chênh bao nhiêu lần? Đây là bằng chứng sống của map-side combine.
3. Sai số approx thực tế bạn đo được? So với `rsd=0.02` cam kết.
4. Ghi các con số vào `labs/lab08/NOTES.md`.

---

## 9. Assignment

**Easy** — Trên Olist tính bằng MỘT `agg` mỗi câu: (a) count/sum/avg/min/max của `price` theo `seller_id`; (b) tổng `payment_value` và số đơn theo `payment_type`. Mỗi câu dán `explain()` và khoanh: đâu là partial, đâu là Exchange, đâu là final.

**Medium** — Multi-level: doanh thu theo (seller → product category → tháng). Join items × products × orders, groupBy 3 cột. Sau đó dùng `rollup` cùng 3 cột để có subtotal từng cấp; dùng `grouping()` tạo cột `level` (0=chi tiết, 1=theo category, 2=theo seller, 3=grand total — gợi ý `F.grouping_id()`). Trả lời: rollup 3 cột tạo mấy mức nhóm? Nhìn Expand trong plan, số dòng nhân lên bao nhiêu?

**Hard** — Ép spill: chạy `items.groupBy("order_id", "product_id", "seller_id").agg(F.collect_list("price"))` (key cardinality cực cao + buffer phình) với memory bóp nhỏ — thêm config khi submit local: `--conf spark.executor.memory=512m --conf spark.sql.shuffle.partitions=2` (sửa lệnh spark-submit trong container, hoặc set trong builder khi run-local). Mở Stages tab tìm cột **Spill (Disk)**. Sau đó tăng `spark.sql.shuffle.partitions=64` chạy lại. Báo cáo: spill còn không? Vì sao tăng số partition giảm spill? (gợi ý: mỗi task ôm bao nhiêu key). Nếu không ép được spill với dataset này, giải thích tại sao (dataset cỡ nào so với memory) — câu trả lời đó cũng được điểm.

**Production Challenge** — Xây `labs/lab08/gold_seller_daily.py`: bảng gold (seller_id, ngày) với revenue, n_orders (exact), n_customers (approx, biện luận vì sao), avg_review_score (join `olist_order_reviews_dataset.csv`), tỷ lệ đơn late (dựa `order_estimated_delivery_date` vs `order_delivered_customer_date` — cẩn thận null!). Ghi ra Parquet partition theo ngày. Kèm 5 dòng comment đầu file: ai dùng bảng này, con số nào cần exact, SLA chạy mấy giờ sáng. Đây là checkpoint 3 của PROJECT 1 phiên bản mini.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| `groupBy` key cardinality thấp (state) | Nhanh | Partial ép trăm nghìn dòng còn vài chục dòng tạm/task — shuffle tí hon |
| `groupBy` key cardinality cao (order_id) | Chậm hơn nhiều | Partial gần như không ép được — shuffle ~ số dòng gốc; hash table to → dễ spill |
| Nhiều hàm trong 1 `agg` | Rẻ | Vẫn 1 lần quét + 1 shuffle; tách nhiều `agg` rồi join lại = trả tiền N lần |
| `countDistinct` | Đắt | Phải dedup trước khi đếm — thêm tầng aggregate; nhiều cột distinct → Expand nhân dữ liệu |
| `approx_count_distinct` | Rẻ | Sketch HLL vài KB, merge được → partial combine hoàn hảo |
| `collect_list` trên key lệch | Nguy hiểm | Buffer phình theo dữ liệu, 1 key triệu phần tử = OOM chờ sẵn |
| `cube` n cột | Đắt theo 2ⁿ | Expand nhân mỗi dòng thành 2ⁿ bản trước aggregate |
| Spill to disk | Chậm nhưng sống | Hash agg thoái hóa về sort-based; chữa bằng tăng shuffle partitions / memory |

---

## 11. Spark UI

Ba chỗ phải nhìn sau mỗi job aggregation:

**SQL tab → query detail**: cây plan có `HashAggregate → Exchange → HashAggregate`. Hover từng node:
- Partial HashAggregate: `number of output rows` — so với input rows là biết partial ép được bao nhiêu.
- `Exchange`: `data size` / `shuffle bytes written` — con số tiền tươi của query.
- Thấy `Expand` → bạn đang trả phí countDistinct nhiều cột hoặc rollup/cube.

**Stages tab**: cột **Shuffle Write** (stage map) và **Shuffle Read** (stage reduce). Và cột **Spill (Memory) / Spill (Disk)** — chỉ hiện khi có spill; hiện là hash table từng đầy.

**Jobs tab**: `pivot` không truyền values → bạn sẽ thấy MỘT JOB THỪA chạy trước (job dò distinct values) — bắt quả tang pitfall bằng mắt.

---

## 12. Common Mistakes

1. **Tách nhiều `agg` rồi join lại** (`gd.sum()` xong `gd.avg()` xong join) — N shuffle thay vì 1. Gom hết vào một `agg`.
2. **`avg` của `avg`**: tính avg theo city rồi avg tiếp theo state = SAI khi city lệch size. Phải mang (sum, count) lên cấp trên rồi mới chia — chính là bài học từ buffer của Spark.
3. **`count("col")` tưởng đếm mọi dòng** — nó bỏ null. Hai báo cáo lệch nhau chỉ vì người dùng `count("*")` người dùng `count("customer_id")`.
4. **`countDistinct` rải khắp dashboard** — mỗi cột thêm là plan thêm tầng/Expand. Cân nhắc approx, hoặc dedup một lần rồi count thường.
5. **`first()`/`last()` trong groupBy để lấy "bản ghi mới nhất"** — không deterministic, mỗi lần chạy có thể ra khác. Dùng window row_number (lesson 10).
6. **`pivot` không truyền danh sách values** — job phụ + schema trôi nổi theo dữ liệu (giá trị rác mới = cột mới, downstream vỡ).
7. **Quên null của rollup/cube có 2 nghĩa** — báo cáo cộng nhầm dòng subtotal vào chi tiết = doanh thu x2. Luôn lọc/gắn nhãn bằng `grouping()`.
8. **Thấy 2 HashAggregate trong plan tưởng bug/chạy trùng** rồi tìm cách "tối ưu bỏ bớt" — đó là partial+final, là TÍNH NĂNG.

---

## 13. Interview

**Junior:**

1. *`groupBy` trả về gì? Vì sao không phải DataFrame?* — `GroupedData` — trạng thái trung gian "đã biết nhóm theo gì, chưa biết tính gì". Phải gọi `agg`/`count`/... mới trả về DataFrame. Thiết kế này tách khai báo nhóm khỏi khai báo phép tính.
2. *Kể 5 hàm aggregate và một lưu ý về null.* — sum, count, avg, min, max (+ countDistinct, collect_list...). Lưu ý: `count("*")` đếm mọi dòng, `count("col")` bỏ qua null; sum/avg cũng bỏ null (avg = sum/số-dòng-không-null).
3. *groupBy có gây shuffle không? Vì sao?* — Có — wide transformation: các dòng cùng key nằm rải nhiều partition/executor, phải gom về cùng reducer mới gộp được. Ranh giới shuffle = ranh giới stage.
4. *pivot làm gì? Một pitfall?* — Xoay giá trị của một cột thành các cột (báo cáo chéo). Pitfall: không truyền danh sách giá trị → Spark chạy job phụ dò distinct + schema phụ thuộc dữ liệu; luôn truyền tường minh.

**Mid:**

5. *Giải thích partial aggregate / map-side combine. Vì sao nó là tối ưu quan trọng nhất của groupBy?* — Mỗi mapper aggregate cục bộ phần dữ liệu của mình trước, chỉ shuffle kết quả tạm (≤ số key/mapper dòng) thay vì dòng thô. Với key cardinality thấp, shuffle giảm hàng trăm–nghìn lần — mà shuffle là chi phí lớn nhất. Trong plan là cặp HashAggregate(partial_*) / Exchange / HashAggregate(final).
6. *Vì sao `avg` không thể "avg của các avg partial"? Spark giải quyết sao?* — Avg của avg sai khi nhóm con khác kích thước. Buffer partial lưu (sum, count); final cộng các sum, cộng các count rồi mới chia. Tổng quát: hàm aggregate phải phân rã được thành update/merge/evaluate — đây cũng là điều bạn phải làm khi viết custom aggregator.
7. *HashAggregateExec vs SortAggregateExec — Spark chọn thế nào, và chuyện gì xảy ra khi hash table hết memory?* — Mặc định Hash (O(1)/dòng, cần buffer kích thước cố định mutable — kiểu số). Buffer kiểu biến thiên (string/collect_list...) → SortAggregate (sort rồi quét tuần tự). Hash table đầy → sort các entry, spill xuống disk, có thể nhiều lần, cuối cùng merge kiểu sort-merge — job sống nhưng chậm; nhìn thấy ở cột Spill trên UI.
8. *`countDistinct` vs `approx_count_distinct` — cơ chế và khi nào dùng gì?* — Exact phải dedup trước khi đếm (thêm tầng agg; nhiều cột → Expand nhân dữ liệu) — đắt. Approx dùng HyperLogLog: sketch vài KB, merge được nên partial combine trọn vẹn, sai số ~2–5% chỉnh bằng rsd. Tài chính/đối soát → exact; dashboard/trend/scale lớn → approx.

**Senior:**

9. *Job groupBy của bạn spill nặng. Nêu chuỗi chẩn đoán và các đòn xử lý theo thứ tự ưu tiên.* — Chẩn đoán: Stages tab xem Spill Memory/Disk, task nào spill (đều hay lệch — lệch là skew, sang lesson 19); SQL tab xem cardinality key qua output rows của partial agg. Xử lý: (1) giảm dữ liệu vào agg — early filter, bỏ cột thừa, tránh collect_list; (2) tăng `spark.sql.shuffle.partitions` để mỗi task ôm ít key (hoặc bật AQE); (3) tăng memory per task (executor memory / giảm concurrent tasks); (4) xét lại thuật toán — approx thay exact, 2 bước dedup+count. Tăng memory mù quáng là đòn CUỐI, không phải đầu.
10. *Thiết kế bảng "monthly active users" trên 10 tỷ event — bạn chọn exact hay HLL? Phân tích sâu hơn: làm sao có MAU exact mà vẫn rẻ?* — Trả lời tốt: phân tầng. Trend/dashboard → `approx_count_distinct` hoặc lưu HLL sketch theo ngày rồi merge thành tuần/tháng (sketch merge được — đếm distinct nhiều khoảng thời gian không cần quét lại). Cần exact → dedup sớm: bảng daily distinct users (dedup 1 lần lúc ingest, mỗi ngày nhỏ hơn ngàn lần event thô) rồi countDistinct trên bảng đã nhỏ. Điểm ăn tiền: nhận ra "đếm distinct" là bài toán THIẾT KẾ dữ liệu, không phải chọn hàm.

---

## 14. Summary

### Mindmap

```
                       LESSON 8 — AGGREGATIONS
                                │
    ┌───────────────┬───────────┼───────────────┬────────────────────┐
    ▼               ▼           ▼               ▼                    ▼
 API             CƠ CHẾ      PHYSICAL        DISTINCT            NHIỀU CHIỀU
    │               │           │               │                    │
 groupBy →       partial    HashAggregate    countDistinct        pivot (+values!)
 GroupedData     → shuffle  (hash table,     = dedup trước, đắt   rollup (n+1 mức)
 agg nhiều hàm   → final    spill khi đầy)   approx = HLL sketch  cube (2ⁿ mức)
 1 lượt = 1      shuffle bé  SortAggregate   vài KB, merge được   null 2 nghĩa →
 shuffle         nhờ combine (buffer động)   sai số ~2-5%         grouping()
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại được sơ đồ partial → shuffle → final, giải thích shuffle nhỏ đi nhờ đâu.
- [ ] Đọc explain: chỉ đúng partial HashAggregate, Exchange, final HashAggregate.
- [ ] Giải thích vì sao avg cần buffer (sum, count) — và vì sao avg-của-avg sai.
- [ ] Nói được khi nào Spark rơi về SortAggregate, và spill xảy ra thế nào.
- [ ] Biết giá của countDistinct và cơ chế HLL của approx.
- [ ] Đã chạy lab: so shuffle write giữa key cardinality thấp/cao, đo sai số approx.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 9 — Joins: broadcast, sort-merge, shuffle-hash.**

GroupBy mới là shuffle "một bảng". Join là shuffle **hai bảng cùng lúc** — nguồn shuffle lớn nhất, nơi các job production sống hoặc chết. Nhưng Spark có tới 3 chiến lược: khi một bảng đủ nhỏ, nó **broadcast** — gửi nguyên bảng nhỏ đến mọi executor và join KHÔNG CẦN shuffle bảng lớn (nhanh gấp chục lần); khi cả hai to, **sort-merge join** vào cuộc. Bạn sẽ học cách đọc physical plan để biết Spark chọn gì, cách ép nó chọn đúng bằng hint, và trả lời câu phỏng vấn kinh điển: "join hai bảng tỷ dòng, anh làm gì?".

Đây là bài quan trọng nhất Module 2 — đừng học lúc buồn ngủ.

> Gõ **"Continue"** khi sẵn sàng.
