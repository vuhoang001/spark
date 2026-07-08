# Lesson 9 — Joins: broadcast, sort-merge, shuffle-hash

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 5 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Các **loại join** (inner/left/right/full/semi/anti/cross) — ngữ nghĩa, khi nào dùng.
- Ba **join strategy** vật lý: **BroadcastHashJoin**, **SortMergeJoin**, **ShuffledHashJoin** — cơ chế, điều kiện Spark chọn, ASCII diagram từng cái.
- `spark.sql.autoBroadcastJoinThreshold` (mặc định 10MB) và **broadcast hint** — cách ép Spark làm điều đúng.
- Đọc physical plan của join bằng `explain()` — kỹ năng sống còn.
- Hai quả mìn dữ liệu: **null join key** và **duplicate key explosion**.
- Join reorder và tư duy chọn strategy cho **bảng tỷ dòng**.

Sau bài này bạn phải làm được:

- Nhìn `explain()` nói ngay job dùng strategy nào, có Exchange ở đâu, vì sao.
- Ép broadcast đúng lúc bằng `F.broadcast(df)` và biết khi nào ép là TỰ SÁT (bảng "nhỏ" hóa to).
- Trả lời trôi chảy câu phỏng vấn: "join 2 bảng tỷ dòng, anh thiết kế thế nào?"

Kiến thức dùng trong thực tế: join là **nguồn shuffle lớn nhất** trong mọi pipeline. Fact join dim, orders join items, event join profile — 80% thời gian chạy của pipeline điển hình nằm ở vài cái join. Tối ưu join = tối ưu cả pipeline.

---

## 2. Why

### Vấn đề: hai bảng, các dòng cần nhau đang ở hai đầu cluster

`orders JOIN items ON order_id` — dòng order X trên executor 1, các item của X trên executor 5. Muốn ghép, chúng phải gặp nhau. Có đúng 2 con đường:

1. **Di chuyển CẢ HAI bảng**: shuffle hai bảng theo hash(order_id) — dòng cùng key về cùng partition. Đắt: hai bảng cùng bay qua network.
2. **Di chuyển MỘT bảng (bảng nhỏ) đến mọi nơi**: nếu bảng dim chỉ 5MB, copy nguyên nó cho mọi executor — bảng lớn KHÔNG di chuyển một byte nào. Đây là broadcast.

Mọi chiến lược join của mọi engine phân tán (Spark, Trino, BigQuery, Snowflake) đều xoay quanh 2 con đường này. Học một lần, dùng cả sự nghiệp.

> **Analogy tổ chức đám cưới hai họ**: SortMergeJoin = hai họ cùng kéo về một nhà hàng trung gian (cả hai đều di chuyển — mệt cả đôi bên). BroadcastJoin = họ nhà gái chỉ có 5 người, in danh sách 5 người đó gửi đến TỪNG nhà họ trai (họ trai nghìn người không ai phải đi đâu). Chở nghìn người đi gặp 5 người là ngu ngốc — vậy mà job Spark không tune vẫn làm thế mỗi đêm.

### Nếu không hiểu bài này thì sao?

- Job join dim 2MB với fact 500GB bằng SortMergeJoin — shuffle 500GB vô ích chỉ vì thiếu 1 dòng hint (hoặc vì thống kê size sai).
- Bạn ép broadcast một bảng "nhỏ" 2GB → executor OOM hàng loạt, và driver cũng gục khi gom bảng về để phát.
- Duplicate key làm kết quả join x10 số dòng — số liệu sai mà dashboard vẫn xanh.
- Phỏng vấn Senior: "kể 3 join strategy và điều kiện chọn" là câu MẶC ĐỊNH — trả lời ấp úng là dừng cuộc chơi.

### Trade-off ba chiến lược — bảng phải thuộc

| | BroadcastHashJoin | SortMergeJoin | ShuffledHashJoin |
|---|---|---|---|
| Shuffle bảng lớn? | **KHÔNG** | Có (cả hai bảng) | Có (cả hai bảng) |
| Sort? | Không | Có (cả hai phía) | Không |
| Điều kiện | 1 bảng ≤ threshold (10MB mặc định) và vừa memory | Key sort được (mặc định cho 2 bảng lớn) | 1 phía nhỏ hơn hẳn phía kia, đủ build hash table per-partition |
| Memory rủi ro | Bảng broadcast phải vừa RAM mọi executor + driver | An toàn nhất (sort spill được) | Hash table per partition — lệch là OOM |
| Hỗ trợ join type | Không full outer | Mọi loại | Không full outer |
| Tốc độ khi hợp lệ | **Nhanh nhất** | Ổn định, scale tốt nhất | Nhanh hơn SMJ khi khớp điều kiện |

---

## 3. Theory

### 3.1. Các loại join — ngữ nghĩa trước, tốc độ sau

```python
a.join(b, on="k", how="inner")        # chỉ dòng khớp cả 2 phía
a.join(b, on="k", how="left")         # mọi dòng a; b không khớp → cột b null
a.join(b, on="k", how="right")        # ngược lại
a.join(b, on="k", how="full")         # mọi dòng 2 phía, không khớp → null phía kia
a.join(b, on="k", how="left_semi")    # dòng a CÓ khớp trong b — chỉ trả CỘT CỦA A, không nhân bản
a.join(b, on="k", how="left_anti")    # dòng a KHÔNG khớp trong b — "NOT EXISTS"
a.crossJoin(b)                        # tích Descartes — n×m dòng, phải gọi tường minh
```

Hai anh em bị bỏ quên mà Senior dùng hằng ngày:

- **left_semi** = "lọc a theo sự tồn tại trong b". Khác inner join ở 2 điểm vàng: không lấy cột của b, và **không nhân bản dòng a khi b có key trùng**. Cần "orders có ít nhất 1 payment" → semi join, KHÔNG PHẢI inner + dropDuplicates.
- **left_anti** = "lọc a theo sự KHÔNG tồn tại trong b" — tìm orphan records, data quality check số 1 (items có seller_id không tồn tại trong bảng sellers?).

### 3.2. BroadcastHashJoin (BHJ)

```
        BẢNG NHỎ dims (5MB)                    BẢNG LỚN facts (500GB, 4000 partition)
              │
   ① driver thu bảng nhỏ về                    KHÔNG DI CHUYỂN, nằm yên tại chỗ
   ② đóng gói, gửi (broadcast)
      đến MỌI executor
              │
   ┌──────────┼──────────────┬────────────────────┐
   ▼          ▼              ▼                    ▼
┌────────────────┐  ┌────────────────┐   ┌────────────────┐
│ Executor 1     │  │ Executor 2     │   │ Executor N     │
│ [copy dims 5MB]│  │ [copy dims 5MB]│   │ [copy dims 5MB]│
│ build hash tbl │  │ build hash tbl │   │ build hash tbl │
│                │  │                │   │                │
│ mỗi task quét  │  │ facts part...  │   │ facts part...  │
│ facts partition│  │ probe hash tbl │   │ probe hash tbl │
│ → probe O(1)   │  │                │   │                │
└────────────────┘  └────────────────┘   └────────────────┘
   KHÔNG shuffle, KHÔNG sort, KHÔNG stage mới cho facts — join ngay trong stage đang chạy
```

Điều kiện Spark TỰ chọn BHJ: size ước lượng của một phía ≤ `spark.sql.autoBroadcastJoinThreshold` (mặc định **10MB**; đặt `-1` để tắt tự động). Lưu ý chữ **ước lượng**: với file có metadata (Parquet) khá chuẩn; sau vài tầng filter/join, ước lượng có thể sai xa → Spark bỏ lỡ cơ hội broadcast (bạn cứu bằng hint) hoặc broadcast nhầm bảng to (bạn cứu bằng tắt/threshold). AQE (lesson 20) sửa một phần vì đo size THẬT lúc runtime.

Giá của BHJ: bảng nhỏ bị kéo về driver rồi phát đi → driver memory + network fan-out; mỗi executor giữ nguyên một bản trong memory. "Nhỏ" nghĩa là nhỏ THẬT — chục MB, không phải "nhỏ hơn bảng kia".

### 3.3. SortMergeJoin (SMJ) — ngựa thồ mặc định

```
     orders (lớn)                          items (lớn)
        │                                     │
① SHUFFLE theo hash(order_id)     ① SHUFFLE theo hash(order_id)
   → dòng cùng key của CẢ HAI bảng rơi vào CÙNG partition số i
        │                                     │
        └────────────┬────────────────────────┘
                     ▼
        Partition i (trên 1 executor):
        ② SORT phía orders theo key      ② SORT phía items theo key
           o1,o2,o3,o5,...                  o1,o1,o2,o5,...
                     │
        ③ MERGE: hai con trỏ chạy song song như khóa kéo (zipper)
           key bằng nhau → ghép; bé hơn → tiến con trỏ bé
           ┌─ orders: o1 o2 o3 o5
           └─ items:  o1 o1 o2 o5   → (o1×2 ghép), (o2 ghép), o3 bỏ/null, (o5 ghép)
```

Vì sao nó là mặc định cho 2 bảng lớn: **sort và merge đều spill được xuống disk** — không có hash table nào phải vừa memory, nên gần như không bao giờ OOM vì bản thân join. Scale tới hàng tỷ dòng ổn định. Giá: 2 lần shuffle + 2 lần sort. (Ghi chú: nếu dữ liệu ĐÃ được bucket/sort sẵn theo key — bucketing, lesson sau của khóa — SMJ bỏ được shuffle/sort, đó là lý do các bảng fact hay được bucket theo join key.)

### 3.4. ShuffledHashJoin (SHJ)

```
① SHUFFLE cả hai bảng theo hash(key) — giống SMJ
② Tại mỗi partition: phía NHỎ hơn → build HASH TABLE (thay vì sort)
                     phía lớn hơn  → probe từng dòng, O(1)
   KHÔNG sort — tiết kiệm CPU so với SMJ
```

Điều kiện Spark tự chọn: một phía nhỏ hơn phía kia đáng kể (heuristic: nhỏ hơn ~3 lần) và mỗi partition của phía nhỏ đủ vừa memory để build hash table; và `spark.sql.join.preferSortMergeJoin=false` (mặc định **true** — nên ngoài đời ít gặp SHJ tự động, thường phải hint `df.hint("shuffle_hash")`). Rủi ro: key skew → một partition phình → hash table không vừa memory → OOM (đúng chỗ SMJ sống khỏe). Từ Spark 3.x SHJ cũng đã biết spill, nhưng SMJ vẫn là lựa chọn an toàn mặc định.

Thứ tự Spark cân nhắc (equi-join): **BHJ nếu có bảng dưới threshold → (SHJ nếu được phép & khớp heuristic) → SMJ**. Non-equi join (điều kiện `<`, `>`, `BETWEEN`) không dùng được 3 anh này theo kiểu thường — rơi về BroadcastNestedLoopJoin (một phía phải broadcast được) hoặc CartesianProduct: thấy 2 tên này trong plan với bảng lớn là ĐÈN ĐỎ.

### 3.5. Null key & duplicate key explosion — hai quả mìn

**Null key**: theo chuẩn SQL, `null == null` → null (không phải true) → **dòng key null KHÔNG BAO GIỜ khớp** trong inner join, lặng lẽ biến mất. Left join thì còn dòng nhưng cột phải toàn null. Tệ hơn về performance: triệu dòng null vẫn bị shuffle — cùng hash → dồn 1 partition → skew task. Xử lý: filter null key trước join (nếu nghiệp vụ cho phép), hoặc tách nhánh null xử lý riêng rồi union.

**Duplicate key explosion**: join khớp **mọi cặp** — key K có m dòng bên trái, n dòng bên phải → **m × n dòng kết quả**. Hai bảng cùng grain giao dịch join nhau qua key không-duy-nhất → nổ số dòng, doanh thu x10, shuffle phình. Phòng thủ chuẩn production: **kiểm tra grain trước khi join** — `df.groupBy("key").count().filter("count > 1")` phải rỗng ở phía bạn TIN là duy nhất; đếm dòng trước/sau join và assert tỷ lệ hợp lý.

### 3.6. Join reorder — thứ tự join là một quyết định

Join 4 bảng: `((facts ⋈ dim1) ⋈ dim2) ⋈ dim3` hay thứ tự khác — kết quả giống nhau, chi phí khác nhau trời vực (join làm nhỏ dữ liệu nên đi trước; join làm nổ dữ liệu nên đi sau). Catalyst có **cost-based join reorder** (`spark.sql.cbo.enabled` + `joinReorder.enabled`, cần chạy `ANALYZE TABLE` lấy statistics — mặc định TẮT). Thực tế PySpark đọc file: bạn **tự sắp thứ tự** — nguyên tắc: filter/semi-join giảm dữ liệu trước, join nở dữ liệu (1-n) sau cùng, dim nhỏ broadcast lúc nào cũng được.

---

## 4. Internal

Đường đi của `orders.join(items, "order_id")` (cả hai lớn → SMJ) từ code đến task:

```
① Logical plan: Join(orders, items, Inner, order_id = order_id)
        │
② Catalyst: đẩy filter xuống dưới join nếu có (PushDownPredicate),
   cắt cột không dùng ở CẢ HAI phía (ColumnPruning) — join ít cột = shuffle ít byte
        │
③ Physical planning — chọn strategy theo thứ tự:
   size ước lượng items ≤ 10MB?  → BroadcastHashJoin, xong
   preferSortMergeJoin? (true)   → SortMergeJoin
        │
④ SMJ đòi hai con: dữ liệu phân vùng theo key + sort theo key
   → chèn Exchange(hashpartitioning(order_id, 200)) MỖI phía
   → chèn Sort(order_id) MỖI phía
        │
⑤ DAG: 3 stage — stage A (scan orders + shuffle write),
   stage B (scan items + shuffle write),
   stage C (fetch cả 2 phía → sort → merge → downstream)
        │
⑥ Với BHJ thay vào đó: driver submit job con thu bảng nhỏ,
   đóng thành broadcast variable, phát tới executor (torrent-style,
   executor chia sẻ mảnh cho nhau — không phải driver gửi N lần từ đầu);
   task bảng lớn build/probe hash table NGAY TRONG stage hiện tại — không stage mới
```

Physical plan đọc thế này — học thuộc hình dạng:

```
SortMergeJoin [order_id], [order_id], Inner
:- Sort [order_id ASC]
:  +- Exchange hashpartitioning(order_id, 200)        ← shuffle phía trái
:     +- Filter isnotnull(order_id)                   ← Spark TỰ thêm cho inner join!
:        +- Scan csv [order_id, ...]
+- Sort [order_id ASC]
   +- Exchange hashpartitioning(order_id, 200)        ← shuffle phía phải
      +- Filter isnotnull(order_id)
         +- Scan csv ...

BroadcastHashJoin [order_id], [order_id], Inner, BuildRight
:- Scan csv [...]                                     ← KHÔNG Exchange phía lớn
+- BroadcastExchange HashedRelationBroadcastMode      ← phía nhỏ bị broadcast
   +- Scan csv [...]
```

Chi tiết thú vị ở dòng `Filter isnotnull(order_id)`: với inner join, null không bao giờ khớp nên Catalyst TỰ chèn filter bỏ null trước shuffle — đỡ được phần shuffle rác, nhưng đừng vì thế quên mìn null ở left join (dòng null vẫn phải giữ).

---

## 5. API

### `df.join(other, on, how)`

```python
orders.join(items, on="order_id", how="inner")           # cột trùng tên: string/list — gọn nhất
orders.join(cust, orders.customer_id == cust.customer_id, "left")  # điều kiện tường minh
items.join(prods, ["product_id"], "left_semi")
```
- **Pitfall 1**: join bằng ĐIỀU KIỆN (cách 2) giữ CẢ HAI cột trùng tên → `Reference 'customer_id' is ambiguous` khi select sau đó. Join bằng string/list tự gộp cột key làm một. Quy tắc: cùng tên → dùng list; khác tên → điều kiện + drop/rename ngay sau join.
- **Pitfall 2**: self-join hoặc join 2 df cùng nguồn lineage → điều kiện `df.a == df2.a` mơ hồ. Fix: `alias` hai phía — `a = df.alias("a"); b = df.alias("b"); a.join(b, F.col("a.k") == F.col("b.k"))`.
- **Pitfall 3**: `how` gõ sai không phải lúc nào cũng nổ sớm — dùng đúng tên chuẩn: `inner, left, right, full, left_semi, left_anti, cross`.

### `F.broadcast(df)` — hint quyền lực nhất khóa học

```python
from pyspark.sql import functions as F
facts.join(F.broadcast(dims), "product_id")             # ÉP broadcast dims, bỏ qua threshold
facts.join(items.hint("merge"), "order_id")             # ép SMJ
facts.join(items.hint("shuffle_hash"), "order_id")      # ép SHJ
```
- **Ý nghĩa**: hint cho physical planner. `broadcast` mạnh nhất — vượt cả threshold, chỉ chịu thua khi loại join không hỗ trợ (full outer không broadcast được cả 2 phía).
- **Khi dùng**: Spark ước size sai (sau filter mạnh, sau agg — plan chưa biết kết quả nhỏ); dim 50–200MB mà cluster memory dư dả và bạn ĐÃ đo.
- **Pitfall chí mạng**: broadcast bảng không-nhỏ-thật → OOM executor + nghẽn driver, lỗi kiểu `Not enough memory to build the hash relation` / timeout `spark.sql.broadcastTimeout` (mặc định 300s). Hint là bạn GIÀNH quyền và NHẬN trách nhiệm — đo size trước khi ép.

### `spark.sql.autoBroadcastJoinThreshold`

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))  # 64MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")                    # tắt tự động
```
- **Ý nghĩa**: trần size (byte, so với **ước lượng**) để Spark TỰ broadcast. Mặc định `10485760` (10MB).
- **Khi chỉnh**: cluster memory rộng + nhiều dim vài chục MB → nâng; debug/so sánh strategy hoặc ước lượng hay sai → tắt và điều khiển 100% bằng hint.
- **Pitfall**: nâng threshold là quyết định TOÀN session — mọi join đều bị ảnh hưởng, con dim 500MB ước lượng láo thành 30MB sẽ chui lọt. Hint theo từng join an toàn hơn chỉnh config toàn cục.

### `explain()` / `explain(mode)`

```python
joined.explain()                  # physical plan — đọc strategy ở đây
joined.explain(mode="formatted")  # tách node + chi tiết, dễ đọc plan dài
joined.explain(mode="cost")       # xem size ước lượng (sizeInBytes) từng node — debug vì sao không broadcast
```
- **Checklist đọc plan join**: (1) tên node join — BroadcastHashJoin/SortMergeJoin/ShuffledHashJoin/BroadcastNestedLoopJoin/Cartesian?; (2) đếm `Exchange` — phía nào phải shuffle?; (3) `BuildLeft/BuildRight` — bảng nào bị build hash/broadcast?; (4) có `Sort` không?
- **Pitfall**: Spark 3.4 bật AQE mặc định — plan in ra là `AdaptiveSparkPlan isFinalPlan=false`, strategy có thể ĐỔI lúc runtime (SMJ → BHJ khi AQE đo thấy nhỏ). Muốn xem plan cuối: chạy action xong xem trên SQL tab của UI (final plan), hoặc so sánh có/không AQE khi học.

### `crossJoin(other)`

- **Ý nghĩa**: tích Descartes tường minh; Spark bắt gọi đúng tên (hoặc bật `spark.sql.crossJoin.enabled`) để bạn không nổ n×m vì tai nạn.
- **Khi dùng hợp pháp**: bảng nhỏ × bảng nhỏ (sinh lưới ngày × cửa hàng để fill số 0 chẳng hạn).

---

## 6. Demo nhỏ

```
Input:  facts 1M dòng (sinh nhanh) + dims 5 dòng
   ↓    join 2 kiểu: để mặc định (tự BHJ) vs tắt broadcast (thành SMJ)
Output: 2 explain + 2 thời gian — cùng kết quả, khác strategy
```

```python
import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("demo09").master("local[2]")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())

facts = spark.range(0, 1_000_000).withColumn("cat_id", (F.col("id") % 5).cast("int"))
dims = spark.createDataFrame(
    [(0, "books"), (1, "toys"), (2, "sports"), (3, "beauty"), (4, "garden")],
    ["cat_id", "cat_name"])

# ── A. Mặc định: dims bé tí → BroadcastHashJoin ────────────────────
a = facts.join(dims, "cat_id")
a.explain()          # tìm: BroadcastHashJoin ... BuildRight, KHÔNG Exchange phía facts
t0 = time.time(); a.count(); print(f"BHJ: {time.time()-t0:.2f}s")

# ── B. Tắt auto-broadcast → SortMergeJoin ──────────────────────────
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
b = facts.join(dims, "cat_id")
b.explain()          # tìm: SortMergeJoin + 2 Exchange + 2 Sort
t0 = time.time(); b.count(); print(f"SMJ: {time.time()-t0:.2f}s")

# ── C. Bật lại bằng hint dù threshold đang -1 ──────────────────────
c = facts.join(F.broadcast(dims), "cat_id")
c.explain()          # BroadcastHashJoin quay lại — hint thắng config

# ── D. Mìn duplicate key: dims có key trùng ────────────────────────
dims_dup = dims.union(spark.createDataFrame([(0, "books-DUP")], ["cat_id", "cat_name"]))
d = facts.join(F.broadcast(dims_dup), "cat_id")
print("facts:", facts.count(), "→ sau join dims trùng key:", d.count())
# 1,000,000 → 1,200,000: 200k dòng cat_id=0 mỗi dòng khớp 2 lần. Không lỗi. Chỉ SAI.
spark.stop()
```

Trên máy bạn BHJ thường nhanh hơn SMJ rõ rệt ngay cả ở 1M dòng local — và khoảng cách này NHÂN LÊN theo size + số node thật. Phần D là quả mìn duplicate key phát nổ có kiểm soát: nhìn con số 1.2M một lần để không bao giờ quên kiểm tra grain.

---

## 7. Production Example

Job gold thực tế: làm giàu order items với đủ 4 chiều — mẫu "fact to, dim nhỏ" gặp ở mọi công ty:

```python
def enrich_order_items(items, orders, products, sellers, customers):
    """Fact items (to nhất, grain = order_item) enrich với orders + 3 dim.
    Chiến lược: filter/cắt cột TRƯỚC join; dim nhỏ → broadcast; orders (cùng cỡ fact) → SMJ."""
    delivered = (orders
        .filter(F.col("order_status") == "delivered")        # early filter: bỏ ~3% rác TRƯỚC shuffle
        .select("order_id", "customer_id",                   # cắt cột: shuffle ít byte
                F.to_date("order_purchase_timestamp").alias("purchase_date")))

    return (items
        .select("order_id", "product_id", "seller_id",
                F.col("price").cast("double").alias("price"))
        # 1) fact ⋈ orders: hai bảng cùng cỡ trăm nghìn/tỷ dòng → để SMJ làm việc của nó
        .join(delivered, "order_id")                          # inner: cũng lọc luôn item mồ côi
        # 2) các dim nhỏ (MB) → broadcast tường minh, khỏi phụ thuộc ước lượng
        .join(F.broadcast(products.select("product_id", "product_category_name")),
              "product_id", "left")
        .join(F.broadcast(sellers.select("seller_id", "seller_state")),
              "seller_id", "left")
        .join(F.broadcast(customers.select("customer_id", "customer_state")),
              "customer_id", "left"))
```

Đọc vị từng quyết định: (1) join làm NHỎ dữ liệu (inner với delivered) đứng đầu, join chỉ làm GIÀU (left với dim) đứng sau; (2) dim dùng `left` không phải `inner` — thiếu 1 dòng bảng products không được phép làm bốc hơi doanh thu, null category xử lý ở data quality; (3) broadcast tường minh cả khi Spark "chắc sẽ tự làm" — code nói rõ ý định, không phụ thuộc ước lượng hên xui; (4) mọi bảng đều bị cắt cột trước join. Một job như này viết đúng chạy 10 phút, viết ẩu chạy 3 tiếng — cùng output.

---

## 8. Hands-on Lab

**Mục tiêu**: join 4 bảng Olist, đo shuffle từng strategy trên Spark UI, gỡ 2 quả mìn.

### Bước 1 — tạo `labs/lab09/joins_olist.py`

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab09-joins").getOrCreate()
DATA = "/workspace/data/olist"
read = lambda name: spark.read.csv(f"{DATA}/{name}", header=True)

orders    = read("olist_orders_dataset.csv")
items     = read("olist_order_items_dataset.csv") \
              .withColumn("price", F.col("price").cast("double"))
customers = read("olist_customers_dataset.csv")
sellers   = read("olist_sellers_dataset.csv")
products  = read("olist_products_dataset.csv")

# ── 1. Join 4 bảng — để Spark tự chọn ──────────────────────────────
enriched = (items
    .join(orders.select("order_id", "customer_id", "order_status"), "order_id")
    .join(customers.select("customer_id", "customer_state"), "customer_id")
    .join(sellers.select("seller_id", "seller_state"), "seller_id"))
print("=== Plan tự chọn ==="); enriched.explain()
enriched.groupBy("customer_state").agg(F.round(F.sum("price"), 0).alias("rev")) \
        .orderBy(F.desc("rev")).show(5)

# ── 2. Tắt broadcast → mọi join thành SMJ, đo lại ─────────────────
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
enriched2 = (items
    .join(orders.select("order_id", "customer_id", "order_status"), "order_id")
    .join(customers.select("customer_id", "customer_state"), "customer_id")
    .join(sellers.select("seller_id", "seller_state"), "seller_id"))
print("=== Plan khi tắt broadcast ==="); enriched2.explain()
enriched2.groupBy("customer_state").count().count()      # action để đo shuffle trên UI

# ── 3. Hint từng loại — so 3 plan cùng 1 join ──────────────────────
for hint in ["broadcast", "merge", "shuffle_hash"]:
    df = items.join(sellers.hint(hint), "seller_id")
    print(f"=== hint: {hint} ==="); df.explain()

# ── 4. Mìn 1 — null key: left join có giữ dòng null không? ────────
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")   # bật lại
o_null = orders.withColumn("customer_id",
    F.when(F.rand(seed=1) < 0.05, None).otherwise(F.col("customer_id")))  # tiêm 5% null
inner_n = o_null.join(customers, "customer_id").count()
left_n  = o_null.join(customers, "customer_id", "left").count()
print(f"tổng={o_null.count()} | inner={inner_n} (null bốc hơi) | left={left_n}")

# ── 5. Mìn 2 — grain check + semi/anti ─────────────────────────────
dup = items.groupBy("order_id", "order_item_id").count().filter("count > 1")
print("Dòng trùng grain (phải = 0):", dup.count())
orphan = items.join(products, "product_id", "left_anti")     # item có product_id lạ
print("Items mồ côi product:", orphan.count())
paid_orders = orders.join(read("olist_order_payments_dataset.csv"),
                          "order_id", "left_semi")
print(f"Orders: {orders.count()} | có payment (semi): {paid_orders.count()}")

input(">>> Mở :4040 SQL tab — so shuffle của bước 1 vs bước 2. Enter thoát.")
spark.stop()
```

### Bước 2 — chạy

```bash
make run F=labs/lab09/joins_olist.py
```

### Bước 3 — quan sát

1. Plan bước 1: join nào là BHJ (customers/sellers bé) — có `BroadcastExchange`? Join items×orders là gì? (Hai bảng này cũng chỉ vài MB — có thể TẤT CẢ đều BHJ! Nếu vậy, chính bước 2 mới cho bạn thấy SMJ.)
2. **SQL tab**: cùng query, phiên bản tắt broadcast có bao nhiêu `Exchange`, tổng shuffle bao nhiêu MB so với bản BHJ? Ghi 2 con số.
3. AQE: plan có `AdaptiveSparkPlan` — trong UI, final plan bước 2 có bị AQE đổi ngược về BHJ không? (Đây là AQE cứu bạn — lesson 20.)
4. Con số mìn null: `inner` mất đúng ~5% dòng so với `left`? Ghi vào `labs/lab09/NOTES.md`.

---

## 9. Assignment

**Easy** — Viết 5 join khác nhau trên Olist (inner, left, left_semi, left_anti, và một join theo điều kiện khác tên cột — `orders.customer_id == customers.customer_id`). Mỗi cái: 1 câu mô tả nghiệp vụ, số dòng kết quả, và dòng physical plan cho biết strategy.

**Medium** — Ép broadcast đúng lúc: lấy `top_sellers = items.groupBy("seller_id").agg(sum(price)).filter(rev > 10000)` (kết quả nhỏ nhưng Spark khó ước lượng vì nằm sau aggregate). Join `items` với `top_sellers`: (a) tắt auto-broadcast, xem plan — strategy gì?; (b) thêm `F.broadcast(top_sellers)` — plan đổi thế nào?; (c) đo thời gian 2 bản. Kết luận: khi nào ước lượng của Spark mù và hint là bắt buộc?

**Hard** — Câu tỷ dòng (trả lời viết, có số liệu giả định): join `transactions` (5 tỷ dòng, 2TB) với `user_profiles` (200 triệu dòng, 40GB) theo user_id trên cluster 50 executor × 16GB. (a) BHJ được không? Chứng minh bằng số. (b) SHJ — mỗi partition phía profiles cỡ bao nhiêu với 2000 shuffle partitions, build hash table nổi không, rủi ro gì nếu 1% user chiếm 30% giao dịch? (c) SMJ — vì sao là đáp án an toàn, giá phải trả? (d) Bonus: nêu 2 cách thiết kế TRÁNH join này ngay từ đầu (gợi ý: bucketing, denormalize).

**Production Challenge** — Viết `labs/lab09/dq_referential.py`: data quality job kiểm tra toàn vẹn tham chiếu của Olist bằng **left_anti**: items→orders, items→products, items→sellers, orders→customers, reviews→orders. Output một DataFrame tổng kết `(relation, n_orphan, pct_orphan)`, in ra và fail (raise) nếu pct > 1%. Ràng buộc: bảng con chỉ đọc 1 lần; dim nhỏ phải broadcast (chứng minh bằng 1 explain đại diện). Job kiểu này chạy trước MỌI pipeline gold ở công ty tử tế.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| BHJ (dim nhỏ thật) | **Nhanh nhất** | Bảng lớn không shuffle không sort; join ngay trong stage đang chạy |
| SMJ 2 bảng lớn | Ổn định | 2 shuffle + 2 sort nhưng mọi bước spill được — không OOM vì join |
| SHJ | Nhanh hơn SMJ khi hợp lệ | Bỏ sort; nhưng hash table per-partition sợ skew |
| Broadcast bảng 1GB | Thảm họa | Driver gom + phát 1GB × N executor, chiếm memory mọi nơi → OOM/timeout |
| Join key null nhiều | Chậm + sai | Null không khớp (mất dòng inner) nhưng vẫn shuffle, dồn 1 partition → skew |
| Duplicate key 2 phía | Nổ m×n | Kết quả phình → shuffle downstream phình theo; luôn check grain |
| Cắt cột trước join | Nhanh hơn | Shuffle tính bằng BYTE không phải dòng — bớt cột là bớt tiền thật |
| Join nở dữ liệu đặt sớm | Chậm | Mọi phép sau đó xử lý dữ liệu đã nở; đặt join 1-n sau cùng nếu được |

Ba câu hỏi trước MỌI join — khắc lên bàn phím: *"Bảng nào nhỏ, nhỏ THẬT không (bao nhiêu MB)? Key có null/trùng không? Filter/cắt cột đã đứng trước join chưa?"*

---

## 11. Spark UI

**SQL tab** — phòng xử án của join:
- Node join hiện đúng tên strategy: `BroadcastHashJoin` / `SortMergeJoin` / `ShuffledHashJoin`. Metrics `number of output rows` của node join so với 2 input — bắt duplicate explosion bằng mắt (output > cả hai input là có nhân bản).
- `BroadcastExchange`: xem `data size` — bảng bị broadcast thực sự nặng bao nhiêu (và `time to build` hash relation).
- `AdaptiveSparkPlan`: click "Final Plan" — AQE có đổi SMJ→BHJ lúc runtime không.

**Stages tab**: job SMJ có stage shuffle write cho TỪNG bảng; cột Shuffle Write/Read cho biết phía nào nặng. Task duration lệch mạnh trong stage join (median 2s, max 5 phút) = **key skew** — ghi nhớ triệu chứng, thuốc ở lesson 19.

**Executors tab**: sau BHJ lớn, xem memory executors — bảng broadcast chiếm chỗ trên MỌI executor; nhiều dim broadcast cộng dồn là một nguồn OOM âm thầm.

---

## 12. Common Mistakes

1. **Không bao giờ `explain()` join** — bay mù. Mọi join trong code production phải từng được nhìn plan ít nhất một lần.
2. **Broadcast bảng "chắc là nhỏ"** không đo — sau mùa sale bảng dim promotions x50 size → job chết mỗi 23h đêm. Đo size + đặt guard (count/size check) trước hint.
3. **Không check grain trước join** → duplicate explosion, doanh thu nhân đôi. Assert unique key ở phía mình tin là duy nhất.
4. **Quên mìn null**: inner join âm thầm nuốt dòng key null; so sánh count trước/sau join phải là phản xạ.
5. **Dùng inner + dropDuplicates thay cho left_semi** — vừa sai ngữ nghĩa tiềm ẩn vừa tốn: kéo cột không cần, nhân bản rồi lại khử.
6. **Join xong mới filter/cắt cột** — shuffle cả những thứ sắp vứt. Early filter + select trước join.
7. **Nâng `autoBroadcastJoinThreshold` toàn cục lên 512MB "cho nhanh"** — mọi join trong app thành ứng viên broadcast, OOM xổ số. Hint từng join thay vì config cả session.
8. **Thấy `CartesianProduct`/`BroadcastNestedLoopJoin` trong plan mà không giật mình** — thường do quên điều kiện join, hoặc điều kiện non-equi vô tình. Hai tên này + bảng lớn = job không bao giờ xong.

---

## 13. Interview

**Junior:**

1. *Kể các loại join và giải thích left_semi, left_anti.* — inner/left/right/full/cross quen thuộc; left_semi: lọc bảng trái theo SỰ TỒN TẠI key ở bảng phải, chỉ trả cột trái, không nhân bản (EXISTS); left_anti: ngược lại — key KHÔNG tồn tại (NOT EXISTS), chuyên tìm orphan/missing.
2. *Broadcast join là gì, nhanh hơn vì sao?* — Gửi nguyên bảng nhỏ đến mọi executor làm hash table; bảng lớn nằm yên, mỗi task probe tại chỗ. Nhanh vì tránh được shuffle + sort bảng lớn — chi phí đắt nhất của join phân tán.
3. *Điều kiện để Spark TỰ chọn broadcast join?* — Size ước lượng một phía ≤ `spark.sql.autoBroadcastJoinThreshold` (mặc định 10MB); tắt bằng -1; ép thủ công bằng `F.broadcast(df)` bất kể threshold. Nhấn "ước lượng" — sau filter/agg Spark có thể đoán sai cả hai chiều.
4. *Làm sao biết job dùng strategy nào?* — `df.explain()` xem tên node (BroadcastHashJoin/SortMergeJoin/ShuffledHashJoin) hoặc SQL tab trên UI; với AQE nhìn final plan trên UI vì strategy có thể đổi lúc runtime.

**Mid:**

5. *Mô tả cơ chế SortMergeJoin và vì sao nó là mặc định cho 2 bảng lớn.* — Shuffle cả 2 bảng theo hash(key) để dòng cùng key về cùng partition → sort từng phía theo key → merge 2 con trỏ kiểu khóa kéo. Mặc định vì mọi bước (sort, merge) đều spill được xuống disk — không cần cấu trúc nào vừa memory → không OOM vì join, scale ổn định tới hàng tỷ dòng. Giá: 2 shuffle + 2 sort.
6. *ShuffledHashJoin khác SMJ chỗ nào, khi nào hơn, rủi ro gì?* — Cùng bước shuffle, nhưng thay sort bằng build hash table phía nhỏ tại mỗi partition rồi probe — tiết kiệm CPU sort. Hơn khi một phía nhỏ hơn hẳn và partition phía nhỏ vừa memory. Rủi ro: key skew làm 1 partition phình → hash table tràn; mặc định `preferSortMergeJoin=true` nên thường phải hint `shuffle_hash`.
7. *Join key có null: chuyện gì xảy ra với inner và left join? Còn hệ quả performance?* — `null = null` → null nên dòng null không khớp: inner mất dòng lặng lẽ (Catalyst thậm chí tự chèn `isnotnull` filter), left giữ dòng với cột phải null. Performance: khi null phải giữ (left), chúng cùng hash về 1 partition → skew. Xử lý: filter null trước nếu được, hoặc tách nhánh null union lại sau.
8. *Duplicate key explosion là gì, phòng thế nào?* — Key K có m dòng trái × n dòng phải → m×n dòng kết quả; hai bảng grain giao dịch join qua key không duy nhất làm nổ số dòng và sai số liệu. Phòng: xác định grain từng bảng trước khi join, assert unique (`groupBy(key).count().filter(">1")` rỗng), so sánh count trước/sau join, dùng semi join khi chỉ cần lọc tồn tại.

**Senior:**

9. *Join 2 bảng hàng tỷ dòng — anh chọn strategy nào, phân tích thế nào?* — Khung trả lời: (1) BHJ loại ngay — không phía nào vừa memory; (2) mặc định SMJ — an toàn, spill được, đã thế thì tối ưu quanh nó: cắt cột + filter trước shuffle, shuffle partitions hợp lý, AQE bật; (3) SHJ chỉ khi một phía nhỏ hơn hẳn và key phân bố đều — nhanh hơn nhờ bỏ sort nhưng ăn OOM nếu skew; (4) câu hỏi hay hơn là TRÁNH shuffle lặp: hai bảng join nhau hằng ngày theo cùng key → bucketing cả hai theo key (SMJ không cần shuffle/sort nữa), hoặc denormalize ở tầng ingest; (5) kiểm tra skew của key thật trước khi chốt. Senior khác Mid ở chỗ đổi đề bài thay vì chỉ giải đề.
10. *Khi nào anh TẮT auto-broadcast dù nó là tối ưu tốt?* — Khi ước lượng size không tin được: bảng sau nhiều tầng transform/agg, nguồn không có statistics (CSV/JSON), hoặc size dữ liệu biến động mạnh theo mùa — auto broadcast hôm nay đúng, ngày sale thành bom OOM. Chiến lược production: threshold để mặc định hoặc tắt, broadcast TƯỜNG MINH bằng hint tại từng join đã đo đạc, kèm guard về size; và dựa vào AQE (đo size runtime) làm lưới an toàn thứ hai. Ý ăn điểm: config toàn cục là cam kết cho MỌI join — hint là quyết định cục bộ có chủ.

---

## 14. Summary

### Mindmap

```
                          LESSON 9 — JOINS
                                │
    ┌──────────────┬────────────┼──────────────────┬───────────────────┐
    ▼              ▼            ▼                  ▼                   ▼
 LOẠI JOIN      3 STRATEGY   CHỌN/ÉP            2 QUẢ MÌN           TỶ DÒNG
    │              │            │                  │                   │
 inner/left/    BHJ: không   threshold 10MB     null key:           BHJ loại,
 right/full     shuffle bảng (ước lượng!)       không khớp,         SMJ mặc định
 semi = EXISTS  lớn          F.broadcast()      dồn 1 partition     (spill được)
 anti = NOT     SMJ: shuffle hint merge/        dup key:            SHJ nếu 1 phía
 EXISTS         +sort, an    shuffle_hash       m×n explosion       nhỏ hẳn + đều
 cross = khai   toàn nhất    explain() đọc      → check grain       né shuffle:
 báo tường minh SHJ: hash    plan TRƯỚC khi     count trước/sau     bucketing/
                per-part     tin                                    denormalize
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ được diagram BHJ và SMJ từ trí nhớ, nói rõ cái gì di chuyển, cái gì không.
- [ ] Thuộc điều kiện chọn của 3 strategy + threshold 10MB là so với size ƯỚC LƯỢNG.
- [ ] Đọc explain: chỉ ra node join, đếm Exchange, biết BuildLeft/BuildRight nghĩa gì.
- [ ] Giải thích được 2 quả mìn (null key, duplicate explosion) kèm cách phòng.
- [ ] Dùng đúng left_semi/left_anti thay cho các mánh inner+dropDuplicates.
- [ ] Đã chạy lab: so shuffle BHJ vs SMJ bằng số liệu trên UI của chính mình.
- [ ] Trả lời được câu "join 2 bảng tỷ dòng" theo khung 5 bước.

---

## 15. Next Lesson

**Lesson 10 — Window Functions.**

GroupBy trả lời "tổng của nhóm là bao nhiêu" — nhưng nó ĐÈ BẸP nhóm thành 1 dòng. Còn cả họ câu hỏi groupBy bó tay: *đơn này là đơn thứ mấy của khách? Doanh thu hôm nay so với hôm qua? Top 3 sản phẩm mỗi danh mục? Tổng cộng dồn từ đầu tháng?* — tất cả cần nhìn nhóm mà **giữ nguyên từng dòng**. Đó là đất của window function: `partitionBy` + `orderBy` + frame. Ta sẽ mổ frame spec (`rowsBetween` vs `rangeBetween` — 90% người dùng không phân biệt được), bộ ba row_number/rank/dense_rank, lag/lead, và bài toán chi phí: mỗi window = 1 shuffle + 1 sort, nhưng nhiều window khôn khéo chung partition spec thì chỉ trả tiền một lần.

Window function là thứ tách analyst giỏi khỏi analyst thường — và DE thì phải giỏi hơn analyst.

> Gõ **"Continue"** khi sẵn sàng.
