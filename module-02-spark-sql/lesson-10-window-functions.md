# Lesson 10 — Window Functions

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 5 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Window function** — tính toán trên một "cửa sổ" các dòng liên quan mà **giữ nguyên số dòng** (khác groupBy đè bẹp nhóm).
- Ba mảnh ghép của window spec: `partitionBy` / `orderBy` / frame (`rowsBetween` vs `rangeBetween` — vẽ diagram phân biệt).
- Bộ ranking: `row_number` vs `rank` vs `dense_rank` — khác nhau đúng 1 chi tiết, sai là sai báo cáo.
- `lag`/`lead` (nhìn dòng trước/sau), running total, `ntile`, `first`/`last` (và cái bẫy frame mặc định của `last`).
- Chi phí: **1 window = 1 shuffle + 1 sort**; nhiều window **cùng partition spec chỉ tốn 1 shuffle**.

Sau bài này bạn phải làm được:

- Chọn đúng công cụ khi nghe yêu cầu: "tổng theo nhóm" → groupBy; "so mỗi dòng với nhóm của nó" → window.
- Viết dedup deterministic bằng `row_number` — thay thế vĩnh viễn cho `dropDuplicates` khi cần "dòng mới nhất".
- Nhìn plan có 2 node `Window` và nói ngay có mấy shuffle, gộp được không.

Kiến thức dùng trong thực tế: dedup lấy bản ghi mới nhất (CDC — lesson 29 sống bằng nó), top-N mỗi nhóm, so sánh kỳ trước/kỳ này, running total, sessionization — 5 pattern này phủ phần lớn yêu cầu analytics mà groupBy không với tới.

---

## 2. Why

### Vấn đề: groupBy trả lời được "bao nhiêu", không trả lời được "dòng này đứng đâu trong nhóm"

Yêu cầu từ business: *"mỗi đơn hàng, cho biết nó là đơn thứ mấy của khách đó và cách đơn trước bao nhiêu ngày"*. Thử bằng đồ nghề hiện có:

```python
# groupBy? Đè bẹp mỗi khách còn 1 dòng — mất luôn từng đơn hàng. Sai đề.
orders.groupBy("customer_id").agg(F.count("*"), F.min("purchase_date"))

# Mánh join lại: đếm bằng groupBy rồi join ngược vào orders?
# → được "tổng số đơn của khách", vẫn KHÔNG được "đơn này là đơn THỨ MẤY".
# Muốn thứ tự phải self-join theo điều kiện date <= date — nổ O(n²) trong nhóm. Đường cùng.
```

Window function giải bằng một nhát:

```python
w = Window.partitionBy("customer_id").orderBy("purchase_date")
orders.withColumn("order_seq", F.row_number().over(w)) \
      .withColumn("days_since_prev",
                  F.datediff("purchase_date", F.lag("purchase_date", 1).over(w)))
```

Mỗi dòng vẫn là một đơn hàng — nhưng giờ nó "nhìn thấy" các đơn anh em cùng khách, biết mình đứng thứ mấy, biết thằng đứng trước mình là ai.

> **Analogy xếp hàng chào cờ**: groupBy là thầy hiệu trưởng đứng trên bục hỏi "lớp 9A bao nhiêu bạn?" — cả lớp thành MỘT con số. Window là mỗi học sinh đứng trong hàng của lớp mình (partitionBy), theo thứ tự chiều cao (orderBy), và tự biết: mình đứng thứ 5 (row_number), bạn ngay trước mình cao 1m6 (lag), tổng chiều cao từ đầu hàng đến mình (running total). Không ai phải rời hàng — **số dòng giữ nguyên**.

### groupBy vs window — bảng đối chiếu phải thuộc

| | `groupBy().agg()` | Window function |
|---|---|---|
| Số dòng output | **Collapse**: 1 dòng/nhóm | **Giữ nguyên**: mỗi dòng input là 1 dòng output |
| Trả lời | "Nhóm này tổng/đếm/trung bình bao nhiêu?" | "Dòng này đứng đâu/so với ai trong nhóm?" |
| Có thứ tự trong nhóm? | Không có khái niệm thứ tự | `orderBy` là công dân hạng nhất |
| Chi phí | Shuffle (được partial agg cứu) | Shuffle + **SORT** (không có partial cho ranking) |
| Ví dụ | Doanh thu theo seller | Top 3 đơn lớn nhất MỖI seller, kèm đủ cột |

### Nếu không có window function thì sao?

Bạn sẽ thấy trong code legacy đủ mánh đau khổ: self-join theo bất đẳng thức (O(n²) mỗi nhóm), groupBy rồi join ngược (2 shuffle + không có thứ tự), `collect_list` rồi UDF xử lý mảng (OOM chờ sẵn + mất Catalyst). Window function tồn tại để 3 mánh này vào viện bảo tàng.

---

## 3. Theory

### 3.1. Giải phẫu một window spec

```python
from pyspark.sql import Window
w = (Window
     .partitionBy("customer_id")        # ① chia dữ liệu thành các nhóm độc lập
     .orderBy(F.col("purchase_date"))   # ② sắp thứ tự TRONG mỗi nhóm
     .rowsBetween(Window.unboundedPreceding, Window.currentRow))  # ③ frame: dòng nào được "nhìn thấy"
```

```
            partitionBy("customer_id")             orderBy("purchase_date")
   ┌────────────────────────────────────┐
   │ partition: customer = C1           │      frame của DÒNG HIỆN TẠI (▼):
   │  ┌──────┬──────┬──────┬──────┐     │
   │  │ 01/03│ 15/03│ 02/04│ 20/04│     │      rowsBetween(unboundedPreceding, currentRow)
   │  └──────┴──────┴──────┴──────┘     │      ┌──────┬──────┬──▼───┐
   ├────────────────────────────────────┤      │ 01/03│ 15/03│ 02/04│ ← 3 dòng trong frame
   │ partition: customer = C2           │      └──────┴──────┴──────┘  (20/04 KHÔNG thấy)
   │  ┌──────┬──────┐                   │
   │  │ 10/01│ 05/06│  ← C2 không bao   │      hàm aggregate chạy TRÊN FRAME này
   │  └──────┴──────┘    giờ thấy C1    │      → sum = running total đến dòng hiện tại
   └────────────────────────────────────┘
```

Ba tầng, mỗi tầng một câu hỏi: **partitionBy** — ai cùng nhóm với tôi? **orderBy** — trong nhóm, xếp theo gì? **frame** — từ vị trí của tôi, tôi được nhìn những dòng nào?

### 3.2. rowsBetween vs rangeBetween — khác nhau ở đơn vị đo

Cùng cú pháp `(start, end)` với các mốc `Window.unboundedPreceding` (−∞), `Window.currentRow` (0), `Window.unboundedFollowing` (+∞), hoặc số nguyên. Nhưng:

- **rowsBetween** đếm bằng **SỐ DÒNG vật lý**: `rowsBetween(-2, 0)` = 2 dòng đứng trước + dòng này.
- **rangeBetween** đo bằng **GIÁ TRỊ của cột orderBy**: `rangeBetween(-6, 0)` với orderBy(day_number) = mọi dòng có giá trị trong đoạn [giá_trị_hiện_tại − 6, giá_trị_hiện_tại].

```
Dữ liệu:  (day=1, 100) (day=2, 50) (day=2, 30) (day=5, 70)   ← chú ý: day=2 có 2 dòng, day=3,4 trống

ROWS  between (-1, 0), dòng hiện tại = (day=5,70):
      ┌────────┬───────▼┐
      │(2, 30) │ (5, 70)│   → sum = 100   (đúng 1 dòng liền trước, bất kể day cách 3 ngày)
      └────────┴────────┘

RANGE between (-1, 0), dòng hiện tại = (day=5,70):
      frame = mọi dòng có day ∈ [4, 5]
      ┌───────▼┐
      │ (5, 70)│            → sum = 70    (day=4 không có dòng nào — frame chỉ có chính nó)
      └────────┘

RANGE between (-1, 0), dòng hiện tại = (day=2,50):
      frame = day ∈ [1, 2] → CẢ (1,100), (2,50) VÀ (2,30) — dòng CÙNG GIÁ TRỊ orderBy
      → sum = 180           (range gom cả "hàng xóm trùng giá trị", rows thì không)
```

Hai hệ quả thực dụng: (1) "trung bình trượt 7 dòng gần nhất" → **rows**; "trung bình trượt 7 NGÀY theo lịch" (kể cả ngày trống) → **range** trên cột số/ngày đã đổi ra số (`F.unix_timestamp`, `F.datediff`); (2) orderBy có giá trị trùng (ties) → range cho kết quả "cả cụm trùng cùng thấy nhau" — đôi khi là chính xác về nghiệp vụ, đôi khi là bug, phải CHỌN có ý thức.

Ràng buộc: `rangeBetween` với offset số đòi orderBy đúng **một cột kiểu số/ngày**; rows thì orderBy gì cũng được.

### 3.3. Frame mặc định — nguồn bug số 1 của last()

Quy tắc chuẩn SQL mà Spark theo:

- Window **có orderBy, không khai frame** → mặc định `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — frame "từ đầu đến tôi".
- Window **không orderBy** → frame = **cả partition**.

Hệ quả kinh điển: `F.last("x").over(Window.partitionBy(k).orderBy(d))` KHÔNG trả "giá trị cuối của nhóm" — frame mặc định dừng ở current row, nên "last của từ-đầu-đến-tôi" chính là... dòng hiện tại. Muốn last thật:

```python
w_full = (Window.partitionBy("k").orderBy("d")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
F.last("x").over(w_full)      # giờ mới là giá trị cuối theo d của cả nhóm
```

Cũng frame mặc định này làm `F.sum().over(w có orderBy)` tự nhiên thành **running total** — tiện, nhưng hãy hiểu VÌ SAO nó chạy vậy chứ đừng thuộc lòng như phép màu.

### 3.4. row_number vs rank vs dense_rank — một ví dụ nói hết

Điểm số: 100, 90, 90, 80 (orderBy desc):

```
giá trị    row_number   rank   dense_rank
 100           1          1        1
  90           2          2        2      ← đồng hạng
  90           3          2        2      ← row_number VẪN tách (2,3) — tie-break không xác định!
  80           4          4        3      ← rank NHẢY CÓC (bỏ hạng 3), dense_rank đi liền
```

- `row_number`: số thứ tự duy nhất, không quan tâm đồng hạng — **dedup, top-N đúng-N-dòng**. Cẩn thận: hai dòng bằng nhau thì đứa nào số 2 đứa nào số 3 là **không xác định** → thêm cột phá hòa vào orderBy (id, timestamp) để deterministic.
- `rank`: đồng hạng cùng số, nhảy cóc sau đó (kiểu xếp hạng Olympic — 2 HC vàng thì không có HC bạc).
- `dense_rank`: đồng hạng cùng số, không nhảy — "top 3 GIÁ TRỊ khác nhau".
- `ntile(n)`: chia partition thành n rổ đều nhau — percentile thô, phân hạng khách A/B/C/D.
- Cả họ ranking **bắt buộc orderBy** và **không nhận custom frame** (frame luôn là toàn partition về mặt logic).

### 3.5. lag / lead — nhìn hàng xóm

```python
F.lag("price", 1).over(w)            # giá trị của DÒNG TRƯỚC theo orderBy (offset 1)
F.lag("price", 1, 0.0).over(w)       # dòng đầu không có dòng trước → default 0.0 thay vì null
F.lead("price", 1).over(w)           # dòng SAU
```

Cặp bài trùng của time-series: chênh lệch ngày này vs ngày trước, phát hiện gap (ngày đứt quãng), tính thời gian giữa 2 sự kiện liên tiếp (chính là nguyên liệu sessionization ở Module 4). Dòng biên nhận `null` — luôn quyết định tường minh: giữ null, default, hay filter.

### 3.6. Chi phí: window = shuffle + sort; và mánh gộp

Để mọi dòng cùng partition-key đứng cạnh nhau ĐÚNG THỨ TỰ, Spark phải: shuffle theo `partitionBy` rồi **sort trong partition** theo (partitionKey, orderBy). Không có "partial window" như partial aggregate — sort là sort.

```
  2 window CÙNG spec (partitionBy customer, orderBy date):
      Window [row_number, lag] ← 2 hàm tính chung   → 1 Exchange + 1 Sort   ✅

  2 window KHÁC spec:
      Window [rank]  partitionBy(seller)  orderBy(revenue)
      +- Window [row_number] partitionBy(customer) orderBy(date)
                                                    → 2 Exchange + 2 Sort   💸💸
```

Catalyst tự gom các hàm **cùng (partitionBy, orderBy, frame tương thích)** vào một node Window. Kỹ năng của bạn: **thiết kế các phép tính xoay quanh ít window spec nhất có thể** — đổi 5 spec lệch nhau thành 2 spec chuẩn thường tiết kiệm quá nửa giờ chạy. Và một biến thể nguy hiểm: `Window.orderBy("x")` KHÔNG partitionBy → toàn bộ dữ liệu dồn về **1 partition duy nhất, 1 task, 1 executor** — Spark còn in cảnh báo `WindowExec: No Partition Defined`. Global ranking bảng lớn cần cách khác (sort toàn cục + zipWithIndex, hoặc chấp nhận xấp xỉ).

---

## 4. Internal

Đường đi của `row_number().over(Window.partitionBy("customer_id").orderBy("d"))`:

```
① Logical plan: Window [row_number() ... PARTITION BY customer_id ORDER BY d]
        │
② Physical: WindowExec đòi hỏi input:
   - phân vùng theo customer_id  → chèn Exchange hashpartitioning(customer_id, 200)
   - sort trong partition theo (customer_id, d) → chèn Sort [customer_id, d]
        │
③ Sau shuffle+sort, mỗi task quét partition TUẦN TỰ:
   - gặp customer mới → reset trạng thái (bộ đếm, buffer frame)
   - cùng customer → dòng nối tiếp nhau đã đúng thứ tự d
   - row_number chỉ là bộ đếm ++; sum/avg giữ buffer theo frame:
       frame chỉ NỞ (unboundedPreceding → currentRow): cộng dồn — O(1)/dòng
       frame TRƯỢT (rows -3..0): cấu trúc trượt thêm/bớt phần tử
       frame FULL partition (first/last unbounded): tính 1 lần cho cả nhóm
        │
④ Buffer của partition hiện hành phải nằm trong memory khi xử lý —
   partition KHỔNG LỒ (skew: 1 customer chiếm 30% bảng, hoặc quên partitionBy)
   → task rùa + spill/OOM. Window kế thừa mọi bệnh của shuffle + sort.
```

Trong `explain()`:

```
Window [row_number() windowspecdefinition(customer_id, d ASC, specifiedwindowframe(...)) AS seq]
+- Sort [customer_id ASC, d ASC], false, 0
   +- Exchange hashpartitioning(customer_id, 200)      ← đây, 1 shuffle
      +- Scan ...
```

Đếm số cặp `Exchange + Sort` phía trên các node `Window` = số lần bạn trả tiền. Hai node Window chồng nhau mà chỉ một Exchange bên dưới = Catalyst đã gộp hoặc tận dụng được thứ tự sẵn có (ví dụ spec thứ hai cùng partitionBy, orderBy là prefix) — plan đẹp.

---

## 5. API

### `Window.partitionBy(...).orderBy(...)`

```python
from pyspark.sql import Window
w = Window.partitionBy("customer_id").orderBy(F.col("purchase_date").desc())
```
- **Ý nghĩa**: dựng window spec — object mô tả, tái sử dụng cho nhiều hàm.
- **Pitfall**: quên `partitionBy` → 1 task ôm cả bảng (cảnh báo `No Partition Defined`). orderBy nhiều dòng trùng giá trị → thêm cột phá hòa để deterministic.

### `F.row_number() / F.rank() / F.dense_rank() / F.ntile(n)` + `.over(w)`

```python
df.withColumn("rn", F.row_number().over(w))
df.withColumn("quartile", F.ntile(4).over(Window.partitionBy("state").orderBy("revenue")))
```
- **Khi dùng**: dedup + top-N (`row_number`), bảng xếp hạng tôn trọng đồng hạng (`rank`/`dense_rank`), phân rổ (`ntile`).
- **Pitfall**: thiếu orderBy → AnalysisException. Top-N: filter `rn <= 3` phải ở **DataFrame mới** sau withColumn — không nhét điều kiện vào window được.

### `F.lag(col, offset, default) / F.lead(col, offset, default)`

```python
df.withColumn("prev_price", F.lag("price", 1).over(w))
df.withColumn("price_diff", F.col("price") - F.lag("price", 1).over(w))
```
- **Pitfall**: dòng đầu partition → null lan truyền vào phép trừ (null - x = null). Quyết định tường minh bằng default hoặc `coalesce`. Lag theo "kỳ trước" chỉ đúng khi mỗi kỳ đúng 1 dòng — dữ liệu ngày thiếu ngày thì lag(1) là "dòng trước" chứ không phải "hôm qua" (đó là việc của rangeBetween hoặc fill lịch).

### `F.sum/avg/min/max/count(...).over(w)` — aggregate làm window

```python
running = Window.partitionBy("customer_id").orderBy("d") \
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("cum_spend", F.sum("price").over(running))
df.withColumn("pct_of_customer", F.col("price") / F.sum("price").over(Window.partitionBy("customer_id")))
```
- **Ý nghĩa**: mọi hàm aggregate đều `.over()` được. Không orderBy → tính trên cả nhóm và DÁN vào từng dòng (dòng 2 ở ví dụ: tỷ trọng đơn này trên tổng chi của khách — khỏi groupBy + join ngược).
- **Pitfall**: có orderBy mà quên khai frame → frame mặc định RANGE-to-current-row: sum thành running sum (có khi ngoài ý muốn), và ties cùng thấy nhau. Muốn kết quả cả nhóm nhưng vẫn cần orderBy cho hàm khác → tách 2 spec.

### `F.first(col, ignorenulls) / F.last(col)` + `rowsBetween/rangeBetween`

```python
w_full = Window.partitionBy("k").orderBy("d") \
               .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df.withColumn("latest_status", F.last("status").over(w_full))
df.withColumn("last_known", F.last("val", ignorenulls=True).over(
    Window.partitionBy("k").orderBy("d")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)))   # forward-fill!
```
- **Khi dùng**: dán giá trị đầu/cuối nhóm lên mọi dòng; `last(ignorenulls=True)` + frame đến currentRow = **forward fill** null theo thời gian — pattern vá sensor data kinh điển.
- **Pitfall**: `last` với frame mặc định = chính dòng hiện tại (mục 3.3) — bug im lặng phổ biến nhất của window.

---

## 6. Demo nhỏ

```
Input:  7 giao dịch của 2 khách (tạo tay, có đồng hạng giá)
   ↓    1 window spec dùng chung: row_number, rank, dense_rank, lag, running sum
Output: show() một bảng — nhìn cả 5 cột cạnh nhau là hiểu cả bài
```

```python
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("demo10").master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()

data = [("C1", "2024-01-05", 100.0), ("C1", "2024-01-20", 90.0),
        ("C1", "2024-02-02", 90.0),  ("C1", "2024-03-11", 80.0),
        ("C2", "2024-01-10", 500.0), ("C2", "2024-04-01", 40.0), ("C2", "2024-04-09", 40.0)]
df = spark.createDataFrame(data, ["customer", "d", "price"]) \
          .withColumn("d", F.to_date("d"))

by_price = Window.partitionBy("customer").orderBy(F.col("price").desc())
by_time  = Window.partitionBy("customer").orderBy("d")
running  = by_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)

out = (df
    .withColumn("rn",    F.row_number().over(by_price))
    .withColumn("rnk",   F.rank().over(by_price))
    .withColumn("drnk",  F.dense_rank().over(by_price))
    .withColumn("prev",  F.lag("price", 1).over(by_time))
    .withColumn("cum",   F.sum("price").over(running)))
out.orderBy("customer", "d").show()
# +--------+----------+-----+---+---+----+-----+-----+
# |customer|         d|price| rn|rnk|drnk| prev|  cum|
# +--------+----------+-----+---+---+----+-----+-----+
# |      C1|2024-01-05|100.0|  1|  1|   1| null|100.0|
# |      C1|2024-01-20| 90.0|  2|  2|   2|100.0|190.0|
# |      C1|2024-02-02| 90.0|  3|  2|   2| 90.0|280.0|  ← rn tách 2/3, rnk & drnk đồng hạng 2
# |      C1|2024-03-11| 80.0|  4|  4|   3| 90.0|360.0|  ← rnk nhảy cóc 4, drnk đi liền 3
# |      C2|2024-01-10|500.0|  1|  1|   1| null|500.0|
# |      C2|2024-04-01| 40.0|  2|  2|   2|500.0|540.0|
# |      C2|2024-04-09| 40.0|  3|  2|   2| 40.0|580.0|
# +--------+----------+-----+---+---+----+-----+-----+

out.explain()   # đếm Exchange: 2 (by_price và by_time khác orderBy → nhưng CÙNG partitionBy!
                # Spark chỉ shuffle 1 lần theo customer, sort 2 lần — soi plan tự kiểm chứng)
spark.stop()
```

Một bảng 7 dòng chứa cả bài học: `rn` vs `rnk` vs `drnk` tại cặp 90.0; `prev` null ở dòng đầu; `cum` cộng dồn theo thời gian; và plan cho thấy tiền shuffle/sort bạn đã trả.

---

## 7. Production Example

Pattern ăn cơm hằng ngày của DE — **dedup lấy bản ghi mới nhất** (đây chính là trái tim của CDC merge ở Module 4, và là thứ `dropDuplicates` KHÔNG làm được vì không deterministic):

```python
def latest_per_key(df, key_cols, ts_col, tie_breaker):
    """Mỗi key giữ đúng 1 dòng MỚI NHẤT theo ts_col, deterministic nhờ tie_breaker.
    Dùng cho: khử trùng lặp ingest, chọn bản ghi hiện hành từ CDC event stream."""
    w = Window.partitionBy(*key_cols).orderBy(
        F.col(ts_col).desc(), F.col(tie_breaker).desc())      # phá hòa → chạy 2 lần ra cùng 1 kết quả
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))

# Và báo cáo "top 3 sản phẩm mỗi danh mục + tỷ trọng" — 2 phép, 1 lần shuffle theo category:
def top3_with_share(items_products):
    by_rev   = Window.partitionBy("category").orderBy(F.col("revenue").desc(),
                                                      F.col("product_id"))     # tie-break!
    cat_total = Window.partitionBy("category")                                  # cùng partitionBy
    return (items_products
        .groupBy("category", "product_id").agg(F.sum("price").alias("revenue"))
        .withColumn("rank_in_cat", F.row_number().over(by_rev))
        .withColumn("share", F.round(F.col("revenue") / F.sum("revenue").over(cat_total), 3))
        .filter(F.col("rank_in_cat") <= 3))
```

Điểm Senior: (1) tie-breaker trong orderBy — thiếu nó, hai lần chạy có thể giữ 2 dòng khác nhau, pipeline "không tái lập được" là ác mộng debug; (2) `by_rev` và `cat_total` cùng `partitionBy("category")` — Spark shuffle theo category MỘT lần cho cả hai cột; (3) groupBy đứng TRƯỚC window: gom về grain (category, product) nhỏ rồi mới window trên dữ liệu đã nhỏ — phối hợp 2 công cụ chứ không chọn phe.

---

## 8. Hands-on Lab

**Mục tiêu**: chạy đủ họ window function trên Olist, đếm shuffle bằng plan, chứng minh luật "chung spec = chung shuffle".

### Bước 1 — tạo `labs/lab10/windows_olist.py`

```python
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("lab10-windows").getOrCreate()
DATA = "/workspace/data/olist"

orders = (spark.read.csv(f"{DATA}/olist_orders_dataset.csv", header=True)
          .filter(F.col("order_status") == "delivered")
          .select("order_id", "customer_id",
                  F.to_date("order_purchase_timestamp").alias("d")))
items = (spark.read.csv(f"{DATA}/olist_order_items_dataset.csv", header=True)
         .select("order_id", "seller_id", "product_id",
                 F.col("price").cast("double").alias("price")))

# ── 1. Hành vi khách: thứ tự đơn, ngày cách đơn trước, chi tiêu cộng dồn ──
by_time = Window.partitionBy("customer_id").orderBy("d", "order_id")   # order_id phá hòa
cust = (orders.join(items, "order_id")
    .groupBy("customer_id", "order_id", "d").agg(F.sum("price").alias("order_value"))
    .withColumn("order_seq", F.row_number().over(by_time))
    .withColumn("days_since_prev", F.datediff("d", F.lag("d", 1).over(by_time)))
    .withColumn("cum_spend", F.round(F.sum("order_value").over(
        by_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2)))
cust.filter(F.col("order_seq") >= 2).show(10)
print("=== plan #1: by_time dùng cho 3 hàm — đếm Exchange ==="); cust.explain()

# ── 2. Xếp hạng seller: row_number vs rank vs dense_rank + ntile ──
sellers = items.groupBy("seller_id").agg(F.round(F.sum("price"), 2).alias("revenue"))
by_rev = Window.orderBy(F.col("revenue").desc())        # CỐ Ý không partitionBy — xem cảnh báo!
ranked = (sellers
    .withColumn("rn",   F.row_number().over(by_rev))
    .withColumn("rnk",  F.rank().over(by_rev))
    .withColumn("drnk", F.dense_rank().over(by_rev))
    .withColumn("tier", F.ntile(4).over(by_rev)))
ranked.show(10)          # để ý log: WARN WindowExec: No Partition Defined... (bảng seller nhỏ nên sống)

# ── 3. Running revenue theo tháng + so tháng trước (lag) ──
monthly = (orders.join(items, "order_id")
    .groupBy(F.date_format("d", "yyyy-MM").alias("month"))
    .agg(F.round(F.sum("price"), 0).alias("rev")))
by_month = Window.orderBy("month")     # 1 dòng/tháng, vài chục dòng — no-partition chấp nhận được
trend = (monthly
    .withColumn("prev_rev", F.lag("rev", 1).over(by_month))
    .withColumn("mom_pct", F.round((F.col("rev") - F.col("prev_rev")) / F.col("prev_rev") * 100, 1))
    .withColumn("rolling_3m", F.round(F.avg("rev").over(
        by_month.rowsBetween(-2, Window.currentRow)), 0)))
trend.show(30)

# ── 4. rows vs range trên cùng dữ liệu — nhìn số khác nhau ──
daily = (orders.join(items, "order_id")
    .groupBy("d").agg(F.sum("price").alias("rev"))
    .withColumn("day_n", F.datediff("d", F.lit("2016-01-01"))))
w_rows  = Window.orderBy("day_n").rowsBetween(-6, 0)
w_range = Window.orderBy("day_n").rangeBetween(-6, 0)
cmp = (daily
    .withColumn("sum_7rows", F.round(F.sum("rev").over(w_rows), 0))    # 7 DÒNG gần nhất
    .withColumn("sum_7days", F.round(F.sum("rev").over(w_range), 0)))  # 7 NGÀY lịch (kể ngày trống)
cmp.orderBy("d").show(15)   # tìm chỗ 2 cột lệch nhau → đó là nơi có ngày trống dữ liệu

# ── 5. Dedup deterministic: bản ghi review mới nhất mỗi order ──
reviews = spark.read.csv(f"{DATA}/olist_order_reviews_dataset.csv", header=True)
w_dedup = Window.partitionBy("order_id") \
                .orderBy(F.col("review_answer_timestamp").desc(), F.col("review_id"))
latest_review = (reviews.withColumn("_rn", F.row_number().over(w_dedup))
                        .filter("_rn = 1").drop("_rn"))
print(f"reviews: {reviews.count()} → sau dedup theo order: {latest_review.count()}")

input(">>> Mở :4040 SQL tab — đếm Exchange+Sort dưới các node Window. Enter thoát.")
spark.stop()
```

### Bước 2 — chạy

```bash
make run F=labs/lab10/windows_olist.py
```

### Bước 3 — quan sát

1. Plan #1: ba hàm (`row_number`, `lag`, `sum`) chung `by_time` → bao nhiêu node Window, bao nhiêu Exchange? (Kỳ vọng: cụm Window chung 1 Exchange theo customer_id.)
2. Log console bước 2: dòng `WARN WindowExec: No Partition Defined for Window operation` — với bảng seller ~3k dòng thì sống, hình dung bảng 1 tỷ dòng thì sao?
3. Bước 4: tìm ngày mà `sum_7rows ≠ sum_7days`, giải thích bằng ngày trống.
4. Ghi số Exchange đếm được + 3 quan sát vào `labs/lab10/NOTES.md`.

---

## 9. Assignment

**Easy** — Xếp hạng seller theo revenue trong TỪNG bang (`seller_state`): dùng cả `row_number` và `rank` trên cùng window. Tìm ít nhất một bang có 2 cột lệch nhau và giải thích vì sao (gợi ý: revenue trùng). Cột nào bạn dùng nếu đề là "lấy đúng 3 seller mỗi bang"?

**Medium** — Running total: với mỗi khách (`customer_unique_id` — join bảng customers để lấy, vì `customer_id` đổi theo đơn), tính chi tiêu cộng dồn theo thời gian và cột `pct_of_lifetime` = cộng dồn / tổng trọn đời (2 window: một chạy dồn có orderBy+frame, một cả nhóm không orderBy — CÙNG partitionBy). Dán explain, chỉ ra 2 hàm chung mấy Exchange.

**Hard** — 2-window: mỗi (product category, ngày) tính doanh thu, rồi: (a) `diff_vs_prev_day` — chênh lệch so với NGÀY CÓ DỮ LIỆU liền trước trong cùng category (lag); (b) `avg_7d` — trung bình 7 ngày LỊCH trượt trong cùng category (rangeBetween trên cột ngày-đổi-ra-số); (c) `best_day_rank` — hạng của ngày đó trong toàn lịch sử category (dense_rank theo doanh thu). Ba spec này gộp được về mấy lần shuffle? Chứng minh bằng explain và giải thích: điều gì QUYẾT ĐỊNH hai window dùng chung được một Exchange?

**Production Challenge** — Viết `labs/lab10/rfm_segmentation.py`: phân hạng khách hàng RFM trên Olist. Mỗi `customer_unique_id`: Recency (số ngày từ đơn cuối đến max ngày toàn dataset), Frequency (số đơn), Monetary (tổng chi). Dùng `ntile(5)` cho từng chiều → điểm R/F/M 1–5, ghép thành segment (`555` = vàng). Output: bảng segment + số khách + doanh thu mỗi segment, ghi Parquet. Ràng buộc: toàn bộ không UDF, không collect; các ntile không partitionBy — biện luận trong comment vì sao chấp nhận được ở grain khách-hàng (bao nhiêu dòng?) và nêu phương án khi bảng khách 500 triệu dòng.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| 1 window có partitionBy | 1 shuffle + 1 sort | WindowExec đòi dữ liệu cùng key liền kề, đúng thứ tự |
| Nhiều hàm chung 1 spec | Vẫn 1 shuffle | Catalyst gom vào 1 node Window — thiết kế code xoay quanh ít spec |
| Nhiều spec cùng partitionBy khác orderBy | 1 shuffle, nhiều sort | Exchange tái dùng, Sort phải làm lại — vẫn rẻ hơn khác partitionBy |
| Window không partitionBy | **Nguy hiểm** | Cả bảng → 1 partition/1 task; chỉ chấp nhận trên dữ liệu đã aggregate nhỏ |
| Partition key lệch (skew) | Task rùa | 1 khách/1 seller khổng lồ = 1 task ôm cả cụm sort — bệnh của shuffle nói chung |
| Window trên dữ liệu thô vs sau groupBy | Sau groupBy rẻ hơn nhiều | Gom về grain cần thiết trước, window trên bảng đã nhỏ |
| row_number dedup vs dropDuplicates | Đắt hơn chút, ĐÚNG hơn nhiều | Thêm sort nhưng deterministic — production chọn đúng trước, nhanh sau |
| rangeBetween offset lớn | Cẩn thận | Frame tính theo GIÁ TRỊ — cụm giá trị dày đặc = frame chứa rất nhiều dòng |

---

## 11. Spark UI

**SQL tab** — đọc window trong plan:
- Node `Window` liệt kê các hàm nó gánh — nhiều hàm trong 1 node = gộp spec thành công.
- Ngay dưới mỗi cụm Window: `Sort` + `Exchange hashpartitioning(...)`. **Đếm Exchange dưới các Window = số shuffle bạn trả cho window.** Hai Window chồng nhau, một Exchange = Catalyst tái dùng phân vùng (thường do cùng partitionBy).
- `Exchange SinglePartition` dưới Window = bạn quên partitionBy — chính là cảnh báo No Partition Defined hiện hình trong plan.

**Stages tab**: stage chứa WindowExec — nhìn **task duration distribution**: median vs max lệch chục lần = partition key skew (một nhóm quá to). Cột Spill xuất hiện = sort trong window không vừa memory.

**Jobs tab**: chuỗi `withColumn(...over...)` nhiều spec khác nhau sinh chuỗi stage nối đuôi — mỗi stage một shuffle. Số stage tăng theo số spec, không theo số hàm — kiểm chứng luật của bài ngay trên UI.

---

## 12. Common Mistakes

1. **Quên `partitionBy`** trên bảng lớn → toàn bộ dữ liệu về 1 task, chạy mãi không xong. Đọc log, dòng WARN No Partition Defined không phải để trang trí.
2. **`last()`/`max().over` với frame mặc định** — có orderBy là frame dừng ở currentRow: `last` = chính nó. Muốn cả nhóm: khai `rowsBetween(unboundedPreceding, unboundedFollowing)` tường minh.
3. **orderBy không tie-breaker** cho row_number/dedup → kết quả đổi giữa 2 lần chạy, bug "không tái lập được". Luôn thêm cột duy nhất (id) cuối orderBy.
4. **Nhầm rows/range**: "7 dòng gần nhất" ≠ "7 ngày gần nhất" khi dữ liệu thiếu ngày; range với ties gom cả cụm trùng giá trị. Chọn theo NGHIỆP VỤ, không theo quen tay.
5. **Mỗi phép một spec mới** viết tùy hứng (nay partitionBy(a), mai partitionBy(a,b), mốt orderBy khác) → N shuffle trong khi quy hoạch lại còn 1–2. Trước khi viết, liệt kê các spec cần dùng.
6. **Dùng window khi groupBy đủ**: chỉ cần tổng theo nhóm (không cần giữ dòng) mà đi `sum().over(partitionBy)` rồi dropDuplicates — đắt hơn (sort + không partial agg) và lủng củng. Collapse được thì groupBy.
7. **lag(1) tưởng là "kỳ trước"** trên dữ liệu thiếu kỳ — lag đếm DÒNG. Fill lịch đủ kỳ trước, hoặc dùng range.
8. **Filter kết quả rank ngay trong spec** — không có cú pháp đó; phải withColumn rồi filter ở bước sau (Spark chưa có QUALIFY như Snowflake/DuckDB).

---

## 13. Interview

**Junior:**

1. *Window function khác groupBy thế nào?* — groupBy collapse: mỗi nhóm 1 dòng, mất chi tiết. Window giữ nguyên số dòng: mỗi dòng được dán thêm kết quả tính trên "cửa sổ" các dòng liên quan (cùng partition, theo thứ tự, trong frame). "Tổng mỗi nhóm" → groupBy; "dòng này so với nhóm" → window.
2. *row_number, rank, dense_rank khác nhau chỗ nào?* — Với ties: row_number vẫn đánh số duy nhất liên tục (tie-break không xác định nếu orderBy không đủ); rank đồng hạng rồi nhảy cóc (1,2,2,4); dense_rank đồng hạng không nhảy (1,2,2,3). Dedup/top-N đúng N dòng → row_number; xếp hạng tôn trọng đồng hạng → rank/dense_rank.
3. *lag/lead làm gì, dòng biên trả gì?* — Lấy giá trị dòng trước/sau theo orderBy trong cùng partition (offset tùy ý). Dòng đầu không có "trước" → null (hoặc default truyền vào tham số 3). Ứng dụng: chênh lệch kỳ, khoảng cách 2 sự kiện, phát hiện gap.
4. *Ba thành phần của window spec?* — partitionBy (chia nhóm độc lập), orderBy (thứ tự trong nhóm), frame rows/rangeBetween (từ dòng hiện tại nhìn được những dòng nào). Ranking bắt buộc orderBy; aggregate không orderBy = tính cả nhóm.

**Mid:**

5. *rowsBetween vs rangeBetween — khác nhau và mỗi cái dùng khi nào?* — rows đếm số DÒNG vật lý quanh dòng hiện tại; range đo bằng GIÁ TRỊ cột orderBy (nên dòng trùng giá trị cùng vào frame, và "lỗ hổng" giá trị làm frame ít dòng đi). Trung bình trượt "7 dòng gần nhất" → rows; "7 ngày lịch" trên chuỗi có ngày trống → range (orderBy 1 cột số/ngày). Ăn điểm: nêu hành vi ties của range.
6. *Frame mặc định là gì và nó gây bug kinh điển nào?* — Có orderBy: RANGE unboundedPreceding→currentRow; không orderBy: cả partition. Bug: `last().over(w có orderBy)` trả chính dòng hiện tại (frame dừng ở nó) thay vì giá trị cuối nhóm; và sum có orderBy bỗng thành running sum. Fix: khai frame tường minh unbounded 2 đầu.
7. *Chi phí của window function? Nhiều window trong 1 query thì sao?* — Mỗi window spec cần dữ liệu phân vùng theo partitionBy + sort theo (partition, order) → 1 shuffle + 1 sort, không có partial như aggregate. Các hàm cùng spec được gom 1 node Window → 1 shuffle; cùng partitionBy khác orderBy → tái dùng Exchange, thêm Sort; khác partitionBy → shuffle riêng. Kỹ năng: quy hoạch phép tính về ít spec nhất, kiểm chứng bằng đếm Exchange trong explain.
8. *Dedup lấy bản ghi mới nhất mỗi key — vì sao row_number chứ không dropDuplicates?* — dropDuplicates giữ dòng không xác định (phụ thuộc phân bố partition/thứ tự thực thi) — chạy 2 lần có thể khác nhau. row_number với orderBy(ts desc, tie_breaker) rồi filter rn=1 là deterministic, tái lập được — yêu cầu tối thiểu của pipeline production và của CDC merge.

**Senior:**

9. *Window trên bảng lớn bị chậm — chuỗi chẩn đoán và các đòn xử lý?* — Chẩn đoán: (1) plan/UI — mấy Exchange dưới Window, có SinglePartition (quên partitionBy)?; (2) Stages — task duration lệch (partition key skew: 1 key khổng lồ)? spill?; (3) dữ liệu vào window đã đúng grain chưa. Xử lý theo thứ tự: gom spec giảm shuffle; groupBy giảm grain trước khi window; xử lý skew key (tách key nóng xử lý riêng, hoặc thêm chiều vào partitionBy nếu nghiệp vụ cho phép — ví dụ partition theo (user, tháng) cho running total trong tháng); global window → thay bằng 2 bước (agg partition-level rồi window trên kết quả nhỏ). Điểm cộng: nói rõ window KHÔNG có map-side combine nên giảm input là đòn mạnh nhất.
10. *Thiết kế "running total toàn cục theo thời gian" cho bảng 5 tỷ dòng — Window.orderBy không partitionBy thì chết. Anh làm gì?* — Nhận diện vấn đề: global window = 1 task. Các lối ra: (1) đổi grain — cộng dồn theo (ngày) thay vì (event): agg về daily trước (5 tỷ → vài nghìn dòng) rồi window trên bảng tí hon, dán ngược về chi tiết nếu cần bằng join; (2) two-phase: partition theo khoảng thời gian, tính subtotal từng khoảng, prefix-sum các subtotal (nhỏ, làm ở driver/1 task), rồi cộng offset vào running nội bộ từng khoảng — chính là scan song song kinh điển; (3) hỏi lại nghiệp vụ: có thật cần chính xác từng event, hay running theo ngày là đủ? Senior = biết thuật toán VÀ biết mặc cả lại đề bài.

---

## 14. Summary

### Mindmap

```
                      LESSON 10 — WINDOW FUNCTIONS
                                  │
   ┌───────────────┬──────────────┼────────────────┬─────────────────────┐
   ▼               ▼              ▼                ▼                     ▼
 KHÁC GROUPBY    SPEC 3 TẦNG    HÀM             FRAME                 CHI PHÍ
   │               │              │                │                     │
 giữ nguyên      partitionBy   row_number/rank/  rows = đếm DÒNG       1 spec = 1 shuffle
 số dòng;        orderBy       dense_rank (ties!) range = đo GIÁ TRỊ    + 1 sort (no partial)
 "dòng này so    frame         lag/lead (null    mặc định: RANGE       chung spec → chung
 với nhóm"       (rows/range)  biên) ntile       →currentRow: bẫy      Exchange; quên
 collapse được   tie-breaker   first/last        last(); running sum   partitionBy = 1 task
 thì cứ groupBy  cho determin. sum.over=running  unbounded 2 đầu       ôm cả bảng
```

### Checklist trước khi gõ "Continue"

- [ ] Nói được không vấp: window giữ nguyên số dòng, groupBy collapse — và ví dụ mỗi bên.
- [ ] Vẽ được frame diagram và giải thích rows vs range trên dữ liệu có ngày trống + ties.
- [ ] Thuộc bảng 100/90/90/80 của row_number/rank/dense_rank.
- [ ] Biết bẫy frame mặc định — vì sao last() trả chính nó, vì sao sum thành running.
- [ ] Viết được dedup deterministic bằng row_number + tie-breaker, chê được dropDuplicates đúng chỗ.
- [ ] Đếm được Exchange dưới Window trong explain, nói được khi nào 2 window chung 1 shuffle.
- [ ] Đã chạy lab: thấy cảnh báo No Partition Defined, thấy chỗ sum_7rows ≠ sum_7days.

---

## 15. Next Lesson

**Lesson 11 — Complex types: array/map/struct, explode, JSON.**

Đến giờ mọi cột đều phẳng: số, chữ, ngày. Nhưng dữ liệu thật đến từ Kafka/API là JSON lồng nhau: struct trong array trong struct — và Debezium CDC message (thứ nuôi sống Module 4) chính là một struct `before/after/source` như vậy. Bài sau bạn học cách Spark chứa dữ liệu lồng (`ArrayType`, `MapType`, `StructType`), mổ JSON bằng `from_json`, duỗi phẳng bằng `explode` (và cái bẫy explode nhân số dòng như duplicate join key!), truy cập field bằng dot notation. Học xong bạn parse được message Kafka bất kỳ thành bảng phẳng sạch sẽ — kỹ năng bắt buộc trước khi chạm vào streaming.

Dữ liệu phẳng là dữ liệu đã được ai đó dọn cho bạn. Từ bài sau, bạn là người dọn.

> Gõ **"Continue"** khi sẵn sàng.
