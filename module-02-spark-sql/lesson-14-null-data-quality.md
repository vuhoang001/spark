# Lesson 14 — Null handling & data quality patterns

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 7 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **NULL semantics & three-valued logic** (đúng/sai/KHÔNG BIẾT) — bảng chân trị phải thuộc như bảng cửu chương.
- Các bẫy NULL chết người: `!=` âm thầm loại dòng NULL, join key NULL không bao giờ khớp, aggregate lặng lẽ bỏ qua NULL, `count(*)` vs `count(col)`.
- Bộ công cụ: `isNull/isNotNull`, **`eqNullSafe` (`<=>`)**, `fillna/dropna`, `coalesce`, `nullif`.
- **Data quality patterns** chuẩn production: assert row count, null %, range check, uniqueness check — và viết **QC report** ra bảng.
- Vị trí của framework: Great Expectations, dbt tests — khi nào tự viết, khi nào dùng đồ có sẵn.

Sau bài này bạn phải làm được:

- Nhìn một câu filter/join bất kỳ và chỉ ra dòng NULL sẽ đi đâu (được giữ, bị loại, hay thành NULL tiếp).
- Viết một hàm QC tái sử dụng: nhận DataFrame + bộ rule, trả về report + quyết định pass/fail pipeline.
- Trả lời được câu hỏi tưởng dễ mà đánh trượt khối người: "count(*), count(col), count(distinct col) khác nhau chỗ nào khi có NULL?"

Kiến thức dùng trong thực tế: **hằng ngày, và nhất là những ngày tồi tệ**. Bug NULL không ném exception — nó cho ra con số SAI mà pipeline vẫn xanh. Data quality check là thứ biến "phát hiện sai sau 6 tháng nhờ khách hàng phàn nàn" thành "phát hiện sau 6 phút nhờ pipeline tự chặn". Project 1 (bài kế tiếp) chấm điểm nặng phần này.

---

## 2. Why

### Vấn đề: NULL không phải một giá trị — nó là sự vắng mặt

NULL nghĩa là "không biết / không có / chưa điền". Và logic hai giá trị (đúng/sai) sập ngay khi gặp "không biết":

- "Số tiền không biết có lớn hơn 100 không?" → **không biết**.
- "Không biết có bằng không biết không?" → **không biết** (hai khách bỏ trống SĐT đâu chắc trùng SĐT).

SQL (và Spark theo chuẩn SQL) giải quyết bằng **three-valued logic**: mọi so sánh với NULL trả về **NULL** (unknown), không phải false. Nghe hợp lý — cho đến khi bạn nhớ ra: **filter chỉ giữ dòng có điều kiện TRUE**. Dòng cho kết quả NULL bị loại **y như false**, không một lời cảnh báo.

### Câu chuyện kinh điển (bạn sẽ gặp phiên bản của riêng mình)

Bảng orders có 100.000 dòng, 5.000 dòng `order_status` là NULL (dữ liệu nguồn lỗi):

```python
delivered = orders.filter(F.col("order_status") == "delivered")   # 96.000 - 5.000 NULL loại đúng? ừ thì...
cancelled = orders.filter(F.col("order_status") != "delivered")   # phần còn lại?

delivered.count() + cancelled.count()   # = 95.000  ← 5.000 dòng BỐC HƠI
```

`== "delivered"` trên NULL → NULL → loại. `!= "delivered"` trên NULL → **cũng NULL → cũng loại**. Hai tập "bằng" và "khác" cộng lại KHÔNG bằng tổng — 5.000 dòng không thuộc về đâu cả. Báo cáo "tỷ lệ hủy đơn" từ đây sai âm thầm, dashboard vẫn xanh, sếp vẫn tin.

### Nếu không có quy trình data quality thì sao?

Dữ liệu xấu KHÔNG dừng pipeline — nó chảy tiếp: bronze nhận NULL → silver join rơi dòng → gold tính thiếu doanh thu → dashboard sai → quyết định kinh doanh sai. Chi phí sửa tăng theo cấp số nhân qua từng tầng ("quy tắc 1-10-100": lỗi bắt ở nguồn giá 1, bắt ở giữa giá 10, để lọt đến người dùng giá 100). QC check là trạm hải quan giữa các tầng: hàng lậu bị chặn tại biên, không vào được nội địa.

### Trade-off

| Được (QC nghiêm) | Mất |
|---|---|
| Bug dữ liệu bị chặn sớm, có địa chỉ | Pipeline chậm thêm (mỗi check là job/scan) |
| Tin được số liệu — tài sản quý nhất của data team | Phải bảo trì rule khi nghiệp vụ đổi |
| Fail có report, debug nhanh | Rule quá gắt → false alarm, pipeline dừng oan lúc 3h sáng |

> Nghệ thuật là chọn đúng độ gắt: check **fail-hard** cho điều bất khả xâm phạm (PK trùng, doanh thu âm), **warn-only** cho điều dao động tự nhiên (null % tăng nhẹ).

---

## 3. Theory

### 3.1. Three-valued logic — bảng chân trị PHẢI THUỘC

So sánh: bất cứ phép so sánh nào (`=, !=, >, <, >=, <=`) có một vế NULL → kết quả **NULL**.

Phép logic với unknown (N = NULL/unknown):

| AND | T | F | N |          | OR | T | F | N |          | NOT |   |
|-----|---|---|---|----------|----|---|---|---|----------|-----|---|
| **T** | T | F | N |        | **T** | T | T | T |        | **T** | F |
| **F** | F | F | F |        | **F** | T | F | N |        | **F** | T |
| **N** | N | F | N |        | **N** | T | N | N |        | **N** | N |

Mẹo nhớ (thay vì học vẹt 9 ô): N là "chưa biết", chỉ cần hỏi *"nếu điền T hoặc F vào chỗ chưa biết, kết quả có đổi không?"* — `F AND N`: dù N là gì kết quả vẫn F → ra **F**. `T AND N`: N quyết định → ra **N**. `T OR N`: dù N là gì vẫn T → ra **T**.

Hệ quả filter (khắc vào tim): **`filter` giữ TRUE, loại cả FALSE lẫn NULL.** Và `NOT(điều kiện NULL)` vẫn là NULL — phủ định không cứu được dòng NULL.

### 3.2. Bản đồ hành vi NULL — nơi nào NULL làm gì

```
                          DÒNG CÓ NULL ĐI ĐÂU?
┌─────────────────┬───────────────────────────────────────────────────┐
│ filter ==, !=   │ BỊ LOẠI (so sánh ra NULL). != là bẫy kín nhất.    │
│ join key = NULL │ KHÔNG khớp với bất kỳ ai — kể cả NULL bên kia     │
│                 │ (inner: mất dòng; left: giữ dòng, cột phải NULL)  │
│ groupBy key     │ ĐƯỢC GIỮ! NULL gom thành MỘT nhóm riêng           │
│                 │ (khác join! — cặp đôi bất đối xứng nổi tiếng)     │
│ sum/avg/min/max │ BỎ QUA NULL. avg = sum/số-dòng-KHÔNG-null         │
│ count(*)        │ đếm MỌI dòng, kể cả dòng toàn NULL                │
│ count(col)      │ chỉ đếm dòng col KHÔNG null                       │
│ count(distinct) │ không đếm NULL như một giá trị                    │
│ orderBy         │ NULL xếp ĐẦU khi asc (nulls first) — đổi được     │
│                 │ bằng asc_nulls_last()                             │
│ biểu thức + - * │ lây lan: NULL + 1 = NULL, concat(NULL,'a')=NULL  │
│ window frame    │ dòng NULL vẫn trong partition; hàm agg trong      │
│                 │ window bỏ qua NULL như agg thường                 │
└─────────────────┴───────────────────────────────────────────────────┘
```

Ba điểm đáng tiền nhất:

1. **`!=` là bẫy kín nhất** — ai cũng ngờ `==`, ít ai ngờ `!=`. Muốn "khác X hoặc NULL": `(col != X) | col.isNull()`.
2. **join vs groupBy bất đối xứng**: groupBy coi NULL là một nhóm; join coi NULL là "không khớp với cả vũ trụ". Muốn join khớp NULL–NULL: **`eqNullSafe` / `<=>`** — nhưng dừng lại tự hỏi đã: hai bản ghi "không biết" khớp nhau là đúng nghiệp vụ không?
3. **avg bỏ NULL**: `avg` của [10, NULL, 20] là **15**, không phải 10. Muốn tính NULL như 0 → `avg(coalesce(col, 0))` — hai nghiệp vụ khác nhau, chọn có chủ đích.

Chú thích từ lesson 13: đây là lý do Catalyst tự chèn `IsNotNull(key)` vào PushedFilters trước inner join/filter — dòng key NULL đằng nào cũng không khớp/không qua, loại sớm từ lúc scan cho rẻ. Plan đã dạy bạn NULL semantics từ trước khi bạn học nó.

### 3.3. Bộ công cụ xử lý NULL

| Công cụ | Làm gì | Ghi nhớ |
|---|---|---|
| `col.isNull()` / `isNotNull()` | Kiểm tra NULL — trả TRUE/FALSE thật (không phải NULL) | Cách duy nhất "hỏi thẳng". `col == None` là sai văn phạm tư duy |
| `col.eqNullSafe(x)` / SQL `<=>` | So sánh coi NULL = NULL là TRUE, NULL = giá trị là FALSE | Không bao giờ trả NULL. Dùng cho dedup/so sánh CDC before-after |
| `F.coalesce(a, b, c)` | Giá trị đầu tiên KHÔNG null | Chuẩn hóa fallback: `coalesce(delivered_date, estimated_date)` |
| `F.nullif(a, b)` | NULL nếu a==b, ngược lại a | Ngược của coalesce; kinh điển: `amount / nullif(qty, 0)` né chia 0 |
| `df.fillna(value, subset)` | Điền giá trị thay NULL | Điền theo kiểu cột; **điền có ý nghĩa nghiệp vụ**, đừng điền 0 vô tội vạ |
| `df.dropna(how, thresh, subset)` | Bỏ dòng có NULL | `how='any'/'all'`, `thresh=n` (giữ dòng có ≥n giá trị non-null) |
| `F.when(...).otherwise(...)` | Rẽ nhánh — không match nào và không otherwise → **NULL** | Quên `otherwise` = tự chế NULL mới |

Triết lý xử lý NULL ở silver — 3 lựa chọn, PHẢI chọn tường minh cho từng cột và ghi vào design doc:
- **Drop**: dòng vô dụng khi thiếu trường này (order không có order_id).
- **Fill**: có giá trị mặc định nghiệp vụ chấp nhận (freight NULL → 0 nếu nghiệp vụ nói "không phí").
- **Keep + đánh dấu**: NULL mang thông tin (đơn chưa giao thì delivered_date NULL là ĐÚNG — điền gì cũng là nói dối).

### 3.4. Data quality patterns — 4 lớp check chuẩn production

```
   PIPELINE          LỚP CHECK              VÍ DỤ RULE (Olist)
 bronze ──▶ ┌─ ① VOLUME     row count trong khoảng kỳ vọng; hôm nay ≥ 50% hôm qua
            ├─ ② COMPLETENESS null % mỗi cột ≤ ngưỡng; order_id null = 0%
 silver ──▶ ├─ ③ VALIDITY   range: price > 0; status ∈ tập cho phép; date không ở tương lai
            └─ ④ UNIQUENESS  PK không trùng: count == count(distinct pk)
 gold  ──▶  ⑤ (nâng cao) CONSISTENCY: tổng revenue gold == tổng price silver (đối soát chéo)
```

Cấu trúc một check chuẩn (anatomy): **rule** (điều khẳng định) → **đo** (query ra con số) → **so ngưỡng** → **hành động** (fail pipeline / warn / quarantine dòng lỗi) → **ghi report** (bảng QC, có timestamp, để trend theo thời gian).

Pattern "quarantine" đáng học sớm: thay vì drop dòng lỗi (mất dấu vết) hay fail cả pipeline (một dòng lỗi chặn 10 triệu dòng sạch), tách dòng lỗi vào bảng `_rejects` kèm lý do — pipeline chạy tiếp, dữ liệu lỗi có chỗ để điều tra.

### 3.5. Great Expectations & dbt tests — đừng phát minh lại bánh xe (mãi)

- **Great Expectations (GX)**: framework Python — khai "expectation" (`expect_column_values_to_not_be_null`, `expect_column_values_to_be_between`...), chạy trên Spark DataFrame được, sinh report HTML + data docs. Mạnh về catalog rule + báo cáo đẹp; giá phải trả: thêm dependency, learning curve.
- **dbt tests**: nếu transform bằng dbt (SQL trên warehouse/Trino), test khai trong YAML (`unique`, `not_null`, `relationships`, `accepted_values`) — chạy sau mỗi model. Triết lý giống hệt cái ta tự viết: query đếm dòng vi phạm, >0 là fail.

Lời khuyên mentor: **tuần này tự viết check bằng PySpark** — hiểu ruột gan từng pattern; khi đi làm, ưu tiên đồ có sẵn của team; nếu team chưa có gì, bản tự viết của bạn (như lab hôm nay) chính là điểm khởi đầu tốt — nhẹ, không dependency, dễ thuyết phục.

---

## 4. Internal

### NULL được lưu thế nào — vì sao check null rẻ

Trong **UnsafeRow** (Tungsten, lesson 13), mỗi row mở đầu bằng **null bitset** — mỗi cột 1 bit. `isNull` = đọc 1 bit, không đụng giá trị. Trong **Parquet**, mỗi cột lưu **definition level** + thống kê `null_count` per row group / page → `WHERE col IS NOT NULL` có thể skip nguyên row group toàn NULL nhờ metadata, chưa cần giải nén dữ liệu. Đây là lý do các check null % chạy nhanh hơn bạn tưởng trên Parquet, và chậm hơn bạn tưởng trên CSV (CSV phải parse hết — thêm một lý do bronze nên sớm thành định dạng columnar).

### Vì sao `count(*)` và `count(col)` khác nhau tận physical plan

`count(*)`: chỉ cần đếm số row — với Parquet có thể trả lời gần như từ metadata. `count(col)`: phải kiểm tra bit null của col từng dòng. `count(distinct col)`: đắt hơn hẳn — cần gom giá trị (expand + 2 tầng aggregate trong plan). Chạy `df.groupBy().agg(count("*"), count("col"), countDistinct("col")).explain()` sẽ thấy 3 hình dạng plan khác nhau.

### Một batch QC nhiều rule = 1 job, nếu bạn viết đúng

Cách ngây thơ: mỗi rule một action (`df.filter(rule1_vi_phạm).count()`, rồi rule2...) → N rule = N job = N lần scan. Cách đúng: gom mọi phép đo thành **một `agg` duy nhất**:

```python
df.agg(
    F.count("*"),
    F.sum(F.col("price").isNull().cast("int")),        # null count của price
    F.sum((F.col("price") <= 0).cast("int")),          # vi phạm range
    F.countDistinct("order_id"),
).collect()   # 1 action, 1 lần scan, đủ số liệu cho cả report
```

Trick `sum(condition.cast("int"))` = "đếm dòng thỏa điều kiện" không tốn thêm scan — đây là viên gạch của mọi QC engine tự chế. (Lưu ý ngược: condition ra NULL thì cast ra NULL và sum bỏ qua — chính là hành vi ta muốn khi đếm vi phạm, nhưng phải Ý THỨC được điều đó.)

---

## 5. API

### `isNull` / `isNotNull`

```python
orders.filter(F.col("order_delivered_customer_date").isNull() &
              (F.col("order_status") == "delivered"))   # delivered mà không có ngày giao? → nghi vấn!
```
- **Pitfall**: viết `F.col("x") == None` — chạy được (Spark dịch hộ) nhưng mập mờ và lint sẽ mắng; chuẩn là `isNull()`.

### `eqNullSafe` / `<=>`

```python
# So sánh before/after của CDC: cột nào THẬT SỰ đổi (NULL→NULL là không đổi)
changed = df.filter(~F.col("before_city").eqNullSafe(F.col("after_city")))
```
- **Pitfall**: dùng `<=>` cho join key NULL mà chưa hỏi nghiệp vụ — hai đơn "không rõ khách" khớp nhau thường là SAI; NULL key nên được surrogate hóa hoặc quarantine trước join.

### `fillna` / `dropna`

```python
df.fillna({"freight_value": 0.0, "product_category_name": "unknown"})
df.dropna(subset=["order_id", "customer_id"])            # thiếu key = dòng bỏ đi (nhưng đếm trước!)
df.dropna(thresh=5)                                      # giữ dòng có ít nhất 5 giá trị non-null
```
- **Pitfall 1**: `fillna(0)` không subset → điền 0 vào MỌI cột số, kể cả cột mà 0 là dối trá (điểm review 0 khác chưa review!).
- **Pitfall 2**: `dropna` không đo trước/sau → âm thầm mất 30% dữ liệu không ai hay. Luôn log count trước/sau (hoặc để QC report làm việc đó).

### `coalesce` (hàm cột — đừng nhầm `df.coalesce(n)` giảm partition!)

```python
df.withColumn("effective_date",
    F.coalesce("order_delivered_customer_date", "order_estimated_delivery_date"))
```
- **Pitfall**: trùng tên với `df.coalesce(numPartitions)` (lesson 16) — một cái là hàm cột fallback NULL, một cái là gộp partition. Câu đùa tuyển dụng: "coalesce nào?".

### `nullif`

```python
df.withColumn("avg_item_price", F.col("total") / F.nullif(F.col("qty"), F.lit(0)))
# qty=0 → nullif ra NULL → phép chia ra NULL (thay vì nổ / ra Infinity) → avg bỏ qua
```

### `count` các thể loại

```python
df.agg(F.count("*"), F.count("review_score"), F.countDistinct("order_id"))
```
- **Pitfall**: dùng `count(col)` để đếm "số dòng" — thiếu dòng NULL lúc nào không hay. Đếm dòng = `count("*")`, chấm hết.

### Khung QC tự viết (dùng cho lab + Project 1)

```python
def qc_report(df, table_name, rules):
    """rules: list[(rule_name, violation_condition_col)] → DataFrame report 1 dòng/rule"""
    aggs = [F.count("*").alias("_total")] + [
        F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in rules]
    r = df.agg(*aggs).collect()[0]
    return [(table_name, name, r["_total"], int(r[name] or 0),
             float(r[name] or 0) / r["_total"] if r["_total"] else 0.0)
            for name, _ in rules]
```
- Lưu ý thiết kế: `when(cond,1).otherwise(0)` thay vì `cond.cast("int")` để NULL-condition ra 0 tường minh, không lệ thuộc hành vi sum-bỏ-NULL.

---

## 6. Demo nhỏ

```
Input:  6 dòng có NULL cài sẵn
   ↓    filter != , join, groupBy, count các loại
Output: tận mắt xem NULL rơi ở đâu
```

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("demo14").master("local[2]").getOrCreate()

orders = spark.createDataFrame(
    [("o1", "delivered"), ("o2", "shipped"), ("o3", None),
     ("o4", "delivered"), ("o5", None), ("o6", "canceled")],
    ["order_id", "status"])

eq  = orders.filter(F.col("status") == "delivered").count()   # 2
neq = orders.filter(F.col("status") != "delivered").count()   # 2  ← KHÔNG PHẢI 4!
print(f"== : {eq} | != : {neq} | tổng: {eq+neq} / 6  → 2 dòng NULL bốc hơi cả 2 phía")

fix = orders.filter((F.col("status") != "delivered") | F.col("status").isNull()).count()
print(f"!= OR isNull: {fix}  → đủ 4")

# groupBy giữ NULL thành nhóm riêng — đối lập với join:
orders.groupBy("status").count().show()          # có dòng status=NULL, count=2

pays = spark.createDataFrame([("o1", 10.0), (None, 99.0)], ["order_id", "amount"])
orders.join(pays, "order_id").show()             # NULL key bên pays không khớp ai — mất 99.0

print(orders.select(F.count("*"), F.count("status"), F.countDistinct("status")).collect())
# count(*)=6, count(status)=4, countDistinct=3
spark.stop()
```

Sáu dòng dữ liệu, bốn cú lừa. Hãy chạy thật — cảm giác "tận mắt thấy 2+2=4/6" đáng giá hơn mười trang lý thuyết.

---

## 7. Production Example

Pipeline Olist thật của bạn (và mọi công ty e-commerce) — nơi NULL là dân bản địa chứ không phải khách lạ:

```
orders (100k dòng, dữ liệu THẬT từ Kaggle):
  order_approved_at            NULL ~0.2%   → đơn tạo xong chưa kịp duyệt: NULL HỢP LỆ
  order_delivered_carrier_date NULL ~1.8%   → chưa bàn giao vận chuyển: HỢP LỆ
  order_delivered_customer_date NULL ~3%    → chưa giao xong: HỢP LỆ với status≠delivered,
                                              NHƯNG là LỖI nếu status='delivered'  ← rule chéo cột!
reviews: review_comment_message NULL ~58%   → khách lười gõ: hợp lệ tuyệt đối, đừng dại drop
products: product_category_name NULL ~1.5%  → thiếu phân loại: fill 'unknown' + báo warn
```

Trạm QC giữa bronze → silver mà một team chuẩn sẽ đặt (nguyên mẫu của Project 1 checkpoint 2):

```
┌────────── QC GATE: silver.orders ──────────────────────────────────┐
│ [FAIL-HARD]  order_id null            = 0 dòng      → vi phạm: DỪNG │
│ [FAIL-HARD]  order_id trùng           = 0 dòng      → vi phạm: DỪNG │
│ [FAIL-HARD]  status ∉ danh sách 8 giá trị hợp lệ    → quarantine    │
│ [WARN]       delivered mà thiếu delivered_date       → đếm + alert  │
│ [WARN]       null % mỗi cột lệch >2× so trung bình 7 ngày → alert   │
│ [FAIL-HARD]  row count < 50% trung bình 7 ngày      → DỪNG (nguồn   │
│              có thể gãy — ingest thiếu file)                        │
└─────────────────────────────────────────────────────────────────────┘
          mọi kết quả → bảng qc_results (append, có run_date)
          → dashboard trend null % theo ngày = radar sớm của data team
```

Vì sao doanh nghiệp làm vậy: rule "row count ≥ 50% hôm qua" từng cứu vô số team khỏi thảm họa "connector chết, hôm nay ingest 0 dòng, dashboard hiển thị doanh thu 0, CEO gọi lúc 7h sáng". Check 5 giây, tránh cuộc họp 3 tiếng.

---

## 8. Hands-on Lab

**Mục tiêu**: kiểm kê NULL trên toàn bộ Olist thật, xây QC engine mini, xuất QC report ra Parquet.

### Bước 1 — kiểm kê NULL toàn cục: `labs/lab14/null_audit.py`

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab14-audit").getOrCreate()
tables = ["orders", "order_items", "order_reviews", "products", "customers"]

for t in tables:
    df = spark.read.csv(f"/workspace/data/olist/olist_{t}_dataset.csv",
                        header=True, inferSchema=True)
    total = df.count()
    # 1 agg duy nhất đo null % MỌI cột — không loop action từng cột!
    row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
    print(f"\n=== {t} ({total:,} dòng) ===")
    for c in df.columns:
        n = row[c] or 0
        if n > 0:
            print(f"  {c:38s} {n:7,} null  ({100.0*n/total:5.2f}%)")
spark.stop()
```

Chạy `make run-local F=labs/lab14/null_audit.py`. Ghi vào NOTES.md: cột nào NULL nhiều nhất mỗi bảng, và với TỪNG cột đó — NULL là hợp lệ nghiệp vụ hay là lỗi? (Đây là câu hỏi quan trọng nhất lab, và máy không trả lời hộ được.)

### Bước 2 — tái hiện bẫy trên dữ liệu thật: `labs/lab14/null_traps.py`

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab14-traps").getOrCreate()
orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)

total = orders.count()
with_date = orders.filter(F.col("order_delivered_customer_date").isNotNull()).count()
neq_trap  = orders.filter(F.col("order_status") != "delivered").count()
eq        = orders.filter(F.col("order_status") == "delivered").count()
print(f"total={total:,} | =={eq:,} | !={neq_trap:,} | cộng={eq+neq_trap:,}")
# status không có NULL trong Olist → cộng đủ. Giờ TỰ TẠO bẫy:
dirty = orders.withColumn("status2",
    F.when(F.rand(seed=1) < 0.05, None).otherwise(F.col("order_status")))
e2 = dirty.filter(F.col("status2") == "delivered").count()
n2 = dirty.filter(F.col("status2") != "delivered").count()
print(f"sau khi tiêm 5% NULL: == {e2:,} | != {n2:,} | cộng={e2+n2:,} / {total:,}")

# Rule chéo cột: delivered mà không có ngày giao — bug thật của dataset thật:
bad = orders.filter((F.col("order_status") == "delivered") &
                    F.col("order_delivered_customer_date").isNull())
print(f"delivered nhưng thiếu delivered_date: {bad.count()} dòng")   # >0 đấy — Olist cũng bẩn!
bad.select("order_id", "order_status", "order_purchase_timestamp").show(5, False)
spark.stop()
```

### Bước 3 — QC engine mini + report: `labs/lab14/qc_engine.py`

```python
from pyspark.sql import SparkSession, functions as F
import datetime

spark = SparkSession.builder.appName("lab14-qc").getOrCreate()

def run_qc(df, table, rules, fail_hard):
    aggs = [F.count(F.lit(1)).alias("_total")] + \
           [F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in rules]
    r = df.agg(*aggs).collect()[0]
    rows, failed = [], []
    for name, _ in rules:
        viol = int(r[name] or 0)
        rows.append((datetime.date.today().isoformat(), table, name,
                     r["_total"], viol, round(viol / r["_total"] * 100, 4)))
        if viol > 0 and name in fail_hard:
            failed.append(name)
    report = spark.createDataFrame(rows,
        ["run_date", "table", "rule", "total_rows", "violations", "violation_pct"])
    return report, failed

orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)
VALID_STATUS = ["delivered", "shipped", "canceled", "unavailable",
                "invoiced", "processing", "created", "approved"]
rules = [
    ("order_id_null",      F.col("order_id").isNull()),
    ("customer_id_null",   F.col("customer_id").isNull()),
    ("status_invalid",     ~F.col("order_status").isin(VALID_STATUS) |
                           F.col("order_status").isNull()),
    ("purchase_ts_null",   F.col("order_purchase_timestamp").isNull()),
    ("delivered_no_date",  (F.col("order_status") == "delivered") &
                           F.col("order_delivered_customer_date").isNull()),
    ("delivery_before_purchase",
                           F.col("order_delivered_customer_date") <
                           F.col("order_purchase_timestamp")),
]
report, failed = run_qc(orders, "orders", rules,
                        fail_hard={"order_id_null", "customer_id_null", "status_invalid"})
report.show(truncate=False)

# uniqueness check — cần query riêng (không gộp được vào agg trên):
dup = orders.groupBy("order_id").count().filter("count > 1").count()
print(f"order_id trùng: {dup}")
if failed or dup > 0:
    report.write.mode("append").parquet("/workspace/labs/lab14/out/qc_results")
    raise SystemExit(f"QC FAILED: {failed + (['pk_duplicated'] if dup else [])}")
report.write.mode("append").parquet("/workspace/labs/lab14/out/qc_results")
print("QC PASSED — pipeline được phép chạy tiếp")
spark.stop()
```

### Bước 4 — quan sát

Chạy 2 lần → đọc lại `qc_results` thấy report tích lũy theo `run_date` (nguyên liệu cho trend). Mở UI :4040: xác nhận toàn bộ rules (trừ uniqueness) chỉ tốn **1 job** — nếu thấy 6 job, bạn đã viết kiểu loop-action, quay lại đọc Internal.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Điền bảng chân trị AND/OR/NOT với NULL (không nhìn tài liệu), nêu mẹo suy luận thay vì học vẹt.
2. Tại sao `filter(col != 'x')` + `filter(col == 'x')` không phủ hết bảng? Viết phiên bản đúng.
3. `count(*)` vs `count(col)` vs `count(distinct col)` với cột có NULL: khác gì?

**Medium** — Viết QC cho `order_reviews` và `order_items`: tự nghĩ ≥5 rule mỗi bảng (gợi ý reviews: score ∈ 1–5, review_id unique, answer_ts ≥ creation_ts; items: price > 0, freight ≥ 0, order_item_id ≥ 1). Phân loại từng rule fail-hard/warn kèm 1 câu lý do. Chạy trên dữ liệu thật, dán report — có rule nào Olist thật vi phạm không?

**Hard** — Xây pattern **quarantine**: viết `split_quarantine(df, rules)` trả về `(clean_df, rejects_df)` trong đó rejects có thêm cột `reject_reasons` (ARRAY các rule vi phạm — dùng kiến thức array lesson 11, gợi ý: build array của `when(cond, lit(name))` rồi `filter` phần tử NULL bằng higher-order function). Yêu cầu: MỘT lần scan, không loop-action; chứng minh `clean.count() + rejects.count() == df.count()`.

**Production Challenge** — Đọc về dbt tests (docs: `unique`, `not_null`, `accepted_values`, `relationships`) hoặc mở repo `../kafka-flink` nếu có dbt/Trino. Map 4 test đó sang 4 rule PySpark bạn đã viết. Viết 10 dòng so sánh: khi nào tự viết PySpark QC thắng (chặn NGAY trong pipeline Spark, trước khi ghi), khi nào dbt test thắng (test SAU khi dữ liệu vào warehouse, dễ khai báo, gần analyst)?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| `isNull` filter trên Parquet | Nhanh | Null bitset + null_count trong metadata row group → skip được cả khối. |
| Gom N rule vào 1 `agg` | Nhanh | 1 job, 1 lần scan cho cả report. |
| Loop N rule, mỗi rule 1 `count()` | Chậm ×N | N job, N lần scan — lỗi kiến trúc QC phổ biến nhất. |
| `count(distinct)` / uniqueness check | Đắt | Expand + shuffle 2 tầng aggregate. Với bảng khổng lồ cân nhắc `approx_count_distinct` cho WARN-level (sai số ~2%), giữ exact cho PK fail-hard. |
| `dropDuplicates(subset)` | Đắt | Shuffle toàn bảng theo subset. Đừng rắc "cho chắc" — chỉ khi biết vì sao có trùng. |
| QC trên CSV vs Parquet | Chậm vs nhanh | CSV parse toàn bộ mọi lần; thêm lý do bronze → columnar sớm. |
| `eqNullSafe` trong join | Cẩn thận | Join `a <=> b` không dùng được equi-join hash thông thường trong mọi trường hợp tối ưu — kiểm tra plan nếu bảng lớn. |

Câu tự vấn của bài này: *"nếu cột này có NULL, dòng đó sẽ ĐI ĐÂU qua phép biến đổi này — và tôi có chủ đích chọn số phận đó không?"*

---

## 11. Spark UI

- **Tab SQL**: mở query QC engine — xác nhận cả bó rule là MỘT query duy nhất, một lượt scan. Trong plan, các `sum(CASE WHEN ...)` xếp hàng trong cùng một HashAggregate — hình ảnh của "1 scan N phép đo".
- Node Scan parquet: khi filter `isNotNull`, nhìn `PushedFilters: [IsNotNull(col)]` — và nhớ lesson 13: Spark tự thêm IsNotNull trước inner join. Giờ bạn hiểu VÌ SAO: NULL key đằng nào cũng không khớp.
- **Tab Jobs**: đếm job của script QC — mỗi job thừa là một lần scan thừa; QC gọn phải ~2 job (agg rules + uniqueness).
- So `count(distinct order_id)` với `approx_count_distinct`: mở 2 plan, thấy expand/2-tầng-agg biến mất ở bản approx — trực quan hóa chữ "đắt" của distinct.

---

## 12. Common Mistakes

1. **`!=` mà quên NULL** — kinh điển của kinh điển. "Khác X" trong đầu người ≠ "khác X" trong SQL. Luôn tự hỏi: dòng NULL nên thuộc phía nào, rồi viết tường minh `| col.isNull()` nếu cần.
2. **`fillna(0)` toàn bảng** — điền 0 vào review_score, vĩ độ, số tiền... mỗi cột một thảm họa riêng. fillna LUÔN đi kèm subset + lý do nghiệp vụ.
3. **`dropna` không đo trước/sau** — "làm sạch" 30% dataset trong im lặng. Mọi phép drop phải để lại số liệu trong QC report.
4. **Join key có NULL mà không xử trước** — inner join âm thầm rơi dòng; hai bảng "khớp 98%" còn 2% biến đi đâu không ai truy. Chuẩn: QC key not-null TRƯỚC join, hoặc quarantine.
5. **QC engine kiểu loop-action** — 20 rule = 20 lần scan, QC chậm hơn cả transform, rồi bị... tắt đi cho nhanh. QC chậm là QC sẽ chết.
6. **Rule không phân cấp** — mọi rule đều fail-hard → pipeline dừng oan 3h sáng vì null % nhích 0.1% → on-call chai lì → tắt luôn alert → ngày dữ liệu hỏng thật không ai nhìn. Phân cấp FAIL/WARN ngay từ đầu.
7. **Check xong không lưu report** — pass/fail hôm nay mà không có trend thì không bao giờ trả lời được "null % tăng từ bao giờ?". Report là dữ liệu, đối xử như dữ liệu.
8. **Quên `otherwise` trong `when`** — tự tay sản xuất NULL mới toanh rồi tuần sau đi debug NULL của chính mình.

---

## 13. Interview

**Junior:**

1. *Three-valued logic là gì? Vì sao SQL cần nó?* — Hệ logic 3 giá trị TRUE/FALSE/UNKNOWN(NULL). Vì NULL nghĩa "không biết", so sánh với nó không thể trả lời chắc true/false — mọi so sánh chứa NULL ra NULL. Filter chỉ giữ TRUE nên dòng NULL bị loại như FALSE.
2. *`col != 'x'` xử lý dòng NULL thế nào? Muốn giữ NULL thì sao?* — NULL != 'x' → NULL → bị filter loại. Muốn giữ: `(col != 'x') | col.isNull()`. Đây là bug-báo-cáo-sai phổ biến bậc nhất vì không có lỗi nào được ném.
3. *count(*) vs count(col)?* — count(*) đếm mọi dòng kể cả toàn NULL; count(col) chỉ đếm dòng col non-null; count(distinct col) không tính NULL là một giá trị. Hiệu count(*) − count(col) chính là số NULL — trick đếm null gọn.
4. *fillna, dropna, coalesce khác nhau thế nào?* — fillna: thay NULL bằng hằng số theo cột. dropna: bỏ dòng chứa NULL (any/all/thresh/subset). coalesce (hàm cột): lấy giá trị non-null đầu tiên trong danh sách cột — fallback theo logic, linh hoạt hơn fillna hằng. (Phân biệt với df.coalesce(n) giảm partition.)

**Mid:**

5. *NULL trong join key vs groupBy key — hành vi khác nhau ra sao?* — Join: NULL không khớp với bất kỳ giá trị nào kể cả NULL (inner mất dòng, left giữ dòng nhưng cột bên kia NULL). groupBy: NULL được gom thành MỘT nhóm riêng. Bất đối xứng này là nguồn bug đối soát: tổng theo group khớp nhưng join lại hụt.
6. *eqNullSafe dùng khi nào?* — Khi cần so sánh coi NULL==NULL là TRUE, luôn trả TRUE/FALSE không bao giờ NULL: so sánh before/after trong CDC để phát hiện thay đổi, dedup bản ghi có field NULL. Thận trọng khi dùng làm join condition: đúng kỹ thuật nhưng phải xác nhận nghiệp vụ muốn NULL khớp NULL, và kiểm tra plan vì có thể không tối ưu như equi-join thường.
7. *avg() với cột có NULL trả gì? Muốn NULL tính là 0?* — avg bỏ qua NULL: sum(non-null)/count(non-null). Muốn NULL là 0: avg(coalesce(col, 0)) — mẫu số thành mọi dòng. Hai con số phục vụ hai câu hỏi nghiệp vụ khác nhau; chọn sai là báo cáo sai chứ không phải bug code.
8. *Thiết kế QC check cho bảng silver: những lớp check nào?* — Volume (row count vs kỳ vọng/lịch sử), completeness (null % per cột so ngưỡng), validity (range, tập giá trị, rule chéo cột như delivered-phải-có-ngày-giao), uniqueness (PK), consistency (đối soát tổng giữa các tầng). Kèm phân cấp fail-hard/warn và ghi report có run_date để theo trend.

**Senior:**

9. *Pipeline chạy xanh 6 tháng, rồi phát hiện doanh thu thiếu ~2% từ lâu — quy trình điều tra và phòng ngừa?* — Điều tra: (a) khoanh tầng bằng đối soát ngược gold→silver→bronze→nguồn (tổng tiền, row count theo ngày) tìm tầng đầu tiên lệch; (b) nghi phạm NULL-shaped: inner join rơi key NULL/không khớp, filter `!=`, dedup quá tay, parse fail thành NULL bị lờ (lesson 11); (c) tìm ngày bắt đầu lệch, đối chiếu deploy/schema change của nguồn. Phòng ngừa: QC gate giữa các tầng có check consistency đối soát tổng chéo tầng (loại check duy nhất bắt được bug này), quarantine thay vì drop, report trend. Câu trả lời hay phải nói được: bug này KHÔNG THỂ bắt bằng test code, chỉ bắt được bằng đối soát dữ liệu.
10. *Tự viết QC engine hay dùng Great Expectations/dbt tests — quyết định thế nào?* — Tiêu chí: (a) vị trí chặn — cần fail NGAY trong job Spark trước khi ghi (in-pipeline) thì check tự viết/GX-on-Spark; test sau khi dữ liệu đã vào warehouse thì dbt gần analyst hơn; (b) chi phí: engine tự viết ~100 dòng, không dependency, đủ cho 80% nhu cầu; GX cho catalog rule + data docs + tổ chức nhiều team; (c) hệ quả vận hành: framework nào cũng vô dụng nếu không có ownership rule, phân cấp alert, quy trình xử lý fail. Senior không trả lời tên công cụ — trả lời tiêu chí chọn và nhấn mạnh phần vận hành.

---

## 14. Summary

### Mindmap

```
                     NULL & DATA QUALITY
                            │
   ┌────────────────┬───────┴─────────┬──────────────────────┐
   ▼                ▼                 ▼                      ▼
 3-VALUED LOGIC   NULL ĐI ĐÂU       CÔNG CỤ              QC PATTERNS
   │                │                 │                      │
 so sánh NULL     filter: loại      isNull/isNotNull      4 lớp: volume,
 → NULL           cả != lẫn ==!     eqNullSafe <=>        completeness,
 filter giữ       join key: không   (NULL==NULL→TRUE)     validity, unique
 TRUE, loại       khớp cả vũ trụ    coalesce: fallback    (+consistency chéo tầng)
 FALSE và NULL    groupBy: nhóm     nullif: né chia 0     fail-hard vs warn
 F AND N = F      riêng (≠ join!)   fillna+subset!        quarantine + reasons
 T OR N = T       agg bỏ NULL       dropna+đo trước/sau   1 agg = N rule = 1 scan
 NOT N = N        count(*)≠count(c) when cần otherwise    report có run_date→trend
```

### Checklist trước khi gõ "Continue"

- [ ] Viết lại bảng chân trị 3 giá trị và mẹo suy luận không cần học vẹt.
- [ ] Giải thích được vụ "2 filter cộng lại thiếu dòng" và cách viết đúng.
- [ ] Thuộc bản đồ: NULL trong filter/join/groupBy/agg/count đi đâu.
- [ ] Dùng đúng chỗ: coalesce, nullif, eqNullSafe, fillna+subset, dropna+đo.
- [ ] Viết được QC engine 1-scan-N-rules, có fail-hard/warn, lưu report Parquet.
- [ ] Tìm ra ít nhất 1 vi phạm QC trong dữ liệu Olist THẬT (có đấy!).
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Project 1 (FULL) — Olist Batch ELT chuẩn production.**

Hết bài lẻ — giờ là trận đánh tổng hợp đầu tiên. Bạn có trong tay đủ vũ khí của 14 bài: đọc/ghi và schema (M1), transformations, aggregation, join, window (L7–10), complex types (L11), kỷ luật tránh UDF (L12), đọc plan (L13), và QC gate (L14). Project 1 yêu cầu bạn dựng trọn pipeline medallion bronze → silver → gold trên Olist: ingest có ingestion_date, dedup + surrogate key + QC report, star schema với metric doanh thu theo seller theo ngày, maintenance, phục vụ BI, và Airflow DAG chạy 2h sáng mỗi ngày — có rubric chấm điểm như một bài giao việc thật ở công ty. Đây là project bạn sẽ đem đi phỏng vấn.

> Gõ **"Continue"** khi sẵn sàng.
