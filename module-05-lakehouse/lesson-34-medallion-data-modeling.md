# Lesson 34 — Medallion architecture & data modeling

> Module 5 · Lakehouse & Iceberg · Tuần 18 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3.5h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Medallion architecture**: bronze / silver / gold — mỗi tầng để làm gì, ai đọc, "hợp đồng" (contract) của từng tầng là gì.
- Tại sao **bronze phải giữ raw** — replay và audit là hai lý do sống còn.
- **Dimensional modeling** kinh điển (Kimball) áp vào lakehouse: star schema, fact vs dimension, **grain**.
- **Surrogate key vs natural key** — và tại sao lakehouse dùng hash key thay vì auto-increment.
- **SCD type 1/2/3** — đặc biệt type 2 với `valid_from`/`valid_to`/`is_current` + MERGE code chạy thật trên Iceberg.
- Thiết kế trọn bộ schema Olist: `fact_order_items`, `dim_products`, `dim_sellers`, `dim_customers`, `dim_date`.
- Trade-off hiện đại: **wide table (OBT) vs star schema** trong lakehouse.

Sau bài này bạn phải làm được:

- Nhận một nguồn dữ liệu bất kỳ và vẽ được pipeline bronze → silver → gold kèm contract mỗi tầng.
- Chọn grain cho fact table và **bảo vệ lựa chọn đó** trước một Senior khó tính.
- Viết MERGE SCD type 2 vào Iceberg không cần nhìn tài liệu.

Kiến thức dùng trong thực tế: đây là bài **thiết kế** đầu tiên của khóa. Từ tuần 18 trở đi bạn không còn là "người viết Spark job" mà là "người thiết kế data platform". Interview Senior DE, câu hỏi modeling xuất hiện với xác suất ~100%.

---

## 2. Why

### Vấn đề: pipeline không tầng lớp = mì spaghetti

Đội bạn có 15 Spark job. Job nào cũng đọc thẳng CSV/Kafka nguồn, tự làm sạch theo cách riêng, ghi ra một bảng "final" nào đó. Sáu tháng sau:

- Marketing hỏi "doanh thu tháng 3 sao dashboard A ra 1.2 tỷ mà dashboard B ra 1.4 tỷ?" — vì 2 job làm sạch 2 kiểu (job A loại đơn `canceled`, job B không).
- Nguồn đổi format một cột → **15 job vỡ cùng lúc**, sửa 15 chỗ.
- Phát hiện bug làm sạch từ 3 tháng trước → không thể tính lại, vì dữ liệu raw... đã bị ghi đè.

Medallion architecture giải quyết bằng một nguyên tắc: **dữ liệu chảy qua các tầng, mỗi tầng có một hợp đồng rõ ràng, tầng sau chỉ đọc tầng ngay trước**. Sửa logic làm sạch? Sửa 1 chỗ (bronze→silver). Bug 3 tháng trước? Replay từ bronze.

### Vấn đề thứ hai: bảng sạch rồi nhưng analyst vẫn khổ

Silver có bảng `orders`, `order_items`, `products`, `sellers`... sạch đẹp. Nhưng analyst muốn "doanh thu theo category theo tháng theo bang của seller" phải tự join 4 bảng, tự nhớ điều kiện lọc `delivered`, tự xử lý seller đổi thành phố giữa chừng. Mỗi analyst join một kiểu → lại ra số khác nhau.

**Dimensional modeling** là câu trả lời 30 năm tuổi (Kimball, 1996) và vẫn thống trị: tổ chức gold layer thành **fact** (sự kiện đo đếm được) và **dimension** (ngữ cảnh mô tả), nối theo hình ngôi sao. Analyst chỉ cần nhớ một mẫu query duy nhất.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| Một nguồn sự thật, số liệu nhất quán toàn công ty | Nhiều tầng = nhiều storage (bronze giữ mãi), nhiều job phải vận hành |
| Replay/audit được mọi thời điểm | Latency: dữ liệu đi qua 3 tầng mới đến dashboard |
| Sửa logic 1 chỗ, downstream tự hưởng | Kỷ luật đội nhóm: chỉ cần 1 người "đi tắt" đọc bronze cho dashboard là vỡ trận |
| Star schema: analyst tự phục vụ được | Modeling tốn công thiết kế trước (không "cứ đổ vào rồi tính") |

> Bài học Senior: medallion không phải công nghệ, nó là **kỷ luật tổ chức dữ liệu**. Iceberg/Delta chỉ là vật liệu; kiến trúc nằm ở hợp đồng giữa các tầng.

---

## 3. Theory

### 3.1. Ba tầng medallion — hợp đồng từng tầng

```
  NGUỒN (Postgres CDC, Kafka, CSV, API...)
     │  ingest AS-IS, không sửa gì
     ▼
┌────────────────────────────────────────────────────────────────┐
│ BRONZE — "két sắt lưu bằng chứng"                              │
│ Mục đích : lưu raw đúng như nguồn + metadata ingest            │
│ Ai đọc   : CHỈ job bronze→silver, và người điều tra sự cố      │
│ Contract : append-only, KHÔNG sửa/xóa nội dung, KHÔNG dedupe,  │
│            schema lỏng (string hết cũng được), có _ingest_ts,  │
│            _source_file. Dữ liệu xấu vẫn được VÀO.             │
└──────────────────────────────┬─────────────────────────────────┘
                               │  clean, cast type, dedupe, chuẩn hóa,
                               │  quality check (lesson 35)
                               ▼
┌────────────────────────────────────────────────────────────────┐
│ SILVER — "sự thật đã kiểm chứng" (source of truth)             │
│ Mục đích : bảng sạch, đúng kiểu, unique theo business key,     │
│            vẫn giữ grain gốc của nguồn (chưa aggregate)        │
│ Ai đọc   : job gold, data scientist, ML feature pipeline       │
│ Contract : schema tường minh + enforced, PK unique, timestamp  │
│            chuẩn UTC, giá trị chuẩn hóa (status lowercase...)  │
└──────────────────────────────┬─────────────────────────────────┘
                               │  model hóa: fact/dim, SCD, aggregate
                               ▼
┌────────────────────────────────────────────────────────────────┐
│ GOLD — "dữ liệu phục vụ nghiệp vụ"                             │
│ Mục đích : star schema / bảng aggregate trả lời câu hỏi        │
│            business, tối ưu cho đọc (BI, Trino, Superset)      │
│ Ai đọc   : analyst, BI tool, executive dashboard, API          │
│ Contract : định nghĩa metric THỐNG NHẤT (doanh thu là gì, đơn  │
│            nào được tính), SCD được xử lý, query nhanh          │
└────────────────────────────────────────────────────────────────┘
```

Ví dụ cùng 1 dòng dữ liệu Olist đi qua 3 tầng:

| Tầng | Dạng tồn tại |
|---|---|
| Bronze | `bronze.orders` — dòng CSV nguyên bản, `order_status="Delivered "` (thừa space, viết hoa), kèm `_ingest_ts`, `_source_file="olist_orders_dataset.csv"`. Có cả dòng trùng `order_id` do ingest 2 lần. |
| Silver | `silver.orders` — `order_status="delivered"` (chuẩn hóa), timestamp cast đúng kiểu, dedupe theo `order_id` giữ bản mới nhất, dòng thiếu `order_id` bị đẩy sang quarantine. |
| Gold | `gold.fact_order_items` — mỗi dòng 1 item đã join giá, phí ship, khóa ngoại trỏ tới `dim_products`, `dim_sellers`, `dim_customers`, `dim_date`. |

> **Analogy nhà hàng**: bronze là kho nguyên liệu nhập về còn nguyên thùng (giữ cả hóa đơn để đối chiếu); silver là nguyên liệu đã sơ chế, rửa sạch, phân loại trong tủ mát; gold là món ăn dọn ra bàn. Khách (analyst) không bao giờ được vào kho bốc nguyên liệu sống — và đầu bếp không vứt hóa đơn nhập hàng, vì khi khách đau bụng (số liệu sai) còn truy được lô nào hỏng.

### 3.2. Tại sao bronze PHẢI giữ raw — replay & audit

1. **Replay**: logic silver có bug (ví dụ parse timestamp sai timezone suốt 2 tháng)? Xóa silver, chạy lại bronze→silver là xong. Nếu bronze đã "làm sạch sẵn" thì thông tin gốc mất vĩnh viễn — không có gì để tính lại. Bronze = **backup của logic**, không chỉ của dữ liệu.
2. **Audit / forensic**: "con số này ở đâu ra?" — truy ngược gold → silver → bronze → `_source_file` + `_ingest_ts` → về tận file/offset nguồn. Bắt buộc trong tài chính, y tế (compliance), và cực quý khi cãi nhau với team nguồn ("dữ liệu bọn tôi gửi đúng mà!" — mở bronze ra xem).
3. **Yêu cầu tương lai chưa biết**: hôm nay bạn drop cột "vô dụng"; 6 tháng sau ML team cần đúng cột đó. Bronze giữ tất → chỉ cần thêm logic silver.
4. Chi phí? Object storage ~vài chục USD/TB/tháng — **rẻ hơn nhiều** so với một lần mất dữ liệu gốc.

### 3.3. Dimensional modeling: fact vs dimension

- **Fact table**: ghi lại **sự kiện đã xảy ra**, chứa các **measure** đo đếm được (price, freight, quantity) + các **foreign key** trỏ đến dimension. Dài (hàng trăm triệu dòng), hẹp, chỉ append/merge.
- **Dimension table**: mô tả **ngữ cảnh** của sự kiện — ai (customer, seller), cái gì (product), khi nào (date), ở đâu (geo). Ngắn (nghìn→triệu dòng), rộng (nhiều cột mô tả), thay đổi chậm (**slowly changing**).

Star schema — mọi dimension nối trực tiếp vào fact, không dimension nào nối dimension nào (đó là snowflake — tránh, trừ khi có lý do):

```
        dim_date                      dim_products
     (date_key, year,              (product_key, category,
      month, is_weekend...)         weight, dimensions...)
              ▲                            ▲
              │                            │
              │      ┌─────────────────────┴──┐
              └──────┤   fact_order_items     │
                     │  ────────────────────  │
                     │  order_id              │
        ┌───────────►│  order_item_id         │◄───────────┐
        │            │  date_key (FK)         │            │
        │            │  product_key (FK)      │            │
  dim_customers      │  seller_key (FK)       │      dim_sellers
 (customer_key,      │  customer_key (FK)     │     (seller_key, city,
  city, state...)    │  price, freight_value  │      state, valid_from,
                     │  (measures)            │      valid_to, is_current)
                     └────────────────────────┘
```

Query mẫu mà analyst chỉ cần thuộc một lần:

```sql
SELECT d.year, d.month, p.category, SUM(f.price) AS revenue
FROM   fact_order_items f
JOIN   dim_date d      ON f.date_key = d.date_key
JOIN   dim_products p  ON f.product_key = p.product_key
GROUP  BY d.year, d.month, p.category
```

### 3.4. Grain — quyết định quan trọng nhất của fact table

**Grain = một dòng fact đại diện cho cái gì.** Phải tuyên bố grain TRƯỚC khi chọn cột. Với Olist có 2 ứng viên:

| | Grain **order-level** (1 dòng = 1 đơn) | Grain **order-item-level** (1 dòng = 1 sản phẩm trong đơn) |
|---|---|---|
| Số dòng | ~99k | ~112k |
| Trả lời "doanh thu theo category"? | **KHÔNG** — 1 đơn chứa nhiều category, không biết chia tiền cho ai | Có — mỗi item biết product → category |
| Trả lời "doanh thu theo seller"? | **KHÔNG** — 1 đơn có thể nhiều seller (marketplace!) | Có |
| Trả lời "số đơn theo tháng"? | Có, trực tiếp | Có — `COUNT(DISTINCT order_id)` |
| Kết luận | Mất thông tin không lấy lại được | **Chọn cái này** |

Quy tắc chọn grain: **lấy grain MỊN NHẤT mà nguồn cho phép** (atomic grain). Từ mịn có thể roll-up lên thô (aggregate); từ thô KHÔNG BAO GIỜ drill-down xuống mịn. Sợ chậm? Xây thêm bảng aggregate `gold.agg_revenue_monthly` bên trên — đừng hy sinh grain gốc.

**Pitfall kinh điển**: trộn 2 grain trong 1 bảng — nhét cột `order_total` (grain đơn) vào bảng item-level → `SUM(order_total)` bị nhân bản theo số item, số liệu sai lặng lẽ. Measure nào không cùng grain với bảng thì không được vào bảng.

### 3.5. Surrogate key vs natural key — và hash key trong lakehouse

- **Natural key**: khóa từ nghiệp vụ — `seller_id` của Olist, mã số thuế, email. Vấn đề: (a) nguồn có thể tái sử dụng/đổi format; (b) **không phân biệt được các phiên bản** của cùng một seller khi làm SCD2 (một seller có 3 version lịch sử → 3 dòng cùng `seller_id`, cần khóa khác để fact trỏ đúng version).
- **Surrogate key**: khóa vô nghĩa do warehouse tự sinh, mỗi **dòng** (mỗi version) một khóa. Kimball cổ điển dùng auto-increment INT.

Nhưng lakehouse **không có auto-increment**: Spark ghi song song hàng trăm task, không có bộ đếm trung tâm; `monotonically_increasing_id()` không ổn định giữa các lần chạy (chạy lại ra id khác → vỡ idempotency, lesson 36). Giải pháp chuẩn lakehouse: **hash key** — deterministic, tính lại bao nhiêu lần cũng ra đúng giá trị đó:

```python
# surrogate key cho 1 VERSION của seller = hash(natural key + thời điểm hiệu lực)
F.sha2(F.concat_ws("||", F.col("seller_id"), F.col("valid_from").cast("string")), 256)
# rẻ hơn (64-bit, đủ cho hàng tỷ dòng, có xác suất collision cực nhỏ):
F.xxhash64("seller_id", F.col("valid_from").cast("string"))
```

Kèm theo **hash_diff** — hash của các cột được theo dõi thay đổi, dùng để phát hiện "seller này có đổi gì không" bằng 1 phép so sánh thay vì so từng cột:

```python
F.sha2(F.concat_ws("||", "seller_city", "seller_state", "seller_zip_code_prefix"), 256).alias("hash_diff")
```

### 3.6. SCD — Slowly Changing Dimensions

Seller `abc123` chuyển từ **Sao Paulo → Rio** ngày 2018-01-15. Đơn hàng tháng 12/2017 tính cho thành phố nào? Câu trả lời phụ thuộc SCD type:

| Type | Cách xử lý | Lịch sử | Khi dùng |
|---|---|---|---|
| **Type 1** | UPDATE đè giá trị mới | **Mất** — đơn 12/2017 giờ hiện "Rio" (sai lịch sử) | Sửa lỗi chính tả, thuộc tính không cần lịch sử |
| **Type 2** | Đóng dòng cũ, INSERT dòng mới (mỗi version 1 dòng) | **Đầy đủ** — chuẩn công nghiệp | Thuộc tính ảnh hưởng phân tích theo thời gian (city, segment, plan) |
| **Type 3** | Thêm cột `previous_city` | Chỉ **1 bước** trước | Hiếm — so sánh "trước/sau một lần tái cấu trúc" |

Cấu trúc SCD type 2:

```
seller_key(hash)  seller_id  seller_city  valid_from   valid_to     is_current
a91f...           abc123     sao paulo    2016-01-01   2018-01-15   false     ← version cũ, ĐÓNG
7c2e...           abc123     rio          2018-01-15   9999-12-31   true      ← version hiện hành
```

- Fact join dimension **theo thời điểm sự kiện**: `ON f.seller_id = d.seller_id AND f.event_date >= d.valid_from AND f.event_date < d.valid_to` (hoặc fact lưu sẵn `seller_key` — join lúc build fact, đọc nhanh hơn).
- `valid_to` của dòng hiện hành: dùng `9999-12-31` (dễ viết range query) — nhớ nhất quán toàn kho.
- MERGE code đầy đủ ở Section 5 + 6.

### 3.7. Wide table (One Big Table) vs star schema — trade-off hiện đại

Trào lưu lakehouse: "join sẵn hết thành một bảng siêu rộng (OBT), Parquet columnar chỉ đọc cột cần, lo gì". Sự thật:

| Tiêu chí | Star schema | Wide table (OBT) |
|---|---|---|
| Tốc độ đọc BI | Phải join (nhưng dim nhỏ → broadcast, rẻ) | **Nhanh nhất** — zero join |
| Dim thay đổi (seller sửa tên) | Sửa 1 dòng dim | **Rebuild/merge cả bảng khổng lồ** |
| SCD, tính nhất quán | Chuẩn chỉnh, 1 nguồn sự thật | Lịch sử "đóng băng" tại lúc build (đôi khi lại là điều bạn muốn!) |
| Storage | Nhỏ (chuẩn hóa) | Lặp dữ liệu dim hàng triệu lần (Parquet nén tốt nên đỡ) |
| Governance | Rõ ràng | Cột mọc um tùm, không ai dám xóa |

Lời khuyên thực chiến: **star schema làm xương sống gold**, rồi **derive** thêm 1–2 wide table từ star cho các dashboard nặng nhất. Wide table là **cache có thể vứt đi rebuild**, không phải nguồn sự thật.

---

## 4. Internal

Hai câu hỏi "bên trong" quyết định thiết kế của bạn chạy nhanh hay chậm:

### 4.1. Star schema join trong Spark = broadcast join

Dimension vài nghìn→vài triệu dòng, dưới ngưỡng `spark.sql.autoBroadcastJoinThreshold` (mặc định 10MB) hoặc bạn hint `F.broadcast(dim)`: Spark gửi nguyên dim đến mọi executor, join tại chỗ, **không shuffle fact table**. Đây là lý do kỹ thuật khiến star schema sống khỏe trên Spark: fact khổng lồ đứng yên, dim bé bay đến. (Ôn lesson về join strategy — module 3.)

```
fact 100 triệu dòng (đứng yên trên executor)
   ⊕ broadcast dim_products (2MB bay đến từng executor)  → KHÔNG shuffle
so với:
fact 100 triệu dòng shuffle theo product_id  → đắt nhất trong Spark
```

### 4.2. MERGE SCD2 trên Iceberg — chuyện gì xảy ra dưới gầm

`MERGE INTO` trên Iceberg (đã học lesson 31) thực hiện: (1) join source với target tìm dòng match; (2) viết lại **những data file chứa dòng bị UPDATE** (copy-on-write mặc định) + ghi file mới cho dòng INSERT; (3) commit **1 snapshot mới nguyên tử** — người đọc thấy hoặc toàn bộ thay đổi hoặc không gì cả. Hệ quả thiết kế:

- Dim SCD2 update ít dòng mỗi ngày → copy-on-write ổn. Bảng bị update dày đặc → cân nhắc merge-on-read (`write.update.mode=merge-on-read`).
- Mỗi lần MERGE = 1 snapshot → time travel được cả **lịch sử của lịch sử** ("dim_sellers trông thế nào hôm qua").
- Fact partition theo ngày sự kiện (hidden partitioning `days(event_ts)` — lesson 33) để BI query theo khoảng thời gian chỉ chạm đúng partition cần.

---

## 5. API

### `CREATE TABLE ... USING iceberg` + `PARTITIONED BY`

```python
spark.sql("""
CREATE TABLE IF NOT EXISTS lake.gold.fact_order_items (
  order_id        STRING,
  order_item_id   INT,
  date_key        INT,
  product_key     BIGINT,
  seller_key      BIGINT,
  customer_key    BIGINT,
  order_status    STRING,
  price           DECIMAL(10,2),
  freight_value   DECIMAL(10,2),
  order_ts        TIMESTAMP
) USING iceberg
PARTITIONED BY (months(order_ts))
""")
```
- **Pitfall**: partition theo cột cardinality cao (`seller_id`) → nghìn partition bé tí, small files. Fact: partition theo THỜI GIAN; dim: thường **không partition** (bé).

### `MERGE INTO` — SCD type 1 (đè)

```python
spark.sql("""
MERGE INTO lake.gold.dim_products t
USING staged_products s
ON t.product_id = s.product_id
WHEN MATCHED AND t.hash_diff <> s.hash_diff THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```
- Điều kiện `hash_diff <> hash_diff`: tránh rewrite file cho dòng không đổi gì — MERGE rẻ đi đáng kể.

### `MERGE INTO` — SCD type 2 (mẫu "union trick", học thuộc)

MERGE chỉ cho mỗi dòng target match 1 lần, nhưng SCD2 cần làm 2 việc (đóng dòng cũ + chèn dòng mới) → nhân đôi source bằng UNION, một nửa mang khóa join thật (để match→UPDATE đóng dòng), một nửa mang khóa NULL (không match→INSERT dòng mới):

```python
spark.sql("""
MERGE INTO lake.gold.dim_sellers t
USING (
  -- nhánh 1: dùng để MATCH và đóng version cũ
  SELECT s.seller_id AS merge_key, s.* FROM staged_sellers s
  UNION ALL
  -- nhánh 2: merge_key NULL -> không bao giờ match -> rơi vào INSERT
  SELECT NULL AS merge_key, s.* FROM staged_sellers s
  JOIN lake.gold.dim_sellers d
    ON s.seller_id = d.seller_id AND d.is_current = true
  WHERE s.hash_diff <> d.hash_diff
) src
ON t.seller_id = src.merge_key AND t.is_current = true
WHEN MATCHED AND t.hash_diff <> src.hash_diff THEN
  UPDATE SET t.is_current = false, t.valid_to = src.effective_date
WHEN NOT MATCHED THEN
  INSERT (seller_key, seller_id, seller_city, seller_state, hash_diff,
          valid_from, valid_to, is_current)
  VALUES (xxhash64(src.seller_id, CAST(src.effective_date AS STRING)),
          src.seller_id, src.seller_city, src.seller_state, src.hash_diff,
          src.effective_date, DATE'9999-12-31', true)
""")
```
- Seller **mới hoàn toàn** đi nhánh 1, không match (chưa có trong dim) → INSERT. Seller **đổi thông tin** xuất hiện ở cả 2 nhánh: nhánh 1 đóng dòng cũ, nhánh 2 chèn version mới. Seller **không đổi** chỉ ở nhánh 1, match nhưng fail điều kiện `hash_diff <>` → không làm gì.

### `F.xxhash64` / `F.sha2` — surrogate & hash_diff

```python
staged = (silver_sellers
  .withColumn("hash_diff", F.sha2(F.concat_ws("||",
       F.coalesce("seller_city", F.lit("")),
       F.coalesce("seller_state", F.lit(""))), 256))
  .withColumn("effective_date", F.current_date()))
staged.createOrReplaceTempView("staged_sellers")
```
- **Pitfall**: quên `coalesce` — `concat_ws` bỏ qua NULL nên `("hn", NULL)` và `(NULL, "hn")` có thể cho cùng hash. Chuẩn hóa NULL→"" và có delimiter `||` rõ ràng.

### `sequence` + `explode` — sinh `dim_date`

```python
dim_date = spark.sql("""
  SELECT CAST(date_format(d,'yyyyMMdd') AS INT) AS date_key, d AS full_date,
         year(d) year, month(d) month, day(d) day, quarter(d) quarter,
         date_format(d,'EEEE') day_name, weekofyear(d) week_of_year,
         dayofweek(d) IN (1,7) AS is_weekend
  FROM (SELECT explode(sequence(DATE'2016-01-01', DATE'2018-12-31')) AS d)
""")
```
- `date_key` dạng `20180115`: người đọc raw thấy hiểu ngay, sort đúng thứ tự thời gian.

---

## 6. Demo nhỏ

SCD2 thu nhỏ — 1 seller chuyển thành phố, xem dim mọc thêm version:

```
Ngày 1: sellers = [(s1, "sao paulo"), (s2, "campinas")]  → dim: 2 dòng current
Ngày 2: sellers = [(s1, "rio"),       (s2, "campinas")]  → s1 đổi city
Kỳ vọng: dim 3 dòng — s1 sao paulo (closed), s1 rio (current), s2 campinas (current)
```

```python
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("demo34-scd2")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "/workspace/warehouse")
    .getOrCreate())

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.demo")
spark.sql("DROP TABLE IF EXISTS lake.demo.dim_sellers")
spark.sql("""CREATE TABLE lake.demo.dim_sellers (
  seller_key BIGINT, seller_id STRING, seller_city STRING, hash_diff STRING,
  valid_from DATE, valid_to DATE, is_current BOOLEAN) USING iceberg""")

def scd2_apply(rows, effective_date):
    (spark.createDataFrame(rows, ["seller_id", "seller_city"])
        .withColumn("hash_diff", F.sha2(F.coalesce("seller_city", F.lit("")), 256))
        .withColumn("effective_date", F.lit(effective_date).cast("date"))
        .createOrReplaceTempView("staged_sellers"))
    spark.sql("""
      MERGE INTO lake.demo.dim_sellers t
      USING (
        SELECT s.seller_id AS merge_key, s.* FROM staged_sellers s
        UNION ALL
        SELECT NULL, s.* FROM staged_sellers s
        JOIN lake.demo.dim_sellers d
          ON s.seller_id = d.seller_id AND d.is_current
        WHERE s.hash_diff <> d.hash_diff
      ) src
      ON t.seller_id = src.merge_key AND t.is_current
      WHEN MATCHED AND t.hash_diff <> src.hash_diff THEN
        UPDATE SET t.is_current = false, t.valid_to = src.effective_date
      WHEN NOT MATCHED THEN
        INSERT (seller_key, seller_id, seller_city, hash_diff,
                valid_from, valid_to, is_current)
        VALUES (xxhash64(src.seller_id, CAST(src.effective_date AS STRING)),
                src.seller_id, src.seller_city, src.hash_diff,
                src.effective_date, DATE'9999-12-31', true)""")

scd2_apply([("s1", "sao paulo"), ("s2", "campinas")], "2018-01-01")
scd2_apply([("s1", "rio"),       ("s2", "campinas")], "2018-01-15")

spark.sql("""SELECT seller_id, seller_city, valid_from, valid_to, is_current
             FROM lake.demo.dim_sellers ORDER BY seller_id, valid_from""").show()
# s1 | sao paulo | 2018-01-01 | 2018-01-15 | false
# s1 | rio       | 2018-01-15 | 9999-12-31 | true
# s2 | campinas  | 2018-01-01 | 9999-12-31 | true   ← chạy 2 lần vẫn chỉ 1 dòng
spark.stop()
```

Chạy lại `scd2_apply` ngày 2 lần nữa — dim **không đổi** (hash_diff bằng nhau). Đó chính là idempotency mà lesson 36 sẽ khai thác.

---

## 7. Production Example

Thiết kế gold layer Olist hoàn chỉnh — đây là "bản vẽ" bạn sẽ dựng trong lab:

```
BRONZE (append-only, giữ vĩnh viễn)          SILVER (sạch, grain gốc)
  bronze.orders          ─────────────►        silver.orders          (PK order_id)
  bronze.order_items     ─────────────►        silver.order_items     (PK order_id+order_item_id)
  bronze.products        ─────────────►        silver.products        (PK product_id, category đã dịch EN)
  bronze.sellers         ─────────────►        silver.sellers         (PK seller_id)
  bronze.customers       ─────────────►        silver.customers       (PK customer_id)
                                                        │
                                                        ▼
GOLD (star schema)
  dim_date          (sinh bằng sequence, 2016→2018, key = yyyyMMdd)
  dim_products      (SCD1 — sửa category/weight thì đè, không cần lịch sử)
  dim_customers     (SCD1 — Olist customer_id là "per-order id", dùng
                     customer_unique_id làm natural key thật — bẫy nổi tiếng!)
  dim_sellers       (SCD2 — city/state ảnh hưởng phân tích logistics theo thời gian)
  fact_order_items  (grain: 1 dòng = 1 item; measures: price, freight_value;
                     partition months(order_ts); FK = *_key hash)
  agg_revenue_daily (bảng aggregate derive từ fact — dashboard chạm cái này trước)
```

Quyết định thiết kế & lý do (kiểu tài liệu design review công ty thật):

1. **Grain item-level**: Olist là marketplace, 1 đơn nhiều seller → grain đơn không trả lời được câu hỏi seller/category (Section 3.4).
2. **dim_sellers SCD2, dim_products SCD1**: business hỏi "hiệu suất giao hàng theo bang của seller theo quý" → cần đúng bang tại thời điểm bán. Product category đổi = sửa phân loại, không ai phân tích "lịch sử đổi category".
3. **fact lưu sẵn `seller_key`** (resolve SCD2 lúc build fact bằng range join theo `order_ts`), BI khỏi làm range join mỗi query.
4. **Metric contract**: "revenue = SUM(price) của item thuộc đơn `delivered`" — định nghĩa ghi vào bảng gold, mọi dashboard dùng chung. Hết cãi nhau 1.2 tỷ vs 1.4 tỷ.

---

## 8. Hands-on Lab

**Mục tiêu**: dựng trọn bronze → silver → gold cho Olist trên Iceberg, có SCD2 thật.

### Bước 0 — chuẩn bị

Cluster đã bật (`make up`). Dataset tại `data/olist/*.csv` (mount vào container tại `/workspace/data/olist/`). Tạo thư mục `labs/lab34/`.

### Bước 1 — `labs/lab34/session.py` (dùng chung cho cả module)

```python
from pyspark.sql import SparkSession

def iceberg_session(app_name):
    return (SparkSession.builder.appName(app_name)
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", "/workspace/warehouse")
        .getOrCreate())
```

### Bước 2 — `labs/lab34/step1_bronze.py`

```python
from pyspark.sql import functions as F
from session import iceberg_session

spark = iceberg_session("lab34-bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")

SOURCES = ["orders", "order_items", "products", "sellers", "customers"]
for name in SOURCES:
    path = f"/workspace/data/olist/olist_{name}_dataset.csv"
    df = (spark.read.csv(path, header=True)          # KHÔNG inferSchema, KHÔNG cast:
          .withColumn("_ingest_ts", F.current_timestamp())   # bronze giữ nguyên string
          .withColumn("_source_file", F.input_file_name()))
    df.writeTo(f"lake.bronze.{name}").createOrReplace()      # lab: replace cho dễ chạy lại
    print(f"bronze.{name}: {df.count():,} dòng")
spark.stop()
```

### Bước 3 — `labs/lab34/step2_silver.py` — cast schema tường minh, dedupe, chuẩn hóa

```python
from pyspark.sql import functions as F, Window as W
from session import iceberg_session

spark = iceberg_session("lab34-silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")

orders = (spark.table("lake.bronze.orders")
    .withColumn("order_status", F.lower(F.trim("order_status")))
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("rn", F.row_number().over(                       # dedupe: giữ bản ingest mới nhất
        W.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc())))
    .filter("rn = 1").drop("rn", "_ingest_ts", "_source_file")
    .filter(F.col("order_id").isNotNull()))
orders.writeTo("lake.silver.orders").createOrReplace()

items = (spark.table("lake.bronze.order_items")
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("price", F.col("price").cast("decimal(10,2)"))
    .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
    .dropDuplicates(["order_id", "order_item_id"])
    .drop("_ingest_ts", "_source_file"))
items.writeTo("lake.silver.order_items").createOrReplace()

for name in ["products", "sellers", "customers"]:
    (spark.table(f"lake.bronze.{name}")
        .dropDuplicates([f"{name[:-1]}_id" if name != "customers" else "customer_id"])
        .drop("_ingest_ts", "_source_file")
        .writeTo(f"lake.silver.{name}").createOrReplace())
print("Silver xong.")
spark.stop()
```

### Bước 4 — `labs/lab34/step3_gold.py` — dim_date, dim SCD1/SCD2, fact

Tự lắp từ Section 5 + 6: (a) sinh `dim_date`; (b) `dim_products`, `dim_customers` MERGE SCD1 (nhớ dùng `customer_unique_id`); (c) `dim_sellers` MERGE SCD2 nguyên mẫu union trick; (d) `fact_order_items`:

```python
fact = (spark.table("lake.silver.order_items").alias("i")
    .join(spark.table("lake.silver.orders").alias("o"), "order_id")
    .join(spark.table("lake.gold.dim_sellers").alias("ds"),        # resolve SCD2 theo thời điểm
          (F.col("i.seller_id") == F.col("ds.seller_id")) &
          (F.col("o.order_purchase_timestamp") >= F.col("ds.valid_from")) &
          (F.col("o.order_purchase_timestamp") <  F.col("ds.valid_to")))
    .select("order_id", "order_item_id",
        F.date_format("o.order_purchase_timestamp", "yyyyMMdd").cast("int").alias("date_key"),
        F.xxhash64("i.product_id").alias("product_key"),
        F.col("ds.seller_key"),
        "o.order_status", "i.price", "i.freight_value",
        F.col("o.order_purchase_timestamp").alias("order_ts")))
fact.writeTo("lake.gold.fact_order_items").createOrReplace()
```

### Bước 5 — chạy & kiểm chứng

```bash
make run F=labs/lab34/step1_bronze.py
make run F=labs/lab34/step2_silver.py
make run F=labs/lab34/step3_gold.py
```

Kiểm chứng bắt buộc (viết `step4_verify.py`):
1. `SUM(price)` của fact (đơn delivered) == `SUM(price)` join tay từ silver — nếu lệch, join SCD2 làm rơi/ nhân dòng.
2. Đếm fact == đếm silver.order_items (không mất dòng vì range join).
3. Query ngôi sao: doanh thu theo `seller_state × month` — chạy được bằng đúng 1 câu SQL join dim.
4. Giả lập seller đổi city (UPDATE 1 dòng silver.sellers rồi chạy lại SCD2) → dim mọc version mới, fact CŨ vẫn trỏ version cũ.

Ghi 4 quan sát vào `labs/lab34/NOTES.md`.

---

## 9. Assignment

**Easy** — Vẽ (ASCII/giấy) diagram medallion cho Olist: mỗi tầng ghi rõ mục đích, ai đọc, contract 1 dòng. Trả lời: nếu ngày mai đổi định nghĩa "revenue" (tính cả freight), phải sửa những tầng nào, tầng nào không đụng?

**Medium** — SCD type 2 tay trần: chọn 3 seller trong `silver.sellers`, sửa `seller_city` (giả lập bằng DataFrame), chạy MERGE SCD2 ba "ngày" liên tiếp (ngày 2 sửa, ngày 3 không đổi). Chứng minh bằng query: (a) mỗi seller đổi có đúng 2 version; (b) chạy ngày 3 không sinh thêm dòng; (c) tổng `is_current=true` == tổng seller distinct.

**Hard** — Grain war: xây thêm `fact_orders` grain order-level (measures: `order_total = SUM(price)`, `n_items`). Viết 3 câu hỏi business mà `fact_orders` trả lời được và 3 câu **chỉ** `fact_order_items` trả lời được. Đo storage + thời gian query "doanh thu theo tháng" trên từng bảng. Kết luận 5 dòng: có đáng duy trì cả hai không?

**Production Challenge** — Xây `gold.wide_order_items` (OBT: fact join sẵn mọi dim, ~30 cột). So với star: (a) thời gian query "revenue theo category theo tháng"; (b) chuyện gì xảy ra khi seller đổi city — bảng nào phản ánh, bảng nào không, cái nào ĐÚNG cho báo cáo lịch sử? Viết 10 dòng khuyến nghị khi nào team nên thêm OBT.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Quyết định thiết kế | Ảnh hưởng | Tại sao |
|---|---|---|
| Grain mịn (item-level) | Fact to hơn ~13% (Olist) nhưng trả lời được mọi câu hỏi | Roll-up rẻ (1 lần aggregate); drill-down bất khả thi |
| Dim nhỏ + star | Join gần như miễn phí | Broadcast join — fact không shuffle |
| Hash key (xxhash64) vs sha2 | xxhash64 nhanh hơn nhiều, key 8 byte vs 64 hex char | BIGINT join/so sánh rẻ hơn STRING dài; sha2 khi cần chống collision tuyệt đối |
| `WHEN MATCHED AND hash_diff <>` | MERGE chỉ rewrite file thật sự có thay đổi | Copy-on-write: mỗi dòng update kéo theo rewrite cả data file chứa nó |
| Fact partition `months(order_ts)` | Query BI theo khoảng thời gian prune thẳng | Dim không partition — bé, đọc cả bảng còn rẻ hơn mở nhiều file |
| Aggregate table trên fact | Dashboard mở trong ms thay vì giây | Đổi tính toán lặp đi lặp lại lấy storage rẻ |

Tự vấn khi thiết kế: *"query phổ biến nhất của tầng này là gì — schema có đang tối ưu cho ĐÚNG query đó không?"*

---

## 11. Spark UI

Chạy `step3_gold.py` rồi soi tab **SQL / DataFrame**:

- Query build fact: tìm node **BroadcastHashJoin** cho các join dim — nếu thấy **SortMergeJoin** với dim vài MB, kiểm tra vì sao không broadcast (thống kê thiếu? threshold?). Đây là lần đầu bạn *thẩm định thiết kế* bằng UI thay vì chỉ debug.
- Query MERGE SCD2: quan sát nó là một job **join + write** — thấy rõ số output rows của nhánh MATCHED vs NOT MATCHED (mở node `MergeRows` / thống kê write). Chạy MERGE lần 2 không có thay đổi → số dòng ghi ra ~0, job ngắn hẳn: bằng chứng `hash_diff` guard hoạt động.
- Tab **Jobs**: range join SCD2 (fact resolve seller_key) là join đắt nhất pipeline — ghi lại duration để so khi bạn tối ưu ở assignment.

---

## 12. Common Mistakes

1. **"Làm sạch luôn ở bronze cho tiện"** → mất khả năng replay. Bronze mà không raw thì chỉ là silver có tên sai — và bạn đã vứt hóa đơn nhập kho.
2. **Cho BI/analyst đọc thẳng silver hoặc bronze** "tạm thời thôi mà" → mỗi dashboard một định nghĩa metric, medallion chết lâm sàng.
3. **Không tuyên bố grain trước khi viết code** → measure khác grain lẻn vào bảng (order_total trong bảng item) → SUM sai lặng lẽ, khó phát hiện nhất trong mọi loại bug.
4. **Dùng `monotonically_increasing_id()` làm surrogate key** → chạy lại job ra key khác → fact trỏ sai dim, idempotency vỡ. Hash key deterministic mới sống được trong lakehouse.
5. **SCD2 mà quên lọc `is_current = true` trong điều kiện MERGE ON** → match cả version đã đóng → "cardinality violation" hoặc đóng nhầm lịch sử.
6. **`concat_ws` tính hash_diff không `coalesce` NULL** → hai bản ghi khác nhau cùng hash → thay đổi không được phát hiện.
7. **SCD2 hóa mọi dimension cho "an toàn"** → dim phình, pipeline phức tạp, không ai dùng lịch sử đó. Chỉ SCD2 thuộc tính mà business thật sự phân tích theo thời gian.
8. **Bẫy Olist**: dùng `customer_id` làm natural key của dim_customers — nó là id per-order! Một người mua 5 lần = 5 customer_id. Phải dùng `customer_unique_id`.

---

## 13. Interview

**Junior:**

1. *Medallion architecture là gì, kể tên và mục đích từng tầng?* — Kiến trúc tổ chức lakehouse 3 tầng: bronze lưu raw đúng như nguồn (append-only, để replay/audit); silver là dữ liệu đã làm sạch, đúng kiểu, unique — source of truth; gold là dữ liệu model hóa phục vụ business (star schema, aggregate) cho BI/analyst. Tầng sau chỉ đọc tầng ngay trước.
2. *Fact table và dimension table khác nhau thế nào?* — Fact ghi sự kiện, chứa measure số học + FK, rất dài và hẹp, append liên tục. Dimension mô tả ngữ cảnh (ai/cái gì/khi nào/ở đâu), ngắn và rộng, thay đổi chậm. Fact trả lời "bao nhiêu", dimension trả lời "theo cái gì".
3. *Grain của fact table là gì? Cho ví dụ.* — Định nghĩa "một dòng đại diện cho cái gì". Ví dụ Olist: grain order-item-level = 1 dòng là 1 sản phẩm trong 1 đơn. Phải tuyên bố grain trước khi chọn cột; mọi measure trong bảng phải cùng grain.
4. *Tại sao bronze phải giữ dữ liệu raw?* — (a) Replay: bug logic silver → chạy lại từ bronze; (b) audit: truy nguồn gốc mọi con số; (c) nhu cầu tương lai chưa biết trước; storage rẻ hơn nhiều so với mất dữ liệu gốc.

**Mid:**

5. *SCD type 1, 2, 3 khác nhau thế nào, khi nào dùng gì?* — Type 1 update đè, mất lịch sử — cho sửa lỗi/thuộc tính không cần lịch sử. Type 2 mỗi thay đổi một dòng version với valid_from/valid_to/is_current, giữ trọn lịch sử — chuẩn cho thuộc tính ảnh hưởng phân tích theo thời gian. Type 3 thêm cột previous_x, giữ đúng 1 bước — hiếm dùng.
6. *Surrogate key là gì, sao không dùng luôn natural key?* — Khóa vô nghĩa warehouse tự sinh cho mỗi dòng dim. Natural key không phân biệt được các version SCD2 của cùng entity, và có thể bị nguồn đổi/tái sử dụng. Trong lakehouse dùng hash key (xxhash64/sha2 của natural key + valid_from) vì không có auto-increment và cần deterministic để re-run an toàn.
7. *Star schema chạy trên Spark có đắt không? Vì sao?* — Rẻ hơn vẻ ngoài: dim nhỏ được broadcast join, fact không phải shuffle. Chi phí thật nằm ở shuffle; star schema thiết kế đúng gần như không gây shuffle fact khi query.
8. *Chọn grain order-level hay order-item-level cho marketplace?* — Item-level. Một đơn chứa nhiều seller/category → grain order không phân bổ được doanh thu theo seller/category, và từ grain thô không bao giờ drill-down được. Nguyên tắc: lấy grain mịn nhất nguồn cho phép, cần nhanh thì xây aggregate bên trên.

**Senior:**

9. *MERGE SCD2 cần làm 2 việc trên cùng source (đóng dòng cũ + chèn dòng mới) trong khi MERGE chỉ match mỗi dòng 1 lần — bạn giải quyết thế nào?* — Union trick: nhân đôi source; nhánh 1 mang merge_key = natural key để match và UPDATE đóng version hiện hành; nhánh 2 (chỉ gồm bản ghi có hash_diff đổi) mang merge_key NULL nên luôn rơi vào WHEN NOT MATCHED → INSERT version mới. Guard `hash_diff <>` để bản ghi không đổi không gây rewrite. Toàn bộ trong 1 transaction Iceberg → atomic.
10. *Team đề xuất bỏ star schema, build một wide table cho tất cả. Bạn phản biện thế nào?* — OBT đọc nhanh nhất (zero join) nhưng: dim đổi phải rebuild bảng khổng lồ; lịch sử đóng băng tại lúc build; cột phình không kiểm soát; nhiều use case = nhiều OBT lệch nhau → quay lại bài toán nhất quán. Đề xuất: star làm nguồn sự thật ở gold, derive OBT như cache rebuild được cho vài dashboard nặng nhất — được cả tốc độ lẫn governance. Nếu chỉ có 1-2 consumer và dữ liệu bất biến (event log) thì OBT thuần là hợp lý — trade-off, không giáo điều.

---

## 14. Summary

### Mindmap

```
                        LESSON 34 — MEDALLION & MODELING
                                     │
      ┌──────────────┬───────────────┼────────────────┬─────────────────┐
      ▼              ▼               ▼                ▼                 ▼
  MEDALLION      STAR SCHEMA       GRAIN         KEYS & SCD         TRADE-OFF
      │              │               │                │                 │
  bronze=raw     fact (measure   1 dòng = gì?    natural vs        star = xương
  (replay,       + FK, dài)      chọn MỊN nhất  surrogate         sống gold
  audit)         dim (ngữ cảnh,  roll-up được   hash key          OBT = cache
  silver=truth   ngắn, rộng)     drill-down     (xxhash64,        derive thêm
  (sạch, PK)     broadcast join  thì không      deterministic)    khi cần đọc
  gold=business  → không shuffle                SCD1 đè           nhanh
  (star, metric  fact            KHÔNG trộn     SCD2 version +
  thống nhất)                    2 grain        MERGE union trick
```

### Checklist trước khi gõ "Continue"

- [ ] Nói được contract của bronze/silver/gold và AI được đọc tầng nào.
- [ ] Giải thích được 2 lý do bronze giữ raw (replay, audit) bằng ví dụ cụ thể.
- [ ] Tuyên bố grain cho fact Olist và bảo vệ được lựa chọn item-level.
- [ ] Viết được MERGE SCD2 union trick không nhìn tài liệu, giải thích từng nhánh.
- [ ] Biết vì sao lakehouse dùng hash key thay auto-increment.
- [ ] Đã dựng đủ bronze→silver→gold Olist trong lab và pass 4 kiểm chứng.
- [ ] Trả lời được 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Lesson 35 — Data quality & testing: constraints, dbt patterns.**

Bạn vừa xây một gold layer đẹp — nhưng đẹp hôm nay thôi. Ngày mai nguồn gửi lên `order_id` NULL, giá âm, seller_id không tồn tại trong dim... và MERGE của bạn sẽ trung thành chép rác vào "source of truth". Star schema mà mất niềm tin của BI thì công thiết kế hôm nay đổ sông. Lesson 35 biến chất lượng dữ liệu thành **hợp đồng có thể thực thi**: uniqueness, referential integrity, freshness, volume anomaly — tự viết QC framework bằng PySpark, quyết định khi nào fail hard khi nào quarantine, và lưu report thành bảng Iceberg để nhìn trend.

Model đúng mà data sai thì vẫn là sai — nên ta học kiểm chứng ngay bây giờ.

> Gõ **"Continue"** khi sẵn sàng.
