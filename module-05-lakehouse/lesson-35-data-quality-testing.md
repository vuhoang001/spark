# Lesson 35 — Data quality & testing: constraints, dbt patterns

> Module 5 · Lakehouse & Iceberg · Tuần 18 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao data quality là **hợp đồng** — và garbage lọt vào gold layer thì mất thứ gì (spoiler: thứ đắt nhất là niềm tin).
- Phân loại đầy đủ các **check**: schema, uniqueness (PK), referential integrity (FK), not null, accepted values, range, freshness, volume anomaly.
- Tự viết một **QC framework nhỏ bằng PySpark**: mỗi check là một hàm trả về dòng report, gom thành report DataFrame.
- Chiến lược khi check fail: **fail hard vs quarantine** — bảng quyết định.
- Lưu QC report thành **bảng Iceberg** và nhìn **trend** theo thời gian.
- Bản đồ công cụ: **dbt tests, Great Expectations, Soda** — chọn gì khi nào.
- **Alert** khi fail vượt ngưỡng %.

Sau bài này bạn phải làm được:

- Liệt kê bộ check tối thiểu cho một bảng bất kỳ trong 2 phút.
- Cắm QC framework vào pipeline bronze→silver của lab 34, chặn được dữ liệu bẩn trước khi nó chạm silver.
- Trả lời câu "pipeline xanh mà số liệu sai thì lỗi tại ai?" như một Senior.

Kiến thức dùng trong thực tế: pipeline không có test = pipeline chưa xong. Ở các công ty data-mature, **không một bảng nào lên gold mà không có test đi kèm** — giống code không có unit test không được merge.

---

## 2. Why

### "Pipeline xanh" không có nghĩa là "dữ liệu đúng"

Câu chuyện có thật ở mọi công ty: Airflow toàn màu xanh suốt 2 tuần. Rồi CFO mở dashboard: doanh thu tuần này **gấp 3 lần** tuần trước. Ăn mừng? Không — team nguồn deploy bản mới, gửi trùng mỗi event 3 lần. Pipeline của bạn không có check uniqueness, MERGE thành append, gold nhân ba.

Chi phí thật sự không phải là sửa dữ liệu (một ngày công). Chi phí thật là: **từ nay CFO không tin dashboard nữa**. Mỗi con số phải "để anh nhờ bạn X kiểm tra lại bằng Excel". Data team từ "nguồn sự thật" rớt xuống "nguồn tham khảo". Xây niềm tin mất một năm, mất niềm tin mất một buổi sáng.

### Data quality là HỢP ĐỒNG

Lesson 34 định nghĩa contract của từng tầng medallion: "silver có PK unique, đúng kiểu, không NULL ở cột bắt buộc". Nhưng contract viết trong tài liệu là contract **chết** — không ai thực thi. Data quality check biến contract thành **code chạy mỗi lần pipeline chạy**:

```
Contract trên giấy:   "silver.orders unique theo order_id"        → hy vọng
Contract bằng code:   check_unique(silver.orders, ["order_id"])   → chặn thật
                      fail → pipeline DỪNG, gold hôm nay không có
                      dữ liệu bẩn, alert bắn về Slack
```

Nguyên tắc vàng: **thà dashboard trống còn hơn dashboard sai**. Dashboard trống, mọi người hỏi "sao chưa có số?" — bạn trả lời "nguồn gửi dữ liệu lỗi, đã chặn ở cổng, đang làm việc với team nguồn". Đó là hình ảnh của một data team kiểm soát được hệ thống. Dashboard sai mà không ai biết mới là thảm họa.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| Chặn rác trước khi vào source of truth | Thêm compute mỗi lần chạy (mỗi check ~1 scan/aggregation) |
| Phát hiện sự cố NGUỒN sớm hơn cả team nguồn | Check quá gắt → pipeline dừng vì chuyện vặt, on-call mệt mỏi |
| Niềm tin của BI = data team có tiếng nói | Phải bảo trì bộ test khi schema/business đổi |
| Report trend = nhìn thấy chất lượng xuống cấp dần | Không bao giờ đủ — test chỉ chứng minh sự có mặt của lỗi đã lường trước |

> Bài học Senior: đừng hỏi "có nên viết test không" — hãy hỏi "check này fail thì pipeline nên DỪNG hay nên CÁCH LY dòng lỗi rồi đi tiếp?". Đó mới là quyết định khó (Section 3.3).

---

## 3. Theory

### 3.1. Phân loại check — bộ từ vựng chuẩn ngành

Xếp từ "cấu trúc" đến "nội dung" đến "hành vi":

| # | Loại check | Câu hỏi | Ví dụ Olist | Tương đương dbt |
|---|---|---|---|---|
| 1 | **Schema check** | Cột/kiểu có đúng như khai báo? Có cột lạ xuất hiện? | `price` phải DECIMAL, không được thành STRING | (contract trong dbt) |
| 2 | **Not null** | Cột bắt buộc có NULL không? | `order_id`, `order_status` không được NULL | `not_null` |
| 3 | **Uniqueness (PK)** | Business key có trùng không? | `order_id` unique trong orders; (`order_id`,`order_item_id`) unique trong items | `unique` |
| 4 | **Referential integrity (FK)** | Mọi FK có tồn tại bên bảng cha? | Mọi `product_id` trong items phải có trong products | `relationships` |
| 5 | **Accepted values** | Giá trị nằm trong tập cho phép? | `order_status` ∈ {delivered, shipped, canceled, ...} | `accepted_values` |
| 6 | **Range** | Số/ngày nằm trong khoảng hợp lý? | `price > 0`, `order_ts` không ở tương lai | custom test |
| 7 | **Freshness** | Dữ liệu mới nhất cách đây bao lâu? | `MAX(_ingest_ts)` phải < 24h trước | `source freshness` |
| 8 | **Volume anomaly** | Số dòng hôm nay có bất thường so với lịch sử? | Hôm nay 500 đơn trong khi trung bình 3.000 → nghi ngờ nguồn hụt | (elementary/soda) |

Ba nhóm theo bản chất — quan trọng vì cách xử lý fail khác nhau:

```
NHÓM A — cấu trúc (1,2,3):    sai là sai TUYỆT ĐỐI, phá vỡ contract kỹ thuật
                              → thường fail hard hoặc quarantine dòng lỗi
NHÓM B — nội dung (4,5,6):    sai theo NGHIỆP VỤ, có thể có ngoại lệ hợp lệ
                              → quarantine + báo, hiếm khi dừng cả pipeline
NHÓM C — hành vi (7,8):       không có dòng nào "sai", cả TẬP dữ liệu khả nghi
                              → cảnh báo con người, vì máy không đủ ngữ cảnh
```

### 3.2. Đặt check ở đâu trong medallion?

```
NGUỒN ──► BRONZE ──[CỔNG QC #1]──► SILVER ──[CỔNG QC #2]──► GOLD ──[CỔNG QC #3]──► BI
           │
           └─ KHÔNG check chặn ở đây! Bronze nhận tất (contract lesson 34).
              Chỉ đo đếm (row count, freshness) để phát hiện nguồn chết.

CỔNG #1 (quan trọng nhất): schema, not null, unique, accepted values, range
         → quyết định dòng nào được vào silver, dòng nào vào quarantine
CỔNG #2: referential integrity giữa các bảng silver, volume anomaly
         → fact build từ dữ liệu đã "chứng nhận"
CỔNG #3: business sanity trên gold — tổng doanh thu hôm nay > 0,
         fact đếm khớp silver, % thay đổi so hôm qua trong ngưỡng
```

> **Analogy sân bay**: bronze là khu vực công cộng — ai vào cũng được nhưng camera ghi hết. Cổng QC #1 là security check — soi từng người (từng dòng), ai mang đồ cấm thì mời vào phòng riêng (quarantine), không đóng cả sân bay. Cổng #3 như kiểm tra cuối trước cửa máy bay — nhanh, tổng thể, nhưng phát hiện gì nghiêm trọng thì máy bay không cất cánh (gold không publish).

### 3.3. Fail hard vs quarantine — bảng quyết định

Hai chiến lược khi check fail:

- **Fail hard**: raise exception → task đỏ → pipeline dừng → downstream không chạy → con người xử lý. Dữ liệu cũ (hôm qua) vẫn phục vụ BI.
- **Quarantine**: tách dòng lỗi vào bảng `quarantine.<table>` kèm lý do + timestamp, dòng sạch đi tiếp. Pipeline sống, nhưng thiếu một phần dữ liệu (phải theo dõi và "chữa" quarantine).

| Tình huống | Chiến lược | Lý do |
|---|---|---|
| Schema đổi đột ngột (cột biến mất, kiểu đổi) | **Fail hard** | Toàn bộ batch khả nghi; xử lý từng dòng vô nghĩa |
| PK trùng **hàng loạt** (>X%) | **Fail hard** | Dấu hiệu nguồn gửi đúp cả batch — đi tiếp là nhân bản doanh thu |
| PK trùng lác đác (<X%) | **Quarantine** (giữ bản mới nhất, cách ly bản thừa) | Trùng lẻ tẻ là chuyện thường của hệ thống phân tán |
| NULL ở cột bắt buộc, vài dòng | **Quarantine** | Vài dòng rác không đáng dừng báo cáo toàn công ty |
| FK mồ côi (product chưa kịp sync) | **Quarantine + retry lần sau** | Thường do lệch nhịp ingest giữa 2 bảng — tự lành ở lần chạy sau |
| Giá trị ngoài range (price < 0) | **Quarantine** | Cần người nhìn: bug nguồn hay nghiệp vụ mới (refund?) |
| Freshness quá hạn | **Fail hard** (skip cũng được) | Không có dữ liệu mới thì chạy transform vô ích |
| Volume anomaly | **Cảnh báo, vẫn chạy** | Máy không đủ ngữ cảnh (hôm nay là Tết?); người quyết định |

Quy tắc gộp: **lỗi mang tính hệ thống (systemic) → fail hard; lỗi mang tính cá thể (per-row) → quarantine; lỗi mang tính thống kê → alert cho người**. Và mọi quarantine phải có **ngưỡng tràn**: quarantine > N% batch → nâng cấp thành fail hard (khi 40% số dòng "lác đác" thì nó không còn lác đác nữa).

### 3.4. QC report là dữ liệu — hãy đối xử với nó như dữ liệu

Mỗi lần chạy check, kết quả ghi thành **dòng** vào bảng Iceberg `lake.audit.qc_results`:

```
run_ts               table            check_name          status  failed  total    failed_pct
2026-07-08 02:10:11  silver.orders    unique_pk           PASS    0       99441    0.0
2026-07-08 02:10:14  silver.orders    not_null_order_id   PASS    0       99441    0.0
2026-07-08 02:10:19  silver.items     fk_product_id       FAIL    312     112650   0.28
```

Lợi ích kép: (a) pipeline đọc bảng này để quyết định đi tiếp/dừng; (b) **trend**: `failed_pct` của `fk_product_id` tăng dần 0.1% → 0.3% → 0.8% qua 3 tuần = nguồn đang xuống cấp từ từ — loại sự cố mà không một lần chạy đơn lẻ nào phát hiện được. Chất lượng dữ liệu là đường cong, không phải điểm.

### 3.5. Bản đồ công cụ: dbt tests / Great Expectations / Soda

Framework tự viết của ta (Section 5) dạy bạn **bản chất**; ngoài đời bạn sẽ gặp 3 cái tên này:

| Tiêu chí | dbt tests | Great Expectations (GX) | Soda |
|---|---|---|---|
| Triết lý | Test gắn vào model SQL, chạy sau khi build | "Expectation suite" — thư viện check đồ sộ, data docs | Check as config (YAML, ngôn ngữ SodaCL) |
| Khai báo | YAML cạnh model: `unique`, `not_null`, `relationships`, `accepted_values` + custom SQL | Python/YAML: `expect_column_values_to_be_between`, ~300 expectations | `checks for orders: - row_count > 0` |
| Chạy trên | Warehouse/engine SQL (Trino/Spark SQL qua adapter) | Pandas/Spark/SQL | Spark/warehouse qua connector |
| Điểm mạnh | Đã dùng dbt thì test "miễn phí", gần transform nhất | Profiling + tài liệu tự sinh, expectation phong phú | Nhẹ, ngôn ngữ check dễ đọc cho non-engineer, monitoring/alert tích hợp |
| Điểm yếu | Chỉ sống trong thế giới dbt; volume/freshness cần package thêm (elementary) | Nặng, learning curve, "framework trong framework" | Ít phổ biến hơn 2 cái trên |
| Chọn khi | Stack đã có dbt | Cần suite check phong phú + documentation | Muốn check tách khỏi code transform, đội vận hành đọc được |

Cả ba đều quy về đúng những check ở bảng 3.1 — công cụ đổi, khái niệm không đổi. Học khái niệm (hôm nay) rồi công cụ nào cũng đọc hiểu trong một buổi chiều.

### 3.6. Alert — ai cần biết, khi nào, qua đâu

- **Ngưỡng %** thay vì con số tuyệt đối: "312 dòng FK mồ côi" không nói lên gì; "0.28% và tuần trước là 0.05%" mới là thông tin.
- Phân tầng: `WARN` (vượt ngưỡng thấp — ghi log + đưa vào report tuần) / `FAIL` (vượt ngưỡng cao — Slack/PagerDuty + chặn pipeline).
- Alert phải chứa đủ ngữ cảnh hành động: bảng nào, check nào, ngưỡng bao nhiêu, thực tế bao nhiêu, **query để xem dòng lỗi** (trỏ vào quarantine). Alert bắt người nhận đi đào lại từ đầu là alert tồi.
- Chống **alert fatigue**: check nào WARN 30 ngày liên tục mà không ai xử lý → hoặc nâng ngưỡng có chủ đích, hoặc sửa nguồn — đừng để thành tiếng ồn nền, vì ngày nó FAIL thật sẽ không ai nhìn.

---

## 4. Internal

Mỗi check chạy như thế nào trong Spark — hiểu để trả giá đúng:

```
check_not_null      →  1 aggregation: SUM(CASE WHEN col IS NULL)         → 1 scan, rẻ
check_unique        →  groupBy(pk).count().filter(>1)                    → 1 SHUFFLE theo pk
check_fk            →  left_anti join bảng con với bảng cha              → shuffle/broadcast join
check_accepted      →  filter ~isin(...)                                 → 1 scan, rẻ
check_range         →  filter (col < lo) | (col > hi)                    → 1 scan, rẻ
check_freshness     →  MAX(ts)                                           → 1 scan (Iceberg: gần free
                                                                            nhờ column stats trong manifest!)
check_volume        →  COUNT(*) hôm nay vs lịch sử từ bảng qc_results    → metadata + đọc bảng audit
```

Ba hệ quả thiết kế:

1. **Gom check cùng loại scan**: 5 check not-null trên cùng bảng = 1 câu `agg` duy nhất với 5 biểu thức SUM(CASE...), không phải 5 job. Framework của ta làm điều này ở `check_not_null(cols=[...])`.
2. **Iceberg metadata là bạn**: `COUNT(*)` và `MAX(partition_col)` trả lời được từ manifest mà không scan data — check freshness/volume trên bảng Iceberg gần như miễn phí. (Nhớ lesson 30: manifest lưu column stats từng data file.)
3. **check_unique và check_fk là 2 check đắt nhất** (shuffle). Trên bảng lớn, chạy chúng trên **partition mới ingest** thay vì full table mỗi lần — full scan để dành cho job audit tuần.

Và một chi tiết quan trọng: nếu pipeline vừa dùng DataFrame để check vừa dùng nó để ghi, hãy nhớ lazy evaluation — check là action riêng, transform là action riêng; **cache** DataFrame nguồn nếu không muốn đọc 2 lần (lesson 2 + 9).

---

## 5. API

Không có API mới nào của Spark hôm nay — có **framework của bạn**. Xây từ 4 viên gạch:

### Viên gạch 1 — chuẩn hóa kết quả: mọi check trả về cùng schema

```python
from pyspark.sql import functions as F
from datetime import datetime, timezone

REPORT_COLS = ["run_ts", "table_name", "check_name", "status",
               "failed_count", "total_count", "failed_pct"]

def _result(table, check, failed, total):
    pct = round(100.0 * failed / total, 4) if total else 0.0
    return (datetime.now(timezone.utc), table, check,
            "PASS" if failed == 0 else "FAIL", failed, total, pct)
```

### Viên gạch 2 — các hàm check (mỗi hàm 1 loại, nhận DataFrame, trả tuple)

```python
def check_not_null(df, table, cols):
    aggs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cols]
    row = df.agg(F.count(F.lit(1)).alias("_total"), *aggs).first()   # 1 job cho N cột
    return [_result(table, f"not_null_{c}", row[c] or 0, row["_total"]) for c in cols]

def check_unique(df, table, key_cols):
    total = df.count()
    dup = (df.groupBy(*key_cols).count().filter("count > 1")
             .agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0])
    return [_result(table, f"unique_{'_'.join(key_cols)}", int(dup), total)]

def check_fk(child_df, table, fk_col, parent_df, pk_col):
    total = child_df.count()
    orphans = (child_df.filter(F.col(fk_col).isNotNull())
               .join(parent_df.select(F.col(pk_col).alias(fk_col)).distinct(),
                     on=fk_col, how="left_anti").count())
    return [_result(table, f"fk_{fk_col}", orphans, total)]

def check_accepted_values(df, table, col, allowed):
    total = df.count()
    bad = df.filter(~F.col(col).isin(allowed) & F.col(col).isNotNull()).count()
    return [_result(table, f"accepted_values_{col}", bad, total)]

def check_range(df, table, col, lo=None, hi=None):
    total = df.count()
    cond = F.lit(False)
    if lo is not None: cond = cond | (F.col(col) < lo)
    if hi is not None: cond = cond | (F.col(col) > hi)
    return [_result(table, f"range_{col}", df.filter(cond).count(), total)]

def check_freshness(df, table, ts_col, max_lag_hours):
    latest = df.agg(F.max(ts_col)).first()[0]
    lag_ok = latest is not None and \
        (datetime.now(timezone.utc).replace(tzinfo=None) - latest).total_seconds() \
        <= max_lag_hours * 3600
    return [_result(table, f"freshness_{ts_col}", 0 if lag_ok else 1, 1)]
```

### Viên gạch 3 — gom về report DataFrame + ghi Iceberg

```python
def build_report(spark, results):          # results: list các tuple từ mọi check
    return spark.createDataFrame(results, REPORT_COLS)

def save_report(report_df):
    report_df.writeTo("lake.audit.qc_results").append()   # bảng tạo sẵn, append mỗi run
```

### Viên gạch 4 — chính sách: đọc report, quyết định số phận pipeline

```python
FAIL_HARD_PCT = {"unique": 1.0, "not_null": 5.0}   # ngưỡng tràn theo loại check

def enforce(report_df):
    fails = report_df.filter("status = 'FAIL'").collect()   # report bé → collect OK
    hard = [r for r in fails
            if any(r.check_name.startswith(k) and r.failed_pct >= v
                   for k, v in FAIL_HARD_PCT.items())]
    if hard:
        detail = "; ".join(f"{r.table_name}.{r.check_name}={r.failed_pct}%" for r in hard)
        raise RuntimeError(f"QC FAIL HARD: {detail}")       # → task Airflow đỏ (lesson 36)
    return fails                                            # mềm → caller quarantine/alert
```

- **Pitfall**: `collect()` ở đây hợp lệ vì report chỉ vài chục dòng — đúng quy tắc lesson 1 ("chỉ collect khi chắc chắn nhỏ").
- Volume anomaly không phải hàm check trên df — nó là query trên chính `qc_results` (so `total_count` hôm nay với trung bình 7 ngày). Xem lab bước 4.

---

## 6. Demo nhỏ

Tiêm 3 loại rác vào một DataFrame bé và xem framework tóm gọn:

```
Input : 6 đơn hàng — 1 cặp trùng PK, 1 NULL order_id, 1 status lạ, 1 giá âm
Check : unique, not_null, accepted_values, range
Output: report DataFrame 4 dòng — PASS/FAIL + failed_pct
```

```python
from pyspark.sql import SparkSession
# copy các hàm check ở Section 5 vào trước đoạn này (hoặc import từ labs/lab35/qc.py)

spark = SparkSession.builder.appName("demo35-qc").master("local[2]").getOrCreate()

data = [("o1", "delivered", 120.0), ("o2", "shipped",   80.0),
        ("o2", "shipped",   80.0),                       # trùng PK
        (None, "delivered", 50.0),                       # NULL PK
        ("o4", "teleported", 99.0),                      # status ngoài vũ trụ
        ("o5", "delivered", -10.0)]                      # giá âm
df = spark.createDataFrame(data, ["order_id", "order_status", "price"])

results  = []
results += check_not_null(df, "demo.orders", ["order_id", "order_status"])
results += check_unique(df, "demo.orders", ["order_id"])
results += check_accepted_values(df, "demo.orders", "order_status",
                                 ["delivered", "shipped", "canceled", "invoiced"])
results += check_range(df, "demo.orders", "price", lo=0)

build_report(spark, results).select("check_name", "status",
                                    "failed_count", "failed_pct").show(truncate=False)
# not_null_order_id     | FAIL | 1 | 16.67
# not_null_order_status | PASS | 0 | 0.0
# unique_order_id       | FAIL | 2 | 33.33   ← đếm CẢ 2 bản ghi dính trùng
# accepted_values_...   | FAIL | 1 | 16.67
# range_price           | FAIL | 1 | 16.67
spark.stop()
```

Tự hỏi: `unique_order_id` báo failed=2 chứ không phải 1 — vì sao? (Cả hai bản ghi của cặp trùng đều "không đáng tin" — chưa biết bản nào đúng. Quy ước này phải ghi vào docstring của framework.)

---

## 7. Production Example

Pipeline Olist tuần 18 sau khi cắm QC — chuỗi đầy đủ chạy mỗi đêm (nhìn trước DAG của lesson 36):

```
02:00  bronze_ingest (append + đo row_count, freshness — KHÔNG chặn)
02:10  ┌─ QC CỔNG #1 trên bronze mới ingest ────────────────────────────┐
       │ schema đúng? not_null PK? unique PK? status hợp lệ? price>=0?  │
       │   systemic fail (unique >1%, schema vỡ)  → RAISE → DAG đỏ      │
       │   per-row fail                           → tách 2 dòng chảy:   │
       │        sạch  → silver.orders (MERGE)                           │
       │        bẩn   → quarantine.orders (+ _qc_reason, _qc_ts)        │
       └────────────────────────────────────────────────────────────────┘
02:30  QC CỔNG #2: fk items→products, items→sellers, volume anomaly
02:40  build gold (fact/dim — lesson 34)
02:55  QC CỔNG #3: fact.count ~= silver.count, SUM(price) hôm nay > 0,
       revenue lệch <50% so trung bình 7 ngày
03:00  save_report → lake.audit.qc_results ; alert nếu có FAIL
```

Hai chi tiết production hay bị bỏ sót:

1. **Quarantine phải có lối ra**: dashboard nhỏ đếm `quarantine.*` theo tuần + quy trình "chữa" (nguồn sửa xong → replay từ bronze, dòng đã lành tự rời quarantine ở lần chạy sau). Quarantine không ai nhìn = bãi rác có tên đẹp.
2. **QC report nuôi chính nó**: check volume anomaly của hôm nay đọc `total_count` 7 ngày trước từ `qc_results` — framework tự dùng dữ liệu của mình làm baseline, không cần hệ thống ngoài.

Đây chính là pattern mà dbt (`store_failures` + elementary), GX (data docs + checkpoint), Soda (soda scan + Soda Cloud) đóng gói lại — bạn vừa hiểu ruột gan của cả ba.

---

## 8. Hands-on Lab

**Mục tiêu**: đóng gói framework thành `labs/lab35/qc.py`, cắm vào pipeline lab 34, lưu report Iceberg, xem trend.

### Bước 1 — `labs/lab35/qc.py`

Gom toàn bộ hàm Section 5 (các `check_*`, `build_report`, `save_report`, `enforce`) vào một module. Thêm docstring quy ước: failed_count của unique đếm mọi bản ghi dính trùng.

### Bước 2 — tạo bảng audit: `labs/lab35/step0_create_audit.py`

```python
import sys; sys.path.insert(0, "/workspace/labs/lab35")
from session import iceberg_session          # copy session.py từ lab34
spark = iceberg_session("lab35-audit")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.audit")
spark.sql("""CREATE TABLE IF NOT EXISTS lake.audit.qc_results (
  run_ts TIMESTAMP, table_name STRING, check_name STRING, status STRING,
  failed_count BIGINT, total_count BIGINT, failed_pct DOUBLE)
  USING iceberg PARTITIONED BY (days(run_ts))""")
spark.sql("""CREATE TABLE IF NOT EXISTS lake.audit.quarantine_orders (
  order_id STRING, payload STRING, _qc_reason STRING, _qc_ts TIMESTAMP)
  USING iceberg""")
spark.stop()
```

### Bước 3 — `labs/lab35/step1_qc_gate.py` — cổng QC #1 + #2 cho Olist

```python
import sys; sys.path.insert(0, "/workspace/labs/lab35")
from pyspark.sql import functions as F
from session import iceberg_session
from qc import (check_not_null, check_unique, check_fk, check_accepted_values,
                check_range, check_freshness, build_report, save_report, enforce)

spark = iceberg_session("lab35-qc-gate")
orders   = spark.table("lake.silver.orders").cache()      # check nhiều lần → cache
items    = spark.table("lake.silver.order_items").cache()
products = spark.table("lake.silver.products")
sellers  = spark.table("lake.silver.sellers")

VALID_STATUS = ["delivered","shipped","canceled","invoiced",
                "processing","unavailable","created","approved"]
results  = []
results += check_not_null(orders, "silver.orders", ["order_id","customer_id","order_status"])
results += check_unique(orders, "silver.orders", ["order_id"])
results += check_accepted_values(orders, "silver.orders", "order_status", VALID_STATUS)
results += check_not_null(items, "silver.order_items", ["order_id","product_id","seller_id"])
results += check_unique(items, "silver.order_items", ["order_id","order_item_id"])
results += check_range(items, "silver.order_items", "price", lo=0)
results += check_fk(items, "silver.order_items", "product_id", products, "product_id")
results += check_fk(items, "silver.order_items", "seller_id",  sellers,  "seller_id")
results += check_fk(items, "silver.order_items", "order_id",   orders,   "order_id")

report = build_report(spark, results)
report.orderBy("table_name", "check_name").show(50, truncate=False)
save_report(report)
enforce(report)          # vượt ngưỡng systemic → RuntimeError → exit code != 0
print("QC gate: PASS (hoặc chỉ fail mềm)")
spark.stop()
```

```bash
make run F=labs/lab35/step0_create_audit.py
make run F=labs/lab35/step1_qc_gate.py
```

### Bước 4 — tiêm rác và xem framework bắt

Viết `step2_inject_dirty.py`: append vào `lake.silver.orders` 200 dòng trùng `order_id` + 50 dòng status `"hacked"`. Chạy lại `step1_qc_gate.py` → quan sát: `unique_order_id` FAIL, `accepted_values` FAIL; nếu vượt ngưỡng `enforce` → job exit khác 0 (chính là tín hiệu Airflow dùng ở lesson 36). Sau đó viết `step3_quarantine.py`: tách dòng status lạ sang `quarantine_orders` kèm `_qc_reason`, dọn silver về sạch.

### Bước 5 — trend: `step4_trend.py`

Chạy gate 3–4 lần (xen kẽ tiêm rác) rồi:

```python
spark.sql("""
  SELECT date(run_ts) d, check_name,
         round(avg(failed_pct), 3) AS avg_failed_pct,
         max(status = 'FAIL')      AS any_fail
  FROM lake.audit.qc_results
  WHERE table_name = 'silver.orders'
  GROUP BY 1, 2 ORDER BY 1, 2""").show(50, truncate=False)
```

Nhìn được đường cong chất lượng — thứ mà log của một lần chạy không bao giờ cho bạn. Ghi nhận vào `labs/lab35/NOTES.md`: check nào đắt nhất (xem Spark UI), quyết định fail-hard/quarantine của bạn cho từng check và lý do.

---

## 9. Assignment

**Easy** — Viết bộ QC tối thiểu (liệt kê check + ngưỡng + chiến lược fail) cho 2 bảng: `silver.customers` và `silver.products`. Lưu ý bẫy `customer_id` vs `customer_unique_id` (lesson 34) — check unique trên cột nào, và cột kia thì check gì?

**Medium** — Thêm 2 check vào framework: (a) `check_schema(df, expected_schema)` — so sánh `df.schema` với StructType kỳ vọng, báo cột thiếu/thừa/sai kiểu thành 3 dòng report riêng; (b) `check_volume_anomaly(spark, table, tolerance_pct)` — đọc `total_count` trung bình 7 lần chạy gần nhất từ `qc_results`, FAIL nếu count hiện tại lệch quá ±tolerance. Test cả hai bằng cách tiêm lỗi.

**Hard** — Quarantine flow trọn vẹn: viết `silver_orders_with_quarantine.py` thay cho step2 của lab 34 — một lần đọc bronze, tách 2 dòng chảy sạch/bẩn (dùng `_qc_reason` tích lũy bằng `concat_ws` các vi phạm), MERGE dòng sạch vào silver, append dòng bẩn vào quarantine, và **replay**: sửa dữ liệu trong bronze rồi chạy lại — chứng minh dòng đã lành vào silver và không bị double.

**Production Challenge** — Alert thật: viết `alert.py` đọc `qc_results` của run mới nhất, dựng message gồm bảng/check/ngưỡng/thực tế/query-xem-dòng-lỗi, gửi qua webhook (dùng `requests.post` tới webhook.site nếu không có Slack). Thêm chống-spam: cùng một check FAIL liên tiếp N run chỉ alert lần đầu + lần thứ N. Giải thích 5 dòng vì sao chống-spam quan trọng không kém bản thân alert.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Check | Chi phí | Ghi chú tối ưu |
|---|---|---|
| not_null / accepted / range | 1 scan, không shuffle | GOM nhiều check 1 bảng vào 1 `agg` — framework đã làm cho not_null; tự mở rộng cho range |
| unique (PK) | **shuffle** theo key | Bảng partition theo ngày → chỉ check partition mới ingest hằng ngày; full check chạy tuần |
| fk (referential) | join (anti) | Bảng cha nhỏ (dim) → broadcast tự động; kiểm chứng trong UI |
| freshness / row count trên Iceberg | ~metadata-only | Manifest có sẵn stats — đừng viết `df.filter(...).count()` khi `MAX(ts)` là đủ |
| Nhiều check cùng 1 DataFrame | mỗi action = 1 lần đọc lại | `.cache()` bảng nguồn trước chuỗi check, `unpersist()` sau |
| QC toàn pipeline | thường 10–20% runtime | Đây là **phí bảo hiểm** — đắt nhất vẫn rẻ hơn một lần CFO mất niềm tin |

Tự vấn: *"check này chạy trên toàn bảng hay chỉ cần trên dữ liệu MỚI của hôm nay?"* — trả lời đúng câu này tiết kiệm 90% chi phí QC trên bảng lớn.

---

## 11. Spark UI

Chạy `step1_qc_gate.py` và soi:

- Tab **Jobs**: đếm job — mỗi `count()`/`agg().first()` của một check là 1 job. Thấy số job ≈ số check → hiểu ngay vì sao phải gom check và cache nguồn. So sánh trước/sau khi thêm `.cache()`: các job sau chạy từ **InMemoryTableScan** (tab Storage thấy bảng cached), duration giảm rõ.
- Job của `check_unique`: mở DAG thấy **Exchange** (shuffle theo PK) — check đắt nhất, đúng như Internal đã hứa. Ghi lại shuffle write size.
- Job của `check_fk`: tìm **BroadcastHashJoin** (dim nhỏ) — nếu là SortMergeJoin, bảng cha to bất thường hoặc thống kê sai.
- Tab **SQL / DataFrame**: câu `agg` gộp của `check_not_null` — 1 node scan duy nhất cho N cột. Đối chiếu: nếu bạn viết N lần `df.filter(col.isNull()).count()` sẽ là N scan.

---

## 12. Common Mistakes

1. **Không có QC gì cả** ("dữ liệu nguồn xịn lắm") — nguồn nào rồi cũng có ngày gửi rác; câu hỏi chỉ là bạn phát hiện lúc ingest hay lúc CFO chỉ vào dashboard.
2. **Check chặn ở bronze** — bronze là két lưu bằng chứng, contract của nó là nhận tất. Chặn ở đó là vứt bằng chứng và mất khả năng replay.
3. **Mọi fail đều dừng pipeline** — 3 dòng NULL trong 10 triệu dòng mà báo cáo toàn công ty trễ nửa ngày. Phân loại systemic/per-row/statistical rồi mới chọn phản ứng.
4. **Quarantine không ai nhìn lại** — thành bãi rác vô hạn. Quarantine phải có dashboard đếm + quy trình chữa + ngưỡng tràn nâng cấp thành fail hard.
5. **Check xong không lưu report** — mất khả năng nhìn trend, mất luôn baseline cho volume anomaly. Report là dữ liệu, ghi vào Iceberg như mọi dữ liệu.
6. **Ngưỡng tuyệt đối thay vì %** — "fail nếu >100 dòng lỗi" đúng hôm bảng 10k dòng, vô nghĩa khi bảng 10 triệu dòng.
7. **Alert mọi thứ về một channel Slack** — 2 tuần sau cả team mute channel. Phân tầng WARN/FAIL, chống lặp, alert phải kèm hành động.
8. **Check trùng lặp với constraint đã enforced** — silver ghi bằng MERGE theo PK thì bản thân MERGE đã chống trùng cho lần ghi đó; check unique vẫn cần nhưng ở tần suất/phạm vi hợp lý, đừng full-scan mỗi giờ.

---

## 13. Interview

**Junior:**

1. *Kể các loại data quality check cơ bản?* — Schema (đúng cột/kiểu), not null, uniqueness (PK), referential integrity (FK), accepted values, range, freshness (dữ liệu mới đến đâu), volume anomaly (số dòng bất thường). Bốn cái đầu về cấu trúc/từng dòng, hai cái cuối về hành vi cả tập dữ liệu.
2. *Tại sao cần check dữ liệu khi pipeline đã chạy thành công?* — "Chạy thành công" chỉ nghĩa là không exception — Spark vui vẻ copy rác từ A sang B. Đúng kỹ thuật ≠ đúng dữ liệu; check là cách duy nhất biến contract dữ liệu thành thứ được thực thi.
3. *Kiểm tra PK unique trong Spark thế nào?* — `groupBy(pk).count().filter("count > 1")` — đếm nhóm có hơn 1 bản ghi. Lưu ý đây là phép shuffle theo key nên là check đắt; trên bảng lớn nên check theo partition mới.
4. *Freshness check là gì, khác gì các check khác?* — Đo `MAX(timestamp)` so với hiện tại, trả lời "dữ liệu có còn chảy không". Khác ở chỗ không có dòng nào sai — cái sai là sự VẮNG MẶT của dữ liệu mới, nên fail thường có nghĩa dừng/skip chứ không quarantine.

**Mid:**

5. *Fail hard vs quarantine — chọn thế nào?* — Theo bản chất lỗi: systemic (schema vỡ, trùng hàng loạt, freshness) → fail hard vì cả batch khả nghi; per-row (NULL lẻ, FK mồ côi, range lệch) → quarantine dòng lỗi kèm lý do, dòng sạch đi tiếp; statistical (volume anomaly) → alert cho người vì máy thiếu ngữ cảnh. Kèm ngưỡng tràn: quarantine vượt N% → nâng thành fail hard.
6. *Referential integrity trong lakehouse enforce kiểu gì khi Iceberg không có FK constraint?* — Đúng, table format không enforce FK như RDBMS — phải kiểm bằng job: left_anti join bảng con với bảng cha đếm orphan, chạy ở cổng QC trước khi build fact. Orphan thường do lệch nhịp ingest → quarantine + tự lành lần chạy sau.
7. *dbt tests khác Great Expectations chỗ nào?* — dbt test gắn vào model SQL (YAML: unique/not_null/relationships/accepted_values + custom SQL), chạy trong warehouse sau khi build, rẻ nếu đã dùng dbt. GX là framework riêng với hàng trăm expectation, profiling, data docs — mạnh hơn nhưng nặng hơn. Cùng một tập khái niệm check bên dưới.
8. *QC report nên lưu thế nào và để làm gì?* — Mỗi check mỗi run một dòng (run_ts, table, check, status, failed_count, total, failed_pct) append vào bảng audit (Iceberg). Hai công dụng: pipeline đọc để enforce, và trend theo thời gian — phát hiện chất lượng xuống cấp dần + làm baseline cho volume anomaly.

**Senior:**

9. *Ngưỡng alert đặt thế nào để tránh alert fatigue mà không bỏ sót sự cố?* — (a) Ngưỡng tương đối (%) thay vì tuyệt đối; (b) hai tầng WARN/FAIL với phản ứng khác nhau; (c) baseline động từ lịch sử qc_results thay vì hằng số; (d) dedup alert (fail liên tiếp chỉ báo lần đầu và mốc leo thang); (e) mọi WARN kinh niên phải được xử lý hoặc nâng ngưỡng có chủ đích — alert không ai hành động là alert cần xóa. Nguyên tắc: mỗi alert phải trả lời được "người nhận cần LÀM gì".
10. *Data quality nên là trách nhiệm của ai — data engineer, nguồn, hay analyst?* — Cả ba, theo hợp đồng: nguồn chịu trách nhiệm về data contract tại điểm phát (schema, semantics); DE enforce contract tại cổng ingest + bảo đảm không làm hỏng thêm (test transform); analyst định nghĩa ngưỡng nghiệp vụ (revenue lệch bao nhiêu là bất thường). Sai lầm tổ chức phổ biến là dồn hết cho DE — DE không thể biết price âm là bug hay là refund; thiếu ngữ cảnh nghiệp vụ thì check chỉ là đoán. Câu trả lời tốt nhắc tới data contract và shift-left (đẩy check về gần nguồn nhất có thể).

---

## 14. Summary

### Mindmap

```
                      LESSON 35 — DATA QUALITY & TESTING
                                    │
     ┌───────────────┬──────────────┼───────────────┬──────────────────┐
     ▼               ▼              ▼               ▼                  ▼
  TẠI SAO         8 LOẠI CHECK   CHIẾN LƯỢC      FRAMEWORK          CÔNG CỤ
     │               │              │               │                  │
  quality =      schema/null/   systemic → HARD  check() → tuple    dbt tests
  contract       unique/FK/     per-row →        build_report()     (gắn model)
  được thực thi  accepted/      QUARANTINE       save → Iceberg     GX (suite
  garbage vào    range/fresh/   statistical →    enforce() →        đồ sộ+docs)
  gold = mất     volume         ALERT người      raise/quarantine   Soda (YAML,
  niềm tin BI    (A cấu trúc,   + ngưỡng tràn    trend từ           vận hành đọc
  → dashboard    B nội dung,    quarantine       qc_results         được)
  trống > sai    C hành vi)     phải có lối ra                      → cùng 1 lõi
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 8 loại check và xếp vào 3 nhóm cấu trúc/nội dung/hành vi.
- [ ] Giải thích được vì sao KHÔNG chặn ở bronze và cổng QC nằm ở đâu trong medallion.
- [ ] Quyết định fail hard / quarantine / alert cho từng tình huống kèm lý do.
- [ ] Framework qc.py chạy được: report DataFrame, ghi Iceberg, enforce ngưỡng.
- [ ] Đã tiêm rác vào lab và thấy job exit khác 0 khi vượt ngưỡng.
- [ ] Nhìn trend failed_pct qua nhiều run từ bảng qc_results.
- [ ] So sánh được dbt tests / GX / Soda trong 1 phút.
- [ ] Trả lời được 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Lesson 36 — Orchestration với Airflow: SparkSubmitOperator, idempotency.**

Bạn đang có trong tay: pipeline medallion (lesson 34) + cổng QC biết raise khi dữ liệu bẩn (lesson 35). Nhưng ai bấm nút chạy chúng lúc 2 giờ sáng? Ai biết bronze xong mới được chạy silver? QC fail thì ai retry, ai nhận alert, và hôm qua fail thì hôm nay chạy bù thế nào? `cron` không trả lời được câu nào trong số đó. Lesson 36 đưa **Airflow** vào làm nhạc trưởng: DAG bronze→silver→gold→maintenance, SparkSubmitOperator, và kỹ năng đáng giá nhất của một DE vận hành — thiết kế job **idempotent** để re-run và backfill 30 ngày không làm hỏng dữ liệu. Exit code khác 0 mà `enforce()` của bạn vừa tạo ra chính là thứ Airflow sẽ lắng nghe.

> Gõ **"Continue"** khi sẵn sàng.
