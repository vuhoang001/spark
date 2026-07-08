# Mini Project 1 — Batch Ingestion v0: CSV → Parquet phân vùng (→ Iceberg → Trino)

> Module 1 · Foundations · Tuần 3 · Thời lượng: 6–10 giờ (trải 2–4 buổi)

---

## 1. Mục tiêu

Đây là bài "ráp súng" đầu tiên: **không có kiến thức mới**, chỉ có 6 lesson vừa học bị bắt làm việc thật cùng nhau. Sản phẩm cuối là một pipeline batch ingestion nhỏ nhưng đúng chuẩn nghề:

- Đọc Olist CSV với **schema tường minh** + xử lý dòng hỏng có chủ đích (lesson 5).
- Ghi **Parquet phân vùng theo ngày**, layout tử tế cho người đọc sau (lesson 5, 6).
- **Đo và chứng minh** bằng số: kích thước, thời gian query, số file được đọc — before/after (lesson 1–4: đọc Spark UI).
- (Tùy chọn, nếu hạ tầng `../kafka-flink` sẵn sàng) Đăng ký dữ liệu thành **bảng Iceberg**, query bằng **Trino** — nếm trước module 5.

Kỹ năng được chấm không phải "code chạy được" — mà là **ra quyết định có căn cứ và chứng minh bằng số đo**. Đó là khác biệt giữa người làm theo tutorial và một Data Engineer.

---

## 2. Kiến trúc

```
  data/olist/*.csv  (9 file, ~120 MB, dữ liệu thật có vết bẩn)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  SPARK (apache/spark:3.4.1, make run / run-local)   │
│                                                     │
│  ① READ    schema StructType + PERMISSIVE           │
│            └─ dòng hỏng → quarantine, đếm & log     │
│  ② CLEAN   ép kiểu timestamp, derive order_date     │
│            loại dòng thiếu key/ngày (có ghi nhận)   │
│  ③ WRITE   Parquet snappy, partitionBy(order_date)  │
│            repartition trước ghi — chống small files │
└─────────────────────────────────────────────────────┘
        │
        ▼
  data/output/mini-project-1/
     orders_clean/order_date=2018-07-02/part-*.parquet
     quarantine/...
        │
        ├──────────── BENCHMARK ────────────┐
        │  cùng query: CSV vs Parquet       │
        │  full-scan vs partition pruning   │
        ▼                                   ▼
   report.md (số đo before/after)      Spark UI (bằng chứng)
        │
        ▼ (TÙY CHỌN — có hạ tầng ../kafka-flink)
┌─────────────────────────────────────────┐
│  ICEBERG table (catalog + metadata)     │
│        ▲ Spark ghi      │ Trino đọc     │
│        └────────────────┴──▶ so tốc độ  │
│             CSV-external vs Iceberg     │
└─────────────────────────────────────────┘
```

---

## 3. Yêu cầu

**Bắt buộc:**

1. Code đặt trong `labs/mini-project-1/` (thư mục mới — không đụng lab cũ), tách file theo bước, ví dụ: `ingest.py`, `benchmark.py`, `schemas.py`.
2. Xử lý tối thiểu 2 bảng: `olist_orders_dataset.csv` và `olist_order_items_dataset.csv`. Khuyến khích thêm `customers`.
3. **Cấm** `inferSchema=True` trong code nộp (được dùng một lần ở máy dev để sinh schema — mẹo lesson 5).
4. Ghi idempotent: chạy pipeline 2 lần, kết quả **giống hệt** (không nhân đôi dữ liệu). Gợi ý: overwrite + `partitionOverwriteMode=dynamic`, hoặc overwrite toàn bảng có chủ đích.
5. `report.md` với số đo thật (mẫu ở mục 5).
6. Chạy được bằng lệnh chuẩn của repo: `make run F=labs/mini-project-1/ingest.py` (đường dẫn dữ liệu trong container: `/workspace/data/olist/`, output: `/workspace/data/output/mini-project-1/`).

**Tùy chọn (điểm cộng lớn):** Checkpoint 4 — Iceberg + Trino trên hạ tầng `../kafka-flink`.

---

## 4. Checkpoint

### Checkpoint 1 — Đọc CSV thành DataFrame, schema tường minh + quarantine

**Việc cần làm:**

- Viết `schemas.py`: StructType cho từng bảng (kiểu đúng: timestamp là TimestampType, tiền là DoubleType, id là StringType) + field `_corrupt_record`.
- Đọc `PERMISSIVE`, tách sạch/hỏng, ghi dòng hỏng ra `quarantine/` (Parquet cũng được — kèm cột `source_file`, `ingest_ts` bằng `F.lit`/`F.current_timestamp`).
- In ra: số dòng mỗi bảng, số dòng hỏng, `printSchema()`.

**Tiêu chí chấm:**

- [ ] Schema đủ cột, kiểu hợp lý, không có cột nào bị "StringType cho nhanh" thiếu căn cứ.
- [ ] Nhớ bẫy lesson 5: đã `cache()` trước khi filter theo `_corrupt_record` (hoặc giải thích cách khác).
- [ ] Số dòng in ra khớp khi đối chiếu `wc -l` file gốc (chênh lệch phải giải thích được: header, dòng hỏng, multiline).
- [ ] Trả lời miệng được: vì sao chọn PERMISSIVE mà không FAILFAST cho dataset này?

### Checkpoint 2 — Ghi Parquet phân vùng theo ngày

**Việc cần làm:**

- Derive `order_date = to_date(order_purchase_timestamp)`; quyết định làm gì với dòng `order_date` NULL (drop có đếm? dồn vào partition `__unknown__`? — chọn và biện luận trong report).
- `repartition("order_date")` (hoặc chiến lược khác bạn biện luận được) rồi `write.partitionBy("order_date").parquet(...)`, mode overwrite idempotent (yêu cầu 3.4).
- Với `order_items`: bảng này không có timestamp mua hàng — join lấy `order_date` từ orders, hoặc partition theo tháng của `shipping_limit_date`. Chọn và ghi lý do.

**Tiêu chí chấm:**

- [ ] Cây thư mục đúng dạng `order_date=YYYY-MM-DD/`; số thư mục hợp lý (~600+ ngày với orders).
- [ ] Mỗi partition 1 file (hoặc số file có chủ đích) — không phải rừng file vụn. Kiểm bằng `find ... -name 'part-*' | wc -l`.
- [ ] Chạy pipeline lần 2: `count()` toàn bảng không đổi — bằng chứng idempotent dán vào report.
- [ ] Giải thích được: partition theo `order_date` (cardinality ~600) ổn, nhưng nếu dataset chỉ có 2 năm dữ liệu mà query toàn theo tháng thì partition theo tháng có hợp lý hơn không? (không có đáp án duy nhất — có lập luận là đạt)

### Checkpoint 3 — Benchmark: chứng minh bằng số

**Việc cần làm** (`benchmark.py` — chạy `make run-local` cho số đo ổn định):

- Query A (aggregate): doanh thu (`sum(price)`) của đơn `delivered` theo tháng — chạy trên (1) CSV gốc, (2) Parquet không filter partition.
- Query B (điểm rơi của pruning): doanh thu đúng ngày `2018-07-02` — chạy trên (1) CSV, (2) Parquet với filter `order_date`.
- Mỗi query đo 2 lần (ghi cả hai — lần 1 lạnh, lần 2 ấm), bọc `time.time()`.
- Chụp/ghi từ Spark UI (tab SQL, node Scan): `number of files read`, `size of files read` cho query B trên Parquet.
- `du -sh` kích thước: CSV gốc vs thư mục Parquet.

**Tiêu chí chấm:**

- [ ] Bảng số liệu đầy đủ 4 ô (2 query × 2 format) + kích thước.
- [ ] Có bằng chứng partition pruning: `number of files read` của query B nhỏ hơn hẳn tổng số file (con số cụ thể, không phải "nhanh hơn nhiều").
- [ ] Có đoạn `explain()` cho query B, khoanh được `PartitionFilters`.
- [ ] Diễn giải trung thực: Olist bé, chênh lệch giây có thể khiêm tốn — người hiểu bài sẽ chỉ vào *bytes read* thay vì chỉ vào giây, và ngoại suy được cho dữ liệu 100×.

### Checkpoint 4 (TÙY CHỌN) — Iceberg + Trino trên hạ tầng kafka-flink

Bạn đã có repo `../kafka-flink` với Iceberg catalog + Trino. Nếu stack đó đang chạy:

**Việc cần làm:**

- Cấu hình SparkSession với Iceberg catalog (theo pattern có sẵn trong `../kafka-flink/processing/spark/` — đọc lại config của chính repo bạn, đó cũng là bài tập).
- Tạo bảng: `CREATE TABLE ... USING iceberg PARTITIONED BY (days(order_purchase_timestamp))` rồi ghi dữ liệu sạch từ Checkpoint 2 vào (`writeTo(...).append()` hoặc `INSERT`). Lưu ý: với Iceberg, ưu tiên để Iceberg quản partition (hidden partitioning) thay vì tự `partitionBy` — ghi nhận khác biệt này vào report, module 5 sẽ giải thích sâu.
- Trino: tạo thêm một external table trỏ thẳng CSV (hive connector) làm "before"; chạy cùng query A/B trên CSV-external vs Iceberg, ghi thời gian.

**Tiêu chí chấm:**

- [ ] Bảng Iceberg query được từ CẢ Spark lẫn Trino (đúng tinh thần compute tách storage — lesson 1).
- [ ] Bảng so sánh thời gian Trino: CSV-external vs Iceberg-Parquet cho 2 query.
- [ ] Chỉ ra được ít nhất 1 thứ Iceberg cho mà Parquet trần không có (ví dụ: `SELECT * FROM table.snapshots` — time travel manh nha).
- Không có hạ tầng? Bỏ qua không mất điểm phần bắt buộc — nhưng đọc phần này để biết mình sẽ quay lại đâu ở module 5.

---

### Gợi ý khung code (được phép dùng, phải tự lấp ruột)

```python
# labs/mini-project-1/schemas.py
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, TimestampType)

ORDERS = StructType([
    StructField("order_id", StringType(), False),
    # ... TỰ ĐIỀN — đủ 8 cột + _corrupt_record
])

ORDER_ITEMS = StructType([
    # order_id, order_item_id (Integer!), product_id, seller_id,
    # shipping_limit_date (Timestamp), price, freight_value (Double)
])
```

```python
# labs/mini-project-1/ingest.py
"""Chạy:  make run F=labs/mini-project-1/ingest.py
Output:  /workspace/data/output/mini-project-1/{orders_clean,items_clean,quarantine}
"""
from pyspark.sql import SparkSession, functions as F
from schemas import ORDERS, ORDER_ITEMS      # cần --py-files hoặc gộp file nếu ngại

SRC, OUT = "/workspace/data/olist", "/workspace/data/output/mini-project-1"

spark = SparkSession.builder.appName("mp1-ingest").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def read_clean(file, schema, name):
    df = (spark.read.schema(schema).option("header", True)
          .option("mode", "PERMISSIVE").csv(f"{SRC}/{file}")).cache()
    bad = df.filter(F.col("_corrupt_record").isNotNull())
    # TODO: ghi bad ra f"{OUT}/quarantine/{name}" kèm source_file + ingest_ts
    # TODO: log số dòng sạch/hỏng
    return df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

orders = read_clean("olist_orders_dataset.csv", ORDERS, "orders")
# TODO: derive order_date, quyết định số phận NULL date,
#       repartition("order_date"), write partitionBy — idempotent!
```

Lưu ý kỹ thuật: `make run` submit MỘT file — muốn import `schemas.py` trong cluster mode, hoặc gộp schema vào `ingest.py`, hoặc chạy `make run-local` (cùng filesystem nên import được nếu submit từ thư mục đó). Vướng ở đây quá 30 phút thì gộp file lại và ghi chú — trọng tâm project không nằm ở packaging (lesson 41 mới là chỗ đó).

---

## 5. Deliverable

Nộp trong `labs/mini-project-1/`:

1. **Code**: `schemas.py`, `ingest.py`, `benchmark.py` (+ file Iceberg nếu làm CP4). Có docstring đầu file: chạy bằng lệnh gì, output ra đâu.
2. **`report.md`** — theo khung:

```markdown
# Mini Project 1 — Report

## 1. Pipeline
(3–5 dòng mô tả + quyết định chính: read mode, xử lý NULL date,
 chiến lược partition, vì sao)

## 2. Data quality
| bảng | dòng đọc | dòng hỏng | dòng NULL date | vào bảng chính |
|---|---|---|---|---|

## 3. Kích thước (before/after)
| dạng | kích thước | số file | tỉ lệ so CSV |
|---|---|---|---|
| CSV gốc | | | 1× |
| Parquet phân vùng | | | |

## 4. Thời gian query (before/after)
| query | CSV lần1/lần2 | Parquet lần1/lần2 | files read (Parquet) |
|---|---|---|---|
| A: revenue theo tháng | | | |
| B: revenue 1 ngày | | | |
(+ nếu CP4: | Trino CSV-external | Trino Iceberg |)

## 5. Bằng chứng
(đoạn explain() có PartitionFilters; ảnh/ghi chép Spark UI;
 log 2 lần chạy chứng minh idempotent)

## 6. Nếu dữ liệu ×100 thì sao?
(5–10 dòng: điều gì trong thiết kế này vẫn đứng vững, điều gì phải đổi)
```

3. **Bằng chứng idempotent**: output `count()` của 2 lần chạy liên tiếp.

---

## 6. Rubric chấm điểm (chuẩn Senior — tổng 100)

| Hạng mục | Điểm | Đạt tối đa khi |
|---|---|---|
| **Đúng đắn** — schema, quarantine, số khớp nguồn | 25 | Kiểu dữ liệu chuẩn, dòng hỏng truy vết được, mọi chênh lệch số dòng giải thích được |
| **Thiết kế ghi** — partition, file layout, idempotent | 25 | Partition có biện luận; không small files; chạy lại 2 lần kết quả y hệt và CHỨNG MINH điều đó |
| **Benchmark & bằng chứng** — số đo, Spark UI, explain | 25 | Kết luận nào cũng có con số/bằng chứng đi kèm; phân biệt được bytes-read với giây-đồng-hồ; diễn giải trung thực với dataset bé |
| **Chất lượng code** — cấu trúc, đặt tên, không magic | 15 | Schema tách file riêng, path là hằng số ở đầu file, chạy được đúng bằng lệnh khai trong docstring |
| **Tư duy scale** — mục "×100" của report | 10 | Chỉ ra đúng điểm gãy (driver? small files? một máy ghi?) thay vì "thêm executor là xong" |
| **Bonus CP4** — Iceberg + Trino | +10 | Bảng đọc được từ 2 engine + so sánh có số |

Thang: **≥85** — làm việc như junior DE cứng, sẵn sàng module 2; **70–84** — đạt, xem lại hạng mục thấp nhất trước khi đi tiếp; **<70** — gặp mentor, sửa và nộp lại (bình thường, đây là vòng lặp học).

Ba lỗi bị trừ thẳng tay dù mọi thứ khác đẹp: `inferSchema` trong code nộp (−10); pipeline chạy 2 lần ra dữ liệu đôi (−15); kết luận "nhanh hơn" không kèm số (−10).

---

## 7. Câu hỏi mở rộng (trả lời trong report hoặc thảo luận với mentor)

1. Ngày mai nguồn CSV thêm cột mới `order_priority` — pipeline của bạn hỏng ở đâu, sửa mấy chỗ? Thiết kế lại thế nào để "thêm cột" chỉ phải sửa MỘT chỗ?
2. Nếu mỗi ngày nhận một file CSV mới (incremental) thay vì full dump: save mode đổi thế nào? Làm sao re-run một ngày cũ mà không đụng ngày khác? (gợi ý: bạn đã có đủ đồ nghề từ lesson 5)
3. Quarantine đang là "hố chôn" — thiết kế quy trình để dòng hỏng được *chữa* và quay lại bảng chính, không double khi chữa xong.
4. Sếp hỏi: "sao không đổ thẳng CSV vào Postgres rồi query?" — trả lời trong 5 câu, có số từ report của bạn.
5. Với 100k đơn của Olist, `partitionBy("order_date")` tạo ~600 partition, mỗi cái vài chục KB — chính bạn vừa tạo ra small files một cách... đúng đề bài. Ở kích thước dữ liệu nào thì thiết kế này bắt đầu ĐÚNG thật sự? Trước ngưỡng đó nên partition thế nào? (câu này không có bẫy — nó là bài học partition-theo-kích-thước-thật quan trọng nhất của project)
6. (Nếu làm CP4) Iceberg "hidden partitioning" khác gì `partitionBy` thư mục trần bạn làm ở CP2? Điều gì xảy ra khi muốn đổi từ partition-theo-ngày sang theo-tháng ở mỗi bên?

---

## 8. Next

**Module 2 · Lesson 7 — Transformations cốt lõi: select / filter / withColumn / when.**

Module 1 khép lại: bạn đã hiểu Spark chạy Ở ĐÂU (kiến trúc), KHI NÀO (lazy, job/stage/task), dữ liệu nằm THẾ NÀO (partition, Parquet) và RA/VÀO ra sao (Data Sources API) — phần "vật lý" của nghề. Module 2 chuyển sang phần "hóa học": biến đổi dữ liệu. Lesson 7 bắt đầu với bộ tứ transformation bạn sẽ gõ nhiều nhất đời — select, filter, withColumn, when — viết song song cả DataFrame API lẫn Spark SQL, và bài học performance đầu tiên của module: thứ tự transformation quyết định lượng dữ liệu chảy qua pipeline. Từ giờ, mọi demo đều chạy trên chính dữ liệu Parquet sạch mà Mini Project 1 của bạn vừa tạo ra — pipeline của bạn nuôi bài học của bạn.

> Gõ **"Continue"** khi sẵn sàng.
