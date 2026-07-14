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

> 📓 **Sổ tiến độ:** mở [`labs/mini-project-1/PROGRESS.md`](../labs/mini-project-1/PROGRESS.md) — lộ trình 6 buổi có cổng chặn, bảng tick 40 bài, và **bảng đo benchmark có ngưỡng đạt** để bạn tự phán "đã tối ưu hay chưa" bằng số chứ không bằng cảm tính. Điền dần vào đó, cuối project 80% `report.md` đã có sẵn.

> **Muốn hiểu sâu chứ không chỉ nộp bài?** 4 checkpoint dưới đây là mức tối thiểu để qua project. **[Phụ lục A](#8-phụ-lục-a--ngân-hàng-bài-tập-40-bài-phủ-lesson-16)** là 40 bài tập nhỏ phủ kín lesson 1→6 — mỗi bài một thí nghiệm đo được, có hướng dẫn cách làm và bẫy hay gặp. Làm hết 18 bài ⭐ ở đó thì bạn không còn "học xong module 1" mà là *hiểu* module 1.

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

## 8. Phụ lục A — Ngân hàng bài tập (40 bài, phủ lesson 1→6)

> **Cách dùng phụ lục này.** 4 checkpoint ở mục 4 là *xương sống* — làm xong là qua project. Phụ lục này là *thịt*: mỗi bài đóng một lỗ hổng kiến thức cụ thể của lesson 1–6. Mỗi bài viết theo cùng một khung:
>
> - **Mục tiêu** — bài này bịt lỗ hổng nào.
> - **Cách làm** — hướng dẫn từng bước + gợi ý API. **Ở đây chỉ có ví dụ và skeleton — code là việc của bạn.** Chỗ nào có snippet thì nó chỉ ra *hình dạng* của lời giải, không phải lời giải.
> - **Bằng chứng** — thứ phải xuất hiện trong `report.md`. Không có bằng chứng = chưa làm.
> - **Bẫy** — chỗ 8/10 người ngã.
>
> **Ký hiệu độ ưu tiên:** ⭐ = làm trước, hiểu bài mới đi tiếp được · ◆ = nên làm, tách người khá khỏi người trung bình · ○ = stretch, làm khi còn thời gian.
>
> **Nơi để code.** Đừng nhét hết vào `ingest.py`. Tạo `labs/mini-project-1/exercises/` và mỗi bài một file nhỏ: `a03_local_vs_cluster.py`, `a15_max_partition_bytes.py`... Mỗi file tự đứng được, chạy bằng `make run F=...` hoặc `make run-local F=...`. Bài tập là *thí nghiệm*, không phải pipeline — code xấu cũng được, miễn số đo đúng.
>
> **Quy tắc vàng cho MỌI bài:** viết **dự đoán ra giấy trước khi chạy**. Sai dự đoán là lúc bạn học được nhiều nhất; chạy trước rồi mới "à đúng rồi" là tự lừa mình.

### Lộ trình đề nghị (nếu không biết bắt đầu từ đâu)

| Buổi | Làm gì | Bài |
|---|---|---|
| 1 | Nhìn thấy cluster & lazy bằng mắt | A1, A2, A5, A6, A9 |
| 2 | Đọc được Spark UI như đọc chữ | A10, A11, A12, A13 |
| 3 | Partition — nút vặn quan trọng nhất | A15, A16, A17, A19, A20 |
| 4 | Ingest thật (Checkpoint 1+2) + dữ liệu bẩn | A21, A22, A23, A24, A25, A26 |
| 5 | Parquet & benchmark (Checkpoint 3) | A30, A31, A32, A33, A35 |
| 6 | Ráp lại, viết report, trả lời câu hỏi mở | A37, A38, A39, A40 |

---

### Track L1 — Kiến trúc: Spark chạy Ở ĐÂU (lesson 1)

#### ⭐ A1. Vẽ bản đồ cluster của chính bạn

**Mục tiêu.** Bạn đang gõ `make run` mỗi ngày mà không biết code mình chạy trên mấy cái máy, mấy core, bao nhiêu RAM. Bài này chấm dứt chuyện đó.

**Cách làm.**
1. `make up`, mở Spark Master UI (cổng 8080) và Application UI (cổng 4040 khi job đang chạy).
2. Chạy một job đủ lâu để kịp mở UI — mẹo: thêm một `input()` hoặc `time.sleep(300)` ở cuối script để giữ SparkSession sống.
3. Vào tab **Executors**, điền bảng này vào report:

| | Địa chỉ | Cores | Memory | On heap / off heap |
|---|---|---|---|---|
| driver | | | | |
| executor 1 | | | | |
| executor N | | | | |

4. Đối chiếu với `docker-compose.spark.yaml` và các flag trong `Makefile` (`--executor-cores`, `--executor-memory`, `--master`) — mỗi con số trên UI **phải** truy được về một dòng cấu hình. Ghi lại cặp `số trên UI ← dòng config nào`.

**Bằng chứng.** Bảng trên + một câu: "tổng số task chạy song song tối đa của cluster này là ___, vì ___".

**Bẫy.** `--executor-memory 2g` không có nghĩa executor có 2GB để chứa dữ liệu. Tìm hiểu `spark.memory.fraction` và ghi vào report con số RAM *thực sự* dùng cho storage+execution.

---

#### ⭐ A2. `make run` vs `make run-local` khác nhau chỗ nào

**Mục tiêu.** Hiểu deploy mode bằng trải nghiệm, không bằng định nghĩa thuộc lòng.

**Cách làm.**
1. Đọc kỹ 2 target trong `Makefile`, ghi ra khác biệt về `--master` và `--deploy-mode`.
2. Viết một script in ra 3 thứ: `spark.sparkContext.master`, `socket.gethostname()`, và số executor thấy được (`spark.sparkContext.getExecutorMemoryStatus` không có ở PySpark — dùng `spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()` hoặc đơn giản là đọc UI).
3. Chạy cùng script bằng cả 2 lệnh, so output.
4. Thêm một `print()` bên trong một transformation (ví dụ trong `.rdd.map(lambda r: (print(r), r)[1])`) — chạy 2 mode, xem `print` xuất hiện ở đâu.

**Bằng chứng.** Bảng 2 cột `run` / `run-local` cho: master URL, hostname, số executor, `print` trong map hiện ở terminal nào.

**Bẫy.** Bài học lớn nhất nằm ở bước 4: `print` trong transformation chạy **trên executor**, log của nó không về terminal của bạn ở cluster mode. Đây là lý do bạn không debug Spark bằng `print`.

---

#### ◆ A3. Local nhanh hơn cluster — giải thích được không?

**Mục tiêu.** Đập tan niềm tin "cluster = nhanh".

**Cách làm.** Cùng một query (ví dụ `count()` trên `olist_customers`, 8.6MB) chạy 3 lần ở 3 nơi: `local[1]`, `local[*]`, và cluster. Đo bằng `time.time()` bao quanh action, **và** đọc "Duration" của job trên UI (hai con số này khác nhau — nói rõ vì sao trong report).

**Bằng chứng.** Bảng 3 dòng + 5 câu giải thích: overhead nào có ở cluster mà local không có (khởi tạo executor, serialize task, network shuffle, đọc file qua volume mount...). Kèm câu trả lời: "dữ liệu phải lớn cỡ nào thì cluster mới thắng?" — ước lượng có căn cứ.

**Bẫy.** Đừng so lần chạy đầu tiên (JVM warmup + đọc file lạnh). Chạy mỗi cái 3 lần, lấy lần 2–3.

---

#### ◆ A4. Làm chết driver có chủ đích

**Mục tiêu.** Cảm nhận được ranh giới driver/executor bằng một cái crash thật.

**Cách làm.**
1. Đọc `olist_geolocation_dataset.csv` (58MB — file to nhất), rồi `.collect()` toàn bộ về driver với driver memory bị bóp nhỏ (`--driver-memory 512m`).
2. Đọc traceback: lỗi là `OutOfMemoryError`? `Total size of serialized results ... is bigger than spark.driver.maxResultSize`? Ghi nguyên văn.
3. Sửa 3 cách khác nhau: `take(20)`, `limit(20).collect()`, `show()`. Giải thích cách nào *thực sự* không kéo hết dữ liệu về (gợi ý: `explain()` cả ba, tìm `LocalLimit`/`GlobalLimit`).

**Bằng chứng.** Nguyên văn lỗi + 3 dòng: cách sửa nào an toàn ở dữ liệu 1000×, cách nào chỉ may mắn.

**Bẫy.** `df.limit(20).collect()` và `df.collect()[:20]` nhìn giống nhau trong Python và khác nhau hoàn toàn ở Spark. Nếu chưa thấy rõ tại sao, bạn chưa hiểu lesson 1.

---

### Track L2 — Lazy, DAG, ba tầng API (lesson 2)

#### ⭐ A5. Chứng minh lazy bằng đồng hồ

**Mục tiêu.** Không tin vào chữ "lazy" — đo nó.

**Cách làm.** Bọc `time.time()` quanh từng nhóm dòng:

```python
t0 = time.time(); df = spark.read.schema(S).csv(PATH);          t1 = time.time()
df2 = df.filter(...).withColumn(...).select(...)                # 10 transformation
t2 = time.time(); n = df2.count();                               t3 = time.time()
# TODO: in ra t1-t0, t2-t1, t3-t2 và tự trả lời: vì sao t2-t1 ≈ 0?
```

Rồi làm biến thể: `spark.read` **không** truyền schema (để Spark tự infer) — đo lại `t1 - t0`. Vì sao lần này `read` tốn thời gian thật dù chưa có action nào?

**Bằng chứng.** 2 bộ số + câu trả lời cho câu hỏi cuối (đây chính là bẫy kinh điển: `inferSchema` là một action trá hình).

---

#### ⭐ A6. Đọc `explain()` như đọc bản đồ

**Mục tiêu.** Nhìn physical plan mà đoán được I/O.

**Cách làm.** Lấy query: đọc orders CSV → filter `order_status = 'delivered'` → select 2 cột → `groupBy` tháng. Chạy `df.explain(mode="formatted")`. Trong output, tô đậm và ghi chú 5 thứ:

- `Scan csv` / `FileScan parquet` — nguồn đọc
- `PushedFilters:` — filter nào bị đẩy xuống tận reader, filter nào không (thử filter theo một biểu thức phức tạp như `substring(order_id,1,2)='ab'` — nó có được push không?)
- `ReadSchema:` — có đúng chỉ 2 cột không (column pruning)
- `Exchange hashpartitioning(...)` — đây là shuffle, đếm xem có mấy cái
- `HashAggregate` xuất hiện mấy lần (gợi ý: partial + final — vì sao 2?)

Sau đó chạy cùng query trên bản Parquet của Checkpoint 2, so 2 plan cạnh nhau.

**Bằng chứng.** 2 block `explain()` dán vào report, kèm chú thích mũi tên 5 điểm trên. Câu trả lời: filter nào CSV không push được mà Parquet push được, và tại sao (`PartitionFilters` vs `PushedFilters` — hai thứ khác nhau!).

---

#### ◆ A7. RDD vs DataFrame — cùng bài toán, hai thế giới

**Mục tiêu.** Hiểu Catalyst đáng giá bao nhiêu, và vì sao đừng viết RDD nữa.

**Cách làm.** Đếm số đơn theo `order_status`, viết 3 cách:
- (a) `df.groupBy("order_status").count()`
- (b) `df.rdd.map(lambda r: (r.order_status, 1)).reduceByKey(lambda a,b: a+b).collect()`
- (c) Spark SQL: `spark.sql("SELECT order_status, COUNT(*) ...")`

Đo thời gian cả 3 (mỗi cái 3 lần). Chạy `explain()` cho (a) và (c) — chúng có giống hệt nhau không?

**Bằng chứng.** Bảng 3 dòng + 5 câu: vì sao (b) chậm hơn hẳn dù logic y hệt? (từ khóa cần xuất hiện trong câu trả lời của bạn: *serialize sang Python worker*, *Catalyst không nhìn thấy lambda*, *whole-stage codegen*).

---

#### ◆ A8. Thứ tự transformation có quan trọng không?

**Mục tiêu.** Kiểm chứng một tín điều: "filter càng sớm càng tốt".

**Cách làm.** Viết 2 query cho cùng kết quả:
- (a) `read → filter(status='delivered') → join(items) → groupBy`
- (b) `read → join(items) → filter(status='delivered') → groupBy`

Đo thời gian, và quan trọng hơn: `explain()` cả hai. Filter ở (b) có bị Catalyst **tự đẩy xuống** trước join không?

**Bằng chứng.** 2 plan + kết luận trung thực. (Spoiler có chủ đích: Catalyst thường tự làm giúp bạn — nên bài học thật là *"biết khi nào nó KHÔNG làm giúp"*. Thử lại với một filter chứa Python UDF, ví dụ `F.udf(lambda s: s == 'delivered')` — giờ nó còn đẩy được không? Đó mới là bài học.)

---

#### ⭐ A9. `cache()` — đo, đừng đoán

**Mục tiêu.** Biết khi nào cache lãi, khi nào lỗ.

**Cách làm.**
1. Một DataFrame sau vài transformation nặng. Gọi `count()` 3 lần liên tiếp, đo từng lần.
2. Thêm `.cache()` (nhớ: cache là lazy — phải có một action để "mồi" nó). Lặp lại.
3. Mở tab **Storage** trên UI: bảng được cache tốn bao nhiêu MB? Đối chiếu với kích thước file gốc — vì sao cache thường *to hơn* dữ liệu trên đĩa?
4. Thử `persist(StorageLevel.MEMORY_AND_DISK)` và `MEMORY_ONLY_SER` — so RAM chiếm.

**Bằng chứng.** Bảng: lần chạy × (không cache / cache) + số MB ở Storage tab + 3 dòng: trong `ingest.py` của bạn, chỗ nào *đáng* cache và chỗ nào cache là phí (gợi ý: DataFrame chỉ dùng đúng 1 lần → cache là lỗ ròng).

**Bẫy.** Đây chính là bẫy của Checkpoint 1: bạn filter `_corrupt_record` hai lần (một lần lấy dòng sạch, một lần lấy dòng hỏng) trên một DataFrame chưa cache → Spark đọc file 2 lần, và tệ hơn, ở một số phiên bản còn ném lỗi. Bài này là để bạn *hiểu* cái bẫy đó chứ không chỉ né nó.

---

### Track L3 — Job / Stage / Task: đọc Spark UI (lesson 3)

#### ⭐ A10. Sổ dự đoán: 6 query, đoán trước — chạy sau

**Mục tiêu.** Kỹ năng cốt lõi của bài này: nhìn code, đoán ra DAG.

**Cách làm.** Tắt AQE trước (`spark.sql.adaptive.enabled=false`) để số liệu sạch. Với mỗi query dưới đây, **viết dự đoán vào bảng TRƯỚC**, rồi chạy và đối chiếu UI:

| # | Query | Job dự đoán | Stage dự đoán | Shuffle? | Thực tế |
|---|---|---|---|---|---|
| 1 | `read.csv.count()` | | | | |
| 2 | `read → filter → count()` | | | | |
| 3 | `read → groupBy(status).count().show()` | | | | |
| 4 | `orders.join(items, "order_id").count()` | | | | |
| 5 | `read → distinct().count()` | | | | |
| 6 | `read → filter → count()` rồi `→ show()` (2 action) | | | | |

**Bằng chứng.** Bảng đã điền đủ 2 cột dự đoán/thực tế + một đoạn "tôi đoán sai ở query số __, vì tôi tưởng ___, thực ra ___". **Dòng đoán-sai này là phần đáng giá nhất của cả bài tập** — không có nó coi như chưa làm.

---

#### ⭐ A11. Ranh giới stage nằm ở đâu

**Mục tiêu.** Nhìn thấy tận mắt: narrow không cắt stage, wide thì cắt.

**Cách làm.** Xây một chain dài toàn narrow (10 cái `withColumn` + `filter` + `select`) → đếm stage. Rồi chèn **một** `repartition(4)` vào giữa → đếm lại. Rồi chèn thêm `groupBy` → đếm lại.

Vào tab **Stages**, click vào DAG Visualization, chụp lại hình hộp — mỗi hộp là một stage, đường kẻ nối là shuffle.

**Bằng chứng.** 3 ảnh DAG + công thức bạn tự rút ra: `số stage = số shuffle + 1` (đúng không? khi nào sai?).

---

#### ◆ A12. Săn "skipped stage"

**Mục tiêu.** Hiểu shuffle reuse — thứ giải thích vì sao job thứ hai nhanh bất thường.

**Cách làm.** Tạo một DataFrame có shuffle (`groupBy`), rồi gọi **2 action** trên nó (`count()` rồi `show()`). Vào tab Jobs: job thứ hai có stage màu xám ghi `(skipped)`. Đọc số task của stage đó.

**Bằng chứng.** Ảnh/ghi chép stage skipped + giải thích: Spark tái dùng cái gì, nó nằm ở đâu trên đĩa (từ khóa: *shuffle files*, *local disk của executor*), và vì sao nếu executor chết thì cái "skipped" đó biến mất.

---

#### ◆ A13. Đặt tên cho job — biến Spark UI thành tài liệu

**Mục tiêu.** Thói quen của người làm production.

**Cách làm.** Trong `ingest.py` thật, trước mỗi bước gọi:

```python
spark.sparkContext.setJobDescription("CP1: read orders + quarantine")
# ... action ...
spark.sparkContext.setJobDescription("CP2: write orders_clean partitioned")
```

Chạy pipeline, mở tab Jobs — giờ nó đọc được như mục lục.

**Bằng chứng.** Ảnh tab Jobs với các mô tả tiếng người. (Bài này 10 phút nhưng là thứ mentor sẽ nhớ về bạn.)

---

#### ○ A14. AQE bật/tắt — cùng query, hai số phận

**Mục tiêu.** Nếm trước module 3.

**Cách làm.** `groupBy("order_status")` (chỉ ~8 giá trị khác nhau!) với `spark.sql.shuffle.partitions=200`:
- AQE **off**: xem tab Stages — stage sau shuffle có bao nhiêu task? Bao nhiêu task xử lý 0 record?
- AQE **on**: đếm lại. Trên tab SQL, tìm node `AQEShuffleRead` và chữ `coalesced`.

**Bằng chứng.** Bảng 2 dòng: số task sau shuffle, thời gian, và một câu: AQE đã làm gì (từ khóa: *coalesce partition sau khi nhìn thấy kích thước thật*).

---

### Track L4 — Partition: nút vặn quan trọng nhất (lesson 4)

#### ⭐ A15. `maxPartitionBytes` — vặn và nhìn số task đổi

**Mục tiêu.** Hiểu partition lúc ĐỌC được quyết định thế nào.

**Cách làm.** Đọc `olist_geolocation_dataset.csv` (58MB) với 3 giá trị:

```python
for size in ["128m", "16m", "4m"]:
    spark.conf.set("spark.sql.files.maxPartitionBytes", size)
    df = spark.read.schema(S).csv(PATH)
    print(size, df.rdd.getNumPartitions())   # rồi chạy count() và đo thời gian
```

Điền bảng: `maxPartitionBytes | numPartitions | số task ở stage 0 | thời gian count()`.

Sau đó tự kiểm chứng công thức trong lesson 4: `numPartitions ≈ totalBytes / maxPartitionBytes` — số bạn đo có khớp không? Nếu lệch, thủ phạm là `openCostInBytes` — tìm hiểu và giải thích.

**Bằng chứng.** Bảng 3 dòng + đường cong thời gian (không phải cứ nhiều partition là nhanh — chỉ ra điểm đảo chiều và giải thích bằng *overhead mỗi task*).

**Bẫy.** CSV **không nén** thì chia được; file `.csv.gz` là *không splittable* → 1 partition duy nhất dù file 10GB. Thử `gzip` một file Olist rồi đọc lại, xem `getNumPartitions()` trả về gì. Đây là bài học đắt giá nhất của track này.

---

#### ⭐ A16. Con số 200 định mệnh

**Mục tiêu.** Thấy tận mắt sự lãng phí của default `spark.sql.shuffle.partitions=200`.

**Cách làm.** `groupBy("order_status").count()` (8 nhóm) với shuffle.partitions = 200 vs 8 vs 1. Với mỗi cái ghi: số task, thời gian, và **số task xử lý 0 byte** (vào tab Stages → click stage → xem cột "Input Size / Records" của từng task; hoặc xem summary metrics, quartile min = 0).

**Bằng chứng.** Bảng 3 dòng + câu trả lời: 200 task cho 8 nhóm thì 192 task làm gì? Chúng vô hại hay tốn kém? (gợi ý: mỗi task vẫn phải được schedule, khởi tạo, ghi một shuffle file rỗng...).

---

#### ⭐ A17. `repartition` vs `coalesce` — hai anh em không giống nhau

**Mục tiêu.** Chọn đúng cái trước khi ghi file — đây là quyết định bạn sẽ ra hàng ngày.

**Cách làm.** Từ một DataFrame 200 partition, giảm về 8 bằng 2 cách. Với mỗi cách:
1. `explain()` — có `Exchange` (shuffle) không?
2. Đo phân bố dữ liệu: `df.rdd.glom().map(len).collect()` → in list 8 số. Có đều không?
3. Đo thời gian.

**Bằng chứng.** Bảng so sánh 3 tiêu chí + kết luận: trong `ingest.py`, trước `write.partitionBy("order_date")` bạn dùng cái nào và **vì sao** (câu này rơi thẳng vào rubric mục "Thiết kế ghi", 25 điểm).

**Bẫy.** `coalesce(1)` trên dữ liệu lớn không shuffle nhưng ép **toàn bộ upstream** co về 1 task — pipeline biến thành single-thread. `repartition(1)` thì shuffle nhưng giữ song song ở trên. Chứng minh điều này bằng số task ở stage TRƯỚC đó.

---

#### ◆ A18. Tự tay chế skew

**Mục tiêu.** Gặp kẻ thù trước khi nó gặp bạn ở production.

**Cách làm.** Join `orders` với `customers`, rồi `repartition("customer_state")`. Bang SP chiếm ~42% dữ liệu Olist. Vào tab Stages → Summary Metrics của stage sau shuffle: đọc **Duration** ở các quartile Min / 25th / Median / 75th / Max.

**Bằng chứng.** Bảng quartile + tỉ lệ `Max / Median`. Nếu tỉ lệ > 3 → bạn vừa nhìn thấy skew. Viết 3 dòng: cả job phải chờ ai?

**Bẫy.** Đừng "sửa" skew ở bài này (salting là lesson module 3). Nhiệm vụ của bạn ở đây chỉ là **đo và mô tả** nó cho chính xác.

---

#### ◆ A19. Soi bên trong từng partition

**Mục tiêu.** Có một đồng hồ đo mà bạn sẽ dùng cả sự nghiệp.

**Cách làm.** Viết một helper nhỏ (2–3 dòng) rồi dùng lại ở mọi bài:

```python
def partition_sizes(df):
    return df.rdd.glom().map(len).collect()   # TODO: in min/max/mean/stddev, vẽ histogram thô bằng '#'
```

Dùng nó soi: DataFrame lúc mới đọc, sau `filter` (chú ý: filter tạo ra partition rỗng!), sau `repartition(8)`, sau `repartition("order_status")`.

**Bằng chứng.** 4 histogram + nhận xét: sau filter mạnh (giữ 1% dữ liệu) thì 200 partition kia ra sao — và điều đó gợi ý bạn nên gọi hàm gì tiếp theo.

**Bẫy.** `glom().collect()` kéo *toàn bộ* dữ liệu về driver — dùng được vì Olist bé. Ở production dùng `mapPartitions` đếm rồi mới collect (chỉ về N con số). Ghi chú điều này trong code, nó cho thấy bạn biết mình đang làm gì.

---

#### ⭐ A20. Sizing thực chiến: chọn số file cho `orders_clean`

**Mục tiêu.** Ra một quyết định kỹ thuật có con số đỡ lưng — đây là bài tập "senior" nhất của track này.

**Cách làm.**
1. Tính: orders có ~100k dòng, ~17MB CSV. Sau khi thành Parquet nén snappy thì còn bao nhiêu? (đo, đừng đoán).
2. Quy tắc nghề: mỗi file Parquet nên 64–256MB. Với dữ liệu này, con số "đúng chuẩn" là **1 file**. Nhưng đề bài bắt partition theo `order_date` (~600 ngày) → 600 file × vài chục KB.
3. Viết trong report: đây là **mâu thuẫn có thật**, không phải bạn làm sai. Sau đó trả lời: ở kích thước dữ liệu nào thì `partitionBy(order_date)` bắt đầu đúng? (gợi ý: mỗi partition-ngày cần ≥64MB → mỗi ngày cần bao nhiêu đơn hàng → Olist cần lớn gấp mấy lần?)
4. Cài một "van an toàn" vào code: `partitionBy` theo tháng khi dữ liệu nhỏ, theo ngày khi lớn — điều khiển bằng một hằng số ở đầu file.

**Bằng chứng.** Phép tính có số + đoạn code có van + kết luận. (Bài này chính là câu hỏi mở rộng số 5 ở mục 7 — làm bài này là trả lời xong câu đó.)

---

### Track L5 — Data Sources API: cửa vào/ra (lesson 5)

#### ⭐ A21. Sinh schema tự động rồi sửa tay

**Mục tiêu.** Mẹo nghề: không ai gõ tay 40 `StructField`.

**Cách làm.**
1. **Một lần duy nhất, ở máy dev** (không nộp code này): `spark.read.option("inferSchema", True).option("header", True).csv(path)` rồi `print(df.schema.json())` hoặc `print(df._jdf.schema().treeString())`.
2. Copy output vào `schemas.py`, rồi **sửa tay**: `inferSchema` sẽ đoán sai — timestamp thành String, id số thành Integer (nguy hiểm: id có số 0 đứng đầu sẽ mất), tiền thành Double (chấp nhận được ở bài học; production dùng Decimal — ghi chú vì sao).
3. Lập bảng "Spark đoán gì / tôi sửa thành gì / vì sao".

**Bằng chứng.** Bảng 3 cột cho ít nhất 5 cột dữ liệu bị sửa.

**Bẫy.** `nullable=False` trên `order_id` là một **lời hứa với Spark**, không phải một kiểm tra. Spark tin bạn và tối ưu dựa trên đó; nếu dữ liệu vẫn có null, kết quả sai *im lặng*. Thử: khai `nullable=False` cho một cột có null thật, chạy, xem có ai báo lỗi không. Kinh nghiệm này đáng giá.

---

#### ⭐ A22. Ba read mode, một file bẩn

**Mục tiêu.** Chọn read mode có căn cứ, không theo thói quen.

**Cách làm.** Dùng file bẩn từ bài A23. Đọc cùng file 3 lần với `PERMISSIVE` / `DROPMALFORMED` / `FAILFAST`, ghi:

| mode | count() trả về | dòng hỏng đi đâu | có exception không | dùng khi nào |
|---|---|---|---|---|

Rồi trả lời câu hỏi của Checkpoint 1: vì sao dataset Olist này chọn PERMISSIVE mà không FAILFAST? Và ngược lại: pipeline nào *bắt buộc* phải FAILFAST? (gợi ý: dữ liệu tài chính, nơi mất 1 dòng tệ hơn dừng pipeline).

**Bằng chứng.** Bảng trên + 5 câu biện luận.

---

#### ⭐ A23. Tự chế dữ liệu bẩn (bài quan trọng nhất track này)

**Mục tiêu.** Bạn không thể xử lý dữ liệu bẩn nếu chưa biết dữ liệu bẩn *trông như thế nào*.

**Cách làm.** Copy `olist_orders_dataset.csv` thành `data/dirty/orders_dirty.csv`, rồi tự tay tiêm **6 loại bẩn** — mỗi loại vài dòng:

1. **Thiếu cột**: xóa 2 field ở cuối một dòng.
2. **Thừa cột**: thêm một field lạ vào giữa.
3. **Sai kiểu**: `order_purchase_timestamp` = `"hôm qua"`.
4. **Dấu phẩy trong text**: một field chứa `São Paulo, SP` mà **không** có ngoặc kép.
5. **Ngoặc kép lệch**: mở `"` mà không đóng.
6. **Dòng trống** + một dòng lặp lại header ở giữa file.

Rồi đọc bằng PERMISSIVE + `_corrupt_record`, lập bảng: **loại bẩn nào Spark bắt được? loại nào lọt qua và trở thành dữ liệu sai im lặng?**

**Bằng chứng.** Bảng 6 dòng. Kết luận: `_corrupt_record` bắt được lỗi *cấu trúc*, không bắt được lỗi *ngữ nghĩa* → đó là lý do bạn cần bài A38 (data quality gate).

**Bẫy.** Loại 4 sẽ khiến số cột đúng nhưng **giá trị lệch cột** — Spark có thể không hề báo lỗi. Đây là loại bẩn nguy hiểm nhất trên đời và bạn vừa tự tạo ra nó.

---

#### ⭐ A24. Tái hiện bẫy `_corrupt_record` rồi sửa

**Mục tiêu.** Gặp đúng cái bẫy mà rubric đang chờ bạn ngã.

**Cách làm.**
1. Đọc file bẩn, **không** cache, rồi:

```python
df = spark.read.schema(S_with_corrupt).option("mode","PERMISSIVE").csv(dirty)
bad  = df.filter(F.col("_corrupt_record").isNotNull())
good = df.filter(F.col("_corrupt_record").isNull())
bad.count(); good.count()      # ← chạy đi, xem điều gì xảy ra
```

2. Ghi lại chính xác hiện tượng (lỗi? hay chạy được nhưng số sai? tùy phiên bản Spark).
3. Sửa bằng 3 cách khác nhau: `.cache()` trước khi filter · ghi `df` ra Parquet tạm rồi đọc lại · `.persist()` + trigger action. So thời gian và độ sạch của từng cách.

**Bằng chứng.** Nguyên văn hiện tượng + 3 cách sửa + chọn 1 cho `ingest.py` với lý do.

---

#### ◆ A25. Bốn save mode

**Mục tiêu.** Idempotency (rubric −15 điểm nếu sai) bắt đầu từ đây.

**Cách làm.** Ghi cùng một DataFrame ra 4 thư mục với 4 mode, mỗi mode **chạy 2 lần liên tiếp**, rồi đếm dòng đọc lại:

| mode | count sau lần 1 | count sau lần 2 | có exception? |
|---|---|---|---|
| `overwrite` | | | |
| `append` | | | |
| `errorifexists` (mặc định!) | | | |
| `ignore` | | | |

**Bằng chứng.** Bảng + câu: mode nào cho idempotent, mode nào là cái bẫy nhân đôi dữ liệu mà rubric trừ 15 điểm.

---

#### ⭐ A26. `partitionOverwriteMode`: static vs dynamic

**Mục tiêu.** Bài học re-run một ngày mà không giết cả bảng — thứ bạn sẽ dùng mỗi tuần trong đời thật.

**Cách làm.**
1. Ghi `orders_clean` phân vùng theo `order_date` (đủ 600 ngày).
2. Giờ giả lập "ngày 2018-07-02 bị tính sai, cần chạy lại": lọc riêng dữ liệu ngày đó, ghi `mode("overwrite").partitionBy("order_date")` với `spark.sql.sources.partitionOverwriteMode = static` (mặc định).
3. `find` đếm số thư mục partition còn lại. **Bao nhiêu?** (chuẩn bị tinh thần).
4. Ghi lại toàn bộ, đổi sang `dynamic`, lặp lại bước 2–3.

**Bằng chứng.** Số thư mục trước/sau ở cả 2 mode + một câu bằng chữ in hoa mà bạn sẽ không bao giờ quên nữa.

---

#### ◆ A27. Bốn format, một bảng số

**Mục tiêu.** Nền cho Checkpoint 3 và cho track L6.

**Cách làm.** Ghi cùng `orders_clean` ra CSV, JSON, Parquet, ORC (+ Avro nếu jar sẵn). Với mỗi format đo: `du -sh`, thời gian ghi, thời gian đọc `count()`, thời gian `select(1 cột).sum()`.

**Bằng chứng.** Bảng 4 format × 4 số đo + câu: vì sao JSON to hơn CSV? vì sao Parquet đọc-1-cột nhanh gấp nhiều lần dù cùng số dòng?

---

#### ○ A28. JDBC partitioned read

**Mục tiêu.** Biết cách đọc database mà không giết nó bằng 1 connection.

**Cách làm.** Nếu `docker-compose.yaml` của repo có Postgres: đổ `olist_orders` vào một bảng, rồi đọc bằng `spark.read.jdbc(...)` hai lần:
- (a) không có `partitionColumn` → xem `getNumPartitions()` (spoiler: 1) → 1 task, 1 connection, không song song.
- (b) có đủ 4 option: `partitionColumn` / `lowerBound` / `upperBound` / `numPartitions=8` → đếm lại partition và bật log SQL của Postgres để **nhìn thấy 8 câu SELECT với 8 dải WHERE khác nhau**.

Không có DB? Vẫn làm được phần lý thuyết: viết ra 8 câu SQL mà Spark *sẽ* sinh ra với `lowerBound=1, upperBound=100000, numPartitions=8`. Sai một ly ở đây là skew ngay.

**Bằng chứng.** 8 câu SQL (thật hoặc suy ra) + câu trả lời: nếu `partitionColumn` là cột phân bố lệch (ví dụ id tăng dần nhưng dữ liệu dồn cuối) thì chuyện gì xảy ra?

---

#### ◆ A29. Truy vết nguồn gốc từng dòng

**Mục tiêu.** Khi sếp hỏi "dòng rác này từ file nào ra?", bạn phải trả lời được trong 30 giây.

**Cách làm.** Đọc bằng glob (`f"{SRC}/olist_order*.csv"`), thêm `F.input_file_name()` thành cột `source_file`, cộng `F.current_timestamp()` thành `ingest_ts`. Ghi cả hai vào bảng quarantine (Checkpoint 1 đã yêu cầu — bài này là để bạn *hiểu* nó chứ không chỉ chép).

**Bằng chứng.** Vài dòng quarantine có đủ `source_file` + `ingest_ts` + `_corrupt_record`. Và một câu: cột nào trong bộ ba này giúp bạn *replay* được lỗi?

---

### Track L6 — Parquet & columnar (lesson 6)

#### ⭐ A30. Column pruning — đo bằng bytes, không bằng giây

**Mục tiêu.** Kỹ năng chấm điểm cao nhất của Checkpoint 3: chỉ vào *bytes read*, không chỉ vào đồng hồ.

**Cách làm.** Trên `orders_clean` Parquet, chạy 2 query: `select("*").count()` vs `select("price").agg(sum)`. Với mỗi cái, vào tab **SQL** → click query → node `Scan parquet` → đọc 2 dòng: **`number of files read`** và **`size of files read`**.

**Bằng chứng.** Bảng 2 dòng × 2 metric + tỉ lệ bytes. Câu chốt: thời gian có thể chênh nhau ít (Olist bé), nhưng bytes chênh nhau __ lần → ở dữ liệu 100× thì thời gian sẽ chênh cỡ nào?

---

#### ⭐ A31. Bốn thuật nén

**Mục tiêu.** Chọn codec có căn cứ.

**Cách làm.** Ghi `orders_clean` 4 lần với `option("compression", ...)`: `none` / `snappy` / `gzip` / `zstd`. Đo: kích thước thư mục, thời gian ghi, thời gian đọc-full, thời gian đọc-1-cột.

**Bằng chứng.** Bảng 4×4 + quyết định: pipeline của bạn dùng codec nào, vì sao. (Câu trả lời "snappy vì mặc định" bị trừ điểm; "snappy vì splittable + CPU rẻ, còn gzip nén tốt hơn __% nhưng không splittable nên một file lớn thành một task" thì được.)

---

#### ◆ A32. `sortWithinPartitions` — mồi cho min/max statistics

**Mục tiêu.** Hiểu tại sao Parquet có thể **bỏ qua cả row group** mà không cần đọc.

**Cách làm.**
1. Ghi `order_items` ra Parquet **không sort**. Chạy `filter(price > 1500)` (giá trị hiếm), đọc `size of files read` từ UI.
2. Ghi lại với `.sortWithinPartitions("price")` trước khi write. Chạy đúng query đó, đọc lại metric.
3. So bytes.

**Bằng chứng.** 2 con số bytes + giải thích cơ chế: khi dữ liệu được sort, min/max của mỗi row group hẹp lại → reader so `price > 1500` với `max` của row group → bỏ qua nguyên block. Không sort thì mọi row group đều có min thấp max cao → không bỏ qua được cái nào.

**Bẫy.** Chỉ sort được theo **một** chiều. Nếu query hay filter theo `product_id` thì sort theo `price` vô ích. Ghi vào report: bạn chọn sort theo cột nào và bạn đang đánh cược vào query pattern nào.

---

#### ◆ A33. Mổ file Parquet bằng PyArrow

**Mục tiêu.** Nhìn tận mắt cấu trúc mà lesson 6 mô tả.

**Cách làm.** Trong `make shell` (hoặc venv có `pyarrow`):

```python
import pyarrow.parquet as pq
f = pq.ParquetFile("<một file part-*.parquet>")
print(f.metadata)                      # num_rows, num_row_groups, created_by
rg = f.metadata.row_group(0)
for i in range(rg.num_columns):
    c = rg.column(i)
    # TODO: in path_in_schema, compression, encodings,
    #       total_compressed_size vs total_uncompressed_size,
    #       và c.statistics.min / .max / .null_count
```

**Bằng chứng.** Output cho ít nhất 5 cột + 3 nhận xét: (a) cột nào nén tốt nhất và vì sao (gợi ý: `order_status` chỉ có 8 giá trị → dictionary encoding), (b) min/max của cột `order_date` có khớp với tên thư mục partition không, (c) file của bạn có mấy row group — có đạt 128MB/row group như lý thuyết không, và điều đó nói gì về kích thước dữ liệu của bạn.

---

#### ○ A34. Schema evolution

**Mục tiêu.** Trả lời câu hỏi mở rộng số 1 bằng thí nghiệm chứ không bằng cảm tính.

**Cách làm.** Ghi `orders_clean` (8 cột) vào thư mục X. Rồi thêm cột `order_priority` vào DataFrame, ghi **append** vào cùng X. Giờ đọc X với:
- mặc định → có thấy cột mới không?
- `option("mergeSchema", "true")` → có? Giá trị của cột mới ở các file cũ là gì?
- Thử ngược lại: **xóa** một cột rồi append. Đọc lại — chuyện gì xảy ra?
- Thử **đổi kiểu** một cột (Double → String) rồi append. Lần này?

**Bằng chứng.** Bảng 4 kịch bản × (đọc được không / kết quả ra sao) + kết luận: Parquet trần chịu được kiểu thay đổi nào, gãy ở kiểu nào → đó chính là chỗ Iceberg (module 5) sinh ra để cứu.

---

#### ⭐ A35. Small files: gây án rồi phá án

**Mục tiêu.** Đây là bài "before/after" mạnh nhất của cả project — nếu chỉ làm được 1 bài trong phụ lục này, làm bài này.

**Cách làm.**
1. **Gây án:** ghi `orders_clean` với `partitionBy("order_date")` mà **không** repartition trước, để nguyên `shuffle.partitions=200`. Đếm file: `find ... -name 'part-*' | wc -l` (chuẩn bị tinh thần: có thể lên tới hàng chục nghìn file).
2. Đo: thời gian ghi, `du -sh`, thời gian chạy query A (revenue theo tháng), và **`number of files read`** trên UI.
3. **Phá án:** thêm `.repartition("order_date")` trước khi ghi. Đếm file lại. Đo lại 4 số trên.
4. Thử biến thể thứ ba: `.repartition(1)` → 1 file/partition nhưng ghi chậm? Và `.coalesce(8)` → chuyện gì xảy ra với layout partition?

**Bằng chứng.** Bảng: `chiến lược | số file | tổng size | thời gian ghi | thời gian query | files read`. Rồi giải thích **vì sao tổng dung lượng cũng tăng** khi có nhiều file nhỏ (gợi ý: mỗi file Parquet có footer/metadata riêng — chi phí cố định × số file), và vì sao đọc 10.000 file nhỏ chậm hơn đọc 600 file dù cùng số byte (gợi ý: mỗi file = một lần open + một task).

---

#### ◆ A36. Partition pruning — bằng chứng cuối cùng

**Mục tiêu.** Khép lại Checkpoint 3 với một con số không cãi được.

**Cách làm.** Query B (`order_date = '2018-07-02'`) trên Parquet phân vùng:
1. `explain()` → tìm dòng `PartitionFilters: [isnotnull(order_date#..), (order_date#.. = 2018-07-02)]`. Khoanh nó.
2. UI → `number of files read`: phải là **1** (hoặc số file trong đúng 1 partition), so với tổng ~600.
3. Giờ phá nó: viết filter theo cách Spark **không** prune được — ví dụ `F.date_format("order_date","yyyy-MM-dd") == "2018-07-02"` hoặc filter qua UDF. Chạy `explain()` lại: `PartitionFilters` biến mất, filter rơi xuống thành `Filter` thường → đọc lại `number of files read` (spoiler: 600).

**Bằng chứng.** 2 plan cạnh nhau + 2 con số files-read. Bài học: **partition pruning không phải phép màu tự động — nó chỉ hoạt động khi bạn filter thẳng vào cột partition bằng biểu thức đơn giản.** Đây là lỗi #1 khiến pipeline production chậm 100× mà không ai hiểu vì sao.

---

### Track tổng hợp — ráp thành pipeline thật

#### ◆ A37. Layout Bronze → Silver → Gold

**Mục tiêu.** Biết mình đang đứng ở tầng nào của một lakehouse.

**Cách làm.** Tổ chức lại output thành 3 tầng:
- `bronze/` — CSV đọc vào, chưa động gì, chỉ thêm `source_file` + `ingest_ts` (ghi Parquet cho gọn).
- `silver/` — đã ép kiểu, đã lọc dòng hỏng, đã derive `order_date`, đã partition. (Chính là `orders_clean` hiện tại.)
- `gold/` — một bảng mart nhỏ: `daily_revenue(order_date, n_orders, revenue, avg_ticket)` — join orders×items, group theo ngày. Bảng này bé (600 dòng) → ghi **1 file, không partition**. Biện luận vì sao gold không partition.

**Bằng chứng.** Cây thư mục 3 tầng + 3 dòng: mỗi tầng phục vụ ai (bronze: replay/audit; silver: analyst; gold: dashboard).

---

#### ◆ A38. Cổng chất lượng dữ liệu (data quality gate)

**Mục tiêu.** Bắt được loại lỗi mà `_corrupt_record` ở A23 không bắt được.

**Cách làm.** Trước khi ghi silver, chạy một loạt kiểm tra và **cho pipeline chết có chủ đích** nếu vi phạm:

```python
CHECKS = [
    ("order_id không null",        lambda d: d.filter(F.col("order_id").isNull()).count() == 0),
    ("order_id unique",            lambda d: d.count() == d.select("order_id").distinct().count()),
    ("order_status trong tập hợp", ...),   # 8 giá trị hợp lệ, cái thứ 9 = dữ liệu lạ
    ("price >= 0",                 ...),
    ("order_date trong 2016..2018",...),   # ngày năm 1970 hoặc 2099 = timestamp hỏng
    ("tỉ lệ null của delivered_date < 5%", ...),
]
# TODO: chạy hết, in bảng PASS/FAIL, raise nếu có FAIL nào ở mức "chặn"
```

Phân biệt 2 mức: **blocking** (sai là dừng pipeline) vs **warning** (ghi log, vẫn chạy). Quyết định check nào thuộc mức nào — và biện luận.

**Bằng chứng.** Bảng kết quả các check trên dữ liệu Olist thật (sẽ có cái FAIL — Olist bẩn thật!) + biện luận mức blocking/warning.

---

#### ◆ A39. Ingest incremental — chạy lại một ngày

**Mục tiêu.** Trả lời câu hỏi mở rộng số 2 bằng code chạy được. Đây là hình dạng của 90% pipeline batch ngoài đời.

**Cách làm.**
1. Giả lập nguồn incremental: chia `olist_orders_dataset.csv` thành nhiều file theo ngày, đặt vào `data/incoming/dt=2018-07-01/...`, `dt=2018-07-02/...`
2. Viết `ingest.py` nhận **tham số ngày**: `make run F=... ARGS="--date 2018-07-02"` (đọc `sys.argv`).
3. Chỉ đọc file của ngày đó, chỉ ghi đè partition của ngày đó (dùng đúng bài học A26 — `dynamic`).
4. **Chứng minh idempotent:** chạy lệnh cho ngày 07-02 ba lần liên tiếp. Sau mỗi lần: `count()` toàn bảng + số thư mục partition + `count()` riêng ngày 07-02. Cả 3 lần phải y hệt.
5. Rồi chạy cho ngày 07-03 → ngày 07-02 phải **không suy suyển**.

**Bằng chứng.** Log 3 lần chạy + bảng count. Đây là bằng chứng idempotent mạnh hơn nhiều so với yêu cầu tối thiểu ở mục 3.4 — nó ăn trọn 25 điểm mục "Thiết kế ghi".

---

#### ⭐ A40. Bài toán ×100 — làm thật, không nói suông

**Mục tiêu.** Mục "Tư duy scale" trong rubric (10 điểm) và câu hỏi khó nhất mà interviewer sẽ hỏi bạn.

**Cách làm.**
1. **Tạo dữ liệu ×100 thật:** nhân bản orders bằng `spark.range(100).crossJoin(orders)` rồi làm nhiễu `order_id` (thêm hậu tố) và dịch ngày ngẫu nhiên trong khoảng 2016–2018 → ~10 triệu đơn, ~1.7GB CSV. Ghi ra `data/olist_100x/`.
2. Chạy **nguyên xi** `ingest.py` của bạn trên đó. Ghi lại: nó chạy được không? mất bao lâu? có OOM/spill không? (tab Stages → cột `Spill (memory)` / `Spill (disk)` — nếu có spill, ghi lại con số).
3. Điều gì gãy **trước tiên**? Sửa **một** thứ, đo lại. Lặp 2–3 vòng.
4. Viết mục 6 của report ("Nếu dữ liệu ×100 thì sao?") bằng số đo thật, không bằng suy đoán.

**Bằng chứng.** Bảng: `vòng | thứ đã sửa | thời gian | spill | số file`. Và câu chốt trung thực: cái gì trong thiết kế của bạn **vẫn đứng vững** ở ×100, cái gì **phải đổi**, và cái gì bạn **chưa biết** — chỉ ra điểm mù của mình là dấu hiệu của senior, không phải yếu kém.

**Bẫy.** `crossJoin` là wide transformation trên dữ liệu lớn — đây cũng là một bài học miễn phí. Xem nó tạo bao nhiêu task.

---

### Bảng theo dõi tiến độ

Copy vào `report.md`, tick dần:

| Bài | Lesson | Ưu tiên | Xong | Bằng chứng nằm ở đâu |
|---|---|---|---|---|
| A1 bản đồ cluster | L1 | ⭐ | ☐ | |
| A2 run vs run-local | L1 | ⭐ | ☐ | |
| A3 local vs cluster | L1 | ◆ | ☐ | |
| A4 giết driver | L1 | ◆ | ☐ | |
| A5 lazy có đồng hồ | L2 | ⭐ | ☐ | |
| A6 đọc explain() | L2 | ⭐ | ☐ | |
| A7 RDD vs DataFrame | L2 | ◆ | ☐ | |
| A8 thứ tự transformation | L2 | ◆ | ☐ | |
| A9 cache đo được | L2 | ⭐ | ☐ | |
| A10 sổ dự đoán 6 query | L3 | ⭐ | ☐ | |
| A11 ranh giới stage | L3 | ⭐ | ☐ | |
| A12 skipped stage | L3 | ◆ | ☐ | |
| A13 setJobDescription | L3 | ◆ | ☐ | |
| A14 AQE on/off | L3 | ○ | ☐ | |
| A15 maxPartitionBytes | L4 | ⭐ | ☐ | |
| A16 con số 200 | L4 | ⭐ | ☐ | |
| A17 repartition vs coalesce | L4 | ⭐ | ☐ | |
| A18 chế skew | L4 | ◆ | ☐ | |
| A19 soi partition | L4 | ◆ | ☐ | |
| A20 sizing thực chiến | L4 | ⭐ | ☐ | |
| A21 sinh schema | L5 | ⭐ | ☐ | |
| A22 ba read mode | L5 | ⭐ | ☐ | |
| A23 chế dữ liệu bẩn | L5 | ⭐ | ☐ | |
| A24 bẫy _corrupt_record | L5 | ⭐ | ☐ | |
| A25 bốn save mode | L5 | ◆ | ☐ | |
| A26 static vs dynamic | L5 | ⭐ | ☐ | |
| A27 bốn format | L5 | ◆ | ☐ | |
| A28 JDBC partitioned | L5 | ○ | ☐ | |
| A29 truy vết nguồn | L5 | ◆ | ☐ | |
| A30 column pruning | L6 | ⭐ | ☐ | |
| A31 bốn thuật nén | L6 | ⭐ | ☐ | |
| A32 sortWithinPartitions | L6 | ◆ | ☐ | |
| A33 mổ Parquet PyArrow | L6 | ◆ | ☐ | |
| A34 schema evolution | L6 | ○ | ☐ | |
| A35 small files | L6 | ⭐ | ☐ | |
| A36 partition pruning | L6 | ◆ | ☐ | |
| A37 bronze/silver/gold | tổng hợp | ◆ | ☐ | |
| A38 data quality gate | tổng hợp | ◆ | ☐ | |
| A39 incremental idempotent | tổng hợp | ◆ | ☐ | |
| A40 dữ liệu ×100 | tổng hợp | ⭐ | ☐ | |

**Mốc tự đánh giá:** làm hết ⭐ (18 bài) = bạn *thật sự* hiểu module 1, không phải "đã đọc xong module 1". Thêm ◆ = bạn đi phỏng vấn junior DE được. Thêm ○ = bạn đã chạm vào những thứ module 3 sắp dạy.

---

## 9. Next

**Module 2 · Lesson 7 — Transformations cốt lõi: select / filter / withColumn / when.**

Module 1 khép lại: bạn đã hiểu Spark chạy Ở ĐÂU (kiến trúc), KHI NÀO (lazy, job/stage/task), dữ liệu nằm THẾ NÀO (partition, Parquet) và RA/VÀO ra sao (Data Sources API) — phần "vật lý" của nghề. Module 2 chuyển sang phần "hóa học": biến đổi dữ liệu. Lesson 7 bắt đầu với bộ tứ transformation bạn sẽ gõ nhiều nhất đời — select, filter, withColumn, when — viết song song cả DataFrame API lẫn Spark SQL, và bài học performance đầu tiên của module: thứ tự transformation quyết định lượng dữ liệu chảy qua pipeline. Từ giờ, mọi demo đều chạy trên chính dữ liệu Parquet sạch mà Mini Project 1 của bạn vừa tạo ra — pipeline của bạn nuôi bài học của bạn.

> Gõ **"Continue"** khi sẵn sàng.
