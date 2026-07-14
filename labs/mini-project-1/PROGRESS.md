# Mini Project 1 — Sổ tiến độ & Bảng đo

> File này là **sổ tay của bạn**, không phải tài liệu. Mở nó mỗi buổi, tick, điền số, viết một dòng "tôi học được gì". Cuối project, 80% nội dung `report.md` đã nằm sẵn ở đây rồi.
>
> Ba nguyên tắc, vi phạm là tự lừa mình:
> 1. **Dự đoán trước khi chạy.** Ô "dự đoán" điền bằng bút, ô "thực tế" điền sau. Đoán sai là lúc học được.
> 2. **Không có số = chưa xong.** Ô trống trong bảng benchmark = bài tập chưa làm, dù code đã chạy.
> 3. **Đo 3 lần, lấy lần 2–3.** Lần 1 là JVM warmup + page cache lạnh, nó nói dối.

---

## Phần 1 — Lộ trình 6 buổi

Mỗi buổi có một **cổng ra**: chưa qua cổng thì đừng sang buổi sau, kiến thức sau sẽ mọc trên nền rỗng.

### Buổi 1 · Nhìn thấy Spark bằng mắt (2h)

*Chưa viết pipeline. Chỉ học cách nhìn.*

- [ ] **A1** Vẽ bản đồ cluster (Executors tab → bảng driver/executor/core/RAM)
- [ ] **A2** `make run` vs `make run-local` (bao gồm: `print()` trong transformation hiện ở đâu)
- [ ] **A5** Đo lazy bằng `time.time()` → phát hiện `inferSchema` là action trá hình
- [ ] **A6** Đọc `explain(mode="formatted")` — khoanh đủ 5 điểm
- [ ] **A9** `cache()` đo được (Storage tab)

> **🚪 Cổng ra:** Nhìn vào 5 dòng code bất kỳ, bạn chỉ đúng được **dòng nào gây ra I/O thật**. Chưa làm được → làm lại A5 + A6.
>
> Học được gì (viết 2 dòng, thật lòng): ______________________________________

### Buổi 2 · Đọc Spark UI như đọc chữ (2h)

- [ ] **A10** Sổ dự đoán 6 query (điền cột dự đoán TRƯỚC)
- [ ] **A11** Ranh giới stage — 3 ảnh DAG
- [ ] **A12** Săn skipped stage
- [ ] **A13** `setJobDescription` cho từng bước
- [ ] *(tuỳ chọn)* **A14** AQE on/off

> **🚪 Cổng ra:** Đưa bạn một query lạ, bạn đoán đúng số shuffle **trước khi** chạy. Sai ≥2/6 ở A10 → đọc lại lesson 3 mục 3.2, làm lại.
>
> Tôi đoán sai ở query số ___ vì tôi tưởng __________, thực ra __________.

### Buổi 3 · Partition — nút vặn quan trọng nhất (2.5h)

- [ ] **A15** `maxPartitionBytes` (nhớ thử file `.csv.gz`!)
- [ ] **A16** Con số 200 định mệnh
- [ ] **A17** `repartition` vs `coalesce`
- [ ] **A19** Soi partition bằng `glom()` — **viết helper, dùng lại cả project**
- [ ] **A20** Sizing thực chiến cho `orders_clean`
- [ ] *(tuỳ chọn)* **A18** Tự chế skew

> **🚪 Cổng ra:** Bạn trả lời được, có số: *"trước khi ghi `orders_clean` tôi dùng `repartition("order_date")` chứ không `coalesce`, vì ___"*. Câu này rơi thẳng vào 25 điểm rubric.
>
> Helper `partition_sizes()` đã viết ở file: ______________________

### Buổi 4 · Ingest thật + dữ liệu bẩn (3h) → **Checkpoint 1 + 2**

- [ ] **A21** Sinh schema bằng inferSchema (1 lần ở dev) rồi sửa tay → `schemas.py`
- [ ] **A23** Tự tiêm 6 loại dữ liệu bẩn → `data/dirty/orders_dirty.csv`
- [ ] **A22** Ba read mode trên file bẩn đó
- [ ] **A24** Tái hiện bẫy `_corrupt_record` rồi sửa
- [ ] **A25** Bốn save mode
- [ ] **A26** `partitionOverwriteMode` static vs dynamic ⚠️
- [ ] **A29** `input_file_name()` + `ingest_ts` vào quarantine
- [ ] ✅ **Checkpoint 1** — `ingest.py` đọc sạch/hỏng, quarantine chạy được
- [ ] ✅ **Checkpoint 2** — ghi Parquet partition theo `order_date`, idempotent

> **🚪 Cổng ra:** Chạy `ingest.py` **2 lần liên tiếp**, `count()` không đổi. Dán 2 log vào Phần 3.6 bên dưới. Chưa qua → xem lại A25 + A26.

### Buổi 5 · Parquet & Benchmark (3h) → **Checkpoint 3**

- [ ] **A27** Bốn format (CSV/JSON/Parquet/ORC)
- [ ] **A31** Bốn thuật nén
- [ ] **A30** Column pruning — đo bằng **bytes**, không bằng giây
- [ ] **A35** Small files: gây án rồi phá án ⚠️ *(bài nặng ký nhất)*
- [ ] **A36** Partition pruning — và cách **phá** nó
- [ ] **A33** Mổ file bằng PyArrow
- [ ] *(tuỳ chọn)* **A32** `sortWithinPartitions`
- [ ] ✅ **Checkpoint 3** — `benchmark.py` + Phần 3 file này điền đủ số

> **🚪 Cổng ra:** Mọi ô trong Phần 3 có số. Mọi kết luận "nhanh hơn" đều kèm con số. Ô trống = trừ điểm.

### Buổi 6 · Ráp lại & viết report (2.5h)

- [ ] **A37** Bronze / Silver / Gold
- [ ] **A38** Data quality gate (sẽ có check FAIL — Olist bẩn thật)
- [ ] **A39** Ingest incremental + chứng minh idempotent 3 lần chạy
- [ ] **A40** Dữ liệu ×100 — chạy thật, tìm điểm gãy đầu tiên
- [ ] 📄 **`report.md`** — copy số từ Phần 3 file này sang
- [ ] 💬 Trả lời 6 câu hỏi mở rộng (mục 7 đề bài)
- [ ] *(bonus)* Checkpoint 4 — Iceberg + Trino

> **🚪 Cổng ra:** Đưa report cho một người **không** làm project này, họ hiểu bạn đã quyết định gì và vì sao — mà không cần hỏi lại.

---

## Phần 2 — Bảng tick tổng (40 bài)

| ✓ | Bài | Lesson | Ưu tiên | File code | Số đo nằm ở đâu |
|---|---|---|---|---|---|
| ☐ | A1 bản đồ cluster | L1 | ⭐ | | §3.0 |
| ☐ | A2 run vs run-local | L1 | ⭐ | | |
| ☐ | A3 local vs cluster | L1 | ◆ | | §3.1 |
| ☐ | A4 giết driver | L1 | ◆ | | |
| ☐ | A5 lazy có đồng hồ | L2 | ⭐ | | §3.1 |
| ☐ | A6 đọc explain() | L2 | ⭐ | | §3.7 |
| ☐ | A7 RDD vs DataFrame | L2 | ◆ | | §3.1 |
| ☐ | A8 thứ tự transformation | L2 | ◆ | | |
| ☐ | A9 cache đo được | L2 | ⭐ | | §3.1 |
| ☐ | A10 sổ dự đoán 6 query | L3 | ⭐ | | §3.2 |
| ☐ | A11 ranh giới stage | L3 | ⭐ | | §3.2 |
| ☐ | A12 skipped stage | L3 | ◆ | | |
| ☐ | A13 setJobDescription | L3 | ◆ | | |
| ☐ | A14 AQE on/off | L3 | ○ | | §3.3 |
| ☐ | A15 maxPartitionBytes | L4 | ⭐ | | §3.3 |
| ☐ | A16 con số 200 | L4 | ⭐ | | §3.3 |
| ☐ | A17 repartition vs coalesce | L4 | ⭐ | | §3.3 |
| ☐ | A18 chế skew | L4 | ◆ | | §3.3 |
| ☐ | A19 soi partition | L4 | ◆ | | |
| ☐ | A20 sizing thực chiến | L4 | ⭐ | | §3.4 |
| ☐ | A21 sinh schema | L5 | ⭐ | | |
| ☐ | A22 ba read mode | L5 | ⭐ | | §3.6 |
| ☐ | A23 chế dữ liệu bẩn | L5 | ⭐ | | §3.6 |
| ☐ | A24 bẫy _corrupt_record | L5 | ⭐ | | |
| ☐ | A25 bốn save mode | L5 | ◆ | | §3.6 |
| ☐ | A26 static vs dynamic | L5 | ⭐ | | §3.6 |
| ☐ | A27 bốn format | L5 | ◆ | | §3.4 |
| ☐ | A28 JDBC partitioned | L5 | ○ | | |
| ☐ | A29 truy vết nguồn | L5 | ◆ | | |
| ☐ | A30 column pruning | L6 | ⭐ | | §3.5 |
| ☐ | A31 bốn thuật nén | L6 | ⭐ | | §3.4 |
| ☐ | A32 sortWithinPartitions | L6 | ◆ | | §3.5 |
| ☐ | A33 mổ Parquet PyArrow | L6 | ◆ | | |
| ☐ | A34 schema evolution | L6 | ○ | | |
| ☐ | A35 small files | L6 | ⭐ | | §3.4 |
| ☐ | A36 partition pruning | L6 | ◆ | | §3.5, §3.7 |
| ☐ | A37 bronze/silver/gold | tổng hợp | ◆ | | |
| ☐ | A38 data quality gate | tổng hợp | ◆ | | §3.6 |
| ☐ | A39 incremental idempotent | tổng hợp | ◆ | | §3.6 |
| ☐ | A40 dữ liệu ×100 | tổng hợp | ⭐ | | §3.8 |

**Đếm:** ⭐ ___/18 · ◆ ___/16 · ○ ___/6 · **Tổng ___/40**

---

## Phần 3 — Bảng đo (điền số vào đây, đừng để trống)

### 3.0 · Môi trường đo — ghi một lần, mọi số dưới đây phụ thuộc nó

Không có phần này thì mọi con số bên dưới **vô nghĩa** (không ai tái lập được).

| | |
|---|---|
| Máy | CPU ___ core, RAM ___ GB |
| Spark | version ___ · master `___` · deploy-mode ___ |
| Executor | ___ executor × ___ core × ___ GB |
| Lệnh chạy | `make run-local F=...` / `make run F=...` |
| Config đã đổi | `spark.sql.shuffle.partitions=___`, AQE=___, `maxPartitionBytes=___` |
| Dữ liệu | Olist gốc (___ MB) / ×100 (___ GB) |

> **Cách đo cho ổn định** (dùng chung cho mọi bảng):
> ```python
> def bench(name, fn, runs=3):
>     ts = []
>     for i in range(runs):
>         t = time.time(); r = fn(); ts.append(time.time() - t)
>     # TODO: in name, ts[0] (lạnh), min(ts[1:]) (ấm), và kết quả r để chắc query có chạy thật
>     # TODO: nhớ trả về cả ts để ghi vào bảng
> ```
> **Bẫy:** `fn` phải kết thúc bằng một **action** (`count()`, `collect()`, `write`). Nếu `fn` chỉ trả về DataFrame thì bạn vừa đo... 0.001 giây của lazy. Đây là lỗi benchmark #1.

---

### 3.1 · Thời gian query: CSV vs Parquet *(Checkpoint 3 — bắt buộc)*

| Query | CSV lạnh | CSV ấm | Parquet lạnh | Parquet ấm | Tăng tốc (ấm/ấm) | 🎯 Ngưỡng |
|---|---|---|---|---|---|---|
| **A** — revenue theo tháng (full scan) | | | | | ___× | ≥ 2× |
| **B** — revenue 1 ngày (`2018-07-02`) | | | | | ___× | ≥ 5× |
| **C** — `select 1 cột, sum()` | | | | | ___× | ≥ 3× |
| **D** — `count(*)` toàn bảng | | | | | ___× | ≥ 10× *(Parquet đọc footer, không đọc data)* |

**Đọc số này thế nào:** Olist bé (17MB), chênh lệch **giây** sẽ khiêm tốn và thậm chí có thể *ngược* (overhead > lợi ích). Nếu số của bạn không đạt ngưỡng — **đừng sửa số, hãy giải thích**. Bảng thật sự chấm điểm bạn là §3.5 (bytes), không phải bảng này.

Giải thích của tôi cho query nào không đạt ngưỡng: _______________________________

---

### 3.2 · Sổ dự đoán DAG *(A10, A11)*

| # | Query | Job (đoán) | Job (thật) | Stage (đoán) | Stage (thật) | Shuffle (đoán) | Shuffle (thật) | ✓? |
|---|---|---|---|---|---|---|---|---|
| 1 | `read.count()` | | | | | | | |
| 2 | `read → filter → count()` | | | | | | | |
| 3 | `groupBy(status).count().show()` | | | | | | | |
| 4 | `orders.join(items).count()` | | | | | | | |
| 5 | `distinct().count()` | | | | | | | |
| 6 | 2 action trên cùng 1 df | | | | | | | |

**Tỉ lệ đoán đúng: ___/6.** Dưới 4/6 → chưa qua cổng buổi 2.

Công thức tôi tự rút ra: `số stage = ___`. Nó sai khi: __________________

---

### 3.3 · Tuning partition: các nút vặn *(A15, A16, A17, A14, A18)*

**Đọc file — `maxPartitionBytes`** (trên `olist_geolocation` 58MB):

| Giá trị | numPartitions | Số task stage 0 | Thời gian `count()` | Nhận xét |
|---|---|---|---|---|
| 128m *(mặc định)* | | | | |
| 16m | | | | |
| 4m | | | | |
| file `.gz` (128m) | | | | ⚠️ không splittable? |

Điểm đảo chiều (nhiều partition hơn nhưng CHẬM hơn) rơi ở: ___ → vì overhead mỗi task ≈ ___ms

**Shuffle — `spark.sql.shuffle.partitions`** (`groupBy(order_status)`, 8 nhóm):

| Giá trị | Số task sau shuffle | Số task xử lý **0 record** | Thời gian | 🎯 |
|---|---|---|---|---|
| 200 *(mặc định)* | | | | *lãng phí* |
| 8 | | | | |
| 1 | | | | |
| 200 + AQE on | | *(coalesced → ___)* | | ← AQE làm hộ bạn |

**Giảm partition — `repartition` vs `coalesce`** (200 → 8):

| | Có `Exchange`? | Phân bố (`glom()`) | Đều? | Thời gian | Số task stage TRƯỚC |
|---|---|---|---|---|---|
| `repartition(8)` | | | | | |
| `coalesce(8)` | | | | | |

**Skew** (`repartition("customer_state")`, sau join customers):

| Metric | Min | 25th | Median | 75th | Max | **Max/Median** |
|---|---|---|---|---|---|---|
| Task duration | | | | | | ___× |
| Records | | | | | | ___× |

> 🎯 **Max/Median > 3 = có skew.** Cả job phải chờ task nào? ___ (đừng sửa — salting là module 3)

---

### 3.4 · Kích thước & layout file *(A20, A27, A31, A35)*

**Format** (cùng `orders_clean`):

| Format | Dung lượng | Tỉ lệ so CSV | Thời gian ghi | Đọc full | Đọc 1 cột |
|---|---|---|---|---|---|
| CSV gốc | | **1×** | — | | |
| JSON | | ___× | | | |
| Parquet + snappy | | ___× | | | |
| ORC | | ___× | | | |

> 🎯 **Ngưỡng:** Parquet phải ≤ **0.35×** CSV. Không đạt → kiểm tra: có đang ghi ra hàng nghìn file nhỏ không (mỗi file có footer riêng)? Nén có bật không?

**Nén** (Parquet):

| Codec | Dung lượng | Thời gian ghi | Đọc full | Splittable? |
|---|---|---|---|---|
| none | | | | |
| snappy | | | | |
| gzip | | | | |
| zstd | | | | |

Tôi chọn: **___** vì: _______________________ *("vì mặc định" = trừ điểm)*

**Small files — gây án rồi phá án** ⚠️ *bảng before/after quan trọng nhất project*:

| Chiến lược ghi | Số file `part-*` | Tổng size | Thời gian ghi | Query A | `files read` (query B) |
|---|---|---|---|---|---|
| `partitionBy` KHÔNG repartition *(200 shuffle parts)* | | | | | |
| `.repartition("order_date")` trước ghi | | | | | |
| `.repartition(1)` | | | | | |
| `.coalesce(8)` | | | | | |

> 🎯 **Ngưỡng:** cột 1 phải giảm ít nhất **10×** giữa dòng 1 và dòng 2.
>
> Câu hỏi ăn điểm: **vì sao tổng dung lượng cũng tăng** khi nhiều file nhỏ? → _______________
>
> Sizing: orders_clean Parquet = ___ MB / ___ ngày = **___ KB mỗi partition-ngày**. Chuẩn nghề là 64–256 MB → tôi đang lệch **___ lần**. `partitionBy(order_date)` chỉ đúng thật khi dữ liệu lớn gấp **___ lần** Olist.

---

### 3.5 · Bytes read — bảng chấm điểm thật sự *(A30, A32, A36)*

*Lấy từ Spark UI → tab **SQL** → click query → node `Scan parquet` → 2 dòng `number of files read` / `size of files read`.*

*(Giây đồng hồ nói dối trên dữ liệu bé. Bytes thì không.)*

| Kịch bản | files read | size read | So với full scan | 🎯 Ngưỡng |
|---|---|---|---|---|
| **Full scan** — `select *` (baseline) | | | 100% | — |
| **Column pruning** — `select price` | | | ___% | ≤ 20% |
| **Partition pruning** — `where order_date='2018-07-02'` | | | ___% | ≤ 1% *(1 / ~600 partition)* |
| **Pruning bị PHÁ** — `where date_format(order_date,...) = '...'` | | | ___% | ≈ 100% ← chứng minh nó không phải phép màu |
| **Row-group skip** — `where price > 1500`, KHÔNG sort | | | ___% | |
| **Row-group skip** — cùng query, CÓ `sortWithinPartitions("price")` | | | ___% | ≤ 50% dòng trên |

> Đây là bảng mà rubric thưởng 25 điểm. Kết luận phải có dạng: *"query B chỉ đọc **1/600 file** và **___ KB / ___ MB** — ở dữ liệu 100× thì nó vẫn chỉ đọc 1/600, trong khi CSV phải đọc cả 1.7GB."* Đó là **ngoại suy có căn cứ**, khác hẳn "nhanh hơn nhiều".

---

### 3.6 · Chất lượng dữ liệu & idempotency *(Checkpoint 1, 2 — bắt buộc)*

**Data quality:**

| Bảng | Dòng đọc | Dòng hỏng (quarantine) | Dòng NULL date | Vào bảng chính | Khớp `wc -l`? |
|---|---|---|---|---|---|
| orders | | | | | |
| order_items | | | | | |
| customers | | | | | |

Chênh lệch với `wc -l` giải thích bằng: ☐ header ☐ dòng hỏng ☐ multiline ☐ khác: ______

**6 loại bẩn tự tiêm (A23) — Spark bắt được cái nào?**

| Loại bẩn | Vào `_corrupt_record`? | Hay lọt thành **dữ liệu sai im lặng**? |
|---|---|---|
| 1. Thiếu cột | | |
| 2. Thừa cột | | |
| 3. Sai kiểu (`"hôm qua"`) | | |
| 4. Dấu phẩy trong text, không ngoặc kép | | ⚠️ |
| 5. Ngoặc kép lệch | | |
| 6. Dòng trống / header lặp | | |

> Loại nguy hiểm nhất là số ___, vì ___________________. Đó là lý do cần data quality gate (A38) — `_corrupt_record` chỉ bắt lỗi **cấu trúc**, không bắt lỗi **ngữ nghĩa**.

**Read mode:**

| Mode | `count()` | Dòng hỏng đi đâu | Exception? | Dùng khi nào |
|---|---|---|---|---|
| PERMISSIVE | | | | |
| DROPMALFORMED | | | | |
| FAILFAST | | | | |

Tôi chọn ___ cho pipeline này vì: __________________

**Save mode — chạy 2 lần liên tiếp:**

| Mode | count sau lần 1 | count sau lần 2 | Exception? | Idempotent? |
|---|---|---|---|---|
| overwrite | | | | |
| append | | | | ⚠️ *(−15 điểm nếu dùng nhầm)* |
| errorifexists | | | | |
| ignore | | | | |

**`partitionOverwriteMode` — re-run đúng 1 ngày (A26):** ⚠️ *bài học đắt nhất*

| | Số thư mục partition TRƯỚC | SAU khi ghi đè 1 ngày |
|---|---|---|
| `static` *(mặc định!)* | ~600 | **___** ← chuẩn bị tinh thần |
| `dynamic` | ~600 | **___** |

Câu tôi sẽ không bao giờ quên: _______________________________________________

**Bằng chứng idempotent (A39) — chạy `ingest.py` 3 lần liên tiếp:**

| Lần chạy | `count()` toàn bảng | Số thư mục partition | `count()` riêng 2018-07-02 |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |

> 🎯 **3 dòng phải giống hệt nhau.** Khác một chữ số = pipeline chưa idempotent = mất 15 điểm.

**Data quality gate (A38) — trên Olist thật:**

| Check | Mức | Kết quả | Số dòng vi phạm |
|---|---|---|---|
| `order_id` không null | blocking | ☐ PASS ☐ FAIL | |
| `order_id` unique | blocking | ☐ PASS ☐ FAIL | |
| `order_status` ∈ 8 giá trị hợp lệ | blocking | ☐ PASS ☐ FAIL | |
| `price >= 0` | blocking | ☐ PASS ☐ FAIL | |
| `order_date` ∈ 2016..2018 | warning | ☐ PASS ☐ FAIL | |
| null rate của `delivered_date` < 5% | warning | ☐ PASS ☐ FAIL | |

*(Sẽ có cái FAIL — Olist bẩn thật. Cái nào FAIL và bạn xử lý thế nào là phần đáng giá.)*

---

### 3.7 · Bằng chứng từ plan *(A6, A36)*

Dán `explain(mode="formatted")` của **query B trên Parquet**, khoanh:

```
-- TODO: dán vào đây
-- ✅ PartitionFilters: [ ... order_date = 2018-07-02 ... ]   ← cái này phải CÓ
-- ✅ PushedFilters:    [ ... ]                                ← khác PartitionFilters thế nào?
-- ✅ ReadSchema:       chỉ đúng số cột cần                    ← column pruning
-- ✅ number of files read: 1 / ~600
```

Và plan của **query bị phá pruning** (A36) để đối chứng:

```
-- TODO: dán vào đây — PartitionFilters biến mất, filter rơi xuống thành Filter thường
```

> Bài học một câu: ________________________________________________________
> *(Gợi ý điều bạn nên tự rút ra: partition pruning KHÔNG tự động — nó chỉ chạy khi filter đánh thẳng vào cột partition bằng biểu thức đơn giản. Đây là lỗi khiến pipeline production chậm 100× mà không ai hiểu vì sao.)*

---

### 3.8 · Bài toán ×100 *(A40 — 10 điểm rubric "Tư duy scale")*

Dữ liệu: ~10 triệu đơn, ~___ GB CSV (sinh bằng `crossJoin` + làm nhiễu id/ngày).

| Vòng | Đã sửa gì | Thời gian pipeline | Spill (mem/disk) | Số file output | Còn gãy ở đâu |
|---|---|---|---|---|---|
| 0 | *(chạy nguyên xi code Olist)* | | | | |
| 1 | | | | | |
| 2 | | | | | |
| 3 | | | | | |

**Thứ gãy ĐẦU TIÊN là:** ______________ *(driver? shuffle spill? small files? một máy ghi?)*

**Ba câu chốt — viết thật lòng:**

- Vẫn đứng vững ở ×100: _________________________________________________
- Phải đổi: _____________________________________________________________
- **Tôi chưa biết / chưa đo được:** ______________________________________
  *(Chỉ ra được điểm mù của mình là dấu hiệu senior, không phải yếu kém. Mục này bỏ trống thì mất điểm — không ai biết hết cả.)*

---

## Phần 4 — Tự chấm trước khi nộp

| Hạng mục | Điểm | Tự chấm | Bằng chứng ở đâu |
|---|---|---|---|
| Đúng đắn — schema, quarantine, số khớp nguồn | 25 | ___ | §3.6 |
| Thiết kế ghi — partition, layout, idempotent | 25 | ___ | §3.3, §3.4, §3.6 |
| Benchmark & bằng chứng — số đo, UI, explain | 25 | ___ | §3.1, §3.5, §3.7 |
| Chất lượng code — cấu trúc, đặt tên, không magic | 15 | ___ | code |
| Tư duy scale — mục ×100 | 10 | ___ | §3.8 |
| *Bonus* Iceberg + Trino | +10 | ___ | |
| | **/100** | **___** | |

**Ba lỗi bị trừ thẳng tay — tự soi lại:**

- [ ] Không có `inferSchema` trong code nộp *(−10)*
- [ ] Chạy 2 lần không nhân đôi dữ liệu — **đã chứng minh** ở §3.6 *(−15)*
- [ ] Không còn câu "nhanh hơn" nào mà thiếu số đi kèm *(−10)* → **Ctrl+F chữ "nhanh" trong report.md, mỗi chỗ phải có một con số ngay cạnh**

**Thang:** ≥85 sẵn sàng module 2 · 70–84 xem lại hạng mục thấp nhất · <70 sửa và nộp lại *(bình thường — đây là vòng lặp học)*

---

## Phần 5 — Nhật ký (2 dòng mỗi buổi, đừng bỏ)

| Buổi | Ngày | Thứ tôi tưởng đúng mà hoá ra sai | Thứ tôi vẫn chưa hiểu |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |
| 5 | | | |
| 6 | | | |

> Cột thứ 3 là thứ bạn sẽ đọc lại sau 6 tháng và thấy mình đã đi được bao xa. Cột thứ 4 là danh sách câu hỏi mang đến mentor.
