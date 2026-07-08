# Lesson 32 — Table maintenance: compaction, snapshot expiration

> Module 5 · Lakehouse & Iceberg · Tuần 17 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao bảng Iceberg **phình** theo thời gian: mỗi commit = snapshot mới + file mới, streaming = hàng nghìn snapshot, MERGE = delete file chồng chất.
- Bộ tứ maintenance: **rewrite_data_files** (compaction), **expire_snapshots**, **remove_orphan_files**, **rewrite_manifests** — mỗi cái dọn tầng nào của cây metadata.
- Compaction 2 chiến lược: **bin-pack** (gộp cho đủ cỡ) vs **sort** (gộp + sắp xếp để stats chặt).
- Lịch chạy khuyến nghị production và cách tự động hóa bằng Airflow.
- Đo lường before/after: số file, số snapshot, thời gian query — maintenance không đo là maintenance mù.

Sau bài này bạn phải làm được:

- Nhìn `.files` / `.snapshots` / `.manifests` của một bảng và chẩn đoán: bảng này thiếu maintenance ở khâu nào.
- Viết đủ 4 câu `CALL` procedure với tham số đúng, giải thích từng option.
- Trả lời: "expire_snapshots có làm mất dữ liệu không?" — chính xác đến từng trường hợp.

Kiến thức dùng trong thực tế: mọi bảng Iceberg production đều cần maintenance định kỳ — không có ngoại lệ. Bảng streaming trong `../kafka-flink` của bạn mà không expire snapshots thì MinIO sẽ đầy trong vài tuần. Đây là kỹ năng "vận hành lakehouse" mà JD Senior DE nào cũng ghi.

---

## 2. Why

### Vấn đề: thiết kế "chỉ thêm, không sửa" có hóa đơn trả sau

Lesson 30–31 cho bạn ACID và time travel nhờ nguyên tắc: không sửa gì tại chỗ, mỗi commit chỉ thêm file mới + snapshot mới. Hóa đơn của nguyên tắc đó:

```
Bảng silver.orders nhận micro-batch mỗi 1 phút, mỗi batch ~2 MB:

Sau 1 ngày:   1,440 snapshot | 1,440+ data file bé xíu | 1,440 manifest
Sau 1 tháng: 43,200 snapshot | metadata.json chứa 43k entry, nặng hàng chục MB
              → MỖI query planning phải đọc metadata này
              → storage giữ MỌI file của MỌI snapshot (kể cả đã bị "xóa" logic)
              → scan 43k file 2MB chậm hơn hàng chục lần scan 200 file 512MB
```

Ba loại "mỡ thừa" tích tụ:

1. **Small files**: mỗi commit ghi file riêng; file 2 MB nghĩa là mở/đóng file, seek, đọc footer... chiếm tỉ trọng lớn hơn đọc dữ liệu. Với MOR (lesson 31) còn thêm delete file phải áp lên mỗi lần đọc.
2. **Snapshot cũ**: mỗi snapshot ghim toàn bộ file nó tham chiếu khỏi bị xóa. DELETE/MERGE "xóa" dữ liệu chỉ về mặt logic — byte vẫn nằm trên storage chờ snapshot cũ hết hạn.
3. **Manifest phân mảnh + orphan file**: commit li ti sinh manifest li ti (planning chậm); job chết giữa chừng để lại file đã ghi nhưng không snapshot nào nhận (orphan — chiếm chỗ vô danh).

### Nếu không maintenance thì sao?

Chuyện thật xảy ra ở mọi công ty dùng lakehouse: quý đầu êm đẹp; quý hai query chậm dần "không hiểu vì sao"; quý ba bill S3 tăng gấp ba dù dữ liệu hữu ích tăng 20%, và một ngày job planning OOM vì metadata.json quá to. Lúc đó dọn một bảng đã 200k snapshot đau đớn gấp trăm lần dọn định kỳ.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| Query nhanh trở lại (ít file, stats chặt, manifest gọn) | Compaction là job Spark thật — tốn compute, đọc/ghi lại dữ liệu |
| Giải phóng storage (expire + orphan) | Expire = **mất time travel** về trước ngưỡng retention |
| Metadata gọn, planning nhanh, commit nhanh | Maintenance cũng là writer → có thể conflict với job đang ghi (có cách né) |
| Vận hành dự đoán được | Phải xây lịch + monitoring — thêm việc |

> Bài học Senior: maintenance không phải "việc phụ khi rảnh" — nó là một phần của thiết kế bảng, quyết định từ ngày tạo bảng: retention bao lâu, compact tần suất nào, ai chạy, đo bằng gì.

---

## 3. Theory

### 3.1. Bản đồ: 4 việc maintenance dọn 4 tầng

```
CÂY METADATA (lesson 30)              VIỆC MAINTENANCE tương ứng
─────────────────────────             ─────────────────────────────────
metadata.json (list snapshots)  ◄──── expire_snapshots
                                      cắt snapshot cũ khỏi metadata,
                                      XÓA file chỉ thuộc snapshot đã cắt

manifest list / manifest files  ◄──── rewrite_manifests
                                      gộp manifest nhỏ → planning nhanh

data files (+ delete files)     ◄──── rewrite_data_files (COMPACTION)
                                      gộp file nhỏ → file ~target size,
                                      áp delete file vào data (MOR trả nợ)

file "vô chủ" ngoài mọi snapshot ◄─── remove_orphan_files
                                      quét storage, đối chiếu metadata,
                                      xóa file không ai tham chiếu
```

### 3.2. `rewrite_data_files` — compaction

Việc quan trọng nhất. Đọc nhóm file nhỏ, ghi lại thành file cỡ chuẩn (mặc định target 512 MB), commit snapshot mới operation `replace` — **dữ liệu logic không đổi một dòng nào**, chỉ cách xếp vào file thay đổi.

```
TRƯỚC:  [2MB][2MB][2MB] ... ×1440 file  +  [delete file]×200
          │  bin-pack: nhét các file nhỏ vào "thùng" 512MB, không quan tâm thứ tự
          │  sort:     như trên NHƯNG sắp xếp dòng theo cột chỉ định trước khi ghi
          ▼
SAU:    [512MB][512MB][512MB][487MB]   (delete file đã được áp vào data → biến mất)
```

- **bin-pack** (mặc định): rẻ nhất, chỉ gộp. Dùng khi mục tiêu là trị small files.
- **sort**: đắt hơn (phải shuffle) nhưng dòng cùng giá trị nằm cạnh nhau → min/max stats mỗi file **chặt** → file pruning tốt hơn hẳn (liên quan sort order — lesson 33).
- Compaction chạy **per-partition** và chỉ commit thay thế những file nó đọc → chạy song song với writer khác tương đối an toàn (conflict xử theo optimistic concurrency; có option `partial-progress.enabled` để commit dần từng phần, đỡ mất công khi conflict).

### 3.3. `expire_snapshots` — cắt lịch sử, giải phóng storage

```
snapshots:  S1 ── S2 ── S3 ── S4 ── S5(current)
                                     retention: giữ 7 ngày & tối thiểu 3 snapshot
expire(older_than = now-7d, retain_last = 3):
  S1, S2 già hơn 7 ngày và ngoài top-3 → CẮT khỏi metadata
  → file CHỈ được S1/S2 tham chiếu (đã bị thay ở S3+) → XÓA VẬT LÝ
  → file S1/S2 tham chiếu mà S3+ vẫn dùng → GIỮ (còn chủ)
```

Hai hệ quả phải khắc cốt: (1) đây là lệnh **duy nhất** thật sự giải phóng dung lượng của dữ liệu đã DELETE/thay thế; (2) **mất time travel** về các snapshot đã expire — retention là hợp đồng với người dùng bảng, đổi phải báo. Snapshot được branch/tag tham chiếu KHÔNG bị expire (mỗi ref có retention riêng).

### 3.4. `remove_orphan_files` — dọn file vô chủ (CẨN THẬN nhất)

Orphan sinh ra khi: job Spark chết sau khi ghi data file nhưng trước khi commit; commit thua conflict không retry; task speculative ghi trùng. Những file này **không nằm trong bất kỳ metadata nào** → expire_snapshots không đụng tới → chỉ `remove_orphan_files` (so danh sách file trên storage với danh sách file mọi snapshot biết) mới dọn được.

**Tại sao nguy hiểm**: một job ĐANG ghi cũng tạo file "chưa được commit" — trông y hệt orphan! Vì thế procedure mặc định chỉ xóa file **cũ hơn 3 ngày** (`older_than`). Đặt `older_than` ngắn hơn thời gian chạy của job dài nhất là tự tay bắn vào chân: xóa file của job đang chạy → commit của nó trỏ đến file ma → bảng hỏng.

### 3.5. `rewrite_manifests` — dọn tầng manifest

Commit nhỏ li ti sinh manifest li ti; manifest cũng có thể "lệch nhóm" so với partition. Planning đọc mọi manifest của snapshot hiện tại → nhiều manifest nhỏ = planning chậm. `rewrite_manifests` gộp lại theo partition, chỉ đụng metadata (không đọc data file) → rẻ, nhanh.

### 3.6. Lịch chạy khuyến nghị (điểm khởi đầu, chỉnh theo bảng)

| Việc | Bảng streaming/CDC | Bảng batch daily | Lý do |
|---|---|---|---|
| rewrite_data_files | Mỗi giờ (partition mới) | Hằng ngày, sau job ghi | Small files + delete files tích nhanh theo commit |
| expire_snapshots | Hằng ngày, giữ 5–7 ngày | Hằng tuần, giữ 7–30 ngày | Cân bằng time travel vs storage |
| rewrite_manifests | Hằng ngày | Hằng tuần | Rẻ, chạy sau compaction |
| remove_orphan_files | Hằng tuần, older_than ≥ 3 ngày | Hằng tháng | Hiếm khi khẩn, rủi ro cao nhất |

---

## 4. Internal

Compaction (`rewrite_data_files` bin-pack) chạy như thế nào bên trong:

```
① PLAN: đọc metadata snapshot hiện tại → liệt kê file theo partition
      lọc ứng viên theo option: file < min-file-size? nhóm nào đủ
      min-input-files? file có delete file gắn kèm?
        │
② GROUP: gom ứng viên thành các file group (mỗi group ≤ max-file-group-size,
      mặc định 100GB) — mỗi group là một đơn vị compact độc lập
        │
③ EXECUTE: với từng group, chạy Spark job:
      đọc các file nhỏ (+ áp delete file nếu MOR) → repartition/sort
      → ghi ra file mới cỡ target-file-size-bytes (512MB)
        │
④ COMMIT: một commit thay thế atomic —
      manifest mới: file mới ADDED, file cũ của group DELETED
      snapshot mới operation="replace"
      ⚠ VALIDATION: nếu trong lúc compact có writer khác ĐÃ THAY/XÓA
      đúng file mà group này đọc (VD MERGE đè lên) → commit group fail;
      với partial-progress.enabled=true, các group khác vẫn commit được
        │
⑤ Dữ liệu logic: SELECT trước và sau compaction trả kết quả GIỐNG HỆT
      (khác duy nhất: nhanh hơn) — file cũ chưa bị xóa vật lý,
      chúng vẫn thuộc snapshot cũ, chờ expire_snapshots
```

Còn `expire_snapshots` bên trong là bài toán **đối chiếu tập hợp**:

```
① Xác định tập snapshot bị cắt (theo older_than / retain_last / refs)
② Tập file được tham chiếu bởi snapshot SỐNG   = KEEP
③ Tập file chỉ được tham chiếu bởi snapshot CHẾT = DELETE vật lý
   (cả data file, manifest, manifest list của snapshot chết)
④ Ghi metadata.json mới không còn snapshot chết + xóa file ở ③
```

Vì bước ② phải duyệt manifest của mọi snapshot sống, expire trên bảng có hàng chục nghìn snapshot là job nặng — thêm một lý do đừng để snapshot chất đống rồi mới dọn.

---

## 5. API

Cả 4 đều là stored procedure gọi qua `CALL <catalog>.system.<procedure>` (cần `spark.sql.extensions` — quen thuộc rồi).

### `rewrite_data_files`

```sql
CALL lakehouse.system.rewrite_data_files(
  table => 'olist.orders_l32',
  strategy => 'binpack',                          -- hoặc 'sort'
  -- sort_order => 'ts ASC',                      -- bắt buộc khi strategy='sort' (trừ khi bảng có sort order sẵn)
  where => 'ts >= TIMESTAMP \'2018-01-01\'',      -- chỉ compact vùng nóng, đỡ tốn
  options => map(
    'target-file-size-bytes', '134217728',        -- 128MB (demo; production thường để mặc định 512MB)
    'min-input-files', '5',                       -- đủ 5 file nhỏ mới đáng gộp
    'partial-progress.enabled', 'true'            -- commit dần từng group
  ))
```
- **Trả về**: `rewritten_data_files_count`, `added_data_files_count`, `rewritten_bytes_count` — log lại để làm metric.
- **Pitfall**: không có `where` trên bảng lớn = compact cả bảng, job khổng lồ. Luôn khoanh vùng partition mới ghi.

### `expire_snapshots`

```sql
CALL lakehouse.system.expire_snapshots(
  table => 'olist.orders_l32',
  older_than => TIMESTAMP '2026-07-01 00:00:00',  -- thường: now() - retention
  retain_last => 5                                 -- lưới an toàn: luôn giữ ≥5 snapshot
)
```
- **Trả về**: số data file / manifest / manifest list đã xóa vật lý.
- **Pitfall**: `older_than => current_timestamp()` (quên trừ retention) = xóa gần hết lịch sử, mất time travel ngay lập tức. Double-check biểu thức thời gian trước khi chạy trên production.

### `remove_orphan_files`

```sql
CALL lakehouse.system.remove_orphan_files(
  table => 'olist.orders_l32',
  older_than => TIMESTAMP '2026-07-05 00:00:00',  -- mặc định now - 3 ngày; ĐỪNG rút ngắn
  dry_run => true                                  -- chạy thử, chỉ LIỆT KÊ không xóa
)
```
- **Pitfall kép**: (1) `older_than` ngắn hơn job dài nhất → xóa file của job đang ghi; (2) `location` trỏ nhầm ra thư mục cha chung → quét (và có thể xóa) file của bảng khác. Luôn `dry_run => true` trước ở lần đầu với một bảng.

### `rewrite_manifests`

```sql
CALL lakehouse.system.rewrite_manifests(table => 'olist.orders_l32')
```
- Rẻ, an toàn, chỉ đụng metadata. Chạy sau compaction hoặc khi `.manifests` cho thấy hàng trăm manifest nhỏ.

---

## 6. Demo nhỏ

```
Input:  bảng nhận 30 lần append nhỏ (giả lập streaming) → 30 snapshot, 30+ file
   ↓    rewrite_data_files (binpack) → còn ~1 file
   ↓    expire_snapshots → còn ~2 snapshot, file cũ bị xóa vật lý
Output: đếm file/snapshot before-after ngay trong console
```

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("demo32").master("local[2]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/tmp/demo_warehouse")
    .getOrCreate())

spark.sql("DROP TABLE IF EXISTS lakehouse.demo.ticks")
spark.sql("CREATE TABLE lakehouse.demo.ticks (id INT, v DOUBLE) USING iceberg")

for i in range(30):                                   # 30 commit bé = streaming thu nhỏ
    spark.range(i * 100, (i + 1) * 100) \
         .selectExpr("cast(id as int) id", "rand() v") \
         .writeTo("lakehouse.demo.ticks").append()

def stats(label):
    f = spark.sql("SELECT count(*) FROM lakehouse.demo.ticks.files").first()[0]
    s = spark.sql("SELECT count(*) FROM lakehouse.demo.ticks.snapshots").first()[0]
    print(f"{label}: {f} data files, {s} snapshots")

stats("TRUOC")                                        # ~30 files, 30 snapshots
spark.sql("""CALL lakehouse.system.rewrite_data_files(
             table => 'demo.ticks', options => map('min-input-files','2'))""").show()
stats("SAU COMPACTION")                               # ~1 file, 31 snapshots (compact cũng là commit!)
spark.sql("""CALL lakehouse.system.expire_snapshots(
             table => 'demo.ticks',
             older_than => current_timestamp(),       -- demo mới dám làm vậy!
             retain_last => 1)""").show()
stats("SAU EXPIRE")                                   # 1 file, 1 snapshot
spark.stop()
```

Ngạc nhiên cần ngấm: sau compaction số snapshot **tăng** (compaction cũng là một commit), và số file trong `.files` giảm nhưng file cũ **vẫn nằm trên đĩa** — chỉ sau expire chúng mới biến mất thật (kiểm chứng bằng `ls data/`).

---

## 7. Production Example

Bảng `silver.orders` trong kiến trúc `kafka-flink` của bạn: Spark Structured Streaming MERGE mỗi phút, Trino đọc phục vụ BI. Quy trình maintenance production-like:

```
Airflow DAG "iceberg_maintenance"  (những gì các công ty chạy thật)

hourly:
  compact_hot_partitions:
    CALL rewrite_data_files(table=>'silver.orders',
         where=>"ts >= <hôm nay>",              ← chỉ vùng đang ghi
         options=>map('partial-progress.enabled','true'))
daily 02:00 (giờ thấp điểm):
  compact_full  → rewrite_data_files strategy sort (vùng 7 ngày gần)
  rewrite_manifests
  expire_snapshots(older_than => now - 7 days, retain_last => 10)
weekly Sunday:
  remove_orphan_files(older_than => now - 3 days, dry_run trước lần đầu)
  → đẩy metric (files_rewritten, bytes_deleted) vào Prometheus/log
```

Vì sao xếp hình như vậy:

1. **Compact theo giờ chỉ vùng nóng**: partition hôm nay hứng micro-batch nên nát nhất; partition cũ đã compact rồi, đụng lại là đốt tiền compute.
2. **Sort compaction vào ban đêm**: sort cần shuffle nặng — chạy giờ thấp điểm, đổi lại Trino query ban ngày prune tốt hơn.
3. **Expire giữ 7 ngày**: đủ cho hầu hết nhu cầu "xem lại hôm qua/tuần trước" + đủ an toàn cho query dài; muốn giữ mốc lâu hơn thì tag (lesson 31), không kéo dài retention cả bảng.
4. **Orphan hằng tuần với older_than 3 ngày**: job dài nhất của công ty chạy 6 giờ — 3 ngày là biên an toàn hàng chục lần.
5. **Metric hóa mọi lần chạy**: `rewritten_data_files_count` tăng đột biến = có job upstream ghi file nhỏ bất thường → điều tra ngược. Maintenance tốt là hệ thống cảnh báo sớm miễn phí.

---

## 8. Hands-on Lab

**Mục tiêu**: tự tay làm bảng "nát" bằng Olist, đo độ nát, dọn từng bước, đo lại — có số liệu before/after.

### Bước 1 — `labs/lab32/make_mess.py`: tạo bảng nát

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("lab32-make-mess")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/workspace/warehouse")
    .getOrCreate())

items = (spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                        header=True, inferSchema=True)
         .select("order_id", "order_item_id", "product_id", "price"))

spark.sql("DROP TABLE IF EXISTS lakehouse.olist.items_l32")
spark.sql("""CREATE TABLE lakehouse.olist.items_l32
             (order_id STRING, order_item_id INT, product_id STRING, price DOUBLE)
             USING iceberg TBLPROPERTIES ('format-version'='2')""")

# Giả lập streaming: 60 commit nhỏ, mỗi commit repartition(3) → ~180 file bé
chunks = items.randomSplit([1.0] * 60, seed=42)
for i, c in enumerate(chunks):
    c.repartition(3).writeTo("lakehouse.olist.items_l32").append()
    if i % 10 == 0: print(f"commit {i}...")
spark.stop()
```

### Bước 2 — `labs/lab32/measure.py`: thước đo dùng lại nhiều lần

```python
import time
# ... (SparkSession config như trên)
t = "lakehouse.olist.items_l32"
files = spark.sql(f"SELECT count(*) c, sum(file_size_in_bytes)/1e6 mb FROM {t}.files").first()
snaps = spark.sql(f"SELECT count(*) c FROM {t}.snapshots").first()[0]
mans  = spark.sql(f"SELECT count(*) c FROM {t}.manifests").first()[0]
t0 = time.time()
spark.sql(f"SELECT product_id, sum(price) FROM {t} WHERE price > 100 GROUP BY product_id").count()
q = time.time() - t0
print(f"files={files['c']} ({files['mb']:.1f} MB) | snapshots={snaps} | manifests={mans} | query={q:.2f}s")
```

### Bước 3 — `labs/lab32/cleanup.py`: dọn theo đúng thứ tự

```python
# ... (SparkSession config như trên)
spark.sql("""CALL lakehouse.system.rewrite_data_files(
    table => 'olist.items_l32', strategy => 'binpack',
    options => map('min-input-files','2','target-file-size-bytes','134217728'))""").show()
spark.sql("CALL lakehouse.system.rewrite_manifests(table => 'olist.items_l32')").show()
spark.sql("""CALL lakehouse.system.expire_snapshots(
    table => 'olist.items_l32',
    older_than => current_timestamp(), retain_last => 2)""").show()
spark.sql("""CALL lakehouse.system.remove_orphan_files(
    table => 'olist.items_l32', dry_run => true)""").show()   # chỉ dám dry_run
```

### Bước 4 — chạy theo kịch bản đo lường

```bash
make run-local F=labs/lab32/make_mess.py
make run-local F=labs/lab32/measure.py     # ghi số TRƯỚC
make run-local F=labs/lab32/cleanup.py
make run-local F=labs/lab32/measure.py     # ghi số SAU
docker exec spark-mastery-spark-submit-1 \
  sh -c 'ls /workspace/warehouse/olist/items_l32/data | wc -l'   # file VẬT LÝ trên đĩa
```

Ghi vào `labs/lab32/NOTES.md` bảng before/after: số data file (logic vs vật lý), MB, số snapshot, số manifest, thời gian query. Trả lời: khâu nào giảm số file logic, khâu nào giảm file vật lý? Query nhanh lên bao nhiêu %?

---

## 9. Assignment

**Easy** — Từ lab: bảng đi từ ~180 file xuống bao nhiêu file sau compaction? Thời gian query giảm bao nhiêu? Giải thích 2 nguyên nhân query nhanh lên (gợi ý: chi phí mở file, và planning đọc bao nhiêu manifest).

**Medium** — Kịch bản retention: append thêm 5 commit vào bảng lab, lấy danh sách snapshot. Viết + chạy `expire_snapshots` sao cho: giữ mọi snapshot trong 10 phút gần nhất VÀ luôn giữ tối thiểu 3 snapshot. Chứng minh bằng `.snapshots` trước/sau, và chứng minh time travel về snapshot đã expire giờ báo lỗi gì (bắt exception, ghi lại message).

**Hard** — Viết Airflow DAG `iceberg_maintenance.py` (chỉ cần code DAG chạy được về mặt cấu trúc, không cần cluster Airflow thật): 3 task `compact_hot` (hourly, có `where` giới hạn vùng) → `rewrite_manifests` → `expire_snapshots` (daily, retention 7 ngày), dùng `SparkSubmitOperator` hoặc `BashOperator` gọi `spark-submit` với script tham số hóa (`--table`, `--retention-days` qua `argparse`). Yêu cầu: task compact phải đẩy output của procedure (số file rewritten) ra log; nêu 1 lý do vì sao KHÔNG gộp cả 3 vào một script duy nhất.

**Production Challenge** — Điều tra bảng Iceberg trong `../kafka-flink`: dùng `.snapshots`/`.files` trả lời — bảng có bao nhiêu snapshot đang tồn? Tuổi snapshot già nhất? Phân bố kích thước file (min/median/max của `file_size_in_bytes`)? Từ đó viết đề xuất maintenance 10 dòng: việc gì cần chạy ngay, lịch định kỳ đề xuất, retention đề xuất và rủi ro của nó (ai đang cần time travel bảng này?).

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Hiện tượng | Thủ phạm | Thuốc |
|---|---|---|
| Query scan chậm dù dữ liệu nhỏ | Ngàn file bé (chi phí mở file > đọc dữ liệu) | rewrite_data_files binpack |
| Query có filter vẫn đọc nhiều file | Stats min/max lỏng (dòng trộn lẫn) | rewrite_data_files **sort** (+ sort order — lesson 33) |
| Planning (trước khi job chạy) lâu | Manifest phân mảnh, metadata.json phình vì vạn snapshot | rewrite_manifests + expire_snapshots |
| Đọc bảng MOR chậm dần | Delete file tích tụ chưa được áp | compaction (áp delete vào data) |
| Bill storage tăng, dữ liệu không tăng | Snapshot cũ ghim file "đã xóa" + orphan | expire_snapshots + remove_orphan_files |
| Compaction hay fail giữa chừng | Conflict với writer, group quá to | `where` khoanh vùng + partial-progress + chạy lệch giờ writer |

Số để nhớ: target file 512 MB (mặc định, hợp lý cho scan); file < ~32 MB là "small file" đáng gộp; bảng có > vài nghìn snapshot là đèn đỏ expire.

---

## 11. Spark UI

Compaction là job Spark thật — mở UI (:4040) khi `cleanup.py` chạy:

**Tab Jobs / Stages**: `rewrite_data_files` hiện thành các job đọc file nhỏ và ghi file to; nhìn Input size của stage đọc — đúng bằng tổng dung lượng file bị gộp. Đây là "chi phí compaction" bạn phải trả, thấy tận mắt để hiểu vì sao production phải khoanh `where`.

**Đo hiệu quả pruning trước/sau sort compaction**: chạy cùng query filter (`price > 100`), so node `BatchScan` trong tab SQL — số file/split đọc trước và sau. Sort compaction thành công = số file scan giảm rõ.

**Phía metadata tables** (đôi mắt thứ hai): sau mỗi bước, `.snapshots` thêm dòng operation `replace` (compaction) — chú ý `summary` có `added-data-files` nhỏ và `deleted-data-files` lớn; `.manifests` giảm số dòng sau rewrite_manifests; `.files` là nơi lấy phân bố `file_size_in_bytes` để quyết định có cần compact — một câu `SELECT percentile(file_size_in_bytes, array(0.1,0.5,0.9)) FROM t.files` là bản chụp sức khỏe bảng.

---

## 12. Common Mistakes

1. **Không maintenance gì cả** cho đến khi query chậm/storage đầy. Sai lầm phổ biến nhất và đắt nhất — dọn 200k snapshot đau hơn trăm lần dọn mỗi ngày. Maintenance phải nằm trong định nghĩa "xong" của một bảng production.
2. **Tưởng DELETE/compaction giải phóng dung lượng.** Không — file cũ vẫn thuộc snapshot cũ. Chỉ `expire_snapshots` (và `remove_orphan_files`) mới xóa byte thật. Đây là câu hỏi bẫy interview kinh điển.
3. **`remove_orphan_files` với `older_than` quá ngắn** trong khi có job đang ghi → xóa file chưa-commit của job → commit xong bảng trỏ đến file ma. Giữ mặc định 3 ngày, luôn `dry_run` lần đầu.
4. **Expire quá tay** (`older_than => current_timestamp()` không `retain_last` tử tế) → mất time travel, và nếu có query/consumer đang đọc snapshot cũ thì nó fail giữa chừng. Retention phải dài hơn query/consumer dài nhất.
5. **Compact cả bảng mỗi lần** thay vì khoanh `where` vùng nóng → job compaction to hơn cả job ETL, tốn tiền vô ích và tăng nguy cơ conflict.
6. **Chạy compaction đúng giờ cao điểm của writer** → conflict, retry storm. Lệch pha với writer, bật `partial-progress.enabled`.
7. **Maintenance không đo lường** — chạy `CALL` xong không ghi lại rewritten/deleted counts, không đo query time → không biết lịch của mình thừa hay thiếu. Không đo = không maintenance, chỉ là cầu may.

---

## 13. Interview

**Junior:**

1. *Tại sao bảng Iceberg sinh ra nhiều file nhỏ?* — Mỗi commit ghi file mới (không sửa file cũ); streaming/CDC commit dày (mỗi phút) nên mỗi lần chỉ ít dữ liệu → file bé. Thêm nữa mỗi task ghi file riêng, và MOR còn sinh delete file kèm theo.
2. *Compaction là gì, có làm thay đổi dữ liệu không?* — `rewrite_data_files`: đọc nhóm file nhỏ, ghi lại thành file cỡ chuẩn (~512MB), commit snapshot thay thế. Dữ liệu logic không đổi một dòng — SELECT trước/sau giống hệt, chỉ nhanh hơn.
3. *expire_snapshots làm gì? Có mất dữ liệu không?* — Cắt snapshot cũ hơn ngưỡng khỏi metadata và xóa vật lý file chỉ thuộc snapshot đó. Dữ liệu *hiện tại* không mất; cái mất là **lịch sử**: time travel/rollback về trước ngưỡng không còn, và dữ liệu đã DELETE trước đó giờ mới biến mất thật khỏi đĩa.
4. *Orphan file là gì, từ đâu ra?* — File nằm trong thư mục bảng nhưng không snapshot nào tham chiếu: do job chết sau khi ghi file nhưng trước commit, hoặc commit thua conflict. Chiếm storage vô ích, chỉ `remove_orphan_files` dọn được.

**Mid:**

5. *bin-pack vs sort compaction — khác nhau và khi nào chọn cái nào?* — Bin-pack: chỉ gộp file nhỏ đủ cỡ target, không shuffle, rẻ — dùng trị small files thuần túy. Sort: gộp + sắp xếp dòng theo cột chỉ định, cần shuffle, đắt hơn — đổi lại min/max stats mỗi file chặt, pruning theo cột sort tốt hẳn. Chọn sort khi query pattern filter nhiều theo một/vài cột; bin-pack cho vòng hourly, sort cho vòng nightly là combo phổ biến.
6. *Vì sao remove_orphan_files nguy hiểm hơn expire_snapshots?* — Expire chỉ xóa file mà metadata *biết chắc* hết chủ. Orphan removal xóa file metadata *không biết* — mà file của job đang-ghi-chưa-commit cũng "không được biết" y như orphan. Nếu `older_than` ngắn hơn job dài nhất, nó xóa file sắp được commit → bảng trỏ file ma. Vì vậy: mặc định 3 ngày, dry_run trước, chạy thưa.
7. *Compaction có conflict với writer đang MERGE không? Giảm thiểu thế nào?* — Có thể: compaction commit kiểu replace, validate rằng file nó đọc chưa bị thay; nếu MERGE đã rewrite/xóa đúng file đó trước → commit compaction (hoặc group đó) fail. Giảm thiểu: khoanh `where` tránh vùng writer đang sửa, bật `partial-progress.enabled` để group khác vẫn commit, lịch compaction lệch pha writer, để cơ chế retry tự xử phần còn lại.
8. *Thiết kế lịch maintenance cho bảng CDC ghi mỗi phút, BI đọc cả ngày, yêu cầu time travel 7 ngày.* — Hourly: binpack compaction giới hạn partition ngày hiện tại (partial-progress). Nightly: sort compaction 3–7 ngày gần theo cột filter chính + rewrite_manifests + expire_snapshots(older_than=now-7d, retain_last≈10). Weekly: remove_orphan_files older_than 3 ngày. Metric hóa output mỗi lần chạy; tag các mốc cần giữ lâu hơn 7 ngày thay vì nới retention.

**Senior:**

9. *Storage bill tăng gấp 3 trong một quý, dữ liệu hữu ích tăng 20%. Điều tra thế nào trên lakehouse Iceberg?* — Theo tầng: (1) `.snapshots` từng bảng — bảng nào vạn snapshot chưa expire → file "đã xóa logic" vẫn ghim đầy đĩa; (2) `.refs` — branch/tag rác giữ snapshot sống vô hạn; (3) đối chiếu tổng size trong `.files` (dữ liệu snapshot hiện tại) với size thật trên bucket — chênh lệch lớn = snapshot cũ + orphan; (4) kiểm tra maintenance có thực chạy không (job fail âm thầm là chuyện thường). Xử theo thứ tự expire → drop ref rác → remove orphan (dry_run trước), rồi vá quy trình: alert khi snapshot count/tuổi vượt ngưỡng.
10. *Trino đang chạy query 4 tiếng trên snapshot S, giữa chừng expire_snapshots xóa file của S — chuyện gì xảy ra và phòng thế nào?* — Query fail FileNotFound giữa chừng: reader phân tán không giữ lock nào lên file, expire không biết có ai đang đọc. Phòng: retention tối thiểu phải > thời lượng query/consumer dài nhất (kèm biên an toàn); SLA hóa quy tắc đó giữa team platform và người dùng; job/consumer quan trọng chạy dài nên pin snapshot-id và bảng của nó phải có retention thỏa thuận; giám sát query duration trên các engine đọc chung catalog trước khi siết retention. Đây là bài toán *tổ chức* nhiều hơn kỹ thuật — Senior phải thấy được điều đó.

---

## 14. Summary

### Mindmap

```
                        TABLE MAINTENANCE
                              │
     ┌──────────────┬─────────┴───────┬──────────────────┐
     ▼              ▼                 ▼                  ▼
  VÌ SAO PHÌNH   COMPACTION       EXPIRE/ORPHAN      VẬN HÀNH
     │              │                 │                  │
  mỗi commit =   rewrite_data_    expire_snapshots:   lịch: compact
  snapshot mới   files            cắt lịch sử →       hourly(hot)/daily,
  + file mới     binpack: gộp     XÓA BYTE THẬT,      expire daily-weekly
  streaming =    sort: gộp+xếp    mất time travel     giữ 7 ngày,
  ngàn snapshot  → stats chặt     remove_orphan:      orphan weekly
  MOR = delete   compact ≠ xóa    file vô chủ,        Airflow tự động
  files tích tụ  byte trên đĩa!   older_than ≥ 3d     ĐO before/after
                 rewrite_manifests: dọn tầng manifest, rẻ
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 3 loại "mỡ thừa" của bảng Iceberg và mỗi loại do đâu.
- [ ] Nói đúng việc nào giảm số file logic, việc nào giải phóng byte vật lý.
- [ ] Viết được 4 câu CALL với option quan trọng (where, target-file-size, older_than, retain_last, dry_run).
- [ ] Giải thích được vì sao remove_orphan_files cần older_than ≥ 3 ngày.
- [ ] Đã đo before/after trong lab: file count, query time.
- [ ] Phác được lịch maintenance cho một bảng streaming khi được hỏi.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 33 — Partitioning & hidden partitioning.**

Compaction hôm nay chữa được small files, nhưng có một thứ nó không chữa nổi: bảng partition sai từ đầu. Partition là quyết định thiết kế lớn nhất của một bảng — sai thì mọi query trả giá mãi mãi. Lesson 33 đi từ Hive partition truyền thống (folder vật lý, người query quên filter đúng cột là full scan) đến vũ khí đặc sản của Iceberg: **hidden partitioning** — partition theo transform `days(ts)`, `bucket(16, id)`, người dùng cứ filter cột gốc mà pruning vẫn chạy; và **partition evolution** — đổi spec không cần rewrite dữ liệu cũ, điều Hive không bao giờ làm được. Kèm nghệ thuật chọn spec: cardinality, kích thước partition mục tiêu, và cạm bẫy over-partitioning mà 90% người mới dính.

> Gõ **"Continue"** khi sẵn sàng.
