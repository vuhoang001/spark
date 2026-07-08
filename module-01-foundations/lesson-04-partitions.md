# Lesson 4 — Partition: đơn vị song song hóa

> Module 1 · Foundations · Tuần 2 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Partition chính xác là gì, và vì sao nó là **nút vặn số 1** của performance Spark (trước cả memory, trước cả số executor).
- Ai quyết định số partition khi **đọc file**: `spark.sql.files.maxPartitionBytes` (mặc định 128MB) và công thức Spark thật sự dùng.
- `spark.sql.shuffle.partitions` mặc định **200** — vì sao con số một-cỡ-cho-tất-cả này là config bị phàn nàn nhiều nhất lịch sử Spark.
- Hai thái cực đều chết: **partition quá ít** (core thất nghiệp, OOM, spill) vs **quá nhiều** (overhead lên lịch, small files).
- `repartition` vs `coalesce` — giới thiệu đủ dùng (mổ xẻ chiến thuật ở lesson 16).
- **Skew** — partition béo partition gầy: nhận diện sớm (trị bệnh ở lesson 19).
- Quy tắc sizing của nghề: **~100–200MB/partition**, số partition là **bội số của tổng core**.

Sau bài này bạn phải làm được:

- Trả lời tức thì: "DataFrame này đang có bao nhiêu partition, tại sao lại là con số đó?"
- Tính số partition hợp lý cho một dataset X GB trên cluster Y core — ra con số cụ thể, có lập luận.
- Nhìn Spark UI và phán: job này đang khổ vì partition ít, nhiều, hay lệch.

Kiến thức dùng trong thực tế: **mỗi job production đều phải trả lời câu hỏi partition**. Đây là chỉnh sửa 1 dòng config mang lại cải thiện 5–10× phổ biến nhất trong nghề — và cũng là thứ đầu tiên senior nhìn khi review job của junior.

---

## 2. Why

### Mọi con đường đều dẫn về partition

Ba bài trước, partition xuất hiện ở mọi khúc cua quan trọng:

- Lesson 1: task xử lý **1 partition** — partition là đơn vị chia việc.
- Lesson 2: RDD/DataFrame *là* tập các partition rải trên executor; lineage tính lại theo **từng partition**.
- Lesson 3: số task của stage = số partition; wave = partition ÷ core; skew = partition béo.

Nói cách khác: **parallelism của Spark không phải do số executor quyết định — do số partition quyết định**. Executor là số ghế; partition là số khách. 100 ghế mà 3 khách thì 97 ghế trống; 100 ghế mà 100.000 khách thì xếp hàng dài — và nếu 1 khách nặng 2 tấn thì ghế nào ngồi cũng sập.

### Analogy chia pizza

Bữa tiệc có 16 người ăn (16 core). Cái pizza (dataset) cắt thế nào?

- **Cắt 3 miếng**: 3 người ăn è cổ (có khi nghẹn — OOM), 13 người nhịn đói ngồi nhìn. *Partition quá ít.*
- **Cắt 10.000 miếng vụn**: ai cũng ăn được, nhưng thời gian gắp–đưa–nhai từng vụn (overhead lên lịch task, mở file) nhiều hơn thời gian ăn. *Partition quá nhiều.*
- **Cắt 16 hoặc 32 hoặc 48 miếng đều nhau**: mỗi người 1–3 miếng vừa miệng, ăn xong cùng lúc. *Chuẩn: bội số của số người, miếng cỡ hợp lý.*
- **Cắt 16 miếng nhưng 1 miếng to bằng nửa cái pizza**: 15 người xong ngồi chờ 1 người. *Skew.*

### Nếu lờ đi bài này thì sao?

Job "chạy được" nhưng: đọc 500GB bằng 40 partition (mỗi task nhai 12GB → spill/OOM), hoặc groupBy trên 50MB dữ liệu chia 200 partition (mỗi task 250KB — thời gian khởi động task lớn hơn thời gian tính), hoặc ghi ra 200 file lắt nhắt mỗi file 1MB làm khổ mọi hệ đọc phía sau (small files — lesson 21). Cả ba bệnh trên đều là **mặc định của Spark nếu bạn không can thiệp** — Spark không biết dữ liệu và cluster của bạn, nó chỉ có con số một-cỡ-cho-tất-cả.

### Trade-off trung tâm

| Partition ÍT (to) | Partition NHIỀU (nhỏ) |
|---|---|
| ✔ Ít overhead lên lịch, ít file output | ✔ Parallelism cao, wave cân tải mượt |
| ✔ Aggregate hiệu quả hơn trên khối lớn | ✔ Task fail thì tính lại rẻ (mất ít) |
| ✘ Core thất nghiệp nếu partition < core | ✘ Overhead: mỗi task tốn ~vài chục ms lên lịch + serialize |
| ✘ Task to → memory căng → **spill/OOM** | ✘ Ghi ra small files, metadata phình |
| ✘ Task fail = tính lại cả khối to | ✘ Shuffle nhiều mảnh vụn → nhiều kết nối fetch |

> Bài học Senior: không có con số đúng tuyệt đối — chỉ có con số đúng **cho dữ liệu này trên cluster này**. Vì thế quy tắc nghề nghiệp là quy tắc *cỡ miếng* (~100–200MB) chứ không phải quy tắc *số miếng*.

---

## 3. Theory

### 3.1. Partition là gì — định nghĩa chuẩn

Partition = **một khúc dữ liệu liền khối, nằm trọn trên một executor, được xử lý bởi đúng một task**. DataFrame 1 tỷ dòng không phải "một bảng" — nó là, ví dụ, 800 khúc, mỗi khúc ~1.25 triệu dòng, rải trên các executor. Spark không có khái niệm "xử lý cả bảng" — chỉ có "xử lý từng partition, song song".

Ba thời điểm số partition được quyết định:

```
① LÚC ĐỌC (input)           → spark.sql.files.maxPartitionBytes + cỡ file
② LÚC SHUFFLE (wide transform)→ spark.sql.shuffle.partitions (mặc định 200)
③ LÚC BẠN RA TAY            → repartition(n) / coalesce(n)
```

### 3.2. ① Đọc file: công thức thật của Spark

Với file **splittable** (CSV/JSON không nén hoặc nén splittable, Parquet, ORC), Spark cắt theo byte:

```
maxPartitionBytes = spark.sql.files.maxPartitionBytes   (mặc định 128MB)
openCostInBytes   = spark.sql.files.openCostInBytes     (mặc định 4MB —
                     "phí mở file": file nhỏ vẫn bị tính tối thiểu 4MB
                     để Spark đừng gom trăm file nhỏ vào 1 partition... quá đà)

bytesPerCore  = (tổng bytes + số file × openCost) / defaultParallelism
maxSplitBytes = min( maxPartitionBytes, max(openCostInBytes, bytesPerCore) )

→ mỗi file bị cắt thành các khúc ≤ maxSplitBytes,
  các khúc/file nhỏ được ĐÓNG GÓI (bin-packing) vào partition ~maxSplitBytes
```

Ba hệ quả bạn phải nắm:

1. **File 1.3GB, cluster đủ core** → maxSplitBytes = 128MB → ⌈1300/128⌉ ≈ **11 partition**. Đây là nguồn gốc con số "128MB" cửa miệng.
2. **File nhỏ hơn (tổng bytes / core) × cluster ít core** → `bytesPerCore` kéo maxSplitBytes XUỐNG dưới 128MB — Spark cố cho mỗi core có việc. Ví dụ file 58MB trên `local[2]`: bytesPerCore ≈ (58+4)/2 = 31MB → cắt ~31MB/khúc → ~2 partition, KHÔNG phải 1. (Lab sẽ kiểm chứng đúng hiện tượng này.)
3. **1.000 file × 1MB** → phí mở file 1.000 × 4MB khiến Spark không nhồi cả nghìn file vào một partition — nhưng vẫn khổ: đây là small files problem (lesson 21).

Cảnh báo quan trọng: **file nén gzip KHÔNG splittable** — file CSV.gz 10GB = đúng 1 partition = 1 task nhai 10GB một mình cả tiếng. Gặp trong thực tế nhiều đến phát chán. (Parquet+snappy thì splittable theo row group — một lý do nữa để yêu Parquet, lesson 6.)

### 3.3. ② Shuffle: con số 200 định mệnh

Sau mọi wide transformation (`groupBy`, `join`, `distinct`, `orderBy`...), dữ liệu được chia lại thành đúng `spark.sql.shuffle.partitions` phần — **mặc định 200, bất kể dữ liệu 5MB hay 5TB**:

```
groupBy trên 50MB  → 200 partition × 0.25MB  → 200 task tí hon,
                     overhead lên lịch > thời gian tính. LÃNG PHÍ.
groupBy trên 2TB   → 200 partition × 10GB    → mỗi task ôm 10GB,
                     memory không chứa nổi → SPILL xuống disk, có khi OOM. THẢM HỌA.
```

Tại sao mặc định tệ vậy mà không sửa? Vì **không tồn tại con số đúng cho mọi người** — 200 là thoả hiệp lịch sử (đủ cho demo, không quá lố cho laptop). Người ta kỳ vọng bạn tự set:

```
shuffle.partitions ≈ (dung lượng dữ liệu VÀO shuffle) / (100–200MB)
                     rồi làm tròn lên bội số của tổng core
```

Ví dụ: shuffle 100GB, cluster 48 core → 100GB/150MB ≈ 683 → chọn **720** (= 48 × 15).

Từ Spark 3.x, **AQE** (`spark.sql.adaptive.coalescePartitions.enabled`, bật mặc định) tự động **gộp** các shuffle partition nhỏ sau khi thấy số liệu thật — chữa được chiều "quá nhiều", đỡ đau đáng kể. Nhưng nó không chữa chiều "quá ít" tốt bằng (chẻ partition to chỉ xảy ra trong skew-join cases) — hiểu gốc vẫn là nghĩa vụ (lesson 20 đào sâu).

### 3.4. ③ Ra tay: `repartition` vs `coalesce`

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Chiều | Tăng hoặc giảm | Chỉ **giảm** (tăng bị lờ đi im lặng) |
| Shuffle? | **CÓ** — full shuffle, dữ liệu chia lại đều tăm tắp (round-robin hoặc theo cột nếu `repartition(n, col)`) | **KHÔNG** — chỉ "dán" các partition sẵn có trên cùng executor lại với nhau |
| Kết quả | Partition đều, đẹp | Nhanh, rẻ, nhưng partition có thể **lệch** (dán 10 khúc vào 3 chỗ khó đều) |
| Giá | 1 lần shuffle toàn dữ liệu | Gần miễn phí, NHƯNG có bẫy: giảm quá sâu làm giảm parallelism của **cả stage phía trước** (coalesce không cắt stage — nó kéo cả chuỗi tính toán trước nó về n task!) |
| Dùng khi | Cần tăng parallelism; cần chia đều lại sau filter làm lệch; cần phân vùng theo cột trước khi ghi | Giảm số file output sau khi tính xong (ví dụ 200 → 16 trước `write`) |

Bẫy `coalesce(1)` kinh điển: "em muốn ghi ra 1 file CSV cho gọn" → cả pipeline phía trước co về **1 task** chạy tuần tự trên 1 core — mất sạch parallelism mà không hiện shuffle nào trên UI nên khó nghi ngờ. Muốn 1 file mà vẫn tính song song: `repartition(1)` (chịu 1 shuffle nhưng stage trước vẫn song song), hoặc tốt hơn — đừng đòi 1 file. *(Toàn bộ nghệ thuật chọn bên nào: lesson 16.)*

### 3.5. Skew — giới thiệu kẻ thù

Số partition đúng nhưng **cỡ partition lệch** thì vẫn chết: hash partitioning chia theo `hash(key) % n`, nên key nào chiếm 40% dữ liệu thì partition chứa nó chiếm 40% dữ liệu — 199 task xong trong 30s, 1 task cày 40 phút, cả stage sau đứng chờ (barrier — lesson 3). Nhận diện: Summary Metrics có **max >> median**. Nguyên nhân đầu bảng trong thực tế: **null / giá trị mặc định / khách hàng khổng lồ**. Thuốc chữa (salting, tách key nóng, AQE skew join) để dành lesson 19 — hôm nay chỉ cần bạn *nhìn thấy nó* trong lab.

### 3.6. Quy tắc sizing — bảng tra của nghề

```
┌──────────────────────────────────────────────────────────────────┐
│ QUY TẮC NGÓN TAY CÁI (thuộc lòng)                                │
│                                                                  │
│ 1. Cỡ partition mục tiêu: ~100–200MB (dữ liệu chưa nén trong RAM)│
│ 2. Số partition ≥ tổng core (không thì core thất nghiệp)         │
│ 3. Số partition = BỘI SỐ của tổng core (wave cuối đầy — ×2, ×3,  │
│    ×4 tổng core là vùng đẹp; task nhỏ thêm còn giúp cân tải)     │
│ 4. Task nên chạy ≳ 100ms–vài giây; đo thấy toàn task <50ms       │
│    → partition đang quá vụn                                      │
│ 5. Sau filter mạnh / trước write / trước join lớn:               │
│    NGHĨ lại số partition (dữ liệu đã đổi cỡ, số cũ hết đúng)     │
└──────────────────────────────────────────────────────────────────┘
```

Ví dụ tính trọn vẹn (dạng câu interview + assignment): dataset 50GB, cluster 4 executor × 4 core = **16 core**.
- Theo cỡ miếng: 50GB / 150MB ≈ 340.
- Làm tròn lên bội số 16: **352** (= 16 × 22) — mỗi core ăn 22 wave... hơi nhiều wave nhưng ổn; hoặc chọn 336 (16 × 21). Vùng 320–384 đều "đúng".
- Kiểm tra ngược: 50GB/352 ≈ 145MB/partition ✔; 352 ≥ 16 ✔; 352 % 16 = 0 ✔.

---

## 4. Internal

Theo chân một partition từ file đến file — nội bộ một job `read → filter → groupBy → write`:

```
① LẬP KẾ HOẠCH SCAN (driver):
   FileSourceScanExec liệt kê file, tính maxSplitBytes (công thức 3.2),
   cắt file thành FilePartition — mỗi cái ghi rõ:
   "đọc file X từ byte A đến byte B (+ file nhỏ Y, Z gói kèm)"
        │
② STAGE 0 — mỗi FilePartition → 1 task:
   task mở file, seek đến byte A, đọc đến B
   (CSV: nhích tới đầu dòng kế tiếp để không cắt đôi một dòng;
    Parquet: cắt theo row group nên khỏi lo)
   → dòng dữ liệu chảy qua chuỗi narrow đã pipeline (filter, cast...)
   → SHUFFLE WRITE: với từng dòng, tính hash(key) % 200
     → ghi vào bucket tương ứng trong shuffle file local
        │
③ RANH GIỚI STAGE (lesson 3): 200 partition "ảo" đang nằm rải
   trong các shuffle file của mọi task stage 0
        │
④ STAGE 1 — đúng 200 task (= spark.sql.shuffle.partitions):
   task i kéo (fetch) bucket i từ TẤT CẢ shuffle file qua network
   → aggregate trên partition của mình
   → write: MỖI TASK GHI 1 FILE → output có 200 file
     (giờ bạn hiểu vì sao thư mục output đầy part-00000...part-00199,
      và vì sao coalesce trước khi write là chuyện phải nghĩ)
```

Ba sự thật nội bộ đáng nhớ:

- **Partition là "ảo" cho đến khi được tính**: FilePartition chỉ là kế hoạch "đọc từ byte nào đến byte nào" — không có dữ liệu nào bị cắt sẵn trên disk. Lazy đến tận xương (lesson 2).
- **Số partition input KHÔNG di truyền qua shuffle**: đọc file ra 88 partition, nhưng qua `groupBy` là thành 200 (hoặc con số bạn set). Hai thế giới, hai config, đừng lẫn.
- **1 task ghi 1 file output** — số file = số partition cuối cùng. Toàn bộ small files problem (lesson 21) và nghi thức `coalesce`-trước-`write` sinh ra từ dòng này.

---

## 5. API

### `df.rdd.getNumPartitions()` — đồng hồ đo

```python
df = spark.read.csv("/workspace/data/olist/olist_geolocation_dataset.csv",
                    header=True, inferSchema=False)
print(df.rdd.getNumPartitions())        # đối chiếu với công thức 3.2!
```
- **Ý nghĩa**: số partition hiện tại của DataFrame = số task của stage tính nó.
- **Pitfall**: bản thân lời gọi rẻ (không chạy job), nhưng nhớ nó nói về *plan hiện tại* — sau một wide transformation nữa con số sẽ khác.

### `spark.sql.files.maxPartitionBytes` — nút vặn lúc đọc

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))  # 32MB
df = spark.read.parquet(...)   # giờ file bị cắt theo khúc ≤32MB → nhiều partition hơn
```
- **Khi dùng**: file ít + to + cluster nhiều core → giảm con số này để tăng parallelism lúc scan; hoặc tăng lên khi task scan quá vụn.
- **Pitfall**: set bằng số (bytes), không có chuỗi "128m" ở mọi phiên bản — an toàn nhất ghi số bytes; và phải set **trước** khi read (nó là config thời-lập-plan).

### `spark.sql.shuffle.partitions` — nút vặn sau shuffle

```python
spark.conf.set("spark.sql.shuffle.partitions", "64")   # thay đổi được giữa chừng
big.groupBy("k").count()      # stage sau shuffle giờ có 64 task
```
- **Ý nghĩa**: số partition đầu ra của MỌI shuffle trong session (trừ khi AQE gộp lại).
- **Pitfall**: (1) đây là config cấp session — hai query cỡ khác nhau trong cùng job dùng chung một con số, nên production hay set theo job, hoặc dựa vào AQE; (2) đừng nhầm với `spark.default.parallelism` — thằng đó là của thế giới RDD.

### `df.repartition(n)` / `df.repartition(n, *cols)` / `df.coalesce(n)`

```python
even   = skewed.repartition(48)              # full shuffle, chia đều — đắt mà đáng
bycol  = df.repartition(48, "order_date")    # gom theo cột (chuẩn bị ghi phân vùng)
fewer  = result.coalesce(8)                  # dán partition, không shuffle — rẻ
```
- **Pitfall `repartition`**: là wide transformation — thêm nguyên một stage shuffle. Đừng gọi "cho chắc".
- **Pitfall `coalesce`**: kéo parallelism của cả chuỗi tính phía trước xuống n (không cắt stage!); và `coalesce(500)` trên df 100 partition không tăng gì — lờ đi im lặng, không lỗi không cảnh báo.

### `spark.createDataFrame` + `spark.range(...)` — dụng cụ thí nghiệm

```python
df = spark.range(0, 10_000_000, numPartitions=16)   # DataFrame số, chỉ định partition
```
- **Khi dùng**: dựng thí nghiệm partition sạch, không phụ thuộc file. Lab hôm nay dùng nhiều.

---

## 6. Demo nhỏ

```
Input:  10 triệu số, lần lượt 1 / 2 / 8 / 200 partition
   ↓    cùng một phép tính nặng CPU
Output: thời gian mỗi cấu hình trên local[2] — thấy hình chữ U
```

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo04").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")   # đo mộc, không cho AQE gộp

def heavy(n_part: int) -> float:
    df = spark.range(0, 10_000_000, numPartitions=n_part)
    t0 = time.time()
    df.withColumn("h", F.sha2(F.col("id").cast("string"), 256)) \
      .filter(F.col("h").startswith("00")).count()
    return time.time() - t0

for n in [1, 2, 8, 200]:
    print(f"{n:>4} partition → {heavy(n):6.2f}s")
# Kết quả điển hình trên local[2]:
#    1 partition →  ~2× chậm nhất  (1 core làm, 1 core xem)
#    2 partition →  chuẩn ngọt     (khớp số core)
#    8 partition →  ~ngang 2       (wave cân tải, overhead chưa đáng kể)
#  200 partition →  chậm lại rõ    (10tr dòng/200 = 50k dòng/task — task vụn,
#                                   overhead lên lịch chiếm sóng)
spark.stop()
```

Hình chữ U này là toàn bộ lesson 4 nén trong 4 con số: quá ít — mất parallelism; quá nhiều — mất vào overhead; đúng cỡ và là bội số của core — điểm ngọt.

---

## 7. Production Example

Ca tối ưu 1-dòng-config nổi tiếng nhất nghề (bạn sẽ tự tay làm ở Project 1, tuần 7):

**Bối cảnh**: job silver→gold của pipeline Olist-style: join 2 bảng lớn rồi aggregate, chạy trên cluster 16 core. Dữ liệu vào shuffle ~24GB. Đêm nào cũng 55 phút.

**Khám bệnh theo checklist partition**:

```
Spark UI → stage sau shuffle:
  • 200 task (mặc định!) → 24GB/200 = ~120MB/partition — cỡ miếng ổn đấy chứ?
  • NHƯNG: Summary Metrics → Shuffle Spill (disk): 18GB  ← !!!
    120MB dữ-liệu-nén-trên-wire nở thành ~500MB trong RAM khi giải nén
    + hash table aggregate → tràn memory dành cho execution → SPILL
  • Và: 200 % 16 = 8 → wave cuối 8 task, 8 core ngồi chơi mỗi wave cuối
```

**Fix**: `spark.sql.shuffle.partitions = 480` (= 16 × 30; ~50MB nén/partition — nở ra vẫn vừa memory). Kết quả: spill về 0, job 55 phút → **14 phút**. Chi phí sửa: một dòng conf trong spark-submit.

**Bài học đóng khung**: (1) "cỡ partition" phải tính theo dữ liệu *sau giải nén trong RAM*, không phải số byte trên wire — nên vùng 100–200MB là điểm xuất phát, còn spill metrics mới là trọng tài; (2) đừng đợi ai cấp thêm máy — 16 core cũ chạy nhanh gấp 4 chỉ nhờ chia việc lại; (3) từ Spark 3.x, bật AQE giúp tự gộp partition thừa, nhưng set trần shuffle.partitions hợp lý vẫn là việc của người hiểu bài.

---

## 8. Hands-on Lab

**Mục tiêu**: kiểm chứng công thức đọc file, đo hình chữ U trên dữ liệu thật, thấy spill/skew sơ khai, và so `repartition` vs `coalesce` bằng đồng hồ.

Môi trường: cluster Docker của repo, Olist tại `/workspace/data/olist/` (file to nhất: `olist_geolocation_dataset.csv` ~58MB). Thư mục `labs/lab04/` đã có bài cũ của bạn — **tạo file mới, không đụng file cũ**.

### Bước 1 — viết file MỚI `labs/lab04/lesson04_partitions.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab04-partitions").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")
GEO = "/workspace/data/olist/olist_geolocation_dataset.csv"   # ~58MB

# ---- PHẦN A: ai quyết định số partition lúc đọc? ----
sc.setJobDescription("A: partition khi doc file")
for mpb in [128, 32, 8]:   # MB
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(mpb * 1024 * 1024))
    geo = spark.read.csv(GEO, header=True, inferSchema=False)
    print(f"maxPartitionBytes={mpb:>3}MB → {geo.rdd.getNumPartitions()} partition")
# TRƯỚC KHI CHẠY: dùng công thức 3.2 (local[2] → defaultParallelism=2,
# file 58MB) dự đoán con số cho từng dòng. Lệch thì tìm hiểu vì sao.

# ---- PHẦN B: hình chữ U — 1 → 2 → 8 → 32 → 200 partition ----
sc.setJobDescription("B: hinh chu U")
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
geo = spark.read.csv(GEO, header=True, inferSchema=False).cache()
geo.count()                                   # nạp cache — loại I/O khỏi phép đo
for n in [1, 2, 8, 32, 200]:
    t0 = time.time()
    (geo.repartition(n)
        .withColumn("h", F.sha2(F.col("geolocation_city"), 256))
        .filter(F.col("h") > "8").count())
    print(f"{n:>4} partition → {time.time()-t0:6.2f}s")

# ---- PHẦN C: shuffle.partitions — 200 mặc định vs con số có não ----
sc.setJobDescription("C: shuffle partitions 200 vs 4")
for sp in [200, 4]:
    spark.conf.set("spark.sql.shuffle.partitions", str(sp))
    t0 = time.time()
    geo.groupBy("geolocation_state").count().collect()
    print(f"shuffle.partitions={sp:>3} → {time.time()-t0:5.2f}s")
# Dữ liệu bé tí → 200 task vụn thua 4 task gọn. Ghi nhớ chiều ngược lại
# cũng đúng với dữ liệu to (mục 7 — spill).

# ---- PHẦN D: repartition(1) vs coalesce(1) khi ghi ----
sc.setJobDescription("D: repartition vs coalesce khi write")
heavy = geo.withColumn("h", F.sha2(F.concat_ws("|", "geolocation_zip_code_prefix",
                                               "geolocation_city"), 256))
t0 = time.time()
heavy.repartition(1).write.mode("overwrite").csv("/tmp/lab04_repart1")
t_rep = time.time() - t0
t0 = time.time()
heavy.coalesce(1).write.mode("overwrite").csv("/tmp/lab04_coal1")
t_coal = time.time() - t0
print(f"repartition(1): {t_rep:.2f}s | coalesce(1): {t_coal:.2f}s")
# Dự đoán trước: cái nào nhanh hơn TRÊN LOCAL[2]? Tại sao trên cluster
# 32 core câu trả lời có thể ĐẢO NGƯỢC? (gợi ý: coalesce(1) kéo cả
# phép tính sha2 về 1 core; repartition(1) cho stage tính chạy song song
# rồi mới gom — trả giá 1 shuffle)

# ---- PHẦN E: nếm thử skew ----
sc.setJobDescription("E: skew so khai")
spark.conf.set("spark.sql.shuffle.partitions", "8")
skewed = geo.withColumn("k", F.when(F.rand(seed=7) < 0.7, F.lit("HOT"))
                              .otherwise(F.col("geolocation_state")))
skewed.groupBy("k").agg(F.count("*").alias("c"),
                        F.avg("geolocation_lat").alias("a")).collect()
print("Mo UI: stage cuoi cua E — nhin Summary Metrics, max vs median!")

input(">>> http://localhost:4040 — soi tung phan A-E roi Enter de thoat...")
spark.stop()
```

### Bước 2 — chạy

```bash
make run-local F=labs/lab04/lesson04_partitions.py
# chạy thêm trên cluster để so (worker 1 core — dự đoán trước khác biệt!):
make run F=labs/lab04/lesson04_partitions.py
```

### Bước 3 — quan sát trên UI (`http://localhost:4040`)

1. **Phần A**: đối chiếu số partition in ra với dự đoán từ công thức. Chú ý trường hợp 128MB: ra ~2 chứ không phải 1 — chính là `bytesPerCore` ra tay (mục 3.2, hệ quả 2).
2. **Phần B**: tab Stages — với n=200, nhìn cột Duration của stage: tổng thời gian task cộng lại so với n=8 thế nào? Event Timeline của n=200: khe hở giữa các task (scheduler delay) dày đặc.
3. **Phần D**: tìm job của `repartition(1)` — thấy 2 stage (tính song song + gom); job `coalesce(1)` — 1 stage duy nhất 1 task (cả sha2 lẫn ghi trên 1 core).
4. **Phần E**: stage sau shuffle — bảng Summary Metrics: partition chứa key `HOT` ôm ~70% dữ liệu. Ghi con số max/median vào notes — đây là bộ mặt của skew, nhớ mặt để lesson 19 xử nó.
5. Ghi mọi số đo + giải thích vào `labs/lab04/NOTES-lesson04.md` (file mới).

---

## 9. Assignment

**Easy** — Tính số partition tối ưu cho dataset **50GB** trên cluster **4 executor × 5 core, 8GB/executor**: (a) trình bày phép tính theo 3 tiêu chí (cỡ miếng 100–200MB, ≥ tổng core, bội số tổng core), chốt một con số; (b) với con số đó, mỗi core chạy mấy wave? (c) nếu dataset là **500MB** thì sao — có nên dùng đủ 20 core không, hay bao nhiêu partition là đủ? Lý luận bằng chi phí overhead.

**Medium** — `repartition` vs `coalesce`: cho 3 tình huống, chọn công cụ + giải thích + viết code minh hoạ chạy được trên Olist:
1. Sau `filter` bỏ 95% dữ liệu, còn 10 partition lệch lạc, phía sau còn join nặng.
2. Kết quả aggregate còn 200 partition bé xíu, chỉ việc ghi ra storage thành ~4 file.
3. Cần ghi output phân vùng theo `order_status` (gợi ý: `repartition(n, col)` — vì sao bản có cột lại hợp?).
Bonus: giải thích vì sao `coalesce(1)` và `repartition(1)` cho cùng output nhưng một cái có thể chậm hơn nhiều lần.

**Hard** — Skew: (a) từ phần E của lab, đo và báo cáo: task chậm nhất chậm hơn median bao nhiêu lần, tổng thời gian stage bị task đó quyết định ra sao; (b) trả lời chuỗi câu hỏi: khi partition lệch, tăng `shuffle.partitions` từ 8 lên 800 có cứu được không? Tăng executor có cứu được không? Vì sao? (gợi ý: mọi dòng cùng key vẫn về đúng một partition); (c) đề xuất 2 ý tưởng thô để chia nhỏ key `HOT` (chưa cần code chuẩn — lesson 19 sẽ làm salting tử tế).

**Production Challenge** — Viết `labs/lab04/partition_report.py`: một hàm `report(df, label)` in ra "sức khoẻ partition" của DataFrame bất kỳ — số partition, số dòng min/max/avg mỗi partition (dùng `df.rdd.glom().map(len).collect()` với dữ liệu vừa phải, hoặc `df.groupBy(F.spark_partition_id()).count()`), tỉ lệ lệch max/avg. Chạy nó trên: (1) geolocation vừa đọc, (2) sau `repartition(8)`, (3) sau `filter` giữ 1 state duy nhất rồi `coalesce(2)`, (4) DataFrame skew của phần E sau shuffle. Nộp output + 5 dòng kết luận: bảng nào "khoẻ", bảng nào "ốm", vì sao. (Hàm này giữ lại — Module 3 bạn sẽ dùng nó như ống nghe bác sĩ.)

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Triệu chứng trên UI | Chẩn đoán partition | Thuốc |
|---|---|---|
| Số task của stage < tổng core | Partition quá ít | Giảm `maxPartitionBytes` (lúc scan) / tăng `shuffle.partitions` / `repartition` |
| Hàng nghìn task, mỗi task <50ms | Partition quá vụn | Tăng `maxPartitionBytes`, giảm `shuffle.partitions`, bật AQE coalesce |
| Shuffle Spill (memory/disk) > 0 | Partition sau shuffle quá to so với memory | Tăng `shuffle.partitions` (chia nhỏ miếng) — thường rẻ hơn tăng memory |
| max duration >> median trong stage | Skew — partition lệch | Lesson 19 (salting, AQE skew join); trước mắt: tìm key thủ phạm |
| Wave cuối lơ thơ | Số partition không chia hết tổng core | Chọn bội số tổng core |
| Output đầy file 1–2MB | Partition cuối quá nhiều → small files | `coalesce`/`repartition` trước `write` (lesson 21 trọn vẹn) |
| Job đọc CSV.gz to đùng chạy 1 task | gzip không splittable | Đổi codec/format (Parquet+snappy), hoặc giải nén trước |

Câu tự vấn mới, dán lên màn hình: *"tại điểm này của pipeline, dữ liệu của tôi đang là bao nhiêu partition, mỗi partition cỡ bao nhiêu MB — và con số đó do ai quyết định?"*

---

## 11. Spark UI

Bài này không mở tab mới — bài này dạy **đọc lại mọi tab bằng con mắt partition**:

- **Tab Stages, cột Tasks**: số task = số partition tại stage đó. Từ nay nhìn con số này phải bật ngay phản xạ: "nó đến từ đâu — maxPartitionBytes, shuffle.partitions, hay ai đó repartition?"
- **Trong stage → Summary Metrics**: bộ tứ min/median/max của Duration + Shuffle Read chính là "phim X-quang cỡ partition". Đều nhau = chia đẹp; max lệch trời = skew; tất cả đều bé tí = vụn.
- **Trong stage → Event Timeline**: khe trống giữa các thanh task = scheduler delay — timeline "nhiều khe hơn thịt" là bằng chứng trực quan của partition vụn.
- **Cột Shuffle Spill (Memory/Disk)** trong bảng task/summary: khác 0 nghĩa là miếng to hơn miệng — tăng shuffle.partitions là nghi phạm giải pháp đầu tiên.
- **Tab Storage** (khi cache): thấy số partition được cache và cỡ từng cái — thêm một chỗ kiểm tra chia đều.

Bài tập con mắt: từ giờ mở bất kỳ stage nào, hãy tự đọc to: *"stage này N task, mỗi task ~XMB, chạy W wave trên C core, max/median = R"* — một câu đó chứa 80% chẩn đoán performance.

---

## 12. Common Mistakes

1. **Không bao giờ đụng `spark.sql.shuffle.partitions`** — chạy mặc định 200 cho cả job 5MB lẫn job 5TB. Config bị bỏ quên nhiều nhất và cũng dễ ăn tiền nhất.
2. **`coalesce(1)`/`repartition(1)` để "ra 1 file cho đẹp"** trên dữ liệu lớn — với coalesce: cả pipeline co về 1 core; với repartition: 1 task ghi toàn bộ + 1 shuffle. "1 file cho đẹp" là yêu cầu của Excel, không phải của data lake.
3. **Tưởng thêm executor = thêm nhanh** trong khi partition chỉ có 8 — 8 task không thể xài 40 core. Parallelism trần = số partition, không phải số máy.
4. **Nhầm hai thế giới config**: chỉnh `maxPartitionBytes` mà mong stage sau groupBy đổi số task (sai — đó là việc của `shuffle.partitions`), hay ngược lại. Nhớ sơ đồ 3 thời điểm (mục 3.1).
5. **`coalesce(n)` để TĂNG partition** — bị lờ đi im lặng, không error, không warning. Muốn tăng: `repartition`.
6. **Đo cỡ partition bằng cỡ file nén trên disk** — 128MB parquet+snappy có thể nở thành 500MB–1GB trong RAM. Spill metrics mới là sự thật, con số trên disk chỉ để ước lượng ban đầu.
7. **Quên rằng filter làm partition "rỗng ruột"**: đọc 88 partition đẹp đẽ, filter còn 2% dữ liệu — vẫn 88 partition nhưng toàn vỏ; mọi stage sau gánh 88 task gần rỗng. Sau filter mạnh, cân nhắc repartition/để AQE gộp.
8. **Gzip cả file CSV khổng lồ rồi thắc mắc "Spark chạy 1 task"** — codec không splittable là trần cứng của parallelism lúc đọc, không config nào cứu.

---

## 13. Interview

**Junior:**

1. *Partition là gì, quan hệ với task và parallelism?* — Khúc dữ liệu liền khối nằm trên một executor, được xử lý bởi đúng 1 task. Số task của stage = số partition; số task chạy đồng thời = tổng core. Vậy parallelism thực = min(số partition, tổng core) — partition là trần của song song hóa.
2. *Spark quyết định số partition khi đọc file thế nào?* — Cắt file theo `spark.sql.files.maxPartitionBytes` (mặc định 128MB), có điều chỉnh: file nhỏ bị tính thêm phí mở file (openCostInBytes, 4MB) và nếu tổng dữ liệu chia số core nhỏ hơn 128MB thì cắt nhỏ hơn để đủ việc cho core (bytesPerCore). File không splittable (gzip) = 1 partition/file bất kể to nhỏ.
3. *`spark.sql.shuffle.partitions` là gì, mặc định bao nhiêu, sao phải quan tâm?* — Số partition đầu ra của mọi shuffle (groupBy/join/...), mặc định 200 bất kể cỡ dữ liệu. Dữ liệu bé → 200 task vụn tốn overhead; dữ liệu lớn → mỗi partition quá to gây spill/OOM. Gần như job production nào cũng phải chỉnh nó (hoặc dựa AQE).
4. *repartition khác coalesce chỗ nào?* — repartition: shuffle đầy đủ, tăng/giảm đều được, chia lại đều. coalesce: không shuffle, chỉ giảm bằng cách dán partition cùng chỗ, rẻ nhưng có thể lệch và kéo giảm parallelism của cả chuỗi tính phía trước (không cắt stage).

**Mid:**

5. *Dataset 50GB, cluster 16 core — chọn bao nhiêu partition, lập luận?* — Cỡ miếng 100–200MB → 50GB/150MB ≈ 340; nâng lên bội số 16 → ~352 (hoặc 320–384). Kiểm tra ngược: ≥16 core ✔, mỗi partition ~145MB ✔, wave cuối đầy ✔. Nói thêm điểm cộng: đó là số cho shuffle stage; lúc scan để maxPartitionBytes lo, và AQE có thể gộp bớt nếu thừa.
6. *Partition quá nhiều thì hại gì? Quá ít thì hại gì?* — Quá nhiều: overhead lên lịch/serialize mỗi task (~chục ms) chiếm tỉ trọng lớn khi task <100ms; shuffle vụn nhiều lần fetch; output small files. Quá ít: core thất nghiệp (parallelism trần thấp); task to gây spill/OOM; một task fail phải tính lại khối lớn; straggler ảnh hưởng nặng.
7. *Vì sao nên chọn số partition là bội số tổng core?* — Task chạy theo wave = ⌈partition/core⌉; nếu không chia hết, wave cuối chỉ có vài task — phần lớn core rảnh nhưng stage vẫn phải chờ (barrier). Bội số của core làm wave cuối đầy, tận dụng trọn tài nguyên.
8. *`coalesce(1)` để ghi 1 file — phân tích cái giá?* — coalesce không cắt stage nên toàn bộ chuỗi tính trước nó co về 1 task/1 core — mất sạch parallelism một cách "tàng hình" (không có shuffle mới trên UI). Thay thế: repartition(1) nếu bắt buộc 1 file (stage tính vẫn song song, trả giá 1 shuffle); tốt nhất là chất vấn yêu cầu 1 file — downstream đọc thư mục nhiều file bình thường.

**Senior:**

9. *Stage sau shuffle bị spill disk nặng. Tăng executor memory hay tăng shuffle.partitions — chọn gì, vì sao?* — Ưu tiên tăng shuffle.partitions: chia nhỏ miếng cho vừa memory hiện có — miễn phí, không cần xin tài nguyên, hiệu quả tuyến tính với mức chia. Tăng memory chỉ khi partition đã hợp lý mà cấu trúc dữ liệu/aggregation vốn ngốn RAM (nhiều key, object nặng), hoặc khi tăng partition làm task quá vụn. Kể thêm: kiểm tra skew trước — nếu spill dồn vào 1 task thì cả hai cách đều trật, phải xử lý key. Và AQE coalesce cứu chiều thừa chứ không cứu chiều thiếu.
10. *Thiết kế số partition cho pipeline nhiều giai đoạn: đọc 1TB Parquet → filter còn 5% → join với bảng 200GB → aggregate còn 1GB → write. Nói cách anh/chị nghĩ.* — Trả lời theo giai đoạn, vì "một con số cho cả pipeline" là sai đề: (a) scan 1TB: maxPartitionBytes mặc định → ~8000 task scan, ổn; (b) sau filter còn ~50GB nhưng vẫn 8000 partition rỗng ruột → để AQE gộp hoặc repartition ~350 (cluster giả định 48 core → 336); (c) join 50GB×200GB: shuffle.partitions theo bên lớn ~200GB/150MB ≈ 1400 → 1392 (48×29), canh spill metrics; (d) sau aggregate còn 1GB → trước write coalesce/repartition về ~8–16 file 64–128MB, cân nhắc repartition theo cột partition của bảng đích. Chốt: partition là đại lượng *per-stage*, senior tính lại nó mỗi khi cỡ dữ liệu đổi bậc.

---

## 14. Summary

### Mindmap

```
                            SPARK LESSON 4
                                 │
      ┌────────────────┬─────────┴─────────┬────────────────────┐
      ▼                ▼                   ▼                    ▼
  3 THỜI ĐIỂM      HAI THÁI CỰC        RA TAY               SIZING
      │                │                   │                    │
 ① đọc file:      ÍT: core rảnh,      repartition:         ~100–200MB/partition
   maxPartition-      spill, OOM        full shuffle,       ≥ tổng core
   Bytes (128MB)   NHIỀU: overhead,     chia đều, ↑↓        BỘI SỐ tổng core
   + bytesPerCore     small files     coalesce: dán         (wave cuối đầy)
   gzip = 1 task!  LỆCH (skew):        lại, rẻ, chỉ ↓,      spill metrics =
 ② shuffle:           max >> median     kéo cả stage         trọng tài cuối
   shuffle.parti-     (lesson 19)       trước co theo!      AQE gộp hộ chiều
   tions = 200 (!)                     (lesson 16)           thừa (lesson 20)
 ③ repartition/coalesce
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 3 thời điểm số partition được quyết định và config tương ứng.
- [ ] Giải thích công thức đọc file: vì sao file 58MB trên local[2] ra 2 partition chứ không phải 1.
- [ ] Nói được vì sao mặc định 200 của shuffle.partitions tệ ở CẢ hai chiều (bé và to).
- [ ] Phân biệt repartition/coalesce + kể được bẫy coalesce(1) không cần nhìn tài liệu.
- [ ] Tính số partition cho 50GB/16 core ra con số cụ thể trong 30 giây.
- [ ] Đã chạy lab: thấy hình chữ U, thấy spill/skew, đo được repartition vs coalesce.
- [ ] Nhìn stage bất kỳ đọc được câu thần chú: "N task, ~XMB/task, W wave, max/median = R".
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 5 — Đọc/ghi dữ liệu: Data Sources API.**

Bộ tứ nền tảng đã khép: kiến trúc → lazy/lineage → job/stage/task → partition. Giờ là lúc làm việc tử tế với thứ mà mọi pipeline bắt đầu và kết thúc: **dữ liệu trên disk**. Lesson 5 trả lời loạt câu hỏi cơm áo gạo tiền: CSV/JSON/Parquet/JDBC — ai hơn ai ở đâu và vì sao Parquet là mặc định của cả ngành? `mode("overwrite")` nguy hiểm cỡ nào? Đọc PostgreSQL qua JDBC làm sao để 10 executor cùng kéo thay vì 1 connection è cổ (partitioned read — và bạn sẽ thấy khái niệm partition hôm nay quay lại ngay lập tức)? Cùng ba chữ P thần thánh: **predicate pushdown, partition pruning, column pruning** — cách Spark "đọc ít mà được nhiều".

Đây cũng là bài mở màn cho Mini Project 1 (tuần 3): CSV Olist → Parquet phân vùng → Iceberg → Trino. Dữ liệu thật, pipeline thật.

> Gõ **"Continue"** khi sẵn sàng.
