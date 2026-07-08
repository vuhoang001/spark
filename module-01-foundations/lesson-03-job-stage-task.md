# Lesson 3 — Job / Stage / Task: mô hình thực thi chi tiết

> Module 1 · Foundations · Tuần 2 · Thời lượng: 4–5 giờ (lý thuyết 2h, lab 2–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **DAG Scheduler** cắt job thành stage như thế nào — thuật toán "cắt tại shuffle boundary" mổ xẻ từng bước.
- Hai loại stage: **ShuffleMapStage** (nấu nguyên liệu cho stage sau) vs **ResultStage** (dọn món ra bàn).
- **Task lifecycle**: một task sống từ lúc được serialize ở driver đến lúc báo kết quả — và chết/hồi sinh ra sao.
- **Wave**: khi số task > tổng số core, task xếp hàng chạy theo đợt — và vì sao wave cuối "mồ côi" là tiền vứt qua cửa sổ.
- Kỹ năng đặc sản: **nhìn code đoán số job/stage trước khi chạy**, rồi kiểm chứng bằng DAG Visualization.
- **Skipped stage**: vì sao có stage màu xám "được chạy free" — shuffle reuse, dạng cache mà bạn không hề bật.

Sau bài này bạn phải làm được:

- Cho một đoạn code 5–10 dòng, nói ngay: mấy job, mấy stage, stage nào phụ thuộc stage nào — sai không quá ±1.
- Mở DAG Visualization của bất kỳ job nào và "đọc" được câu chuyện: dữ liệu đi từ đâu, bị cắt ở đâu, vì sao.
- Trả lời: "job của tôi có 200 task mà cluster có 8 core — chuyện gì xảy ra?"

Kiến thức dùng trong thực tế: đây là **ngôn ngữ chẩn đoán** của Spark. Mọi cuộc điều tra "job chậm" đều bắt đầu bằng: job nào chậm → stage nào của job đó → task nào của stage đó. Không nói được ngôn ngữ này thì Spark UI chỉ là đống chữ xanh đỏ.

---

## 2. Why

### Câu chuyện có thật (bạn sẽ gặp trong 6 tháng tới)

Pipeline chạy đêm bỗng lâu gấp 3. Bạn mở Spark UI, thấy 7 job, 23 stage, 4.816 task. Sếp hỏi: "sao chậm?". Junior nhìn màn hình như nhìn ma trận. Senior nhìn 10 giây: "Job 5 chiếm 80% thời gian → job 5 có stage 12 chạy 40 phút trong khi các stage khác vài giây → stage 12 có 200 task nhưng 1 task chạy 39 phút còn 199 task chạy 30 giây → **skew** ở key X". Toàn bộ chẩn đoán đó đứng trên đúng một thứ: hiểu mô hình Job → Stage → Task.

### Analogy nhà máy may

Đơn hàng 10.000 áo (job). Quy trình: cắt vải → nhuộm → may → đóng gói. Nhưng máy nhuộm nằm ở **xưởng khác** — phải chất vải lên xe tải chở qua (shuffle). Vậy công việc tự nhiên tách thành các **công đoạn** (stage): mọi việc làm được tại xưởng A gộp làm một mạch (cắt + phân loại — pipeline!), rồi chở hàng, rồi mọi việc tại xưởng B làm một mạch. Trong mỗi công đoạn, 10.000 áo chia cho các tổ thợ — mỗi bó áo giao một thợ là một **task**. Xưởng có 8 thợ mà 200 bó? Làm 25 **đợt** (wave).

Ranh giới công đoạn KHÔNG phải do ai thích mà đặt — nó nằm đúng chỗ **phải chở hàng qua xưởng khác**. Spark y hệt: ranh giới stage nằm đúng chỗ dữ liệu phải bay qua network.

### Nếu không hiểu mô hình này thì sao?

- Bạn tune mù: tăng executor mà job vẫn chậm (vì bottleneck là 1 task skew — thêm 100 thợ cũng vô ích khi 1 bó áo to bằng 199 bó kia cộng lại).
- Bạn đọc tài liệu/blog về Spark như đọc tiếng nước ngoài — mọi bài tuning đều nói bằng ngôn ngữ stage/task.
- Bạn không trả lời nổi câu interview mid-level phổ biến nhất: "walk me through what happens when you run a Spark query".

### Trade-off của thiết kế stage-based

| Được | Mất |
|---|---|
| Pipeline mọi narrow transformation trong 1 stage — không materialize trung gian | Stage sau phải **chờ stage trước xong 100%** (barrier) — 1 task rùa kéo cả đoàn tàu |
| Shuffle file ghi ra disk → stage sau fail chỉ chạy lại stage sau, shuffle reuse cho job sau | Shuffle chạm disk + network — đắt (đó là lý do ta đếm shuffle như đếm tiền) |
| Task độc lập, retry từng task được, dễ scale | Quá nhiều task nhỏ = overhead lên lịch; quá ít = lãng phí core (lesson 4 giải bài này) |

> Bài học Senior: **thời gian job ≈ tổng thời gian các stage nằm trên đường găng, và mỗi stage chậm bằng task chậm nhất của nó**. Tối ưu Spark, suy cho cùng, là làm cho các task trong một stage đều nhau và ít phải chờ nhau.

---

## 3. Theory

### 3.1. Ôn nhanh chuỗi phân rã (lesson 1) — giờ ta zoom vào từng tầng

```
Application ──▶ Job ──▶ Stage ──▶ Task
 1 SparkSession  1 action  cắt tại    1 partition
                           shuffle    trên 1 core
```

### 3.2. DAG Scheduler cắt stage như thế nào

Khi action được gọi, physical plan cuối cùng là một DAG các RDD/operator. DAG Scheduler đi **ngược từ đích về nguồn**, gặp phụ thuộc kiểu shuffle (wide dependency) ở đâu thì **chém** ở đó:

```
Code:  orders.filter(...).join(items, "order_id").groupBy("seller").agg(sum)
       .write.parquet(...)                        (AQE tắt, không broadcast)

Đi ngược từ write:
                                   ┌── cắt! (shuffle của groupBy)
                                   │           ┌── cắt! (shuffle của join)
                                   ▼           ▼
  [Scan orders → Filter] ──shuffle──┐
                                    ├──▶ [Join] ──shuffle──▶ [Agg → Write]
  [Scan items]           ──shuffle──┘
        ▲                              ▲                        ▲
     STAGE 0                        STAGE 2                  STAGE 3
     STAGE 1 (items)             (ShuffleMapStage)         (ResultStage)
  (2 ShuffleMapStage
   độc lập, chạy song song)

→ 1 job, 4 stage. Stage 0 và 1 không đợi nhau; Stage 2 đợi cả 0 và 1; Stage 3 đợi 2.
```

Quy tắc bỏ túi (thuộc lòng):

- **Mỗi action = 1 job** (cộng vài job ẩn: `inferSchema`, sampling của `orderBy`...).
- **Số stage của job = 1 + số shuffle** trên đường tính (mỗi nguồn dữ liệu vào một shuffle tính riêng một nhánh).
- Trong một stage, mọi narrow transformation (`filter`, `select`, `withColumn`...) được **pipeline**: mỗi dòng dữ liệu chảy xuyên qua cả chuỗi trong một lần chạm — không có "bảng sau filter" nào tồn tại.

### 3.3. ShuffleMapStage vs ResultStage

| | ShuffleMapStage | ResultStage |
|---|---|---|
| Vai trò | Mọi stage "ở giữa" | Stage **cuối cùng** của job |
| Output | **Shuffle file ghi xuống local disk** của executor, chia sẵn theo partition đích cho stage sau đọc | Kết quả của action: dòng gửi về driver (`show`/`collect`), hoặc file ghi ra storage (`write`) |
| Số lượng trong 1 job | 0..n | đúng 1 |
| Sau khi xong | Shuffle file **còn nằm lại trên disk** (chưa xoá) → mở đường cho *shuffle reuse* (3.6) | Job kết thúc |

Chi tiết đắt giá: shuffle map task không "gửi" dữ liệu cho stage sau. Nó **ghi ra disk địa phương rồi ngồi im**; task của stage sau chủ động **kéo** (fetch) đúng phần của mình từ mọi executor. Push đâu — pull đấy. Kiến trúc pull này là lý do stage sau phải chờ stage trước xong 100% (mọi mảnh phải sẵn sàng mới kéo đủ bộ), học sâu ở lesson 15.

### 3.4. Task lifecycle — đời một con task

```
DRIVER                                        EXECUTOR
──────                                        ────────
① TaskScheduler chọn task cho core rảnh,
   ưu tiên data locality (task xử lý
   partition nào thì gửi đến nơi partition
   đó đang nằm)
② Serialize task = (mã byte của chuỗi hàm
   + thông tin partition cần xử lý) ──────▶  ③ Deserialize, nhận 1 core, chạy trên 1 thread
                                             ④ Lấy INPUT theo 1 trong 3 nguồn:
                                                • đọc file (scan) — task của stage nguồn
                                                • kéo shuffle blocks từ các executor khác
                                                • đọc cache (nếu có)
                                             ⑤ Chạy chuỗi phép đã pipeline trên partition
                                             ⑥ OUTPUT: ghi shuffle file (ShuffleMap task)
                                                hoặc trả kết quả/ghi storage (Result task)
⑧ Nhận status update. Task fail?      ◀────  ⑦ Báo driver: success/fail + metrics
   → retry trên executor khác,               (bytes read, GC time, shuffle write...)
   tối đa spark.task.maxFailures = 4 lần
   → fail lần 4: CẢ JOB CHẾT
```

Thêm hai nhân vật phụ hay gặp trên UI:
- **Straggler** (task rùa): cùng stage, 199 task xong trong 30s còn 1 task chạy 39 phút. Nguyên nhân phổ biến nhất: partition đó to bất thường — **skew** (lesson 19).
- **Speculative execution** (`spark.speculation=true`): thấy task rùa, driver cho chạy *bản sao* trên executor khác — ai xong trước lấy kết quả người đó, bản kia bị kill. Chữa rùa-do-máy-yếu, không chữa được rùa-do-skew (bản sao xử lý cùng partition to thì cũng rùa y nhau).

### 3.5. Wave — khi task nhiều hơn core

Tổng slot chạy đồng thời = tổng core của mọi executor. Task nhiều hơn thì xếp hàng:

```
Stage có 8 task, cluster có tổng 2 core (như local[2] của ta):

core 1: [T0───][T2───][T4───][T6───]
core 2: [T1───][T3───][T5───][T7───]
         wave1  wave2  wave3  wave4      → số wave = ⌈8 / 2⌉ = 4

Stage có 9 task, 2 core:
core 1: [T0───][T2───][T4───][T6───][T8───]
core 2: [T1───][T3───][T5───][T7───](rảnh!)   ← wave 5 chỉ 1 task,
                                                  1 core ngồi chơi
```

- Nhiều wave **không sai** — đó là cách bình thường để 8 core cân 2.000 task. Task nhỏ xếp hàng còn giúp cân tải (thợ nhanh lấy thêm bó áo, thợ chậm làm ít bó).
- Cái dở là **wave cuối lệch**: 201 task trên 100 core = 2 wave + 1 task mồ côi — 100 core chờ 1 task. Vì thế có quy tắc *số partition nên là bội số của tổng core* (lesson 4 đào sâu).

### 3.6. Skipped stage — shuffle reuse

Shuffle file của ShuffleMapStage **không bị xoá ngay** sau job. Nếu job sau cần lại đúng đoạn tính toán đó, DAG Scheduler thấy "output stage này còn nguyên trên disk" → **bỏ qua, không chạy lại** — trên UI stage hiện màu xám, chữ *skipped*:

```python
big = orders.join(items, "order_id")     # join → shuffle
big.count()                              # Job 1: stage scan×2 + stage join  (chạy đủ)
big.groupBy("seller_id").count().show()  # Job 2: các stage trước join → SKIPPED
                                         #        chỉ chạy phần groupBy trở đi
```

Đây là dạng "cache ngầm" bạn được tặng không — nhưng **đừng dựa vào nó thay cache**: shuffle file mất khi executor chết/bị thu hồi, và chỉ reuse được khi plan trùng khớp chính xác. Nó là tối ưu cơ hội, không phải hợp đồng.

### 3.7. Luyện đoán: 4 query mẫu (AQE tắt, không broadcast, không inferSchema)

| # | Code | Job | Stage | Giải thích |
|---|---|---|---|---|
| 1 | `read.parquet → filter → select → write` | 1 | **1** | Toàn narrow, không shuffle — scan-đến-write một mạch |
| 2 | `read → groupBy(a).agg → show` | 1 | **2** | 1 shuffle (groupBy) |
| 3 | `A.join(B, k) → groupBy(x).agg → show` (x ≠ k) | 1 | **4** | scan A, scan B (2 ShuffleMapStage) → join (shuffle vì groupBy khác key) → agg. 2 shuffle của join + 1 của groupBy, nhưng scan A/B song song |
| 4 | `read → groupBy(a).agg → orderBy(b) → show` | 2 | **3** (+1 skipped ở job 2) | `orderBy` cần **job sampling** riêng để tìm ranh giới range partition → job phụ xuất hiện. Shuffle: groupBy + sort |

Đừng học thuộc đáp án — học **cách đếm**: tìm wide transformation, mỗi cái một vết chém, cộng 1.

> Bài học Senior: từ Spark 3.2, **AQE (Adaptive Query Execution) bật mặc định** — nó chạy từng đoạn plan như các job nhỏ rồi tối ưu tiếp dựa trên số liệu thực (lesson 20). Hệ quả: trên UI bạn thấy *nhiều job hơn* số action, số stage có thể bị gộp/đổi. Khi **học đếm stage**, tắt nó đi cho sạch sân: `spark.conf.set("spark.sql.adaptive.enabled", "false")`. Khi chạy production, để nguyên — nó là bạn.

---

## 4. Internal

Hành trình đầy đủ từ action đến kết quả — ghép DAG Scheduler và Task Scheduler vào bức tranh lesson 1:

```
① df.write.parquet(...)  ← action trong code driver
        │
② Catalyst cho ra physical plan (cây operator)
        │
③ DAG SCHEDULER:
   • đi ngược plan từ đích, gặp ShuffleDependency thì cắt → danh sách stage
   • xây đồ thị phụ thuộc stage (stage 2 cần stage 0,1...)
   • stage nào MỌI cha đã xong (hoặc không có cha) → submit
   • với mỗi stage: tạo 1 task cho MỖI partition cần tính
     (trừ partition nào output còn trên disk → skipped)
        │
④ TASK SCHEDULER:
   • nhét task vào hàng đợi, ghép task ↔ core rảnh
   • ưu tiên locality: PROCESS_LOCAL (data trong cùng executor)
     > NODE_LOCAL > RACK_LOCAL > ANY — chờ một chút
     (spark.locality.wait, mặc định 3s) để được chạy gần data
     trước khi chấp nhận chạy xa
        │
⑤ EXECUTOR chạy task (lifecycle mục 3.4), báo kết quả từng task
        │
⑥ DAG SCHEDULER nghe ngóng:
   • mọi task của stage xong → đánh dấu stage xong → submit stage con
   • task fail vì FETCH shuffle file hỏng (executor giữ file đã chết)?
     → không chỉ retry task: chạy lại CẢ phần ShuffleMapStage
       đã mất output (đây là lúc bạn thấy stage "resubmitted" trên UI)
        │
⑦ ResultStage xong → job xong → driver code của bạn chạy tiếp dòng sau
```

Hai chi tiết nội bộ đáng tiền:

- **Barrier giữa các stage**: stage con không khởi động khi cha chưa xong *toàn bộ* task — vì task con cần kéo shuffle block từ **mọi** task cha (mỗi map task giữ một mảnh của mọi partition đích). Một task cha rùa = cả stage con đứng chờ. Đây là lý do skew độc: nó không làm chậm 1 task, nó làm chậm cả dây chuyền phía sau.
- **Task là đơn vị serialize**: chuỗi hàm của bạn (closure) phải serialize được để gửi đến executor. Lỗi huyền thoại `Task not serializable` / `PicklingError` sinh ra ở đây — bạn vô tình tham chiếu một object không đóng gói được (connection, file handle, `self` của class to đùng) trong lambda.

---

## 5. API

Bài này API nhẹ — công cụ chính là con mắt. Nhưng có 4 thứ phải nắm:

### `spark.conf.set("spark.sql.adaptive.enabled", "false")`

```python
spark.conf.set("spark.sql.adaptive.enabled", "false")   # chỉ để HỌC đếm stage
```
- **Ý nghĩa**: tắt AQE để job/stage trên UI khớp 1-1 với lý thuyết đếm tay.
- **Pitfall**: quên bật lại (hoặc hardcode tắt trong job production) → vứt đi trợ thủ tối ưu miễn phí của Spark 3.x. Chỉ tắt trong lab học.

### `df.rdd.getNumPartitions()`

```python
print(df.rdd.getNumPartitions())    # stage xử lý df này sẽ có bấy nhiêu task
```
- **Ý nghĩa**: soi số partition hiện tại = số task của stage tính nó. Vũ khí kiểm chứng số 1 của lesson này và lesson 4.
- **Pitfall**: gọi trên DataFrame *sau* wide transformation thì con số là partition **sau shuffle** (mặc định 200 — gặp lại ở lesson 4), đừng nhầm với partition lúc đọc file.

### `df.repartition(n)` — dùng làm "máy tạo task" trong lab

```python
df8 = df.repartition(8)     # wide! tự nó gây 1 shuffle — dùng có chủ đích
```
- **Ý nghĩa**: chia lại dữ liệu thành n partition → stage sau có n task. Hôm nay ta dùng nó để dựng thí nghiệm wave; bản chất repartition/coalesce học kỹ ở lesson 4 và 16.
- **Pitfall**: chính nó là một shuffle — đừng rắc bừa để "cho nhanh".

### `spark.sparkContext.setJobDescription("...")`

```python
sc.setJobDescription("BUOC 3: tinh doanh thu theo seller")
revenue.show()
```
- **Ý nghĩa**: đặt tên hiển thị cho job trên Spark UI — thay dòng `showString at ...` vô hồn bằng mô tả người đọc được.
- **Khi dùng**: pipeline production nhiều action/nhiều bước. Team on-call sẽ cảm ơn bạn.
- **Pitfall**: đặt xong quên đổi — mọi job sau đó mang cùng tên, gây hiểu nhầm còn tệ hơn không đặt. Đặt lại trước mỗi khối, hoặc set về `None` khi xong.

---

## 6. Demo nhỏ

```
Input:  1 triệu số, chia 8 partition (local[2] → 2 core)
   ↓    biến đổi narrow (không cắt stage) + 1 groupBy (cắt!)
Output: 1 job, 2 stage; stage đầu 8 task chạy 4 wave trên 2 core
```

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo03").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")   # học đếm stage cho sạch

df = spark.range(0, 1_000_000).repartition(8)           # 8 partition
slow = (df.withColumn("bucket", F.col("id") % 10)                    # narrow — cùng stage
          .withColumn("h", F.sha2(F.col("id").cast("string"), 512))  # narrow, nặng CPU
          .groupBy("bucket").count())                                # wide — CẮT stage

t0 = time.time()
slow.show()
print(f"Job mất {time.time()-t0:.1f}s")
input(">>> Mở http://localhost:4040 → tab Stages → Event Timeline. Enter để thoát...")
spark.stop()
```

Dự đoán trước khi mở UI: job của `show` có... 2 stage (repartition cũng là shuffle → thực ra 3! `range → repartition` cắt một lần, `groupBy` cắt lần nữa). Stage 8-task chạy trên 2 core → **Event Timeline hiện 4 tầng task xếp gạch** — đó chính là 4 wave bằng xương bằng thịt. Đếm được wave trên timeline là pass demo này.

---

## 7. Production Example

Ca trực có thật (mô phỏng từ pipeline Olist-style, dạng bạn sẽ vận hành ở Module 6):

**Hiện tượng**: job gold hằng đêm `fact_sales` bình thường 12 phút, đêm nay 71 phút, chưa xong. Airflow sắp bắn SLA alert.

**Điều tra bằng ngôn ngữ Job/Stage/Task** (đúng trình tự senior):

```
Bước 1 — Tab Jobs: 6 job. Job 4 (write fact_sales) chạy 63 phút,
         5 job kia cộng lại 8 phút.               → nghi phạm: Job 4
Bước 2 — Vào Job 4, DAG Visualization: 4 stage.
         Stage 11 (sau shuffle của join orders×items): 58 phút.
         3 stage kia: <2 phút.                    → nghi phạm: Stage 11
Bước 3 — Vào Stage 11, Summary Metrics:
         Duration  min 4s | median 6s | max 55 MIN
         Shuffle Read min 12MB | median 14MB | max 9.8GB
                                                  → 1 task ôm 9.8GB: SKEW
Bước 4 — Sort task theo Shuffle Read, lấy task to nhất, soi key:
         một seller "ảo" (seller_id rỗng do bug upstream) chiếm 40% items.
```

**Fix đêm đó**: filter dòng seller_id rỗng ở silver (dữ liệu rác), job về 11 phút. **Fix căn cơ**: data quality check chặn từ bronze (lesson 14), kỹ thuật trị skew thật sự học lesson 19.

Bài học: không đọc được Job→Stage→Task thì bước 1 đã tắc, và người ta sẽ "fix" bằng cách tăng gấp đôi cluster — tốn gấp đôi tiền để job vẫn chậm y nguyên, vì 1 task skew không xài được 100 core.

---

## 8. Hands-on Lab

**Mục tiêu**: dự đoán số job/stage của 3 query trên Olist, kiểm chứng bằng DAG Visualization, nhìn thấy wave và skipped stage tận mắt.

Môi trường: cluster Docker của repo, dataset tại `/workspace/data/olist/`. Thư mục `labs/lab03/` đã có bài cũ — **tạo file mới, không sửa file cũ**.

### Bước 1 — bật cluster, viết file MỚI `labs/lab03/lesson03_stages.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               TimestampType, DoubleType, IntegerType)

spark = SparkSession.builder.appName("lab03-stages").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")   # để đếm stage sạch

# Schema tường minh → KHÔNG có job ẩn của inferSchema (bài học lesson 1)
orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])
items_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType()),
])
orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, schema=orders_schema)
items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, schema=items_schema)

# ---- QUERY 1: dự đoán job/stage TRƯỚC khi nhìn UI, ghi ra giấy ----
sc.setJobDescription("Q1: filter+select — narrow thuần")
orders.filter(F.col("order_status") == "canceled") \
      .select("order_id", "order_purchase_timestamp") \
      .write.mode("overwrite").csv("/tmp/lab03_q1")

# ---- QUERY 2: 1 shuffle ----
sc.setJobDescription("Q2: groupBy status")
orders.groupBy("order_status").count().show()

# ---- QUERY 3: join + groupBy khác key + orderBy ----
sc.setJobDescription("Q3: join + groupBy + orderBy")
(orders.join(items, "order_id")
       .groupBy("seller_id").agg(F.sum("price").alias("revenue"))
       .orderBy(F.desc("revenue"))
       .show(10))

# ---- QUERY 4: skipped stage — chạy lại nhánh đã shuffle ----
sc.setJobDescription("Q4: chay lai de thay SKIPPED")
joined = orders.join(items, "order_id")
joined.count()                                          # job: shuffle đầy đủ
joined.groupBy("order_status").count().show()           # job: scan+join SKIPPED?

# ---- QUERY 5: wave — 12 task trên 2 core ----
sc.setJobDescription("Q5: wave 12 task / 2 core")
items.repartition(12).withColumn("x", F.log1p("price")) \
     .groupBy("seller_id").count().count()

input(">>> Giữ UI sống — http://localhost:4040. Enter để thoát...")
spark.stop()
```

### Bước 2 — dự đoán rồi mới chạy

Trên giấy, với từng query Q1–Q5: mấy job? mấy stage? stage nào song song? Xong mới:

```bash
make run-local F=labs/lab03/lesson03_stages.py
```

(Chạy `make run` trên cluster cũng được — nhớ worker chỉ có 1 core, wave sẽ dài gấp đôi. Tự giải thích tại sao!)

### Bước 3 — kiểm chứng trên UI (phần ăn điểm)

1. **Tab Jobs**: nhờ `setJobDescription`, từng job mang tên Q1..Q5. So từng dòng với dự đoán. Q3 có job phụ không tên (sampling của `orderBy`) — bắt được nó chưa?
2. **DAG Visualization của Q3**: đếm khối stage, xác định 2 nhánh scan chạy song song, chỉ tay vào 2 vết cắt (Exchange của join, của groupBy, của orderBy).
3. **Q4**: mở job thứ hai của Q4 — thấy stage **xám chữ "skipped"**. Giải thích bằng 2 câu: cái gì được reuse, nằm ở đâu.
4. **Q5 → tab Stages → chọn stage 12 task → Event Timeline**: đếm số wave (⌈12/2⌉ = 6). Nhìn wave cuối: có core nào rảnh không?
5. Ghi toàn bộ bảng "dự đoán vs thực tế" + giải thích chênh lệch vào `labs/lab03/NOTES-lesson03.md` (file mới).

---

## 9. Assignment

**Easy** — Không chạy code, đoán số **stage** của 5 job sau (AQE tắt, không broadcast), kèm 1 câu giải thích mỗi job; sau đó viết script kiểm chứng và báo cáo dự đoán đúng mấy/5:
1. `read.csv(schema=...) → withColumn → filter → write`
2. `read → dropDuplicates() → count()`
3. `read → groupBy(a).agg → filter → show`
4. `A.join(B, "k").groupBy("k").agg(...).show()` (để ý: groupBy trùng key join!)
5. `read → repartition(20) → write`

**Medium** — Vẽ DAG (tay hoặc ASCII) cho join 3 bảng Olist: `orders ⋈ items ⋈ products`, sau đó `groupBy("product_category_name")`. Yêu cầu: đánh số stage, tô rõ stage nào là ShuffleMapStage/ResultStage, chỉ ra các stage chạy song song được. Chạy thật để đối chiếu hình vẽ với DAG Visualization, dán kết luận chênh lệch.

**Hard** — Trả lời "tại sao shuffle nằm ở đây mà không ở kia": trong query 4 của bài Easy, giải thích vì sao `groupBy("k")` sau `join(B, "k")` **không** tạo thêm shuffle (gợi ý: dữ liệu sau join đã được phân vùng theo hash của k — output partitioning thoả yêu cầu của groupBy). Sửa query thành `groupBy("m")` (cột khác) và chứng minh bằng UI rằng shuffle mới xuất hiện. Viết 10 dòng đúc kết quy luật: khi nào Spark tiết kiệm được một shuffle.

**Production Challenge** — "Diễn tập on-call": tự tạo skew giả — union bảng items với chính nó 5 lần rồi thay 40% `seller_id` thành `"BIG_SELLER"` (`F.when(F.rand() < 0.4, "BIG_SELLER").otherwise(F.col("seller_id"))`), xong `groupBy("seller_id").agg(F.sum("price"))` với `spark.sql.shuffle.partitions=20`. Nhiệm vụ: (1) chụp/chép Summary Metrics của stage sau shuffle (min/median/max duration + shuffle read), (2) viết "incident note" 10 dòng theo đúng trình tự 4 bước của mục 7: Job nào → Stage nào → Task nào → key nào, (3) đề xuất 2 hướng xử lý (chưa cần làm — lesson 19 sẽ làm).

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Quan sát | Ý nghĩa performance |
|---|---|
| Job có nhiều stage | Nhiều shuffle. Mỗi vết cắt stage = một lần dữ liệu chạm disk + network. Giảm được 1 stage thường quý hơn tăng 2× executor. |
| Stage chậm hơn hẳn các stage khác | Đường găng nằm đó. Tối ưu chỗ khác = vô ích (định luật Amdahl phiên bản Spark). |
| max task duration >> median (trong 1 stage) | Skew — thêm tài nguyên KHÔNG cứu được, phải xử lý dữ liệu/key (lesson 19). |
| Số task ít hơn tổng core | Core thừa ngồi chơi cả stage — partition quá ít (lesson 4). |
| Wave cuối lơ thơ vài task | Chọn số partition chia hết cho tổng core để wave cuối đầy. |
| Stage "skipped" xuất hiện | Tin tốt — shuffle reuse. Nhưng nếu bạn *chủ động* cần reuse, hãy cache tường minh, đừng phó mặc. |
| Nhiều job lắt nhắt ngoài dự kiến | Action ẩn (inferSchema, orderBy sampling) hoặc action debug bỏ quên — dọn đi. |

Câu tự vấn mới: *"stage nào là đường găng của job này, và task chậm nhất của stage đó vì sao chậm?"*

---

## 11. Spark UI

Bài này mở khóa tab **Stages** — tab bạn sẽ ở lì trong đó suốt Module 3:

**Tab Stages** — nhìn gì:
- Danh sách mọi stage mọi job: trạng thái, số task (`succeeded/total`), Input/Output, **Shuffle Read/Write** (hai cột tiền tệ của Spark — từ nay nhìn nó như nhìn hoá đơn).
- Stage "skipped" màu xám: không tốn giây nào — shuffle reuse.

**Trong một stage** (click vào):
- **Event Timeline** (bấm mở ra): mỗi thanh ngang = 1 task, xếp theo executor/core. Đếm tầng = đếm wave. Màu trong từng thanh: xanh lá = compute, đỏ/cam = scheduler delay + deserialize, xanh dương = shuffle — thanh nào toàn màu không-phải-xanh-lá là task "sống khổ".
- **Summary Metrics**: bảng min / 25% / median / 75% / max cho duration, GC, shuffle read... **Đọc lệch max-vs-median thành phản xạ**: lệch 2–3× là bình thường, lệch 50× là skew.
- **Tasks table**: sort theo Duration/Shuffle Read tìm task cá biệt, xem nó chạy trên executor nào, locality gì.

**DAG Visualization** (trong tab Jobs, ôn từ lesson 1, giờ đọc sâu hơn): mỗi khối lớn = stage; trong khối thấy chuỗi operator được pipeline (`Scan → Filter → Project` dính nhau một khối = narrow); giữa các khối là `Exchange` = shuffle. Đường viền đỏ nhạt quanh operator = nơi tốn thời gian nhất (một số bản UI).

---

## 12. Common Mistakes

1. **Đổ lỗi "Spark chậm" mà không khoanh vùng Job→Stage→Task.** Chậm luôn có địa chỉ cụ thể. Không có địa chỉ = chưa điều tra = chưa được kết luận.
2. **Tune tài nguyên để chữa skew** — tăng executor, tăng memory trong khi max/median của stage lệch 50×. Một task ôm 10GB không quan tâm bạn có bao nhiêu core. Nhận diện skew trước, tune sau.
3. **Quên các job/stage ẩn** — orderBy sinh job sampling, inferSchema sinh job đọc, AQE chẻ nhỏ job — rồi hoảng "sao UI nhiều job thế". Đếm có hiểu biết, đừng đếm vẹt.
4. **Nghĩ stage sau chạy gối đầu stage trước** ("task xong cái nào đẩy xuống cái nấy"). Sai: barrier — stage sau chờ *toàn bộ* task cha. Vì hiểu sai điều này mà nhiều người không hiểu vì sao 1 task rùa kéo sập SLA cả pipeline.
5. **Coi skipped stage là bug** ("sao Spark không chạy stage của tôi?!") hoặc ngược lại — dựa vào skipped stage như một cơ chế cache chính thức. Nó là tối ưu cơ hội: có thì mừng, mất không được khóc.
6. **Không đặt `setJobDescription`** trong pipeline nhiều bước — nửa đêm on-call nhìn 30 job tên `showString at NativeMethodAccessorImpl.java:0`, khóc tiếng Mán.
7. **Ỷ lại speculative execution trị mọi task rùa** — nó chỉ trị rùa-do-node-yếu. Rùa-do-skew thì bản sao cũng rùa: cùng một partition béo thì chạy đâu cũng béo.

---

## 13. Interview

**Junior:**

1. *Job, Stage, Task — định nghĩa và cái gì sinh ra cái gì?* — Action sinh job. DAG Scheduler cắt job thành stage tại các ranh giới shuffle (wide dependency). Mỗi stage sinh 1 task cho mỗi partition; task chạy trên 1 core executor. Số task của stage = số partition của dữ liệu ở stage đó.
2. *Ranh giới stage nằm ở đâu? Tại sao ở đó?* — Tại shuffle — nơi partition con cần dữ liệu từ nhiều partition cha (groupBy, join, distinct, orderBy, repartition). Vì dữ liệu phải được ghi ra và tái phân phối qua network, không thể pipeline xuyên qua điểm đó, nên tự nhiên hình thành vết cắt.
3. *Trong một stage, chuyện gì xảy ra với chuỗi filter/select/withColumn?* — Chúng được pipeline: mỗi dòng dữ liệu chảy qua cả chuỗi hàm trong một lần, không tạo kết quả trung gian. Đó là lý do 5 narrow transformation không chậm hơn 1 cách đáng kể — cùng 1 lần quét dữ liệu.
4. *Một stage có 100 task, cluster có 20 core — bao nhiêu task chạy cùng lúc?* — 20. Còn lại xếp hàng, chạy thành ~5 wave. Tổng thời gian stage ≈ số wave × thời gian task (nếu task đều).

**Mid:**

5. *ShuffleMapStage vs ResultStage?* — ShuffleMapStage: các stage giữa, output là shuffle file ghi xuống local disk của executor, chia sẵn theo partition đích cho stage sau kéo về; có thể được reuse (skipped stage). ResultStage: stage cuối cùng duy nhất của job, tính kết quả action (trả driver hoặc ghi storage).
6. *Skipped stage là gì, khi nào xuất hiện?* — Stage mà output (shuffle file) đã tồn tại từ job trước nên DAG Scheduler bỏ qua không chạy lại. Xuất hiện khi nhiều action/job dùng chung một đoạn plan có shuffle, và shuffle file còn sống trên executor. Là tối ưu cơ hội — mất khi executor chết, không thay thế cache tường minh.
7. *Task fail thì chuyện gì xảy ra? Fetch failure khác gì task failure thường?* — Task failure thường: driver retry task đó trên executor khác, tối đa `spark.task.maxFailures` (4); quá thì job fail. Fetch failure (không kéo được shuffle file vì executor giữ file đã chết): không chỉ retry task hiện tại — DAG Scheduler phải resubmit phần ShuffleMapStage đã mất output để tạo lại shuffle file, đắt hơn nhiều.
8. *Vì sao stage sau phải chờ stage trước xong hết (barrier)? Hệ quả thực tế?* — Vì mỗi map task giữ một mảnh dữ liệu của mọi partition đích; reduce task phải kéo đủ mảnh từ tất cả map task mới có trọn dữ liệu của mình. Hệ quả: 1 task chậm (skew/straggler) chặn toàn bộ stage sau → tối ưu Spark phần lớn là làm task đều nhau.

**Senior:**

9. *Nhìn Spark UI thấy 1 stage có max duration 40 phút, median 30 giây. Chẩn đoán và các bước tiếp theo?* — Dấu hiệu kinh điển của data skew. Bước tiếp: (a) sort tasks theo duration/shuffle read xác nhận 1–vài task ôm phần lớn dữ liệu; (b) xác định key gây skew (đếm phân bố key của cột shuffle — thường là null/giá trị mặc định/khách hàng khổng lồ); (c) xử lý theo bản chất: lọc key rác, salting, tách key nóng xử lý riêng, bật AQE skew join. Điều KHÔNG làm: tăng executor/memory mù quáng — không giải quyết được 1 partition béo.
10. *Speculative execution hoạt động thế nào, khi nào bật, khi nào vô dụng?* — Driver theo dõi task chạy lâu bất thường so với median của stage, khởi chạy bản sao trên executor khác, lấy kết quả của bản xong trước và kill bản kia. Bật khi cluster không đồng nhất/hay có node ốm yếu (cloud, spot instance) — trị straggler do phần cứng. Vô dụng (thậm chí hại — tốn thêm slot) với straggler do skew: bản sao xử lý cùng partition béo nên chậm y hệt. Cần sink ghi idempotent vì có thể có 2 attempt cùng ghi.

---

## 14. Summary

### Mindmap

```
                          SPARK LESSON 3
                               │
     ┌──────────────┬──────────┴──────────┬──────────────────┐
     ▼              ▼                     ▼                  ▼
 DAG SCHEDULER   HAI LOẠI STAGE       TASK LIFECYCLE      ĐỌC UI
     │              │                     │                  │
 đi ngược plan   ShuffleMapStage:     serialize→chạy      Jobs → Stages
 gặp shuffle     ghi shuffle file      →báo kết quả        → Tasks (trình tự
 thì CẮT         (reuse → skipped!)   retry ≤4 lần         điều tra chuẩn)
 narrow =        ResultStage: trả     fetch fail =        Event Timeline
 pipeline        kết quả action       chạy lại stage cha   = đếm WAVE
 1 job =         barrier: chờ 100%    straggler/skew:     Summary Metrics:
 1+số shuffle    task cha xong        max >> median        max vs median
```

### Checklist trước khi gõ "Continue"

- [ ] Giải thích được thuật toán cắt stage: đi ngược từ action, chém tại wide dependency.
- [ ] Phân biệt ShuffleMapStage / ResultStage và nói được output của mỗi loại nằm đâu.
- [ ] Kể lại task lifecycle: serialize → locality → chạy → báo cáo → retry (4 lần) → fetch failure thì khác gì.
- [ ] Tính được số wave: ⌈số task / tổng core⌉, và giải thích vì sao wave cuối lệch là lãng phí.
- [ ] Đoán đúng (±1) số stage của 5 query trong assignment Easy.
- [ ] Đã nhìn thấy tận mắt: wave trên Event Timeline, stage skipped màu xám.
- [ ] Biết vì sao 1 task skew kéo sập cả pipeline (barrier giữa stage).
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 4 — Partition: đơn vị song song hóa.**

Suốt hai bài nay, "partition" xuất hiện ở mọi câu quan trọng: số task = số partition, task skew = partition béo, wave = partition ÷ core. Đã đến lúc lôi nhân vật này ra ánh sáng: **ai quyết định số partition khi bạn đọc một file** (spoiler: `spark.sql.files.maxPartitionBytes` và con số 128MB)? Tại sao sau mỗi shuffle, Spark mặc định chia **200 partition** — và vì sao con số một-cỡ-cho-tất-cả đó là config bị chửi nhiều nhất lịch sử Spark? Partition quá ít thì core ngồi chơi, quá nhiều thì overhead nuốt sạch lợi ích — vậy con số đúng tính thế nào (gợi ý: ~100–200MB/partition, bội số của tổng core)? Và `repartition` khác `coalesce` chỗ nào mà dùng nhầm là trả giá bằng cả một shuffle?

Nắm xong lesson 4, bạn khép kín bộ tứ nền tảng: kiến trúc → lazy/lineage → job/stage/task → partition. Từ đó trở đi, mọi bài đều là xây nhà trên móng này.

> Gõ **"Continue"** khi sẵn sàng.
