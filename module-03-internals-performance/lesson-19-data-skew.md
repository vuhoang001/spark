# Lesson 19 — Data Skew: phát hiện và xử lý

> Module 3 · Internals & Performance Tuning · Tuần 10 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **Data skew** là gì: dữ liệu phân bổ lệch giữa các partition — 1 partition khổng lồ, phần còn lại tí hon.
- Tại sao **1 task rùa bò kéo cả stage đứng chờ** (stage kết thúc theo task CHẬM NHẤT, không phải trung bình).
- Phát hiện skew qua Spark UI: task duration max >> median, shuffle read size lệch, summary metrics percentiles.
- 4 vũ khí xử lý: **salting**, **broadcast join**, **tách hot key**, và **AQE skew join** (tự động).
- Skew khi GHI: partition file lệch — thứ ít người để ý cho đến khi bảng downstream chậm bí ẩn.

Sau bài này bạn phải làm được:

- Nhìn tab Stages 30 giây và phán: "stage này skew hay không, skew ở key nào".
- Vẽ ASCII diagram giải thích salting cho một đồng nghiệp junior — từng bước, không bỏ bước nào.
- Chọn đúng vũ khí cho từng tình huống: khi nào salting, khi nào broadcast, khi nào phó mặc AQE.

Kiến thức dùng trong thực tế: skew là **nguyên nhân số 1** của "job chạy 6 tiếng mà 5 tiếng rưỡi chỉ chờ 1 task". Interviewer Senior gần như chắc chắn hỏi skew. Và Project 3 tuần sau, pipeline bạn phải cứu có skew được cài sẵn.

---

## 2. Why

### Câu chuyện có thật (xảy ra ở mọi công ty)

Job join `orders` với `sellers` chạy 20 phút ổn định suốt 3 tháng. Một ngày đẹp trời, công ty ký hợp đồng với một seller khổng lồ — seller này chiếm **40% tổng đơn hàng**. Job đột nhiên chạy 4 tiếng. Code không đổi một dòng. Dữ liệu chỉ tăng 15%.

Chuyện gì xảy ra? Nhớ lại lesson 15: khi `join`/`groupBy` theo key, Spark **hash partition** — mọi dòng cùng key BẮT BUỘC về cùng partition. Seller khổng lồ kia → toàn bộ 40% dữ liệu dồn vào **một** partition → **một** task xử lý, trong khi 199 task còn lại xong từ lâu, ngồi chơi.

```
Timeline stage (200 task):

task 001  ██ 30s
task 002  ██ 28s
task 003  ██ 31s
  ...       (196 task nữa, đều ~30s, xong hết)
task 187  ████████████████████████████████████████████ 3h 40m  ← hot key ở đây
          └──────────── CẢ STAGE chờ task này ────────────┘

Thời gian stage = max(task) = 3h40m, KHÔNG PHẢI avg(task) = 30s
```

### Tại sao "thêm máy" không cứu được

Phản xạ đầu tiên của junior: "cluster yếu, xin thêm executor". Vô ích. Bạn có 1000 executor thì partition nóng kia vẫn là **một** partition, vẫn do **một** task trên **một** core xử lý. 999 executor còn lại ngồi xem. Đây là bài học đắt giá: **skew là bài toán phân bổ dữ liệu, không phải bài toán tài nguyên**.

> **Analogy siêu thị**: 10 quầy thu ngân (task), khách được chia hàng theo chữ cái đầu của tên (hash key). Bỗng 90% khách hôm nay đều tên "Nguyễn" → quầy N xếp hàng dài 3 km, 9 quầy kia ngáp. Xây thêm 100 quầy? Vô nghĩa — quy tắc "tên Nguyễn về quầy N" vẫn còn đó. Phải đổi CÁCH CHIA (salting) hoặc bỏ luôn việc xếp hàng (broadcast).

### Trade-off của các giải pháp (Senior phải thuộc)

| Giải pháp | Được | Mất |
|---|---|---|
| Salting | Trị được mọi mức skew, kể cả bảng 2 bên đều lớn | Code phức tạp, bảng kia bị nhân bản ×N (explode) |
| Broadcast join | Né shuffle hoàn toàn → skew biến mất | Chỉ khi 1 bảng nhỏ (vừa RAM executor) |
| Tách hot key | Đơn giản, dễ hiểu, kiểm soát được | Phải BIẾT trước key nào nóng; union thêm bước |
| AQE skew join | Tự động, không sửa code | Chỉ trị skew ở JOIN sort-merge; ngưỡng mặc định có thể không khớp |

---

## 3. Theory

### 3.1. Định nghĩa và thuật ngữ

| Thuật ngữ | Nghĩa |
|---|---|
| **Data skew** | Phân bổ dữ liệu lệch giữa các partition sau shuffle: vài partition rất lớn, đa số nhỏ. |
| **Hot key** | Key xuất hiện với tần suất áp đảo (seller bán 40% đơn, user_id=NULL, quốc gia "US" trong log toàn cầu...). |
| **Straggler** | Task rùa bò — chạy lâu hơn hẳn các task cùng stage. Skew là nguyên nhân phổ biến nhất của straggler. |
| **Salting** | Kỹ thuật "rắc muối": thêm hậu tố/tiền tố ngẫu nhiên vào key nóng để băm nó ra nhiều partition. |
| **Salt factor (N)** | Số mảnh muốn chẻ key nóng ra. Hot key 1 partition → N partition. |

### 3.2. Skew sinh ra ở đâu?

Skew CHỈ gây đau ở **wide transformation** (có shuffle). Ba điểm nóng:

1. **Join theo key lệch** — `orders JOIN sellers ON seller_id`: seller nóng dồn 1 partition. Đau nhất vì partition nóng còn phải sort + merge.
2. **groupBy/aggregate theo key lệch** — `groupBy("seller_id").agg(...)`. Đỡ hơn join một chút vì có partial aggregation (map-side combine) gom bớt trước khi shuffle — nhưng `countDistinct`, `collect_list` thì không gom được, vẫn chết.
3. **Window function** — `Window.partitionBy("seller_id")`: toàn bộ dòng của key nóng phải về 1 partition VÀ sort. Không có map-side combine. Đau ngang join.

Và một loại thầm lặng: **skew khi ghi** — `df.write.partitionBy("country")` với 90% dữ liệu là 1 quốc gia → 1 folder khổng lồ, các folder khác lèo tèo. Đọc downstream lệch theo (xem 3.5).

### 3.3. Toán học của nỗi đau

Stage có P partition, tổng dữ liệu D, C core:

- **Không skew**: mỗi task xử lý D/P, thời gian stage ≈ (P/C) × t(D/P) — chia đều, mọi core bận.
- **Skew, hot key chiếm tỉ lệ s**: task nóng xử lý s×D một mình. Thời gian stage ≥ t(s×D), **bất kể** bạn có bao nhiêu core.

Ví dụ D = 100 GB, s = 0.4 → một task nhai 40 GB. Với executor 1 GB memory (cluster lab của bạn!) task đó không chỉ chậm — nó **spill** xối xả (lesson 15) hoặc OOM luôn. Skew và spill là cặp bài trùng: skew gây spill cục bộ, và trong UI bạn sẽ thấy đúng task nóng có spill trong khi task khác không.

### 3.4. Ngưỡng phát hiện — quy tắc thực chiến

Mở Stages tab → Summary Metrics (bảng percentile min/25th/median/75th/max):

| Dấu hiệu | Ngưỡng nghi ngờ | Kết luận |
|---|---|---|
| Duration: max vs median | max > 4× median | Skew gần như chắc chắn |
| Shuffle Read Size: max vs median | max > 5–10× median | Skew ở dữ liệu vào task |
| Spill: chỉ vài task có spill | spill lệch hẳn về 1–2 task | Skew + memory không đủ |
| Task còn RUNNING khi 95%+ đã SUCCESS | 1–2 task lì lợm | Straggler — soi shuffle read của nó |

AQE dùng ngưỡng tương tự để tự phát hiện: partition được coi là skew khi **> 5× median** VÀ **> 256 MB** (hai config ở section 5).

### 3.5. Skew khi ghi — cái bẫy cuối pipeline

```
df.write.partitionBy("customer_state").parquet(...)

Kết quả trên storage:
  customer_state=SP/   ████████████████████  8 GB   (São Paulo chiếm 42% dân số mua sắm)
  customer_state=RR/   ▌                     12 MB
  customer_state=AP/   ▌                     9 MB
```

Hậu quả: (a) task ghi folder SP chạy lâu nhất — skew ngay lúc ghi; (b) downstream đọc theo state thì query SP chậm gấp trăm lần; (c) nếu bạn `repartition("customer_state")` trước khi ghi để mỗi folder 1 file — file SP thành 1 file 8 GB, không splittable tốt. Giải pháp ở section 5.6.

---

## 4. Internal

Vì sao hash partitioning — cơ chế nền của shuffle — bất lực trước hot key:

```
Shuffle write (lesson 15): mỗi dòng được gán reducer partition bằng
    partition_id = hash(key) % numPartitions

Key "seller_HOT" → hash ra MỘT số cố định → 100% dòng của nó về MỘT partition.
Hash chia đều KEY, không chia đều DÒNG.

  Mapper 1: [HOT, HOT, A, HOT, B]  ─┐            ┌→ partition 0: [A...]        (nhỏ)
  Mapper 2: [HOT, HOT, HOT, C]     ─┼─ shuffle ──┼→ partition 1: [B..., C...]  (nhỏ)
  Mapper 3: [HOT, D, HOT, HOT]     ─┘            └→ partition 7: [HOT × 8 triệu] (BOOM)
```

Salting can thiệp đúng vào chỗ này — đổi key trước khi hash:

```
key mới = concat(key, "_", salt)   với salt = số ngẫu nhiên 0..N-1

hash("seller_HOT_0") % P = 3
hash("seller_HOT_1") % P = 9      → hot key giờ rơi vào N partition khác nhau
hash("seller_HOT_2") % P = 1
```

Nhưng join cần **khớp key hai bên**. Bên trái (orders) mỗi dòng nhận 1 salt ngẫu nhiên; bên phải (sellers) không biết dòng orders nào mang salt nào → bên phải phải **nhân bản mỗi dòng thành N bản**, mỗi bản một salt, đảm bảo mọi giá trị salt bên trái đều tìm được bạn nhảy bên phải:

```
BÊN TRÁI (lớn, skew) — rắc muối NGẪU NHIÊN:      BÊN PHẢI (nhỏ hơn) — EXPLODE đủ bộ muối:

seller_HOT | order_1   → seller_HOT_2 | order_1   seller_HOT | name → seller_HOT_0 | name
seller_HOT | order_2   → seller_HOT_0 | order_2                      → seller_HOT_1 | name
seller_HOT | order_3   → seller_HOT_1 | order_3                      → seller_HOT_2 | name
seller_A   | order_4   → seller_A_1   | order_4   seller_A   | name → seller_A_0   | name
                                                                     → seller_A_1   | name
                                                                     → seller_A_2   | name

JOIN ON key_salted:  mỗi mảnh HOT_0, HOT_1, HOT_2 join độc lập, chạy SONG SONG 3 core.
Kết quả join = y hệt join gốc (mỗi dòng trái khớp đúng 1 dòng phải, vì salt trái ⊂ bộ salt phải).
```

Chi phí: bên phải phình ×N. Vì vậy N chọn vừa đủ (5–20 thường ổn), và chỉ salting khi broadcast bất khả thi.

Còn AQE skew join làm gì bên trong? Sau khi shuffle write xong, AQE đọc **map output statistics** (kích thước từng partition — driver có sẵn số liệu này), thấy partition 7 to gấp 5× median → chẻ partition 7 thành nhiều **task con**, mỗi task con đọc một dải mapper output, và **nhân bản partition tương ứng bên bảng kia** cho mỗi task con. Bản chất là salting-tự-động ở tầng vật lý — không đổi key, chỉ đổi cách task đọc dữ liệu. Chi tiết ở lesson 20.

---

## 5. API

### 5.1. Chẩn đoán nhanh bằng code — đếm phân bố key

```python
from pyspark.sql import functions as F

# Top key nặng nhất — chạy TRƯỚC khi join để biết mình đối mặt với gì
(orders.groupBy("seller_id").count()
       .orderBy(F.desc("count"))
       .show(10))
```
- **Khi dùng**: nghi ngờ skew, muốn biết hot key là ai và nặng bao nhiêu %.
- **Pitfall**: bản thân lệnh này cũng shuffle — nhưng `count` có partial aggregation nên nhẹ hơn join nhiều, chấp nhận được để chẩn đoán.

### 5.2. Broadcast join — vũ khí số 1 khi một bảng nhỏ

```python
from pyspark.sql.functions import broadcast

result = orders.join(broadcast(sellers), "seller_id")   # sellers ~3k dòng → quá nhỏ
```
- **Ý nghĩa**: gửi nguyên bảng nhỏ đến mọi executor → join tại chỗ, **không shuffle bảng lớn** → không còn khái niệm partition theo key → skew tan biến.
- **Khi dùng**: bảng nhỏ < `spark.sql.autoBroadcastJoinThreshold` (mặc định 10 MB, nâng được lên ~vài trăm MB nếu executor đủ memory). Luôn thử broadcast TRƯỚC khi nghĩ đến salting.
- **Pitfall**: broadcast bảng lớn → OOM driver/executor (bảng phải vừa RAM từng executor — nhớ worker lab chỉ có 1 GB). Và broadcast là hint, Spark có thể từ chối nếu ước tính quá to.

### 5.3. Salting thủ công — full pattern

```python
from pyspark.sql import functions as F

N = 10  # salt factor

# Bên lớn + skew: salt ngẫu nhiên 0..N-1
orders_salted = orders.withColumn(
    "seller_salted",
    F.concat(F.col("seller_id"), F.lit("_"), (F.rand() * N).cast("int"))
)

# Bên kia: explode đủ N bản salt cho MỖI dòng
sellers_salted = (sellers
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
    .withColumn("seller_salted",
                F.concat(F.col("seller_id"), F.lit("_"), F.col("salt"))))

result = orders_salted.join(sellers_salted, "seller_salted")
```
- **Pitfall 1**: quên explode bên phải → mất dòng (salt trái không tìm thấy bạn nhảy). Kết quả SAI lặng lẽ — tệ hơn cả chậm.
- **Pitfall 2**: salting cho `groupBy` thì phải **aggregate 2 tầng**: `groupBy(key_salted).agg(...)` rồi bóc salt và `groupBy(key).agg(...)` lần nữa. Chỉ đúng với hàm phân rã được (sum, count, min, max — sum của sum là sum). `avg` phải tách thành sum + count rồi chia sau. `countDistinct` thì không phân rã kiểu này được.

### 5.4. Tách hot key xử lý riêng

```python
HOT = ["48436dade18ac8b2bce089ec2a041202"]   # đã biết từ bước chẩn đoán 5.1

hot   = orders.filter(F.col("seller_id").isin(HOT))
cold  = orders.filter(~F.col("seller_id").isin(HOT))

r_hot  = hot.join(broadcast(sellers.filter(F.col("seller_id").isin(HOT))), "seller_id")
r_cold = cold.join(sellers, "seller_id")     # phần này hết skew, sort-merge bình thường

result = r_cold.unionByName(r_hot)
```
- **Khi dùng**: 1–2 hot key biết trước, ổn định theo thời gian (vd: key NULL, khách hàng lớn cố định).
- **Pitfall**: hot key ĐỔI theo mùa (Black Friday seller khác nóng) → hardcode danh sách là quả bom hẹn giờ. Nếu hot key động, để AQE hoặc salting lo.

### 5.5. AQE skew join — bật và chỉnh ngưỡng

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")                    # 3.2+ mặc định true
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")           # mặc định true
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")            # > 5× median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m") # VÀ > 256MB
```
- **Pitfall**: điều kiện là **AND** — partition lệch 20× median nhưng chỉ 100 MB thì AQE mặc định KHÔNG chẻ (dưới 256 MB). Cluster nhỏ/lab: hạ threshold xuống (vd `8m`) mới thấy AQE hành động. Đây là lý do nhiều người than "bật AQE mà chẳng thấy gì".

### 5.6. Trị skew khi ghi

```python
# Mỗi folder partition được cắt file theo ngưỡng dòng — folder to thành nhiều file vừa
(df.repartition("customer_state")           # gom mỗi state về 1 task ghi...
   .write
   .option("maxRecordsPerFile", 1_000_000)  # ...nhưng cắt file mỗi 1M dòng
   .partitionBy("customer_state")
   .mode("overwrite")
   .parquet("output/orders_by_state"))
```
Muốn folder to có NHIỀU task ghi song song: `df.repartition(F.col("customer_state"), (F.rand()*8).cast("int"))` — lại là salting, lần này cho writer. Lesson 21 đào tiếp chuyện file layout.

---

## 6. Demo nhỏ

```
Input:  10 triệu dòng, key "HOT" chiếm 90%, 100 key khác chia 10% còn lại
   ↓    groupBy(key).count  — shuffle hash theo key
Output: thời gian + phân bố task trong UI (1 task ôm 9M dòng)
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (SparkSession.builder.appName("demo19-skew").master("local[2]")
         .config("spark.sql.adaptive.enabled", "false")     # tắt AQE để thấy skew trần trụi
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

df = (spark.range(10_000_000)
      .withColumn("key", F.when(F.rand() < 0.9, F.lit("HOT"))
                          .otherwise(F.concat(F.lit("k"), (F.rand()*100).cast("int")))))

t0 = time.time()
df.groupBy("key").agg(F.count("*").alias("cnt"), F.avg("id").alias("a")).collect()
print(f"Skew, no AQE: {time.time()-t0:.1f}s")

input(">>> Mở http://localhost:4040 → Stages → stage cuối → Summary Metrics. "
      "So sánh Duration max vs median, Shuffle Read max vs median. Enter để thoát...")
spark.stop()
```

Chạy xong hãy tự hỏi: 8 partition sau shuffle, mấy partition thật sự có việc? Task nào ôm 9 triệu dòng? Nếu bật lại AQE thì sao (thử đổi config rồi chạy lại)?

---

## 7. Production Example

Pipeline Olist-CDC thật của bạn (kiến trúc lesson 1) gặp skew ở đâu:

```
Kafka → Spark silver: dedup theo order_id            ← ít skew (order_id gần unique)
      → Spark gold:   fact_sales JOIN dim_seller     ← SKEW: vài mega-seller
                      fact JOIN dim_customer          ← SKEW: customer_id NULL (đơn guest!)
                      groupBy(seller_id) daily agg    ← SKEW: cùng mega-seller
      → Iceberg: write partitionBy(order_date)        ← SKEW GHI: ngày sale 11/11 gấp 50× ngày thường
```

Cách các công ty lớn xử lý từng điểm:

1. **NULL key** — kinh điển nhất trần đời. Đơn không có customer đăng nhập → customer_id NULL → hàng triệu NULL về 1 partition. Fix chuẩn: filter NULL ra, union lại sau (chính là "tách hot key"), hoặc thay NULL bằng giá trị random rồi bỏ kết quả join (null không bao giờ khớp dim).
2. **Dim nhỏ** → broadcast hết. Quy tắc nhà nghề: dimension < vài trăm MB thì broadcast là mặc định, sort-merge là ngoại lệ.
3. **Mega-seller trong agg** → bật AQE + hạ skew threshold theo cỡ cluster; nếu vẫn lệch (skew trong groupBy, AQE không trị) → salting 2 tầng.
4. **Ngày sale khi ghi** → `maxRecordsPerFile` + salting writer, hoặc partition theo giờ riêng cho ngày sale (Iceberg partition evolution làm được — module 5).

Bài học: skew không phải bug, nó là **bản chất của dữ liệu kinh doanh** (Pareto: 20% khách tạo 80% doanh thu). Pipeline production phải được THIẾT KẾ sẵn cho skew, không phải vá khi cháy.

---

## 8. Hands-on Lab

**Mục tiêu**: tự tạo skew trên dataset Olist, đo nỗi đau, rồi lần lượt cứu bằng 3 cách — đo từng cách.

### Bước 0 — chuẩn bị

Cluster Docker của repo (worker 1 GB / 1 core — cấu hình này CỐ TÌNH nhỏ để bạn tái hiện spill/skew dễ dàng):

```bash
make up     # master UI: http://localhost:8080, app UI khi chạy: http://localhost:4040
```

Dataset Olist tại `data/olist/*.csv` (trong container: `/workspace/data/olist/`).

### Bước 1 — viết `labs/lab19/make_skew.py` (tạo dữ liệu lệch)

Olist gốc khá đều — ta "bơm" một mega-seller: nhân bản đơn của 1 seller lên 300×.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("lab19-make-skew").getOrCreate()

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)

top = (items.groupBy("seller_id").count().orderBy(F.desc("count")).first())
print(f"Seller sẽ thành mega-seller: {top['seller_id']} ({top['count']} items)")

hot  = items.filter(F.col("seller_id") == top["seller_id"])
big  = hot.crossJoin(spark.range(300).select(F.lit(1).alias("dup"))).drop("dup")
skewed = items.unionByName(big)

skewed.write.mode("overwrite").parquet("/workspace/labs/lab19/items_skewed")
print(f"Tổng: {skewed.count():,} dòng")
spark.stop()
```

### Bước 2 — viết `labs/lab19/join_baseline.py` (nếm đòn)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (SparkSession.builder.appName("lab19-baseline")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # ép sort-merge join
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())

items   = spark.read.parquet("/workspace/labs/lab19/items_skewed")
sellers = spark.read.csv("/workspace/data/olist/olist_sellers_dataset.csv",
                         header=True, inferSchema=True)

t0 = time.time()
(items.join(sellers, "seller_id")
      .groupBy("seller_state").agg(F.sum("price").alias("gmv"))
      .collect())
print(f"BASELINE (sort-merge, no AQE): {time.time()-t0:.1f}s")

input(">>> UI :4040 → Stages: chụp lại Duration max/median và Shuffle Read max/median. Enter...")
spark.stop()
```

### Bước 3 — ba bản cứu, mỗi bản một file

- `labs/lab19/fix_broadcast.py`: như baseline nhưng `items.join(broadcast(sellers), ...)` và bỏ config threshold.
- `labs/lab19/fix_salting.py`: pattern 5.3 với N=10 (vẫn tắt AQE + tắt broadcast để thấy công lực salting thuần).
- `labs/lab19/fix_aqe.py`: như baseline nhưng `adaptive.enabled=true`, `skewJoin.enabled=true`, và hạ `skewedPartitionThresholdInBytes` xuống `8m` (dữ liệu lab nhỏ hơn 256 MB mặc định).

### Bước 4 — chạy và ghi bảng so sánh

```bash
make run F=labs/lab19/make_skew.py
make run F=labs/lab19/join_baseline.py
make run F=labs/lab19/fix_broadcast.py
make run F=labs/lab19/fix_salting.py
make run F=labs/lab19/fix_aqe.py
```

Ghi vào `labs/lab19/NOTES.md`: bảng 4 dòng (baseline / broadcast / salting / AQE) × 4 cột (thời gian, duration max/median, shuffle read max/median, số task của stage join). Kèm 3 câu trả lời: cách nào nhanh nhất? tại sao? nếu sellers nặng 5 GB thì xếp hạng đổi thế nào?

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Tại sao stage kết thúc theo task chậm nhất chứ không phải trung bình? Thêm executor có cứu được skew không, vì sao?
2. Nêu 3 dấu hiệu skew trong Spark UI và ngưỡng con số cụ thể cho từng dấu hiệu.
3. Vì sao salting bên trái thì bên phải phải explode? Quên explode thì hậu quả gì?

**Medium** — Trên `items_skewed` của lab: chạy `groupBy("seller_id").agg(countDistinct("order_id"))` — có skew không, nặng hơn hay nhẹ hơn `count(*)`, vì sao (gợi ý: partial aggregation)? Sau đó viết bản salting 2 tầng cho phép `sum("price")` theo seller_id: groupBy key muối → bóc muối → groupBy lần 2. Chứng minh kết quả khớp bản không salting.

**Hard** — AQE đấu salting: tăng mega-seller lên 1000× (sửa `make_skew.py`), chạy lại `fix_aqe.py` và `fix_salting.py`. Trường hợp nào AQE thua salting? Mở UI xem AQE chẻ partition nóng thành mấy mảnh (SQL tab → AQEShuffleRead), thử chỉnh `skewedPartitionFactor` từ 5 xuống 2 và quan sát số mảnh đổi.

**Production Challenge** — Viết hàm `skew_report(df, key_col)` trả về DataFrame: tổng số key, top 10 key theo count, % của top key, tỉ lệ max/median. Chạy trên 3 cột của Olist: `seller_id`, `customer_state` (bảng customers), `product_category_name` (products). Cột nào đáng lo nhất nếu dùng làm join key / partition ghi? Viết 5 dòng khuyến nghị như một tech lead.

> Nộp bài bằng cách paste code + số đo + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Tình huống | Chi phí | Ghi chú |
|---|---|---|
| Join skew, không xử lý | Thời gian = t(hot partition), spill/OOM ở 1 task | Cluster to mấy cũng vậy |
| Broadcast join | Rẻ nhất: 0 shuffle bảng lớn | Điều kiện: bảng nhỏ vừa RAM executor |
| Salting N=10 | Bảng phải ×10 dòng, thêm 2 cột tính toán | Đổi 1 task 40 GB lấy 10 task 4 GB — hời to |
| AQE skew join | Gần miễn phí (chẻ ở tầng đọc shuffle) | Chỉ trị JOIN; ngưỡng phải khớp cỡ dữ liệu |
| Tách hot key | 2 job con + union | Rẻ nếu hot key ít và biết trước |

Quy tắc chọn vũ khí (theo thứ tự ưu tiên):

1. Một bảng nhỏ? → **broadcast**. Hết chuyện.
2. Hai bảng đều lớn, Spark 3.2+? → **bật AQE + chỉnh threshold**, đo. Đủ tốt thì dừng.
3. AQE không đủ (skew ở groupBy/window, hoặc lệch quá nặng)? → **salting**.
4. Hot key ít, cố định, nghiệp vụ rõ (NULL, khách VIP)? → **tách riêng** — dễ đọc dễ bảo trì nhất.

Câu tự vấn trước mọi join/groupBy từ nay: *"key này phân bố thế nào? có Pareto không? NULL có nhiều không?"*

---

## 11. Spark UI

Bài này mở khóa kỹ năng đọc **Summary Metrics** — bảng quan trọng nhất của Stages tab:

**Tab Stages → click stage nghi ngờ → Summary Metrics for N completed tasks:**

```
Metric              Min     25th    Median   75th    Max
Duration            0.2s    0.3s    0.4s     0.5s    4.8min   ← max/median = 720×: SKEW!
Shuffle Read Size   1 MB    2 MB    2.1 MB   2.4 MB  890 MB   ← dữ liệu vào lệch: gốc rễ đây
Spill (disk)        0       0       0        0       1.2 GB   ← chỉ task nóng spill
```

- Đọc từ dưới lên: **Shuffle Read lệch → Duration lệch → Spill cục bộ**. Thấy chuỗi này = skew, không cần đoán.
- Bảng **Tasks** phía dưới: sort theo Duration giảm dần → nhìn cột Shuffle Read Size / Records của task đầu bảng → task nóng lộ diện. Cột Host cho biết nó nằm executor nào.
- **Event Timeline** (trong trang stage): 1 thanh dài lê thê giữa rừng thanh ngắn — bức ảnh "chụp skew" đẹp nhất để bỏ vào báo cáo.
- Khi AQE bật: tab **SQL** → query → node `AQEShuffleRead` sẽ ghi `number of skewed partitions` và `number of skewed partition splits` — bằng chứng AQE đã ra tay (lesson 20 đọc sâu).

---

## 12. Common Mistakes

1. **Thấy chậm là tăng executor/memory** mà không mở UI. Skew không ăn tài nguyên tổng — nó ăn MỘT core. Đo trước, sửa sau.
2. **Salting quên explode bảng bên kia** → join mất dòng, kết quả sai lặng lẽ. Luôn assert count kết quả khớp bản chưa salting trên dữ liệu test.
3. **Salt factor to vô tội vạ (N=200)** → bảng kia phình 200×, shuffle nặng hơn cả bệnh gốc. N chỉ cần đủ chẻ hot partition xuống ~cỡ median.
4. **Tin AQE lo hết** rồi thắc mắc sao vẫn chậm: quên rằng ngưỡng mặc định 256 MB quá cao cho cluster nhỏ, và AQE skew join KHÔNG trị skew trong groupBy/window.
5. **Bỏ quên NULL key** — hot key phổ biến nhất vũ trụ. Kiểm tra `df.filter(col(k).isNull()).count()` trước mọi join.
6. **`repartition(1000)` để "chia nhỏ cho hết skew"** — vô dụng với hash partition theo key: hot key vẫn về 1 partition trong 1000 cái đó. Đổi SỐ partition không đổi CÁCH phân phối key.
7. **Fix skew ở compute nhưng để nguyên skew khi ghi** → bảng output file lệch, downstream lãnh đủ. Kiểm tra kích thước file/folder sau khi ghi.

---

## 13. Interview

**Junior:**

1. *Data skew là gì? Cho ví dụ thực tế.* — Dữ liệu phân bổ lệch giữa các partition sau shuffle: một vài partition rất lớn do hot key (mọi dòng cùng key phải về cùng partition). Ví dụ: một mega-seller chiếm 40% đơn hàng, customer_id NULL, quốc gia "US" trong log toàn cầu.
2. *Tại sao skew làm job chậm?* — Stage chỉ kết thúc khi task cuối cùng xong; task ôm hot partition chạy lâu gấp trăm lần các task khác nên cả stage (và các stage phụ thuộc) đứng chờ nó. Thời gian stage = max(task), không phải avg.
3. *Làm sao phát hiện skew?* — Spark UI → Stages → Summary Metrics: Duration max >> median (>4×), Shuffle Read Size max >> median, spill chỉ xuất hiện ở 1–2 task; Event Timeline có 1 thanh dài bất thường. Bằng code: groupBy key đếm, xem top key chiếm bao nhiêu %.
4. *Broadcast join giúp gì cho skew?* — Bảng nhỏ được gửi nguyên vẹn tới mọi executor, join tại chỗ, bảng lớn KHÔNG phải shuffle theo key → không còn hot partition → skew biến mất. Điều kiện: bảng nhỏ vừa memory executor.

**Mid:**

5. *Giải thích salting từng bước.* — Bên lớn/skew: thêm salt ngẫu nhiên 0..N-1 vào key (`key_salt`) → hot key bị băm ra N partition. Bên kia: explode mỗi dòng thành N bản, mỗi bản một giá trị salt, để mọi salt bên trái đều khớp được. Join theo key muối; kết quả tương đương join gốc. Chi phí: bảng kia ×N.
6. *Salting cho groupBy khác gì salting cho join?* — Không cần explode bảng nào, nhưng phải aggregate 2 tầng: groupBy(key+salt) tính partial, rồi groupBy(key) gộp lại. Chỉ đúng với hàm phân rã được (sum/count/min/max); avg phải tách sum+count; countDistinct không phân rã kiểu này được.
7. *AQE xử lý skew thế nào? Điều kiện kích hoạt?* — Sau shuffle write, AQE đọc kích thước thật từng partition; partition > skewedPartitionFactor × median (mặc định 5) VÀ > skewedPartitionThresholdInBytes (mặc định 256 MB) sẽ bị chẻ thành nhiều task con, partition đối ứng bên kia được nhân bản. Chỉ áp dụng cho sort-merge join (và shuffled hash join từ 3.2).
8. *Vì sao tăng `spark.sql.shuffle.partitions` không cứu được skew?* — Hash partitioning gửi TOÀN BỘ dòng cùng key về một partition bất kể tổng số partition là bao nhiêu. Tăng số partition chỉ chia nhỏ phần "cold", hot key vẫn nguyên khối.

**Senior:**

9. *Join 2 bảng đều 1 TB, key lệch nặng, Spark 3.4 — anh/chị xử lý thế nào?* — (a) Chẩn đoán: đếm phân bố key, xác định hot keys và tỉ trọng; kiểm tra NULL. (b) Không broadcast được (cả 2 to). (c) Bật AQE skew join, chỉnh factor/threshold theo cỡ partition thực tế, đo — thường đủ cho join. (d) Nếu skew cực nặng (1 key = 30%+) hoặc cần groupBy/window sau join: salting với N tính từ (kích thước hot partition / kích thước partition mục tiêu). (e) Nếu hot key nghiệp vụ rõ ràng: tách xử lý riêng để code dễ bảo trì. (f) Xem lại thiết kế: có cần join full không, filter/pre-aggregate trước được không.
10. *Skew khi ghi partition — vấn đề và giải pháp?* — `partitionBy(col)` với cột lệch tạo folder khổng lồ cạnh folder tí hon: task ghi lệch, downstream đọc lệch, file to không splittable tốt hoặc 1 folder ngàn file nhỏ. Giải pháp: `maxRecordsPerFile` để cắt file trong folder to; repartition theo (col, salt) để nhiều task cùng ghi folder nóng; xem lại cột partition (cardinality và phân bố phải hợp lý — lesson 21); với Iceberg dùng partition evolution / hidden partitioning.

---

## 14. Summary

### Mindmap

```
                            DATA SKEW (L19)
                                  │
      ┌───────────────┬───────────┴───────────┬────────────────────┐
      ▼               ▼                       ▼                    ▼
   BẢN CHẤT        PHÁT HIỆN               XỬ LÝ                SKEW KHI GHI
      │               │                       │                    │
  hash(key) dồn    UI Stages:            1. broadcast          partitionBy cột lệch
  hot key về 1     max >> median (4×)       (bảng nhỏ)         → folder to/nhỏ lệch
  partition        shuffle read lệch     2. AQE skew join      fix: maxRecordsPerFile
  stage = max(task) spill cục bộ            (factor 5, 256MB)       + salt writer
  thêm máy vô ích  code: đếm key         3. salting N mảnh
  NULL = hot key      phân bố               (explode bên kia!)
  kinh điển                              4. tách hot key riêng
```

### Checklist trước khi gõ "Continue"

- [ ] Giải thích được vì sao 1 task rùa kéo cả stage, và vì sao thêm executor không cứu.
- [ ] Đọc Summary Metrics: chỉ ra 3 dòng nào tố cáo skew, ngưỡng bao nhiêu.
- [ ] Vẽ lại diagram salting (2 bên: rắc muối vs explode) từ trí nhớ.
- [ ] Nói được điều kiện AND của AQE skew join và vì sao lab phải hạ threshold.
- [ ] Đã chạy đủ 4 bản lab (baseline + 3 fix) và có bảng số liệu so sánh.
- [ ] Biết thứ tự ưu tiên: broadcast → AQE → salting → tách hot key.
- [ ] Trả lời 10 câu interview không nhìn đáp án.

---

## 15. Next Lesson

**Lesson 20 — AQE (Adaptive Query Execution).**

Hôm nay bạn đã thấy AQE "tự chẻ" partition skew như phép màu. Nhưng skew join chỉ là 1 trong 3 tuyệt chiêu của nó: AQE còn tự gộp shuffle partitions thừa (bye bye chuyện đau đầu chỉnh `spark.sql.shuffle.partitions`), và tự đổi sort-merge join thành broadcast khi phát hiện bảng sau filter nhỏ hơn dự tính. Câu hỏi lớn: tại sao plan tĩnh của Catalyst — tối ưu cỡ đó — vẫn sai, và AQE lấy đâu ra số liệu để sửa giữa chừng? Lesson 20 mổ máy: runtime statistics, query stage, cách đọc `AdaptiveSparkPlan` trong explain, và — quan trọng không kém — danh sách những thứ AQE **bó tay**, để bạn không ngồi chờ phép màu không bao giờ đến.

> Gõ **"Continue"** khi sẵn sàng.
