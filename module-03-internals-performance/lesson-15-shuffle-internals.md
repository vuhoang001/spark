# Lesson 15 — Shuffle internals: shuffle write/read, spill

> Module 3 · Internals & Performance Tuning · Tuần 8 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Shuffle thật sự diễn ra như thế nào: **2 phase — map side write và reduce side read** — không còn là "dữ liệu bay qua network" mơ hồ nữa.
- **Shuffle files** trên local disk: mỗi map task ghi 1 file data + 1 file index, và tại sao thiết kế này thắng thiết kế cũ.
- **Spill to disk**: chuyện gì xảy ra khi execution memory không đủ chỗ để sort/aggregate.
- Đọc thành thạo 4 metric sống còn trên Spark UI Stages tab: **Shuffle Write, Shuffle Read, Spill (Memory), Spill (Disk)**.
- **External shuffle service** — tại sao production cluster cần nó.
- Chi phí THẬT của một shuffle: serialize + disk I/O + network + deserialize.

Sau bài này bạn phải làm được:

- Vẽ lại sơ đồ shuffle 2 phase từ trí nhớ, chỉ đúng chỗ nào là disk, chỗ nào là network.
- Nhìn một stage trên Spark UI và trả lời: "stage này shuffle bao nhiêu, có spill không, spill nặng cỡ nào?"
- Cố tình tạo ra spill trong lab (với executor 512MB) và đo được nó chậm hơn bao nhiêu lần.

Kiến thức dùng trong thực tế: đây là bài **quan trọng nhất Module 3**. 80% job Spark chậm là do shuffle. Không hiểu shuffle internals thì mọi kỹ thuật tuning ở lesson 16–22 đều là bấm nút cầu may.

---

## 2. Why

### Bạn đã biết shuffle "đắt" — nhưng đắt Ở ĐÂU?

Từ lesson 3 và lesson 9, bạn biết: `groupBy`, `join`, `distinct`, `repartition` gây shuffle, và shuffle cắt job thành stage. Nhưng "shuffle đắt" là câu trả lời của junior. Senior phải trả lời được: **đắt ở khâu nào, đo bằng metric gì, giảm bằng cách nào**.

Sự thật gây bất ngờ số 1: shuffle KHÔNG phải chỉ là network. Một record đi qua shuffle phải trả 4 loại phí:

```
Executor A (map side)                              Executor B (reduce side)
┌─────────────────────────────┐                   ┌─────────────────────────────┐
│ ① SERIALIZE                 │                   │ ④ DESERIALIZE               │
│   object JVM → bytes        │                   │   bytes → object JVM        │
│ ② GHI DISK (shuffle file)   │ ── ③ NETWORK ──▶  │   (+ có thể spill tiếp      │
│   (+ có thể spill trước đó) │      fetch        │    khi aggregate)           │
└─────────────────────────────┘                   └─────────────────────────────┘
```

Nghĩa là: kể cả khi 2 task nằm **cùng một máy**, shuffle vẫn tốn serialize + ghi disk + đọc disk + deserialize. Network chỉ là một trong bốn hóa đơn.

Sự thật gây bất ngờ số 2: **shuffle data LUÔN chạm disk** ở map side — kể cả khi memory dư thừa. Đây là thiết kế cố ý (để fault tolerance và để reducer đến lấy sau), không phải "Spark hết RAM nên mới ghi disk". Còn **spill** là chuyện khác: ghi disk *ngoài kế hoạch* vì memory hết giữa chừng — cái này mới là thứ ta phải săn lùng và tiêu diệt.

### Analogy: bưu điện phân loại thư

Hãy tưởng tượng 100 nhân viên (map task) mỗi người ôm một thùng thư hỗn độn, cần chuyển đến 200 quận (reduce partition):

1. **Map side write**: mỗi nhân viên *phân loại thư của mình theo quận* (sort theo partition id), bó lại thành 1 kiện có mục lục dán ngoài ("thư quận 1 từ trang 0–40, quận 2 từ trang 41–90..." — chính là **index file**), rồi đặt kiện ở kho của bưu cục mình (**local disk**).
2. **Reduce side read**: bưu tá của mỗi quận (reduce task) đi *gõ cửa cả 100 bưu cục*, nhìn mục lục, chỉ lấy đúng phần thư của quận mình (**fetch qua network**).
3. **Spill**: bàn phân loại của nhân viên quá nhỏ (execution memory hết) → phân loại được một mớ phải bê tạm xuống sàn (spill file), cuối cùng phải gộp các mớ dưới sàn lại (merge) — làm đi làm lại, chậm hẳn.

### Nếu không hiểu shuffle thì sao?

- Job chạy 2 tiếng, bạn tăng executor memory gấp đôi — không nhanh hơn, vì bottleneck là 500 GB shuffle write mà bạn không nhìn thấy.
- Bạn thấy "Spill (Disk): 40 GB" trên UI mà không biết nó nghĩa là job đang trả phí disk I/O gấp nhiều lần cần thiết.
- Interviewer hỏi "shuffle hoạt động thế nào" — câu này xuất hiện trong hầu hết vòng phỏng vấn DE mid/senior.

### Trade-off của thiết kế shuffle (Senior phải thuộc)

| Được | Mất |
|---|---|
| Ghi shuffle file xuống disk → reducer fail thì fetch lại, không cần chạy lại mapper | Disk I/O kể cả khi RAM dư |
| Mỗi mapper chỉ 1 data file + 1 index file → hàng nghìn task không làm nổ số lượng file | Phải sort theo partition id trước khi ghi |
| Spill cho phép xử lý dữ liệu lớn hơn memory (không OOM) | Spill = ghi + đọc lại + merge → chậm gấp nhiều lần |
| Reducer kéo song song từ nhiều mapper | Network là tài nguyên chia sẻ — shuffle to làm nghẽn cả cluster |

---

## 3. Theory

### 3.1. Thuật ngữ nền (dùng suốt Module 3)

| Thuật ngữ | Nghĩa |
|---|---|
| **Map side / Mapper** | Các task của stage TRƯỚC ranh giới shuffle — bên "ghi ra". |
| **Reduce side / Reducer** | Các task của stage SAU ranh giới shuffle — bên "đọc vào". |
| **Shuffle file** | File trên local disk của executor chứa output đã phân vùng của 1 map task. |
| **Spill** | Đổ dữ liệu trung gian từ memory xuống disk vì execution memory hết. |
| **Shuffle block** | Khúc dữ liệu trong shuffle file dành cho 1 reduce partition cụ thể. |
| **spark.sql.shuffle.partitions** | Số partition (= số reduce task) sau mỗi shuffle của Spark SQL. Mặc định **200**. |

### 3.2. Bức tranh toàn cảnh: 2 phase

Ví dụ: `orders.groupBy("customer_state").count()` với 4 map task và 3 reduce partition:

```
STAGE 1 (map side)                            STAGE 2 (reduce side)
─ mỗi task đọc 1 partition input ─            ─ mỗi task phụ trách 1 reduce partition ─

Map task 1 ──▶ sort theo partition id ──▶ ┌────────────────────┐
              (partial agg trước nếu là  │ shuffle_1.data      │◀─┐
               groupBy — combiner)       │ [P0 | P1 | P2 ]     │  │
                                          │ shuffle_1.index     │  │  fetch block P0
Map task 2 ──▶ ... ──────────────────▶   │ shuffle_2.data/index│◀─┼──── Reduce task P0
                                          ├────────────────────┤  │     (gom "SP" từ mọi mapper)
Map task 3 ──▶ ... ──────────────────▶   │ shuffle_3.data/index│◀─┤
                                          ├────────────────────┤  │  fetch block P1
Map task 4 ──▶ ... ──────────────────▶   │ shuffle_4.data/index│◀─┴──── Reduce task P1
                                          └────────────────────┘        ...
                                           LOCAL DISK của từng           Reduce task P2
                                           executor (spark.local.dir)
        │◀────────── SHUFFLE WRITE ──────────▶│◀──── SHUFFLE READ (network) ────▶│
```

Đọc kỹ 3 điểm:

1. **Map side write**: mỗi map task xử lý xong partition của mình thì tính `hash(key) % numPartitions` cho từng record để biết record thuộc reduce partition nào, **sort theo partition id**, rồi ghi TẤT CẢ xuống **một file data duy nhất** trên local disk, kèm **một file index** ghi offset từng block. Với `groupBy`+`count`, Spark còn làm **partial aggregation** ngay tại map side (gom "SP: 1050 dòng" thành 1 record) — nên shuffle write thường nhỏ hơn input rất nhiều.
2. **Reduce side read**: mỗi reduce task hỏi driver "block của tôi nằm ở đâu?" rồi mở kết nối đến các executor nguồn, **fetch đúng block của mình qua network** (block cùng máy thì đọc thẳng disk, không qua network). Fetch về xong thì merge/aggregate tiếp ra kết quả cuối.
3. **Ranh giới stage nằm chính giữa**: Stage 1 chỉ xong khi TẤT CẢ map task ghi xong file. Stage 2 mới bắt đầu fetch. Đây là lý do 1 task rùa bò ở stage 1 (skew — lesson 19) chặn cả stage 2.

### 3.3. Shuffle file: data + index

Mỗi map task tạo đúng 2 file:

```
shuffle_0_5_0.data     ← toàn bộ output của map task 5, sort theo reduce partition id
shuffle_0_5_0.index    ← "mục lục": offset bắt đầu của từng partition trong file .data

.index:  [P0: byte 0] [P1: byte 41_320] [P2: byte 98_770] ... [P199: byte 8_112_004]
                │
                ▼  reduce task P1 chỉ cần seek đến byte 41_320, đọc đến 98_770 — xong.
```

Tại sao thiết kế này quan trọng? Bản hash-based shuffle đời đầu của Spark ghi **mỗi (map task × reduce partition) một file riêng**: 1.000 mapper × 1.000 reducer = **1.000.000 file** → hệ điều hành chết ngộp (file descriptor, random I/O). **Sort-based shuffle** (mặc định từ Spark 1.2 đến nay) đưa về `2 × số map task` file — đổi lại phải sort theo partition id trước khi ghi. Đây là ví dụ kinh điển của trade-off "trả CPU để cứu I/O".

### 3.4. Spill: khi bàn làm việc quá nhỏ

Map task sort dữ liệu **trong execution memory** (một buffer trong vùng unified memory — lesson 17). Khi buffer đầy mà record vẫn còn:

```
Execution memory (buffer sort)
┌────────────────┐
│ ████████████░░ │ đầy! ──▶ sort phần đang có ──▶ ghi ra SPILL FILE #1 trên disk
└────────────────┘          (giải phóng buffer, nhận record tiếp)
        │ đầy lần nữa ──▶ SPILL FILE #2 ... #N
        ▼
   Kết thúc task: MERGE (N spill file + phần trong memory)
                  ──▶ 1 shuffle file cuối cùng (data + index)
```

- Spill xảy ra ở **cả hai phía**: map side (khi sort/partial-agg) và reduce side (khi gom các block fetch về để aggregate/sort/join).
- Trên UI: **Spill (Memory)** = kích thước dữ liệu trong memory lúc bị đổ xuống (dạng object đã "nở"), **Spill (Disk)** = kích thước thực ghi xuống disk (đã serialize + nén, thường nhỏ hơn nhiều lần). Cặp số này còn cho bạn ước lượng "hệ số nở" của dữ liệu khi deserialize.
- Spill KHÔNG phải lỗi — nó là cơ chế tự vệ chống OOM. Nhưng spill nhiều = task làm việc kiểu "viết ra giấy nháp rồi chép lại" — mỗi byte bị xử lý nhiều lần.

### 3.5. External shuffle service

Shuffle file nằm trên disk **của executor**. Vậy executor chết (hoặc bị thu hồi bởi dynamic allocation) thì file ai phục vụ? → Cả đống map output mất → Spark phải **chạy lại các map task** đó. Giải pháp: **external shuffle service** — một process độc lập chạy trên mỗi worker node, giữ và phục vụ shuffle file thay cho executor:

```
Không có ESS:  reducer ──fetch──▶ executor (chết là mất)
Có ESS:        reducer ──fetch──▶ shuffle service của node (executor chết vẫn còn file)
                                   └─ điều kiện bắt buộc để bật dynamic allocation trên YARN
```

Trên Kubernetes (không có ESS truyền thống), Spark 3.x dùng hướng khác: `shuffleTracking` (giữ executor còn shuffle data sống lâu hơn) hoặc push-based/remote shuffle service (Magnet, Celeborn) ở các công ty lớn.

### 3.6. Tổng kết chi phí thật của shuffle

| Khâu | Phí | Ai trả |
|---|---|---|
| Serialize record → bytes | CPU | map task |
| Sort theo partition id (+ spill nếu hết memory) | CPU + disk | map task |
| Ghi shuffle file | disk write | map task |
| Fetch block | network + disk read | reduce task |
| Deserialize + merge/aggregate (+ spill) | CPU + disk | reduce task |

> Câu thần chú Module 3: **shuffle rẻ nhất là shuffle không xảy ra; shuffle tốt nhì là shuffle nhỏ (lọc/gom sớm); shuffle tệ nhất là shuffle có spill.**

---

## 4. Internal

Đi sâu một tầng nữa — chuyện gì xảy ra bên trong một map task khi bạn chạy `groupBy`:

```
① Record đi ra khỏi các operator trước đó (filter, project...)
        │
② Tính reduce partition: pid = hash(key) % 200
        │
③ Đưa vào cấu trúc in-memory trong EXECUTION MEMORY:
   • Có aggregation (groupBy)  → map (bảng băm) vừa gom vừa chứa
   • Không aggregation (repartition/join input) → buffer + sort theo pid
        │
④ Buffer chạm ngưỡng → xin thêm memory từ TaskMemoryManager
   → không được cấp? SPILL: sort phần hiện có, ghi spill file, reset
        │
⑤ Hết input → merge spill files + in-memory data (merge-sort theo pid)
   → ghi 1 file .data (từng block nén + serialize) + 1 file .index
        │
⑥ Task báo cáo MapStatus về driver: "file của tôi ở executor X,
   block cho từng partition to bao nhiêu" → driver giữ trong MapOutputTracker
        │
⑦ Reduce task khởi động → hỏi MapOutputTracker vị trí block
   → ShuffleBlockFetcherIterator kéo song song nhiều block
     (tối đa spark.reducer.maxSizeInFlight = 48m đang bay cùng lúc)
   → block local đọc thẳng disk, block remote qua Netty
        │
⑧ Reduce side aggregate/sort — lại dùng execution memory, lại có thể spill
```

Ghi nhớ vài cái tên để đọc log/stack trace không hoang mang:

- **`ExternalSorter` / `ExternalAppendOnlyMap`**: các cấu trúc "ưu tiên memory, tràn thì spill" — thấy tên này trong log `INFO ExternalSorter: Task 12 force spilling in-memory map to disk` nghĩa là đang spill.
- **`MapOutputTracker`**: sổ cái trên driver ghi vị trí mọi shuffle block.
- **`ShuffleBlockFetcherIterator`**: bên reduce, kéo block về; lỗi `FetchFailedException` nghĩa là không lấy được block (executor nguồn chết, disk hỏng, network timeout) → Spark phải **chạy lại map stage** để tạo lại block — đây là lý do job "chạy lại stage cũ" một cách bí ẩn.
- Shuffle file nằm dưới `spark.local.dir` (mặc định `/tmp`), trong các thư mục `blockmgr-*`. Production luôn trỏ `spark.local.dir` vào disk nhanh và to (SSD/NVMe) — shuffle nặng mà local dir là disk bèo thì tuning gì cũng vô ích.

Một chi tiết hay bị hỏi phỏng vấn: **partial aggregation (map-side combine)**. Với `groupBy().agg(sum/count/...)`, Spark gom trước tại map side nên shuffle write bé. Nhưng `groupBy().agg(collect_list(...))` hay `distinct` trên key gần-unique thì gom trước chẳng giảm được gì — shuffle write ≈ toàn bộ dữ liệu. Cùng cú pháp `groupBy`, chi phí khác nhau một trời một vực.

---

## 5. API

Shuffle không có "API" trực tiếp — bạn điều khiển nó qua config và quan sát qua plan/UI.

### `spark.sql.shuffle.partitions`

```python
spark.conf.set("spark.sql.shuffle.partitions", "64")   # đổi được giữa chừng, ảnh hưởng shuffle SAU đó
```
- **Ý nghĩa**: số reduce partition của mọi shuffle trong Spark SQL/DataFrame. Mặc định 200 — quá to cho data bé (200 task tí hon, overhead scheduling), quá bé cho data khổng lồ (mỗi task ôm quá nhiều → spill/OOM).
- **Pitfall**: đây là con dao chỉnh tuning số 1 nhưng là giá trị TOÀN CỤC — Spark 3 có AQE coalesce (lesson 20) tự gom bớt partition thừa, nhưng bạn vẫn phải hiểu gốc.

### `df.explain()` — tìm chữ `Exchange`

```python
df.groupBy("customer_state").count().explain()
# == Physical Plan ==
# ... +- Exchange hashpartitioning(customer_state#24, 200), ENSURE_REQUIREMENTS ...
```
- **`Exchange` = shuffle**. Đếm số `Exchange` trong plan = đếm số shuffle. Thói quen senior: explain trước khi chạy job to.

### Config shuffle đáng biết (đọc hiểu, chưa cần thuộc)

```python
# nén shuffle output (mặc định true — đừng tắt)
"spark.shuffle.compress": "true"
# buffer ghi file của mỗi task — tăng nhẹ (64k→1m) giảm số lần chạm disk
"spark.shuffle.file.buffer": "32k"
# lượng dữ liệu 1 reducer được kéo "đang bay" cùng lúc
"spark.reducer.maxSizeInFlight": "48m"
# nơi đặt shuffle file + spill — production trỏ vào SSD
"spark.local.dir": "/tmp"
# bật external shuffle service (cần service chạy sẵn trên worker)
"spark.shuffle.service.enabled": "false"
```

- **Pitfall**: junior thấy job chậm là gán ngay `spark.shuffle.*` linh tinh theo blog. Thứ tự đúng: nhìn UI xác định *có spill không, shuffle to không* → giảm dữ liệu vào shuffle / chỉnh số partition → cuối cùng mới đến config vi mô.

---

## 6. Demo nhỏ

```
Input:  DataFrame nhỏ tạo tay
   ↓    groupBy(state) (transformation — sẽ sinh Exchange)
Output: explain() để NHÌN THẤY shuffle trước khi chạy + count() để chạy thật
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("demo15")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

data = [("SP", 120.0), ("RJ", 80.0), ("SP", 300.0), ("MG", 150.0), ("RJ", 500.0)] * 1000
df = spark.createDataFrame(data, ["state", "amount"])

agg = df.groupBy("state").agg(F.sum("amount").alias("total"))

agg.explain()          # tìm dòng: Exchange hashpartitioning(state, 4)
agg.count()            # chạy thật — sinh 2 stage

input("Mở http://localhost:4040 → tab Stages → xem cột Shuffle Write của stage 1 "
      "và Shuffle Read của stage 2 (2 số này phải ≈ bằng nhau!). Enter để thoát...")
spark.stop()
```

Quan sát cần rút ra: (a) plan có đúng 1 `Exchange`; (b) job có 2 stage; (c) **Shuffle Write của stage trước = Shuffle Read của stage sau** — dữ liệu ghi ra bao nhiêu thì đọc vào bấy nhiêu; (d) shuffle write rất bé so với input vì partial aggregation đã gom 5.000 dòng thành vài dòng/state ngay tại map side.

---

## 7. Production Example

Tình huống thật (mô-típ gặp ở mọi công ty): pipeline gold layer join bảng `events` 2 TB/ngày với `users` 50 GB rồi `groupBy user_id` tính metric. Job chạy 3 tiếng, đội muốn xuống dưới 1 tiếng.

Kỹ sư senior mở Spark UI của lần chạy gần nhất, tab Stages, sort theo Duration:

```
Stage 7 (sort-merge join):  Shuffle Read 2.1 TB │ Spill (Disk) 3.8 TB  ← !!!
Stage 9 (groupBy):          Shuffle Read 400 GB │ Spill (Disk) 0
```

Chẩn đoán: Stage 7 spill **nhiều hơn cả shuffle read** — mỗi task ôm ~10 GB (2.1 TB / 200 partition mặc định) trong khi execution memory mỗi task chỉ ~2 GB → sort đi sort lại qua disk nhiều vòng. Xử lý theo đúng thứ tự ưu tiên:

1. **Giảm dữ liệu vào shuffle**: phát hiện `events` có 40% cột không dùng và 30% event type không cần — thêm `select` + `filter` trước join. Shuffle read còn 900 GB. (Miễn phí, hiệu quả nhất.)
2. **Tăng số shuffle partition**: 200 → 2000, mỗi task còn ~450 MB → hết spill. (Đây chính là bài "tăng partitions cứu memory" — lesson 17 giải thích bản chất.)
3. Cân nhắc bật lại dynamic allocation → phải bật **external shuffle service** trên YARN trước, nếu không executor bị thu hồi sẽ kéo theo mất shuffle file và stage bị chạy lại lòng vòng.

Kết quả: 3h → 40 phút, **không thêm một GB tài nguyên nào**. Toàn bộ chẩn đoán chỉ dùng đúng các metric bạn học hôm nay.

---

## 8. Hands-on Lab

**Mục tiêu**: tự tay tạo một job shuffle nặng trên cluster Docker (worker 1G/1core — memory thấp là LỢI THẾ để demo spill), quan sát Shuffle Write/Read/Spill trên UI, sờ tận tay shuffle file trên disk.

### Bước 0 — bật cluster

```bash
make up          # master UI: http://localhost:8080, app UI khi chạy: http://localhost:4040
```

Dataset Olist chỉ ~120 MB — quá bé để shuffle "đau". Ta sẽ **thổi phồng** `order_items` bằng `explode` để tạo shuffle đáng kể.

### Bước 1 — viết `labs/lab15/shuffle_spill.py`

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("lab15-shuffle-spill")
         .config("spark.executor.memory", "512m")        # ép memory thấp để dễ spill
         .config("spark.sql.shuffle.partitions", "4")     # ít partition → task to → spill
         .config("spark.sql.adaptive.enabled", "false")   # tắt AQE để thấy shuffle "trần trụi"
         .getOrCreate())

items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv",
                       header=True, inferSchema=True)

# Thổi phồng ~113k dòng × 40 = ~4.5 triệu dòng, thêm cột "nặng" để record to
big = (items
       .withColumn("n", F.explode(F.sequence(F.lit(1), F.lit(40))))
       .withColumn("payload", F.sha2(F.concat_ws("-", "order_id", "n"), 256)))

# groupBy trên key gần-unique + collect_list => partial agg vô dụng => shuffle to thật sự
t0 = time.time()
result = (big.groupBy("order_id")
             .agg(F.count("*").alias("cnt"),
                  F.collect_list("payload").alias("payloads")))
print(f"Số nhóm: {result.count():,} | thời gian: {time.time() - t0:.1f}s")

input(">>> GIỮ NGUYÊN terminal này. Mở http://localhost:4040 → Stages. Enter để thoát.")
spark.stop()
```

### Bước 2 — chạy và quan sát UI (phần quan trọng nhất)

```bash
make run F=labs/lab15/shuffle_spill.py
```

Khi script dừng ở `input()`, mở `http://localhost:4040` → tab **Stages**, click vào stage của `count`:

1. Stage map side: cột **Shuffle Write** — bao nhiêu MB, bao nhiêu record?
2. Stage reduce side: **Shuffle Read** — có bằng Shuffle Write không?
3. Tìm **Spill (Memory)** và **Spill (Disk)** — nếu > 0, bạn vừa tự tay tạo spill. So sánh 2 số: memory lớn hơn disk mấy lần? (Đó là hệ số serialize + nén.)
4. Mở bảng task bên dưới (Event Timeline + Tasks): thời gian từng task, cột Spill của từng task.

### Bước 3 — sờ shuffle file thật

Trong lúc job đang dừng ở `input()`:

```bash
docker exec spark-mastery-spark-worker-1 sh -c \
  'find /tmp -path "*blockmgr*" -name "*.data" -o -path "*blockmgr*" -name "*.index" | head -20'
```

Bạn sẽ thấy các cặp `shuffle_*.data` / `shuffle_*.index` — chính là thứ ở sơ đồ mục 3.2.

### Bước 4 — thí nghiệm đối chứng: xóa spill

Copy file thành `labs/lab15/shuffle_no_spill.py`, chỉ đổi `spark.sql.shuffle.partitions` từ `4` lên `64`, chạy lại, ghi vào `labs/lab15/NOTES.md`:

| | partitions=4 | partitions=64 |
|---|---|---|
| Thời gian | ? | ? |
| Spill (Disk) | ? | ? |
| Shuffle Write | ? | ? |

Câu hỏi tự trả lời: Shuffle Write có đổi không (gợi ý: không — tổng dữ liệu qua shuffle y nguyên), vậy thứ gì đã thay đổi khiến job nhanh hơn?

---

## 9. Assignment

**Easy** — Ước lượng kích thước shuffle bằng tay: với `big` ở lab (≈4.5 triệu record, mỗi record gồm `order_id` ~32 byte + payload sha2 64 byte + vài cột số), tính gần đúng tổng shuffle write theo công thức `#records × record size`. So với con số Spark UI báo. Chênh lệch do đâu? (Gợi ý: nén + serialize + partial agg.)

**Medium** — Truy tìm config quản shuffle memory: giải thích bằng chữ của bạn vai trò của `spark.memory.fraction`, `spark.shuffle.file.buffer`, `spark.reducer.maxSizeInFlight`, và tại sao Spark hiện đại KHÔNG còn config kiểu `spark.shuffle.memoryFraction` riêng (gợi ý: unified memory — sẽ học kỹ ở lesson 17; assignment này là "học trước một nhịp").

**Hard** — Cố tình gây spill và định lượng thiệt hại: từ lab bước 4, chạy 3 cấu hình `partitions = 2, 8, 64` (giữ nguyên data). Lập bảng thời gian + Spill (Disk) + thời gian task max. Kết luận: spill làm chậm gấp mấy lần, và spill biến mất ở ngưỡng partition nào? Vẽ (ASCII hoặc chữ) quan hệ "kích thước task vs execution memory".

**Production Challenge** — Viết một **runbook nửa trang** cho team: "Khi thấy job chậm nghi do shuffle, kiểm tra theo thứ tự nào?" Yêu cầu có: 3 metric phải nhìn đầu tiên trên Stages tab, ngưỡng nào coi là báo động (ví dụ spill > 0, shuffle read/task > X), 3 hành động xử lý theo thứ tự ưu tiên (giảm data → chỉnh partition → mới đến memory). Runbook này bạn sẽ dùng lại ở Project 3 tuần 11.

> Nộp bài bằng cách paste code + số liệu + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Hiện tượng | Đắt cỡ nào | Vì sao |
|---|---|---|
| Shuffle bình thường (không spill) | Chuẩn phí: serialize + 1 lần ghi disk + network + deserialize | Thiết kế cố ý — chấp nhận được |
| Spill map side | Mỗi byte bị sort + ghi + đọc + merge **nhiều lần** | Execution memory < dữ liệu task phải sort |
| Spill reduce side | Như trên, cộng thêm sau khi đã tốn network | Reduce partition quá to (ít partition / skew) |
| `collect_list` / `distinct` key gần-unique | Shuffle write ≈ toàn bộ data | Partial aggregation không gom được gì |
| `FetchFailedException` | Chạy lại cả map stage | Mất shuffle file (executor chết, không có ESS) |

Chiến lược giảm chi phí, theo thứ tự hiệu quả:

1. **Đừng shuffle thứ không cần**: `select` cột cần thiết + `filter` càng sớm càng tốt — mỗi cột thừa là byte thừa qua đủ 4 khâu phí.
2. **Chọn số shuffle partition sao cho mỗi task nhai ~100–200 MB** — quy tắc ngón tay cái, chi tiết ở lesson 16–17.
3. **Tránh shuffle hoàn toàn khi được**: broadcast join cho bảng bé (lesson 9), ghi bucketed table cho join lặp lại.
4. Spill dai dẳng dù đã tăng partition → nghi ngờ **skew** (lesson 19).

---

## 11. Spark UI

Bài này "mở khóa" toàn bộ sức mạnh của tab **Stages** — tab quan trọng nhất với performance engineer:

**Trang danh sách Stages** — nhìn gì:
- Cột **Shuffle Read / Shuffle Write** của từng stage: bản đồ "dữ liệu chảy qua đâu nhiều nhất". Stage có shuffle read to nhất = nơi bắt đầu điều tra.
- Hai stage liên tiếp: Write của stage trước ≈ Read của stage sau.

**Trang chi tiết 1 stage** — đọc gì:
- **Summary Metrics (min / 25th / median / 75th / max)**: hàng `Shuffle Read Size` mà max >> median → skew (lesson 19). Hàng `Spill (Memory/Disk)` xuất hiện → task không đủ execution memory.
- **Aggregated Metrics by Executor**: executor nào gánh nhiều shuffle nhất.
- **Tasks table**: bật thêm cột (nút "Show Additional Metrics") — `Shuffle Read Blocked Time` cao nghĩa là task ngồi CHỜ network/disk của bên kia, không phải thiếu CPU.

Checklist đọc-stage-trong-30-giây của senior: *Duration → Shuffle Read/Write → Spill → max/median task*. Bốn liếc mắt đó trả lời được 80% câu "job này sao chậm".

---

## 12. Common Mistakes

1. **Tưởng shuffle chỉ là network.** Quên serialize + disk ở cả 2 đầu → tối ưu sai chỗ (ví dụ đòi nâng cấp network trong khi nghẽn là spill trên disk `/tmp` bèo).
2. **Không phân biệt shuffle write (cố ý) với spill (ngoài kế hoạch).** Thấy "ghi disk" là hoảng. Shuffle write luôn có; spill mới là mùi khét.
3. **Để `spark.sql.shuffle.partitions=200` mặc định cho mọi job** — data 10 GB hay 10 TB cũng 200. Job to thì spill, job bé thì 200 task tí hon toàn overhead.
4. **`groupBy().agg(collect_list(...))` trên key to** rồi ngạc nhiên shuffle khổng lồ — partial aggregation không cứu được collect_list.
5. **Bật dynamic allocation mà quên external shuffle service** (trên YARN) → executor bị thu hồi kéo theo shuffle file → `FetchFailedException` → stage chạy lại lòng vòng, job "lúc nhanh lúc chậm" bí ẩn.
6. **Đo job bằng tổng thời gian mà không mở Stages tab.** Hai lần chạy cùng 30 phút: một lần shuffle sạch, một lần spill 500 GB đang chờ nổ khi data tăng 20% — nhìn ngoài giống hệt nhau.

---

## 13. Interview

**Junior:**

1. *Shuffle là gì, khi nào xảy ra?* — Là việc phân phối lại dữ liệu giữa các executor sao cho các record cùng key về cùng partition. Xảy ra ở wide transformation: `groupBy`, `join` (trừ broadcast), `distinct`, `repartition`, window theo partition key. Shuffle là ranh giới cắt stage.
2. *Shuffle gồm mấy phase? Mô tả ngắn.* — 2 phase. Map side write: task sort output theo reduce partition id, ghi 1 file data + 1 file index xuống local disk. Reduce side read: task hỏi driver vị trí block rồi fetch phần của mình từ các executor qua network, merge/aggregate tiếp.
3. *Spill là gì? Có phải lỗi không?* — Là đổ dữ liệu trung gian xuống disk khi execution memory hết trong lúc sort/aggregate. Không phải lỗi — là cơ chế chống OOM — nhưng là tín hiệu performance xấu vì dữ liệu bị ghi/đọc/merge nhiều lần.
4. *Trên Spark UI, nhìn đâu để biết job shuffle nhiều?* — Tab Stages: cột Shuffle Read/Write từng stage; vào chi tiết stage xem Summary Metrics và Spill (Memory/Disk).

**Mid:**

5. *Tại sao shuffle write phải ghi xuống disk kể cả khi memory dư?* — (a) Fault tolerance: reducer fail chỉ cần fetch lại file, không phải chạy lại mapper; (b) decoupling 2 stage: mapper xong là xong, reducer đến lấy sau, không cần 2 bên cùng sống cùng lúc; (c) map output của mọi mapper phải tồn tại đầy đủ trước khi reducer bắt đầu.
6. *Sort-based shuffle giải quyết vấn đề gì của hash-based shuffle?* — Hash-based ghi mỗi (mapper × reducer) 1 file → M×R file, nổ file descriptor và random I/O với job lớn. Sort-based sort output theo partition id, ghi 1 data file + 1 index file mỗi mapper → 2M file; reducer dùng index để seek đúng block.
7. *Shuffle Write của stage A là 100 GB, Shuffle Read của stage B (ngay sau) là bao nhiêu? Spill thì sao?* — Read ≈ 100 GB (cùng một khối dữ liệu, đầu ghi đầu đọc). Spill thì KHÔNG có quan hệ cố định — có thể 0 (đủ memory) hoặc lớn hơn cả shuffle size nhiều lần (sort nhiều vòng); spill phụ thuộc memory per task, không phụ thuộc tổng shuffle.
8. *External shuffle service để làm gì, khi nào bắt buộc?* — Process độc lập trên worker node phục vụ shuffle file thay executor, để executor chết/bị thu hồi không làm mất map output. Bắt buộc khi bật dynamic allocation trên YARN/standalone; trên K8s dùng shuffleTracking hoặc remote shuffle service thay thế.

**Senior:**

9. *Job có spill nặng. Trình bày cây quyết định xử lý của bạn.* — (1) Xác định spill ở stage nào, map hay reduce side, đều các task hay dồn 1 task (mở Summary Metrics: max vs median). (2) Nếu dồn 1 task → skew, xử lý bằng salting/AQE skew join chứ tăng memory vô ích. (3) Nếu đều → giảm dữ liệu vào shuffle (prune cột, filter sớm, tránh collect_list) rồi tăng số shuffle partition để giảm data/task. (4) Vẫn spill → mới cân nhắc tăng executor memory hoặc giảm số core/executor (tăng memory per task). Tăng memory là bước CUỐI, không phải đầu — vì nó tốn tiền và thường chỉ che triệu chứng.
10. *Tại sao `groupBy("state").count()` shuffle rất bé còn `groupBy("order_id").agg(collect_list(...))` shuffle khổng lồ, dù cú pháp giống nhau?* — Khác nhau ở partial aggregation tại map side. Với count theo state (ít key, agg gom được): mapper gom hàng triệu dòng còn vài chục record trước khi shuffle. Với collect_list theo key gần-unique: không gom được gì (mỗi key ~1 record, và collect_list phải giữ nguyên mọi phần tử) → toàn bộ dữ liệu đi qua shuffle. Bài học: chi phí shuffle = f(số key, loại aggregate), không đọc được từ cú pháp.

---

## 14. Summary

### Mindmap

```
                        SHUFFLE INTERNALS (L15)
                                │
     ┌──────────────┬───────────┴───────────┬──────────────────┐
     ▼              ▼                       ▼                  ▼
  2 PHASE       SHUFFLE FILES            SPILL             QUAN SÁT
     │              │                       │                  │
  map write:    1 .data + 1 .index      execution mem hết   Stages tab:
  sort theo     mỗi map task            → sort → ghi tạm    Shuffle R/W
  partition id  (sort-based,            → merge cuối        Spill (Mem/Disk)
  → local disk  thay hash-based         cả 2 phía map/reduce max vs median
  reduce read:  M×R files)              chậm vì đọc/ghi     Exchange trong
  fetch block   ESS: sống sót khi       nhiều lần           explain()
  qua network   executor chết          KHÔNG phải lỗi
                                        nhưng là mùi khét
          CHI PHÍ THẬT = serialize + disk + network + deserialize
```

### Checklist trước khi gõ "Continue"

- [ ] Vẽ lại sơ đồ shuffle 2 phase, chỉ đúng chỗ disk và chỗ network.
- [ ] Giải thích được shuffle file gồm gì (data + index) và tại sao sort-based thắng hash-based.
- [ ] Phân biệt rạch ròi: shuffle write (cố ý, luôn có) vs spill (ngoài kế hoạch, tín hiệu xấu).
- [ ] Đã tự tay tạo spill trong lab với executor 512MB và xóa nó bằng cách tăng shuffle partitions.
- [ ] Đọc được Shuffle Write/Read/Spill trên Stages tab và biết Write stage trước ≈ Read stage sau.
- [ ] Nói được external shuffle service để làm gì và khi nào bắt buộc.
- [ ] Trả lời được 10 câu interview mà không xem đáp án.

---

## 15. Next Lesson

**Lesson 16 — Partitioning chiến lược: repartition vs coalesce.**

Hôm nay bạn thấy số shuffle partition quyết định sống chết: 4 partition thì spill, 64 thì êm. Nhưng đổi số partition cũng có giá riêng — `repartition` chính là một shuffle trọn gói, còn `coalesce` rẻ hơn nhưng có cái bẫy nổi tiếng khiến cả job co về 1 task mà không ai hay. Lesson 16 cho bạn bảng quyết định dứt khoát: khi nào repartition, khi nào coalesce, và cách khống chế số lượng + kích thước file khi ghi bảng partitioned — kỹ năng mà mọi data engineer ghi lakehouse hàng ngày đều phải có.

Bạn vừa hiểu shuffle đắt thế nào — giờ học cách chi tiêu nó một cách chiến lược.

> Gõ **"Continue"** khi sẵn sàng.
