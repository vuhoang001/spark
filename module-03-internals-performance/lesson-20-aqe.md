# Lesson 20 — AQE (Adaptive Query Execution): Spark tự sửa plan giữa chừng

> Module 3 · Internals & Performance Tuning · Tuần 10 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 2.5–3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao plan tĩnh của Catalyst — dù tối ưu công phu — vẫn **sai thường xuyên**: vì nó dựa trên ƯỚC LƯỢNG.
- AQE (Adaptive Query Execution) là gì: **replan giữa chừng** dựa trên số liệu THẬT thu được sau mỗi shuffle stage.
- 3 tuyệt chiêu của AQE, kèm diagram từng cái: **coalesce shuffle partitions**, **switch join strategy** (sort-merge → broadcast), **skew join optimization** (nối tiếp lesson 19).
- Bộ config `spark.sql.adaptive.*` — cái nào chỉnh, cái nào để yên.
- Đọc explain/UI khi AQE bật: `AdaptiveSparkPlan`, `isFinalPlan`, final plan trong SQL tab.
- **AQE không fix được gì** — danh sách này quan trọng ngang danh sách nó fix được.

Sau bài này bạn phải làm được:

- Giải thích cho team vì sao từ Spark 3.2 gần như không ai phải chỉnh `spark.sql.shuffle.partitions` thủ công nữa.
- Nhìn explain output có `AdaptiveSparkPlan isFinalPlan=false` và biết phải đi đâu xem plan thật.
- Phân xử: job chậm này AQE cứu được không, hay phải tự tay sửa (UDF, skew logic, small files)?

Kiến thức dùng trong thực tế: AQE bật mặc định từ Spark 3.2 — tức là **mọi job bạn chạy đều đang được AQE can thiệp**. Không hiểu nó, bạn sẽ đọc sai UI, đổ oan cho code, và không giải thích được vì sao "số partition tôi set 200 mà UI hiện 4".

---

## 2. Why

### Vấn đề: plan tĩnh xây trên cát

Lesson 13 bạn đã học Catalyst Optimizer: nhận query → logical plan → tối ưu → physical plan. Nhưng để CHỌN physical plan (join kiểu gì? mấy partition?), Catalyst phải trả lời "bảng này to bao nhiêu?" — và nó chỉ có thể **ước lượng**:

- Kích thước file trên đĩa (nhưng nén Parquet 10× thì sao? decode ra to gấp mấy?).
- Statistics từ `ANALYZE TABLE` (mấy ai chạy? có chạy thì cũng stale sau ETL đêm qua).
- Selectivity của filter: `WHERE status = 'delivered'` giữ lại bao nhiêu %? Catalyst đoán mò.

```
Catalyst lúc lập plan (chưa chạy):              Thực tế lúc chạy:

  orders (ước tính 2 GB sau filter)               filter giữ 0.9% → 18 MB!
      → "2 GB > 10 MB, không broadcast được"          → 18 MB thừa sức broadcast
      → chọn SORT-MERGE JOIN                          → nhưng plan đã đúc, cứ thế
      → shuffle 2 bảng, sort, merge                     shuffle cả bảng lớn 50 GB
                                                        để join với... 18 MB. Đau.
```

Sai lầm kiểu này KHÔNG phải bug — với thông tin tại thời điểm lập plan, quyết định đó hợp lý. Vấn đề nằm ở kiến trúc: **plan đúc một lần trước khi chạy, không được sửa**.

### Ý tưởng AQE: chờ có số thật rồi mới quyết

Spark có một điểm dừng tự nhiên: **shuffle**. Mỗi stage phải ghi shuffle output xuống đĩa xong thì stage sau mới đọc (lesson 15). Tại điểm dừng đó, driver biết CHÍNH XÁC: mỗi partition bao nhiêu bytes, bao nhiêu dòng. AQE tận dụng: *đã dừng rồi thì tranh thủ nhìn số thật, sửa plan phần còn lại*.

> **Analogy dẫn đường**: Plan tĩnh là in bản đồ giấy từ nhà — kẹt xe ở cầu thì vẫn cứ đâm vào vì "bản đồ bảo đi lối này". AQE là Google Maps: đến mỗi ngã tư (shuffle boundary) nó nhìn giao thông THẬT rồi mới chọn đoạn tiếp theo. Không thể sửa đoạn đã đi qua, nhưng đoạn chưa đi thì luôn được cập nhật.

### Trade-off (Senior phải thuộc)

| Được | Mất |
|---|---|
| Bớt phụ thuộc `shuffle.partitions` chỉnh tay | Plan trong explain trước khi chạy KHÔNG phải plan cuối — đọc UI phải biết chỗ |
| Tự cứu join estimate sai, skew join | Chỉ can thiệp được TẠI shuffle boundary — query không shuffle thì AQE đứng ngoài |
| Bật mặc định, đa số job hưởng lợi miễn phí | Chút overhead lập plan lại + benchmark cũ chạy 2 lần có thể ra 2 plan khác nhau (khó so sánh) |
| Ngưỡng chỉnh được theo cluster | Tạo ảo giác "khỏi cần tuning" — sai, xem section "AQE bó tay" |

---

## 3. Theory

### 3.1. Thuật ngữ nền

| Thuật ngữ | Nghĩa |
|---|---|
| **Runtime statistics** | Số liệu THẬT sau khi stage chạy xong: bytes/rows của từng shuffle partition (map output statistics). |
| **Query stage** | Đơn vị AQE: một khúc plan kết thúc bằng shuffle (hoặc broadcast) — chạy xong mới materialize số liệu. |
| **Replan / re-optimize** | Lấy plan phần chưa chạy + số liệu mới → cho Catalyst tối ưu lại → plan mới. |
| **`AdaptiveSparkPlan`** | Node gốc trong explain khi AQE bật — cái vỏ bao lấy plan có thể thay đổi. |
| **Advisory size** | Kích thước partition "mong muốn" mà AQE nhắm tới khi gộp/chẻ (không phải cứng). |

### 3.2. Vòng đời một query dưới AQE

```
   Catalyst tạo plan ban đầu (vẫn như xưa)
        │
        ▼
   Cắt plan thành QUERY STAGES tại các shuffle boundary
        │
        ▼
   ┌─→ Chạy các stage lá (chưa phụ thuộc ai)
   │        │
   │        ▼
   │   Stage xong → driver thu MAP OUTPUT STATISTICS (size/rows từng partition)
   │        │
   │        ▼
   │   RE-OPTIMIZE phần plan còn lại với số thật:
   │     • partition bé lắt nhắt?  → gộp (coalesce)
   │     • một bên join hoá ra nhỏ? → đổi sang broadcast
   │     • partition lệch?          → chẻ (skew split)
   │        │
   └────────┘  lặp đến khi hết stage → FINAL PLAN
```

Chú ý chữ "phần plan còn lại": stage đã chạy là ván đã đóng thuyền. AQE chỉ thông minh dần **về phía trước**.

### 3.3. Tuyệt chiêu 1 — Coalesce shuffle partitions

Vấn đề muôn thuở: `spark.sql.shuffle.partitions` mặc định 200 cho MỌI shuffle, bất kể dữ liệu 2 MB hay 2 TB. Dữ liệu nhỏ + 200 partition = 200 task tí hon, overhead scheduling > thời gian làm việc thật (và nếu ghi ra luôn thì 200 file nhỏ — lesson 21 sẽ chửi tiếp).

```
Không AQE (shuffle.partitions = 200, dữ liệu thật 320 MB):

  [1.6MB][1.6MB][1.6MB] ... 200 partition còm cõi ... [1.6MB]
    ↓      ↓      ↓                                      ↓
  200 task, mỗi task làm 0.01s, tốn 0.05s overhead — lỗ vốn

AQE (advisoryPartitionSizeInBytes = 64MB):

  [1.6MB][1.6MB]...[1.6MB]  ← vẫn shuffle write 200 mảnh như cũ
   └──────── gộp các mảnh LIỀN KỀ đến khi ~64MB ────────┘
                    ↓
  [64MB]  [64MB]  [64MB]  [64MB]  [64MB]  → 5 task đầy đặn
```

Điểm tinh tế: AQE **không shuffle lại** — nó chỉ bảo reducer "mày đọc gộp các mảnh 0–39, mày đọc 40–79...". Gộp là phép đọc, miễn phí. Vì vậy chiến lược hiện đại: set `shuffle.partitions` (hoặc `initialPartitionNum`) CAO một cách hào phóng, để AQE gộp xuống — cao dư thì gộp được, thấp thiếu thì không chẻ được (trừ khi skew).

### 3.4. Tuyệt chiêu 2 — Dynamically switch join strategy

```
Plan ban đầu (estimate: cả 2 bảng lớn):          Sau khi stage filter chạy xong:

     SortMergeJoin                                  runtime stats: nhánh phải = 18 MB!
      /         \                                        │
  shuffle     shuffle                                    ▼
    |            |                                 BroadcastHashJoin
  orders      filter(events)  ← ước 2GB             /         \
  (50 GB)                                        orders      broadcast(18 MB)
                                                 (né shuffle + sort bảng 50 GB)
```

Khi stage con chạy xong và AQE thấy một bên < `spark.sql.adaptive.autoBroadcastJoinThreshold`, nó đổi sort-merge → broadcast hash join. Có một chi tiết nhà nghề: nếu bảng lớn ĐÃ shuffle write mất rồi (stage đã chạy) thì sao? AQE dùng **local shuffle reader** (`AQEShuffleRead local`): reducer đọc lại mảnh shuffle ngay trên node mapper, khỏi kéo qua network — vớt vát được phần lớn chi phí.

### 3.5. Tuyệt chiêu 3 — Skew join optimization

Ôn nhanh lesson 19, giờ nhìn từ phía AQE:

```
Map output statistics sau shuffle:
  p0: 60MB   p1: 58MB   p2: 61MB   p3: 950MB ← > 5× median VÀ > threshold → SKEW
                                     │
                                     ▼ chẻ p3 thành 4 mảnh-đọc (theo dải mapper output)
  Bên trái:  p3a(240MB)  p3b(240MB)  p3c(240MB)  p3d(230MB)   → 4 task song song
  Bên phải:  p3 được ĐỌC LẶP LẠI 4 lần (mỗi task một bản)
```

Điều kiện AND: `skewedPartitionFactor` (mặc định 5× median) **và** `skewedPartitionThresholdInBytes` (mặc định 256 MB). Áp dụng cho sort-merge join và shuffled hash join. KHÔNG áp dụng cho skew trong `groupBy`/window — nhớ kỹ, đây là câu interview bẫy.

### 3.6. Timeline tính năng

| Spark | AQE |
|---|---|
| 1.6–2.x | Chưa có (có prototype thử nghiệm) |
| 3.0 | Ra mắt đầy đủ 3 tính năng, `adaptive.enabled` mặc định **false** |
| 3.2 | Mặc định **true** — bước ngoặt; thêm tối ưu cho shuffled hash join |
| 3.4 (bản của khóa này) | Ổn định, mặc định bật, thêm nhiều rule nhỏ (empty relation propagation...) |

---

## 4. Internal

Mổ máy sâu hơn một tầng — chuyện gì xảy ra trong driver khi AQE chạy:

```
① Action được gọi → physical plan ban đầu được bọc trong AdaptiveSparkPlan
        │
② AdaptiveSparkPlan duyệt plan từ DƯỚI lên, tìm các Exchange (shuffle) node
   → mỗi Exchange + cây con của nó = 1 QueryStage (ShuffleQueryStage / BroadcastQueryStage)
        │
③ Submit các QueryStage không còn phụ thuộc → chạy như job con bình thường
        │
④ Stage xong → MapOutputTracker trên driver đã có statistics:
   mỗi (mapper, reducer-partition) bao nhiêu bytes — chính metadata
   shuffle của lesson 15, giờ được dùng lại để tối ưu
        │
⑤ Thay QueryStage đã xong bằng "kết quả đã materialize + stats"
   → chạy lại logical optimization trên phần plan còn lại
   → áp các AQE rule vật lý:
        • CoalesceShufflePartitions   (gộp partition liền kề)
        • OptimizeSkewedJoin           (chẻ partition lệch)
        • đổi join strategy nếu stats cho phép (re-plan ra BroadcastHashJoin)
        • OptimizeShuffleWithLocalRead (đọc shuffle tại chỗ khi đổi join)
        │
⑥ Lặp ②→⑤ cho đến khi node gốc materialize → isFinalPlan = true
```

Hai hệ quả thực chiến từ cơ chế này:

1. **UI Jobs tab trông "lạ"**: một query AQE sinh nhiều job con hơn bạn đếm theo action — mỗi query stage submit riêng. Đừng hoảng khi 1 action ra 3–4 job.
2. **Explain trước khi chạy chỉ là plan ứng viên**. `df.explain()` trước action in `AdaptiveSparkPlan isFinalPlan=false` + plan ban đầu. Plan THẬT chỉ tồn tại sau khi chạy — xem ở SQL tab của UI, hoặc gọi lại `explain()` sau action (plan đã cached final).

Chốt một câu cho cơ chế: AQE không phải optimizer mới — nó là **vòng lặp cho Catalyst chạy lại nhiều lần**, mỗi lần với thông tin thật hơn.

---

## 5. API

### 5.1. Công tắc tổng và bộ config đáng nhớ

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")   # 3.2+ mặc định true — biết để TẮT khi cần so sánh
```

| Config (`spark.sql.adaptive.`) | Mặc định 3.4 | Ý nghĩa & khi chỉnh |
|---|---|---|
| `enabled` | true | Công tắc tổng. Tắt khi debug/so sánh plan. |
| `coalescePartitions.enabled` | true | Bật gộp partition nhỏ. |
| `advisoryPartitionSizeInBytes` | 64m | Cỡ partition mục tiêu khi gộp/chẻ. Job nặng memory → giảm; cluster to đọc S3 → tăng 128m. |
| `coalescePartitions.initialPartitionNum` | (= shuffle.partitions) | Số partition khởi điểm TRƯỚC khi gộp — set cao hào phóng thay vì chỉnh shuffle.partitions. |
| `coalescePartitions.minPartitionSize` | 1m | Sàn kích thước khi gộp. |
| `coalescePartitions.parallelismFirst` | true | Ưu tiên tận dụng core hơn đạt advisory size — tắt (false) nếu muốn file/partition chuẩn size. |
| `autoBroadcastJoinThreshold` | (= sql.autoBroadcastJoinThreshold) | Ngưỡng đổi sang broadcast LÚC RUNTIME — đáng tin hơn ngưỡng tĩnh vì đo số thật. |
| `skewJoin.enabled` | true | Bật chẻ skew join. |
| `skewJoin.skewedPartitionFactor` | 5 | Lệch bao nhiêu lần median thì tính là skew. |
| `skewJoin.skewedPartitionThresholdInBytes` | 256m | VÀ phải to hơn ngưỡng này. Lab/cluster nhỏ: hạ xuống. |
| `localShuffleReader.enabled` | true | Đọc shuffle tại chỗ khi join đổi sang broadcast. |

- **Pitfall lớn nhất**: chỉnh `advisoryPartitionSizeInBytes` mà quên `parallelismFirst=true` đang cho phép AQE gộp "non tay" để đủ việc cho core — thấy partition bé hơn advisory đừng vội kêu bug.

### 5.2. Đọc explain khi AQE bật

```python
df = big.join(small_filtered, "key")
df.explain()                 # TRƯỚC action
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false        ← chưa chạy, plan ứng viên
# +- SortMergeJoin [key], [key], Inner       ← có thể sẽ bị đổi!
#    ...

df.collect()                 # chạy thật

df.explain()                 # SAU action — plan đã chốt
# AdaptiveSparkPlan isFinalPlan=true
# +- == Final Plan ==
#    *(2) BroadcastHashJoin ...              ← AQE đã đổi chiến thuật!
#    +- AQEShuffleRead local                 ← và đọc shuffle tại chỗ
#    ...
#    +- == Initial Plan ==
#       SortMergeJoin ...                    ← plan cũ giữ lại cho bạn so sánh
```
- **Khi dùng**: mọi lần điều tra "Spark thực sự làm gì". So `Final Plan` với `Initial Plan` = biết AQE đã cứu gì.

### 5.3. Bật/tắt để đối chứng (pattern lab)

```python
def run(aqe: bool):
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
    t0 = time.time()
    query().collect()          # dựng lại DataFrame SAU khi đổi conf
    return time.time() - t0
```
- **Pitfall**: đổi conf giữa chừng KHÔNG ảnh hưởng DataFrame/plan đã thực thi trước đó; luôn dựng lại query sau khi set conf. Và một số conf chỉ ăn khi tạo session — nhóm `adaptive.*` thì may mắn đổi runtime được.

### 5.4. Kiểm tra AQE đã làm gì bằng số partition thật

```python
out = big.join(small, "key").groupBy("key2").count()
out.collect()
print(out.rdd.getNumPartitions())   # AQE bật: con số THẬT sau coalesce (vd 4)
                                    # AQE tắt: đúng bằng shuffle.partitions (vd 200)
```
- **Pitfall**: gọi `.rdd` TRƯỚC action đôi khi tự kích hoạt thực thi một phần — chỉ dùng chiêu này để quan sát sau khi đã chạy, đừng rắc vào code production.

---

## 6. Demo nhỏ

```
Input:  bảng lớn 5M dòng join bảng "tưởng to hoá nhỏ" (filter còn vài trăm dòng)
   ↓    chạy 2 lần: AQE tắt vs AQE bật
Output: thời gian + explain + số partition — 3 bằng chứng AQE can thiệp
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (SparkSession.builder.appName("demo20-aqe").master("local[2]")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # chặn broadcast TĨNH,
         .getOrCreate())                                        # để AQE runtime tự phát hiện

big   = spark.range(5_000_000).withColumn("key", (F.col("id") % 100_000))
small = (spark.range(100_000).withColumn("key", F.col("id"))
         .withColumn("flag", F.rand())
         .filter(F.col("flag") < 0.003))   # estimate to, thực tế ~300 dòng

def bench(aqe):
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    q = big.join(small, "key").groupBy((F.col("key") % 10).alias("b")).count()
    t0 = time.time(); q.collect(); dt = time.time() - t0
    print(f"AQE={aqe}: {dt:.1f}s, partitions cuối = {q.rdd.getNumPartitions()}")
    q.explain()

bench("false")
bench("true")
input(">>> UI :4040 → SQL tab → mở 2 query, so plan: SortMergeJoin vs BroadcastHashJoin, "
      "tìm node AQEShuffleRead xem 'number of coalesced partitions'. Enter...")
spark.stop()
```

Tự hỏi sau khi chạy: bản AQE join kiểu gì (nhìn Final Plan)? 200 partition bị gộp còn mấy? Vì sao ta phải set `autoBroadcastJoinThreshold=-1` ở config tĩnh mà AQE vẫn broadcast được (gợi ý: hai ngưỡng khác nhau ở 5.1)?

---

## 7. Production Example

Chuyện thật ở các data team khi nâng cấp Spark 2.4 → 3.2+:

**Trước AQE** — một team e-commerce có file `tuning.conf` dài 40 dòng cho MỖI pipeline: pipeline hourly nhỏ set `shuffle.partitions=32`, pipeline daily set `800`, pipeline backfill set `3000`. Mỗi lần dữ liệu tăng trưởng lại lôi ra chỉnh. Ai quên chỉnh → hoặc 3000 task tí hon (job hourly), hoặc 32 task khổng lồ spill tung toé (backfill).

**Sau AQE** — cả 3 pipeline dùng chung một cấu hình:

```
spark.sql.adaptive.enabled                                 true
spark.sql.adaptive.coalescePartitions.initialPartitionNum  2000   # trần hào phóng
spark.sql.adaptive.advisoryPartitionSizeInBytes            128m
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 128m  # cluster vừa, hạ khỏi 256m
```

Job nhỏ: AQE gộp 2000 → 6 partition. Job backfill: giữ gần nguyên 2000. Không ai phải mở file conf nữa khi dữ liệu tăng 3×. Ngoài ra dashboard theo dõi của họ thêm 1 metric: đếm query có `OptimizeSkewedJoin` xuất hiện trong final plan — tăng đột biến nghĩa là dữ liệu nguồn bắt đầu lệch, điều tra NGUỒN thay vì đợi cháy.

Bài học production: AQE biến tuning partition từ việc **per-pipeline thủ công** thành **policy toàn cluster** — nhưng chú ý nó không thay bạn chọn cột partition khi ghi, không gỡ UDF, không gom small files ở nguồn. Ba việc đó vẫn là việc của bạn (và của lesson 21).

---

## 8. Hands-on Lab

**Mục tiêu**: đo đạc 3 tính năng AQE trên cluster Docker, thu bằng chứng từ explain + UI.

### Bước 0 — chuẩn bị

```bash
make up      # master :8080, app UI :4040 khi job chạy
```

Nhớ đặc sản cluster này: worker 1 GB / 1 core — mọi hiệu ứng partition/memory hiện rõ mồn một.

### Bước 1 — `labs/lab20/aqe_coalesce.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (SparkSession.builder.appName("lab20-coalesce")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv",
                        header=True, inferSchema=True)

for aqe in ["false", "true"]:
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    q = (orders.groupBy("customer_id").count()
               .groupBy("count").count())          # 2 tầng shuffle trên dữ liệu NHỎ
    t0 = time.time(); q.collect()
    print(f"AQE={aqe}: {time.time()-t0:.1f}s, partitions={q.rdd.getNumPartitions()}")

input(">>> UI :4040 → Stages: so số task các stage giữa 2 lần chạy. Enter...")
spark.stop()
```

### Bước 2 — `labs/lab20/aqe_switch_join.py`

Tái dựng demo section 6 nhưng dùng Olist: join `order_items` với `products` đã filter còn 1 category hiếm (`fashion_underwear_beach` chẳng hạn). Tĩnh: ép `autoBroadcastJoinThreshold=-1`. Chạy AQE off/on, lưu cả hai explain output ra file bằng `df._sc` — đơn giản nhất: `print` explain và redirect log khi chạy `make run`.

### Bước 3 — `labs/lab20/aqe_skew.py`

Dùng lại `items_skewed` của lab19 (đã có parquet). Chạy join với sellers 3 kịch bản:
1. AQE off — baseline đau khổ.
2. AQE on, threshold mặc định 256m — dự đoán: KHÔNG chẻ (partition lab < 256 MB). Kiểm chứng trong SQL tab.
3. AQE on + `skewJoin.skewedPartitionThresholdInBytes=8m` + `advisoryPartitionSizeInBytes=8m` — dự đoán: chẻ. Đếm `number of skewed partitions` trong node AQEShuffleRead.

### Bước 4 — chạy và ghi nhận

```bash
make run F=labs/lab20/aqe_coalesce.py
make run F=labs/lab20/aqe_switch_join.py
make run F=labs/lab20/aqe_skew.py
```

`labs/lab20/NOTES.md`: bảng 3 tính năng × (bằng chứng explain, bằng chứng UI, thời gian before/after). Thêm mục "một điều làm tôi bất ngờ" — bắt buộc, vì AQE luôn có gì đó bất ngờ (số job con, partition gộp non, plan đổi ngoài dự đoán).

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Vì sao plan tĩnh hay sai? Kể 3 nguồn ước lượng sai của Catalyst.
2. AQE lấy runtime statistics từ đâu, tại thời điểm nào? Vì sao shuffle boundary là điểm can thiệp tự nhiên?
3. `AdaptiveSparkPlan isFinalPlan=false` nghĩa là gì? Muốn xem plan thật thì làm gì (2 cách)?

**Medium** — Từ lab: bật AQE, chạy query có join + groupBy trên Olist, rồi trả lời bằng bằng chứng UI: (a) 1 action sinh mấy job, giải thích từng job là query stage nào; (b) node `AQEShuffleRead` báo bao nhiêu `coalesced partitions`; (c) nếu set `advisoryPartitionSizeInBytes=1m` thì số partition cuối đổi thế nào — dự đoán TRƯỚC, chạy sau, giải thích chênh lệch.

**Hard** — Thiết kế thí nghiệm chứng minh một trường hợp **AQE bó tay**: viết UDF Python làm nghẽn một stage (vd tính toán chuỗi nặng từng dòng), chạy AQE on/off, cho thấy thời gian không đổi đáng kể. Sau đó thay UDF bằng built-in function tương đương, đo lại. Kết luận 5 dòng: ranh giới trách nhiệm giữa AQE và người viết code nằm ở đâu?

**Production Challenge** — Viết `labs/lab20/aqe_policy.md`: đề xuất bộ config `spark.sql.adaptive.*` chuẩn cho 3 loại pipeline của công ty giả định (hourly 500 MB / daily 50 GB / backfill 2 TB) chạy cluster 20 executor × 4 core × 16 GB. Mỗi con số phải kèm 1 dòng lý do. Câu hỏi chốt: có nên set 3 bộ khác nhau, hay 1 bộ chung như Production Example — lập luận.

> Nộp bài bằng cách paste code + số đo + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Tình huống | AQE giúp bao nhiêu | Ghi chú |
|---|---|---|
| Shuffle nhỏ + partitions=200 | Lớn: bỏ 90%+ task overhead, bớt small files khi ghi | Coalesce là tính năng "ăn tiền" thầm lặng nhất |
| Join mà estimate sai (filter mạnh) | Rất lớn: né shuffle + sort bảng lớn | Cần bảng nhỏ THẬT SỰ nhỏ lúc runtime |
| Skew join nặng | Lớn (xem lesson 19) | Nhớ điều kiện AND của 2 ngưỡng |
| Query không shuffle (scan + filter + write) | **Zero** | Không có shuffle boundary = không có chỗ can thiệp |
| Nghẽn vì UDF / small files nguồn / GC | **Zero** | Xem danh sách "bó tay" bên dưới |

### AQE KHÔNG fix được gì — học thuộc

1. **UDF chậm**: AQE tối ưu HÌNH DẠNG plan (partition, join strategy) — không tăng tốc HÀM của bạn. UDF Python serialize từng dòng qua Python worker vẫn y nguyên (lesson 12).
2. **Skew do logic**: skew trong `groupBy`/window; hoặc cross join, hoặc explode làm nổ dòng — skew join optimization chỉ phục vụ join có shuffle 2 phía.
3. **Small files ở NGUỒN**: hàng vạn file nhỏ → hàng vạn task đọc + listing chậm xảy ra TRƯỚC shuffle đầu tiên — AQE chưa kịp có mặt. (Coalesce của AQE chỉ cứu partition SAU shuffle.) Lesson 21 xử.
4. **Thiếu tài nguyên thật**: dữ liệu 2 TB trên 2 executor thì plan đẹp mấy cũng chậm.
5. **Đọc dư dữ liệu**: quên filter pushdown, select *, không partition pruning — AQE không đọc giúp ít đi.

Câu tự vấn: *"chỗ chậm này nằm TRƯỚC hay SAU một shuffle boundary?"* — trước: tự xử; sau: kiểm tra AQE đã làm gì rồi mới ra tay.

---

## 11. Spark UI

Bài này mở khóa cách đọc **SQL tab thời AQE** — từ nay là tab bạn mở ĐẦU TIÊN khi điều tra:

**Tab SQL/DataFrame → click query:**

- Plan graph hiển thị là **final plan** (khác explain trước khi chạy!). Mỗi lần AQE replan, graph này tự cập nhật — query đang chạy mà thấy graph đổi hình là bình thường.
- Node **`AQEShuffleRead`** — nhân chứng số 1, đọc metrics của nó:
  - `number of coalesced partitions`: gộp còn bao nhiêu.
  - `number of skewed partitions` / `... splits`: chẻ skew bao nhiêu mảnh.
  - chữ `local` trong node: local shuffle reader đã kích hoạt (join vừa được đổi sang broadcast).
- So **Initial Plan vs Final Plan**: bấm "Details" cuối trang query — text explain đầy đủ có cả hai. `SortMergeJoin` biến thành `BroadcastHashJoin` là chữ ký của tuyệt chiêu 2.

**Tab Jobs**: 1 action → nhiều job con (mỗi query stage 1 job + job cuối). Cột Description giúp map job ↔ stage. Đừng hoảng — đếm job để hiểu AQE chạy mấy vòng replan.

**Tab Stages**: số task của stage sau shuffle KHÁC `shuffle.partitions` — đó là AQE coalesce. Ghi nhớ phản xạ: "số task lạ = nhìn AQEShuffleRead trước khi nghi ngờ config".

---

## 12. Common Mistakes

1. **Đọc `explain()` trước action rồi tuyên bố "Spark chọn sort-merge join"** — đó là plan ứng viên. Plan thật ở SQL tab / explain sau action. Review PR cũng vậy: đừng cãi nhau dựa trên initial plan.
2. **Benchmark AQE-on vs AQE-off nhưng quên dựng lại DataFrame sau khi đổi conf** → hai lần chạy cùng một plan, kết luận "AQE vô dụng". Conf ăn vào lúc plan được thực thi lần đầu.
3. **Set `shuffle.partitions=8` "cho khớp cluster nhỏ" khi AQE bật** — tự chặt chân: AQE gộp được chứ không chẻ được (ngoài skew). Cứ để trần cao, AQE lo phần giảm.
4. **Chờ AQE cứu job toàn UDF** — replan không tăng tốc code Python của bạn. Profile trước, đổ lỗi sau.
5. **Quên ngưỡng skew 256 MB trên dữ liệu bé** → "em bật skewJoin rồi mà chẳng thấy gì". Hạ threshold theo cỡ partition thực tế.
6. **Tắt AQE vĩnh viễn vì một lần plan đổi gây khó debug** — mất cả rổ tối ưu miễn phí. Cần plan ổn định để so sánh thì tắt CỤC BỘ trong phiên điều tra thôi.
7. **So sánh 2 lần chạy benchmark ra 2 plan khác nhau rồi kết luận lung tung** — AQE phản ứng theo số liệu runtime; dữ liệu đổi thì plan đổi. Benchmark nghiêm túc phải ghi lại plan kèm số đo.

---

## 13. Interview

**Junior:**

1. *AQE là gì, giải quyết vấn đề gì?* — Adaptive Query Execution: cơ chế tối ưu lại physical plan GIỮA lúc chạy, dựa trên thống kê thật (size/rows từng partition) thu được sau mỗi shuffle stage. Giải quyết việc plan tĩnh dựa trên ước lượng sai (kích thước sau filter, nén, stats cũ).
2. *Kể 3 tính năng chính của AQE.* — (a) Coalesce shuffle partitions: gộp partition nhỏ liền kề về cỡ advisory; (b) đổi join strategy runtime: sort-merge → broadcast khi một bên hoá nhỏ; (c) skew join optimization: chẻ partition lệch thành nhiều task, nhân bản partition đối ứng.
3. *AQE bật mặc định từ bản nào?* — Ra mắt Spark 3.0 (mặc định tắt), bật mặc định từ 3.2. Spark 3.4 của khóa: đang bật sẵn trong mọi job.
4. *Vì sao AQE chọn shuffle boundary làm điểm can thiệp?* — Vì shuffle bắt buộc materialize dữ liệu (map output ghi đĩa) trước khi stage sau đọc — điểm dừng tự nhiên, và driver có sẵn thống kê chính xác kích thước từng partition tại đó, sửa plan không phá dở việc đang chạy.

**Mid:**

5. *AQE coalesce partitions hoạt động thế nào — có shuffle lại không?* — Không. Shuffle write vẫn ra đủ N mảnh; AQE chỉ đổi cách ĐỌC: một reducer đọc gộp nhiều mảnh liền kề đến khi đạt ~advisoryPartitionSizeInBytes. Vì là phép đọc nên gần như miễn phí; hệ quả là nên set initialPartitionNum cao rồi để AQE gộp xuống.
6. *Phân biệt `spark.sql.autoBroadcastJoinThreshold` và `spark.sql.adaptive.autoBroadcastJoinThreshold`.* — Cái đầu dùng lúc lập plan tĩnh với kích thước ƯỚC LƯỢNG; cái sau dùng lúc runtime với kích thước THẬT sau khi stage chạy. Có thể tắt static (-1) mà AQE vẫn broadcast ở runtime — và ngưỡng runtime đáng tin hơn.
7. *Local shuffle reader là gì, xuất hiện khi nào?* — Khi AQE đổi sort-merge → broadcast mà bảng lớn đã trót shuffle write, reducer chuyển sang đọc mảnh shuffle ngay trên node mapper (không kéo qua network theo hash nữa) — vớt lại phần lớn chi phí shuffle đã lỡ. Trong plan là `AQEShuffleRead local`.
8. *1 action mà UI hiện 4 job — giải thích?* — AQE cắt plan thành query stages tại shuffle boundary và submit từng stage như job con để thu statistics rồi replan; job cuối chạy phần plan chốt. Số job ≈ số vòng materialize + 1, không còn là "1 action = 1 job" tuyệt đối.

**Senior:**

9. *Với AQE, còn cần chỉnh `spark.sql.shuffle.partitions` không? Chiến lược của anh/chị?* — Vẫn cần nhưng đổi vai: nó (hoặc initialPartitionNum) là TRẦN, không phải con số phải đúng. Chiến lược: đặt trần hào phóng theo cỡ job lớn nhất (vd 2000), đặt advisorySize theo mục tiêu memory/file (64–128m), để AQE gộp xuống cho job nhỏ. Lưu ý parallelismFirst có thể gộp non tay để đủ core; và AQE không chẻ partition to nếu không phải skew join — nên trần thấp vẫn nguy hiểm.
10. *Kể các lớp vấn đề AQE không giải quyết được và cách anh/chị xử từng lớp.* — (a) UDF/logic chậm per-row → thay bằng built-in/pandas UDF, profile Python worker; (b) skew ngoài join (groupBy/window) → salting 2 tầng hoặc thiết kế lại key; (c) small files ở nguồn → compaction, chỉnh writer upstream (AQE chỉ cứu partition sau shuffle, không cứu task đọc file); (d) đọc thừa dữ liệu → pushdown, partition pruning, column pruning; (e) thiếu tài nguyên/GC → resource sizing. Nguyên tắc: AQE tối ưu HÌNH DẠNG plan quanh shuffle; mọi thứ trước shuffle đầu tiên và trong hàm của bạn là việc của bạn.

---

## 14. Summary

### Mindmap

```
                           AQE (L20)
                               │
    ┌──────────────┬───────────┴────────────┬───────────────────┐
    ▼              ▼                        ▼                   ▼
 TẠI SAO       CƠ CHẾ                  3 TUYỆT CHIÊU        GIỚI HẠN
    │              │                        │                   │
 plan tĩnh =   cắt plan thành          1. coalesce          UDF: không cứu
 ước lượng     query stages               (advisory 64m,    skew groupBy: không
 (nén, filter  chạy stage → lấy           đọc gộp, free)    small files nguồn: không
 selectivity,  map output stats        2. SMJ → broadcast   query không shuffle: không
 stats cũ)     → Catalyst chạy LẠI        (+local reader)   đọc thừa data: không
 sai → SMJ oan phần còn lại            3. skew join split      │
    │          isFinalPlan=true           (5× AND 256m)     3.2+ bật mặc định
 shuffle =     khi xong                                     → mọi job ĐANG có AQE
 điểm dừng     UI SQL tab =
 tự nhiên      final plan
```

### Checklist trước khi gõ "Continue"

- [ ] Giải thích được 3 nguồn ước lượng sai của plan tĩnh và vì sao shuffle là điểm replan tự nhiên.
- [ ] Vẽ lại vòng lặp: query stage → stats → re-optimize → final plan.
- [ ] Nói được cơ chế + config chính của cả 3 tính năng (coalesce / switch join / skew split).
- [ ] Biết đọc `AdaptiveSparkPlan`, phân biệt Initial vs Final plan, tìm node `AQEShuffleRead` trong UI.
- [ ] Thuộc danh sách 5 thứ AQE bó tay — và nói được ai phải xử thay (bạn).
- [ ] Đã chạy 3 lab, có bảng bằng chứng explain + UI + thời gian.
- [ ] Trả lời 10 câu interview không nhìn đáp án.

---

## 15. Next Lesson

**Lesson 21 — Small files problem & file layout.**

Section 10 vừa nói AQE bó tay với small files ở nguồn — và đó không phải chuyện hiếm: nó là căn bệnh mãn tính số 1 của mọi data lake. Streaming ghi mỗi phút một nhúm file, partition folder quá mịn, shuffle partitions cao khi ghi... vài tháng sau bảng của bạn có 2 triệu file 40 KB, và một câu `SELECT count(*)` mất 20 phút chỉ để LIỆT KÊ file. Lesson 21 đo đạc tận tay 1000 file nhỏ vs 10 file lớn, rồi xây bộ kỹ năng writer: repartition/coalesce trước khi ghi, maxRecordsPerFile, compaction định kỳ, và nguyên tắc thiết kế file layout (cỡ file 128 MB–1 GB, partition folder, sort trong partition) mà mọi lakehouse tử tế đều tuân theo.

> Gõ **"Continue"** khi sẵn sàng.
