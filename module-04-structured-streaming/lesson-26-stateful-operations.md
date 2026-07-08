# Lesson 26 — Stateful operations: aggregation, dedup, state store

> Module 4 · Structured Streaming · Tuần 13 · Thời lượng: 5–6 giờ (lý thuyết 2.5h, lab 3h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **State** là gì — bộ nhớ mà stream mang theo giữa các micro-batch, và bảng phân loại **stateful vs stateless**.
- **State store**: nằm ở đâu (executor), được bảo hiểm thế nào (checkpoint), và cuộc so găng **HDFSBackedStateStore vs RocksDB** (Spark 3.2+).
- `dropDuplicates` + watermark = dedup có **TTL** — vũ khí chống double từ Kafka at-least-once.
- Giải phẫu state của streaming aggregation (nối tiếp lesson 25).
- **Arbitrary stateful processing**: `applyInPandasWithState` (PySpark 3.4) — tự tay viết sessionization.
- Căn bệnh mạn tính số 1: **state phình to** — nguyên nhân & phác đồ điều trị.
- Giám sát state qua `query.lastProgress["stateOperators"]` — đọc được là chẩn được.

Sau bài này bạn phải làm được:

- Nhìn một query nói ngay: stateful hay stateless, state key là gì, cái gì dọn state.
- Bật RocksDB state store và giải thích cho tech lead khi nào BẮT BUỘC bật.
- Viết vòng lặp monitoring in `numRowsTotal`/`stateMemory` và diễn giải xu hướng.

Kiến thức dùng trong thực tế: sessionization (Project 3 checkpoint 3), lịch sử giao dịch user (Project 4 checkpoint 4), dedup CDC (Project 2) — toàn bộ là stateful. Stream chết sau 3 tuần chạy êm? 8/10 lần thủ phạm là state.

---

## 2. Why

### Stream có trí nhớ — và trí nhớ phải trả tiền

Batch job là người mất trí nhớ hạnh phúc: chạy xong, quên hết, lần sau tính lại từ đầu. Stream thì không được phép quên:

- "Count theo user" — batch sau phải **nhớ** count của batch trước để cộng tiếp.
- "Loại message trùng" — phải **nhớ** những id đã gặp.
- "Session 30 phút của user" — phải **nhớ** user này đang giữa session, bắt đầu lúc nào, đã click gì.

Cái "nhớ" đó là **state**. Và đây là chỗ streaming lộ bản chất khó của nó: state là dữ liệu **sống**, nằm trong hệ thống 24/7, lớn dần theo số key, phải sống sót qua crash, và KHÔNG hiện ra trong code của bạn — `groupBy("user_id").count()` nhìn vô hại như batch, nhưng ngầm ký hợp đồng nuôi một entry state cho MỖI user, vĩnh viễn nếu không ai dọn.

### Nếu không hiểu state thì sao?

Kịch bản có thật ở mọi công ty: stream chạy đẹp 3 tuần → batch duration dốc lên từ từ → executor GC quằn quại → một sáng thứ Hai OOM. Dev nhìn code: "có gì đâu, mỗi groupBy với dropDuplicates?" — chính xác, và mỗi cái đang nuôi hàng chục triệu entry state không hạn sử dụng. Không hiểu state, bạn sẽ chữa bằng cách tăng RAM (mua thời gian 3 tuần nữa) thay vì chữa gốc.

### Trade-off phải thuộc

| Được (nhờ state) | Trả giá |
|---|---|
| Tính incremental — không quét lại lịch sử | Memory/disk trên executor bị chiếm thường trực |
| Dedup, session, streak... — logic "xuyên batch" | Phải thiết kế TTL (watermark/timeout) — không có dọn tự nhiên |
| Checkpoint bảo hiểm → crash không mất trí nhớ | Checkpoint to ra, commit lâu ra; schema state gần như bất biến |

> **Analogy quán phở**: stateless là anh bán mang đi — khách đến, làm tô phở, khách đi, quên nhau. Stateful là chị chủ quán ghi sổ nợ — mỗi khách một dòng sổ, khách càng đông sổ càng dày, và nếu không có luật "nợ quá 6 tháng xóa sổ" (TTL), một ngày chị sẽ ôm cuốn sổ to hơn cái quán.

---

## 3. Theory

### 3.1. Stateful vs stateless — bảng phân loại

| Operation | Loại | State key | Ai dọn state? |
|---|---|---|---|
| `select`, `filter`, `withColumn`, `from_json` | **Stateless** | — | Không có gì để dọn |
| map-only rồi ghi sink (bronze ingest lesson 24) | **Stateless** | — | — |
| `groupBy(...).agg(...)` | Stateful | nhóm key (+window) | Watermark (nếu có window+WM); không thì KHÔNG AI |
| `groupBy(window(...))` + watermark | Stateful | window × key | Watermark ✅ (lesson 25) |
| `dropDuplicates([...])` | Stateful | các cột dedup | Watermark nếu có cột event time; không thì KHÔNG AI |
| stream-stream join | Stateful (nặng) | join key 2 phía | Watermark + range condition (lesson 28) |
| `applyInPandasWithState` | Stateful (tự chế) | group key | **BẠN** — qua timeout tự khai |

Cột cuối là cột sống còn. Quy tắc vàng đọc code review: *thấy operation stateful → hỏi ngay "ai dọn state này?"* — không trả lời được là bug đang ủ bệnh.

Stateless stream (bronze ingest) là loại "ngủ ngon": không state, restart nhẹ tênh, scale thoải mái. Đó là lý do kiến trúc đẩy logic nặng về silver/gold còn bronze giữ mỏng.

### 3.2. State nằm ở đâu — executor giữ, checkpoint bảo hiểm

```
        DRIVER (điều phối, KHÔNG giữ state)
           │
   ┌───────┴────────────────┐
   ▼                        ▼
 EXECUTOR 1               EXECUTOR 2
 ┌─────────────────┐      ┌─────────────────┐
 │ task p0 → state │      │ task p2 → state │   state bị BĂM THEO KEY
 │   store part 0  │      │   store part 2  │   (hash key % số partition —
 │ task p1 → state │      │ task p3 → state │   cùng số shuffle partitions),
 │   store part 1  │      │   store part 3  │   MỖI PARTITION 1 state store,
 └────────┬────────┘      └────────┬────────┘   sống trên executor giữa các batch
          │   cuối mỗi batch: snapshot/delta    │
          ▼                                     ▼
      CHECKPOINT  chk/state/<operatorId>/<partitionId>/1.delta, 2.delta, ..., 10.snapshot
      (storage bền — nguồn khôi phục khi executor/cả app chết)
```

Ba hệ quả thực dụng:

- **State ăn tài nguyên executor** — cùng miếng bánh memory với shuffle/cache (mô hình lesson 17). State to = execution memory nghẹt = spill = chậm.
- **`spark.sql.shuffle.partitions` bị ĐÓNG BĂNG lúc query chạy lần đầu** — số state store partition đi theo checkpoint mãi mãi. Muốn đổi phải checkpoint mới (mất state) hoặc offline tool. Vì thế streaming job phải CHỌN CON SỐ NÀY TRƯỚC KHI START (đừng để mặc định 200 nếu state bé, đừng để 8 nếu state sẽ khổng lồ).
- **Restore có phí**: executor chết → state partition của nó dựng lại từ snapshot + delta trong checkpoint. Checkpoint chậm (S3) + state to = restore lâu.

### 3.3. HDFSBackedStateStore vs RocksDB

**HDFSBackedStateStore** (mặc định): state là **HashMap trong JVM heap** của executor; cuối batch ghi delta xuống checkpoint, thi thoảng nén thành snapshot.

- Nhanh (thao tác in-memory thuần) và đơn giản — quá đủ khi state bé/vừa.
- Điểm chết: **toàn bộ state phải vừa trong heap**. State lớn → GC pause dài (đồ thị batch duration lởm chởm răng cưa) → rồi OOM. Ngưỡng đau thường bắt đầu từ vài GB state mỗi executor.

**RocksDB state store** (Spark 3.2+): state nằm trong **RocksDB — key-value store nhúng, trên native memory + LOCAL DISK của executor**, ngoài heap.

```python
spark.conf... # đặt TRƯỚC KHI START QUERY (config cấp session):
.config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

| | HDFSBacked (mặc định) | RocksDB |
|---|---|---|
| State sống ở | JVM heap | Native mem + local disk (ngoài heap) |
| Trần kích thước | ~heap executor | ~disk executor (lớn hơn hàng chục lần) |
| GC pressure | Cao khi state to | Gần như không |
| Tốc độ truy cập | Nhanh nhất (map trong mem) | Chậm hơn chút (vẫn rất nhanh; có bloom filter/block cache) |
| Khi nào chọn | State chắc chắn bé (vài trăm MB/executor) | **State lớn hơn heap chịu nổi**, key hàng chục triệu, dedup dài hạn, session đông user |

Quy tắc quyết định: ước lượng `số key sống × kích thước value` mỗi executor. Vượt ~vài GB hoặc không dám chắc → RocksDB. Nhiều team production bật RocksDB mặc định cho mọi stateful job — bảo hiểm rẻ. Lưu ý: đổi provider là đổi format state → **cần checkpoint mới**, không đổi giữa chừng trên checkpoint cũ.

### 3.4. `dropDuplicates` + watermark — dedup có hạn sử dụng

Lesson 24 đã cảnh báo: Kafka + retry = duplicate là chuyện thường. Dedup trên stream:

```python
# ❌ Bom hẹn giờ: nhớ MỌI order_id từ thuở khai thiên, state chỉ có tăng
dedup = events.dropDuplicates(["order_id"])

# ✅ Dedup có TTL: chỉ nhớ order_id trong cửa sổ 24h theo event time
dedup = (events
         .withWatermark("created_at", "24 hours")
         .dropDuplicates(["order_id", "created_at"]))   # cột event time PHẢI có mặt
```

Cơ chế: mỗi bộ giá trị cột dedup = 1 entry state; khi watermark vượt qua event time của entry → entry bị dọn. Duplicate đến trong vòng 24h bị chặn; duplicate đến sau 24h... lọt (hợp đồng là hợp đồng). Chọn TTL theo cùng phương pháp lesson 25: đo khoảng cách duplicate thực tế (retry của producer thường trong phút/giờ, không phải tuần).

Spark 3.5+ có `dropDuplicatesWithinWatermark` cho ngữ nghĩa thoáng hơn — bản 3.4 của ta dùng pattern trên là chuẩn.

### 3.5. State của streaming aggregation — nhìn lại lesson 25 bằng mắt mới

`groupBy(window, city).agg(sum)` — state là bảng ngầm `{(window, city) → sum đang tích}`:

- Mỗi batch: đọc entry của key liên quan → cộng → ghi lại (đây là phần `stateOnCurrentVersionSizeBytes` bạn sẽ soi ở lab).
- Watermark vượt window.end → entry bị xóa (số `numRowsRemoved` trong metrics).
- Kích thước state ≈ `số window đang sống × cardinality của key phụ`. Window 10 phút, watermark 1h, key là city (63 giá trị) → ~6×63 entry: bé xíu. Đổi key thành user_id (10M) → 60M entry: chuyện khác hẳn — và là lúc RocksDB lên tiếng.

### 3.6. Arbitrary stateful — `applyInPandasWithState`

Khi aggregation/dedup có sẵn không đủ (logic "nếu user im 30 phút thì chốt session và tính funnel"), bạn được tự quản state. Scala/Java có `mapGroupsWithState`/`flatMapGroupsWithState`; PySpark từ **3.4** (đúng bản cluster của ta) có `applyInPandasWithState`:

```python
def update_session(key, pdf_iter, state):
    # key: tuple group key; pdf_iter: iterator các pandas.DataFrame của batch này
    # state: GroupState — .exists / .get / .update(tuple) / .remove()
    #        .setTimeoutTimestamp(...) — hẹn giờ "gọi lại tôi khi hết giờ"
    if state.hasTimedOut:                 # watermark vượt deadline → chốt session
        (start, end, n) = state.get
        state.remove()
        yield pd.DataFrame([{"user": key[0], "start": start, "end": end, "events": n}])
    else:
        ...gộp event mới vào state, dời deadline = max_event_time + 30 phút...

out = (events
    .withWatermark("event_time", "10 minutes")            # đồng hồ cho timeout
    .groupBy("user_id")
    .applyInPandasWithState(update_session,
        outputStructType=..., stateStructType=...,        # schema output & schema state
        outputMode="append",
        timeoutConf="EventTimeTimeout"))
```

Ý niệm cần nắm (code đầy đủ ở lab): bạn nhận **state cũ + event mới**, trả về **output + state mới**, và BẠN chịu trách nhiệm đặt **timeout** (EventTimeTimeout theo watermark, hoặc ProcessingTimeTimeout theo đồng hồ thật) — quên timeout là state bất tử, đúng căn bệnh mục 3.7. Đây là dao mổ: sắc, nhưng mọi vết cắt là của bạn — dùng khi built-in không tả nổi logic, không phải để khoe.

### 3.7. State phình to — nguyên nhân & phác đồ

| # | Nguyên nhân | Triệu chứng | Thuốc |
|---|---|---|---|
| 1 | Aggregation/dedup **không watermark** | numRowsTotal tăng tuyến tính mãi | Thêm watermark + window/cột event time — sửa ngay từ code review |
| 2 | Watermark có nhưng **không tiến** (nguồn cạn, 1 partition Kafka im, producer đồng hồ lùi) | numRowsTotal đi ngang rồi tăng; watermark gap phình (lesson 25) | Sửa nguồn; alert watermark gap |
| 3 | **Key cardinality vô hạn** (key = uuid mỗi event, session_id không tái sử dụng) | Row added ≈ input rows, removed ≈ 0 | Thiết kế lại key; hỏi "key này có HỮU HẠN key sống không?" |
| 4 | TTL/timeout **quá rộng** so với nhu cầu (dedup 30 ngày cho retry 5 phút) | State to ổn định nhưng to vô lý | Đo rồi siết TTL |
| 5 | applyInPandasWithState **quên remove/timeout** | Như #1 nhưng ở operator custom | Luôn có nhánh hasTimedOut + state.remove() |
| 6 | State hợp lý nhưng **vượt heap** (HDFSBacked) | GC lởm chởm, OOM dù code đúng | Chuyển RocksDB; thêm executor memory/disk; tăng số shuffle partitions TỪ ĐẦU |

Thứ tự chẩn đoán khi batch duration dốc lên: nhìn `stateOperators` (mục 3.8) → phân biệt "row không được dọn" (#1–#5, bệnh logic) vs "row hợp lý nhưng nặng" (#6, bệnh dung lượng) → bốc thuốc đúng cột.

### 3.8. Monitoring qua `query.lastProgress["stateOperators"]`

```python
p = query.lastProgress
for op in p["stateOperators"]:
    op["operatorName"]              # vd 'stateStoreSave', 'dedupe', 'sessionWindowStateStoreSave'
    op["numRowsTotal"]              # ★ tổng entry state — ĐỒ THỊ QUAN TRỌNG NHẤT:
                                    #   đi ngang = khỏe, dốc lên mãi = có bệnh ở 3.7
    op["numRowsUpdated"]            # entry được ghi trong batch này
    op["numRowsRemoved"]            # ★ entry bị dọn — bằng 0 kéo dài = không ai dọn
    op["allUpdatesTimeMs"], op["allRemovalsTimeMs"], op["commitTimeMs"]
    op["stateMemory"] / op["memoryUsedBytes"]   # bytes state (theo provider)
    op["customMetrics"]             # RocksDB: rocksdbGetLatency, BytesCopied, ...
```

Phương trình sức khỏe cần nhớ: **ổn định ⇔ tốc độ thêm ≈ tốc độ dọn** (`numRowsUpdated` cho key mới ≈ `numRowsRemoved` về dài hạn). Production: đẩy các số này ra Prometheus qua `StreamingQueryListener` — Project 2 checkpoint 6 sẽ làm thật.

---

## 4. Internal

Một micro-batch của query stateful — phóng to bước ⑤ của lesson 23:

```
① Driver chốt batch N (offsets — lesson 24), lập plan có StateStoreRestore/Save
        │
② Task của partition p chạy trên executor ĐANG GIỮ state store p
   (state store có "location preference" — Spark cố gắng ghim
    task vào đúng executor cũ để khỏi load lại state)
        │
③ StateStoreRestore: mở state store p đúng PHIÊN BẢN batch N-1
   • HDFSBacked: HashMap có sẵn trong heap (hoặc dựng lại từ
     snapshot + delta nếu executor mới)
   • RocksDB: mở DB local, checkpoint version tương ứng
        │
④ Với mỗi key trong batch: get(state cũ) → tính → put(state mới)
   Watermark quét: key/window hết hạn → delete (numRowsRemoved)
        │
⑤ StateStoreSave — commit phiên bản N:
   • HDFSBacked: ghi N.delta (chỉ thay đổi) lên checkpoint;
     mỗi ~10 version nén thành N.snapshot
   • RocksDB: upload changelog/SST files lên checkpoint
        │
⑥ Driver ghi commits/N — batch chốt. State version N là chính thức.
   RESTART từ đây: dựng lại state đúng version N ở bất kỳ executor nào
        │
⑦ Nếu batch N chạy LẠI (crash trước commits/N): state store mở lại
   version N-1 và tính lại → không double-count trong state ✅
```

Hai insight đắt giá:

- **State cũng có version theo batch** — cùng nhịp với offsets/commits. Bộ ba offset–state–commit tiến bước NGUYÊN TỬ theo batch id: đây chính là nền móng để lesson 27 xây exactly-once.
- **Vì sao đổi số shuffle partitions phá state**: state partition p được định nghĩa bằng `hash(key) % numPartitions`. Đổi numPartitions là toàn bộ phép chia lại — file state cũ vô nghĩa. (Cùng lý do: đổi group key, đổi schema agg → checkpoint cũ không dùng được; kế hoạch "state migration" là việc thật của DE streaming.)

---

## 5. API

### `dropDuplicates` (+ watermark)

```python
events.withWatermark("created_at", "24 hours").dropDuplicates(["order_id", "created_at"])
```
- **Pitfall**: quên cột event time trong danh sách cột dedup → watermark không gắn được vào state dedup → state bất tử. Cả bộ (`withWatermark` + cột trong list) mới thành TTL.

### Bật RocksDB (Spark 3.4)

```python
spark = (SparkSession.builder.appName("...")
  .config("spark.sql.streaming.stateStore.providerClass",
          "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
  .config("spark.sql.shuffle.partitions", "16")   # QUYẾT ĐỊNH TRƯỚC KHI START — đóng băng theo checkpoint!
  .getOrCreate())
```
- **Pitfall**: áp vào checkpoint cũ (đang HDFSBacked) → lỗi/không tương thích. Provider chọn lúc khai sinh query.

### `applyInPandasWithState` (PySpark 3.4)

```python
df.groupBy("user_id").applyInPandasWithState(
    func,                       # func(key, iter_pdf, state) -> Iterator[pd.DataFrame]
    outputStructType=out_schema,
    stateStructType=state_schema,
    outputMode="append",
    timeoutConf="EventTimeTimeout")   # hoặc "ProcessingTimeTimeout" / "NoTimeout"
```
- **GroupState API**: `state.exists`, `state.get` (tuple), `state.update(tuple)`, `state.remove()`, `state.hasTimedOut`, `state.setTimeoutTimestamp(ms)` (event-time) / `state.setTimeoutDuration(ms)` (processing-time).
- **Pitfall #1**: `timeoutConf="NoTimeout"` + logic không tự remove = state bất tử. **#2**: EventTimeTimeout đòi `withWatermark` phía trước — không có là AnalysisException. **#3**: state là tuple THEO ĐÚNG THỨ TỰ stateStructType — thêm field là đổi schema state (checkpoint mới).

### Soi state — `lastProgress` & UI

```python
import json; print(json.dumps(query.lastProgress["stateOperators"], indent=2))
```
- **Pitfall**: `lastProgress` là snapshot MỘT batch; kết luận xu hướng phải nhìn chuỗi (vòng lặp in định kỳ, hoặc `query.recentProgress` — list ~100 progress gần nhất).

---

## 6. Demo nhỏ

Dedup có TTL — nhìn state được dọn bằng số:

```
Input:  rate 10 dòng/s → cứ 5 dòng thì "gửi lại" 1 bản trùng (giả duplicate)
   ↓    withWatermark 30s + dropDuplicates[id, event_time]
Output: console + vòng lặp in numRowsTotal/numRowsRemoved mỗi 10s
```

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("demo26").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

src = (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
       .withColumn("id", (F.col("value") / 5).cast("long"))   # 5 dòng chung 1 id → duplicate!
       .withColumnRenamed("timestamp", "event_time"))

dedup = (src.withWatermark("event_time", "30 seconds")
            .dropDuplicates(["id", "event_time"]))

q = (dedup.writeStream.format("console").outputMode("append")
     .trigger(processingTime="5 seconds")
     .option("checkpointLocation", "/tmp/chk-demo26")
     .start())

for _ in range(12):                       # giám sát 2 phút
    time.sleep(10)
    p = q.lastProgress
    if p and p["stateOperators"]:
        s = p["stateOperators"][0]
        print(f">>> state rows={s['numRowsTotal']}, "
              f"added~{s['numRowsUpdated']}, removed={s['numRowsRemoved']}")
q.stop(); spark.stop()
```

Quan sát: phút đầu `numRowsTotal` tăng (nạp key mới, chưa gì hết hạn); sau ~30–40s `numRowsRemoved` bắt đầu > 0 và `numRowsTotal` **đi ngang** — hệ cân bằng: thêm ≈ dọn. Giờ comment dòng `withWatermark` (đổi checkpoint mới, outputMode giữ append sẽ bị chê — chuyển `update`): `removed` mãi bằng 0, `total` leo thang tuyến tính — bạn vừa nhìn thấy "memory leak hợp pháp" bằng số thật.

---

## 7. Production Example

Nguyên mẫu Project 4 (Fraud Detection) — nơi state là nhân vật chính:

```
Kafka transactions (50k msg/s peak, 20M user hoạt động/tháng)
        │
Spark Structured Streaming — RocksDB state store (bắt buộc, xem số bên dưới)
  ① dropDuplicates(txn_id, event_time) + watermark 6h        ← chống double từ producer retry
  ② applyInPandasWithState theo user_id:
     state = 10 giao dịch gần nhất + tổng 24h (struct gọn, ~200 byte/user)
     timeout: EventTimeTimeout, dọn user im lặng 30 ngày     ← checkpoint 6 của project
  ③ chấm risk_score → high-risk ra Kafka alerts, tất cả ra Iceberg audit
        │
Bài toán dung lượng (làm TRƯỚC khi viết code — thói quen Senior):
  20M user × ~200 byte ≈ 4 GB state "sống" + dedup 6h × 50k/s × ~60 byte ≈ 65 GB
  → HDFSBacked (heap!) chết chắc → RocksDB trên NVMe local, 32 shuffle partitions,
    checkpoint S3, alert khi numRowsTotal lệch >20% so với dự toán
```

Ba bài học:

1. **Ước lượng state trên giấy trước khi start query** — vì shuffle partitions và provider bị đóng băng theo checkpoint, sửa sau rất đắt.
2. **Mọi state đều có điều khoản dọn** ghi rõ trong design doc: dedup 6h (watermark), user state 30 ngày (timeout). Reviewer fraud pipeline hỏi câu đầu tiên: "user bỏ app 2 năm, state của họ đi đâu?"
3. **Dự toán trở thành alert**: con số 4 GB/65 GB không nằm trong đầu ai đó — nó nằm trong Prometheus rule. State lệch dự toán là bug logic lộ sớm.

---

## 8. Hands-on Lab

**Mục tiêu**: nuôi một state khỏe và một state bệnh, chẩn đoán bằng stateOperators, bật RocksDB, và viết sessionization đầu tiên.

### Bước 1 — state khỏe vs state bệnh: `labs/lab26/state_health.py`

Viết query như demo mục 6 nhưng thêm cờ: `MODE = "healthy"` (có watermark) / `"sick"` (không watermark, key = `value` — cardinality vô hạn, outputMode update). Vòng lặp monitoring ghi CSV: `time, mode, numRowsTotal, numRowsUpdated, numRowsRemoved, batchDurationMs` (lấy `p["batchDuration"]`).

```bash
make run-local F=labs/lab26/state_health.py     # chạy mỗi mode ~3 phút, checkpoint riêng
```

Vẽ (hoặc kẻ tay) 2 đường `numRowsTotal` theo thời gian — một đi ngang, một leo thang. Đính vào NOTES: với tốc độ leo đó, bao lâu thì state chạm 4 GB heap?

### Bước 2 — bật RocksDB, soi customMetrics

Thêm vào bản "healthy":

```python
.config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```

(checkpoint MỚI!). Chạy lại, trong monitoring in thêm `stateOperators[0]["customMetrics"]` — thấy họ metric `rocksdb*` xuất hiện là provider đã đổi thành công. So batch duration hai provider ở tải này (state bé → RocksDB có thể ngang hoặc nhỉnh hơn chút — ghi nhận trung thực; lợi thế của nó chỉ lộ khi state to hơn heap).

### Bước 3 — sessionization: `labs/lab26/sessionize.py`

Dùng lại feeder socket của lab25 (sửa: bắn `{"user": "u1", "event_time": "...", "action": "click"}`, cho u1/u2 xen kẽ, thỉnh thoảng nghỉ dài). Query:

```python
import pandas as pd
from pyspark.sql.types import (StructType, StructField, StringType,
                               TimestampType, LongType)

out_schema = StructType([StructField("user", StringType()),
                         StructField("session_start", TimestampType()),
                         StructField("session_end", TimestampType()),
                         StructField("num_events", LongType())])
state_schema = StructType([StructField("start", TimestampType()),
                           StructField("end", TimestampType()),
                           StructField("n", LongType())])
GAP_MS = 60 * 1000   # session gap 60s cho dễ thấy trong lab

def sessionize(key, pdfs, state):
    if state.hasTimedOut:
        start, end, n = state.get
        state.remove()
        yield pd.DataFrame([{"user": key[0], "session_start": start,
                             "session_end": end, "num_events": n}])
    else:
        mn, mx, cnt = None, None, 0
        for pdf in pdfs:
            mn = min(pdf.event_time.min(), mn) if mn is not None else pdf.event_time.min()
            mx = max(pdf.event_time.max(), mx) if mx is not None else pdf.event_time.max()
            cnt += len(pdf)
        if state.exists:
            s, e, n = state.get
            mn, mx, cnt = min(mn, s), max(mx, e), cnt + n
        state.update((mn, mx, cnt))
        state.setTimeoutTimestamp(int(mx.timestamp() * 1000) + GAP_MS)  # hẹn giờ chốt
        yield pd.DataFrame([], columns=["user","session_start","session_end","num_events"])

sessions = (events                      # events: parse socket như lab25, có to_timestamp
    .withWatermark("event_time", "30 seconds")
    .groupBy("user")
    .applyInPandasWithState(sessionize, out_schema, state_schema,
                            "append", "EventTimeTimeout"))

# writeStream ra console, checkpoint /workspace/data/chk/lab26_sess
```

Chạy 2 terminal như lab25. Quan sát: session KHÔNG hiện lúc user đang hoạt động; chỉ khi user nghỉ quá 60s **và watermark tiến qua deadline** (cần user khác tiếp tục bắn event để đẩy watermark!) thì dòng session mới chốt ra console. Chính hiện tượng "cần dữ liệu mới để chốt chuyện cũ" này là bản chất event-time timeout — nghiệm lại lesson 25.

### Bước 4 — quan sát UI

Tab **Structured Streaming**: hàng **Aggregated State Memory In Use** và đồ thị Operation Duration (`stateStoreCommit`). Tab Jobs → stage có `StateStoreRestore/Save` trong DAG — chỉ được node đó là gọi tên được state trong physical plan. Ghi vào `labs/lab26/NOTES.md`.

---

## 9. Assignment

**Easy** — Trả lời bằng chữ của bạn:
1. Phân loại stateful/stateless và chỉ ra "ai dọn state": (a) filter+ghi Kafka; (b) groupBy(window)+watermark; (c) dropDuplicates(["id"]) trần; (d) applyInPandasWithState có EventTimeTimeout.
2. State nằm ở đâu lúc chạy, ở đâu khi crash? Vì sao đổi `spark.sql.shuffle.partitions` trên checkpoint cũ là không được?
3. HDFSBackedStateStore chết vì gì khi state lớn? RocksDB né bằng cách nào?

**Medium** — Từ CSV của Bước 1: (a) tính tốc độ tăng state (rows/phút) của bản "sick"; (b) giả sử 60 byte/row và heap executor 4 GB (nửa cho execution), ước ngày tử vong; (c) đề xuất 2 phương án cứu KHÁC NHAU về bản chất (một sửa logic, một sửa hạ tầng) và nói rõ phương án nào là chữa gốc, vì sao.

**Hard** — Nâng cấp sessionize.py: (a) thêm field `actions` (list action trong session — cẩn thận: state schema đổi nghĩa là gì với checkpoint?); (b) session đang mở mà cần "xem tạm" — vì sao output mode append không cho bạn thấy session dở dang, và bạn đổi thiết kế thế nào nếu nghiệp vụ đòi xem (gợi ý: emit bản nháp trong nhánh chưa timeout + cột `is_final`); (c) so sánh giải pháp của bạn với `F.session_window` + agg có sẵn — khi nào built-in đủ, khi nào phải custom?

**Production Challenge** — Viết `labs/lab26/state_alert.py`: dùng `StreamingQueryListener` (PySpark 3.4: `spark.streams.addListener`) bắt `onQueryProgress`, và in cảnh báo khi: numRowsTotal tăng >20% giữa 2 lần đo cách 1 phút, hoặc numRowsRemoved == 0 trong 5 phút liên tiếp khi có input. Đây là tiền thân của alerting Project 2/4 — giữ code lại dùng tiếp.

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor sẽ review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| Stateful op không TTL | Chậm dần → OOM | Bệnh #1 mục 3.7 — không phải "performance issue", là bug logic. |
| HDFSBacked với state nhiều GB | GC pause lởm chởm | State chiếm heap, GC quét đống object khổng lồ mỗi lượt. Đồ thị batch duration hình răng cưa là chữ ký của bệnh này → RocksDB. |
| shuffle partitions 200 (mặc định) cho state bé | Chậm lặt vặt mãi mãi | 200 state store con tí hon: mỗi batch 200 lượt mở/commit/file checkpoint. Chọn 8–32 từ đầu — nhớ: đóng băng theo checkpoint. |
| shuffle partitions quá ÍT cho state to | Nghẽn + không cứu được | Mỗi partition ôm state khổng lồ, restore lâu, không chia lại được nếu thiếu executor. Ước lượng dung lượng TRƯỚC (mục 7). |
| Checkpoint state trên S3, trigger ngắn | commitTimeMs cao | Mỗi batch upload delta/SST. Trigger dài hơn, hoặc RocksDB changelog checkpointing, hoặc storage nhanh hơn. |
| applyInPandasWithState value to (list ngàn phần tử) | Chậm + phình | Mỗi batch serialize cả value qua Arrow. Giữ state là TÓM TẮT (count/sum/top-k), đừng giữ raw events. |

Câu tự vấn phiên bản hoàn chỉnh của module: *"state của tôi: key là gì, bao nhiêu key sống, bao nhiêu byte một key, ai dọn, và con số đó nằm trong alert chưa?"*

---

## 11. Spark UI

Tab **Structured Streaming** — bài này đọc bằng con mắt state:

- **Aggregated State Memory In Use**: dung lượng state theo thời gian. Cùng câu thần chú: **đi ngang = khỏe, dốc lên mãi = bệnh** (đối chiếu bảng 3.7 để gọi tên bệnh).
- **Aggregated Number Of Total State Rows** (`numRowsTotal`): theo dõi cùng nhịp với memory — rows tăng mà memory tăng nhanh hơn nghĩa là value đang phình (bệnh khác nhau, thuốc khác nhau).
- **Operation Duration**: khi state to, `stateStoreCommit` và phần restore lấn át `addBatch` thuần — dấu hiệu chi phí đã chuyển từ "tính toán" sang "khuân vác state", đến giờ cân nhắc RocksDB/partition lại.
- **Batch Duration** hình răng cưa chu kỳ ~10 batch: trùng nhịp snapshot compaction của HDFSBacked — nhận ra pattern này đỡ mất một buổi debug oan.

Kết hợp UI (nhìn xu hướng nhanh) + `lastProgress`/listener (số chính xác, đẩy đi alert) — cặp bài trùng giám sát state.

---

## 12. Common Mistakes

1. **`dropDuplicates(["id"])` trần trên stream** — nhớ mọi id vĩnh viễn. Luôn kèm watermark + cột event time trong danh sách cột.
2. **Đổi `spark.sql.shuffle.partitions`/provider/group-key rồi restart trên checkpoint cũ** — state không tương thích, lỗi khó hiểu hoặc kết quả sai. Các quyết định này thuộc "khai sinh" của query.
3. **Nhồi raw data vào state** (giữ nguyên list event trong session state) — state phải là tóm tắt đủ dùng, không phải kho lưu trữ. Kho là Iceberg.
4. **Bật RocksDB xong quên local disk** — RocksDB ăn disk của executor; executor không có volume/disk tử tế (container mặc định!) thì nhanh chóng đầy. Kiểm tra dung lượng + IOPS trước.
5. **Test stateful bằng dữ liệu hữu hạn rồi kết luận "timeout không chạy"** — event-time timeout cần watermark TIẾN, tức cần dữ liệu MỚI. Nguồn cạn = watermark đứng = không gì chốt (bài học lesson 25 tái diễn ở tầng state).
6. **Không monitoring state từ ngày đầu** — đến khi OOM mới soi thì đã mất lịch sử để chẩn đoán. numRowsTotal phải lên dashboard cùng ngày query lên production.
7. **Dùng applyInPandasWithState cho việc `window`/`dropDuplicates` làm được** — tự nhận về mình mọi trách nhiệm dọn dẹp mà không thêm giá trị. Built-in trước, dao mổ sau.

---

## 13. Interview

**Junior:**

1. *State trong Structured Streaming là gì? Cho 2 ví dụ operation cần state.* — Dữ liệu trung gian engine phải nhớ giữa các micro-batch để tính incremental. Ví dụ: streaming aggregation (tổng đang tích mỗi key), dropDuplicates (tập khóa đã gặp), stream-stream join, sessionization.
2. *Stateful khác stateless thế nào về vận hành?* — Stateless (filter/map): không nhớ gì, restart nhẹ, memory phẳng. Stateful: giữ state trên executor + checkpoint, memory tăng theo số key sống, bắt buộc thiết kế TTL và giám sát.
3. *State nằm ở đâu?* — Bản sống: trong state store trên executor (heap với HDFSBacked, native mem + local disk với RocksDB), băm theo key thành partition. Bản bảo hiểm: thư mục state/ trong checkpoint (delta + snapshot theo version batch) để khôi phục khi crash.
4. *dropDuplicates trên stream cần chú ý gì?* — Kèm withWatermark và đưa cột event time vào danh sách cột dedup để state có TTL; không thì state lớn vô hạn. Duplicate đến ngoài cửa sổ TTL sẽ lọt — chọn TTL theo khoảng cách duplicate thực tế.

**Mid:**

5. *Khi nào chuyển từ HDFSBackedStateStore sang RocksDB?* — Khi state không còn thoải mái trong heap: hàng chục triệu key, dedup dài hạn, session user đông; triệu chứng GC pause dài/batch duration răng cưa/OOM dù logic đúng. RocksDB đưa state ra ngoài heap (native mem + local disk), trần dung lượng cao hơn hàng chục lần, đổi lấy truy cập chậm hơn chút và cần disk local tử tế. Đổi provider cần checkpoint mới.
6. *Vì sao số shuffle partitions của streaming job bị "đóng băng"?* — State bị băm `hash(key) % numPartitions` thành từng state store partition lưu trong checkpoint; đổi numPartitions làm phép băm cũ vô nghĩa, không đọc lại được state. Phải chọn đúng từ đầu theo dự toán state, hoặc chấp nhận checkpoint mới/công cụ migrate offline.
7. *Kể quy trình chẩn đoán state phình.* — Nhìn stateOperators qua lastProgress/listener: numRowsTotal dốc lên mãi? numRowsRemoved = 0? → bệnh logic (thiếu watermark, watermark kẹt, key cardinality vô hạn, timeout quên). Rows hợp lý nhưng memory/commit time cao → bệnh dung lượng (provider, partitions, value quá to). Gọi tên bệnh trước, bốc thuốc sau.
8. *applyInPandasWithState dùng khi nào, trách nhiệm gì đi kèm?* — Khi logic xuyên batch không tả được bằng agg/window/dedup có sẵn (sessionization tùy biến, state machine, đếm có điều kiện phức tạp). Trách nhiệm: tự định nghĩa schema state, tự update/remove, tự đặt timeout (event-time cần watermark) — quên là state bất tử; và schema state gần như bất biến theo checkpoint.

**Senior:**

9. *Thiết kế state cho fraud: 20M user, 50k txn/s, cần 10 giao dịch gần nhất + tổng 24h mỗi user. Trình bày các quyết định.* — Dự toán trước: ~200 byte/user × 20M ≈ 4 GB + dedup 6h ≈ chục GB → RocksDB bắt buộc, NVMe local, shuffle partitions ~32–64 chọn theo (state_total / mục tiêu ~1–2 GB mỗi partition) và số core; state là TÓM TẮT (ring buffer 10 txn + tổng trượt), không giữ raw; TTL hai tầng: dedup 6h (watermark), user im lặng 30 ngày (EventTimeTimeout + remove); checkpoint S3, alert numRowsTotal lệch dự toán >20%; kế hoạch schema state versioning (đổi struct = checkpoint mới + chiến lược warm-up). Điểm ăn tiền: nói dự toán ra CON SỐ trước khi nói công nghệ.
10. *Stream chạy ổn 3 tuần rồi batch duration tăng dần, chưa OOM. Anh/chị điều tra thế nào?* — (1) Structured Streaming tab: state memory & numRowsTotal 3 tuần — dốc lên? (2) Nếu dốc: removed ≈ 0? → truy nguồn dọn: watermark có gắn đúng chỗ không, watermark gap có phình không (một partition Kafka chết làm min-watermark kẹt), key có phải cardinality vô hạn không. (3) Nếu rows đi ngang mà duration vẫn tăng: Operation Duration — stateStoreCommit tăng (checkpoint chậm dần vì file nhiều → dọn maintenance/snapshot), hay GC (Executors tab, GC time) → provider. (4) Vá tạm (thêm memory) TÁCH BẠCH với vá gốc (TTL/RocksDB/re-key), kèm timeline vá gốc. Trình bày có nhánh loại trừ + phân biệt triệu chứng/nguyên nhân là chuẩn Senior.

---

## 14. Summary

### Mindmap

```
                        STATEFUL OPERATIONS
                                │
   ┌────────────────┬───────────┴───────────┬───────────────────────┐
   ▼                ▼                       ▼                       ▼
 STATE LÀ GÌ     STATE STORE             OPERATIONS              VẬN HÀNH
   │                │                       │                       │
 trí nhớ giữa    executor giữ (băm       agg theo key/window     bệnh phình to:
 các micro-      theo key, partition     (watermark dọn)         không TTL / WM kẹt /
 batch           = shuffle.partitions    dropDuplicates          key vô hạn / vượt heap
 stateless:      — ĐÓNG BĂNG!)           + watermark = TTL       lastProgress
 filter/map      checkpoint bảo hiểm     applyInPandasWithState  .stateOperators:
 stateful:       HDFSBacked: heap        (tự quản + timeout      numRowsTotal đi ngang
 agg/dedup/      RocksDB (3.2+): mem     bắt buộc)               = khỏe; dốc lên = bệnh
 join/session    +disk, state > heap     sessionization          alert từ ngày đầu
```

### Checklist trước khi gõ "Continue"

- [ ] Phân loại stateful/stateless một query bất kỳ và trả lời được "ai dọn state này".
- [ ] Vẽ được: state sống trên executor (băm theo key), bản bảo hiểm trong checkpoint state/.
- [ ] Nói được vì sao shuffle partitions và provider bị đóng băng theo checkpoint.
- [ ] Chọn được HDFSBacked vs RocksDB kèm ngưỡng và triệu chứng.
- [ ] Viết đúng pattern dedup có TTL (watermark + cột event time trong list).
- [ ] Đã chạy sessionization bằng applyInPandasWithState và giải thích vì sao session chỉ chốt khi watermark tiến.
- [ ] Đã in numRowsTotal/numRowsRemoved và diễn giải được đồ thị khỏe/bệnh.

---

## 15. Next Lesson

**Lesson 27 — Exactly-once semantics: idempotent sink, foreachBatch.**

Bạn đã gom đủ ba mảnh ghép: offset chốt trước (lesson 24), watermark có kỷ luật (lesson 25), state versioned theo batch (lesson 26). Nhưng còn một lỗ hổng bạn đã thấy mà ta chưa vá: batch chạy lại thì **sink nhận dữ liệu hai lần** — Kafka sink của lesson 24 vẫn chỉ là at-least-once. Lesson 27 vá nốt mảnh cuối: at-most-once / at-least-once / exactly-once khác nhau ở đâu, vì sao exactly-once thực chất là "at-least-once + sink idempotent", và `foreachBatch` — cánh cửa đưa cả thế giới batch (MERGE INTO Iceberg!) vào streaming. Xong lesson 27, bạn kill job thoải mái mà bảng vàng không double một dòng — và đó chính là kỹ năng Project 2 chấm điểm.

> Gõ **"Continue"** khi sẵn sàng.
