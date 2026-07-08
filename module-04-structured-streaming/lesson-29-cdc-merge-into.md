# Lesson 29 — CDC pattern hoàn chỉnh: Debezium envelope, MERGE INTO

> Module 4 · Structured Streaming · Tuần 15 · Thời lượng: 6–7 giờ (lý thuyết 3h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- **CDC (Change Data Capture)** là gì, và vì sao **log-based CDC** (Debezium đọc WAL) nghiền nát query-based CDC.
- **Debezium envelope** đến từng field: `schema`/`payload`, `before`/`after`, `op` (c/u/d/r), `ts_ms`, `source` (lsn, txId) — đọc envelope như đọc tiếng mẹ đẻ.
- Parse envelope trong Spark bằng `from_json` + schema tường minh.
- **Dedup trong batch**: một micro-batch chứa NHIỀU thay đổi của cùng một row — lấy event cuối per key theo `ts_ms` + `lsn`, và vì sao thiếu bước này MERGE nổ.
- **foreachBatch + MERGE INTO Iceberg** đầy đủ 3 nhánh: DELETE / UPDATE / INSERT.
- Tombstone, snapshot read (`op='r'`), schema evolution từ source, và **ordering guarantee per key** nhờ Kafka partition theo PK.

Sau bài này bạn phải làm được:

- Nhìn một message Debezium raw và nói ngay: thao tác gì, row trước/sau ra sao, thứ tự so với message khác.
- Viết pipeline Kafka → parse → dedup → MERGE INTO Iceberg chạy được, UPDATE/DELETE ở Postgres phản ánh vào Iceberg trong vài giây.
- Trả lời câu senior: "hai update liên tiếp vào cùng row, làm sao chắc chắn áp dụng đúng thứ tự?"

Kiến thức dùng trong thực tế: CDC là **nghề** của Data Engineer 2020s. Mọi công ty đều có bài toán "bản sao database production trong lakehouse, trễ dưới 1 phút, không đè chết DB nguồn". Đây chính là xương sống của Project 2 tuần này, và là chủ đề phỏng vấn nặng ký ở mọi công ty có lakehouse.

---

## 2. Why

### Vấn đề: bảng `orders` ở Postgres, báo cáo ở lakehouse

Cách ngây thơ nhất: mỗi đêm `SELECT * FROM orders` đổ vào lakehouse. Đau ngay:

- Bảng 500M row, full dump mỗi đêm = đấm vào DB production + tốn network + trễ 24h.
- Khôn hơn: `WHERE updated_at > lần_trước` (query-based CDC / incremental pull). Vẫn dính 3 nhát dao:
  1. **Không bắt được DELETE** — row biến mất khỏi nguồn thì query nào thấy?
  2. **Lệ thuộc cột `updated_at`** — app quên set, hay UPDATE bằng tay không đổi cột đó → mất thay đổi. Hai update giữa 2 lần poll → chỉ thấy bản cuối, mất lịch sử trung gian.
  3. **Vẫn poll, vẫn tải DB**, và độ trễ = chu kỳ poll.

### Log-based CDC: đọc trộm nhật ký của database

Mọi database ghi mọi thay đổi vào **transaction log** trước khi làm thật (Postgres: WAL — Write-Ahead Log). Debezium giả làm một **replication client**, đọc dòng log đó và biến từng thay đổi committed thành một event Kafka:

| | Query-based | Log-based (Debezium) |
|---|---|---|
| Bắt DELETE | ✘ | ✔ |
| Bắt mọi thay đổi trung gian | ✘ (chỉ thấy trạng thái lúc poll) | ✔ (từng event một) |
| Tải lên DB nguồn | Query nặng lặp lại | Gần như 0 (đọc log tuần tự) |
| Cần sửa schema/app nguồn | Cần cột updated_at, đôi khi trigger | Không |
| Độ trễ | Chu kỳ poll (phút→giờ) | Sub-second |
| Thứ tự thay đổi | Không đảm bảo | Đúng thứ tự commit trong log |

> Analogy: query-based là **chụp ảnh kho hàng mỗi đêm** rồi ngồi so hai tấm ảnh tìm khác biệt — hàng nhập rồi xuất trong ngày là vô hình. Log-based là **đọc sổ ghi chép của thủ kho** — từng dòng nhập/xuất/hủy, đúng thứ tự, không sót.

### Nếu không có pattern hôm nay thì sao?

Bạn có event CDC trong Kafka rồi... nhưng bảng lakehouse thì sao? Append thô mọi event → bảng thành **nhật ký thay đổi** chứ không phải **bản sao hiện trạng** — mọi query phải tự "tua" lịch sử. Pattern chuẩn: **MERGE INTO** — áp từng đợt thay đổi vào bảng đích để nó luôn là ảnh phản chiếu của bảng nguồn. Đó là nội dung hôm nay.

---

## 3. Theory

### 3.1. Dòng chảy CDC end-to-end

```
 PostgreSQL                Debezium (Kafka Connect)            Kafka
┌───────────┐   WAL      ┌──────────────────────────┐   ┌─────────────────┐
│ INSERT    │──────────► │ đọc WAL qua replication  │──►│ topic:           │
│ UPDATE    │  (logical  │ slot (pgoutput plugin)   │   │ cdc.olist.sellers│
│ DELETE    │  decoding) │ mỗi thay đổi committed → │   │ key = PK         │
│ COMMIT    │            │ 1 event envelope         │   │ partition theo   │
└───────────┘            └──────────────────────────┘   │ hash(key)        │
                                                        └────────┬────────┘
                                                                 │
                                              Spark Structured Streaming
                                              parse → dedup → MERGE INTO
                                                                 │
                                                                 ▼
                                                       Iceberg (bản sao
                                                        + lịch sử snapshot)
```

Hai khái niệệm Postgres cần nắm: **replication slot** — con trỏ của Debezium trong WAL, DB giữ WAL lại cho đến khi slot đọc xong (slot bỏ hoang = WAL phình đầy disk — tai nạn kinh điển!); **publication** — danh sách bảng được stream. Và **LSN (Log Sequence Number)** — số thứ tự byte trong WAL, tăng đơn điệu toàn cục: hai event bất kỳ so LSN là biết cái nào xảy ra trước.

### 3.2. Debezium envelope — mổ xẻ từng field

Một message value (JSON converter, chưa tắt schema) có 2 tầng: `schema` (mô tả kiểu — chiếm chỗ, thường tắt đi hoặc dùng Avro + Schema Registry) và `payload` (thịt). Payload của một **UPDATE**:

```json
{
  "before": { "seller_id": "S042", "city": "sao paulo",  "state": "SP" },
  "after":  { "seller_id": "S042", "city": "campinas",   "state": "SP" },
  "source": {
    "version": "2.x", "connector": "postgresql", "db": "mydb",
    "ts_ms": 1751960000123,          ← thời điểm thay đổi TRONG DB nguồn
    "schema": "olist", "table": "sellers",
    "lsn": 24023128,                 ← vị trí trong WAL — TIE-BREAKER thứ tự
    "txId": 771
  },
  "op": "u",
  "ts_ms": 1751960000456             ← thời điểm DEBEZIUM xử lý (≥ source.ts_ms)
}
```

Bảng giải mã `op` — học thuộc:

| op | Nghĩa | before | after |
|---|---|---|---|
| `c` | create (INSERT) | null | row mới |
| `u` | update | row cũ¹ | row mới |
| `d` | delete | row bị xóa¹ | **null** |
| `r` | read (snapshot ban đầu) | null | row hiện trạng |

¹ `before` chỉ đầy đủ khi bảng nguồn đặt `REPLICA IDENTITY FULL` (repo `kafka-flink` đã đặt — xem `cdc/olist-cdc-setup.sql`); mặc định Postgres chỉ ghi PK vào before.

- **`op='r'`**: lúc connector khởi động lần đầu với `snapshot.mode=initial`, Debezium SELECT toàn bộ bảng và phát mỗi row một event `r` — để đích có trạng thái xuất phát, sau đó mới stream thay đổi thật. Với MERGE, `r` xử lý **hệt như upsert** (`c`/`u`).
- **`ts_ms` hai nơi**: `source.ts_ms` = lúc commit ở DB (event time); `payload.ts_ms` = lúc Debezium đọc được. Dedup/ordering dùng **source.ts_ms**, nhưng nó chỉ chính xác đến millisecond — hai update cùng ms thì sao? → **`source.lsn` là tie-breaker tuyệt đối** (tăng đơn điệu theo WAL).
- **Tombstone**: SAU event `op='d'`, Debezium mặc định phát thêm một message `value = null` (tombstone) cùng key — không phải cho bạn, mà cho **Kafka log compaction** (dọn hẳn key khỏi topic compacted). Repo `kafka-flink` đặt `tombstones.on.delete=false` nên không có; nếu nguồn khác bật, code Spark PHẢI lọc `value IS NOT NULL` trước khi parse, không thì `from_json` trả null tràn pipeline.

### 3.3. Ordering guarantee — vì sao tin được thứ tự?

Kafka chỉ đảm bảo thứ tự **trong một partition**. Debezium dùng **PK của bảng làm message key**, và Kafka partition theo `hash(key)`:

```
mọi event của S042 ──hash──► partition 1  →  đúng thứ tự WAL với nhau
mọi event của S317 ──hash──► partition 0  →  đúng thứ tự WAL với nhau
S042 vs S317 chéo partition → KHÔNG đảm bảo thứ tự tương đối (và không cần!)
```

Per-key ordering là đủ cho MERGE: trạng thái cuối của mỗi row chỉ phụ thuộc chuỗi thay đổi **của chính nó**. Hệ quả ngược: ai đó đổi số partition của topic đang chạy, hoặc producer không key theo PK → thứ tự per-key vỡ → bảng đích sai không thể sửa bằng code Spark. Đây là guarantee **phải bảo vệ ở tầng hạ tầng**.

### 3.4. Dedup trong batch — event cuối per key thắng

Trigger 30 giây, một row bị UPDATE 5 lần trong 30 giây đó → micro-batch chứa 5 event cùng PK. MERGE INTO **cấm** nhiều source row khớp cùng một target row (`Cannot perform MERGE ... multiple source rows`). Kể cả không cấm, áp cả 5 cũng vô nghĩa — chỉ **event cuối** quyết định trạng thái. Chuẩn dedup:

```
per key (PK):  giữ event có (source.ts_ms, source.lsn) LỚN NHẤT
               — ts_ms trước, lsn phá hòa
```

Cài bằng window function trong foreachBatch (xem Section 5). Lưu ý: dedup này là **trong một batch**; giữa các batch, thứ tự đã được Kafka per-partition ordering + Spark xử lý batch tuần tự lo.

### 3.5. MERGE INTO — một câu SQL, ba số phận

```sql
MERGE INTO iceberg.lab29.sellers t          -- bảng đích (bản sao)
USING changes s                             -- batch đã parse + dedup
   ON t.seller_id = s.seller_id             -- khớp theo PK
WHEN MATCHED AND s.op = 'd' THEN DELETE     -- nguồn xóa → đích xóa
WHEN MATCHED THEN UPDATE SET                -- nguồn sửa → đích sửa
     t.city = s.city, t.state = s.state, ...
WHEN NOT MATCHED AND s.op != 'd' THEN       -- chưa có → chèn (c/u/r đều vậy)
     INSERT (seller_id, city, state, ...) VALUES (s.seller_id, s.city, ...)
```

Đọc kỹ từng nhánh — đề phỏng vấn nằm cả ở đây:

- `WHEN MATCHED AND op='d' THEN DELETE` phải đứng **trước** `WHEN MATCHED THEN UPDATE` — các nhánh MATCHED xét theo thứ tự, nhánh đầu khớp là chốt.
- Vì sao `NOT MATCHED AND op != 'd'`? Delete cho row đích không có (ví dụ replay event đã áp, hoặc row sinh-và-chết gọn trong một batch được dedup còn đúng event `d`) → **bỏ qua êm ái** thay vì insert row rác. Đây chính là tính **idempotent**: replay cả batch, MERGE lại, kết quả y nguyên — khớp nối hoàn hảo với exactly-once của lesson 27.
- Event `u` mà đích chưa có row (mất event `c` do bắt đầu đọc topic giữa chừng)? Rơi vào nhánh NOT MATCHED → INSERT từ `after` — tự lành. Đó là lý do lấy giá trị từ `after` chứ không nghĩ theo kiểu "update thì chỉ set cột đổi".

### 3.6. Schema evolution từ source

Nguồn `ALTER TABLE ADD COLUMN phone` → event mới có thêm field trong `after`. Chuỗi phản ứng:

1. **`from_json` với schema cố định**: field lạ bị **lờ đi lặng lẽ** — pipeline sống nhưng MẤT cột mới. (Ngược lại field thiếu → null — an toàn.)
2. Muốn ăn cột mới: cập nhật schema trong code + `ALTER TABLE ... ADD COLUMN` trên Iceberg (Iceberg evolution an toàn — lesson 30) + cập nhật câu MERGE. Deploy lại — checkpoint giữ nguyên vì source/sink không đổi, chỉ projection đổi.
3. Hàng công nghiệp: Avro + Schema Registry (repo `kafka-flink` dùng cho Flink) — schema đi kèm data, đổi tương thích tự lan. Với Spark thuần JSON: quy trình là "schema nguồn đổi = một PR có kiểm soát", và **giám sát**: đếm event có field ngoài schema (parse thêm cột `_rescued` bằng cách so sánh raw vs parsed) để biết mà phản ứng, đừng để 3 tháng sau mới phát hiện thiếu cột.

---

## 4. Internal

Chuyện gì xảy ra với MỘT câu `UPDATE olist.sellers SET city='campinas' WHERE seller_id='S042'` — từ Postgres đến Iceberg:

```
① Postgres: ghi thay đổi vào WAL, commit transaction (LSN 24023128)
        │
② Logical decoding: plugin pgoutput đọc WAL, lọc theo publication olist_pub,
   giải mã record nhị phân → thay đổi logical (bảng, before, after)
        │
③ Debezium connector (trong Kafka Connect): bọc thành envelope
   (op='u', source.lsn, source.ts_ms), key = {"seller_id":"S042"},
   produce vào topic cdc.olist.sellers; xác nhận xong thì TIẾN slot
   (confirmed_flush_lsn) — Postgres được phép dọn WAL tới đó
        │
④ Kafka: hash(key) → partition p; append offset o — từ đây thứ tự
   per-key là bất biến vật lý
        │
⑤ Spark micro-batch N: WAL checkpoint chốt dải offset (lesson 27),
   executor đọc, from_json bung envelope thành cột
        │
⑥ foreachBatch: dedup per key theo (ts_ms, lsn) → MERGE INTO
   Iceberg: join target × source tìm file chứa row khớp,
   viết lại file đó (copy-on-write) với city mới
        │
⑦ Iceberg commit snapshot mới (atomic CAS trên catalog REST) —
   Trino/Spark reader thấy 'campinas' từ snapshot này trở đi;
   snapshot cũ vẫn còn → time travel (Project 2 checkpoint 5)
```

Điểm nội tạng đáng giá:

- **Slot là hợp đồng giữ WAL**: Spark chết không sao (Kafka giữ data theo retention), nhưng **Debezium/Connect chết lâu** = slot không tiến = Postgres tích WAL không được xóa → đầy disk DB production. Giám sát `pg_replication_slots` là việc của DE, không chỉ DBA.
- **MERGE copy-on-write**: mỗi batch viết lại các data file chứa row bị chạm. Bảng to + update rải rác đều = viết lại nhiều → đây là lý do tồn tại merge-on-read (lesson 31) và compaction (lesson 32).
- **Snapshot lineage**: mỗi batch một snapshot `operation=overwrite/append` — chuỗi snapshot chính là "lịch sử CDC đã áp", nguyên liệu cho time travel và audit.

---

## 5. API

### Schema envelope + `from_json`

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

row_schema = StructType([                      # khớp bảng olist.sellers
    StructField("seller_id", StringType()),
    StructField("zip_code_prefix", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
])
envelope_schema = StructType([
    StructField("before", row_schema),
    StructField("after",  row_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("source", StructType([
        StructField("ts_ms", LongType()),
        StructField("lsn",   LongType()),
        StructField("table", StringType()),
    ])),
])

parsed = (raw
    .filter(F.col("value").isNotNull())        # chặn tombstone
    .select(F.from_json(F.col("value").cast("string"), envelope_schema).alias("e"))
    .select("e.op", "e.source.ts_ms", "e.source.lsn",
            "e.before", "e.after"))
```

- **Pitfall**: nếu connector để `value.converter.schemas.enable=true` (mặc định JsonConverter), payload nằm dưới một tầng `payload` nữa → schema phải bọc thêm `StructField("payload", envelope_schema)` hoặc tắt option đó. Parse ra **toàn null** mà không lỗi = 99% sai tầng schema. `from_json` không bao giờ raise — sai là null lặng lẽ, hãy assert sớm.

### Dedup: event cuối per key

```python
from pyspark.sql.window import Window

def latest_per_key(df, key_cols):
    w = Window.partitionBy(*key_cols).orderBy(F.desc("ts_ms"), F.desc("lsn"))
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))
```

- **Pitfall**: `dropDuplicates(key)` KHÔNG thay thế được — nó giữ row *tùy hứng*, không phải row mới nhất. Phải là window + order by (ts_ms, lsn) DESC.

### Trải phẳng: lấy row từ `after`, riêng delete lấy key từ `before`

```python
flat = parsed.select(
    "op", "ts_ms", "lsn",
    F.coalesce("after.seller_id", "before.seller_id").alias("seller_id"),
    F.col("after.zip_code_prefix").alias("zip_code_prefix"),
    F.col("after.city").alias("city"),
    F.col("after.state").alias("state"),
)
```

Event `d` có `after = null` → mọi cột data null, nhưng **key phải sống** để MERGE tìm được row cần xóa — vì thế `coalesce(after.pk, before.pk)`.

### `MERGE INTO` trong foreachBatch

```python
def apply_cdc(batch_df, batch_id):
    changes = latest_per_key(batch_df, ["seller_id"])
    changes.createOrReplaceTempView("changes")
    changes.sparkSession.sql("""
        MERGE INTO iceberg.lab29.sellers t
        USING changes s
           ON t.seller_id = s.seller_id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
             t.zip_code_prefix = s.zip_code_prefix,
             t.city = s.city, t.state = s.state
        WHEN NOT MATCHED AND s.op != 'd' THEN
             INSERT (seller_id, zip_code_prefix, city, state)
             VALUES (s.seller_id, s.zip_code_prefix, s.city, s.state)
    """)
```

- **Pitfall**: MERGE INTO cần extension `IcebergSparkSessionExtensions` trong config session (thiếu → `MERGE INTO TABLE is not supported temporarily`). Và tempview `changes` tạo từ `batch_df` — trong cùng session của batch, đừng tạo trước ở ngoài.

---

## 6. Demo nhỏ

Chưa cần hạ tầng — cầm 4 event envelope tự chế, đi trọn parse → dedup → trạng thái cuối:

```
Input:  4 event của 2 seller: S1 được insert rồi update 2 lần, S2 insert rồi delete
   ↓    parse from_json → dedup (ts_ms, lsn) → còn 2 event "cuối"
Output: S1 = bản update cuối (op=u), S2 = op=d (sẽ thành DELETE khi MERGE)
```

```python
# labs/lab29/demo_envelope.py  (chạy: make run-local F=labs/lab29/demo_envelope.py)
events = [
  '{"before":null,"after":{"seller_id":"S1","city":"osasco","state":"SP","zip_code_prefix":"06210"},"op":"c","ts_ms":1,"source":{"ts_ms":100,"lsn":10,"table":"sellers"}}',
  '{"before":{"seller_id":"S1","city":"osasco","state":"SP","zip_code_prefix":"06210"},"after":{"seller_id":"S1","city":"campinas","state":"SP","zip_code_prefix":"13000"},"op":"u","ts_ms":2,"source":{"ts_ms":200,"lsn":20,"table":"sellers"}}',
  '{"before":{"seller_id":"S1","city":"campinas","state":"SP","zip_code_prefix":"13000"},"after":{"seller_id":"S1","city":"santos","state":"SP","zip_code_prefix":"11000"},"op":"u","ts_ms":3,"source":{"ts_ms":200,"lsn":21,"table":"sellers"}}',
  '{"before":{"seller_id":"S2","city":"recife","state":"PE","zip_code_prefix":"50000"},"after":null,"op":"d","ts_ms":4,"source":{"ts_ms":300,"lsn":30,"table":"sellers"}}',
]
raw = spark.createDataFrame([(e,) for e in events], ["value"])
# ... parse bằng envelope_schema (section 5), flatten, dedup ...
```

Chạy và tự kiểm: sau dedup phải còn đúng 2 row — S1 với `city='santos'` (chú ý: 2 update **cùng ts_ms=200**, lsn 21 > 20 quyết định — đây chính là vai trò tie-breaker của lsn!) và S2 với `op='d'`. Nếu bạn dedup chỉ theo ts_ms, S1 ra kết quả hên xui — chạy vài lần sẽ thấy.

---

## 7. Production Example

Chính là pipeline repo `kafka-flink` của bạn, nay do Spark đảm nhiệm tầng compute:

```
PostgreSQL olist.sellers (REPLICA IDENTITY FULL, publication olist_pub)
   │  wal_level=logical, slot: olist_sellers_slot
   ▼
Debezium PostgresConnector trên Kafka Connect (:8083)
   │  topic.prefix=cdc → topic cdc.olist.sellers, key=PK
   │  snapshot.mode=initial (op='r' cho dữ liệu có sẵn)
   │  heartbeat.interval.ms=10000 (giữ slot tiến cả khi bảng im ắng!)
   ▼
Kafka (retention dài hơn thời gian sửa sự cố tối đa)
   ▼
Spark Structured Streaming: parse → dedup(ts_ms,lsn) → foreachBatch MERGE
   ▼
Iceberg iceberg.cdc.sellers (bản sao) ──► Trino (:8080) phục vụ query
```

Vì sao từng lựa chọn (đối chiếu file `cdc/olist-postgres-connector.json` trong repo):

1. **`REPLICA IDENTITY FULL`**: để `before` chứa đủ cột — cần cho engine so sánh và cho SCD2 (Project 2). Giá: WAL to hơn.
2. **`heartbeat.interval.ms=10000`**: bảng ít thay đổi → slot vẫn tiến đều nhờ heartbeat → WAL không tích. Nhớ lesson 28: nguồn "ế" gây đủ thứ bệnh — heartbeat là thuốc.
3. **`snapshot.mode=initial`**: lần đầu chạy có luôn hiện trạng bảng (op='r'), MERGE nuốt như thường — không cần job backfill riêng.
4. **Một topic một bảng, MERGE một bảng đích**: nhiều bảng = nhiều topic; Spark có thể `subscribePattern("cdc.olist.*")` rồi rẽ nhánh theo `source.table` trong foreachBatch — nhưng mỗi-bảng-một-query dễ vận hành hơn (checkpoint, lag, restart độc lập). Trade-off ghi sổ.
5. Repo đang để **AvroConverter** cho Flink; cho lab Spark ta tạo connector thứ hai dùng **JsonConverter** (Project 2 checkpoint 1) — cùng nguồn, hai định dạng, không ảnh hưởng nhau vì khác slot + topic prefix.

---

## 8. Hands-on Lab

**Mục tiêu**: UPDATE/DELETE ở Postgres phản ánh vào Iceberg sau vài giây, qua pipeline tự tay dựng. Hạ tầng: network đã nối như lab 27.

### Bước 1 — bật nguồn CDC (dùng đồ có sẵn của `../kafka-flink`)

```bash
cd ../kafka-flink && docker compose up -d postgres broker kafka-connect minio minio-init iceberg-rest
# schema, role cdc_user, publication — script idempotent có sẵn:
docker exec -i postgres psql -U postgres -d mydb < cdc/olist-cdc-setup.sql
```

### Bước 2 — đăng ký connector JSON riêng cho lab

Tạo `labs/lab29/lab29-connector.json` (nhái `cdc/olist-postgres-connector.json` nhưng đổi 4 chỗ: `slot.name=lab29_slot`, `topic.prefix=lab29`, converter JSON, tắt schemas):

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres", "database.port": "5432",
  "database.user": "cdc_user", "database.password": "cdc_pass",
  "database.dbname": "mydb",
  "topic.prefix": "lab29",
  "schema.include.list": "olist",
  "table.include.list": "olist.sellers",
  "plugin.name": "pgoutput",
  "slot.name": "lab29_slot",
  "publication.name": "olist_pub",
  "publication.autocreate.mode": "disabled",
  "snapshot.mode": "initial",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "heartbeat.interval.ms": "10000",
  "tombstones.on.delete": "false"
}
```

```bash
curl -s -X PUT http://localhost:8083/connectors/lab29-sellers/config \
     -H 'Content-Type: application/json' -d @labs/lab29/lab29-connector.json
curl -s http://localhost:8083/connectors/lab29-sellers/status | python3 -m json.tool
# soi envelope bằng mắt trước khi viết code — thói quen vàng:
docker exec broker kafka-console-consumer --bootstrap-server broker:29092 \
  --topic lab29.olist.sellers --from-beginning --max-messages 3 --property print.key=true
```

### Bước 3 — `labs/lab29/cdc_to_iceberg.py`

Ghép các mảnh Section 5 thành pipeline hoàn chỉnh: SparkSession config Iceberg (như lab 27) → `CREATE TABLE IF NOT EXISTS iceberg.lab29.sellers` → readStream Kafka topic `lab29.olist.sellers`, `startingOffsets=earliest` → filter tombstone → parse → flatten → `writeStream.foreachBatch(apply_cdc)` với checkpoint `/workspace/labs/lab29/ckpt`, trigger 10s. Submit với đủ 3 packages (Kafka + Iceberg runtime + aws-bundle — lệnh mẫu ở lesson 27 Section 5).

### Bước 4 — nghiệm pháp c/u/d/r

```bash
# trong lúc stream chạy, mở psql:
docker exec -it postgres psql -U postgres -d mydb

INSERT INTO olist.sellers (seller_id, zip_code_prefix, city, state)
VALUES ('LAB29X', '10000', 'hanoi', 'HN');            -- op='c'
UPDATE olist.sellers SET city='saigon' WHERE seller_id='LAB29X';   -- op='u'
UPDATE olist.sellers SET city='danang' WHERE seller_id='LAB29X';   -- op='u' (cùng batch? xem dedup!)
DELETE FROM olist.sellers WHERE seller_id='LAB29X';   -- op='d'
```

Sau mỗi lệnh (hoặc dồn dập rồi chờ 1 batch), query bảng Iceberg xác nhận: xuất hiện → đổi city → biến mất. Rồi kiểm chứng snapshot lịch sử:

```sql
SELECT snapshot_id, committed_at, operation, summary['spark.app.id']
FROM iceberg.lab29.sellers.snapshots ORDER BY committed_at;
```

### Bước 5 — phá để hiểu

1. **Bỏ dedup** (MERGE thẳng batch thô), bơm 2 UPDATE sát nhau → hứng lỗi `multiple source rows matched`. Đọc kỹ message lỗi — gặp lại trong production bạn sẽ nhận ra ngay.
2. **Kill & restart** giữa chừng (bài lesson 27): xác nhận bảng vẫn đúng — MERGE idempotent + checkpoint = exactly-once tổ hợp.
3. `ALTER TABLE olist.sellers ADD COLUMN phone text;` rồi UPDATE một row có phone → xem Spark: pipeline sống, nhưng phone đi đâu? Ghi câu trả lời + cách xử lý (section 3.6) vào `labs/lab29/NOTES.md`.

---

## 9. Assignment

**Easy** —
1. Viết tay (không nhìn tài liệu) payload envelope cho: một INSERT, một DELETE, một event snapshot. Ghi rõ before/after/op từng cái.
2. Từ topic lab, dùng `kafka-console-consumer` chép ra 1 event `u` thật; khoanh: PK ở đâu (key hay value?), ts_ms nào là event time, lsn để làm gì.
3. Query-based CDC bỏ sót DELETE — giải thích cho một backend dev trong 5 câu.

**Medium** — MERGE INTO đủ 3 đường: viết test script batch (không cần streaming) tạo bảng đích 3 row, tạo DataFrame changes chứa đồng thời 1 op='c' (key mới), 1 op='u' (key có sẵn), 1 op='d' (key có sẵn), 1 op='d' (key KHÔNG có sẵn — phải không nổ, không insert rác). Chạy MERGE, assert từng kết quả. Chạy MERGE **lần thứ hai cùng input** — chứng minh idempotent.

**Hard** — Schema evolution end-to-end: nguồn thêm cột `phone`. Viết quy trình + code hoàn chỉnh để pipeline ăn được cột mới **không mất event nào**: thứ tự các bước (ALTER Iceberg trước hay sửa code trước? dừng stream lúc nào?), xử lý event cũ không có phone (null), event mới có phone. Bonus: viết "detector" — so sánh raw JSON keys với schema đã khai, đếm event có field lạ, in cảnh báo mỗi batch.

**Production Challenge** — Ordering: đồng nghiệp đề xuất tăng topic từ 3 lên 12 partition để "Spark đọc nhanh hơn". Viết phân tích 15 dòng: chuyện gì xảy ra với per-key ordering tại thời điểm đổi (event cũ của S042 ở partition cũ, event mới sang partition mới — consumer đọc lệch nhịp thì sao)? Cách làm đúng nếu thật sự cần scale (gợi ý: topic mới + snapshot lại, hoặc chốt số partition từ ngày đầu dư dả). Kèm: vì sao dedup (ts_ms,lsn) trong MỘT batch không cứu được ordering vỡ GIỮA các batch?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Thao tác | Nhanh/Chậm | Tại sao |
|---|---|---|
| MERGE mỗi batch 10s vào bảng copy-on-write | Ngày càng chậm nếu không chăm | Mỗi update viết lại cả data file chứa row. Update rải đều bảng to = rewrite nhiều. Thuốc: partition bảng đích khôn (update thường chạm row mới), MoR (lesson 31), compaction (lesson 32). |
| Trigger quá dày (1–5s) | Snapshot bloat + small files | Mỗi batch 1 snapshot Iceberg. CDC hạ nguồn hiếm khi cần <30s. 30s–2min là điểm ngọt phổ biến. |
| Dedup window trong batch | Rẻ | Batch vài nghìn event — window per key trên đó là muỗi. Đừng "tối ưu" bỏ nó. |
| Envelope kèm `schema` (schemas.enable=true) | Message phình 5–10× | Tầng schema lặp lại mỗi message. Tắt đi (lab) hoặc dùng Avro + Registry (production). |
| Snapshot initial bảng 500M row | Chậm & dồn dập lúc đầu | op='r' ồ ạt qua cùng topic. Cân nhắc `snapshot.mode=no_data` + backfill bảng đích bằng Spark batch riêng, rồi stream từ LSN hiện tại. |
| `subscribePattern` nhiều bảng 1 query | Tiện nhưng dính chùm | 1 bảng nghẽn kéo lag cả cụm, restart ảnh hưởng tất cả. Bảng quan trọng: tách query riêng. |

---

## 11. Spark UI

- Tab **Structured Streaming**: theo dõi `numInputRows` từng batch — bảng nguồn im ắng mà vẫn đều đặn vài row? Đó là **heartbeat event** của Debezium (topic riêng `__debezium-heartbeat.*` nếu subscribe pattern rộng) — nhận diện để khỏi hoảng.
- Tab **SQL**: mở plan của MERGE — thấy join giữa target scan và source batch, và các node ghi file. **Số file ghi ra mỗi batch** ở đây là chỉ báo sớm của bệnh small-files.
- `query.lastProgress["sources"][0]`: `startOffset`/`endOffset` per partition — đối chiếu với `kafka-consumer-groups --describe` phía Kafka để tính **lag** (Project 2 checkpoint 6 sẽ tự động hóa việc này).
- Sau vài chục batch, chạy `SELECT count(*) FROM iceberg.lab29.sellers.snapshots` — cảm nhận tốc độ sinh snapshot, nghĩ trước về expire_snapshots (lesson 32).

---

## 12. Common Mistakes

1. **Parse ra toàn null mà pipeline vẫn "xanh"** — sai tầng schema (quên/thừa lớp `payload`), hoặc schema field gõ sai tên. `from_json` không bao giờ kêu. Luôn smoke-test parse trên vài message thật trước khi nối MERGE.
2. **MERGE batch thô không dedup** → `multiple source rows matched`. Hoặc tệ hơn: dedup bằng `dropDuplicates` → giữ event *ngẫu nhiên*, bảng đích thỉnh thoảng "quay ngược thời gian" — bug ma quái bậc nhất.
3. **Dedup chỉ theo ts_ms, quên lsn** — hai thay đổi cùng millisecond (transaction dồn dập) → hên xui. Tie-breaker lsn không phải trang sức.
4. **Quên lọc tombstone** (`value IS NULL`) khi nguồn bật `tombstones.on.delete` → null tràn vào parse, coalesce key ra null, MERGE nổ hoặc rác.
5. **Lấy key delete từ `after`** — after của delete là null. Phải `coalesce(after.pk, before.pk)`.
6. **Nhánh NOT MATCHED không chặn op='d'** → replay/miss tạo row "ma" toàn null trong bảng đích.
7. **Bỏ rơi replication slot** (tắt connector nghỉ lễ, quên xóa slot) → Postgres giữ WAL chờ slot → đầy disk **DB production**. CDC là con dao hai đầu — đầu kia dí vào DB nguồn.
8. **Coi ts_ms là event time vạn năng**: `payload.ts_ms` là lúc Debezium xử lý, KHÔNG phải lúc commit. Dùng `source.ts_ms`. Lẫn hai cái này, dedup vẫn chạy nhưng thống kê độ trễ sai hết.

---

## 13. Interview

**Junior:**

1. *CDC là gì? Log-based hơn query-based chỗ nào?* — Change Data Capture: bắt từng thay đổi (insert/update/delete) của database nguồn thành dòng sự kiện. Log-based đọc transaction log (WAL) nên: bắt được DELETE, thấy mọi thay đổi trung gian, gần như không tải DB nguồn, độ trễ sub-second, đúng thứ tự commit — query-based (poll theo updated_at) trượt cả bốn.
2. *Kể các giá trị của op trong Debezium và before/after tương ứng.* — c: insert (before null, after row); u: update (cả hai); d: delete (before row, after null); r: read/snapshot ban đầu (before null, after hiện trạng). Delete lấy key từ before vì after null.
3. *op='r' xuất hiện khi nào, xử lý thế nào?* — Khi connector snapshot bảng lần đầu (snapshot.mode=initial) — mỗi row có sẵn một event r. Với MERGE xử lý như upsert, hệt c/u.
4. *Tombstone là gì?* — Message value=null Debezium phát sau delete, phục vụ Kafka log compaction xóa hẳn key. Consumer Spark phải lọc trước khi parse (hoặc tắt bằng tombstones.on.delete=false).

**Mid:**

5. *Vì sao phải dedup trong batch trước MERGE, và dedup thế nào cho đúng?* — Một micro-batch có thể chứa nhiều event cùng PK; MERGE cấm nhiều source row khớp một target row, và về logic chỉ event cuối quyết định trạng thái. Đúng: window per PK, order by (source.ts_ms DESC, lsn DESC), lấy row_number=1 — lsn phá hòa cùng millisecond. `dropDuplicates` là sai vì giữ row tùy ý.
6. *Thứ tự event per key được đảm bảo bởi cái gì, qua từng chặng?* — Postgres: WAL theo thứ tự commit (LSN đơn điệu). Debezium: đọc tuần tự, key message = PK. Kafka: cùng key → cùng partition → ordering trong partition. Spark: đọc offset tăng dần per partition, batch xử lý tuần tự. Vỡ nếu: đổi số partition topic giữa chừng, hoặc key không phải PK.
7. *Viết miệng câu MERGE cho CDC — điều kiện các nhánh?* — ON theo PK; WHEN MATCHED AND op='d' THEN DELETE (đứng trước); WHEN MATCHED THEN UPDATE SET từ after; WHEN NOT MATCHED AND op!='d' THEN INSERT từ after. Chặn op='d' ở NOT MATCHED để delete-không-thấy-row bỏ qua êm — cho idempotent.
8. *Nguồn thêm cột, pipeline from_json schema cố định phản ứng ra sao?* — Không lỗi, cột mới bị lờ lặng lẽ → mất data cột đó cho đến khi cập nhật schema code + ALTER bảng Iceberg + sửa MERGE. Nên có detector so raw keys với schema để cảnh báo, hoặc dùng Avro + Schema Registry cho evolution có kiểm soát.

**Senior:**

9. *Thiết kế CDC cho bảng 500M row, update 2k/s, yêu cầu đích trễ <1 phút. Những quyết định then chốt?* — (a) Snapshot: không đổ 500M qua Kafka — no_data + backfill batch song song, stream nối từ LSN chốt; (b) topic partition theo PK, số partition chốt dư từ đầu (không đổi được); (c) trigger ~30s, dedup (ts_ms,lsn); (d) bảng đích: 2k update/s rải rác → copy-on-write rewrite khủng khiếp → MoR + compaction lịch; partition/sort theo pattern update; (e) vận hành: giám sát slot lag (WAL DB nguồn!), consumer lag, và đường re-sync khi vỡ (snapshot lại có kiểm soát). Điểm ăn tiền: nói được rằng nút cổ chai thật thường là MERGE phía đích chứ không phải Kafka/Spark.
10. *Nếu ordering per key bị vỡ (event đến lộn thứ tự giữa các batch), MERGE hiện tại hỏng thế nào và chống sao?* — Batch sau chứa event CŨ hơn row đã áp → MERGE mù quáng ghi đè bằng dữ liệu cũ (dedup nội-batch bó tay vì khác batch). Chống: guard theo version — thêm cột ts_ms/lsn vào bảng đích, nhánh UPDATE thêm điều kiện `AND s.ts_ms >= t.ts_ms` (hoặc so cặp (ts_ms,lsn)), event cũ hơn bị từ chối. Đây là "conditional update / last-writer-wins có kiểm chứng" — cũng chính là lớp giáp cuối cùng khiến pipeline sống sót cả khi hạ tầng phản bội. Trade-off: DELETE cần tombstone version riêng (row đã xóa lấy gì so? — giữ soft-delete hoặc bảng đã-xóa).

---

## 14. Summary

### Mindmap

```
                          CDC + MERGE (L29)
                                │
     ┌───────────────┬──────────┴────────┬───────────────────┐
     ▼               ▼                   ▼                   ▼
  CDC LÀ GÌ      ENVELOPE            PIPELINE SPARK       GUARANTEE
     │               │                   │                   │
 log-based đọc   before/after        from_json (schema    ordering per key:
 WAL > query-    op: c/u/d/r          tường minh, null     PK làm Kafka key
 based (DELETE,  source.ts_ms =       lặng lẽ = bẫy)      → cùng partition
 trung gian,     event time          dedup: (ts_ms,lsn)   đổi partition = vỡ
 tải, trễ)       lsn = tie-breaker    DESC, rn=1           schema evolution:
 slot giữ WAL    tombstone: value    foreachBatch +        from_json lờ field
 (bỏ hoang =     null — lọc!         MERGE: d→DELETE,      lạ → detector +
 đầy disk!)      op='r' = upsert     u→UPDATE, c/u/r→      quy trình ALTER
                                     INSERT (chặn d)
```

### Checklist trước khi gõ "Continue"

- [ ] Viết tay được envelope của c/u/d/r, chỉ đúng event time và tie-breaker.
- [ ] Giải thích được chuỗi ordering per key từ WAL đến Iceberg, và điều gì phá nó.
- [ ] Thuộc câu MERGE 3 nhánh + lý do thứ tự nhánh + lý do chặn op='d' ở NOT MATCHED.
- [ ] Đã chạy lab: INSERT/UPDATE/DELETE ở Postgres hiện vào Iceberg, và làm cả 3 bài "phá để hiểu".
- [ ] Biết dedup đúng kiểu (window ts_ms+lsn) và vì sao dropDuplicates là sai.
- [ ] Nói được 2 tai nạn hạ tầng: slot bỏ hoang đầy disk, đổi partition vỡ ordering.
- [ ] Trả lời 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Project 2 (FULL) — CDC Lakehouse.**

Hết bài lẻ — giờ là trận đánh thật. Bạn sẽ dựng trọn con đường PostgreSQL → Debezium → Kafka → Spark → Iceberg → Trino trên hạ tầng của chính mình, thêm hai boss mới: **SCD Type 2** (bảng đích không chỉ là bản sao mà giữ cả lịch sử valid_from/valid_to — câu MERGE sẽ xoắn não hơn hôm nay một bậc) và **monitoring lag có alert** (pipeline không ai canh là pipeline đã chết, chỉ chưa ai biết). Sáu checkpoint, mỗi cái có tiêu chí nghiệm thu — làm đến đâu chắc đến đó.

> Gõ **"Continue"** khi sẵn sàng.
