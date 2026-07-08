# Project 2 (FULL) — CDC Lakehouse

> Module 4 · Structured Streaming · Tuần 15 · Thời lượng: 12–16 giờ (trải 4–6 buổi)

---

## 1. Mục tiêu

Dựng **trọn vẹn** một CDC Lakehouse production-grade trên máy của bạn:

```
PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Iceberg → Trino
```

Đây là bài tổng kết Module 4 — không có kiến thức mới, chỉ có **kiến thức cũ dưới áp lực thật**:

- Exactly-once end-to-end (lesson 27): kill bất kỳ mắt xích nào, dữ liệu không mất không trùng.
- Debezium envelope + dedup + MERGE INTO (lesson 29): bản sao bảng nguồn luôn đúng.
- **Nâng cấp mới #1 — SCD Type 2**: bảng đích không chỉ là bản sao hiện trạng mà giữ **toàn bộ lịch sử** với `valid_from`/`valid_to`/`is_current`.
- **Nâng cấp mới #2 — Monitoring**: đo lag (max_offset − committed_offset), alert khi trễ quá 10 phút. Pipeline không có mắt canh là pipeline đã chết mà chưa ai biết.
- Trino đứng cuối: query bản sao, query lịch sử SCD2, và **time travel** trên snapshot Iceberg.

Sau project này, bạn có một repo demo mà mang đi phỏng vấn Senior DE là **nói chuyện được 45 phút không cạn**: từ WAL, slot, ordering, exactly-once, MERGE, SCD2, đến vận hành.

**Điều kiện tiên quyết** (thiếu cái nào thì quay lại bài đó trước khi bắt đầu):

- [ ] Lab lesson 27 đã pass kill & restart không duplicate (foreachBatch + MERGE + checkpoint).
- [ ] Lab lesson 29 đã chạy trọn c/u/d/r từ Postgres vào Iceberg, kể cả 3 bài "phá để hiểu".
- [ ] Thuộc câu MERGE 3 nhánh và lý do dedup theo (ts_ms, lsn).
- [ ] Repo `../kafka-flink` up được đủ: postgres, broker, kafka-connect, minio, iceberg-rest, trino.
- [ ] Đã nối network 2 cụm Docker và ping thông `broker` từ container Spark.

**Quy tắc chơi**: làm tuần tự 6 checkpoint, mỗi checkpoint có **tiêu chí nghiệm thu** — tự chấm đạt rồi mới đi tiếp. Bí quá được phép xem lại lesson 27–29, không được phép copy code cũ mà không hiểu. Mỗi checkpoint hoàn thành, commit code một lần với message rõ ràng — lịch sử commit cũng là deliverable ngầm.

---

## 2. Kiến trúc

```
┌─────────────────────────── hạ tầng ../kafka-flink (docker compose) ───────────────────────────┐
│                                                                                               │
│  PostgreSQL (:5555 host / postgres:5432)     Kafka Connect + Debezium (:8083)                 │
│  ┌──────────────────────┐   WAL (logical)   ┌─────────────────────────┐                       │
│  │ olist.sellers        │ ────────────────► │ connector: p2-sellers   │                       │
│  │ (PK, REPLICA         │   slot: p2_slot   │ JsonConverter,          │                       │
│  │  IDENTITY FULL)      │                   │ snapshot.mode=initial   │                       │
│  └──────────────────────┘                   └───────────┬─────────────┘                       │
│                                                         │ produce (key = PK)                  │
│                                                         ▼                                     │
│                                             Kafka broker (broker:29092)                       │
│                                             topic: p2.olist.sellers (3 partitions)            │
│                                                         │                                     │
└─────────────────────────────────────────────────────────┼─────────────────────────────────────┘
                                                          │ (docker network connect)
┌───────────────────────── cluster spark-mastery ─────────┼─────────────────────────────────────┐
│                                                         ▼                                     │
│   Spark Structured Streaming  (labs/project2/)                                                │
│   ① parse envelope → ② dedup (ts_ms, lsn) → ③ foreachBatch:                                   │
│        MERGE INTO  iceberg.p2.sellers          (bản sao hiện trạng — lesson 29)               │
│        MERGE/INSERT iceberg.p2.sellers_history (SCD Type 2 — valid_from/valid_to)             │
│   ④ monitor: lag = max_offset − committed_offset → alert > 10 min                             │
└───────────────────────────────────────┬───────────────────────────────────────────────────────┘
                                        │ atomic snapshot commit
                                        ▼
                  Iceberg REST catalog (:8181) + MinIO (:9000)
                                        │
                                        ▼
                  Trino (:8080 của kafka-flink) — query hiện trạng,
                  lịch sử SCD2, và TIME TRAVEL theo snapshot
```

Chuẩn bị hạ tầng (một lần):

```bash
cd ../kafka-flink && docker compose up -d postgres broker kafka-connect minio minio-init iceberg-rest trino
cd ../spark-mastery && make up
for c in spark-submit-1 spark-master-1 spark-worker-1; do
  docker network connect kafka-flink_confluent spark-mastery-$c; done   # tên network: docker network ls
```

Code đặt tại `labs/project2/`. Submit như lesson 27 Section 5 (3 packages: kafka, iceberg-spark-runtime, iceberg-aws-bundle + `spark.jars.ivy=/workspace/.ivy2`).

---

## 3. Checkpoint chi tiết

### ✅ Checkpoint 1 — PostgreSQL + Debezium connector (enable CDC)

**Việc phải làm:**

1. Chạy `cdc/olist-cdc-setup.sql` của repo `kafka-flink` (idempotent) — schema `olist`, role `cdc_user`, `REPLICA IDENTITY FULL`, publication `olist_pub`. Seed bảng sellers từ CSV Olist (~3k row) nếu chưa có.
2. Viết `labs/project2/p2-connector.json`: nhái connector lesson 29 nhưng `slot.name=p2_slot`, `topic.prefix=p2`, JsonConverter với `schemas.enable=false`, `heartbeat.interval.ms=10000`, `tombstones.on.delete=false`. Đăng ký qua REST `:8083` bằng PUT.
3. Kiểm tra sức khỏe: status connector RUNNING, và slot xuất hiện trong Postgres.

**Tiêu chí nghiệm thu:**

- [ ] `curl :8083/connectors/p2-sellers/status` → `"state": "RUNNING"` ở cả connector lẫn task.
- [ ] `SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots;` thấy `p2_slot` active.
- [ ] Trả lời được (viết vào README): vì sao dùng slot RIÊNG cho project thay vì dùng chung slot của Flink? Chuyện gì xảy ra với disk của Postgres nếu bạn tắt connector này 2 tuần mà không xóa slot?

### ✅ Checkpoint 2 — Kafka topic nhận Debezium events (JSON)

**Việc phải làm:**

1. Xác nhận topic `p2.olist.sellers` được tạo, xem số partition.
2. Consume 5 message đầu bằng `kafka-console-consumer` với `print.key=true`: đối chiếu từng field với lesson 29 section 3.2.
3. Gây đủ 4 loại event: sẵn có `op='r'` (snapshot ~3k row); tự INSERT/UPDATE/DELETE vài row để có `c`/`u`/`d`. Chép mỗi loại 1 message vào `labs/project2/samples/`.

**Tiêu chí nghiệm thu:**

- [ ] Có 4 file sample tương ứng r/c/u/d, kèm chú thích tay: key ở đâu, before/after, source.ts_ms vs payload.ts_ms, lsn.
- [ ] Chỉ ra được message key = PK và giải thích 3 câu vì sao điều đó quyết định ordering (lesson 29 §3.3).
- [ ] `kafka-consumer-groups --describe` chưa có group nào của Spark (chưa chạy) — hiểu vì sao Spark quản offset bằng checkpoint chứ không bằng consumer group commit.

### ✅ Checkpoint 3 — Spark parse Debezium, convert insert/update/delete

**Việc phải làm:**

1. `labs/project2/streaming_job.py`: readStream Kafka → lọc tombstone → `from_json` envelope schema tường minh → flatten (`coalesce(after.pk, before.pk)` cho key) → dedup per key theo `(source.ts_ms, lsn)` DESC.
2. Giai đoạn này sink tạm là console (biết là at-least-once — đủ để debug parse).
3. Smoke test bắt buộc: đếm `parsed.filter("op IS NULL")` — phải bằng 0 (chống lỗi "null lặng lẽ" của from_json).

**Tiêu chí nghiệm thu:**

- [ ] Console hiện đủ 3k+ event snapshot (`op='r'`) khi đọc `earliest`, và các event c/u/d bạn gây ra realtime (< 15 giây từ psql đến console).
- [ ] Bơm 3 UPDATE liên tiếp vào cùng row trong 1 trigger interval → console (sau dedup) chỉ còn **1 row** cho key đó, đúng bản cuối.
- [ ] Trả lời: nếu 2 update cùng `source.ts_ms`, cái gì quyết định? Chứng minh bằng demo nhỏ lesson 29 §6.

### ✅ Checkpoint 4 — MERGE INTO Iceberg + SCD Type 2

Trái tim của project. Hai bảng đích, **cùng một foreachBatch**:

**Bảng 1 — bản sao hiện trạng** `iceberg.p2.sellers` (chính là lesson 29):

```sql
MERGE INTO iceberg.p2.sellers t USING changes s ON t.seller_id = s.seller_id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED AND s.op != 'd' THEN INSERT ...
```

**Bảng 2 — lịch sử SCD Type 2** `iceberg.p2.sellers_history`:

```sql
CREATE TABLE IF NOT EXISTS iceberg.p2.sellers_history (
    seller_id string, zip_code_prefix string, city string, state string,
    valid_from timestamp,    -- source.ts_ms của event mở version này
    valid_to   timestamp,    -- null khi đang hiệu lực; ts đóng khi bị thay thế/xóa
    is_current boolean,
    op string                -- op mở version (r/c/u) — tiện audit
) USING iceberg
```

Logic SCD2 mỗi batch — làm bằng **2 bước** trong foreachBatch (dễ đúng hơn 1 câu MERGE xoắn):

```
Bước A — ĐÓNG version cũ: MERGE INTO history t USING changes s
         ON t.seller_id = s.seller_id AND t.is_current = true
         WHEN MATCHED THEN UPDATE SET
              t.valid_to = s.change_ts, t.is_current = false
         (mọi event u/d đều đóng version hiện tại; event c/r cho key mới
          không MATCHED nên vô hại)

Bước B — MỞ version mới: INSERT INTO history
         SELECT ..., change_ts AS valid_from, NULL AS valid_to, true AS is_current
         FROM changes WHERE op != 'd'
         (delete chỉ đóng, không mở — row "chết" không có version current)
```

Gợi ý gài sẵn (đọc sau khi tự nghĩ 30 phút): `change_ts = to_timestamp(source.ts_ms / 1000)`. Bước A chạy trước B. Cả hai bước đều trong cùng foreachBatch → replay batch thì Bước A idempotent (đóng cái đã đóng = no-op vì `is_current=true` không còn match)... nhưng Bước B là INSERT thuần — **replay sẽ nhân đôi version mới**. Đây là lỗ hổng cố ý để lại: bịt nó là một tiêu chí nghiệm thu (gợi ý: chống trùng bằng MERGE theo `(seller_id, valid_from)` thay vì INSERT, hoặc điều kiện NOT EXISTS).

**Tiêu chí nghiệm thu:**

- [ ] UPDATE 1 seller 2 lần (cách nhau > 1 trigger) → `sellers` có bản cuối; `sellers_history` có 3 version: 2 đã đóng (valid_to đặt đúng, is_current=false) + 1 current.
- [ ] DELETE seller đó → `sellers` mất row; `history` mọi version đóng hết, không version nào current.
- [ ] `SELECT seller_id FROM sellers_history WHERE is_current GROUP BY seller_id HAVING count(*) > 1` → **rỗng** (bất biến SCD2 số 1).
- [ ] **Kill & restart test** (nghi thức lesson 27): kill giữa batch, restart, chạy lại 2 query trên + so `count(*)` history trước/sau — không trùng version. Nếu trùng: bạn chưa bịt lỗ hổng Bước B.
- [ ] Số dòng `sellers` == số `is_current=true` trong history (bất biến số 2 — viết thành script check).

### ✅ Checkpoint 5 — Trino query + time travel

**Việc phải làm:**

1. Vào Trino (`docker exec -it trino trino` — catalog iceberg đã trỏ sẵn REST catalog của repo kafka-flink).
2. Query hiện trạng + lịch sử: bản sao khớp Postgres; câu hỏi SCD2 kiểu "seller S042 ngày X thuộc city nào?" (`WHERE ts BETWEEN valid_from AND coalesce(valid_to, now())`).
3. Time travel: liệt kê snapshot rồi query bảng **như quá khứ**:

```sql
SELECT snapshot_id, committed_at, operation FROM iceberg.p2."sellers$snapshots" ORDER BY committed_at;
SELECT * FROM iceberg.p2.sellers FOR VERSION AS OF <snapshot_id> WHERE seller_id = 'S042';
SELECT * FROM iceberg.p2.sellers FOR TIMESTAMP AS OF TIMESTAMP '2026-07-08 10:00:00 UTC' WHERE seller_id = 'S042';
```

**Tiêu chí nghiệm thu:**

- [ ] `count(*)` từ Trino khớp `count(*)` từ psql trên bảng nguồn (bản sao đúng).
- [ ] Chứng minh time travel bằng một câu chuyện cụ thể: UPDATE city lúc T → query `FOR VERSION AS OF` snapshot trước T thấy city cũ, snapshot sau T thấy city mới. Chụp/paste cả hai kết quả.
- [ ] Trả lời (viết vào README): time travel theo snapshot vs lịch sử SCD2 — khác nhau gì, khi nào dùng cái nào? (Gợi ý: snapshot bị expire theo maintenance thì time travel còn không? SCD2 thì sao? Snapshot là lịch sử *của bảng* theo batch commit; SCD2 là lịch sử *của nghiệp vụ* theo event time.)

### ✅ Checkpoint 6 — Monitor lag + alert > 10 phút

**Việc phải làm:**

1. `labs/project2/lag_monitor.py` — script Python (chạy host hoặc container, dùng `kafka-python`) lặp mỗi 30 giây:
   - **max_offset**: `end_offsets` từng partition của topic (hỏi thẳng broker).
   - **committed_offset của Spark**: KHÔNG nằm ở consumer group — đọc file checkpoint mới nhất trong `labs/project2/ckpt/offsets/` (JSON per-partition — chính là WAL lesson 27) hoặc từ `query.lastProgress` nếu bạn nhúng monitor vào streaming job.
   - `lag_messages = Σ(max_offset − committed_offset)`; **lag_time** = now − timestamp của message tại committed offset (hoặc gần đúng: now − `source.ts_ms` cuối cùng đã MERGE — ghi giá trị này ra một bảng/file từ foreachBatch).
2. Alert: `lag_time > 10 phút` → in `ALERT` + ghi `labs/project2/alerts.log` (bonus: gửi webhook Slack/Discord).
3. **Diễn tập cháy**: tắt streaming job, bơm data liên tục vào Postgres 12 phút → alert phải kêu. Bật lại job → xem lag tụt về ~0, alert tự hết.

**Tiêu chí nghiệm thu:**

- [ ] Monitor in mỗi 30s: per-partition committed/max/lag + lag_time ước tính.
- [ ] Diễn tập cháy pass: alert xuất hiện sau >10 phút tắt job, biến mất sau khi job đuổi kịp. Paste log làm bằng chứng.
- [ ] Trả lời: vì sao KHÔNG đọc lag từ `kafka-consumer-groups --describe` với Spark? (Spark quản offset trong checkpoint, không commit consumer group — group trong describe chỉ là group tạm mỗi lần chạy, không phản ánh tiến độ commit thật.)
- [ ] Trả lời: lag_messages = 0 có nghĩa pipeline khỏe không? (Chưa — connector chết thì không có message mới, lag 0 mà data trễ vô hạn. Vì thế cần cả lag_time theo ts_ms nguồn + giám sát slot/connector — viết 5 dòng thiết kế "3 tầng mắt canh": connector status, slot lag, consumer lag.)

---

### Lộ trình gợi ý theo buổi (nếu bạn học 2–3h/buổi)

| Buổi | Việc | Bẫy thời gian cần né |
|---|---|---|
| 1 | Checkpoint 1 + 2: hạ tầng, connector, soi envelope | Network giữa 2 cụm Docker — làm bước `docker network connect` NGAY, đừng để đến lúc Spark timeout mới đi tìm |
| 2 | Checkpoint 3: parse + dedup, sink console | "Null lặng lẽ" của from_json — smoke test trước khi viết tiếp |
| 3 | Checkpoint 4a: MERGE bản sao (chạy lại cơ lesson 29 cho nhuyễn) | Quên extension Iceberg trong SparkSession |
| 4 | Checkpoint 4b: SCD2 hai bước + bịt lỗ hổng replay | Đây là buổi khó nhất — nếu 1 câu MERGE xoắn không ra, cứ tách 2 bước như đề gợi ý |
| 5 | Checkpoint 5: Trino + time travel; chạy trọn test_plan | Trino catalog phải trỏ cùng REST catalog — nếu không thấy bảng, kiểm tra namespace |
| 6 | Checkpoint 6: monitor + diễn tập cháy; viết README | Diễn tập cháy cần 12+ phút đồng hồ thật — bật monitor rồi đi pha cà phê, đừng ngồi nhìn |

### Troubleshooting nhanh (các vết xe đổ của khóa trước)

| Triệu chứng | Nguyên nhân thường gặp | Hướng xử lý |
|---|---|---|
| Spark báo `UnknownHostException: broker` | Container Spark chưa nối network `kafka-flink_confluent` | `docker network connect` cả 3 container; verify bằng `docker exec ... ping broker` |
| Parse ra DataFrame toàn null, không lỗi | Sai tầng schema (`schemas.enable` của connector vs schema from_json) | Consume 1 message bằng console-consumer, so cấu trúc thật với schema đã khai |
| `MERGE INTO TABLE is not supported` | Thiếu `IcebergSparkSessionExtensions` trong config | Thêm config extensions + kiểm tra đủ 2 package Iceberg |
| `multiple source rows matched` lúc MERGE | Dedup thiếu hoặc dedup sai cột key | Kiểm tra window partitionBy đúng PK, orderBy (ts_ms, lsn) DESC |
| Topic không có event mới dù đã UPDATE | Connector FAILED (xem status), hoặc UPDATE vào bảng ngoài publication | `curl :8083/.../status`; `SELECT * FROM pg_publication_tables` |
| Bảng history nhân đôi version sau restart | Lỗ hổng Bước B chưa bịt (INSERT thuần không idempotent) | Đọc lại Checkpoint 4 — chuyển INSERT thành MERGE theo (seller_id, valid_from) |
| Trino không thấy bảng `p2.sellers` | Khác catalog/namespace giữa Spark và Trino | So URI REST catalog 2 phía; `SHOW SCHEMAS FROM iceberg;` |
| Postgres disk tăng đều dù ít data | Slot bỏ hoang giữ WAL (connector tắt lâu) | `pg_replication_slots` → drop slot không dùng; bài học Checkpoint 1 |
| Job restart đọc lại từ đầu topic | Checkpoint dir bị đổi đường dẫn/xóa | Checkpoint là ký ức của stream (lesson 27) — cố định đường dẫn trong `/workspace` |

---

## 4. Deliverable

Nộp thư mục `labs/project2/` gồm:

| # | File | Nội dung |
|---|---|---|
| 1 | `README.md` | Kiến trúc (vẽ lại bằng tay, không copy), cách chạy từ zero, và các câu trả lời nghiệm thu |
| 2 | `p2-connector.json` + lệnh đăng ký | Debezium setup |
| 3 | `samples/` | 4 envelope r/c/u/d có chú thích |
| 4 | `streaming_job.py` | parse → dedup → foreachBatch (MERGE bản sao + SCD2) |
| 5 | `ddl.sql` | DDL 2 bảng Iceberg + chú thích từng cột SCD2 |
| 6 | `lag_monitor.py` + `alerts.log` | Monitoring + bằng chứng diễn tập cháy |
| 7 | `test_plan.md` | Kịch bản test đã chạy: c/u/d/r, dedup trong batch, kill & restart, 2 bất biến SCD2, time travel, diễn tập lag — mỗi kịch bản: bước làm, kỳ vọng, kết quả thật |

`test_plan.md` là thứ interviewer nhìn lâu nhất — nó chứng minh bạn **vận hành** được chứ không chỉ code được.

---

## 5. Rubric Senior

Tự chấm (rồi mentor chấm lại) — thang 100:

| Hạng mục | Điểm | Đạt khi |
|---|---|---|
| Checkpoint 1–2: nguồn CDC | 10 | Connector RUNNING, hiểu slot/publication/heartbeat, 4 sample có chú thích đúng |
| Checkpoint 3: parse + dedup | 15 | Schema tường minh, chặn tombstone, dedup (ts_ms,lsn) chuẩn, có smoke test null |
| Checkpoint 4: MERGE + SCD2 | 25 | 2 bất biến SCD2 pass; **kill & restart không trùng** (lỗ hổng Bước B đã bịt có giải thích) |
| Checkpoint 5: Trino + time travel | 10 | Query khớp nguồn; demo time travel 2 snapshot; phân biệt snapshot vs SCD2 |
| Checkpoint 6: monitoring | 15 | Lag đo đúng nguồn (checkpoint offsets, không phải consumer group); diễn tập cháy pass |
| Chất lượng vận hành | 15 | test_plan.md đầy đủ kết quả thật; README chạy lại được từ zero; config không hardcode bừa (bootstrap, catalog URI tách biến) |
| Chiều sâu hiểu | 10 | Trả lời trôi chảy các câu nghiệm thu "vì sao"; chỉ được điểm gãy exactly-once nếu bị hỏi vặn |

Ngưỡng: **< 60** — làm lại checkpoint hổng trước khi đi tiếp; **60–79** — đạt, đọc kỹ feedback; **80+** — chuẩn Senior, project đủ tư cách nằm trên CV. **Điểm liệt**: kill & restart bị trùng data mà test_plan ghi "pass" — trung thực với kết quả test là phẩm chất số 1 của DE.

---

## 6. Câu hỏi mở rộng (nghĩ trước khi vào Module 5)

1. **Scale bảng**: nguồn có 50 bảng thay vì 1 — bạn nhân bản 50 query hay 1 query `subscribePattern` rẽ nhánh? Chi phí vận hành/checkpoint/lag từng phương án? Có tạo được "framework" config-driven (bảng, PK, schema) không?
2. **Backfill khổng lồ**: bảng 500M row — snapshot initial qua Kafka là dở (lesson 29 §10). Thiết kế quy trình backfill bằng Spark batch + nối stream từ LSN chốt sao cho không hở không trùng khe nối.
3. **Ordering bị phản bội**: ai đó tăng partition topic từ 3 lên 12 khi đang chạy. Bảng đích hỏng kiểu gì, phát hiện bằng bất biến nào, và lớp giáp `AND s.ts_ms >= t.ts_ms` trong MERGE (lesson 29 câu interview 10) cứu được bao nhiêu phần?
4. **Small files & snapshot bloat**: trigger 30s = ~2.880 snapshot/ngày/bảng. Bao lâu thì Trino chậm? Đây chính là cửa vào Module 5: compaction, expire_snapshots, rewrite manifests (lesson 32).
5. **Schema evolution có quy trình**: nguồn thêm cột lúc 2 giờ sáng. Viết runbook: detector cảnh báo → ALTER Iceberg → deploy schema mới → backfill cột null — thứ tự nào không mất event?
6. **Multi-sink thật sự**: đội risk muốn thêm Kafka alert cho seller đổi state — nhét vào foreachBatch hiện tại (không atomic! lesson 27 §3.6) hay outbox từ bảng history? Vẽ 2 phương án, chọn 1, ghi lý do.
7. **So găng với Flink**: repo `kafka-flink` làm chính việc này bằng Flink. Sau khi tự tay làm bằng Spark: liệt kê 3 điểm Flink CDC sướng hơn (per-event latency? Flink CDC connector đọc thẳng WAL không cần Kafka?) và 3 điểm Spark sướng hơn (một engine batch+stream? foreachBatch + MERGE? hệ sinh thái?). Không có đáp án đúng — chỉ có trade-off nói được thành lời.

---

## 7. Next: Module 5

**Lesson 30 — Iceberg internals: metadata, snapshot, manifest.**

Suốt Module 4, Iceberg là "cái hộp đen tin cậy được": atomic commit, skip batch trùng, time travel. Module 5 mở hộp: `metadata.json` trỏ đi đâu, snapshot chứa manifest list gì, manifest liệt kê data file ra sao — và vì sao hiểu 3 tầng đó xong thì compaction, hidden partitioning, MoR vs CoW đều trở nên hiển nhiên. Bảng `iceberg.p2.sellers` bạn vừa nuôi bằng CDC chính là con chuột bạch: lesson 30 sẽ mổ đúng thư mục metadata của nó — hàng nghìn snapshot bạn vừa tạo ra tuần này giờ thành giáo cụ.

> Gõ **"Continue"** khi sẵn sàng.
