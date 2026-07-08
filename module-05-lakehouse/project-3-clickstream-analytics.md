# Project 3 (FULL) — Clickstream Analytics

> Module 5 · Lakehouse & Iceberg · Tuần 19 · Thời lượng: 12–16 giờ (project, làm trong 4–6 buổi)

---

## 1. Đề bài

Xây trọn một nền tảng phân tích hành vi người dùng web — từ event đầu tiên đến biểu đồ funnel cuối cùng:

```
 event generator ──► Kafka ──► Spark Structured Streaming ──► ICEBERG ──► Trino ──► funnel chart
 (Python, 1000        topic     - đọc stream                  bronze      SQL        Step1→2→3
  events/sec)         clicks    - sessionization (state,      silver                 conversion %
                                  30-min timeout)             gold          ▲
                                                                │           │
                                                     AIRFLOW ───┘ (daily: compaction,
                                                                   funnel + retention metrics)
```

Đây là bài toán thật 100%: mọi công ty có website/app đều có pipeline này (Shopee, Tiki, báo điện tử, ngân hàng số). Bạn dùng đồ nghề đã học: Kafka source + stateful streaming (module 4), Iceberg + medallion + QC + Airflow (module 5).

**Luật chơi**: project là bài KHÔNG có lời giải mẫu đầy đủ. Mentor cấp: đề, kiến trúc, code generator, skeleton chỗ khó nhất, checkpoint và rubric. Phần còn lại là của bạn — bí ở đâu, hỏi ở đó.

### Chuẩn bị hạ tầng

- Kafka + Trino: dùng hạ tầng có sẵn ở repo `../kafka-flink` (bật Kafka broker, Trino có catalog Iceberg trỏ cùng warehouse). Airflow: container `airflow-lab` từ lesson 36.
- Spark: cluster `make up` như mọi khi. Packages khi submit job streaming:
  `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3`
- Code đặt tại `labs/project-clickstream/` (generator/, jobs/, dags/, sql/, NOTES.md).

### Kế hoạch 6 buổi (gợi ý, tự điều chỉnh)

| Buổi | Việc | Checkpoint |
|---|---|---|
| 1 | Dựng hạ tầng (Kafka, topic, Trino, Airflow), chạy generator, đo throughput | 1, 2 |
| 2 | Bronze streaming job + kiểm chứng raw vào Iceberg | 4 (nửa đầu) |
| 3–4 | Sessionization — dành hẳn 2 buổi, đây là phần khó nhất; bắt đầu với rate thấp + timeout ngắn | 3 |
| 5 | Silver QC + gold funnel/retention + time travel | 4, 5 |
| 6 | Airflow DAG + Trino funnel + viết NOTES, chuẩn bị demo | 6, 7 |

Nguyên tắc làm project: **mỗi buổi kết thúc bằng một thứ CHẠY ĐƯỢC end-to-end ở quy mô nhỏ**, rồi mới tăng scale/độ phức tạp. Đừng viết 7 job xong mới chạy lần đầu.

---

## 2. Checkpoint 1 — Event generator

**Yêu cầu**: sinh event `user_id, event_type (view/click/purchase), timestamp` mô phỏng hành vi thật — user vào theo "phiên", xem vài trang, đôi khi click, hiếm khi mua. Code cấp sẵn (đọc hiểu từng dòng — interview hỏi được đấy):

```python
# labs/project-clickstream/generator/gen_events.py
"""Sinh clickstream: user hoạt động theo phiên, funnel view -> click -> purchase."""
import json, random, time, uuid, argparse
from datetime import datetime, timezone

PAGES = ["/home", "/search", "/product/101", "/product/202",
         "/product/303", "/cart", "/checkout"]

def make_user_pool(n):          # 500 user, mỗi người một "độ nghiện" khác nhau
    return [{"user_id": f"u{i:04d}", "activity": random.uniform(0.2, 1.0)}
            for i in range(n)]

def next_event(user, last_type):
    """Chuỗi hành vi có logic funnel: phải view rồi mới click, click rồi mới purchase."""
    r = random.random()
    if last_type is None or last_type == "purchase":
        etype = "view"
    elif last_type == "view":
        etype = "click" if r < 0.30 else "view"          # 30% view -> click
    else:  # last = click
        etype = "purchase" if r < 0.15 else ("click" if r < 0.40 else "view")
    return {
        "event_id":   str(uuid.uuid4()),
        "user_id":    user["user_id"],
        "event_type": etype,
        "page":       random.choice(PAGES),
        "event_ts":   datetime.now(timezone.utc).isoformat(),
    }

def main(rate, bootstrap, topic):
    from kafka import KafkaProducer                       # pip install kafka-python
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode(),              # key = user_id
        value_serializer=lambda v: json.dumps(v).encode())
    users, last_type, sent = make_user_pool(500), {}, 0
    print(f"Producing ~{rate} events/sec to {topic} ...")
    while True:
        t0 = time.time()
        for _ in range(rate):
            user = random.choice(users)
            if random.random() > user["activity"]:        # user "lười" bỏ lượt
                continue
            evt = next_event(user, last_type.get(user["user_id"]))
            last_type[user["user_id"]] = evt["event_type"]
            producer.send(topic, key=evt["user_id"], value=evt)
            sent += 1
        producer.flush()
        if sent % (rate * 10) < rate:
            print(f"sent={sent:,}")
        time.sleep(max(0.0, 1.0 - (time.time() - t0)))    # giữ nhịp ~rate/giây

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=int, default=1000)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="clickstream.events")
    a = p.parse_args()
    main(a.rate, a.bootstrap, a.topic)
```

**Đạt checkpoint khi**: giải thích được vì sao **key = user_id** (mọi event của 1 user vào cùng Kafka partition → giữ thứ tự per-user — điều kiện sống còn cho sessionization), và vì sao generator mô phỏng funnel chứ không random đều (gold layer cần dữ liệu có cấu trúc để funnel ra số có nghĩa).

## 3. Checkpoint 2 — Stream 1000 events/sec vào Kafka

- Tạo topic `clickstream.events` với **6 partition** (đủ chỗ cho Spark song song).
- Chạy `python gen_events.py --rate 1000`, kiểm chứng bằng console consumer + đo throughput thật (offset cuối − offset đầu qua 60 giây ÷ 60).
- **Đạt khi**: ≥ 900 events/sec bền vững trong 5 phút; nêu được nếu cần 100k events/sec thì thay đổi gì (nhiều producer, `linger.ms`/batching, nhiều partition).

## 4. Checkpoint 3 — Stateful sessionization, timeout 30 phút

Trái tim của project. **Session** = chuỗi event liên tiếp của 1 user, kết thúc khi user im lặng quá 30 phút. Batch không làm được chuyện này theo thời gian thực — cần **state** sống giữa các micro-batch.

Gợi ý dùng `applyInPandasWithState` (Spark 3.4): group theo `user_id`, state giữ session dở dang, timeout đóng session:

```python
# labs/project-clickstream/jobs/sessionize.py — SKELETON, tự hoàn thiện
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

# state schema:  session_id, start_ts, last_ts, n_view, n_click, n_purchase, path
# output schema: user_id, session_id, start_ts, end_ts, duration_sec,
#                n_view, n_click, n_purchase, path (chuỗi trang đã qua)

def sessionize(key, pdf_iter, state: GroupState):
    if state.hasTimedOut:                 # 30 phút im lặng -> ĐÓNG session
        (sid, start, last, nv, nc, np_, path) = state.get
        state.remove()
        yield make_output_pdf(key, sid, start, last, nv, nc, np_, path)
        return
    for pdf in pdf_iter:                  # gom event mới vào session đang mở
        pdf = pdf.sort_values("event_ts")
        # TODO: nếu state trống -> mở session mới (session_id = uuid? hash?)
        #       nếu event cách last_ts > 30 phút -> đóng session cũ (yield), mở mới
        #       cập nhật counters + path, state.update((...))
    state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 30 * 60 * 1000)
    # yield pd.DataFrame() nếu chưa có session nào đóng trong batch này

sessions = (events                         # events: đã parse JSON + watermark
    .withWatermark("event_ts", "10 minutes")
    .groupBy("user_id")
    .applyInPandasWithState(sessionize, OUTPUT_SCHEMA, STATE_SCHEMA,
                            "append", GroupStateTimeout.EventTimeTimeout))
```

Câu hỏi bắt buộc trả lời trong NOTES.md: watermark 10 phút đóng vai gì so với session timeout 30 phút? State nằm ở đâu khi job restart (checkpoint location!)? Vì sao demo nên hạ timeout xuống 2–3 phút khi thử nghiệm?

**Đạt khi**: tắt generator 3 phút (timeout thử nghiệm) → session tự đóng và xuất hiện ở output; restart streaming job → không mất session dở dang, không double.

## 5. Checkpoint 4 — Medallion: bronze → silver → gold

Áp nguyên contract lesson 34:

- **bronze.events**: streaming job 1 ghi raw Kafka (key, value, topic, partition, offset, timestamp) vào Iceberg, `toTable()` + checkpoint, partition `days(kafka_ts)`. Không parse, không lọc. Skeleton:

```python
# labs/project-clickstream/jobs/bronze_stream.py — SKELETON
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "clickstream.events")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING) AS user_id", "CAST(value AS STRING) AS payload",
                "topic", "partition", "offset", "timestamp AS kafka_ts"))

q = (raw.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/workspace/warehouse/_checkpoints/bronze_events")
    .trigger(processingTime="30 seconds")     # trigger dài hơn = ít small files hơn
    .toTable("lake.bronze.events"))
q.awaitTermination()
```

Câu hỏi tự vấn: vì sao checkpoint location PHẢI cố định và mỗi query một thư mục riêng? Đổi logic query rồi giữ checkpoint cũ thì chuyện gì xảy ra? (Ôn module 4 — đây là lỗi số 1 của mọi streaming job.)
- **silver.sessions**: chính là output sessionization (job 2 đọc lại từ Kafka hoặc từ bronze) — mỗi dòng 1 session ĐÃ ĐÓNG. Grain tuyên bố rõ: *1 dòng = 1 session*. Kèm QC (lesson 35): unique session_id, n_view ≥ 0, end_ts ≥ start_ts, freshness.
- **gold** (batch job, chạy bởi Airflow):
  - `gold.funnel_daily` — grain: 1 dòng = 1 ngày × 1 bước funnel: `sessions_total, sessions_with_view, sessions_with_click, sessions_with_purchase` + conversion %.
  - `gold.retention_weekly` — cohort retention: user xuất hiện tuần W có quay lại tuần W+1, W+2...? (grain: cohort_week × offset_week). Bộ khung tính cohort — hoàn thiện nốt phần TODO:

```sql
WITH first_seen AS (          -- tuần đầu tiên mỗi user xuất hiện = cohort
  SELECT user_id, date_trunc('week', MIN(start_ts)) AS cohort_week
  FROM lake.silver.sessions GROUP BY user_id
),
activity AS (                 -- mọi tuần user có hoạt động
  SELECT DISTINCT user_id, date_trunc('week', start_ts) AS active_week
  FROM lake.silver.sessions
)
SELECT f.cohort_week,
       CAST((a.active_week - f.cohort_week) AS ...) AS offset_week,   -- TODO: datediff tuần
       COUNT(DISTINCT a.user_id) AS users
FROM first_seen f JOIN activity a USING (user_id)
GROUP BY 1, 2
-- TODO: chia cho cohort size (offset 0) để ra retention %; ghi overwritePartitions
```

- **QC gold** (lesson 35): retention % phải ∈ [0, 100]; offset 0 luôn = 100%; funnel không bao giờ "ngược" (purchase ≤ click ≤ view).

**Đạt khi**: vẽ diagram 3 tầng kèm contract từng tầng; `SUM` số session ở gold khớp `COUNT` silver; giải thích vì sao sessionization đặt ở silver chứ không phải bronze.

## 6. Checkpoint 5 — Time travel: so sánh retention hôm nay vs hôm qua

Iceberg trả công bạn ở đây:

```sql
-- retention hiện tại
SELECT * FROM lake.gold.retention_weekly;
-- retention như NÓ ĐÃ TỪNG LÀ lúc này hôm qua (số liệu đã "trôi" thế nào?)
SELECT * FROM lake.gold.retention_weekly
  FOR SYSTEM_TIME AS OF (current_timestamp - INTERVAL 1 DAY);
```

Viết `jobs/compare_retention.py`: join 2 phiên bản theo cohort, in cột `delta`. **Đạt khi**: chỉ ra được cohort nào thay đổi và giải thích vì sao retention của cohort cũ vẫn nhích lên theo thời gian (late-arriving behavior — user tuần trước hôm nay mới quay lại).

## 7. Checkpoint 6 — Airflow: daily compaction + metrics

Streaming ghi Iceberg mỗi micro-batch = **small files như mưa** (lesson 32 đã cảnh báo). DAG `clickstream_daily` (schedule 3AM):

```
qc_silver >> [compute_funnel, compute_retention] >> qc_gold >> maintenance
                                                              (rewrite_data_files
                                                               + expire_snapshots >7d)
```

Mọi job nhận `--ds`, ghi idempotent (funnel_daily: `overwritePartitions` theo ngày; retention: recompute + replace). Lưu ý bẫy từ lesson 32/36: `expire_snapshots` đừng gọt sát quá — Checkpoint 5 cần snapshot ≥ 1 ngày tuổi để time travel!

**Đạt khi**: trigger DAG 2 lần cùng `ds` → gold không đổi; đếm file của `bronze.events` trước/sau compaction (dùng metadata table `.files`) — giảm rõ rệt; streaming job vẫn sống xuyên suốt lúc maintenance chạy (ACID của Iceberg cân được reader/writer đồng thời).

## 8. Checkpoint 7 — Trino: funnel chart

Trino (repo `../kafka-flink`) trỏ cùng warehouse Iceberg → query được ngay bảng gold mà không đụng Spark:

```sql
-- sql/funnel.sql (chạy trong trino-cli)
SELECT step, sessions,
       round(100.0 * sessions / max(sessions) OVER (), 1) AS pct_of_top
FROM (
  SELECT '1. view' AS step,     sum(sessions_with_view)     AS sessions FROM lake.gold.funnel_daily WHERE d = DATE '2026-07-08'
  UNION ALL
  SELECT '2. click',            sum(sessions_with_click)                FROM lake.gold.funnel_daily WHERE d = DATE '2026-07-08'
  UNION ALL
  SELECT '3. purchase',         sum(sessions_with_purchase)             FROM lake.gold.funnel_daily WHERE d = DATE '2026-07-08'
) ORDER BY step;
--  1. view      12840   100.0
--  2. click      4213    32.8   ← conversion view->click
--  3. purchase    655     5.1   ← conversion tổng
```

Trực quan hóa: Superset (nếu có trong `../kafka-flink`) hoặc đơn giản in bar chart ASCII bằng script Python đọc qua `trino` client. **Đạt khi**: con số funnel khớp với xác suất bạn cài trong generator (30% view→click...) — sai lệch lớn nghĩa là sessionization hoặc gold có bug. Đây là phép **kiểm chứng end-to-end** đẹp nhất: bạn BIẾT ground truth vì bạn sinh ra dữ liệu.

---

## 9. Deliverable

Nộp trong `labs/project-clickstream/`:

1. `generator/gen_events.py` + `EVENT_SCHEMA.md` (schema event + lý do key=user_id).
2. `jobs/`: bronze_stream.py, sessionize.py, gold_funnel.py, gold_retention.py, compare_retention.py, qc_gate.py, maintenance.py.
3. `dags/clickstream_daily.py`.
4. `sql/funnel.sql` + screenshot/ASCII funnel chart.
5. `NOTES.md`: diagram kiến trúc, các quyết định thiết kế (grain, timeout, watermark, chiến thuật idempotent từng bảng), 3 sự cố đã gặp và cách debug (kèm dấu vết Spark UI/Airflow log).
6. Demo sống 10 phút: generator chạy → session đóng → dashboard nhích — quay màn hình hoặc demo trực tiếp trong buổi review.

## 10. Rubric — chấm theo chuẩn Senior

| Tiêu chí | Trọng số | Không đạt | Đạt | Senior |
|---|---|---|---|---|
| Sessionization đúng | 25% | Session không đóng theo timeout / mất event | Timeout + counters + path đúng | Restart job không mất/double session; giải thích watermark vs timeout rành mạch |
| Medallion & modeling | 20% | Trộn tầng, không tuyên bố grain | 3 tầng đúng contract, grain rõ | Bronze replay được sang silver; định nghĩa metric ghi thành tài liệu |
| Idempotency & Airflow | 20% | Re-run nhân bản dữ liệu | DAG chạy, re-run an toàn | Backfill dải ngày an toàn; QC retries=0, alert callback có runbook |
| Vận hành Iceberg | 15% | Small files mặc kệ | Compaction + expire chạy hằng ngày | Đo trước/sau bằng metadata tables; cân bằng expire vs time-travel cần dùng |
| Kiểm chứng số liệu | 10% | "Chạy được là xong" | Gold khớp silver | Đối chiếu funnel với ground truth của generator, giải thích sai lệch |
| Trình bày & debug | 10% | Không kể lại được sự cố | NOTES đủ, demo trơn | Kể được 3 sự cố với dấu vết UI/log và bài học rút ra |

Thang: ≥85% + không tiêu chí nào "Không đạt" = **pass mức Senior**; 60–85% = pass, kèm việc phải sửa; <60% = làm lại checkpoint hỏng — project này là nền của Capstone, không được nợ.

Gợi ý khi bí (đọc theo thứ tự, đừng đọc trước):

1. Sessionization sai → hạ rate xuống 10 events/sec, 3 user, timeout 2 phút, `console` sink để nhìn từng batch bằng mắt.
2. Job streaming chết khi restart → 90% là đổi schema/logic mà giữ checkpoint cũ. Xóa checkpoint = job đọc lại từ startingOffsets — hiểu hệ quả trước khi xóa.
3. Trino không thấy bảng → hai catalog phải trỏ cùng warehouse path + cùng loại catalog (hadoop/hive/REST); so từng config hai bên.
4. Funnel lệch ground truth → đếm lại từ silver bằng tay cho MỘT user cụ thể, dò ngược lên sessionize → bronze → Kafka offset.
5. Airflow task xanh mà gold trống → nghi ngờ `--ds` render sai hoặc filter partition lệch múi giờ; in `ds` ra log ở dòng đầu mỗi job.

---

## 11. Next Lesson

**Module 6 — Lesson 37: Deployment — client vs cluster mode, YARN/K8s.**

Project 3 xong nghĩa là bạn đã xây được cả nhà máy dữ liệu trên... máy mình. Module 6 đưa nhà máy đó ra thế giới thật: `spark-submit` chạy ở đâu thì driver nằm ở đâu (client vs cluster mode — bạn đã gặp thoáng qua từ lesson 1 và lesson 36, giờ mổ xẻ tận gốc), chọn YARN hay Kubernetes, config driver/executor lúc submit, và vì sao job chạy ngon ở local lại chết ngay khi lên cluster. Từ tuần 20, câu hỏi không còn là "code thế nào" mà là "vận hành thế nào" — đúng phần phân biệt Mid với Senior.

> Gõ **"Continue"** khi sẵn sàng.
