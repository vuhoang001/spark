# 📚 ROADMAP — Apache Spark cho Production Data Engineering

> **Thời lượng:** 8–12 giờ/tuần | **Tổng:** 6 tháng (part-time) hoặc 3–4 tháng (full-time)
> 
> **💡 Cách sử dụng:** Tích ✅ vào ô checkbox khi hoàn thành từng bài/project
> 
> **⚠️ LƯU Ý:** Không nhảy cóc! Học tuần tự để nắm vững từng bước.

---

# 📊 TIẾN ĐỘ TỔNG QUÁT

```
Module 1          Module 2          Module 3            Module 4            Module 5           Module 6
Foundations  →    Spark SQL &  →    Internals &    →    Structured     →    Lakehouse &   →    Production
(tuần 1-3)        DataFrame         Performance         Streaming           Iceberg            Engineering
                  (tuần 4-7)        (tuần 8-11)         (tuần 12-15)        (tuần 16-19)       (tuần 20-24)
```

---

# 🎯 MODULE 1 — FOUNDATIONS (Tuần 1–3)
## **Mục tiêu:** Hiểu Spark từ A-Z: kiến trúc, DAG, partition, các định dạng dữ liệu

---

## **TUẦN 1** ⏰
### 📖 Lesson 1: Tại sao Spark tồn tại — Distributed Computing & Kiến trúc tổng quan

- [ ] Học lý thuyết: MapReduce vs Spark, Driver/Executor/Cluster Manager, DAG
- [ ] **Lab:** Cài Spark local, chạy wordcount đầu tiên
- [ ] **Assignment Easy:** Giải thích kiến trúc bằng sơ đồ, định nghĩa các thành phần
- [ ] **Assignment Medium:** Tính số executor cần cho dataset 10GB với 4GB RAM/executor
- [ ] **Assignment Hard:** Vẽ DAG cho 1 job có shuffle, giải thích từng stage

### 📖 Lesson 2: SparkSession, RDD → DataFrame → Dataset, Lazy Evaluation

- [ ] Học lý thuyết: transformation vs action, lazy evaluation tại sao cần
- [ ] **Lab:** Tạo 3 cách: RDD, DataFrame, Dataset từ cùng 1 dữ liệu, so sánh tốc độ
- [ ] **Assignment Easy:** Viết 5 transformations khác nhau
- [ ] **Assignment Medium:** Debug bằng cách thêm `action()` sau từng transformation để thấy lỗi ở đâu
- [ ] **Assignment Hard:** Giải thích tại sao Spark không thực thi gì khi bạn chỉ có transformations

---

## **TUẦN 2** ⏰
### 📖 Lesson 3: Job / Stage / Task — mô hình thực thi

- [ ] Học lý thuyết: Job là gì, Stage là gì, Task là gì, ranh giới shuffle
- [ ] Đọc: Spark: The Definitive Guide ch.15
- [ ] **Lab:** Chạy job có 2-3 stage, so sánh code với DAG trên Spark UI
- [ ] **Assignment Easy:** Đoán trước số stage của 5 job khác nhau (không chạy), rồi kiểm chứng
- [ ] **Assignment Medium:** Vẽ DAG cho join 3 bảng
- [ ] **Assignment Hard:** Giải thích tại sao có shuffle ở vị trí này, không ở vị trí khác

### 📖 Lesson 4: Partition — đơn vị song song hóa

- [ ] Học lý thuyết: partition là gì, partition key, # partition quyết định parallelism
- [ ] **Lab:** Đọc CSV 100MB, thay đổi # partition (từ 1 → 8 → 32), đo tốc độ
- [ ] **Assignment Easy:** Xác định # partition tối ưu cho dataset 50GB trên cluster 4 executor
- [ ] **Assignment Medium:** Hiểu repartition vs coalesce — khi nào dùng cái nào
- [ ] **Assignment Hard:** Khi partition không đều (skew), điều gì xảy ra?

---

## **TUẦN 3** ⏰
### 📖 Lesson 5: Đọc/ghi dữ liệu — Data Sources API

- [ ] Học lý thuyết: CSV, JSON, Parquet, JDBC — ưu nhược điểm
- [ ] Học: predicate pushdown, column pruning, partition pruning
- [ ] **Lab:** Đọc PostgreSQL qua JDBC với partitioned read, ghi Parquet
- [ ] **Assignment Easy:** Đọc 3 format (CSV, JSON, Parquet) cùng 1 bảng, so sánh tốc độ
- [ ] **Assignment Medium:** Chọn format phù hợp cho 3 tình huống khác nhau (nêu lý do)
- [ ] **Assignment Hard:** Viết JDBC read từ PostgreSQL với 10 partition, kiểm tra query chạy 10 lần parallel

### 📖 Lesson 6: Parquet & columnar format — tại sao DE sống chết với Parquet

- [ ] Học lý thuyết: row-oriented vs column-oriented, compression, nested types
- [ ] **Lab:** Ghi Parquet với nested struct, đọc lại, so sánh kích thước vs CSV
- [ ] **Assignment Easy:** Giải thích tại sao Parquet nhỏ hơn CSV 5-10 lần
- [ ] **Assignment Medium:** Chọn compression (snappy vs gzip) cho 3 trường hợp (latency vs ratio)
- [ ] **Assignment Hard:** Viết bảng Iceberg với struct/array, query chỉ một vài columns, chứng minh column pruning hoạt động

### ✅ **MINI PROJECT 1: Batch Ingestion v0**

- [ ] **Yêu cầu:** CSV (Olist) → Spark → Parquet phân vùng → Iceberg → Trino
- [ ] **Checkpoint 1:** Đọc CSV thành Spark DF, hiển thị schema
- [ ] **Checkpoint 2:** Ghi Parquet phân vùng theo ngày (date column)
- [ ] **Checkpoint 3:** Tạo bảng Iceberg trỏ đến thư mục Parquet
- [ ] **Checkpoint 4:** Query bằng Trino, so sánh tốc độ CSV-external-table vs Iceberg-Parquet
- [ ] **Deliverable:** Report kích thước + thời gian query before/after

---

# 🎯 MODULE 2 — SPARK SQL & DATAFRAME MASTERY (Tuần 4–7)
## **Mục tiêu:** Thành thạo DataFrame API & SQL, tối ưu query

---

## **TUẦN 4** ⏰
### 📖 Lesson 7: Transformations cốt lõi (select/filter/withColumn/when)

- [ ] Học lý thuyết: syntax DataFrame API vs Spark SQL
- [ ] **Lab:** Xây 10 transformations khác nhau từ bảng Olist
- [ ] **Assignment Easy:** Viết 1 query bằng cả SQL + API (phải give same kết quả)
- [ ] **Assignment Medium:** Kết hợp select/filter/withColumn để xây tính toán phức tạp
- [ ] **Assignment Hard:** Performance: reorder transformations để tối ưu (early filtering)

### 📖 Lesson 8: Aggregations & groupBy — hash aggregate hoạt động ra sao

- [ ] Học lý thuyết: partial aggregate → final aggregate, hash table memory
- [ ] **Lab:** Tính revenue/quantity theo seller, đo shuffle bằng Spark UI
- [ ] **Assignment Easy:** count/sum/avg/min/max trên Olist
- [ ] **Assignment Medium:** Multi-level groupBy (seller → category → date)
- [ ] **Assignment Hard:** Aggregate khi memory thấp: partial aggregate → disk spill

---

## **TUẦN 5** ⏰
### 📖 Lesson 9: Joins — broadcast, sort-merge, shuffle-hash

- [ ] Học lý thuyết: 3 loại join strategy, join là nguồn shuffle lớn nhất
- [ ] Đọc: Spark: The Definitive Guide ch.8
- [ ] **Lab:** Join orders × items × customers × sellers (Olist), đo shuffle bằng Spark UI
- [ ] **Assignment Easy:** Viết 5 join khác nhau, kiểm tra physical plan
- [ ] **Assignment Medium:** Ép broadcast join đúng lúc (hint, hint config)
- [ ] **Assignment Hard:** JOIN hàng tỷ records: shuffle-merge hay hash? Tại sao?

### 📖 Lesson 10: Window Functions trong Spark

- [ ] Học lý thuyết: PARTITION BY, ORDER BY, frame specification
- [ ] **Lab:** Row number, rank, dense_rank, LAG/LEAD, running sum
- [ ] **Assignment Easy:** Xếp hạng seller theo revenue (row_number, rank)
- [ ] **Assignment Medium:** Running total (sum từ đầu đến hiện tại)
- [ ] **Assignment Hard:** 2-window: tính chênh lệch giá so với ngày trước per category

---

## **TUẦN 6** ⏰
### 📖 Lesson 11: Complex types (array/map/struct), explode, JSON

- [ ] Học lý thuyết: nested types, explode/flatten
- [ ] **Lab:** Parse JSON từ Debezium, convert thành bảng phẳng
- [ ] **Assignment Easy:** Hiểu schema của Debezium message (before, after, source)
- [ ] **Assignment Medium:** Xử lý NULL values bên trong array/struct
- [ ] **Assignment Hard:** Array xử lý schema evolution (thêm column trong struct)

### 📖 Lesson 12: UDF vs built-in vs pandas UDF

- [ ] Học lý thuyết: tại sao UDF giết performance (serialization, mất Catalyst)
- [ ] **Lab:** So sánh tốc độ built-in vs Python UDF vs pandas UDF
- [ ] **Assignment Easy:** Viết 1 custom logic bằng cả UDF + built-in, so sánh
- [ ] **Assignment Medium:** Khi nào dùng pandas_udf (vectorized), lợi ích?
- [ ] **Assignment Hard:** Profile để chứng minh UDF chậm (overhead đâu?)

---

## **TUẦN 7** ⏰
### 📖 Lesson 13: Catalyst Optimizer — logical/physical plan, explain()

- [ ] Học lý thuyết: logical plan → rule optimization → physical plan
- [ ] **Lab:** Chạy `explain(mode="formatted")` trên 10 query khác nhau
- [ ] **Assignment Easy:** Đọc explain output của 1 simple query (select/filter/agg)
- [ ] **Assignment Medium:** Giải thích optimize rules: predicate pushdown, constant folding
- [ ] **Assignment Hard:** Khi explain không tối ưu như mong, gợi ý hint cho Spark

### 📖 Lesson 14: Null handling, data quality patterns

- [ ] Học lý thuyết: null semantics, three-valued logic (TRUE/FALSE/NULL)
- [ ] **Lab:** Xây pattern: assert no nulls, fillna, coalesce, ifnull
- [ ] **Assignment Easy:** Tìm & xử lý NULL trong Olist (fillna/drop/keep?)
- [ ] **Assignment Medium:** Viết data quality checks (columns not null, values in range, etc.)
- [ ] **Assignment Hard:** Lưu report QC: # rows trước/sau, % null per column

### ✅ **PROJECT 1 (FULL): Olist Batch ELT — Production chuẩn**

- [ ] **Yêu cầu:** CSV/Postgres → Spark bronze/silver/gold → Iceberg → Trino dashboard
- [ ] **Checkpoint 1:** Bronze layer: ingest raw CSV, thêm ingestion_date, save Iceberg
- [ ] **Checkpoint 2:** Silver layer: dedup (nếu cần), kiểm tra data quality, thêm hash surrogate keys
- [ ] **Checkpoint 3:** Gold layer: fact/dim (star schema), business metrics (revenue per seller per day)
- [ ] **Checkpoint 4:** Iceberg metadata: compaction, snapshot management
- [ ] **Checkpoint 5:** Trino query gold layer, tạo dashboard BI
- [ ] **Checkpoint 6:** Airflow DAG: dependency bronze → silver → gold, cron daily 2AM
- [ ] **Deliverable:** Design doc (schema, SLA), Airflow DAG code, test cases, run log 3 days

---

# 🎯 MODULE 3 — INTERNALS & PERFORMANCE TUNING (Tuần 8–11)
## **Mục tiêu:** Hiểu Spark internals, tuning performance chuyên sâu

---

## **TUẦN 8** ⏰
### 📖 Lesson 15: Shuffle internals — shuffle write/read, spill

- [ ] Học lý thuyết: shuffle chia thành 2 phase (write/read), mapper output, reducer input
- [ ] Học: spill to disk khi memory hết
- [ ] **Lab:** Chạy job có shuffle, quan sát Shuffle Write/Read trong Spark UI (Stages tab)
- [ ] **Assignment Easy:** Tính kích thước shuffle (# records × record size)
- [ ] **Assignment Medium:** Xác định shuffle memory config (spark.shuffle.memory, etc.)
- [ ] **Assignment Hard:** Cố tình gây shuffle spill, đo tốc độ vs không spill (gấp mấy lần chậm?)

### 📖 Lesson 16: Partitioning chiến lược — repartition vs coalesce

- [ ] Học lý thuyết: repartition (gây shuffle), coalesce (không shuffle)
- [ ] **Lab:** Dùng repartition/coalesce để giảm/tăng partition, đo tốc độ
- [ ] **Assignment Easy:** Partition pruning: filter trước groupBy để giảm data
- [ ] **Assignment Medium:** Khi 1000 partition → 100 partition: dùng repartition hay coalesce?
- [ ] **Assignment Hard:** Thiết kế partitioning scheme cho Iceberg bảng 100GB (theo gì? bao nhiêu partition?)

---

## **TUẦN 9** ⏰
### 📖 Lesson 17: Spark Memory Model — unified memory, execution vs storage

- [ ] Học lý thuyết: Heap memory chia thành execution + storage, éviction policy
- [ ] **Lab:** Chạy job, xem memory profile trong Spark UI (Executors tab)
- [ ] **Assignment Easy:** Tính heap size cần cho executor (task × memory per task)
- [ ] **Assignment Medium:** OOM exception: executor hay driver? Giải pháp?
- [ ] **Assignment Hard:** Memory tuning: tăng shuffle partition hay tăng executor memory?

### 📖 Lesson 18: Caching & persistence — khi nào cache giúp

- [ ] Học lý thuyết: cache vs persist, MEMORY vs DISK vs MEMORY_AND_DISK
- [ ] **Lab:** Cache 1 bảng dùng 10 lần, đo tốc độ vs không cache
- [ ] **Assignment Easy:** Chọn storage level (MEMORY vs DISK) cho 3 tình huống
- [ ] **Assignment Medium:** Cache kích thước bao nhiêu là hợp lý (% total memory)
- [ ] **Assignment Hard:** Cache có thể gây OOM — khi nào cache hại?

---

## **TUẦN 10** ⏰
### 📖 Lesson 19: Data Skew — phát hiện và xử lý

- [ ] Học lý thuyết: skew là gì (1 partition lớn, các cái khác nhỏ), tại sao chậm
- [ ] **Lab:** Tạo dataset skew (một seller có 10M orders, khác chỉ 100), đo lag
- [ ] **Assignment Easy:** Phát hiện skew bằng Spark UI (task duration variance)
- [ ] **Assignment Medium:** Xử lý skew: salting (thêm random prefix vào key)
- [ ] **Assignment Hard:** Khi AQE bật, skew handling tự động không? (Spark 3.0+)

### 📖 Lesson 20: AQE (Adaptive Query Execution) — Spark 3.x

- [ ] Học lý thuyết: AQE chạy mid-flight optimization, replan base on runtime stats
- [ ] **Lab:** Chạy query WITH AQE vs WITHOUT AQE (spark.sql.adaptive.enabled), so sánh
- [ ] **Assignment Easy:** Bật/tắt AQE, xem explain output có gì khác
- [ ] **Assignment Medium:** AQE auto skew join, auto broadcast — test 2 case này
- [ ] **Assignment Hard:** AQE không fix được gì? (ngẫu nhiên query pattern khác)

---

## **TUẦN 11** ⏰
### 📖 Lesson 21: Small files problem & file layout

- [ ] Học lý thuyết: small file là bottleneck (thư mục metadata, memory phát file)
- [ ] **Lab:** Tạo 10000 files nhỏ vs 100 files lớn, so sánh tốc độ scan
- [ ] **Assignment Easy:** Giải thích tại sao small files vấn đề (list file slow, memory spike)
- [ ] **Assignment Medium:** Compaction: gộp 1000 files nhỏ → 10 files lớn, đo lợi ích
- [ ] **Assignment Hard:** Streaming ghi small files — làm sao compact mà không downtime?

### 📖 Lesson 22: Quy trình tuning tổng hợp — Checklist Senior

- [ ] Học lý thuyết: tuning playbook bước 1-10
- [ ] **Lab:** Apply checklist trên 1 job chậm thực tế
- [ ] **Assignment Easy:** Liệt kê 10 tham số tuning (executor memory, partition, shuffle, cache, etc.)
- [ ] **Assignment Medium:** Viết runbook: khi job chậy, kiểm tra gì trước/tiếp theo?
- [ ] **Assignment Hard:** Tuning từ O(1h) → O(10min) với 3-4 optimization (ghi nhận lại)

### ✅ **PROJECT 3 (TUẦN 11): "Cứu pipeline chậm"**

- [ ] **Yêu cầu:** Mentor cung cấp job viết tệ (skew + UDF + small files), optimize
- [ ] **Checkpoint 1:** Profile bằng Spark UI — identify bottleneck #1,2,3
- [ ] **Checkpoint 2:** Viết báo cáo chẩn đoán (vấn đề × giải pháp)
- [ ] **Checkpoint 3:** Tối ưu từng vấn đề (1 optimization per day)
- [ ] **Checkpoint 4:** Chứng minh improvement ≥5× (before/after metrics)
- [ ] **Deliverable:** Before/after explain, Spark UI screenshots, metrics table, tuning notes

---

# 🎯 MODULE 4 — STRUCTURED STREAMING (Tuần 12–15)
## **Mục tiêu:** Thành thạo real-time stream processing, exactly-once semantics

---

## **TUẦN 12** ⏰
### 📖 Lesson 23: Streaming 101 — micro-batch vs continuous, unbounded table

- [ ] Học lý thuyết: micro-batch, watermark concept, event time vs processing time
- [ ] **Lab:** Chạy StreamingContext từ socket, micro-batch duration 5s
- [ ] **Assignment Easy:** Vẽ timeline: data đến → batch → output, xác định latency
- [ ] **Assignment Medium:** Tăng batch duration 5s → 30s → 2m, xem latency thay đổi thế nào
- [ ] **Assignment Hard:** Continuous mode (experimental) vs micro-batch: trade-off gì?

### 📖 Lesson 24: Kafka source/sink — offset, checkpoint, trigger

- [ ] Học lý thuyết: offset management, checkpoint trong Spark Structured Streaming
- [ ] **Lab:** Đọc Kafka topic → console output, checkpoint được lưu ở đâu
- [ ] **Assignment Easy:** Khởi động lại stream, kiểm tra offset resume từ đâu
- [ ] **Assignment Medium:** Trigger types: ProcessingTime vs Once vs Continuous — khi nào dùng?
- [ ] **Assignment Hard:** Max offset lag: từ 1000 → 10000 messages, stream có handle được?

---

## **TUẦN 13** ⏰
### 📖 Lesson 25: Event time, watermark, xử lý late data

- [ ] Học lý thuyết: event time vs processing time, watermark = event time - allowed lateness
- [ ] **Lab:** Stream data có late events, xem watermark hành động
- [ ] **Assignment Easy:** Giải thích watermark (vẽ timeline)
- [ ] **Assignment Medium:** Windowed agg (5 min window) + watermark 10 min, late event đến sau 15 min?
- [ ] **Assignment Hard:** Allowed lateness cho 3 tình huống (IoT sensor vs user click vs payment): bao lâu hợp lý?

### 📖 Lesson 26: Stateful operations — aggregation, deduplication, state store

- [ ] Học lý thuyết: state = accumulator data (từ micro-batch quá khứ), state store (RocksDB)
- [ ] **Lab:** Stateful aggregation: user's session state = last 10 minutes revenue
- [ ] **Assignment Easy:** Streaming count per key (user), state size bao nhiêu là safe?
- [ ] **Assignment Medium:** Deduplication (dropDuplicates with watermark): state TTL = 24h
- [ ] **Assignment Hard:** State store phình to (state không clear): giải pháp?

---

## **TUẦN 14** ⏰
### 📖 Lesson 27: Exactly-once semantics — idempotent sink, foreachBatch

- [ ] Học lý thuyết: at-most-once vs at-least-once vs exactly-once (hard!)
- [ ] **Lab:** Ghi Iceberg từ stream (foreachBatch), kill job, restart, check deduplicate
- [ ] **Assignment Easy:** Idempotent write: nếu ghi 2 lần cùng data, Iceberg merge hay insert?
- [ ] **Assignment Medium:** Checkpoint offset + Iceberg write atomic (transactional)?
- [ ] **Assignment Hard:** Multi-sink: nếu Iceberg success nhưng Kafka alert fail → rollback?

### 📖 Lesson 28: Stream-stream & stream-static join

- [ ] Học lý thuyết: stream-stream join có state, stream-static join từ lookup table
- [ ] **Lab:** Stream 1 (orders) + Stream 2 (returns) = join → find frauds
- [ ] **Assignment Easy:** Inner join vs left join — stream side nào?
- [ ] **Assignment Medium:** State cleanup: join state hàng ngày → xóa cũ hơn 7 ngày
- [ ] **Assignment Hard:** Stream + static (rebroadcast user profile table daily 1AM)

---

## **TUẦN 15** ⏰
### 📖 Lesson 29: CDC pattern hoàn chỉnh — Debezium envelope, MERGE INTO

- [ ] Học lý thuyết: CDC (Change Data Capture), Debezium source connector schema
- [ ] **Lab:** Parse Debezium JSON (before/after/op), convert op='insert/update/delete'
- [ ] **Assignment Easy:** Extract before/after payload từ Debezium message
- [ ] **Assignment Medium:** MERGE INTO Iceberg (insert → insert, update → update, delete → delete)
- [ ] **Assignment Hard:** Schema evolution: source thêm column, Spark deserialize & MERGE đúng cách

### ✅ **PROJECT 2 (FULL): CDC Lakehouse**

- [ ] **Yêu cầu:** PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Iceberg → Trino
- [ ] **Checkpoint 1:** PostgreSQL setup + Debezium connector (enable CDC)
- [ ] **Checkpoint 2:** Kafka topic nhận Debezium events (JSON format)
- [ ] **Checkpoint 3:** Spark Streaming parse Debezium, convert to insert/update/delete
- [ ] **Checkpoint 4:** MERGE INTO Iceberg (handle SCD type 2: thêm valid_from/valid_to)
- [ ] **Checkpoint 5:** Trino query Iceberg, xem version history (time travel)
- [ ] **Checkpoint 6:** Monitor: lag tracker (max_offset - current_offset), alerting > 10 min
- [ ] **Deliverable:** Debezium connector setup, PySpark code, Iceberg schema, test plan

---

# 🎯 MODULE 5 — LAKEHOUSE & ICEBERG CHUYÊN SÂU (Tuần 16–19)
## **Mục tiêu:** Thành thạo Iceberg, medallion architecture, data modeling

---

## **TUẦN 16** ⏰
### 📖 Lesson 30: Iceberg internals — metadata, snapshot, manifest

- [ ] Học lý thuyết: metadata layer (snapshot = version), manifest (file list), data files (Parquet)
- [ ] **Lab:** Mổ xẻ thư mục metadata của 1 bảng Iceberg (mở file JSON)
- [ ] **Assignment Easy:** Đọc v1.metadata.json, xác định # snapshots, # manifests, current snapshot
- [ ] **Assignment Medium:** Timeline: snapshot 1 → 2 → 3, mỗi cái thêm/xóa files nào
- [ ] **Assignment Hard:** Tính dung lượng metadata vs dung lượng data (% overhead)

### 📖 Lesson 31: Iceberg + Spark — DDL, MERGE, time travel

- [ ] Học lý thuyết: Iceberg table format (v1/v2), ACID transaction
- [ ] **Lab:** CREATE, INSERT, UPDATE, DELETE, MERGE bảng Iceberg
- [ ] **Assignment Easy:** Time travel query (AS OF TIMESTAMP '...')
- [ ] **Assignment Medium:** Branch/tag: tạo branch để test trước merge
- [ ] **Assignment Hard:** Row-level ACID: 2 job cùng UPDATE 1 row, conflict resolve thế nào?

---

## **TUẦN 17** ⏰
### 📖 Lesson 32: Table maintenance — compaction, snapshot expiration

- [ ] Học lý thuyết: compaction = gộp small files, snapshot expire = xóa cũ, rewrite manifests
- [ ] **Lab:** Run maintenance (spark.sql call `optimize` + `expire_snapshots`)
- [ ] **Assignment Easy:** Compaction job: từ 1000 files → 100 files, đo tốc độ query
- [ ] **Assignment Medium:** Expire snapshots > 30 days cũ, giải phóng storage
- [ ] **Assignment Hard:** Automatic maintenance (Airflow daily): compaction + expire + stats recompute

### 📖 Lesson 33: Partitioning & hidden partitioning

- [ ] Học lý thuyết: explicit partition (physical folder), hidden partition (logical)
- [ ] **Lab:** Tạo bảng with/without partition, so sánh query performance
- [ ] **Assignment Easy:** Partition theo date (dt), test partition pruning
- [ ] **Assignment Medium:** Multi-level partition (year/month/day) vs single-level (date)
- [ ] **Assignment Hard:** Hidden partition (bucketing by user_id): khi nào tốt hơn explicit?

---

## **TUẦN 18** ⏰
### 📖 Lesson 34: Medallion architecture & data modeling

- [ ] Học lý thuyết: bronze/silver/gold layer, star schema, fact vs dim, SCD type 2
- [ ] **Lab:** Thiết kế schema Olist (fact_orders, dim_products, dim_sellers)
- [ ] **Assignment Easy:** Vẽ ER diagram cho medallion (bronze → silver → gold)
- [ ] **Assignment Medium:** SCD type 2: seller thay đổi thành phố, lưu lại version cũ
- [ ] **Assignment Hard:** Fact table grain: order level vs order-item level? Chọn gì?

### 📖 Lesson 35: Data quality & testing — constraints, dbt

- [ ] Học lý thuyết: constraint check, data tests, dbt test patterns
- [ ] **Lab:** Viết Spark job: kiểm tra PK (unique), FK (referential integrity), range check
- [ ] **Assignment Easy:** Lưu QC report per table (row count, null %, distinct check)
- [ ] **Assignment Medium:** dbt tests (relationships, unique, not_null, custom)
- [ ] **Assignment Hard:** Alert khi QC fail (e.g., # null > 1%)

---

## **TUẦN 19** ⏰
### 📖 Lesson 36: Orchestration với Airflow — SparkSubmitOperator, idempotency

- [ ] Học lý thuyết: Airflow task dependencies, SparkSubmitOperator, idempotency (re-run)
- [ ] **Lab:** Viết DAG: bronze_ingestion → silver_transform → gold_agg → snapshot
- [ ] **Assignment Easy:** 4-task DAG (linear), set trigger daily 2AM
- [ ] **Assignment Medium:** Idempotency: re-run ngày hôm qua, kết quả phải giống (UPSERT logic)
- [ ] **Assignment Hard:** Backfill: chạy lại 30 ngày quá khứ với Airflow, backfill_start_date

### ✅ **PROJECT 3 (FULL): Clickstream Analytics**

- [ ] **Yêu cầu:** Simulate web events → Kafka → Spark Streaming (sessionization) → Iceberg → Trino dashboard
- [ ] **Checkpoint 1:** Event generator: user_id, event_type (view/click/purchase), timestamp
- [ ] **Checkpoint 2:** Stream to Kafka (1000 events/sec)
- [ ] **Checkpoint 3:** Spark: stateful session (30-min session timeout), track user path
- [ ] **Checkpoint 4:** Bronze (raw events) → Silver (sessionized) → Gold (funnel metrics, retention)
- [ ] **Checkpoint 5:** Iceberg with time travel (query 1 day ago, compare retention)
- [ ] **Checkpoint 6:** Airflow: daily compaction + funnel/retention metric compute
- [ ] **Checkpoint 7:** Trino: funnel chart (Step 1 → 2 → 3 conversion %)
- [ ] **Deliverable:** Event schema, Spark sessionization code, Airflow DAG, Superset dashboard

---

# 🎯 MODULE 6 — PRODUCTION ENGINEERING & CAPSTONE (Tuần 20–24)
## **Mục tiêu:** Deploy, monitor, debug real-world Spark pipeline

---

## **TUẦN 20** ⏰
### 📖 Lesson 37: Deployment — client vs cluster mode, standalone/YARN/Kubernetes

- [ ] Học lý thuyết: client mode vs cluster mode, spark-submit, YARN vs K8s
- [ ] **Lab:** spark-submit từ Airflow, submit app.py → executor chạy
- [ ] **Assignment Easy:** Submit job local mode vs cluster mode, port 4040 lấy UI ở đâu?
- [ ] **Assignment Medium:** Quyết định mode: local dev vs YARN staging vs K8s prod?
- [ ] **Assignment Hard:** spark-submit config (driver memory, executor memory, cores, instances)

### 📖 Lesson 38: Resource sizing — executor/core/memory calculation

- [ ] Học lý thuyết: CPU (core), Memory (heap), tính toán # executor, # task per executor
- [ ] **Lab:** Dataset 100GB, cho sẵn cluster 10 node × 16 core × 64GB → allocate sao?
- [ ] **Assignment Easy:** Total parallelism = # executor × # task per executor = # partition?
- [ ] **Assignment Medium:** Overhead: driver memory 2GB, executor 50GB data → need mấy GB?
- [ ] **Assignment Hard:** Resize: nếu job chậy → thêm executor hay thêm core/executor?

---

## **TUẦN 21** ⏰
### 📖 Lesson 39: Monitoring & alerting — metrics, event log, history server

- [ ] Học lý thuyết: Spark metrics (executor.rddBlocks, executor.shuffleRead, etc.), event log, structured logging
- [ ] **Lab:** Enable Spark history server, push metrics to Prometheus/CloudWatch
- [ ] **Assignment Easy:** Parse event log, extract task duration, identify slow tasks
- [ ] **Assignment Medium:** Alert: nếu stage duration > baseline × 2
- [ ] **Assignment Hard:** Trace 1 task: memory usage → CPU → shuffle read timing

### 📖 Lesson 40: Debugging playbook — OOM, stragglers, stuck jobs

- [ ] Học lý thuyết: common errors (OOM, serialization, timeout) → nguyên nhân → fix
- [ ] **Lab:** Reproduce 5 common errors (OOM executor, OOM driver, task failed, executor lost)
- [ ] **Assignment Easy:** OOM stack trace → nguyên nhân (shuffle, cache, large object)?
- [ ] **Assignment Medium:** Straggler (1 task chạy 100s, khác 10s): skew hay hardware issue?
- [ ] **Assignment Hard:** Job stuck (không progress 30 min): check blockManagers, shuffle files đâu?

---

## **TUẦN 22** ⏰
### 📖 Lesson 41: CI/CD cho Spark — test PySpark, packaging

- [ ] Học lý thuyết: pytest, test fixture, data validation, packaging (.whl, .zip)
- [ ] **Lab:** Viết test: unit (transform logic) + integration (read/write Iceberg)
- [ ] **Assignment Easy:** Write 5 unit test (DataFrame assertion bằng chispa lib)
- [ ] **Assignment Medium:** Mock Iceberg reader/writer, test senza real infrastructure
- [ ] **Assignment Hard:** CI pipeline: push code → GitHub → Docker build → test → deploy

### 📖 Lesson 42: Cost & capacity — spot instances, autoscaling, khi nào KHÔNG dùng Spark

- [ ] Học lý thuyết: Spark cost (cluster + storage), Spot vs On-demand, autoscaling policy
- [ ] **Lab:** AWS EMR / GCP Dataproc: estimate cost cho monthly workload
- [ ] **Assignment Easy:** Spot instance trade-off (savings vs interruption risk)
- [ ] **Assignment Medium:** Autoscaling: min executor → max executor dựa vào queue size
- [ ] **Assignment Hard:** Alternative to Spark: DuckDB (single machine), Presto (query), khi nào?

---

## **TUẦN 23–24** ⏰
### ✅ **PROJECT 4 (CAPSTONE): Real-time Fraud Detection**

- [ ] **Yêu cầu:** Transaction stream → Spark Streaming (scoring + enrichment) → Alert + Audit
- [ ] **Checkpoint 1:** Transaction event schema (amount, merchant, user, country, timestamp)
- [ ] **Checkpoint 2:** Rule-based scoring: if amount > $1000 → risk_score++, if country != user_home → risk_score++
- [ ] **Checkpoint 3:** Stream-static join: transactions + user profile table (loaded daily 1AM)
- [ ] **Checkpoint 4:** Stateful processing: user's transaction history (last 10 txn, time window 24h)
- [ ] **Checkpoint 5:** Output: 
  - High-risk transaction → alert topic (Kafka)
  - All transaction → audit log (Iceberg) with risk_score
  - Metrics (# alerts/min, avg risk_score) → Prometheus
- [ ] **Checkpoint 6:** State TTL: cleanup user state khi chưa thấy user 30 ngày
- [ ] **Checkpoint 7:** Monitoring: lag, error rate, # alerts, SLA (P99 latency < 100ms)
- [ ] **Checkpoint 8:** Airflow schedule + Kubernetes deployment (HPA: CPU > 70% → scale)

### ✅ **INTERVIEW PREP**

- [ ] Review 100 câu hỏi Junior→Senior Data Engineer
- [ ] Mock phỏng vấn: system design "thiết kế pipeline streaming 10M events/sec"
- [ ] Review mindmap toàn bộ 6 module (từ DAG đến ACID)
- [ ] Code review: chọn 3 project tốt nhất, optimize code style

---

# 📋 TỔNG KẾT

| Module | Tiêu đề | Tuần | Học/Lab/Project |
|--------|--------|------|-----------------|
| **1** | Foundations | 1–3 | 6 lessons + Mini Project 1 |
| **2** | Spark SQL & DataFrame | 4–7 | 8 lessons + Project 1 |
| **3** | Internals & Performance | 8–11 | 8 lessons + Project 3 |
| **4** | Structured Streaming | 12–15 | 7 lessons + Project 2 |
| **5** | Lakehouse & Iceberg | 16–19 | 7 lessons + Project 3 |
| **6** | Production & Capstone | 20–24 | 6 lessons + Project 4 + Interview |

---

# 🎓 Hướng dẫn học tập

1. **Tuần đầu:** Tích ✅ vào checkbox khi hoàn thành
2. **Thứ tự:** KHÔNG nhảy cóc! Mỗi lesson phụ thuộc bài trước
3. **Phương pháp:** Theory → Lab → Assignment (Easy → Medium → Hard)
4. **Review:** Cuối tuần, tổng hợp learning notes
5. **Project:** Làm đầy đủ checkpoint, nộp mentor review
6. **Pivot:** Nếu stuck > 2h, hỏi mentor, không ngồi chỉ suy nghĩ

---

**Bắt đầu từ MODULE 1 — Lesson 1 ngay hôm nay! 🚀**
