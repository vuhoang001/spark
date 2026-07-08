# Interview Prep — Tổng ôn toàn khóa

> Module 6 · Production Engineering · Tuần 24 · Thời lượng: 1 tuần (ôn + mock interview)

---

## 1. Cách dùng tài liệu này

Đây không phải bài học mới — đây là **bản nén 24 tuần thành một buổi ôn thi**. Cách dùng đúng:

1. Đọc mindmap, tự kiểm tra: nhánh nào bạn *không thể tự giảng lại trong 2 phút* → quay về lesson tương ứng.
2. 50 câu hỏi: che đáp án, trả lời **thành tiếng** (nói, không nghĩ thầm — phỏng vấn là môn nói), rồi mới so.
3. 3 bài system design: làm trên giấy/whiteboard 30 phút mỗi bài **trước khi** đọc lời giải khung.
4. Checklist mục 5: cái nào chưa tick, ôn lesson được map bên cạnh.

Đáp án ở đây là **đáp án ngắn để kích hoạt trí nhớ** — trong phỏng vấn thật, bạn nói dài hơn, có ví dụ từ project của mình.

### Lịch ôn gợi ý (7 ngày trước phỏng vấn)

| Ngày | Nội dung | Thời lượng |
|---|---|---|
| 1 | Mindmap + 10 câu Architecture (mục A) — vẽ lại kiến trúc từ trí nhớ | 2h |
| 2 | 10 câu SQL & optimization (B) + đọc lại 3 output `explain()` từ lab cũ | 2h |
| 3 | 10 câu Performance (C) + xem lại before/after của Project "cứu pipeline chậm" | 2h |
| 4 | 10 câu Streaming (D) + chạy lại kill-test của Project 4 một lần | 2–3h |
| 5 | 10 câu Lakehouse/Production (E) + mổ metadata 1 bảng Iceberg | 2h |
| 6 | 3 bài system design — bấm giờ 30 phút/bài, tự nói thành tiếng | 2–3h |
| 7 | Checklist mục 5 + chuẩn bị 4 câu chuyện STAR + 5–7 con số từ project | 2h |

---

## 2. Mindmap toàn khóa

```
                            APACHE SPARK MASTERY (24 tuần)
                                        │
   ┌───────────┬───────────────┬───────┴────────┬────────────────┬─────────────┐
   ▼           ▼               ▼                ▼                ▼             ▼
 M1 FOUNDATIONS  M2 SQL/DF      M3 PERFORMANCE   M4 STREAMING     M5 LAKEHOUSE   M6 PRODUCTION
   │           │               │                │                │             │
 Driver/Exec   select/filter   Shuffle          Micro-batch      Iceberg:      Deploy modes
 Cluster Mgr   groupBy: hash   write/read/spill   unbounded tbl    snapshot     YARN vs K8s
   │           agg 2 phase       │                │                manifest    Resource sizing
 Lazy eval     Join: broadcast Repartition vs   Kafka source:      │             │
 DAG           /SMJ/SHJ        coalesce         offset+checkpoint ACID, MERGE  Monitoring:
   │             │               │                │               time travel  metrics, lag,
 Job=action    Window funcs    Memory model:    Watermark ←──late   │           history server
 Stage=shuffle   │             exec vs storage  data             Hidden          │
 Task=partition Complex types    │                │              partitioning  Debug playbook:
   │           explode/JSON    Cache: khi nào   Stateful: state    │           OOM/straggler/
 Partition =   UDF = chậm →    giúp/hại         store RocksDB,   Maintenance:  stuck
 parallelism   built-in/         │              TTL, timeout     compaction,     │
   │           pandas_udf      Skew → salting     │              expire        CI/CD: pytest,
 Formats:        │             /AQE             Exactly-once:    snapshots     packaging
 Parquet >>>   Catalyst:        │               checkpoint +       │             │
 CSV/JSON      logical→physical AQE (3.x)       idempotent sink  Medallion:    Cost: spot,
 pushdown/     explain()       Small files →    foreachBatch     bronze/silver autoscaling,
 pruning         │             compaction         │              /gold, SCD2   "đừng dùng
               Null: 3-valued                   Stream-static      │           Spark khi..."
               logic                            /stream-stream   Airflow:
                                                join             idempotent DAG,
                                                                 backfill
   └───────────┴───────────────┴────────────────┴────────────────┴─────────────┘
                                        │
                        4 PROJECTS: Batch ELT · CDC Lakehouse ·
                        Clickstream · Fraud Detection (capstone)
```

Một câu tóm mỗi module — nếu bị hỏi "tóm tắt Spark trong 1 phút":

- **M1**: Spark chia dữ liệu thành partition, xử lý song song trên executor, driver điều phối; mọi thứ lazy cho đến khi gặp action.
- **M2**: DataFrame API được Catalyst tối ưu — hãy để engine làm việc, tránh UDF, hiểu join strategy.
- **M3**: Kẻ thù là shuffle, skew, small files, và memory — tuning là đo trước, sửa sau.
- **M4**: Streaming = bảng vô hạn xử lý theo micro-batch; đúng đắn đến từ checkpoint + watermark + idempotent sink.
- **M5**: Iceberg đem ACID, time travel, schema evolution lên data lake — compute tách storage.
- **M6**: Job chạy được chưa phải xong — phải deploy, giám sát, debug và tính được tiền.

---

## 3. 50 câu hỏi interview quan trọng nhất

### A. Architecture & execution model (10 câu)

**A1. [Junior] Driver và Executor khác nhau thế nào? Cái nào chết thì application chết?**
Driver: 1 process/app, chạy `main()`, dịch code → DAG → task, điều phối. Executor: nhiều JVM process trên worker, chạy task trên partition, giữ cache. Driver chết = app chết (trạng thái điều phối tập trung ở driver); executor chết thì task được reschedule — fault tolerance. *(Lesson 1)*

**A2. [Junior] Phân biệt Application / Job / Stage / Task. Cái gì sinh ra cái gì?**
Application = 1 SparkSession/spark-submit. Mỗi **action** sinh 1 Job. Job bị cắt thành Stage tại ranh giới **shuffle**. Mỗi Stage có số Task = số partition; 1 task chạy trên 1 core executor. *(Lesson 3)*

**A3. [Junior] Lazy evaluation là gì, tại sao Spark cố tình "lười"?**
Transformation chỉ ghi lại plan, action mới thực thi. Lợi ích:
- Catalyst thấy **toàn bộ** pipeline trước khi chạy → tối ưu được (pushdown, gộp bước, bỏ cột thừa).
- Không materialize kết quả trung gian vô ích, không tốn I/O cho thứ sẽ bị vứt.
*(Lesson 2)*

**A4. [Junior] Narrow vs wide transformation?**
Narrow: mỗi partition output chỉ cần 1 partition input (`filter`, `select`, `withColumn`) — pipeline được trong 1 stage. Wide: cần dữ liệu từ nhiều partition (`groupBy`, `join`, `repartition`) — gây shuffle, cắt stage. *(Lesson 2, 3)*

**A5. [Mid] Client mode vs cluster mode — khác gì, production dùng gì?**
Khác ở chỗ driver chạy: client mode driver ở máy submit (notebook, debug tương tác); cluster mode driver chạy trong cluster — chuẩn production vì sống độc lập với máy submit, máy submit tắt cũng không sao. *(Lesson 1, 37)*

**A6. [Mid] Executor chết giữa job thì chuyện gì xảy ra? Cache trên đó thì sao?**
Driver phát hiện qua heartbeat mất → reschedule task sang executor khác; partition nguồn đọc lại được. Cache mất → tính lại từ lineage. Retry task mặc định 4 lần, quá thì stage/job fail. Đây chính là fault tolerance "miễn phí" của Spark. *(Lesson 1)*

**A7. [Mid] Code PySpark của tôi chạy ở đâu? UDF thì sao?**
Script Python chạy ở **driver**, chỉ là remote control qua Py4J — dữ liệu và task nằm trong JVM executor. Python UDF buộc mở Python worker trên executor + serialize dữ liệu JVM↔Python từng batch — đó là chỗ trả giá. Trả lời sai câu này = rớt từ vòng gửi xe. *(Lesson 1, 12)*

**A8. [Mid] Điều gì quyết định số task chạy đồng thời? "Wave" là gì?**
Tổng core: `số executor × core/executor`. Ví dụ 5×4 = 20 task song song; stage có 200 task thì chạy 10 wave. Số task nên gấp 2–4× số core để cân tải giữa các task nhanh/chậm. *(Lesson 3, 38)*

**A9. [Mid] Data locality là gì và ảnh hưởng scheduling thế nào?**
Task Scheduler ưu tiên gửi task đến node đang giữ dữ liệu (PROCESS_LOCAL → NODE_LOCAL → ANY) — xử lý tại chỗ rẻ hơn kéo qua network. Chờ locality quá lâu cũng hại → tunable qua `spark.locality.wait`. *(Lesson 1)*

**A10. [Senior] Khi nào bạn khuyên KHÔNG dùng Spark?**
- Dữ liệu vừa 1 máy (< vài chục GB): DuckDB/Polars/Postgres — overhead phân tán > thời gian xử lý.
- Latency per-event sub-second: Flink/Kafka Streams.
- OLTP/point lookup: database thường.
- Query BI interactive: Trino/ClickHouse.
Câu này phân biệt engineer với fanboy — interviewer nào cũng thích hỏi. *(Lesson 1, 42)*

### B. Spark SQL & optimization (10 câu)

**B1. [Mid] Catalyst Optimizer hoạt động qua những bước nào?**
Unresolved logical plan → resolve với catalog → **optimized logical plan** (rule-based: predicate pushdown, constant folding, column pruning) → sinh các physical plan → chọn theo cost → whole-stage codegen. Xem bằng `explain(mode="formatted")`. *(Lesson 13)*

**B2. [Junior] Predicate pushdown và column pruning là gì?**
- Pushdown: đẩy filter xuống tầng đọc (Parquet row-group stats, JDBC WHERE) — không đọc thứ sẽ bị loại.
- Pruning: chỉ đọc cột được dùng — columnar format mới hưởng lợi.
Cả hai là lý do combo Parquet + lazy evaluation mạnh đến vậy. *(Lesson 5, 6)*

**B3. [Mid] Ba join strategy chính? Khi nào Spark chọn cái nào?**
- **Broadcast hash join**: 1 bảng nhỏ (< `spark.sql.autoBroadcastJoinThreshold`, mặc định 10MB) phát cho mọi executor — không shuffle bảng lớn. Nhanh nhất.
- **Sort-merge join**: 2 bảng lớn — shuffle cả hai theo key, sort, merge. Mặc định cho bảng lớn, scale tốt.
- **Shuffle hash join**: shuffle rồi build hash table — tốt khi 1 bên nhỏ hơn hẳn nhưng quá to để broadcast.
*(Lesson 9)*

**B4. [Mid] Tại sao Python UDF chậm? Thay bằng gì?**
Mất Catalyst (black box — không pushdown, không codegen) + serialize dữ liệu JVM↔Python. Thứ tự ưu tiên: built-in functions → SQL expression → `pandas_udf` (Arrow, vectorized) → UDF thường (đường cùng). Trong phỏng vấn, kể luôn số đo từ lab lesson 12. *(Lesson 12)*

**B5. [Mid] Đọc `explain()` bạn nhìn gì đầu tiên?**
1. Join strategy nào được chọn — broadcast mong đợi có xuất hiện không?
2. `Exchange` (shuffle) — mấy lần, ở đâu?
3. Filter/Project có được đẩy sát scan không (PushedFilters)?
4. Scan format và partition pruning có hoạt động không?
*(Lesson 13)*

**B6. [Mid] Window function có gây shuffle không? Rủi ro gì?**
Có — shuffle theo `PARTITION BY`. Rủi ro: partition key cardinality thấp (tệ nhất: window không có PARTITION BY) → toàn bộ dữ liệu dồn về ít task → OOM/straggler. Luôn hỏi: partition key có bao nhiêu giá trị distinct? *(Lesson 10)*

**B7. [Junior] NULL trong Spark SQL có gì bẫy?**
Three-valued logic: so sánh với NULL ra NULL, không phải FALSE — `col != 'x'` **loại luôn** dòng NULL. Dùng `isNull/isNotNull`, `eqNullSafe` (`<=>`), `coalesce`. Nhớ thêm: `count(col)` bỏ NULL nhưng `count(*)` thì không. *(Lesson 14)*

**B8. [Junior] Tại sao Parquet gần như mặc định trong DE, thay vì CSV/JSON?**
Columnar (column pruning), có schema + statistics (predicate pushdown), nén tốt theo cột (nhỏ hơn CSV 5–10×), splittable, hỗ trợ nested types. CSV/JSON chỉ tốt ở biên hệ thống: trao đổi với bên ngoài, ingest thô vào bronze. *(Lesson 6)*

**B9. [Mid] AQE làm được gì?**
Re-plan **giữa chừng** dựa trên runtime statistics sau mỗi stage:
- Gộp shuffle partition nhỏ (coalesce partitions).
- Đổi sort-merge → broadcast khi thấy bảng thực tế nhỏ hơn ước lượng.
- Tự xẻ partition skew trong join.
Bật mặc định từ Spark 3.2. Không cứu được: UDF, thuật toán sai, skew ngoài join. *(Lesson 20)*

**B10. [Senior] Broadcast join có thể làm chết job như thế nào?**
Bảng "nhỏ" bị ước lượng sai (sau filter tưởng nhỏ nhưng thực tế to; statistics cũ) → build hash table quá lớn → OOM driver (dữ liệu được collect về driver trước khi phát) hoặc OOM executor. Xử lý: kiểm tra size thật trước khi hint, hạ threshold, refresh statistics, đừng tin ước lượng mù quáng. *(Lesson 9, 17)*

### C. Performance tuning (10 câu)

**C1. [Junior] Shuffle là gì, tại sao đắt nhất Spark?**
Tái phân phối dữ liệu theo key giữa các executor: map side ghi file shuffle xuống **disk**, reduce side kéo qua **network**. Đắt vì đủ combo: serialize + disk I/O + network + có thể spill. Ranh giới shuffle = ranh giới stage. *(Lesson 15)*

**C2. [Junior] `repartition` vs `coalesce`?**
`repartition(n)`: full shuffle, tăng/giảm đều được, phân bố đều. `coalesce(n)`: chỉ giảm, gộp partition trên cùng executor, **không shuffle** — rẻ nhưng có thể lệch. Giảm mạnh số file trước khi ghi → coalesce; cần phân bố lại theo key → repartition. *(Lesson 16)*

**C3. [Mid] Phát hiện và xử lý data skew?**
Phát hiện: Spark UI Stages tab — 199 task xong trong 10s, 1 task chạy 20 phút; max vs median task duration/shuffle read lệch lớn. Xử lý theo thứ tự:
1. Bật AQE skew join (rẻ nhất, Spark 3+).
2. Hỏi nghiệp vụ: hot key có phải NULL/giá trị rác không → filter sớm hoặc tách riêng.
3. Salting: thêm prefix ngẫu nhiên vào hot key, join 2 bước.
4. Tách hot key xử lý bằng broadcast riêng, union lại.
*(Lesson 19)*

**C4. [Mid] Unified memory model: execution vs storage?**
Heap executor: `spark.memory.fraction` (~60%) là unified pool gồm **execution** (shuffle, join, sort, agg) và **storage** (cache) — mượn qua lại; execution có thể đuổi cache (đến ngưỡng `storageFraction` được bảo vệ), cache không đuổi được execution. Phần còn lại: user memory + overhead ngoài heap. *(Lesson 17)*

**C5. [Mid] Gặp OOM — chẩn đoán driver hay executor, và fix gì?**
- Driver OOM: `collect()`/`toPandas()` bảng lớn, broadcast quá to → sửa code, đừng chỉ tăng RAM.
- Executor OOM: partition quá to (quá ít partition), skew, cache tham lam, agg key cardinality khổng lồ → tăng shuffle partitions, xử skew, bớt cache, rồi mới tăng memory.
Nguyên tắc vàng: OOM là triệu chứng — tìm bệnh trước khi mua RAM. *(Lesson 17, 40)*

**C6. [Mid] Khi nào cache giúp, khi nào cache hại?**
Giúp: 1 DataFrame dùng cho ≥2 action (vòng lặp, self-join, branch pipeline). Hại: dùng 1 lần (tốn công materialize + chiếm memory), cache to đẩy execution vào spill, quên `unpersist`. Không chắc vừa RAM → `MEMORY_AND_DISK`. Luôn kiểm chứng bằng tab Storage. *(Lesson 18)*

**C7. [Mid] Small files problem — vì sao tệ và xử lý ra sao?**
Nghìn file nhỏ = nghìn lần mở file + listing chậm + metadata phình + task quá ngắn (overhead > việc thật). Nguồn sinh: streaming ghi liên tục, partition quá mịn, shuffle partitions cao khi ghi. Xử lý: compaction định kỳ (Iceberg `rewrite_data_files`), coalesce trước khi ghi, target file 128–512MB. *(Lesson 21, 32)*

**C8. [Mid] `spark.sql.shuffle.partitions` chỉnh thế nào?**
Mặc định 200 — sai cho hầu hết job. Heuristic: tổng shuffle data ÷ 128–200MB mỗi partition, và nên là bội của tổng core. Quá ít → partition to, spill/OOM; quá nhiều → task li ti + small files. Spark 3: bật AQE coalesce, đặt trần cao, để AQE tự co. *(Lesson 15, 16, 20)*

**C9. [Senior] Spill là gì, phát hiện và giảm thế nào?**
Execution memory không đủ cho sort/agg/join → tràn xuống disk — job vẫn chạy nhưng chậm nhiều lần vì I/O + serialize hai chiều. Phát hiện: cột "Spill (Memory/Disk)" trong Stages tab. Giảm: tăng shuffle partitions (mỗi task ôm ít data hơn), tăng memory/executor, sửa skew, giảm cardinality trung gian. *(Lesson 15, 17)*

**C10. [Senior] Job chậm — playbook tuning của bạn theo thứ tự nào?**
1. **Đo trước**: Spark UI — job/stage nào chiếm thời gian?
2. Stage đó bị gì: skew? spill? shuffle to? GC?
3. Sửa **thuật toán trước, config sau**: filter/prune sớm, bỏ UDF, broadcast đúng chỗ, giảm số shuffle.
4. Rồi mới: partition count, memory, AQE.
5. Mỗi lần đổi đúng 1 thứ → đo lại → ghi nhận.
Không bao giờ mở đầu bằng "tăng executor lên rồi tính". *(Lesson 22)*

### D. Streaming (10 câu)

**D1. [Junior] Structured Streaming xử lý dữ liệu theo mô hình gì?**
"Unbounded table": stream = bảng được append liên tục; query của bạn là query trên bảng đó, engine chạy **incremental** theo micro-batch. Cùng API với batch — điểm bán hàng lớn nhất về mặt vận hành đội ngũ so với chạy hai framework. *(Lesson 23)*

**D2. [Junior] Event time vs processing time?**
Event time: lúc sự kiện *xảy ra* (nằm trong payload). Processing time: lúc hệ thống *xử lý*. Mạng trễ, thiết bị offline → event đến muộn và lộn xộn. Aggregation nghiệp vụ (doanh thu theo giờ) phải theo event time — và vì thế phải có watermark. *(Lesson 25)*

**D3. [Mid] Watermark là gì và dùng để làm gì?**
`withWatermark("ts", "10 minutes")`: engine coi mọi event muộn hơn `max(event_time) − 10 phút` là quá muộn → được phép **đóng cửa sổ và dọn state**. Không watermark → windowed agg/dedup giữ state vĩnh viễn → OOM. Trade-off: watermark dài = chịu late data tốt hơn, nhưng state to hơn và kết quả chốt muộn hơn. *(Lesson 25)*

**D4. [Mid] Checkpoint trong streaming chứa gì? Khi nào checkpoint "vỡ"?**
Chứa: offset log (WAL) từng micro-batch, commit log, và **state** của stateful operators. Vỡ khi thay đổi không tương thích:
- Thêm/bớt/đổi stateful operator hoặc grouping key.
- Đổi schema của state, đổi source/topic.
Đổi trigger interval, thêm filter stateless thì thường OK. Vỡ = chạy lại từ offset mới + mất state → phải có kế hoạch backfill. *(Lesson 24)*

**D5. [Senior] Exactly-once trong Spark Structured Streaming đạt được thế nào?**
Công thức 3 phần: **replayable source** (Kafka offset) + **checkpoint** (ghi offset trước khi xử lý) + **idempotent/transactional sink**. Spark lo 2 phần đầu; phần sink là việc của bạn: Iceberg/Delta commit atomic theo batch, hoặc foreachBatch + MERGE theo key, hoặc dedup theo batchId. Kafka sink chỉ at-least-once → consumer phải idempotent. Nói "Spark tự exactly-once" là **sai** — nó là exactly-once end-to-end *có điều kiện*. *(Lesson 27)*

**D6. [Mid] State store là gì? Tại sao production dùng RocksDB provider?**
Nơi giữ state của stateful ops (agg, dedup, stream-stream join, applyInPandasWithState), versioned theo micro-batch, backup vào checkpoint. Mặc định HDFS-backed in-memory — state to là OOM executor. RocksDB provider đưa state xuống disk local, memory footprint nhỏ → chịu được hàng chục triệu key. *(Lesson 26)*

**D7. [Mid] Stream-static join vs stream-stream join?**
- Stream-static: stream join bảng tĩnh để enrich — không cần state cho phía static, nhưng bảng static bị "đóng băng" theo plan → cần pattern reload (foreachBatch + cache TTL, như Project 4 checkpoint 3).
- Stream-stream: hai stream join nhau — cả hai phía giữ **state** chờ bên kia; bắt buộc watermark + time range condition, không thì state phình vô hạn.
*(Lesson 28)*

**D8. [Mid] `foreachBatch` dùng khi nào?**
Khi sink không có streaming writer sẵn (JDBC), cần MERGE INTO/upsert, cần fan-out nhiều sink từ 1 lần đọc, hoặc cần logic batch tùy ý per micro-batch. Nhớ 2 điều: nhiều sink trong foreachBatch **không atomic với nhau** (thiết kế cho duplicate vô hại), và `persist()` batch_df nếu dùng nhiều lần. *(Lesson 27)*

**D9. [Junior] Các loại trigger và khi nào dùng?**
- `processingTime="5 seconds"`: nhịp đều — mặc định cho pipeline sống 24/7.
- `availableNow`: xử lý hết backlog rồi dừng — chạy "streaming theo lịch Airflow" như batch, tiết kiệm.
- `continuous`: experimental, latency ~ms nhưng giới hạn operator — thực tế hiếm dùng.
*(Lesson 24)*

**D10. [Senior] Consumer lag tăng liên tục — chẩn đoán và xử lý?**
Lag tăng đơn điệu = tốc độ xử lý < tốc độ vào. Thứ tự chẩn đoán:
1. Batch duration có > trigger interval không? (có = không bao giờ đuổi kịp)
2. Nghẽn ở đâu: **nguồn** (số Kafka partition < tổng core → tăng partition topic), **xử lý** (stage nào lâu — skew? state to? UDF?), hay **sink** (ghi chậm, small files)?
3. Cầm máu: `maxOffsetsPerTrigger` để ổn định + scale executor/dynamic allocation.
4. Đường dài: tối ưu logic, sửa gốc.
Đừng chỉ nói "thêm máy" — interviewer chờ câu hỏi "nghẽn ở đâu" trước. *(Lesson 24, 39, 40)*

### E. Lakehouse / Iceberg + Production (10 câu)

**E1. [Junior] Tại sao cần table format (Iceberg) thay vì thư mục Parquet trần?**
Parquet trần: không ACID (đọc lúc đang ghi thấy dữ liệu nửa vời), schema evolution nguy hiểm (rename cột = thảm họa), listing chậm, không time travel. Iceberg thêm **metadata layer**: commit atomic theo snapshot, statistics để prune file, schema evolution theo column ID, nhiều engine đọc/ghi an toàn đồng thời. *(Lesson 30)*

**E2. [Mid] Cấu trúc metadata của Iceberg: snapshot, manifest là gì?**
Chuỗi trỏ: `metadata.json` (trạng thái bảng + con trỏ snapshot hiện tại) → snapshot (một version) → manifest list → manifest files (danh sách data file + stats min/max per column) → data files (Parquet). Commit = swap metadata.json mới một cách atomic trên catalog — nguồn gốc của ACID. Stats trong manifest cho phép **file-level pruning** trước khi mở bất kỳ data file nào. *(Lesson 30)*

**E3. [Junior] Time travel dùng làm gì trong thực tế?**
- Debug: "hôm qua số đúng, hôm nay sai" — diff hai snapshot.
- Khôi phục: rollback về snapshot trước khi ghi hỏng.
- Reproducibility cho ML/report, và audit.
Giới hạn: chỉ sống đến khi snapshot bị expire bởi maintenance. *(Lesson 31)*

**E4. [Mid] Hidden partitioning của Iceberg hơn gì partition kiểu Hive?**
Hive: partition là cột vật lý (`dt='2026-07-08'`) — query phải filter đúng cột đó mới prune được, đổi scheme là viết lại bảng. Iceberg: partition là **transform** trên cột gốc (`days(event_time)`, `bucket(16, user_id)`) — người query cứ filter cột gốc, engine tự prune; partition evolution không cần rewrite dữ liệu cũ. *(Lesson 33)*

**E5. [Mid] Pipeline CDC hoàn chỉnh từ Postgres vào lakehouse trông thế nào?**
Postgres WAL → Debezium (envelope before/after/op) → Kafka → Spark Streaming parse op → foreachBatch `MERGE INTO` Iceberg (op=c/u → upsert, op=d → delete; SCD2 thì đóng valid_to + mở bản ghi mới). Ba điểm khó phải nói ra được: thứ tự event per key (Kafka key = PK), dedup trong batch (lấy op cuối per key theo LSN), schema evolution từ nguồn. *(Lesson 29, Project 2)*

**E6. [Mid] Bảng Iceberg cần maintenance gì định kỳ?**
- `rewrite_data_files` — compaction gộp small files (bắt buộc với streaming sink).
- `expire_snapshots` — xóa snapshot cũ, giải phóng storage (đánh đổi: mất time travel tương ứng).
- `remove_orphan_files`, rewrite manifests khi metadata phình.
Chạy bằng Airflow DAG daily — bỏ bê thì bảng chậm dần và storage phình âm thầm. *(Lesson 32)*

**E7. [Junior] Medallion architecture — tại sao 3 lớp?**
- Bronze: raw như nguồn, immutable — replay/audit được, là "sự thật gốc".
- Silver: sạch, dedup, chuẩn hóa — dùng chung nhiều team.
- Gold: mô hình theo nghiệp vụ (star schema, metrics) — phục vụ BI/ML.
Tách lớp = mỗi lớp một hợp đồng chất lượng; sửa logic chỉ cần replay từ lớp dưới thay vì ingest lại từ nguồn. *(Lesson 34)*

**E8. [Mid] SCD Type 2 là gì, implement bằng Iceberg thế nào?**
Giữ lịch sử dimension: thay vì UPDATE đè, thêm bản ghi mới với `valid_from/valid_to/is_current`. MERGE INTO: matched & có thay đổi → đóng bản cũ (`valid_to = now`, `is_current = false`) + insert bản mới; not matched → insert. Fact join dim theo khoảng thời gian hiệu lực hoặc surrogate key. *(Lesson 34, 29)*

**E9. [Senior] Hai writer cùng commit vào một bảng Iceberg — chuyện gì xảy ra?**
Optimistic concurrency: cả hai đọc snapshot S, cùng chuẩn bị metadata mới; commit là compare-and-swap trên catalog — kẻ đến sau bị reject và tự **retry**: kiểm tra xung đột với snapshot mới (append vs append thường hòa; overwrite/delete chồng file thì fail hẳn). Bài học vận hành: compaction và streaming writer cùng bảng phải cấu hình retry/partial progress để không giẫm chân nhau. *(Lesson 31, 32)*

**E10. [Senior] Sizing cluster cho một job: bạn tính thế nào?**
Đi từ dữ liệu lên:
1. Input size sau pruning → số partition ≈ size ÷ 128MB.
2. SLA → tổng core cần: số partition ÷ core = số wave; wave × thời gian/task ≤ SLA.
3. Executor shape: 4–5 core/executor (béo quá → I/O kém, GC lâu; gầy quá → tốn overhead), memory ≈ partition size × hệ số nở 3–5× × core + overhead ~10%.
4. **Chạy thử và đo lại** — con số trên giấy chỉ là điểm xuất phát.
*(Lesson 38)*

---

## 4. Ba bài system design (lời giải khung)

Nguyên tắc chung cho mọi bài design DE: **Clarify → Số liệu → Kiến trúc → Deep-dive điểm khó → Vận hành → Trade-off**. Interviewer chấm *cách bạn nghĩ*, không chấm việc vẽ đúng logo công nghệ.

### Design 1 — "Thiết kế pipeline streaming 10M events/sec"

**Câu hỏi clarify nên hỏi (trước khi vẽ bất cứ thứ gì):**
- Event là gì, bao nhiêu bytes? (10M × 1KB = 10GB/sec ≈ 860TB/ngày — con số này đổi cả bài toán)
- Latency yêu cầu: sub-second per-event hay near-real-time giây/phút? Ai tiêu thụ output?
- Cần stateful không (dedup, sessionize, join) hay stateless transform?
- Exactly-once bắt buộc, hay at-least-once + downstream idempotent là đủ?
- Late data quan trọng không, muộn tối đa bao lâu?

**Kiến trúc khung:**

```
producers ──► KAFKA (N trăm partitions, key=entity_id, Avro+Schema Registry)
                 │
                 ├──► STREAM COMPUTE (Spark SS nếu SLA giây; Flink nếu sub-second)
                 │      parse → filter/enrich sớm → [stateful: RocksDB + watermark + TTL]
                 │      │
                 │      ├──► hot path: Kafka topic / Redis  (serving, alert)
                 │      └──► cold path: Iceberg (audit, replay) + compaction định kỳ
                 │
                 └──► dead-letter topic (poison messages)
 Prometheus/Grafana: lag (SLI số 1), batch duration, error rate  → autoscale theo backlog
```

**Khung giải từng bước:**
1. **Ingestion**: Kafka là buffer bắt buộc ở tải này. Tính partition: 10M events/sec ÷ ~10–20k events/sec/partition an toàn → hàng trăm–nghìn partition; key theo entity để giữ thứ tự per key. Nói về replication, acks, producer batching.
2. **Compute**: tải này ở biên của micro-batch. SLA giây → Spark Structured Streaming scale ngang được (tổng core tương xứng số Kafka partition); sub-100ms per-event → nói thẳng Flink/Kafka Streams hợp hơn — **dám đổi công cụ là điểm cộng lớn**.
3. **Serialization**: Avro/Protobuf + Schema Registry, không JSON — ở 10M events/sec, CPU parse là tiền thật.
4. **State**: nếu dedup/sessionize — RocksDB state store, watermark chặt, TTL; ước lượng state = số key hoạt động × bytes/key, **nói to con số này**.
5. **Sink**: fan-out hot/cold như diagram; không ghi trực tiếp DB OLTP ở tải này.
6. **Vận hành**: lag là SLI số 1; autoscale theo backlog chứ không chỉ CPU; dead-letter topic; game day: kill 20% executor, đo thời gian lag hồi phục.

**Trade-off nên chủ động nêu:** micro-batch latency vs vận hành đơn giản (một engine cho batch+stream); exactly-once vs throughput (transactional sink chậm hơn); JSON dễ debug vs Avro rẻ CPU; ít partition (rebalance nhanh) vs nhiều partition (trần parallelism cao).

**Red flags (điều khiến bạn rớt):** không hỏi kích thước event; vẽ 15 công nghệ mà không có con số nào; nói "Spark tự lo exactly-once"; không nhắc monitoring; không dám nói "sub-second thì tôi cân nhắc Flink".

### Design 2 — "Batch ELT 5TB/ngày, SLA 2h"

**Clarify:** 5TB raw hay compressed? Nguồn gì (DB dump, file, Kafka)? Data đến một cục lúc 0h hay rải rác cả ngày? SLA 2h tính từ mốc nào đến mốc nào (data đủ → gold sẵn sàng)? Downstream cần bảng nào, tươi cỡ nào? Có late/restated data (backfill) không?

**Kiến trúc khung:**

```
sources ──► landing (files/CDC dump)
              │  Airflow sensor "data đủ chưa?" (không cron mù)
              ▼
        BRONZE (Iceberg, as-is, partition dt) ──► SILVER (clean, dedup, conform)
              │                                        │
              │            broadcast dims, filter sớm  ▼
              │                                   GOLD (star schema, metrics)
              ▼                                        │
        cluster ephemeral/spot, ~200–400 core          ▼
        (job 2h/ngày — đừng nuôi cluster 24/7)    Trino / BI
 SLA guard: p95 runtime per task + alert baseline ×1.5 TRƯỚC khi vỡ SLA tổng
```

**Khung giải từng bước:**
1. **Số trước**: 5TB/2h ≈ 700MB/sec sustained. 5TB ÷ 128MB ≈ 40.000 partition. Chọn task 30–60s, chạy ≤10 wave → khởi điểm ~200–400 core, **đo rồi điều chỉnh**. Trình bày *phép tính*, không phải đáp số thuộc lòng.
2. **Kiến trúc**: medallion trên Iceberg như diagram; orchestrate bằng Airflow, trigger theo sensor "data đủ".
3. **Ba điểm ăn SLA nhất**: (a) đọc nguồn — JDBC phải partitioned read, file phải splittable; (b) shuffle ở silver/gold — filter/prune sớm, broadcast dim; (c) ghi — file size chuẩn, tránh small files.
4. **Idempotency**: mọi task re-run được — `INSERT OVERWRITE` theo partition ngày hoặc MERGE theo key; backfill 30 ngày = chạy lại DAG với logical date, không viết code riêng.
5. **SLA engineering**: budget 2h chia cho từng stage; đo p95 runtime nhiều ngày; alert khi task vượt baseline ×1.5; có "degraded mode" — gold tối thiểu trước, phần enrich nặng chạy sau.
6. **Cost**: cluster ephemeral + spot cho executor, on-demand cho driver; checkpoint giữa các layer để mất spot không phải chạy lại từ đầu.

**Trade-off:** ELT (transform trong lakehouse, replay dễ) vs ETL; overwrite partition (đơn giản, re-run an toàn) vs MERGE (đúng hơn với late update, đắt hơn); spot rẻ vs rủi ro mất executor gần cuối job.

**Red flags:** không tính con số nào; không nói idempotency/backfill; SLA chỉ được nhắc lại chứ không được *thiết kế* (không monitoring, không budget theo stage); "cứ bật AQE là nhanh".

### Design 3 — "Job đang chạy 4h thay vì 30min — debug thế nào?"

Đây là bài **quy trình chẩn đoán** — người ta muốn xem bạn có playbook hay chỉ đoán mò.

**Clarify (câu "what changed?" — 70% sự cố nằm ở đây):** Chậm từ bao giờ — đột ngột hôm nay hay xuống cấp dần? Có deploy code/config mới không? Data volume hôm nay so với baseline? Cluster có job khác giành tài nguyên không?

**Cây quyết định:**

```
So sánh run 30min (History Server) vs run 4h
   │
   ├─ Cùng plan, 1 stage phình? → mở Stages tab, nhìn phân phối task:
   │     ├─ max >> median task duration        → SKEW → tìm hot key → AQE/salting/filter
   │     ├─ đều chậm + cột Spill có số         → MEMORY → shuffle partitions↑ / mem↑
   │     ├─ vạn task ngắn ngủn                 → SMALL FILES / partition quá mịn
   │     └─ GC time đỏ                         → cache tham lam / executor quá béo
   ├─ Plan ĐỔI? → explain 2 bản: broadcast thành SMJ? (stats cũ, dim vượt threshold)
   │              filter mất pushdown? (đổi kiểu cột, UDF chen giữa)
   └─ Không phải Spark? → executor được cấp đủ chưa (cluster busy)?
                          nguồn chậm (DB nghẽn, S3 throttle)? sink nghẽn ghi?
                          → nhìn timeline: thời gian nằm ở scheduler delay hay compute?
```

**Khung giải từng bước (nói đúng thứ tự này):**
1. So sánh với lần chạy tốt trên History Server: job/stage nào phình? Cùng số stage hay xuất hiện stage mới (plan đổi)?
2. Nếu 1 stage chậm → đi theo nhánh trên của cây quyết định, mỗi nhánh nêu cả cách xác nhận lẫn cách fix.
3. Nếu plan đổi → so `explain`, tìm nguyên nhân statistics/schema.
4. Nếu không phải Spark → nguồn/sink/cluster — nhìn scheduler delay và executor add time.
5. **Đóng sự cố như senior**: số liệu before/after + nguyên nhân gốc một câu + guard (alert baseline ×1.5, test/stats) để không tái diễn.

**Trade-off nêu ra:** fix nhanh (thêm tài nguyên — hôm nay kịp SLA) vs fix đúng (sửa skew/logic — mai mới xong): nói rõ làm cả hai theo thứ tự đó; retry ngay vs giữ hiện trường để chẩn đoán.

**Red flags:** mở đầu bằng "tăng executor memory thử xem"; không hỏi "what changed"; không nhắc Spark UI/History Server; đổ cho "Spark chậm"; fix xong không đặt guard.

---

## 5. Checklist tự đánh giá trước phỏng vấn

Tick được ≥90% thì bạn sẵn sàng cho vòng technical. Ô nào trống → ôn lesson bên cạnh.

**Nền tảng & execution**
- [ ] Vẽ kiến trúc Driver/Executor/Cluster Manager và giảng lại trong 2 phút — *Lesson 1*
- [ ] Giải thích lazy evaluation và vì sao nó cho phép Catalyst tối ưu — *Lesson 2, 13*
- [ ] Nhìn 1 đoạn code, đoán đúng số job/stage trước khi chạy — *Lesson 3*
- [ ] Nói được partition quyết định parallelism thế nào, repartition vs coalesce — *Lesson 4, 16*

**SQL & tối ưu query**
- [ ] Đọc `explain(mode="formatted")` và chỉ ra join strategy + vị trí shuffle — *Lesson 9, 13*
- [ ] Kể 3 join strategy, điều kiện chọn, và 1 tai nạn broadcast — *Lesson 9*
- [ ] Giải thích tại sao UDF chậm + demo thay bằng built-in/pandas_udf — *Lesson 12*
- [ ] Kể 2 bẫy NULL từng gặp (three-valued logic, count) — *Lesson 14*

**Performance**
- [ ] Mô tả shuffle write/read và spill, biết nhìn ở đâu trong UI — *Lesson 15*
- [ ] Kể 1 lần xử lý skew end-to-end (phát hiện → salting/AQE → kết quả đo được) — *Lesson 19, 20*
- [ ] Giải thích memory model và chẩn đoán OOM driver vs executor — *Lesson 17, 40*
- [ ] Đọc thuộc playbook tuning 5 bước, "đo trước sửa sau" — *Lesson 22*

**Streaming**
- [ ] Vẽ timeline watermark với late event và nói chuyện gì xảy ra — *Lesson 25*
- [ ] Giải thích exactly-once end-to-end: source replay + checkpoint + idempotent sink — *Lesson 27*
- [ ] Kể cấu trúc checkpoint và 2 thay đổi làm nó vỡ — *Lesson 24*
- [ ] Mô tả stateful op mình từng viết (dedup/sessionize/applyInPandasWithState) + TTL — *Lesson 26, Project 4*

**Lakehouse & production**
- [ ] Mổ được metadata Iceberg: snapshot → manifest → data file, và vì sao commit là atomic — *Lesson 30, 31*
- [ ] Kể quy trình maintenance (compaction/expire) và chuyện gì xảy ra nếu bỏ bê — *Lesson 32*
- [ ] Trình bày pipeline CDC Debezium → MERGE INTO trong 3 phút — *Lesson 29, Project 2*
- [ ] Tính sizing cluster cho 1 job cụ thể, ra con số kèm phép tính — *Lesson 38*
- [ ] Kể 4 project của khóa theo STAR, mỗi cái 90 giây, có số liệu — *Project 1–4*

---

## 6. Tips trả lời phỏng vấn DE

**0. Chuẩn bị elevator pitch 30 giây — câu "giới thiệu về bạn" luôn mở màn.** Công thức: *hiện tại → bằng chứng → hướng tới*. Mẫu (sửa theo bạn):

> "Tôi là Data Engineer tập trung vào Spark ecosystem. Sáu tháng qua tôi xây 4 pipeline end-to-end trên stack Kafka–Spark–Iceberg–Airflow: từ batch ELT medallion, CDC lakehouse với Debezium, đến capstone là hệ fraud detection streaming 1k events/sec có stateful processing và exactly-once audit — có kill-test chứng minh. Tôi đang tìm team mà DE own cả vận hành pipeline chứ không chỉ viết job, vì phần tôi thích nhất là debug và tuning có số liệu."

Ba yêu cầu: dưới 45 giây, có ít nhất 1 con số, và câu cuối *mở đường* cho interviewer hỏi tiếp thứ bạn muốn được hỏi.

**1. Luôn trả lời bằng trade-off, không bằng đáp án tuyệt đối.** Câu hỏi "nên dùng X hay Y?" là bẫy tuyển senior: đáp án đúng có dạng *"phụ thuộc vào Z — nếu Z lớn thì X vì..., ngược lại Y vì...; ở project của tôi Z là ... nên tôi chọn ..."*. Người trả lời "luôn dùng X" tự dán nhãn junior bất kể X có đúng hay không.

**2. Nói bằng số đo, không bằng tính từ.** "Job nhanh hơn nhiều" = 0 điểm. "Runtime từ 55 phút xuống 9 phút, shuffle write từ 800GB xuống 90GB sau khi broadcast dim và filter sớm" = tin được ngay. Trước phỏng vấn, ghi lại 5–7 con số thật từ 4 project của khóa (throughput, latency, before/after tuning, kích thước data) — chúng là đạn của bạn.

**3. Kể project theo STAR, và đứng ở chữ R lâu nhất.**
- **S**ituation: 1 câu — "Pipeline fraud detection, yêu cầu 1k events/sec, alert trễ tối đa 10s."
- **T**ask: 1 câu — "Tôi phụ trách tầng streaming: scoring, state, exactly-once."
- **A**ction: 3–4 câu, nhấn *quyết định* chứ không phải *thao tác* — "Tôi chọn foreachBatch fan-out 3 sink thay vì 3 query vì đọc Kafka 1 lần và state 1 nơi; đánh đổi là mất atomicity giữa các sink, nên tôi thiết kế alert consumer idempotent theo transaction_id."
- **R**esult: số liệu + bài học — "Kill-test giữa chừng không sinh duplicate trong audit; điểm gãy đo được ở 5k events/sec do state store, và tôi biết cần gì để lên 10k."

**4. Được hỏi thứ chưa biết: đừng bịa — suy luận thành tiếng.** "Tôi chưa dùng Delta Lake production, nhưng nó cùng lớp table format với Iceberg mà tôi dùng nhiều, nên nó cũng phải giải các bài toán snapshot isolation, compaction...; khác biệt cụ thể tôi cần tra lại." Interviewer chấm cách bạn tư duy với thông tin thiếu — đúng năng lực cần cho on-call lúc 3h sáng.

**5. Với system design: hỏi trước, vẽ sau, số luôn miệng.** 5 phút đầu chỉ để clarify (mẫu câu hỏi ở mục 4). Mỗi mũi tên vẽ ra phải kèm một con số (events/sec, GB/ngày, số partition). Luôn tự kết bằng phần vận hành — monitoring, failure mode, cost — vì đó là phần ứng viên khác quên.

**6. Chuẩn bị 3 câu hỏi ngược cho interviewer.** Ví dụ: "Sự cố data gần nhất của team là gì và root cause?", "Ai own data quality — DE hay analyst?", "Tỷ lệ thời gian xây mới vs vận hành của team?". Câu hỏi ngược tốt cho thấy bạn đã *sống* trong nghề, không chỉ *học* nghề.

**7. Năm câu hành vi (behavioral) gần như chắc chắn gặp — chuẩn bị sẵn chuyện:**

| Câu hỏi | Chuyện nên kể (từ khóa học này) | Bẫy cần tránh |
|---|---|---|
| "Kể một lần bạn làm hỏng data/pipeline" | Lần checkpoint vỡ hoặc ghi duplicate trong lab lesson 27 — nhấn: phát hiện thế nào, khắc phục, và guard đặt sau đó | Nói "tôi chưa từng làm hỏng" — không ai tin, và nghĩa là bạn chưa làm gì đủ lớn |
| "Quyết định kỹ thuật khó nhất bạn từng đưa ra?" | Chọn Spark vs Flink cho capstone, hoặc 1 query fan-out vs 3 query — trình bày như trade-off có số liệu | Kể quyết định mà không nói phương án bị loại và vì sao |
| "Bất đồng với đồng nghiệp/mentor về giải pháp?" | Lần review code: bạn muốn giữ UDF vì dễ đọc, mentor yêu cầu built-in — bạn đo benchmark rồi mới kết luận | Kết thúc bằng "tôi đúng, họ sai" — người ta tuyển đồng đội, không tuyển quan tòa |
| "Deadline gấp, chất lượng hay tiến độ?" | Degraded mode của Design 2: gold tối thiểu trước SLA, phần enrich bổ sung sau — có phương án C chứ không chỉ chọn A/B | Trả lời tuyệt đối một phía |
| "Học công nghệ mới thế nào?" | Chính khóa này: theory → lab → đo đạc → project — kể kèm ví dụ applyInPandasWithState từ chưa biết đến chạy được trong capstone | Liệt kê tên khóa học suông, không có bằng chứng đầu ra |

**8. Ngày phỏng vấn — checklist 10 phút cuối:** mở sẵn 5–7 con số của bạn (tip 2); nhẩm lại mindmap mục 2; nhớ công thức exactly-once 3 phần (D5) và playbook debug (Design 3) — hai thứ được hỏi nhiều nhất ở vòng senior; và ngủ đủ — không câu trả lời nào cứu được một cái đầu đặc.

---

## 7. Lời cuối của mentor

24 tuần trước, câu hỏi đầu tiên của khóa là *"code PySpark của tôi chạy Ở ĐÂU?"*. Hôm nay bạn có trong tay: 4 project chạy được, một capstone đạt chuẩn design spec, và — quan trọng hơn cả — ba thói quen: *đo trước khi sửa, hỏi trade-off trước khi chọn, mở Spark UI trước khi đoán*. Không JD nào ghi ba điều đó ra, nhưng mọi buổi phỏng vấn senior đều kiểm tra chúng.

Đi phỏng vấn, mang theo con số. Vào nghề, mang theo checklist. Chúc bạn săn được offer xứng đáng.

> Chúc mừng bạn đi đến cuối khóa. Gõ **"Continue"** nếu muốn ôn tập bất kỳ phần nào.
