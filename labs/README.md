# Labs Overview — Spark Mastery

Tài liệu này chuyên dùng để hướng dẫn các lab từ cơ bản đến nâng cao trong khoá học.

## Cách dùng

1. Bật infrastructure:
```bash
docker compose up -d
```
2. Với mỗi lab, mở folder `labs/lab0X/`.
3. Đọc `README.md` của lab, làm tuần tự các bài tập Basic → Intermediate → Advanced.
4. Chạy script bằng `spark-submit` nếu có.
5. Ghi kết quả, lỗi, và câu hỏi vào `labs/lab0X/README.md`.

---

## Danh sách lab

- `lab01/` — Spark basics & DataFrame startup
- `lab02/` — DataFrame transformations & SQL
- `lab03/` — Joins, aggregations, window functions
- `lab04/` — File formats, partition, performance
- `lab05/` — Structured Streaming & Kafka integration
- `lab06/` — CDC, production patterns, deployment

---

## Mục tiêu mỗi lab

### Lab 01 — Spark cơ bản
- Basic: chạy SparkSession, đọc CSV, show schema
- Intermediate: filter/select/count, lưu kết quả Parquet
- Advanced: tạo cột mới, groupBy đơn giản, nắm lazy evaluation

### Lab 02 — DataFrame transformations & SQL
- Basic: các phép `select`, `filter`, `withColumn`, `when`
- Intermediate: viết query Spark SQL, so với DataFrame API
- Advanced: aggregate phức tạp, expressions, null handling

### Lab 03 — Joins & Window
- Basic: inner join 2 bảng, hiểu shuffle
- Intermediate: broadcast join, multiple joins
- Advanced: window functions, ranking, running totals

### Lab 04 — File formats & performance
- Basic: ghi/đọc Parquet
- Intermediate: partitionBy, predicate pushdown
- Advanced: đo small files, benchmarking, caching

### Lab 05 — Streaming & Kafka
- Basic: Kafka producer/consumer local
- Intermediate: Structured Streaming đọc Kafka
- Advanced: watermark, stateful aggregation, restart stream

### Lab 06 — CDC & production patterns
- Basic: Debezium connector setup
- Intermediate: MERGE INTO Iceberg / UPSERT logic
- Advanced: Airflow DAG, monitoring, idempotency

---

## Ghi chú

- `data/olist/` là nơi chứa dữ liệu Olist. Nếu chưa có, đã có script `scripts/move_olist_data.sh` để copy từ `../kafka-flink/data/olist/`.
- Nếu bạn chỉ muốn làm batch, chỉ cần Spark + Postgres là đủ.
- Nếu muốn làm streaming/CDC, bật `docker compose up -d` để có Kafka và Kafka Connect.
