# Lab 06 — CDC & Production Patterns

## Mục tiêu

- Hiểu CDC bằng Debezium
- Xây ứng dụng MERGE/UPSERT vào Iceberg hoặc Parquet
- Học cách thiết kế production pipeline với idempotency

## Tasks

### Basic
- [ ] Đọc tài liệu Debezium Postgres connector
- [ ] Khởi chạy `docker compose up -d`
- [ ] Kết nối Postgres với Kafka Connect

### Intermediate
- [ ] Viết script Spark để đọc Kafka topic Debezium
- [ ] Xử lý `before/after` message và chuyển đổi hành động `c/u/d`
- [ ] Ghi kết quả vào Parquet hoặc Iceberg với MERGE logic

### Advanced
- [ ] Thiết kế Airflow DAG để orchestrate batch + stream
- [ ] Làm monitoring cơ bản: lag, lỗi, restart
- [ ] Ghi runbook: nếu job fail, kiểm tra gì đầu tiên?

## Notes

- Nếu chưa dùng Iceberg, bạn có thể làm bằng Parquet trước và sau đó nâng cấp.
- Lấy sample payload từ `data/olist` bằng cách convert CSV thành JSON để gửi vào Kafka.
