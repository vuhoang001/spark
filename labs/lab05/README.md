# Lab 05 — Structured Streaming & Kafka Integration

## Mục tiêu

- Bật Kafka và Kafka Connect từ `docker compose`
- Hiểu cách publish message vào topic
- Xây pipeline Spark Structured Streaming đọc Kafka

## Tasks

### Basic
- [ ] Start infrastructure:
  ```bash
docker compose up -d
```
- [ ] Tạo topic Kafka `olist_orders`
- [ ] Publish 10 sample events vào topic

### Intermediate
- [ ] Viết Spark Structured Streaming để đọc Kafka topic
- [ ] Parse JSON payload
- [ ] Write output ra console hoặc Parquet

### Advanced
- [ ] Thêm watermark cho event-time
- [ ] Làm stateful aggregation theo user hoặc order
- [ ] Kill job rồi restart, đảm bảo không bị duplicate nếu dùng checkpoint

## Notes

- Nếu Docker Compose chưa sẵn sàng, bạn có thể chỉ làm phần batch của lab này sau.
