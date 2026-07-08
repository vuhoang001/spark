# Lab 04 — File Formats, Partitioning & Performance

## Mục tiêu

- Luyện tập ghi/đọc Parquet
- Hiểu partitionBy và pushdown
- Nhận biết small-files issue và cache

## Tasks

### Basic
- [ ] Đọc `olist_orders_dataset.csv`
- [ ] Ghi thành Parquet tới `data/output/lab04/orders_parquet`
- [ ] Đọc lại Parquet và xác nhận schema

### Intermediate
- [ ] Ghi Parquet partitioned theo `order_status` hoặc `order_purchase_timestamp`
- [ ] So sánh thời gian đọc Parquet không partition vs partition
- [ ] Sử dụng `explain(True)` để kiểm tra predicate pushdown

### Advanced
- [ ] Tạo ra nhiều file nhỏ (ví dụ `.repartition(50)` rồi ghi)
- [ ] Đo ảnh hưởng small files bằng Spark UI
- [ ] Chạy `cache()` trên DataFrame và so sánh tốc độ khi đọc lại

## Run

```bash
spark-submit labs/lab04/lab04.py
```
