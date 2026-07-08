# Lab 01 — Spark Basics & DataFrame Startup

## Mục tiêu

- Khởi tạo SparkSession
- Đọc dataset Olist CSV
- Hiểu DataFrame schema và lazy execution
- Luyện tập `show()`, `printSchema()`, `count()`

## Thực hành

### Basic
- [ ] Chạy Spark script mẫu:
  ```bash
  spark-submit labs/lab01/lab01.py
  ```
- [ ] Xem schema và 5 dòng đầu
- [ ] Xác nhận file `data/olist/olist_orders_dataset.csv` đã đọc được

### Intermediate
- [ ] Thêm filter `order_status == 'delivered'`
- [ ] Chọn cột `order_id`, `customer_id`, `order_status`
- [ ] Đếm số đơn hàng giao thành công

### Advanced
- [ ] Tạo cột mới `order_year` từ `order_purchase_timestamp`
- [ ] GroupBy theo `order_year` và đếm đơn hàng
- [ ] Ghi ra Parquet dưới `data/output/lab01/orders_by_year`

## Notes

- Nếu lỗi `File not found`, kiểm tra `data/olist` đã chứa các CSV chưa.
- Nếu chạy chậm, mở Spark UI: http://localhost:8080
- Để chạy bằng Docker thay vì local, sử dụng file `docker-compose.spark.yaml` với Spark standalone cluster (`spark-master` + `spark-worker`).
