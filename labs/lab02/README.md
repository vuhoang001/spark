# Lab 02 — DataFrame Transformations & SQL

## Mục tiêu

- Thực hành các phép biến đổi DataFrame cơ bản
- So sánh DataFrame API và Spark SQL
- Hiểu cách Spark tối ưu hóa query

## Tasks

### Basic
- [ ] Đọc `olist_orders_dataset.csv`
- [ ] Chọn các cột `order_id`, `customer_id`, `order_status`
- [ ] Lọc `order_status == 'delivered'`
- [ ] In 10 dòng đầu

### Intermediate
- [ ] Thêm cột `order_day` từ `order_purchase_timestamp`
- [ ] Tính số đơn hàng mỗi `order_status`
- [ ] Viết cùng query bằng Spark SQL (tạo temp view)
- [ ] So sánh `explain(True)` giữa DataFrame API và SQL

### Advanced
- [ ] Viết `withColumn` để tính `is_express = order_estimated_delivery_date < order_delivered_customer_date`
- [ ] Thực hiện `select` + `when` + `otherwise`
- [ ] Lưu kết quả ra `data/output/lab02/` theo Parquet
- [ ] Ghi ra file log thời gian chạy (grep `duration` từ Spark UI nếu dùng)

## Run

```bash
spark-submit labs/lab02/lab02.py
```
