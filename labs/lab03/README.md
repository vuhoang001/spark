# Lab 03 — Joins, Aggregations & Window Functions

## Mục tiêu

- Hiểu join types và impact lên shuffle
- Thực hành aggregate, groupBy, window functions
- Nhận biết khi nào dùng broadcast join

## Tasks

### Basic
- [ ] Đọc `olist_orders_dataset.csv` và `olist_order_items_dataset.csv`
- [ ] Thực hiện inner join 2 bảng theo `order_id`
- [ ] Chọn cột `order_id`, `order_item_id`, `product_id`

### Intermediate
- [ ] Tính tổng `price` và `freight_value` theo `order_id`
- [ ] Dùng broadcast join nếu một bảng nhỏ
- [ ] Chạy `explain(True)` và xác định `BroadcastHashJoin` hoặc `SortMergeJoin`

### Advanced
- [ ] Tạo window function để tính `row_number` theo `order_purchase_timestamp` cho mỗi `customer_id`
- [ ] Tính `sum(product_price)` theo `customer_id` trong 30 ngày gần nhất
- [ ] Ghi báo cáo ngắn: join type nào chạy nhanh nhất với dữ liệu của bạn?

## Run

```bash
spark-submit labs/lab03/lab03.py
```
