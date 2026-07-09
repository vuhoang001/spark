# Bài tập PARTITION — bộ luyện tập

> Dùng data `data/olist/*`. Chạy bằng `make run-local F=...` hoặc `make run F=...`.
> Chỉ có ĐỀ BÀI, không có đáp án. Muốn lời giải: nhắn "đáp án <mã bài>".
> Với mỗi bài, luôn tự hỏi: có shuffle không? số partition thay đổi ra sao? soi Spark UI (localhost:8080 / 4040) để đối chiếu.

## Nhóm A — Đếm & quan sát partition (Easy)
- [ ] A1. Đếm số partition mặc định khi đọc `olist_order_items_dataset.csv` (dùng `.rdd.getNumPartitions()`).
- [ ] A2. In ra `spark.sparkContext.defaultParallelism` và so sánh với số partition thực tế của một DataFrame vừa tạo bằng `range()`.
- [ ] A3. Viết hàm in số dòng của TỪNG partition (dùng `mapPartitionsWithIndex` hoặc `glom`) — partition có đều nhau không?
- [ ] A4. Chạy một action, mở Spark UI và xác nhận: số TASK ở stage = số partition. Ghi lại con số.
- [ ] A5. Đổi `spark.sql.files.maxPartitionBytes` (ví dụ 1m) rồi đọc lại file — số partition thay đổi thế nào? Giải thích.

## Nhóm B — repartition & coalesce (Medium)
- [ ] B1. `repartition(10)` một DataFrame đang có ít partition — kiểm tra số partition mới và xem trên UI có sinh shuffle (Exchange) không.
- [ ] B2. `coalesce(2)` cùng DataFrame đó — số partition mới là bao nhiêu, có shuffle không? Khác `repartition` chỗ nào?
- [ ] B3. Thử `coalesce(50)` để TĂNG số partition từ 8 lên 50 — có tăng thật không? Tại sao?
- [ ] B4. `repartition(8, F.col("seller_id"))` — partition theo hash cột. Kiểm tra các dòng cùng `seller_id` có rơi vào cùng partition không.
- [ ] B5. `repartitionByRange(8, "price")` — khác `repartition` theo hash ở điểm nào? In khoảng giá trị `price` trong mỗi partition.
- [ ] B6. Đo thời gian ghi ra parquet giữa `repartition(1)` và `coalesce(1)` trên cùng data. Cái nào nhanh hơn, vì sao?

## Nhóm C — Shuffle partitions (Medium)
- [ ] C1. Không set gì, chạy một `groupBy().agg()` rồi soi UI: stage sau shuffle có bao nhiêu partition? Chứng minh mặc định = 200.
- [ ] C2. Set `spark.sql.shuffle.partitions=8`, chạy lại groupBy — đếm task ở stage sau. So với C1.
- [ ] C3. Với data nhỏ + shuffle.partitions=200, dùng `mapPartitionsWithIndex` đếm partition rỗng/tí hon. Vấn đề gì xảy ra?
- [ ] C4. Bật/tắt AQE (`spark.sql.adaptive.enabled`), chạy cùng groupBy — AQE có tự gộp partition sau shuffle không? So số partition cuối.

## Nhóm D — Partition khi GHI file (Medium/Hard)
- [ ] D1. Ghi `orders` ra parquet với `.partitionBy("order_status")` — mô tả cấu trúc thư mục sinh ra.
- [ ] D2. Trong một thư mục partition (vd `order_status=delivered/`), số file part-* phụ thuộc cái gì? Chứng minh bằng cách đổi số partition trước khi ghi.
- [ ] D3. Đọc lại data đã partitionBy, filter `order_status='delivered'`, chạy `explain` — tìm bằng chứng **partition pruning** (chỉ đọc 1 thư mục).
- [ ] D4. Thử `partitionBy("customer_id")` (cardinality rất cao) — quan sát "small files problem". Đếm số file/thư mục sinh ra.

## Nhóm E — Data skew & salting (Hard)
- [ ] E1. Tạo data lệch nhân tạo (90% cùng 1 key), groupBy, soi UI: 1 task chạy lâu hơn hẳn. Ghi lại thời gian task max vs min.
- [ ] E2. Áp dụng kỹ thuật **salting** (thêm hậu tố ngẫu nhiên vào key rồi gộp 2 lần) để cân lại tải. So sánh thời gian với E1.
- [ ] E3. Tạo skew trong **join** (một bảng có 1 key chiếm đa số), quan sát 1 task treo. Mô tả hiện tượng.
- [ ] E4. Bật AQE skew-join (`spark.sql.adaptive.skewJoin.enabled`) — Spark tự tách partition lệch không? So sánh với E3.

## Nhóm F — Production Challenge
- [ ] F1. Cho cluster 4 core: chọn số partition "chuẩn" là bao nhiêu (quy tắc 2–3× số core)? Chứng minh bằng đo thời gian ở vài mức khác nhau.
- [ ] F2. Ghi bảng ra dạng **bucketed** (`bucketBy`) theo `order_id`, rồi join 2 bảng đã bucket — chứng minh join KHÔNG còn shuffle (soi `explain`).
- [ ] F3. Data pipeline ghi ra 200 file tí hon. Sửa để ghi ra ~ 8 file gọn (repartition/coalesce trước write). Đo lại số file + tổng thời gian đọc lại.
