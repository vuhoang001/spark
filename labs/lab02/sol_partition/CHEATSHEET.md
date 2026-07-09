# 🗂️ PARTITION CHEATSHEET — dán lên bàn

## Mô hình 1 câu
> **1 partition = 1 task = 1 core làm 1 lúc.**
> Mục tiêu: mỗi partition **~100–200MB**, tổng số partition ≈ **2–3× số core**.

---

## 5 con số tròn cần nhớ
| Thứ | Giá trị | Dùng để |
|---|---|---|
| Partition lý tưởng | ~128 MB | cỡ mỗi mảnh khi đọc/ghi |
| `spark.sql.shuffle.partitions` | 2–3× số core (mặc định **200**) | số task sau groupBy/join |
| Kích thước 1 file khi ghi | vài chục–trăm MB | tránh "small files" |
| `defaultParallelism` | local[N]→N \| cluster→max(core,2) | trần song song |
| Bật **AQE** | `spark.sql.adaptive.enabled=true` | để Spark tự chỉnh giùm |

---

## Quy trình: ĐO → CHẨN → VẶN (không đoán mò)

### B1. ĐO — luôn có sẵn 2 thứ
```python
df.rdd.getNumPartitions()          # đang mấy partition?
```
+ Mở Spark UI `localhost:8080` → app → **Stages**: xem số Task & thời gian mỗi task.

Xem phân bố dòng mỗi partition (phát hiện skew):
```python
df.rdd.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]).collect()
```

### B2. CHẨN — triệu chứng ↔ bệnh
| Triệu chứng trên UI | Bệnh | Nút vặn |
|---|---|---|
| Ít task, mỗi task lâu, vài core rảnh | quá ÍT partition | `repartition(n)` ↑ / `maxPartitionBytes` ↓ |
| 200 task nhưng data nhỏ, xong tức thì | quá NHIỀU partition rác | `coalesce` / `shuffle.partitions` ↓ / bật AQE |
| 1 task chạy mãi, còn lại xong sớm | **SKEW** (lệch) | salting / `adaptive.skewJoin.enabled` |
| Ghi ra hàng nghìn file tí hon | nhiều partition trước write | `coalesce(n)` trước `.write` |
| Đọc lại quét cả bảng dù đã filter | không partition pruning | `.partitionBy(cột_lọc)` khi ghi |

### B3. VẶN — chỉ 4 nút chính
| Nút | Tác dụng | Shuffle? |
|---|---|---|
| `df.repartition(n)` | TĂNG hoặc rải đều partition | ✅ có |
| `df.repartition(n, "col")` | rải theo hash cột (cùng key → cùng partition) | ✅ có |
| `df.coalesce(n)` | GIẢM partition (không tách được) | ❌ không |
| `spark.conf.set("spark.sql.shuffle.partitions", n)` | số task sau shuffle | — |
| `spark.conf.set("spark.sql.files.maxPartitionBytes", n)` | độ mịn khi ĐỌC (set TRƯỚC read) | — |

---

## repartition vs coalesce (nhớ nhanh)
| | repartition(n) | coalesce(n) |
|---|---|---|
| Tăng partition | ✅ | ❌ (chỉ giảm) |
| Giảm partition | ✅ | ✅ |
| Shuffle | luôn có (full) | không |
| Chia đều | đều | có thể lệch |
| Dùng khi | cần rải đều / tăng / theo key | gom bớt trước khi ghi |

---

## Công thức số partition khi ĐỌC file (chỉ để hiểu, KHÔNG cần thuộc)
```
bytesPerCore  = (fileSize + 4MB) / defaultParallelism
maxSplitBytes = min( 128MB , max( 4MB , bytesPerCore ) )
số partition ≈ ceil( fileSize / maxSplitBytes )
```
- `max(4MB, …)` = SÀN → file < 4MB luôn 1 partition.
- `min(128MB, …)` = TRẦN → file cực to mỗi mảnh ≤ 128MB.
- Nhiều core → mảnh nhỏ hơn → nhiều partition hơn.

---

## Triết lý
> Đừng học để **tính trước**. Học để **đọc vị và phản ứng**:
> chạy → mở UI → thấy bất thường → tra bảng trên → vặn 1 nút → đo lại.
> Trong Spark 3+ production: **bật AQE** để nó tự lo phần lớn.

---

## Lệnh chạy nhanh (repo này)
```bash
make run-local F=labs/lab02/sol_partition/sA.py          # local[2]
make run       F=labs/lab02/sol_partition/sA.py          # cluster
# Bác sĩ partition (chĩa vào file bất kỳ):
make run-local F=labs/lab02/sol_partition/partition_doctor.py
docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
  --master 'local[4]' /workspace/labs/lab02/sol_partition/partition_doctor.py data/olist/olist_orders_dataset.csv
```
