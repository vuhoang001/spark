"""
BÁC SĨ PARTITION — chĩa vào 1 file CSV, tự chẩn đoán + gợi ý.

Chạy:
  make run-local F=labs/lab02/sol_partition/partition_doctor.py          # mặc định order_items
  # hoặc truyền file khác (đường dẫn tương đối từ gốc repo):
  docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
    --master 'local[4]' /workspace/labs/lab02/sol_partition/partition_doctor.py \
    data/olist/olist_orders_dataset.csv
"""
import os
import sys
import math
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[3]
# file truyền qua tham số, mặc định order_items
rel = sys.argv[1] if len(sys.argv) > 1 else "data/olist/olist_order_items_dataset.csv"
FILE = str(ROOT / rel) if not os.path.isabs(rel) else rel
MB = 1024 * 1024

spark = SparkSession.builder.appName("partition_doctor").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv(FILE, header=True, inferSchema=True)

# ---------- ĐO ----------
num_parts = df.rdd.getNumPartitions()
dp = spark.sparkContext.defaultParallelism
file_size = os.path.getsize(FILE)
dist = sorted(df.rdd.mapPartitionsWithIndex(
    lambda i, it: [(i, sum(1 for _ in it))]).collect())
counts = [n for _, n in dist]
total_rows = sum(counts)
avg = total_rows / len(counts) if counts else 0
mx, mn = (max(counts), min(counts)) if counts else (0, 0)
skew_ratio = mx / avg if avg else 0

# ---------- TÍNH GỢI Ý ----------
target_by_size = max(1, math.ceil(file_size / (128 * MB)))   # mỗi partition ~128MB
target_by_core_lo, target_by_core_hi = dp * 2, dp * 3        # 2-3x core
suggest_shuffle = dp * 2

print("=" * 60)
print(f"📄 FILE: {rel}")
print(f"   kích thước: {file_size/MB:.2f} MB | tổng dòng: {total_rows:,}")
print("=" * 60)
print("\n🔎 ĐO ĐƯỢC")
print(f"   số partition hiện tại : {num_parts}")
print(f"   defaultParallelism    : {dp}  (số core khả dụng)")
print(f"   dòng/partition         : min={mn:,}  avg={avg:,.0f}  max={mx:,}")
print(f"   tỉ lệ lệch (max/avg)   : {skew_ratio:.2f}x")

print("\n🩺 CHẨN ĐOÁN")
issues = []
if skew_ratio >= 2:
    issues.append(f"⚠️  SKEW: partition to nhất gấp {skew_ratio:.1f}x trung bình -> 1 task sẽ chậm. "
                  "Cân nhắc salting / bật adaptive.skewJoin.")
if num_parts < dp:
    issues.append(f"⚠️  ÍT partition ({num_parts}) < số core ({dp}) -> chưa tận dụng hết core. "
                  f"repartition({target_by_core_lo}).")
if num_parts > target_by_core_hi * 4 and file_size < 64 * MB:
    issues.append(f"⚠️  NHIỀU partition ({num_parts}) cho data nhỏ -> nhiều task rác. "
                  f"coalesce({target_by_core_lo}).")
if not issues:
    issues.append("✅ Không thấy vấn đề rõ ràng: partition cân đối với data & số core.")
for s in issues:
    print("   " + s)

print("\n💡 GỢI Ý CON SỐ")
print(f"   • Theo kích thước data (~128MB/partition): ~{target_by_size} partition")
print(f"   • Theo số core (2–3× {dp})              : {target_by_core_lo}–{target_by_core_hi} partition")
print(f"   • spark.sql.shuffle.partitions gợi ý     : {suggest_shuffle} (thay cho mặc định 200)")
print(f"   → Chọn số LỚN hơn giữa 'theo size' và 'theo core' = "
      f"{max(target_by_size, target_by_core_lo)} partition là hợp lý.")
print("=" * 60)

spark.stop()
