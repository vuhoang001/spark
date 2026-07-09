"""Nhóm D — Partition khi GHI file. Chạy: make run-local F=labs/lab02/sol_partition/sD.py"""
import os
import glob
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
ORDERS = str(ROOT / "data" / "olist" / "olist_orders_dataset.csv")

spark = SparkSession.builder.appName("solD").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
orders = spark.read.csv(ORDERS, header=True, inferSchema=True)

OUT = "/tmp/sol_d"


def list_dirs(path):
    return sorted(d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d)))


def count_part_files(path):
    return len(glob.glob(os.path.join(path, "part-*")))


print("\n================= D1 =================")
p1 = OUT + "/by_status"
orders.write.mode("overwrite").partitionBy("order_status").parquet(p1)
print("D1) partitionBy('order_status') -> mỗi giá trị 1 thư mục:")
for d in list_dirs(p1):
    print("   ", d)
print("    Cấu trúc kiểu 'order_status=delivered/part-*.parquet' — tên cột nằm trong tên thư mục.")

print("\n================= D2 =================")
# ghi lại nhưng repartition(3) trước -> mỗi thư mục status tối đa 3 file
p2 = OUT + "/by_status_rep3"
orders.repartition(3).write.mode("overwrite").partitionBy("order_status").parquet(p2)
delivered = os.path.join(p2, "order_status=delivered")
print("D2) sau repartition(3), thư mục delivered có", count_part_files(delivered), "file part-*")
print("    -> số file trong mỗi thư mục = số partition (trong bộ nhớ) có chứa dữ liệu status đó.")

print("\n================= D3 =================")
back = spark.read.parquet(p1)
plan = back.filter(F.col("order_status") == "delivered")._jdf.queryExecution().toString()
pruned_line = [l.strip() for l in plan.splitlines() if "PartitionFilters" in l]
print("D3) đọc lại + filter order_status='delivered' -> explain có PartitionFilters:")
print("   ", pruned_line[0] if pruned_line else "(xem PartitionFilters trong explain)")
print("    -> Spark chỉ đọc thư mục delivered, BỎ QUA các thư mục status khác = partition pruning.")

print("\n================= D4 =================")
# customer_id cardinality rất cao -> lấy 2000 dòng minh hoạ small-files
p4 = OUT + "/by_customer"
sample = orders.limit(2000)
sample.write.mode("overwrite").partitionBy("customer_id").parquet(p4)
n_dirs = len(list_dirs(p4))
print(f"D4) partitionBy('customer_id') trên 2000 dòng -> {n_dirs} thư mục (gần bằng số dòng!)")
print("    -> cột cardinality CAO = bùng nổ thư mục + hàng nghìn file tí hon = 'small files problem'.")
print("    Quy tắc: chỉ partitionBy cột cardinality THẤP (status, ngày, vùng...).")

spark.stop()
