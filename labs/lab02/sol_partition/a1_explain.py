"""A1 giải thích: in đủ tham số + tự tính maxSplitBytes -> ra số partition."""
import math
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[3]
ITEMS = str(ROOT / "data" / "olist" / "olist_order_items_dataset.csv")

spark = SparkSession.builder.appName("a1_explain").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

items = spark.read.csv(ITEMS, header=True, inferSchema=True)
actual = items.rdd.getNumPartitions()

# --- Lấy các tham số Spark dùng để chia file ---
maxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024)))
openCostInBytes  = int(spark.conf.get("spark.sql.files.openCostInBytes",  str(4 * 1024 * 1024)))
defaultParallelism = spark.sparkContext.defaultParallelism
# minPartitionNum mặc định = defaultParallelism (nếu không set riêng)
minPartitionNum = int(spark.conf.get("spark.sql.files.minPartitionNum", str(defaultParallelism)))

import os
fileSize = os.path.getsize(ITEMS)

# --- Công thức Spark (FilePartition.maxSplitBytes) ---
totalBytes  = fileSize + openCostInBytes          # mỗi file cộng thêm openCost
bytesPerCore = totalBytes / minPartitionNum
maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))
est_partitions = math.ceil(fileSize / maxSplitBytes)

MB = 1024 * 1024
print("========== THAM SỐ ==========")
print(f"fileSize            = {fileSize:,} bytes  (~{fileSize/MB:.2f} MB)")
print(f"maxPartitionBytes   = {maxPartitionBytes:,} bytes  (~{maxPartitionBytes/MB:.0f} MB)")
print(f"openCostInBytes     = {openCostInBytes:,} bytes  (~{openCostInBytes/MB:.0f} MB)")
print(f"defaultParallelism  = {defaultParallelism}")
print(f"minPartitionNum     = {minPartitionNum}")
print("\n========== TỰ TÍNH ==========")
print(f"totalBytes   = fileSize + openCost           = {totalBytes:,}")
print(f"bytesPerCore = totalBytes / minPartitionNum  = {bytesPerCore:,.0f}  (~{bytesPerCore/MB:.2f} MB)")
print(f"maxSplitBytes= min({maxPartitionBytes/MB:.0f}MB, max({openCostInBytes/MB:.0f}MB, {bytesPerCore/MB:.2f}MB))")
print(f"             = {maxSplitBytes:,.0f} bytes  (~{maxSplitBytes/MB:.2f} MB)")
print(f"số partition ước tính = ceil({fileSize/MB:.2f}MB / {maxSplitBytes/MB:.2f}MB) = {est_partitions}")
print("\n========== THỰC TẾ ==========")
print(f"items.rdd.getNumPartitions() = {actual}")

spark.stop()
