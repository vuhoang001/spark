"""Nhóm A — Đếm & quan sát partition. Chạy: make run-local F=labs/lab02/sol_partition/sA.py"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
ITEMS = str(ROOT / "data" / "olist" / "olist_order_items_dataset.csv")

spark = SparkSession.builder.appName("solA").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext


def rows_per_partition(df):
    """Trả list [(partition_index, so_dong), ...]"""
    return (df.rdd
            .mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))])
            .collect())


print("\n================= A1 =================")
items = spark.read.csv(ITEMS, header=True, inferSchema=True)
print("A1) Số partition mặc định khi đọc order_items:", items.rdd.getNumPartitions())
# ------------------------------------------------------------------
# CÁCH SPARK TÍNH SỐ PARTITION KHI ĐỌC FILE (FilePartition.maxSplitBytes)
#
# 4 tham số:
#   fileSize          = 15,438,671 B  (~14.72 MB)   <- kích thước file thật
#   maxPartitionBytes = 128 MB  (spark.sql.files.maxPartitionBytes) <- trần 1 partition
#   openCostInBytes   = 4 MB    (spark.sql.files.openCostInBytes)   <- "phí" mở 1 file (cũng là SÀN)
#   minPartitionNum   = defaultParallelism = N trong local[N]       <- 2 nếu make run-local
#
# 3 bước:
#   (1) totalBytes    = fileSize + openCostInBytes        = 14.72MB + 4MB = 18.72MB
#   (2) bytesPerCore  = totalBytes / minPartitionNum      = 18.72MB / 2   = 9.36MB
#   (3) maxSplitBytes = min(maxPartitionBytes,
#                           max(openCostInBytes, bytesPerCore))
#                     = min(128MB, max(4MB, 9.36MB))      = 9.36MB
#
#   số partition = ceil(fileSize / maxSplitBytes) = ceil(14.72 / 9.36) = 2
#
# => Chỉ bước (2) phụ thuộc số core:
#      local[2]: bytesPerCore=9.36MB -> maxSplit=9.36MB -> ceil(14.72/9.36) = 2 partition
#      local[4]: bytesPerCore=4.68MB -> maxSplit=4.68MB -> ceil(14.72/4.68) = 4 partition
#   Nhiều core hơn -> mảnh nhỏ hơn -> nhiều partition hơn.
#   Hai chốt chặn: max(4MB, ...) là SÀN (file <4MB luôn 1 partition);
#                  min(128MB, ...) là TRẦN (file cực to mỗi mảnh tối đa 128MB).
# ------------------------------------------------------------------

print("\n================= A2 =================")
print("A2) defaultParallelism =", sc.defaultParallelism)
rng = spark.range(1000)
print("    spark.range(1000).getNumPartitions() =", rng.rdd.getNumPartitions(),
      "-> DataFrame tự tạo lấy số partition = defaultParallelism")

print("\n================= A3 =================")
dist = rows_per_partition(items)
print("A3) Số dòng mỗi partition của order_items:")
for idx, n in sorted(dist):
    print(f"    partition {idx}: {n} dòng")
sizes = [n for _, n in dist]
print(f"    -> min={min(sizes)}, max={max(sizes)}  (lệch nhau ít = chia khá đều)")

print("\n================= A4 =================")
print("A4) order_items có", items.rdd.getNumPartitions(),
      "partition -> khi chạy 1 action sẽ có ĐÚNG bấy nhiêu TASK ở stage đọc.")
print("    Chạy count() rồi mở Spark UI (localhost:8080 -> app -> Stages) đối chiếu số Tasks.")
print("    count() =", items.count())

print("\n================= A5 =================")
# Phải set TRƯỚC khi đọc file. Đặt maxPartitionBytes = 1MB.
spark.conf.set("spark.sql.files.maxPartitionBytes", str(1 * 1024 * 1024))  # 1MB
items_small = spark.read.csv(ITEMS, header=True, inferSchema=True)
print("A5) maxPartitionBytes = 1MB -> số partition khi đọc lại:",
      items_small.rdd.getNumPartitions())
print("    (giảm max split size => mỗi split nhỏ hơn => NHIỀU partition hơn A1)")
# trả lại mặc định
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))

spark.stop()
