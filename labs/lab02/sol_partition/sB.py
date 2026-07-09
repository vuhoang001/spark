"""Nhóm B — repartition & coalesce. Chạy: make run-local F=labs/lab02/sol_partition/sB.py"""
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
ITEMS = str(ROOT / "data" / "olist" / "olist_order_items_dataset.csv")

spark = SparkSession.builder.appName("solB").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

items = spark.read.csv(ITEMS, header=True, inferSchema=True)
print("Xuất phát: order_items có", items.rdd.getNumPartitions(), "partition")


def has_exchange(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    return "Exchange" in plan


print("\n================= B1 =================")
b1 = items.repartition(10)
print("B1) repartition(10) -> số partition:", b1.rdd.getNumPartitions(),
      "| có shuffle (Exchange)?", has_exchange(b1))
print("    repartition LUÔN full-shuffle (RoundRobinPartitioning) để rải đều.")

print("\n================= B2 =================")
b2 = items.coalesce(2)
print("B2) coalesce(2) -> số partition:", b2.rdd.getNumPartitions(),
      "| có shuffle (Exchange)?", has_exchange(b2))
print("    coalesce chỉ GỘP partition kề nhau trên cùng executor -> KHÔNG shuffle.")

print("\n================= B3 =================")
b3 = items.coalesce(50)
print("B3) coalesce(50) từ", items.rdd.getNumPartitions(), "partition -> thực tế:",
      b3.rdd.getNumPartitions())
print("    coalesce KHÔNG tăng được số partition (chỉ gộp, không tách). Muốn tăng phải repartition().")

print("\n================= B4 =================")
b4 = items.repartition(8, F.col("seller_id")).withColumn("pid", F.spark_partition_id())
max_np = (b4.groupBy("seller_id").agg(F.countDistinct("pid").alias("np"))
            .agg(F.max("np").alias("max_np")).collect()[0]["max_np"])
print("B4) repartition(8, seller_id): mỗi seller_id rơi vào tối đa", max_np, "partition")
print("    -> = 1 nghĩa là cùng key LUÔN cùng partition (hash partitioning). Rất quan trọng cho join.")

print("\n================= B5 =================")
b5 = items.repartitionByRange(8, "price").withColumn("pid", F.spark_partition_id())
rng = (b5.groupBy("pid").agg(F.min("price").alias("min_p"), F.max("price").alias("max_p"))
         .orderBy("pid").collect())
print("B5) repartitionByRange(8, price): khoảng giá mỗi partition (tăng dần, KHÔNG chồng lấn):")
for r in rng:
    print(f"    partition {r['pid']}: price {r['min_p']:.2f} .. {r['max_p']:.2f}")

print("\n================= B6 =================")
t0 = time.time()
items.repartition(1).write.mode("overwrite").parquet("/tmp/b6_repartition")
t_rep = time.time() - t0
t0 = time.time()
items.coalesce(1).write.mode("overwrite").parquet("/tmp/b6_coalesce")
t_col = time.time() - t0
print(f"B6) ghi 1 file: repartition(1)={t_rep:.2f}s | coalesce(1)={t_col:.2f}s")
print("    coalesce(1) thường nhanh hơn (không shuffle) NHƯNG bóp toàn bộ về 1 task ở bước trước -> mất song song.")

spark.stop()
