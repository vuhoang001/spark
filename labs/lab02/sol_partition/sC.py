"""Nhóm C — Shuffle partitions & AQE. Chạy: make run-local F=labs/lab02/sol_partition/sC.py"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
ITEMS = str(ROOT / "data" / "olist" / "olist_order_items_dataset.csv")

spark = SparkSession.builder.appName("solC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
items = spark.read.csv(ITEMS, header=True, inferSchema=True)


def non_empty_partitions(df):
    counts = (df.rdd
              .mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))])
              .collect())
    total = len(counts)
    non_empty = sum(1 for _, n in counts if n > 0)
    return total, non_empty


print("\n================= C1 =================")
# Tắt AQE để thấy con số THÔ 200 (AQE sẽ gộp lại nếu bật)
spark.conf.set("spark.sql.adaptive.enabled", "false")
agg = items.groupBy("seller_id").agg(F.sum("price").alias("total"))
print("C1) AQE OFF | mặc định spark.sql.shuffle.partitions =",
      spark.conf.get("spark.sql.shuffle.partitions"))
print("    số partition sau groupBy (stage sau shuffle):", agg.rdd.getNumPartitions())
print("    -> đúng bằng 200 (giá trị mặc định của shuffle.partitions).")

print("\n================= C2 =================")
spark.conf.set("spark.sql.shuffle.partitions", "8")
agg2 = items.groupBy("seller_id").agg(F.sum("price").alias("total"))
print("C2) set shuffle.partitions=8 -> số partition sau groupBy:", agg2.rdd.getNumPartitions())
print("    -> stage sau shuffle giờ chỉ 8 task thay vì 200.")

print("\n================= C3 =================")
spark.conf.set("spark.sql.shuffle.partitions", "200")
agg3 = items.groupBy("order_status" if "order_status" in items.columns else "order_id").agg(F.count("*"))
# dùng cột cardinality thấp giả lập data nhỏ: group theo order_item_id (ít nhóm)
agg_small = items.groupBy("order_item_id").agg(F.count("*").alias("c"))
total, non_empty = non_empty_partitions(agg_small)
print(f"C3) data nhỏ + shuffle.partitions=200: tổng {total} partition, chỉ {non_empty} có dữ liệu,",
      f"{total - non_empty} partition RỖNG.")
print("    -> quá nhiều partition rỗng = task rác, overhead lập lịch vô ích.")

print("\n================= C4 =================")
# So AQE ON vs OFF trên cùng groupBy
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "false")
off = items.groupBy("seller_id").agg(F.sum("price")).rdd.getNumPartitions()
spark.conf.set("spark.sql.adaptive.enabled", "true")
on_df = items.groupBy("seller_id").agg(F.sum("price"))
on_df.count()  # cần 1 action để AQE thực sự gộp
on = on_df.rdd.getNumPartitions()
print(f"C4) AQE OFF -> {off} partition | AQE ON -> {on} partition sau khi tối ưu")
print("    AQE tự coalesce các partition tí hon sau shuffle => ít partition, ít task rác.")

spark.stop()
