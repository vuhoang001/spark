"""Bài 5.4 — Mini-project chốt Lab 01: báo cáo đơn hàng theo (năm, tháng).

Đủ 5 yêu cầu:
  1. Đọc CSV với schema tường minh (không inferSchema)
  2. Làm sạch: bỏ dòng thiếu order_purchase_timestamp
  3. Report theo (year, month): tổng đơn, delivered, canceled, avg delivery_days, % trễ
  4. Ghi Parquet partition theo năm
  5. Đọc lại + filter năm 2017 -> soi PartitionFilters (partition pruning)

Chạy:  make run F=labs/lab01/lab01_report.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

DUONG_DAN_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
DUONG_DAN_PARQUET = "/workspace/data/output/lab01/orders_report"

# ---- 1. Schema tường minh: nhanh (không đọc file 2 lần), kiểu chính xác 100%
SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])

spark = (
    SparkSession.builder
    .appName("Lab 01 Report - don hang theo thang")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.option("header", True).schema(SCHEMA).csv(DUONG_DAN_CSV)

# ---- 2. Làm sạch
sach = orders.filter(F.col("order_purchase_timestamp").isNotNull())
print(f"Dong truoc lam sach: {orders.count()} | sau lam sach: {sach.count()}")

# ---- 3. Report theo (year, month)
co_cot_phu = (
    sach
    .withColumn("order_year", F.year("order_purchase_timestamp"))
    .withColumn("order_month", F.month("order_purchase_timestamp"))
    .withColumn("delivery_days", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("is_late", F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
)

report = (
    co_cot_phu
    .groupBy("order_year", "order_month")
    .agg(
        F.count("*").alias("tong_don"),
        F.count(F.when(F.col("order_status") == "delivered", 1)).alias("don_delivered"),
        F.count(F.when(F.col("order_status") == "canceled", 1)).alias("don_canceled"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg(F.col("is_late").cast("int")) * 100, 2).alias("pct_tre"),
    )
    .orderBy("order_year", "order_month")
)

print("=== Report theo (nam, thang) ===")
report.show(30)

# ---- 4. Ghi Parquet, partition theo năm
# coalesce(1): report bé tí, gom về 1 file/partition cho gọn (bài 3.2)
(
    report.coalesce(1)
    .write.mode("overwrite")
    .partitionBy("order_year")
    .parquet(DUONG_DAN_PARQUET)
)
print(f"Da ghi Parquet vao {DUONG_DAN_PARQUET}")

# ---- 5. Đọc lại, filter 1 năm, soi partition pruning
doc_lai = spark.read.parquet(DUONG_DAN_PARQUET)
nam_2017 = doc_lai.filter(F.col("order_year") == 2017)

print("=== Physical plan khi filter order_year = 2017 (tim dong PartitionFilters) ===")
nam_2017.explain()
print(f"So thang cua nam 2017 trong report: {nam_2017.count()}")

spark.stop()
