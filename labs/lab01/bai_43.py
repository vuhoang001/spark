from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("Lab 01 - Demo 42")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

orders = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("/workspace/data/olist/olist_orders_dataset.csv")
)


df = (
    orders
    .withColumn("order_year",  F.year("order_purchase_timestamp"))
    .withColumn("order_month", F.month("order_purchase_timestamp"))
    .withColumn("delivery_days", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("is_late", F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
)


df.groupBy("order_year").count().orderBy("order_year").show(truncate=False)



result = df.groupBy("order_year").agg(
    F.round(F.avg("delivery_days"), 1).alias("avg_days"),
    F.min("delivery_days").alias("min_days"),
    F.max("delivery_days").alias("max_days"),
).orderBy("order_year")


result.write.mode("overwrite").parquet("/workspace/data/olist/output/lab01/bai_43.parquet")




