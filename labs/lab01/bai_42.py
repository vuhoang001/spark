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


result = (
    orders
    .withColumn('order_year', F.year('order_purchase_timestamp'))
    .withColumn('delivery_days', F.datediff('order_delivered_customer_date', 'order_purchase_timestamp'))
    .withColumn('is_late', F.when(F.col('delivery_days') > 7, True).otherwise(False))
)


result.select('order_id', 'order_year', 'delivery_days', 'is_late').show(10, truncate=False)





