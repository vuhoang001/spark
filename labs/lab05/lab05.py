from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Lab 05 - Structured Streaming")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", TimestampType()),
        StructField("order_approved_at", TimestampType()),
        StructField("order_delivered_customer_date", TimestampType()),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "olist_orders")
        .option("startingOffsets", "earliest")
        .load()
    )

    json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("payload"))
    output_df = json_df.select("payload.*")

    query = (
        output_df.writeStream
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()
