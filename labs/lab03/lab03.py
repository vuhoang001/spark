from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "olist"
ORDERS_FILE = DATA_DIR / "olist_orders_dataset.csv"
ORDER_ITEMS_FILE = DATA_DIR / "olist_order_items_dataset.csv"

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Lab 03 - Joins and Window")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    orders = spark.read.option("header", True).option("inferSchema", True).csv(str(ORDERS_FILE))
    items = spark.read.option("header", True).option("inferSchema", True).csv(str(ORDER_ITEMS_FILE))

    joined = orders.join(items, on="order_id", how="inner")
    joined.select("order_id", "order_item_id", "product_id").show(10, truncate=False)

    summary = joined.groupBy("order_id").sum("price", "freight_value")
    summary.show(10, truncate=False)

    small_df = broadcast(items)
    broadcast_join = orders.join(small_df, on="order_id", how="inner")
    broadcast_join.show(5, truncate=False)

    spark.stop()
