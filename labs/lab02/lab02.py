from pyspark.sql import SparkSession
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "olist"
ORDERS_FILE = DATA_DIR / "olist_orders_dataset.csv"

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Lab 02 - DataFrame Transformations")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(ORDERS_FILE))
    )

    orders.createOrReplaceTempView("orders")

    delivered = orders.filter("order_status = 'delivered'")
    delivered.select("order_id", "customer_id", "order_status").show(10, truncate=False)

    status_counts = delivered.groupBy("order_status").count()
    status_counts.show(truncate=False)

    sql_result = spark.sql(
        "SELECT order_status, COUNT(*) AS cnt FROM orders WHERE order_status = 'delivered' GROUP BY order_status"
    )
    sql_result.show(truncate=False)

    spark.stop()
