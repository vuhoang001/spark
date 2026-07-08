from pyspark.sql import SparkSession
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "olist"
ORDERS_FILE = DATA_DIR / "olist_orders_dataset.csv"
OUTPUT_DIR = ROOT / "data" / "output" / "lab04"

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Lab 04 - File Formats and Performance")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    orders = spark.read.option("header", True).option("inferSchema", True).csv(str(ORDERS_FILE))
    parquet_path = OUTPUT_DIR / "orders_parquet"
    parquet_path.mkdir(parents=True, exist_ok=True)

    orders.write.mode("overwrite").parquet(str(parquet_path))
    print(f"Wrote Parquet to {parquet_path}")

    parquet_df = spark.read.parquet(str(parquet_path))
    parquet_df.printSchema()
    parquet_df.show(5, truncate=False)

    spark.stop()
