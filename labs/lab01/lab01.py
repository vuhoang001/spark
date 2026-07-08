from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "olist"
ORDERS_FILE = DATA_DIR / "olist_orders_dataset.csv"

if __name__ == "__main__":
    # Không hardcode .master() ở đây — để lệnh spark-submit quyết định:
    #   --master local[4]                  -> chạy local trong 1 JVM
    #   --master spark://spark-master:7077 -> chạy trên standalone cluster

    
    spark = (
        SparkSession.builder
        .appName("Lab 01 - Spark Session")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    
    orders = (
        spark.read
        # .option("header", True)
        .parquet("/workspace/data/olist/output/lab01/bai_43.parquet")
    )

    
    orders.printSchema()
    spark.stop()