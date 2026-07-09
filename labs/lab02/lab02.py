import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    spark.sparkContext.setLogLevel("ERROR")

    data = [("HN", "book", 120.0), ("SG", "food", 80.0), ("HN", "food", 300.0),
            ("DN", "book", 150.0), ("SG", "book", 500.0)] * 40_000   # 200.000 dòng
    df = spark.createDataFrame(data, ["city", "category", "amount"])

    t0 = time.time()
    pipeline = (
        df.filter(F.col("amount") > 100)
        .withColumn("amount_usd", F.col("amount") / 25_000)
        .groupBy("city").agg(F.sum("amount_usd").alias("usd"))
    )

    print(f"Xây 3 transformation trên 200.000 dòng mất: {time.time()-t0:.4f}s")  # ~0.05s!
    pipeline.explain(True)


    t0 = time.time()
    pipeline.show()
    print(f"Action show() mất: {time.time()-t0:.2f}s")

    spark.stop()
