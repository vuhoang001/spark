from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Lab 06 - CDC & Production")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    print("Lab 06 is a template for CDC and production pipeline tasks.")
    print("Use this script as a starting point for reading Kafka/Debezium and writing MERGE logic.")

    spark.stop()
