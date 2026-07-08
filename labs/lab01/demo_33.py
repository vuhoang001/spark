"""Demo bài 3.3 — spark.sql.shuffle.partitions và AQE.

Chạy: spark-submit --master spark://spark-master:7077 /workspace/labs/lab01/demo_33.py
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Demo 3.3 - shuffle partitions").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

orders = (
    spark.read.option("header", True)
    .config("spark.sql.adaptive.enabled", "true")
    .csv("/workspace/data/olist/olist_orders_dataset.csv")
)

print("\n=== Kết quả groupBy (chạy 1 lần cho biết mặt mũi) ===")
orders.groupBy("order_status").count().orderBy(F.col("count").desc()).show()


def thi_nghiem(label):
    grouped = orders.groupBy("order_status").count()
    t0 = time.time()
    ket_qua = grouped.collect()          # action -> shuffle thật sự chạy
    thoi_gian = time.time() - t0
    so_manh = grouped.rdd.getNumPartitions()  # số partition SAU shuffle
    print(f"{label:<40} | ket qua: {len(ket_qua):>2} dong | partition sau shuffle: {so_manh:>3} | {thoi_gian:.2f}s")


print("=== 3 màn thí nghiệm ===")

# Màn 1: tắt AQE, để mặc định 200 -> thấy cái bẫy
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")
thi_nghiem("MAN 1: AQE OFF, shuffle.partitions=200")

# Màn 2: tắt AQE, chỉnh tay 8 -> đúng cỡ dữ liệu
spark.conf.set("spark.sql.shuffle.partitions", "8")
thi_nghiem("MAN 2: AQE OFF, shuffle.partitions=8")

# Màn 3: bật AQE, trả lại 200 -> Spark tự gộp mảnh rỗng
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
thi_nghiem("MAN 3: AQE ON , shuffle.partitions=200")

spark.stop()
