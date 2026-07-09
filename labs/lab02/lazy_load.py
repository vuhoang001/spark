import time 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("Lab 02 - DataFrame Transformations").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --------------------- Lazy evaluation ---------------------

t0 = time.time()
items = spark.read.csv("/workspace/data/olist/olist_order_items_dataset.csv", header=True, inferSchema=True)
orders = spark.read.csv("/workspace/data/olist/olist_orders_dataset.csv", header=True, inferSchema=True)

t_read = time.time() - t0


t0 = time.time()
revenue = (
    orders.filter(F.col("order_status") == "delivered")
    .join(items, on="order_id")
    .groupBy(F.to_date("order_purchase_timestamp").alias("d"))
    .agg(F.sum("price").alias("revenue"))
)

t_transform = time.time() - t0



t0 = time.time()
revenue.orderBy(F.desc("revenue")).show(5)
t_action = time.time() - t0

print(f"[A] read={t_read:.2f}s  transform={t_transform:.4f}s  action={t_action:.2f}s")
# Dự đoán trước khi chạy: cái nào gần 0? cái nào ngốn thời gian?
# (read KHÔNG gần 0 — thủ phạm là inferSchema. Đổi thành inferSchema=False chạy lại mà xem!)

# ---------- PHẦN B: RDD vs DataFrame, cùng một phép tính ----------
# Tổng price theo seller_id — cách 1: DataFrame (JVM thuần)
t0 = time.time()
df_top = (items.groupBy("seller_id").agg(F.sum("price").alias("total"))
               .orderBy(F.desc("total")))
df_result = df_top.take(5)
t_df = time.time() - t0

# cách 2: RDD (mỗi record đi vòng qua Python)
t0 = time.time()
rdd_result = (items.rdd
              .map(lambda r: (r["seller_id"], r["price"]))
              .reduceByKey(lambda a, b: a + b)
              .takeOrdered(5, key=lambda kv: -kv[1]))
t_rdd = time.time() - t0

print(f"[B] DataFrame={t_df:.2f}s | RDD={t_rdd:.2f}s | RDD chậm hơn ~{t_rdd/t_df:.1f}x")
print("DF :", [(r["seller_id"][:8], round(r["total"], 2)) for r in df_result])
print("RDD:", [(k[:8], round(v, 2)) for k, v in rdd_result])   # phải khớp nhau!

# ---------- PHẦN C: soi plan ----------
revenue.explain("formatted")

input(">>> Mở http://localhost:4040 xem Jobs, rồi Enter để thoát...")
spark.stop()