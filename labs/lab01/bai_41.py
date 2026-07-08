"""Bài 4.1 — Đếm số đơn theo order_status, sắp xếp giảm dần.

Chạy:
  docker exec -it spark-mastery-spark-submit-1 \
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    /workspace/labs/lab01/bai_41.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Bai 4.1 - Dem don theo status")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Bài này chỉ đụng cột order_status (kiểu chữ) nên không cần inferSchema.
orders = (
    spark.read.option("header", True)
    .csv("/workspace/data/olist/olist_orders_dataset.csv")
)

# Bước 1: groupBy tạo các nhóm theo giá trị order_status  -> đây là shuffle (bài 3.3!)
# Bước 2: .count() trong ngữ cảnh groupBy là AGGREGATION -> thêm cột "count" cho mỗi nhóm
#         (khác với orders.count() đứng một mình - cái đó là action trả về 1 con số)
# Bước 3: orderBy sắp giảm dần theo cột count
by_status = (
    orders
    .groupBy("order_status")
    .count()
    .orderBy(F.col("count").desc())
)

# Tất cả phía trên là transformation - chưa chạy gì. show() mới là action.
print("=== So don theo tung trang thai ===")
by_status.show()

# Trả lời 2 câu hỏi của bài: bao nhiêu delivered, bao nhiêu canceled?
# collect() ở đây hợp lệ (bài 5.2): sau aggregation chỉ còn 8 dòng, kéo về driver vô hại.
ket_qua = {row["order_status"]: row["count"] for row in by_status.collect()}
tong = orders.count()

print(f"Tong so don           : {tong}")
print(f"Don delivered         : {ket_qua['delivered']} ({100 * ket_qua['delivered'] / tong:.1f}%)")
print(f"Don canceled          : {ket_qua['canceled']} ({100 * ket_qua['canceled'] / tong:.2f}%)")

spark.stop()
