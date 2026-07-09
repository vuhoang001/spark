"""Nhóm E — Data skew & salting. Chạy: make run-local F=labs/lab02/sol_partition/sE.py"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("solE").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # tắt để nhìn skew thô


def partition_sizes(df):
    return sorted(df.rdd
                  .mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))])
                  .collect())


# Data lệch: key 'HOT' chiếm ~90%, còn lại rải nhiều key nguội
hot = spark.range(900_000).select(F.lit("HOT").alias("key"), F.lit(1.0).alias("amt"))
cold = spark.range(100_000).select(F.concat(F.lit("k"), (F.col("id") % 500)).alias("key"),
                                   F.lit(1.0).alias("amt"))
data = hot.union(cold)

print("\n================= E1 =================")
# repartition theo key để lộ skew: các dòng cùng key về 1 partition
skewed = data.repartition(8, "key")
sizes = partition_sizes(skewed)
print("E1) Số dòng mỗi partition khi hash theo 'key' (data 90% là HOT):")
for i, n in sizes:
    print(f"    partition {i}: {n:>7} dòng")
mx, mn = max(n for _, n in sizes), min(n for _, n in sizes)
print(f"    -> max={mx}, min={mn}, lệch {mx/max(mn,1):.0f}x. 1 partition khổng lồ = 1 task chạy mãi (straggler).")

print("\n================= E2 =================")
t0 = time.time()
plain = data.groupBy("key").agg(F.sum("amt").alias("s"))
plain.count()
t_plain = time.time() - t0

# SALTING: tách HOT thành nhiều key phụ (key + salt 0..15), gộp 2 bước
SALT = 16
t0 = time.time()
salted = data.withColumn("salt", (F.rand() * SALT).cast("int"))
partial = salted.groupBy("key", "salt").agg(F.sum("amt").alias("ps"))   # bước 1: gộp theo key+salt (rải đều)
final = partial.groupBy("key").agg(F.sum("ps").alias("s"))              # bước 2: gộp lại theo key
final.count()
t_salt = time.time() - t0

# kiểm tra kết quả HOT khớp nhau
hot_plain = plain.filter(F.col("key") == "HOT").collect()[0]["s"]
hot_salt = final.filter(F.col("key") == "HOT").collect()[0]["s"]
print(f"E2) groupBy thường = {t_plain:.2f}s | salting({SALT}) = {t_salt:.2f}s")
print(f"    Kết quả HOT: thường={hot_plain}  salting={hot_salt}  -> {'KHỚP' if hot_plain==hot_salt else 'SAI'}")
print("    Salting rải HOT ra 16 sub-key => 16 task cùng gánh thay vì 1 task ôm hết.")

print("\n================= E3 =================")
# skew trong join: bảng lớn lệch join bảng nhỏ dim
big = data  # 90% HOT
dim = spark.createDataFrame([("HOT", "hot_name")] +
                            [(f"k{i}", f"n{i}") for i in range(500)], ["key", "name"])
joined = big.join(dim, on="key")
js = partition_sizes(joined.repartition(8, "key"))
print("E3) Join theo 'key' lệch -> phân bố sau join:")
mxj = max(n for _, n in js)
print(f"    partition lớn nhất giữ {mxj} dòng (toàn HOT). Task đó xử lý lâu nhất = cả job chờ nó.")

print("\n================= E4 =================")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
j2 = big.join(dim, on="key")
t0 = time.time()
j2.count()
t_aqe = time.time() - t0
plan = j2._jdf.queryExecution().executedPlan().toString()
print(f"E4) AQE skewJoin ON: join xong trong {t_aqe:.2f}s")
print("    AQE phát hiện partition lệch và TỰ TÁCH nó thành nhiều partition con (split skew) —",
      "không cần salting thủ công. Tìm 'AQEShuffleRead ... skewed' trong plan.")
print("    có OptimizeSkewedJoin/AQEShuffleRead trong plan?", "AQEShuffleRead" in plan)

spark.stop()
