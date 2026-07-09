"""Nhóm F — Production Challenge. Chạy: make run-local F=labs/lab02/sol_partition/sF.py"""
import time
import glob
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
ITEMS = str(ROOT / "data" / "olist" / "olist_order_items_dataset.csv")

spark = (SparkSession.builder.appName("solF")
         .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
items = spark.read.csv(ITEMS, header=True, inferSchema=True).cache()
items.count()
CORES = spark.sparkContext.defaultParallelism

print("\n================= F1 =================")
print(f"F1) Cluster có {CORES} core. Quy tắc thực dụng: shuffle.partitions ≈ 2–3× số core.")
spark.conf.set("spark.sql.adaptive.enabled", "false")
for p in [1, CORES, CORES * 3, 200]:
    spark.conf.set("spark.sql.shuffle.partitions", str(p))
    t0 = time.time()
    items.groupBy("seller_id").agg(F.sum("price")).count()
    dt = time.time() - t0
    tag = "  <- quá ít, không tận dụng core" if p == 1 else ("  <- 200 mặc định, thừa task rác" if p == 200 else "")
    print(f"    shuffle.partitions={p:>3}: {dt:.2f}s{tag}")
print(f"    -> chọn ~{CORES*2}-{CORES*3} partition: đủ song song mà không sinh task rác.")

print("\n================= F2 =================")
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # tắt broadcast để ép sort-merge join
spark.sql("DROP TABLE IF EXISTS it_bucketed")
spark.sql("DROP TABLE IF EXISTS ord_bucketed")
base = spark.read.csv(ITEMS, header=True, inferSchema=True).select("order_id", "seller_id", "price")
base.write.mode("overwrite").bucketBy(8, "order_id").sortBy("order_id").saveAsTable("it_bucketed")
base.select("order_id", "price").write.mode("overwrite").bucketBy(8, "order_id").sortBy("order_id").saveAsTable("ord_bucketed")
a = spark.table("it_bucketed")
b = spark.table("ord_bucketed")
joined = a.join(b, on="order_id")
plan = joined._jdf.queryExecution().executedPlan().toString()
has_exchange = "Exchange" in plan
print("F2) Join 2 bảng đã bucketBy(8, order_id): có Exchange (shuffle) trong plan?", has_exchange)
print("    -> False = KHÔNG còn shuffle. Vì đã bucket cùng cột+cùng số bucket, dữ liệu sẵn 'cùng chỗ'.")
print("    Lợi ích: join lặp lại nhiều lần không phải shuffle lại mỗi lần.")

print("\n================= F3 =================")
# pipeline 'xấu' ghi ra 200 file tí hon
spark.conf.set("spark.sql.shuffle.partitions", "200")
bad = items.groupBy("seller_id").agg(F.sum("price").alias("total"))
bad.write.mode("overwrite").parquet("/tmp/f3_bad")
n_bad = len(glob.glob("/tmp/f3_bad/part-*.parquet"))
# sửa: coalesce về ~8 file trước khi ghi
good = bad.coalesce(8)
good.write.mode("overwrite").parquet("/tmp/f3_good")
n_good = len(glob.glob("/tmp/f3_good/part-*.parquet"))
print(f"F3) trước sửa: {n_bad} file part-*  ->  sau coalesce(8): {n_good} file")
# đo thời gian đọc lại
for path, label in [("/tmp/f3_bad", "nhiều file"), ("/tmp/f3_good", "ít file")]:
    t0 = time.time()
    spark.read.parquet(path).count()
    print(f"    đọc lại ({label}): {time.time()-t0:.2f}s")
print("    -> ít file to đọc nhanh & rẻ hơn nhiều file tí hon (giảm overhead mở/liệt kê file).")

spark.stop()
