"""A26 — `partitionOverwriteMode`: static vs dynamic. BÀI ĐẮT NHẤT CỦA CẢ TRACK.

Chạy:
    make run-local F=labs/mini-project-1/exercises/a26_partition_overwrite_mode.py
    (local đủ và AN TOÀN hơn: bài này ghi/xoá nhiều, không cần 6 core.)

Output: số thư mục partition TRƯỚC/SAU khi "chạy lại đúng 1 ngày", ở CẢ 2 mode.

Kịch bản mô phỏng đúng đời thật:
    "Sếp: ngày 2018-07-02 tính sai, chạy lại đúng ngày đó thôi nhé."
    Bạn: lọc ngày đó, .mode('overwrite').partitionBy('order_date').parquet(BẢNG)
    ... rồi đi ăn trưa. Khi quay lại thì công ty không còn dữ liệu.

Ghi ra thư mục RIÊNG (/workspace/data/bench/a26/...), KHÔNG đụng vào
data/output/silver/ — vì bài này CỐ TÌNH phá bảng để chứng minh nó phá được.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from schemas import ORDERS  # noqa: E402

SRC = "/workspace/data/olist/olist_orders_dataset.csv"
TABLE = "/workspace/data/bench/a26/orders_clean"
RERUN_DAY = "2018-07-02"


# --- Hadoop FS helpers: nhìn thư mục bằng đúng con mắt của Spark ---
def hadoop(spark):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path


def rmrf(spark, path):
    fs, Path = hadoop(spark)
    if fs.exists(Path(path)):
        fs.delete(Path(path), True)


def partition_dirs(spark, path):
    """Liệt kê các thư mục order_date=... — chính là 'find -type d | wc -l' của đề."""
    fs, Path = hadoop(spark)
    if not fs.exists(Path(path)):
        return []
    return sorted(
        s.getPath().getName() for s in fs.listStatus(Path(path))
        if s.isDirectory() and s.getPath().getName().startswith("order_date=")
    )


def count_part_files(spark, path):
    fs, Path = hadoop(spark)
    st = fs.globStatus(Path(path + "/*/part-*"))
    return len(st) if st else 0


def build_full_table(spark, df_all):
    """Ghi lại BẢNG ĐẦY ĐỦ (~600 ngày) từ đầu. Đây là 'trạng thái ban đầu' của mỗi thí nghiệm."""
    rmrf(spark, TABLE)
    # repartition("order_date") trước khi ghi: gom mọi dòng cùng ngày về CÙNG 1 task
    # -> mỗi partition-ngày ra ĐÚNG 1 file, thay vì 200 mảnh vụn (xem A35).
    (df_all.repartition("order_date")
        .write.mode("overwrite").partitionBy("order_date").parquet(TABLE))


def snapshot(spark, label):
    dirs = partition_dirs(spark, TABLE)
    n_rows = spark.read.parquet(TABLE).count() if dirs else 0
    n_day = 0
    if dirs:
        n_day = (spark.read.parquet(TABLE)
                 .filter(F.col("order_date") == F.lit(RERUN_DAY)).count())
    files = count_part_files(spark, TABLE)
    print(f"  [{label}] thư mục partition = {len(dirs):4d} | file part-* = {files:4d} "
          f"| tổng dòng = {n_rows:6d} | dòng ngày {RERUN_DAY} = {n_day}")
    return len(dirs), n_rows, n_day, files


def main():
    spark = SparkSession.builder.appName("a26-partition-overwrite").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    all_orders = (spark.read.schema(ORDERS).option("header", True)
                  .option("mode", "PERMISSIVE").csv(SRC)
                  .withColumn("order_date", F.to_date("order_purchase_timestamp"))
                  .filter(F.col("order_date").isNotNull())   # A26 không bàn về null date, đó là việc của ingest.py
                  ).cache()
    total_src = all_orders.count()
    one_day = all_orders.filter(F.col("order_date") == F.lit(RERUN_DAY)).cache()
    n_one_day = one_day.count()
    print(f"Nguồn: {total_src} đơn có order_date; ngày {RERUN_DAY} có {n_one_day} đơn.\n")

    results = {}

    # =====================================================================
    # THÍ NGHIỆM 1 — STATIC (MẶC ĐỊNH! bạn không phải gõ gì để dính nó)
    # =====================================================================
    print("=" * 78)
    print("THÍ NGHIỆM 1 — partitionOverwriteMode = static  (MẶC ĐỊNH của Spark)")
    print("=" * 78)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    print(f"  conf hiện tại = {spark.conf.get('spark.sql.sources.partitionOverwriteMode')}")

    build_full_table(spark, all_orders)
    before = snapshot(spark, "TRƯỚC khi re-run 1 ngày")

    print(f"\n  >>> Giờ 'chạy lại đúng ngày {RERUN_DAY}' — đúng câu lệnh mà 99% người sẽ viết:")
    print("      one_day.write.mode('overwrite').partitionBy('order_date').parquet(TABLE)")
    (one_day.write.mode("overwrite").partitionBy("order_date").parquet(TABLE))

    after_static = snapshot(spark, "SAU khi re-run 1 ngày ")
    results["static"] = (before, after_static)

    print(f"\n  KẾT QUẢ: {before[0]} thư mục -> {after_static[0]} thư mục. "
          f"{before[1]} dòng -> {after_static[1]} dòng.")
    print("  Không exception. Không cảnh báo. Job báo SUCCESS.")
    print("  VÌ SAO: 'static' nghĩa là phạm vi ghi đè được xác định bởi ĐƯỜNG DẪN bạn ghi,")
    print("  chứ không phải bởi DỮ LIỆU bạn ghi. Bạn ghi vào TABLE -> Spark xoá TABLE.")
    print("  Nó làm đúng thứ bạn BẢO nó làm, chỉ là không phải thứ bạn NGHĨ.")

    # =====================================================================
    # THÍ NGHIỆM 2 — DYNAMIC (phải tự bật)
    # =====================================================================
    print("\n" + "=" * 78)
    print("THÍ NGHIỆM 2 — partitionOverwriteMode = dynamic")
    print("=" * 78)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    print(f"  conf hiện tại = {spark.conf.get('spark.sql.sources.partitionOverwriteMode')}")

    build_full_table(spark, all_orders)     # dựng lại bảng đầy đủ
    before_d = snapshot(spark, "TRƯỚC khi re-run 1 ngày")

    # Ghi đè có CHỦ ĐÍCH: chỉ những partition CÓ MẶT trong DataFrame mới bị thay.
    # Ở đây DF chỉ chứa 1 ngày -> chỉ 1 thư mục bị thay, 599 thư mục kia không ai động vào.
    print(f"\n  >>> Cùng CHÍNH XÁC câu lệnh đó, chỉ khác 1 dòng conf:")
    (one_day.write.mode("overwrite").partitionBy("order_date").parquet(TABLE))

    after_dyn = snapshot(spark, "SAU khi re-run 1 ngày ")
    results["dynamic"] = (before_d, after_dyn)

    print(f"\n  KẾT QUẢ: {before_d[0]} thư mục -> {after_dyn[0]} thư mục. "
          f"{before_d[1]} dòng -> {after_dyn[1]} dòng.")

    # =====================================================================
    # THÍ NGHIỆM 3 — dynamic có IDEMPOTENT không? Ghi đè CÙNG ngày 2 lần nữa
    # =====================================================================
    print("\n" + "=" * 78)
    print("THÍ NGHIỆM 3 — dynamic chạy lại 2 lần nữa: có nhân đôi dòng của ngày đó không?")
    print("=" * 78)
    for i in (2, 3):
        one_day.write.mode("overwrite").partitionBy("order_date").parquet(TABLE)
        snapshot(spark, f"sau lần re-run thứ {i}   ")
    print("  (3 dòng trên phải giống hệt nhau. Khác = pipeline không idempotent.)")

    # =====================================================================
    # BẢNG BẰNG CHỨNG
    # =====================================================================
    print("\n" + "=" * 78)
    print("BẢNG (dán vào PROGRESS §3.6 'partitionOverwriteMode')")
    print("=" * 78)
    print()
    print("| | Số thư mục partition TRƯỚC | SAU khi ghi đè 1 ngày | Tổng dòng TRƯỚC | Tổng dòng SAU |")
    print("|---|---|---|---|---|")
    for mode in ("static", "dynamic"):
        b, a = results[mode]
        note = " *(mặc định!)*" if mode == "static" else ""
        print(f"| `{mode}`{note} | {b[0]} | **{a[0]}** | {b[1]} | **{a[1]}** |")

    b, a = results["static"]
    lost_dirs = b[0] - a[0]
    lost_rows = b[1] - a[1]
    print(f"""
CÂU IN HOA MÀ TÔI SẼ KHÔNG BAO GIỜ QUÊN:

    OVERWRITE MẶC ĐỊNH (static) KHÔNG GHI ĐÈ PARTITION — NÓ GHI ĐÈ CẢ BẢNG.
    CHẠY LẠI MỘT NGÀY BẰNG static = XOÁ {lost_dirs} THƯ MỤC VÀ {lost_rows} DÒNG DỮ LIỆU,
    JOB VẪN BÁO SUCCESS.

Ba điều rút ra:
1. 'static' phạm vi ghi đè = ĐƯỜNG DẪN. 'dynamic' phạm vi ghi đè = CÁC PARTITION
   CÓ MẶT TRONG DATAFRAME. Một chữ conf, hai vũ trụ khác nhau.
2. Vì sao Spark để static làm mặc định? Vì static là ngữ nghĩa "ghi đè" của
   filesystem thuần (xoá thư mục, ghi lại) — nó có TRƯỚC khi có khái niệm partition.
   Đây là nợ lịch sử, không phải thiết kế. Biết vậy để đừng tin vào mặc định.
3. dynamic ĐÃ đủ để idempotent (thí nghiệm 3 chứng minh: re-run 3 lần, số không đổi),
   NHƯNG nó chỉ ghi đè partition CÓ trong DataFrame. Nếu một ngày biến mất khỏi
   nguồn (dữ liệu bị rút lại), dynamic sẽ KHÔNG xoá partition cũ đó — dữ liệu ma
   nằm lại. Muốn xử lý cả trường hợp đó thì cần bảng có transaction: Delta/Iceberg
   (module 5). Ghi nhận: đây là giới hạn tôi CHƯA giải quyết được ở module 1.

=> src/ingest.py PHẢI có dòng này, và phải có nó TRƯỚC mọi lệnh write:
   spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
""")
    spark.stop()


if __name__ == "__main__":
    main()
