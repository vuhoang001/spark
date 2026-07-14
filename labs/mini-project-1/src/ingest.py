"""ingest.py — Checkpoint 1 + Checkpoint 2 của Mini Project 1.

Chạy:
    make run       F=labs/mini-project-1/src/ingest.py     # cluster (6 core)
    make run-local F=labs/mini-project-1/src/ingest.py     # local[2] — số đo ổn định hơn

Đọc:
    /workspace/data/olist/olist_orders_dataset.csv
    /workspace/data/olist/olist_order_items_dataset.csv
    /workspace/data/olist/olist_customers_dataset.csv

Ghi:
    /workspace/data/output/silver/orders_clean/      Parquet, partitionBy(order_date)
    /workspace/data/output/silver/items_clean/       Parquet, partitionBy(order_date)  (join lấy ngày từ orders)
    /workspace/data/output/silver/customers_clean/   Parquet, KHÔNG partition (biện luận bên dưới)
    /workspace/data/output/quarantine/<bảng>/        Parquet, dòng hỏng + lineage đầy đủ

IDEMPOTENT: chạy bao nhiêu lần cũng ra cùng một kết quả. Cơ chế:
    mode("overwrite") + spark.sql.sources.partitionOverwriteMode=dynamic   (xem A25, A26)

Toàn bộ file này là kết tinh của 9 bài tập track L5:
    A21 schema tường minh (src/schemas.py)   A22 chọn PERMISSIVE có căn cứ
    A23 biết dữ liệu bẩn trông ra sao        A24 cache() TRƯỚC khi filter _corrupt_record
    A25 chỉ overwrite mới idempotent         A26 overwrite phải đi kèm dynamic
    A29 lineage source_file + ingest_ts      (A27 format: đã chọn Parquet)
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)   # để `from schemas import ...` chạy được ở cả cluster lẫn local

from pyspark.sql import DataFrame, SparkSession, functions as F  # noqa: E402
from schemas import (  # noqa: E402
    CORRUPT_COL,
    CUSTOMERS_CORRUPT,
    ORDER_ITEMS_CORRUPT,
    ORDERS_CORRUPT,
)

SRC = "/workspace/data/olist"
SILVER = "/workspace/data/output/silver"
QUARANTINE = "/workspace/data/output/quarantine"

# Cột partition. Cardinality ~600 ngày -> mỗi partition ~165 dòng.
# THÀNH THẬT: với Olist (17 MB) con số này là SAI CHUẨN NGHỀ (chuẩn 64-256 MB/partition,
# ở đây mỗi partition ~15 KB — nhỏ hơn 4000 lần). Ta vẫn partition theo ngày vì:
#   (a) đề yêu cầu, và mục tiêu là HỌC partition pruning;
#   (b) nó mô phỏng đúng bảng THẬT sẽ có ở quy mô ×100..×10000.
# Đo cụ thể + biện luận đầy đủ nằm ở A20/A35. Đây là quyết định CÓ Ý THỨC, không phải cargo cult.
PARTITION_COL = "order_date"


def hadoop(spark):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path


def count_dirs(spark, path, prefix=f"{PARTITION_COL}="):
    fs, Path = hadoop(spark)
    if not fs.exists(Path(path)):
        return 0
    return sum(1 for s in fs.listStatus(Path(path))
               if s.isDirectory() and s.getPath().getName().startswith(prefix))


def count_files(spark, path):
    fs, Path = hadoop(spark)
    st = fs.globStatus(Path(path + "/*/part-*"))
    n = len(st) if st else 0
    if n == 0:   # bảng không partition
        st = fs.globStatus(Path(path + "/part-*"))
        n = len(st) if st else 0
    return n


# ---------------------------------------------------------------------------
# BƯỚC 1 — ĐỌC + TÁCH SẠCH/HỎNG  (Checkpoint 1)
# ---------------------------------------------------------------------------
def read_split(spark, filename, schema, name):
    """Đọc PERMISSIVE, gắn lineage, tách sạch/hỏng, ghi hỏng ra quarantine.

    Trả về (df_good, stats).

    Ba quyết định thiết kế, mỗi cái đều có một bài tập đứng sau:

    1. mode=PERMISSIVE (A22): Olist là dữ liệu phân tích. Mất vài dòng < dừng cả
       dashboard công ty. Nhưng PERMISSIVE chỉ hợp lệ khi CÓ quarantine + CÓ đếm
       -> nếu không thì nó chỉ là DROPMALFORMED chậm hơn.

    2. .cache() NGAY sau read (A24): bắt buộc. Filter theo _corrupt_record trên DF
       đọc thẳng từ CSV -> Spark NÉM LỖI (từ 2.3). Và ta đọc DF này 2 lần (bad + good)
       -> không cache thì parse CSV 2 lần. Cache là đúng cả về ngữ nghĩa lẫn hiệu năng.

    3. lineage NGAY sau read (A29): input_file_name() phải gắn khi DF còn dính FileScan.
       Gắn muộn (sau groupBy/join) -> ra chuỗi rỗng.
    """
    path = f"{SRC}/{filename}"
    df = (spark.read.schema(schema)
          .option("header", True)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", CORRUPT_COL)
          .csv(path)
          .withColumn("source_file", F.input_file_name())
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("ingest_run_id", F.lit(spark.sparkContext.applicationId))
          ).cache()

    n_total = df.count()                       # action -> vật chất hoá cache
    bad = df.filter(F.col(CORRUPT_COL).isNotNull())

    # BẢNG SẠCH bỏ ingest_ts + ingest_run_id, CHỈ GIỮ source_file.
    # VÌ SAO: current_timestamp() và applicationId đổi theo TỪNG LẦN CHẠY. Nhét chúng
    # vào silver thì chạy 2 lần ra 2 file KHÁC NHAU về byte -> không so được checksum,
    # không phát hiện được thay đổi thật. Idempotent nghĩa là "chạy lại ra ĐÚNG cái cũ",
    # nên bảng chính phải chứa TOÀN cột tất định. Hai cột kia thuộc về QUARANTINE
    # (nơi ta cần biết "lần chạy nào, lúc mấy giờ đẻ ra dòng rác này" — A29).
    good = (df.filter(F.col(CORRUPT_COL).isNull())
              .drop(CORRUPT_COL, "ingest_ts", "ingest_run_id"))
    n_bad = bad.count()
    n_good = n_total - n_bad

    # wc -l phiên bản Spark: đếm dòng THÔ để đối chiếu (tiêu chí chấm của CP1).
    n_raw = spark.read.text(path).count()

    # Quarantine: dòng hỏng + ĐỦ BỘ LINEAGE (địa chỉ + thời gian + tang vật).
    # overwrite: quarantine phản ánh LẦN CHẠY GẦN NHẤT -> idempotent.
    # (Muốn giữ lịch sử thì partitionBy(ngày chạy) + dynamic overwrite. Đánh đổi:
    #  giữ lịch sử thì bảng phình và phải tự dọn. Chọn overwrite cho project này.)
    q_path = f"{QUARANTINE}/{name}"
    (bad.select("source_file", "ingest_ts", "ingest_run_id", CORRUPT_COL)
        .write.mode("overwrite").parquet(q_path))

    stats = dict(name=name, raw_lines=n_raw, parsed=n_total,
                 good=n_good, bad=n_bad, quarantine=q_path)
    print(f"\n--- [{name}] {path}")
    print(f"    dòng THÔ (wc -l, kể cả header) : {n_raw}")
    print(f"    dòng Spark parse ra            : {n_total}   (= {n_raw} − 1 header)")
    print(f"      trong đó SẠCH                : {n_good}")
    print(f"      trong đó HỎNG (quarantine)   : {n_bad}   -> {q_path}")
    if n_raw - 1 != n_total:
        print(f"    ⚠️ LỆCH {n_raw - 1 - n_total} dòng so với wc-l−1. Nguyên nhân có thể:")
        print("       multiLine, ngoặc kép lệch nuốt dòng, hoặc dòng trống cuối file. PHẢI điều tra.")
    print("    schema đọc được:")
    for line in good._jdf.schema().treeString().splitlines():
        print("      " + line)
    return good, stats


# ---------------------------------------------------------------------------
# BƯỚC 2 — GHI PARQUET PHÂN VÙNG THEO NGÀY  (Checkpoint 2)
# ---------------------------------------------------------------------------
def write_partitioned(spark, df: DataFrame, path: str, label: str):
    """Ghi Parquet partitionBy(order_date), idempotent.

    repartition(PARTITION_COL) TRƯỚC khi ghi — KHÔNG phải coalesce:
      - Không repartition: mỗi task đang giữ dữ liệu của NHIỀU ngày -> mỗi task đẻ
        1 file trong MỖI thư mục ngày nó chạm tới -> 200 task × 600 ngày = rừng file
        vụn (xem A35). Mỗi file Parquet có footer riêng -> tổng dung lượng còn PHÌNH LÊN.
      - repartition("order_date"): hash theo ngày -> mọi dòng cùng ngày về CÙNG 1 task
        -> mỗi thư mục ngày ra ĐÚNG 1 file. Trả giá bằng 1 shuffle (~17 MB, rẻ).
      - coalesce(n) KHÔNG dùng được ở đây: coalesce chỉ GỘP partition sẵn có mà KHÔNG
        shuffle -> không gom được các dòng cùng ngày về một chỗ -> vẫn ra nhiều file/ngày.
        coalesce là để giảm SỐ FILE, không phải để SẮP XẾP dữ liệu.
    """
    t0 = time.time()
    (df.repartition(PARTITION_COL)
       .write.mode("overwrite")          # A25: chỉ overwrite mới idempotent
       .partitionBy(PARTITION_COL)       # A26: + dynamic (đã set ở main) mới không giết cả bảng
       .parquet(path))
    dt = time.time() - t0
    n_dirs = count_dirs(spark, path)
    n_files = count_files(spark, path)
    n_rows = spark.read.parquet(path).count()
    print(f"\n--- [{label}] ghi xong trong {dt:.1f}s -> {path}")
    print(f"    thư mục {PARTITION_COL}=... : {n_dirs}")
    print(f"    file part-*                : {n_files}   (mong đợi = số thư mục, tức 1 file/ngày)")
    print(f"    đọc lại đếm được           : {n_rows} dòng")
    return dict(label=label, dirs=n_dirs, files=n_files, rows=n_rows, write_sec=round(dt, 2))


def main():
    spark = (SparkSession.builder.appName("mp1-ingest").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # *** DÒNG QUAN TRỌNG NHẤT FILE NÀY *** (A26)
    # Thiếu nó: mọi lệnh overwrite lên bảng partition sẽ XOÁ CẢ BẢNG, không chỉ
    # partition đang ghi. Job vẫn báo SUCCESS. Đây là cách người ta mất 2 năm dữ liệu.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    print("=" * 78)
    print("MINI PROJECT 1 — INGEST (Checkpoint 1 + 2)")
    print("=" * 78)
    print(f"  master                    = {spark.sparkContext.master}")
    print(f"  applicationId             = {spark.sparkContext.applicationId}")
    print(f"  defaultParallelism        = {spark.sparkContext.defaultParallelism}")
    print(f"  shuffle.partitions        = {spark.conf.get('spark.sql.shuffle.partitions')}")
    print(f"  AQE                       = {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(f"  partitionOverwriteMode    = "
          f"{spark.conf.get('spark.sql.sources.partitionOverwriteMode')}   <-- PHẢI là dynamic")

    t_start = time.time()

    # ================= CHECKPOINT 1 =================
    print("\n" + "=" * 78)
    print("CHECKPOINT 1 — đọc, tách sạch/hỏng, quarantine")
    print("=" * 78)
    orders, s_orders = read_split(spark, "olist_orders_dataset.csv", ORDERS_CORRUPT, "orders")
    items, s_items = read_split(spark, "olist_order_items_dataset.csv",
                                ORDER_ITEMS_CORRUPT, "order_items")
    customers, s_cust = read_split(spark, "olist_customers_dataset.csv",
                                   CUSTOMERS_CORRUPT, "customers")

    # ================= CHECKPOINT 2 =================
    print("\n" + "=" * 78)
    print("CHECKPOINT 2 — derive order_date, ghi Parquet phân vùng, idempotent")
    print("=" * 78)

    # --- orders: order_date = to_date(order_purchase_timestamp) ---
    orders_dated = orders.withColumn(
        PARTITION_COL, F.to_date(F.col("order_purchase_timestamp")))

    # SỐ PHẬN CỦA DÒNG order_date NULL — quyết định + biện luận (đề bắt phải chọn):
    #   Chọn: ĐẨY VÀO QUARANTINE (có đếm), KHÔNG dồn vào partition "__unknown__".
    #   Vì sao:
    #     - Một đơn hàng KHÔNG CÓ NGÀY MUA là một đơn hàng vô nghĩa về mặt nghiệp vụ:
    #       nó không thuộc tháng nào, không vào được báo cáo nào, không join được theo ngày.
    #       Giữ nó trong bảng chính chỉ để mọi query sau này phải viết `WHERE date IS NOT NULL`.
    #     - Nếu để null, Spark ném nó vào thư mục __HIVE_DEFAULT_PARTITION__ — một cái tên
    #       mà 6 tháng sau không ai nhớ nghĩa, và nó ÂM THẦM lọt vào mọi full scan.
    #     - Quarantine thì nó vẫn CÒN ĐÓ (không mất dữ liệu), có source_file để truy ngược,
    #       và số lượng được ĐẾM ra màn hình mỗi lần chạy -> upstream hỏng là biết ngay.
    #   Đánh đổi tôi chấp nhận: bảng silver KHÔNG chứa 100% dòng nguồn. Chênh lệch được
    #   ghi rõ trong bảng tổng kết cuối file -> ai đọc report cũng cộng lại được.
    n_null_date = orders_dated.filter(F.col(PARTITION_COL).isNull()).count()
    print(f"\n[orders] số dòng có {PARTITION_COL} NULL = {n_null_date}"
          f"  -> đẩy vào quarantine (xem lý do trong code, không dồn vào __unknown__)")
    if n_null_date > 0:
        (orders_dated.filter(F.col(PARTITION_COL).isNull())
            .withColumn("quarantine_reason", F.lit("null_order_date"))
            .write.mode("overwrite").parquet(f"{QUARANTINE}/orders_null_date"))
    orders_ok = orders_dated.filter(F.col(PARTITION_COL).isNotNull())

    r_orders = write_partitioned(spark, orders_ok, f"{SILVER}/orders_clean", "orders_clean")

    # --- order_items: bảng này KHÔNG có ngày mua hàng ---
    # Hai lựa chọn, phải chọn 1 và nói lý do (đề yêu cầu):
    #   (a) partition theo tháng của shipping_limit_date  -> KHÔNG shuffle, rẻ, nhưng
    #       shipping_limit_date là HẠN GIAO của người bán, KHÔNG phải ngày phát sinh
    #       doanh thu. Query "doanh thu ngày 2018-07-02" sẽ KHÔNG prune được -> vô dụng
    #       đúng lúc cần nhất.
    #   (b) JOIN với orders để lấy order_date  -> tốn 1 shuffle join, nhưng items nằm
    #       CÙNG partition-ngày với orders. Mọi query doanh thu theo ngày prune được ở
    #       CẢ HAI bảng, và join orders×items sau này cùng khoá partition -> rẻ hơn.
    # CHỌN (b). Lý do một câu: PARTITION PHẢI THEO CỘT MÀ NGƯỜI TA FILTER, không phải
    # theo cột mà mình SẴN CÓ.
    order_dates = orders_ok.select("order_id", PARTITION_COL)
    items_dated = (items.join(F.broadcast(order_dates), on="order_id", how="left"))
    # broadcast: bảng order_dates chỉ 2 cột × 99k dòng (~2 MB) -> nhét vừa RAM mọi executor
    # -> Spark gửi nó tới từng executor thay vì shuffle CẢ HAI bảng. Xem lesson 3.

    n_orphan = items_dated.filter(F.col(PARTITION_COL).isNull()).count()
    print(f"\n[order_items] số dòng KHÔNG khớp order_id nào (mồ côi) = {n_orphan}")
    if n_orphan > 0:
        (items_dated.filter(F.col(PARTITION_COL).isNull())
            .withColumn("quarantine_reason", F.lit("orphan_order_id"))
            .write.mode("overwrite").parquet(f"{QUARANTINE}/items_orphan"))
    items_ok = items_dated.filter(F.col(PARTITION_COL).isNotNull())
    r_items = write_partitioned(spark, items_ok, f"{SILVER}/items_clean", "items_clean")

    # --- customers: KHÔNG partition ---
    # Vì sao: bảng này 8.6 MB, KHÔNG có cột thời gian, và luôn được dùng ở vế "dimension"
    # của join. Partition theo customer_state (27 giá trị) thì được gì? Query nào filter
    # theo state? Gần như không. Partition sai = thêm 27 thư mục để... không prune gì cả,
    # và tạo ra file nhỏ. KHÔNG PARTITION là câu trả lời đúng cho bảng dimension nhỏ.
    t0 = time.time()
    customers.coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/customers_clean")
    n_cust = spark.read.parquet(f"{SILVER}/customers_clean").count()
    print(f"\n--- [customers_clean] ghi xong {time.time() - t0:.1f}s, KHÔNG partition, "
          f"{count_files(spark, f'{SILVER}/customers_clean')} file, {n_cust} dòng")
    print("    (coalesce(1): 8.6 MB thì 1 file là đúng. Nhiều file nhỏ chỉ tổ hại — A35.)")

    # ================= TỔNG KẾT (dán vào PROGRESS §3.6) =================
    print("\n" + "=" * 78)
    print("BẢNG DATA QUALITY (dán vào PROGRESS §3.6)")
    print("=" * 78)
    print()
    print("| Bảng | Dòng thô (wc -l) | Dòng parse | Dòng hỏng (quarantine) | "
          "Dòng NULL date | Vào bảng chính | Khớp? |")
    print("|---|---|---|---|---|---|---|")
    print(f"| orders | {s_orders['raw_lines']} | {s_orders['parsed']} | {s_orders['bad']} | "
          f"{n_null_date} | {r_orders['rows']} | "
          f"{'✅' if s_orders['raw_lines'] - 1 == s_orders['bad'] + n_null_date + r_orders['rows'] else '❌ ĐIỀU TRA'} |")
    print(f"| order_items | {s_items['raw_lines']} | {s_items['parsed']} | {s_items['bad']} | "
          f"{n_orphan} *(mồ côi)* | {r_items['rows']} | "
          f"{'✅' if s_items['raw_lines'] - 1 == s_items['bad'] + n_orphan + r_items['rows'] else '❌ ĐIỀU TRA'} |")
    print(f"| customers | {s_cust['raw_lines']} | {s_cust['parsed']} | {s_cust['bad']} | — | "
          f"{n_cust} | {'✅' if s_cust['raw_lines'] - 1 == s_cust['bad'] + n_cust else '❌ ĐIỀU TRA'} |")
    print("\nQuy tắc kiểm: (dòng thô − 1 header) = hỏng + null_date/mồ côi + vào bảng chính.")
    print("Không khớp = có dòng bốc hơi ở đâu đó = PHẢI điều tra, không được bỏ qua.")

    print("\n" + "=" * 78)
    print("BẰNG CHỨNG IDEMPOTENT — chạy lại file này lần 2, 3 và so 3 dòng dưới đây")
    print("=" * 78)
    print()
    print("| Bảng | count() toàn bảng | số thư mục partition | số file part-* | count() 2018-07-02 |")
    print("|---|---|---|---|---|")
    for path, label in ((f"{SILVER}/orders_clean", "orders_clean"),
                        (f"{SILVER}/items_clean", "items_clean")):
        d = spark.read.parquet(path)
        n_day = d.filter(F.col(PARTITION_COL) == F.lit("2018-07-02")).count()
        print(f"| {label} | {d.count()} | {count_dirs(spark, path)} | "
              f"{count_files(spark, path)} | {n_day} |")

    print(f"\nTổng thời gian pipeline: {time.time() - t_start:.1f}s")
    print("Chạy lại chính lệnh này lần nữa -> 3 con số trên PHẢI không đổi. Đổi = mất 15 điểm.")
    spark.stop()


if __name__ == "__main__":
    main()
