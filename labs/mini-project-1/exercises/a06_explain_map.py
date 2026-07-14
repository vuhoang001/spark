"""A6 — Đọc `explain(mode="formatted")` như đọc bản đồ. Khoanh đủ 5 điểm.

Chạy (local là đủ; bài này đọc PLAN chứ không đo giây):

    make run-local F=labs/mini-project-1/exercises/a06_explain_map.py

TỰ ĐỨNG ĐƯỢC: đề bảo "so với bản Parquet của Checkpoint 2", nhưng CP2 có thể chưa
làm xong. Script này TỰ ghi lấy một bản Parquet phân vùng theo order_date vào
/workspace/data/bench/a06_orders_pq (chỉ ghi nếu chưa có) rồi mới so plan.
Nên bài này KHÔNG phụ thuộc bài nào khác. Muốn ghi lại từ đầu: thêm tham số `rebuild`.

5 ĐIỂM PHẢI KHOANH TRONG MỖI PLAN (đề bài yêu cầu):
  1. `Scan csv` / `FileScan parquet` — đọc từ nguồn nào
  2. `PushedFilters:`      — filter nào ĐẨY được xuống tận reader
  3. `PartitionFilters:`   — KHÁC HẲN PushedFilters (xem phần kết luận cuối file)
  4. `ReadSchema:`         — có đúng chỉ những cột cần không (column pruning)
  5. `Exchange` = shuffle · `HashAggregate` xuất hiện 2 lần = partial + final

Script tự bắt 5 điểm đó ra thành bảng markdown, đồng thời in nguyên văn plan để dán
vào report (bằng chứng thô, không được sửa tay).
"""

import re
import sys
import contextlib
import io

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
ORDERS_PQ = "/workspace/data/bench/a06_orders_pq"   # parquet phân vùng, script tự ghi
TARGET_DAY = "2018-07-02"

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])


# ---------------------------------------------------------------------------
# Bộ đọc plan: bắt 5 điểm ra khỏi đống chữ
# ---------------------------------------------------------------------------
def explain_str(df, mode="formatted"):
    """Lấy explain ra dạng CHUỖI để vừa in vừa phân tích được.

    df.explain() in thẳng ra stdout và trả về None -> không xử lý được.
    Bắt lại bằng redirect_stdout (PySpark dùng print() bên trong .explain()).
    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def plan_nodes(plan):
    """Đếm node trong plan.

    Trong `mode="formatted"`, phần dưới của plan liệt kê chi tiết từng node theo dạng
        (5) HashAggregate
        (6) Exchange
    Đếm ở ĐÂY chứ không đếm trong cái cây phía trên — vì mỗi node xuất hiện đúng 1 lần
    ở đây, còn ở cây thì tên node lặp lại và dễ đếm trùng.
    """
    names = []
    for line in plan.splitlines():
        m = re.match(r"^\((\d+)\)\s+(.+?)\s*$", line)
        if m:
            # bỏ hậu tố kiểu "[codegen id : 1]"
            names.append(re.sub(r"\s*\[.*$", "", m.group(2)).strip())
    return names


def field(plan, key):
    """Lấy mọi dòng bắt đầu bằng `key` (vd 'PushedFilters:') trong plan."""
    out = []
    for line in plan.splitlines():
        s = line.strip()
        if s.startswith(key):
            out.append(s)
    return out


def n_read_cols(plan):
    """Đếm số cột trong ReadSchema — đây là bằng chứng COLUMN PRUNING."""
    rs = field(plan, "ReadSchema:")
    if not rs:
        return "?"
    inner = re.search(r"struct<(.*)>", rs[0])
    if not inner or not inner.group(1):
        return 0
    # tách theo dấu phẩy ở cấp ngoài cùng là đủ với schema phẳng của Olist
    return len(inner.group(1).split(","))


def report(name, df, plan):
    """In một block bằng chứng đầy đủ cho 1 query."""
    nodes = plan_nodes(plan)
    print("\n" + "=" * 78)
    print("QUERY: {}".format(name))
    print("=" * 78)
    print(plan)   # <-- BẰNG CHỨNG THÔ, dán nguyên văn vào report
    print("--- 5 điểm khoanh (script tự bắt từ plan trên) ---")
    print("| điểm | giá trị đo được |")
    print("|---|---|")
    scan = [n for n in nodes if "Scan" in n or "FileScan" in n]
    print("| 1. nguồn đọc | `{}` |".format(", ".join(scan) if scan else "(không thấy)"))
    pf = field(plan, "PushedFilters:")
    print("| 2. PushedFilters | `{}` |".format(pf[0][len("PushedFilters:"):].strip() if pf else "(node này không có dòng PushedFilters)"))
    parf = field(plan, "PartitionFilters:")
    print("| 3. PartitionFilters | `{}` |".format(parf[0][len("PartitionFilters:"):].strip() if parf else "(không có — nguồn không phân vùng)"))
    rs = field(plan, "ReadSchema:")
    print("| 4. ReadSchema | {} cột / 8 cột gốc — `{}` |".format(
        n_read_cols(plan), rs[0][len("ReadSchema:"):].strip() if rs else "?"))
    print("| 5a. số `Exchange` (= số shuffle) | **{}** |".format(nodes.count("Exchange")))
    print("| 5b. số `HashAggregate` | **{}** |".format(nodes.count("HashAggregate")))
    print("| (phụ) toàn bộ node | {} |".format(" → ".join(nodes)))
    return nodes


# ---------------------------------------------------------------------------
def path_exists(spark, path):
    """Hỏi Hadoop FS xem thư mục có chưa (không dùng os.path vì đường dẫn là của Spark)."""
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hpath)


def build_parquet(spark, orders):
    """Ghi bản Parquet phân vùng theo order_date để có cái mà so plan.

    repartition("order_date") TRƯỚC khi ghi: gom mọi dòng cùng ngày về cùng 1 task
    -> mỗi partition-ngày ra 1 file, thay vì 200 mảnh vụn. (Lý do đầy đủ: bài A17/A35.)
    """
    print("\n[setup] Chưa có {} -> đang ghi (chỉ làm 1 lần)...".format(ORDERS_PQ))
    (orders
     .withColumn("order_date", F.to_date("order_purchase_timestamp"))
     .filter(F.col("order_date").isNotNull())
     .repartition("order_date")
     .write.mode("overwrite")
     .partitionBy("order_date")
     .parquet(ORDERS_PQ))
    print("[setup] Xong.")


def main():
    rebuild = len(sys.argv) > 1 and sys.argv[1] == "rebuild"

    spark = SparkSession.builder.appName("a06-explain-map").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    orders = (spark.read
              .schema(ORDERS_SCHEMA)      # KHÔNG inferSchema (bài A5 đã chứng minh vì sao)
              .option("header", True)
              .csv(ORDERS_CSV))

    if rebuild or not path_exists(spark, ORDERS_PQ):
        build_parquet(spark, orders)

    pq = spark.read.parquet(ORDERS_PQ)

    # -----------------------------------------------------------------------
    # Q1 — CSV: filter đơn giản -> select -> groupBy tháng   (query gốc của đề)
    # -----------------------------------------------------------------------
    q1 = (orders
          .filter(F.col("order_status") == "delivered")
          .select("order_id", "order_purchase_timestamp")
          .groupBy(F.date_format("order_purchase_timestamp", "yyyy-MM").alias("thang"))
          .agg(F.count("order_id").alias("so_don")))
    n1 = report("Q1 · CSV · filter(status=delivered) -> select 2 cột -> groupBy(tháng)",
                q1, explain_str(q1))
    print("\n=> Kết quả thật (chứng minh query chạy được, không phải plan suông):")
    q1.orderBy("thang").show(3, truncate=False)

    # -----------------------------------------------------------------------
    # Q2 — CSV: filter PHỨC TẠP. Nó có được push xuống reader không?
    # -----------------------------------------------------------------------
    q2 = (orders
          .filter(F.substring(F.col("order_id"), 1, 2) == "ab")
          .select("order_id", "order_purchase_timestamp"))
    report("Q2 · CSV · filter(substring(order_id,1,2)='ab') — biểu thức PHỨC TẠP",
           q2, explain_str(q2))
    print("\n=> So dòng PushedFilters của Q2 với Q1. Reader CSV chỉ nhận được vài kiểu")
    print("   filter đơn giản (IsNotNull / EqualTo / GreaterThan...). `substring(...)`")
    print("   không nằm trong danh sách đó -> nó KHÔNG xuống được reader, phải leo lên")
    print("   thành một node `Filter` chạy SAU khi đã đọc đủ cả 99.441 dòng.")

    # -----------------------------------------------------------------------
    # Q3 — PARQUET: y hệt Q1. So 2 plan cạnh nhau.
    # -----------------------------------------------------------------------
    q3 = (pq
          .filter(F.col("order_status") == "delivered")
          .select("order_id", "order_purchase_timestamp")
          .groupBy(F.date_format("order_purchase_timestamp", "yyyy-MM").alias("thang"))
          .agg(F.count("order_id").alias("so_don")))
    report("Q3 · PARQUET · y hệt Q1", q3, explain_str(q3))

    # -----------------------------------------------------------------------
    # Q4 — PARQUET: filter vào ĐÚNG CỘT PHÂN VÙNG + một cột dữ liệu thường.
    #      Đây là chỗ PartitionFilters và PushedFilters cùng xuất hiện -> nhìn rõ
    #      chúng KHÔNG phải một thứ.
    # -----------------------------------------------------------------------
    q4 = (pq
          .filter((F.col("order_date") == F.lit(TARGET_DAY))          # cột PHÂN VÙNG
                  & (F.col("order_status") == "delivered"))           # cột DỮ LIỆU
          .select("order_id", "order_status"))
    report("Q4 · PARQUET · where order_date='{}' AND status='delivered'".format(TARGET_DAY),
           q4, explain_str(q4))
    print("\n=> Số dòng thật của Q4: {}".format(q4.count()))

    # -----------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("KẾT LUẬN A6 — trả lời đúng câu đề hỏi")
    print("=" * 78)
    print("""
[a] VÌ SAO `HashAggregate` XUẤT HIỆN 2 LẦN? (Q1 đếm được: {} lần)
    Vì Spark gom 2 pha, ngăn cách bởi Exchange:
      HashAggregate (partial) -> Exchange (shuffle) -> HashAggregate (final)
    Pha partial chạy NGAY TẠI CHỖ trên từng partition: 99.441 dòng gộp sớm còn ~vài
    chục dòng (số tháng) TRƯỚC khi bay qua mạng. Nếu không có pha này, cả 99.441 dòng
    phải shuffle. Đây là "combiner" của MapReduce, Catalyst tự cài hộ.
    -> Đếm `Exchange` = đếm số lần dữ liệu bay qua network. Q1 có {} cái.

[b] `PushedFilters` KHÁC `PartitionFilters` THẾ NÀO — câu này đề hỏi thẳng:

    PartitionFilters  = LOẠI BỎ CẢ THƯ MỤC, không mở file ra xem.
        Cột phân vùng (order_date) không nằm trong file Parquet — nó nằm trong TÊN
        THƯ MỤC (order_date=2018-07-02/). Spark chỉ cần liệt kê thư mục là biết bỏ
        cái nào. 599/600 thư mục bị loại mà KHÔNG TỐN 1 BYTE I/O nào.
        -> tiết kiệm ở mức FILE.

    PushedFilters     = MỞ FILE RA, nhưng bỏ bớt row-group / dòng bên trong.
        order_status là dữ liệu thật nằm trong file. Spark vẫn phải mở file, đọc
        footer, so min/max của từng row-group rồi mới bỏ được phần không khớp.
        -> tiết kiệm ở mức TRONG FILE. Vẫn phải chạm vào file.

    Với CSV thì PartitionFilters không tồn tại (không có thư mục phân vùng) và
    PushedFilters gần như vô dụng (CSV không có footer/min-max để mà skip — reader
    vẫn phải parse từng dòng text rồi mới bỏ). Đó là toàn bộ lý do ta đổi sang Parquet.

[c] Filter nào CSV KHÔNG push được mà Parquet push được?
    -> So dòng `PushedFilters` của Q1 (CSV) và Q3 (Parquet) ở trên. Và quan trọng hơn,
       so `PartitionFilters` của Q4: CSV KHÔNG BAO GIỜ có dòng này.

[d] COLUMN PRUNING: ReadSchema của Q1/Q3 chỉ có {} cột trong khi bảng gốc có 8.
    5 cột kia không bao giờ được đọc lên. Với CSV thì lợi ích này là GIẢ — reader vẫn
    phải quét hết từng ký tự của dòng text để biết dấu phẩy ở đâu, rồi mới vứt cột
    thừa đi. Với Parquet (lưu theo CỘT) thì nó là THẬT: cột không cần = không đọc byte
    nào. Muốn thấy bằng BYTES chứ không phải bằng chữ -> bài A30.

LƯU Ý AQE: cụm này bật AQE, nên plan mở đầu bằng `AdaptiveSparkPlan isFinalPlan=false`.
Nghĩa là plan bạn thấy ở đây là plan DỰ ĐỊNH; lúc chạy thật Spark có thể đổi (gộp
partition sau shuffle...). Plan CUỐI CÙNG chỉ xem được ở tab SQL của Spark UI sau khi
job chạy xong. Đây là chỗ explain() nói dối một nửa — nhớ lấy.
""".format(n1.count("HashAggregate"), n1.count("Exchange"), n_read_cols(explain_str(q1))))

    spark.stop()


if __name__ == "__main__":
    main()
