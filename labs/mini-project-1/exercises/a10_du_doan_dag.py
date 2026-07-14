"""A10 — Sổ dự đoán: 6 query, ĐOÁN TRƯỚC, chạy sau.

Chạy:
    make run F=labs/mini-project-1/exercises/a10_du_doan_dag.py        # cluster
    make run-local F=labs/mini-project-1/exercises/a10_du_doan_dag.py  # local[2] cũng ra CÙNG số job/stage/shuffle

Output: bảng markdown dán thẳng vào PROGRESS.md §3.2 + phần "TÔI ĐOÁN SAI Ở ĐÂU".

────────────────────────────────────────────────────────────────────────────
⚠️ ĐỌC TRƯỚC KHI CHẠY — nếu không, bài này VÔ GIÁ TRỊ
────────────────────────────────────────────────────────────────────────────
Giá trị của bài không nằm ở cột "thực tế" (máy tự in). Nó nằm ở khoảng cách
giữa cột "đoán" và cột "thực tế". Vậy nên:

    1. MỞ dict PREDICTIONS bên dưới.
    2. SỬA các con số thành DỰ ĐOÁN CỦA CHÍNH BẠN (đừng đọc phần `why`).
    3. RỒI mới chạy.

Số đang có sẵn trong file là dự đoán của người viết đề, suy ra từ lesson 3
mục 3.2 (`số stage = số shuffle + 1`). Chép lại nó rồi bảo "tôi đoán đúng 6/6"
là tự lừa mình — và cổng ra buổi 2 sẽ chặn bạn ở query lạ tiếp theo.
────────────────────────────────────────────────────────────────────────────

Hai config bị TẮT có chủ đích (chỉ trong lab, KHÔNG bao giờ tắt ở production):
    spark.sql.adaptive.enabled = false
        AQE gộp/tách stage theo số liệu chạy thật -> số trên UI không khớp lý
        thuyết đếm tay. Tắt để học đếm. (Bật lại ở bài A14 để thấy khác biệt.)
    spark.sql.autoBroadcastJoinThreshold = -1
        Mặc định 10MB. items CSV ~15MB nên có thể bị broadcast hoặc không, tùy
        Spark ước lượng size. Broadcast join = KHÔNG shuffle -> query 4 sẽ ra
        số hoàn toàn khác. Tắt broadcast để join là SortMergeJoin đúng như
        lesson 3 mục 3.2 giả định. Đây cũng là một bài học: "số stage" của một
        query KHÔNG phải hằng số — nó phụ thuộc config.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # để import uiprobe

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

import uiprobe

SRC = "/workspace/data/olist"

# Schema tường minh — bắt buộc. Nếu để Spark tự đoán (header=True mà không có
# schema), Spark phải đọc file để lấy tên cột -> ĐẺ RA MỘT JOB ẨN, job đó rơi
# vào jobGroup đang mở và làm sai toàn bộ phép đếm của bài này.
ORDERS = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])
ITEMS = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType()),
])

# ============================================================================
# ↓↓↓ SỬA CHỖ NÀY TRƯỚC KHI CHẠY — dự đoán của BẠN ↓↓↓
# jobs / stages / shuffles.  `why` = lý lẽ; sai lý lẽ mới đáng sợ, sai số thì không.
# ============================================================================
PREDICTIONS = {
    "q1": dict(jobs=1, stages=2, shuffles=1, why=(
        "BẪY SỐ 1 CỦA CẢ BÀI. Ai cũng đoán 1 stage / 0 shuffle vì 'đọc file rồi "
        "đếm, có gì mà shuffle'. Nhưng count() = HashAggregate(partial) trên từng "
        "partition -> Exchange SinglePartition -> HashAggregate(final). Cái "
        "Exchange đó LÀ một shuffle (bé xíu: mỗi partition gửi đúng 1 con số), "
        "nên: 2 stage, 1 shuffle.")),
    "q2": dict(jobs=1, stages=2, shuffles=1, why=(
        "filter là narrow -> bị pipeline chung stage với scan, không cắt gì cả. "
        "Y hệt q1: shuffle duy nhất vẫn là cái Exchange của count().")),
    "q3": dict(jobs=1, stages=2, shuffles=1, why=(
        "groupBy = 1 shuffle -> 2 stage. show() chỉ lấy 20 dòng đầu của kết quả "
        "đã gom, không đẻ thêm job. Lưu ý: stage sau shuffle có 200 task "
        "(spark.sql.shuffle.partitions) cho vỏn vẹn 8 nhóm -> đó là bài A16.")),
    "q4": dict(jobs=1, stages=4, shuffles=3, why=(
        "SortMergeJoin (đã tắt broadcast): scan orders -> shuffle theo order_id "
        "(stage 1), scan items -> shuffle theo order_id (stage 2), hai stage này "
        "CHẠY SONG SONG vì không phụ thuộc nhau. Stage 3 join + partial count -> "
        "shuffle của count. Stage 4 final count. => 3 shuffle, 4 stage.")),
    "q5": dict(jobs=1, stages=3, shuffles=2, why=(
        "distinct() = groupBy(TẤT CẢ cột).agg() -> 1 shuffle (đắt: key là cả "
        "dòng, không phải 1 cột). Rồi count() thêm 1 shuffle nữa. => 2 shuffle, "
        "3 stage.")),
    "q6": dict(jobs=2, stages=3, shuffles=1, why=(
        "MỖI ACTION = MỘT JOB. count() -> job A (2 stage, 1 shuffle như q2). "
        "show() trên cùng df -> job B. show() là CollectLimit: lấy 20 dòng, "
        "narrow thuần -> 1 stage, 0 shuffle. Tổng 2 job / 3 stage / 1 shuffle. "
        "KHÔNG có stage skipped ở đây vì nhánh chung (scan+filter) chưa hề ghi "
        "shuffle file nào để mà tái dùng — muốn thấy skipped phải có shuffle "
        "chung: đó là bài A12. "
        "[KHÔNG CHẮC] show() đôi khi đẻ >1 job nếu partition đầu không đủ 20 "
        "dòng — nó sẽ quét thêm partition theo cấp số nhân. Xem cột thực tế.")),
}


def q1(spark, orders, items):
    """read.csv.count()"""
    return orders.count()


def q2(spark, orders, items):
    """read -> filter -> count()"""
    return orders.filter(F.col("order_status") == "delivered").count()


def q3(spark, orders, items):
    """read -> groupBy(status).count().show()"""
    orders.groupBy("order_status").count().show()
    return None


def q4(spark, orders, items):
    """orders.join(items, "order_id").count()"""
    return orders.join(items, "order_id").count()


def q5(spark, orders, items):
    """read -> distinct().count()"""
    return orders.distinct().count()


def q6(spark, orders, items):
    """2 action trên CÙNG một df: count() rồi show()"""
    df = orders.filter(F.col("order_status") == "delivered")
    n = df.count()          # action 1
    df.show(5)              # action 2
    return n


QUERIES = [
    ("q1", "`read.csv.count()`", q1),
    ("q2", "`read → filter → count()`", q2),
    ("q3", "`read → groupBy(status).count().show()`", q3),
    ("q4", "`orders.join(items,'order_id').count()`", q4),
    ("q5", "`read → distinct().count()`", q5),
    ("q6", "`filter → count()` rồi `→ show()` (2 action)", q6),
]


def main():
    spark = SparkSession.builder.appName("a10-du-doan-dag").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Xem docstring đầu file để biết VÌ SAO tắt hai cái này.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    uiprobe.wait_for_executors(spark, expected=2)

    # ĐỌC FILE TRƯỚC KHI setJobGroup lần đầu.
    # Với schema tường minh, read KHÔNG đẻ job — nhưng đọc trước cho chắc:
    # lỡ có job ẩn nào thì nó rơi ra ngoài mọi nhóm, không làm bẩn phép đếm.
    orders = spark.read.csv(SRC + "/olist_orders_dataset.csv", header=True, schema=ORDERS)
    items = spark.read.csv(SRC + "/olist_order_items_dataset.csv", header=True, schema=ITEMS)

    print("\n" + "=" * 100)
    print("A10 — SỔ DỰ ĐOÁN DAG (AQE=off, broadcast=off, schema tường minh)")
    print("=" * 100)
    print("master=%s | defaultParallelism=%d | shuffle.partitions=%s" % (
        sc.master, sc.defaultParallelism, spark.conf.get("spark.sql.shuffle.partitions")))
    print("orders: %d partition | items: %d partition (= số task của stage scan)" % (
        orders.rdd.getNumPartitions(), items.rdd.getNumPartitions()))

    results = {}
    for key, label, fn in QUERIES:
        # Nhãn nhóm: mọi job do query này sinh ra đều mang nhãn `key`.
        # Đây là cách DUY NHẤT đếm đúng — xem bẫy số 1 trong uiprobe.py.
        sc.setJobGroup(key, "A10 " + label)
        print("\n" + "-" * 100)
        print(">>> %s  %s" % (key.upper(), label))
        print("-" * 100)
        fn(spark, orders, items)
        results[key] = uiprobe.summarize_group(spark, key)
        uiprobe.print_stage_table(results[key])

    # ------------------------------------------------------------------ BẢNG
    print("\n" + "=" * 100)
    print("BẢNG DÁN VÀO PROGRESS.md §3.2")
    print("=" * 100 + "\n")
    print("| # | Query | Job (đoán) | Job (thật) | Stage (đoán) | Stage (thật) | "
          "Shuffle (đoán) | Shuffle (thật) | ✓? |")
    print("|---|---|---|---|---|---|---|---|---|")

    wrong = []
    for key, label, _ in QUERIES:
        p, a = PREDICTIONS[key], results[key]
        # So "stages_run": stage THỰC SỰ chạy. Stage SKIPPED có nằm trong
        # job.stageIds nhưng không chạy -> nếu tính vào thì số sẽ vênh với
        # cách đếm tay ở lesson 3. A12 sẽ mổ xẻ đúng chỗ vênh này.
        ok = (p["jobs"] == a["jobs"] and p["stages"] == a["stages_run"]
              and p["shuffles"] == a["shuffles"])
        if not ok:
            wrong.append(key)
        print("| %s | %s | %d | %d | %d | %d | %d | %d | %s |" % (
            key[1], label, p["jobs"], a["jobs"], p["stages"], a["stages_run"],
            p["shuffles"], a["shuffles"], "✓" if ok else "✗"))

    n_ok = len(QUERIES) - len(wrong)
    print("\n**Tỉ lệ đoán đúng: %d/6.**%s" % (
        n_ok, "  (dưới 4/6 -> chưa qua cổng buổi 2, đọc lại lesson 3 §3.2)"
        if n_ok < 4 else ""))

    # ------------------------------------------------- PHẦN ĐÁNG GIÁ NHẤT
    print("\n" + "=" * 100)
    print('PHẦN "TÔI ĐOÁN SAI Ở ĐÂU" — không có phần này coi như CHƯA LÀM BÀI')
    print("=" * 100)
    if not wrong:
        print("""
Không sai câu nào. Hai khả năng, tự thành thật chọn một:
  (a) Bạn thực sự đếm đúng cả 6 -> qua cổng.
  (b) Bạn không sửa PREDICTIONS mà chạy luôn số có sẵn -> bạn vừa chấm điểm
      cho người viết đề, không phải cho mình. Chạy lại cho tử tế.""")
    for key in wrong:
        p, a = PREDICTIONS[key], results[key]
        print("""
[%s] TÔI ĐOÁN: %d job / %d stage / %d shuffle
     THỰC TẾ : %d job / %d stage chạy (+%d skipped) / %d shuffle
     LÝ LẼ ĐÚNG: %s""" % (key.upper(), p["jobs"], p["stages"], p["shuffles"],
                          a["jobs"], a["stages_run"], a["stages_skipped"],
                          a["shuffles"], p["why"]))

    print("""
--- CÔNG THỨC RÚT RA (điền vào PROGRESS §3.2) ---
    số stage của MỘT job = số Exchange (shuffle) thực sự chạy + 1

Nó SAI (hoặc gây bất ngờ) trong 4 trường hợp — cả 4 đều gặp ở bài này:
  1. Bạn quên count() TỰ NÓ đẻ một Exchange (gom partial count về 1 partition).
     -> q1 tưởng 1 stage, thật ra 2. Đây là lỗi phổ biến nhất.
  2. Nhiều action = nhiều JOB, phải cộng stage của từng job (q6).
  3. Join hai nguồn: hai stage scan chạy SONG SONG rồi mới nhập lại. Công thức
     vẫn đúng (4 = 3+1) nhưng DAG không còn là một đường thẳng.
  4. Stage SKIPPED: nằm trong job nhưng KHÔNG chạy -> "số stage tab Jobs hiện"
     lớn hơn "số stage thật sự chạy". Đó là bài A12.
Và nó sai hẳn khi BẬT AQE (bài A14) hoặc khi Spark chọn BroadcastHashJoin
(broadcast = không shuffle -> q4 chỉ còn 2 stage / 1 shuffle).
""")
    print("=" * 100 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
