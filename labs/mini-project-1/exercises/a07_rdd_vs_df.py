"""A7 — RDD vs DataFrame vs SQL: cùng một bài toán, ba thế giới.

Bài toán: đếm số đơn theo `order_status`.

Chạy — NÊN chạy CẢ HAI để thấy điều thú vị nhất của bài này:

    make run-local F=labs/mini-project-1/exercises/a07_rdd_vs_df.py   # số ổn định
    make run       F=labs/mini-project-1/exercises/a07_rdd_vs_df.py   # RDD còn tệ hơn nữa

(Ở cluster, mỗi executor phải fork thêm một tiến trình `python3` để chạy lambda ->
phí "hải quan" JVM<->Python bị nhân lên. Local[2] chỉ có 1 JVM nên đỡ hơn.)

CÁCH ĐO CHO CÔNG BẰNG — đọc kỹ, đây là chỗ 8/10 người làm sai:
  * Cả 3 cách đều phải kết thúc bằng ACTION thật (`collect()`), không thì đo lazy = 0.001s.
  * Nguồn được CACHE và MỒI SẴN trước khi đo. Vì sao? Nếu không, cả 3 cách đều phải
    đọc lại 17MB CSV -> thời gian đọc file (một hằng số chung) cộng vào cả 3 và PHA
    LOÃNG tỉ lệ. Ta muốn đo ĐÚNG phần khác nhau: chi phí của API, không phải của I/O.
  * 3 lần chạy, vứt lần 1 (JVM warmup + codegen chưa nóng), lấy min lần 2-3.
"""

import time
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
RUNS = 3

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


def bench(name, fn, runs=RUNS):
    """Đo `fn` runs lần. fn PHẢI kết thúc bằng action và TRẢ VỀ KẾT QUẢ THẬT.

    Trả (lạnh, ấm, kết quả). Ấm = min các lần sau lần 1.
    """
    times, result = [], None
    for _ in range(runs):
        t0 = time.time()
        result = fn()
        times.append(time.time() - t0)
    return times[0], min(times[1:]), result, times


def norm(pairs):
    """Chuẩn hoá kết quả về dict để so 3 cách có ra CÙNG một đáp án không.

    Không kiểm tra bước này thì bạn có thể đang benchmark 3 query khác nhau mà không
    biết — và mọi con số trở thành rác.
    """
    return {k: int(v) for k, v in pairs}


def main():
    spark = SparkSession.builder.appName("a07-rdd-vs-df").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    df = (spark.read
          .schema(ORDERS_SCHEMA)
          .option("header", True)
          .csv(ORDERS_CSV))

    # --- Mồi cache: loại chi phí đọc CSV ra khỏi phép đo (xem docstring) ---
    t0 = time.time()
    df.cache()
    n_rows = df.count()          # action mồi -> từ giờ df nằm trong memory
    t_prime = time.time() - t0
    df.createOrReplaceTempView("orders")

    print("\nmaster={} | defaultParallelism={} | dòng={:,} | mồi cache={:.2f}s".format(
        sc.master, sc.defaultParallelism, n_rows, t_prime))

    # -----------------------------------------------------------------------
    # (a) DataFrame API — Catalyst NHÌN THẤY ý định của bạn
    # -----------------------------------------------------------------------
    def way_a():
        rows = df.groupBy("order_status").count().collect()
        return [(r["order_status"], r["count"]) for r in rows]

    # -----------------------------------------------------------------------
    # (b) RDD — Catalyst chỉ thấy một CỤC HÀM PYTHON, không tối ưu được gì
    # -----------------------------------------------------------------------
    def way_b():
        return (df.rdd                                   # <-- rời khỏi thế giới Tungsten
                .map(lambda r: (r.order_status, 1))      # <-- lambda: hộp đen với Catalyst
                .reduceByKey(add)
                .collect())

    # -----------------------------------------------------------------------
    # (c) Spark SQL — chữ khác, ruột giống hệt (a)?  Sẽ kiểm chứng bằng plan.
    # -----------------------------------------------------------------------
    def way_c():
        rows = spark.sql(
            "SELECT order_status, COUNT(*) AS n FROM orders GROUP BY order_status"
        ).collect()
        return [(r["order_status"], r["n"]) for r in rows]

    sc.setJobGroup("a07-df", "A7 (a) DataFrame groupBy")
    a_cold, a_warm, a_res, a_all = bench("DataFrame", way_a)
    sc.setJobGroup("a07-rdd", "A7 (b) RDD map+reduceByKey")
    b_cold, b_warm, b_res, b_all = bench("RDD", way_b)
    sc.setJobGroup("a07-sql", "A7 (c) Spark SQL")
    c_cold, c_warm, c_res, c_all = bench("SQL", way_c)

    same = norm(a_res) == norm(b_res) == norm(c_res)

    print("\n" + "=" * 78)
    print("BẢNG SỐ ĐO — đếm đơn theo order_status ({:,} dòng, nguồn đã cache)".format(n_rows))
    print("=" * 78)
    print("\n| cách viết | lần 1 (lạnh) | lần 2 | lần 3 | ẤM (min 2-3) | so với DataFrame |")
    print("|---|---|---|---|---|---|")
    for label, cold, all_t, warm in [
        ("(a) `df.groupBy().count()`", a_cold, a_all, a_warm),
        ("(b) `df.rdd.map().reduceByKey()`", b_cold, b_all, b_warm),
        ("(c) `spark.sql(...)`", c_cold, c_all, c_warm),
    ]:
        print("| {} | {:.3f}s | {:.3f}s | {:.3f}s | **{:.3f}s** | {:.1f}× |".format(
            label, cold, all_t[1], all_t[2], warm, warm / a_warm))

    print("\n3 cách có ra CÙNG một kết quả không? **{}**".format("CÓ" if same else "KHÔNG — số đo vô nghĩa, sửa code!"))
    print("Kết quả (sắp xếp): {}".format(sorted(norm(a_res).items(), key=lambda kv: -kv[1])))

    # -----------------------------------------------------------------------
    # (a) và (c) có giống hệt nhau không? So PLAN — không so cảm tính.
    # -----------------------------------------------------------------------
    plan_a = df.groupBy("order_status").count()._jdf.queryExecution().optimizedPlan().toString()
    plan_c = spark.sql(
        "SELECT order_status, COUNT(*) AS n FROM orders GROUP BY order_status"
    )._jdf.queryExecution().optimizedPlan().toString()

    # Bỏ tên cột output ("count" vs "n") và các id nội bộ (#123L) trước khi so —
    # chúng khác nhau về HÌNH THỨC chứ không phải về CÁCH CHẠY.
    import re

    def canon(p):
        p = re.sub(r"#\d+L?", "#x", p)          # bỏ expression id
        p = re.sub(r"\bcount\b|\bn\b", "AGG", p)  # đồng nhất tên cột kết quả
        return p.strip()

    print("\n" + "=" * 78)
    print("(a) DataFrame API vs (c) SQL — CÙNG MỘT PLAN?")
    print("=" * 78)
    print("\n--- Optimized Logical Plan của (a) DataFrame ---")
    print(plan_a)
    print("\n--- Optimized Logical Plan của (c) SQL ---")
    print(plan_c)
    print("\n=> Sau khi chuẩn hoá tên cột + id: **{}**".format(
        "GIỐNG HỆT" if canon(plan_a) == canon(plan_c) else
        "KHÁC (đọc 2 plan trên để xem khác ở đâu — ghi trung thực vào report)"))

    print("""
VÌ SAO (a) và (c) giống nhau? Vì cả hai đều đi qua ĐÚNG MỘT cửa: Catalyst.
DataFrame API và SQL chỉ là hai cách GÕ ra cùng một cây logical plan. Từ chỗ đó trở
đi chúng là một. Nên "SQL nhanh hơn DataFrame" (hay ngược lại) là chuyện hoang đường —
chọn cái nào là chuyện dễ đọc, không phải chuyện tốc độ.
""")

    # -----------------------------------------------------------------------
    print("=" * 78)
    print("VÌ SAO (b) RDD CHẬM HƠN — 5 câu, đủ từ khoá đề yêu cầu")
    print("=" * 78)
    print("""
1. SERIALIZE SANG PYTHON WORKER. `df.rdd.map(lambda...)` bắt TỪNG DÒNG trong 99.441
   dòng phải: đóng gói từ UnsafeRow (nhị phân, trong JVM) -> pickle -> đẩy qua socket
   sang tiến trình `python3` -> chạy lambda -> pickle ngược -> trả về JVM. Trả phí
   "hải quan" hai chiều trên MỖI DÒNG. DataFrame không hề rời JVM một bước nào.

2. CATALYST KHÔNG NHÌN THẤY LAMBDA. Với `F.col("order_status")` Spark đọc hiểu được
   "à, nó cần đúng 1 cột" -> chỉ đọc 1 cột (column pruning). Với `lambda r: (r.order_status, 1)`
   Spark chỉ thấy một hộp đen -> buộc phải dựng ĐỦ CẢ 8 CỘT của mỗi Row rồi mới đưa
   sang Python. Đọc thừa 7 cột × 99.441 dòng, chỉ để vứt đi.

3. MẤT WHOLE-STAGE CODEGEN. Với DataFrame, Tungsten SINH RA một class Java riêng cho
   query này, gộp scan+filter+aggregate thành một vòng lặp chặt chạy sát phần cứng
   (dấu `*` trước node trong explain là nó đấy). RDD thì chạy qua các lớp trừu tượng
   iterator lồng nhau — mỗi dòng là vài lời gọi hàm ảo.

4. MẤT TUNGSTEN MEMORY FORMAT. RDD giữ dữ liệu dạng object JVM/Python -> tốn RAM gấp
   nhiều lần, GC phải làm việc. DataFrame giữ dạng nhị phân off-heap gọn hơn.

5. MẤT LUÔN TAB SQL. Mở Spark UI: job của (b) KHÔNG xuất hiện ở tab SQL/DataFrame —
   bằng chứng trực quan rằng RDD sống NGOÀI vòng pháp luật của Catalyst. Không có plan
   để mà tối ưu, không có metric để mà nhìn.

=> LUẬT NGHỀ: trong PySpark, DataFrame là MẶC ĐỊNH, RDD là NGOẠI LỆ PHẢI GIẢI TRÌNH.
   Thấy `df.rdd.map(...)` trong code review, câu hỏi đầu tiên luôn là: "hàm built-in
   nào trong pyspark.sql.functions làm được việc này?" — 9/10 lần là có.
   (Ngoại lệ hợp lệ duy nhất ở project này: `df.rdd.glom().map(len)` để SOI partition —
   bài A19. Nó chỉ trả về vài con số, không phải dữ liệu.)
""")

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
