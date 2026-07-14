"""A5 — Chứng minh LAZY bằng đồng hồ, và bắt quả tang `inferSchema` là ACTION TRÁ HÌNH.

Chạy (chỉ cần local là đủ — bài này đo lazy, không đo cluster):

    make run-local F=labs/mini-project-1/exercises/a05_lazy_clock.py

Muốn xem con số ở cluster (không bắt buộc):
    make run F=labs/mini-project-1/exercises/a05_lazy_clock.py

Ý TƯỞNG. Đề bảo "lazy" nhưng bảo thì ai chẳng bảo được. Bài này đặt 2 đồng hồ:

  ĐỒNG HỒ 1 (time.time)  — driver mất bao lâu ở mỗi nhóm dòng code.
  ĐỒNG HỒ 2 (số JOB)     — nhóm dòng đó có ĐẺ RA JOB không.

Đồng hồ 2 mới là bằng chứng cứng. Thời gian có thể gần 0 vì máy nhanh, nhưng
"đẻ ra 1 job" thì không cãi được: job = Spark đã THỰC SỰ đọc dữ liệu.
Lấy số job bằng REST API của Spark UI (/api/v1/applications/<id>/jobs), lọc theo
jobGroup — mỗi giai đoạn tôi gắn một nhãn jobGroup riêng nên không thể đếm nhầm.

BẪY MÔI TRƯỜNG (đã dính, ghi lại để người sau khỏi mất thời gian):
  - Listener bus của Spark là BẤT ĐỒNG BỘ. Gọi REST ngay sau action thì job có thể
    chưa kịp lên bảng -> đếm thiếu. Phải chờ (_settle) rồi mới đọc.
  - PySpark 3.4 KHÔNG có sc.clearJobGroup(). Chỉ cần setJobGroup() nhãn mới là xong.
"""

import json
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
GEO_CSV = "/workspace/data/olist/olist_geolocation_dataset.csv"
RUNS = 3  # LUẬT: chạy 3 lần, VỨT lần 1 (JVM warmup + page cache lạnh), lấy min lần 2-3

# Schema tường minh — đây chính là thứ thay thế inferSchema.
# Gõ tay 8 dòng này một lần, đổi lại: read trở thành LAZY thật (0 job).
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

# geolocation: để ý zip_code_prefix là STRING dù trông như số — vì nó có số 0 đứng đầu
# ("01037"). inferSchema sẽ đoán là Integer và ĂN MẤT số 0. Phần B sẽ chứng minh.
GEO_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType()),
    StructField("geolocation_lat", DoubleType()),
    StructField("geolocation_lng", DoubleType()),
    StructField("geolocation_city", StringType()),
    StructField("geolocation_state", StringType()),
])


# ---------------------------------------------------------------------------
# Đồng hồ 2: đếm job qua REST API của Spark UI
# ---------------------------------------------------------------------------
def _settle():
    """Chờ listener bus của Spark ghi xong job vào bảng trước khi đi hỏi REST.

    Không có dòng này thì đếm job ra số nhỏ hơn thật -> kết luận sai.
    """
    time.sleep(1.2)


def jobs_in_group(sc, group):
    """Danh sách job thuộc jobGroup `group`.

    Vì sao phải lọc theo jobGroup mà không lấy jobs[-n:]? Vì REST trả job MỚI NHẤT
    TRƯỚC, và có những job mọc ra ngoài ý muốn (đọc header CSV chẳng hạn). Gắn nhãn
    là cách duy nhất chắc chắn.
    """
    url = "{}/api/v1/applications/{}/jobs".format(sc.uiWebUrl, sc.applicationId)
    with urllib.request.urlopen(url, timeout=10) as r:
        jobs = json.loads(r.read())
    return [j for j in jobs if j.get("jobGroup") == group]


def count_jobs(sc, group):
    _settle()
    return len(jobs_in_group(sc, group))


# ---------------------------------------------------------------------------
def part_a(spark, sc):
    """PHẦN A — 3 nhóm dòng code, 3 con số. Cái nào ~0? Cái nào tốn thật?"""
    print("\n" + "=" * 78)
    print("PHẦN A — read (schema tường minh) / 10 transformation / count()")
    print("=" * 78)

    # --- nhóm 1: READ (có schema) ---
    sc.setJobGroup("A-read", "A5 phan A: spark.read voi schema tuong minh")
    t0 = time.time()
    df = (spark.read
          .schema(ORDERS_SCHEMA)          # đưa sẵn schema -> Spark KHÔNG cần quét file
          .option("header", True)
          .csv(ORDERS_CSV))
    t_read = time.time() - t0
    j_read = count_jobs(sc, "A-read")

    # --- nhóm 2: 10 TRANSFORMATION (tất cả đều lười) ---
    sc.setJobGroup("A-transform", "A5 phan A: 10 transformation")
    t0 = time.time()
    df2 = (df
           .filter(F.col("order_status") == "delivered")                       # 1  narrow
           .withColumn("d", F.to_date("order_purchase_timestamp"))             # 2  narrow
           .withColumn("y", F.year("d"))                                       # 3  narrow
           .withColumn("m", F.month("d"))                                      # 4  narrow
           .withColumn("ym", F.date_format("d", "yyyy-MM"))                    # 5  narrow
           .withColumn("wait_days", F.datediff("order_delivered_customer_date",
                                               "order_purchase_timestamp"))    # 6  narrow
           .withColumn("late", F.col("order_delivered_customer_date")
                       > F.col("order_estimated_delivery_date"))               # 7  narrow
           .filter(F.col("wait_days").isNotNull())                             # 8  narrow
           .select("order_id", "ym", "wait_days", "late")                      # 9  narrow
           .orderBy("ym"))                                                     # 10 WIDE (shuffle)
    t_transform = time.time() - t0
    j_transform = count_jobs(sc, "A-transform")
    # Chú ý: dòng số 10 là orderBy — một WIDE transformation, tức là có SHUFFLE.
    # Kể cả thứ đắt đỏ như shuffle cũng KHÔNG chạy ở đây. Lười là lười triệt để.

    # --- nhóm 3: ACTION ---
    sc.setJobGroup("A-action", "A5 phan A: count()")
    t0 = time.time()
    n = df2.count()
    t_action = time.time() - t0
    j_action = count_jobs(sc, "A-action")

    print("""
| giai đoạn | thời gian | số job sinh ra | kết luận |
|---|---|---|---|
| `spark.read` (có schema) | {:.4f}s | {} | {} |
| 10 transformation (có cả orderBy = shuffle) | {:.4f}s | {} | {} |
| `count()` (ACTION) | {:.4f}s | {} | job chạy thật, {:,} dòng |
""".format(
        t_read, j_read, "LƯỜI" if j_read == 0 else "CÓ job -> KHÔNG lười!",
        t_transform, j_transform, "LƯỜI" if j_transform == 0 else "CÓ job -> KHÔNG lười!",
        t_action, j_action, n,
    ))
    print("VÌ SAO t(transform) ~ 0 ? Vì 10 dòng đó chỉ dựng thêm 10 tầng vào cây plan")
    print("nằm trong RAM của driver. 0 byte dữ liệu bị đụng tới, 0 task được phát đi.")
    print("Cả cụm 6 core đứng nhìn. Chỉ khi count() gọi, domino mới đổ.")


def read_variant(spark, sc, tag, path, schema, header, infer):
    """Đọc 1 lần theo 1 cách, trả (thời gian, số job, df).

    Đây là hàm ĐO. Nó chỉ gọi spark.read — KHÔNG có action nào.
    Nếu vẫn thấy job sinh ra thì thủ phạm nằm trong chính spark.read.
    """
    sc.setJobGroup(tag, tag)
    t0 = time.time()
    r = spark.read.option("header", header)
    if schema is not None:
        r = r.schema(schema)
    if infer:
        r = r.option("inferSchema", True)
    df = r.csv(path)
    dt = time.time() - t0
    return dt, count_jobs(sc, tag), df


def part_b(spark, sc):
    """PHẦN B — cùng một file, 3 cách đọc. Cách nào là action trá hình?"""
    print("\n" + "=" * 78)
    print("PHẦN B — 3 cách đọc × 2 file. Đâu là ACTION TRÁ HÌNH?")
    print("=" * 78)

    cases = [
        # (nhãn file, đường dẫn, schema, có bật inferSchema?, mô tả cách đọc)
        ("orders 17MB", ORDERS_CSV, ORDERS_SCHEMA, False, "schema tường minh"),
        ("orders 17MB", ORDERS_CSV, None, False, "không schema, không infer (all String)"),
        ("orders 17MB", ORDERS_CSV, None, True, "inferSchema=True"),
        ("geo 58MB", GEO_CSV, GEO_SCHEMA, False, "schema tường minh"),
        ("geo 58MB", GEO_CSV, None, False, "không schema, không infer (all String)"),
        ("geo 58MB", GEO_CSV, None, True, "inferSchema=True"),
    ]

    rows = []
    for i, (fname, path, schema, infer, how) in enumerate(cases):
        times, jobs = [], []
        for k in range(RUNS):
            dt, nj, df = read_variant(
                spark, sc, "B-{}-{}".format(i, k), path, schema, True, infer)
            times.append(dt)
            jobs.append(nj)
        warm = min(times[1:])          # vứt lần 1
        parts = df.rdd.getNumPartitions()   # (lưu ý: dòng này KHÔNG đọc dữ liệu, chỉ hỏi file index)
        rows.append((fname, how, times[0], warm, jobs, parts, df))

    print("\n| file | cách đọc | t_read lần 1 (lạnh) | t_read ẤM (min lần 2-3) | job sinh ra mỗi lần đọc | numPartitions | lười? |")
    print("|---|---|---|---|---|---|---|")
    for fname, how, cold, warm, jobs, parts, _df in rows:
        lazy = "✅ LƯỜI" if max(jobs) == 0 else "❌ KHÔNG LƯỜI ({} job)".format(jobs[-1])
        print("| {} | {} | {:.4f}s | {:.4f}s | {} | {} | {} |".format(
            fname, how, cold, warm, jobs, parts, lazy))

    # --- Đòn kết liễu: inferSchema không chỉ CHẬM, nó còn SAI ---
    print("\n" + "-" * 78)
    print("BONUS — inferSchema không chỉ chậm, nó còn ĐOÁN SAI (bằng chứng cho A21):")
    print("-" * 78)
    _, _, df_infer = read_variant(spark, sc, "B-zip-infer", GEO_CSV, None, True, True)
    _, _, df_hand = read_variant(spark, sc, "B-zip-hand", GEO_CSV, GEO_SCHEMA, True, False)
    zip_infer_type = dict(df_infer.dtypes)["geolocation_zip_code_prefix"]
    zip_hand_type = dict(df_hand.dtypes)["geolocation_zip_code_prefix"]
    sc.setJobGroup("B-zip-show", "lay 1 dong xem zip bi bien dang chua")
    v_infer = df_infer.select("geolocation_zip_code_prefix").first()[0]
    v_hand = df_hand.select("geolocation_zip_code_prefix").first()[0]
    print("\n| cột | inferSchema đoán | giá trị dòng đầu | schema tay | giá trị dòng đầu |")
    print("|---|---|---|---|---|")
    print("| geolocation_zip_code_prefix | {} | `{}` | {} | `{}` |".format(
        zip_infer_type, v_infer, zip_hand_type, v_hand))
    print("\nFile gốc ghi \"01037\". Nếu cột trên hiện 1037 thì inferSchema vừa ăn mất số 0")
    print("đứng đầu — và không ai báo lỗi. Đây là DỮ LIỆU SAI IM LẶNG.")


def main():
    spark = SparkSession.builder.appName("a05-lazy-clock").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("\nmaster = {} | defaultParallelism = {}".format(sc.master, sc.defaultParallelism))

    part_a(spark, sc)
    part_b(spark, sc)

    print("\n" + "=" * 78)
    print("CÂU TRẢ LỜI CHO CÂU HỎI CỦA ĐỀ: vì sao read với inferSchema tốn thời gian")
    print("THẬT dù chưa có action nào?")
    print("=" * 78)
    print("""
Vì để ĐOÁN được kiểu dữ liệu, Spark buộc phải NHÌN dữ liệu. Muốn nhìn dữ liệu thì
phải đọc file. Muốn đọc file phân tán thì phải phát task. Phát task nghĩa là...
CHẠY MỘT JOB. Nó chỉ không mang tên "action" thôi, chứ nó là action đúng nghĩa:
có job, có stage, có task, có I/O thật, hiện lên Spark UI đàng hoàng.

Nói cách khác: `inferSchema=True` phá vỡ lời hứa "read là transformation".
Với `samplingRatio` mặc định = 1.0, nó quét TOÀN BỘ file — file càng to càng đắt
(so cột "geo 58MB" với "orders 17MB" ở bảng phần B để thấy nó tỉ lệ với kích thước).

Ba lý do bỏ inferSchema trong code production, xếp theo mức độ nguy hiểm TĂNG DẦN:
  1. Chậm  — quét thừa toàn bộ dữ liệu một lượt trước khi làm việc thật.
  2. Bất định — schema phụ thuộc DỮ LIỆU HÔM NAY. Mai file thiếu vài dòng là kiểu
     cột đổi, pipeline gãy ở chỗ chẳng liên quan gì.
  3. SAI IM LẶNG — "01037" thành 1037 (xem BONUS). Không exception, không cảnh báo,
     chỉ có một cột zip code hỏng chảy thẳng vào bảng silver.
Đó là lý do rubric trừ 10 điểm nếu thấy inferSchema trong code nộp.
""")
    spark.stop()


if __name__ == "__main__":
    main()
