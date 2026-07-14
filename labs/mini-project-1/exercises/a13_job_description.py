"""A13 — Đặt tên cho job: biến Spark UI thành MỤC LỤC thay vì ma trận.

Chạy:
    make run F=labs/mini-project-1/exercises/a13_job_description.py        # cluster
    make run-local F=labs/mini-project-1/exercises/a13_job_description.py  # local[2] cũng được

Output: bảng tab Jobs dạng markdown — cột "Description" đọc được bằng TIẾNG NGƯỜI.
Ghi tạm ra: /workspace/data/output/tmp/a13/
Dọn:  docker exec spark-mastery-spark-submit-1 rm -rf /workspace/data/output/tmp/a13

────────────────────────────────────────────────────────────────────────
VÌ SAO BÀI NÀY ĐÁNG 10 PHÚT CỦA BẠN
Mặc định, tab Jobs của Spark hiện tên job là CALL SITE — dòng code Python nào
gọi action. Trong PySpark nó ra như thế này:

    count at NativeMethodAccessorImpl.java:0
    parquet at NativeMethodAccessorImpl.java:0
    count at NativeMethodAccessorImpl.java:0

Ba job, cùng một cái tên vô nghĩa (vì mọi lệnh PySpark đều đi qua đúng cái cầu
Java đó). 3h sáng, pipeline fail, bạn mở UI và thấy 7 job giống hệt nhau. Chúc
may mắn.

setJobDescription() sửa đúng chỗ đó. Nó KHÔNG làm job chạy nhanh hơn một mili
giây nào — nó làm CON NGƯỜI đọc được. Đây là thứ phân biệt code chạy được với
code vận hành được.
────────────────────────────────────────────────────────────────────────

Script này là bản thu nhỏ của ingest.py thật (đọc -> quarantine -> derive ->
ghi partition), gắn nhãn từng bước. Nó CỐ TÌNH dính bẫy ở BƯỚC 5 để bạn thấy
mặt trái của công cụ.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StringType, StructField, StructType, TimestampType)

import uiprobe

SRC = "/workspace/data/olist/olist_orders_dataset.csv"
OUT = "/workspace/data/output/tmp/a13"

# _corrupt_record BẮT BUỘC là StringType và phải có trong schema thì PERMISSIVE
# mới có chỗ nhét dòng hỏng vào.
ORDERS = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
    StructField("_corrupt_record", StringType()),
])


def main():
    spark = SparkSession.builder.appName("a13-job-description").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    uiprobe.wait_for_executors(spark, expected=2)

    print("\n" + "=" * 100)
    print("A13 — ĐẶT TÊN CHO JOB")
    print("=" * 100)
    print("Spark UI: %s  -> tab Jobs (chụp ảnh cột Description)" % sc.uiWebUrl)

    # ---- BƯỚC 0: job KHÔNG có mô tả — để làm đối chứng ------------------
    # (job "mồi executor" của uiprobe cũng nằm ở đây, cũng không tên.)
    spark.range(1000).count()

    # ---- BƯỚC 1: đọc + đếm dòng hỏng ------------------------------------
    sc.setJobDescription("CP1 · đọc orders (PERMISSIVE) + đếm dòng hỏng")
    df = (spark.read
          .option("header", True)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .schema(ORDERS)
          .csv(SRC)
          .cache())          # cache TRƯỚC khi filter _corrupt_record hai lần —
                             # bẫy kinh điển của lesson 5, mổ xẻ ở bài A24.
    total = df.count()       # action -> job này mang nhãn "CP1 · đọc orders..."
    bad = df.filter(F.col("_corrupt_record").isNotNull())
    n_bad = bad.count()

    # ---- BƯỚC 2: ghi quarantine -----------------------------------------
    sc.setJobDescription("CP1 · ghi quarantine (%d dòng hỏng)" % n_bad)
    (bad.withColumn("source_file", F.input_file_name())
        .withColumn("ingest_ts", F.current_timestamp())
        .write.mode("overwrite").parquet(OUT + "/quarantine"))

    # ---- BƯỚC 3: derive order_date + repartition ------------------------
    sc.setJobDescription("CP2 · derive order_date + repartition(order_date)")
    clean = (df.filter(F.col("_corrupt_record").isNull())
               .drop("_corrupt_record")
               .withColumn("order_date", F.to_date("order_purchase_timestamp")))
    n_null_date = clean.filter(F.col("order_date").isNull()).count()
    clean = clean.filter(F.col("order_date").isNotNull()).repartition("order_date")

    # ---- BƯỚC 4: ghi bảng chính -----------------------------------------
    sc.setJobDescription("CP2 · ghi orders_clean partitionBy(order_date)")
    (clean.write.mode("overwrite")
          .partitionBy("order_date")
          .parquet(OUT + "/orders_clean"))

    # ---- BƯỚC 5: BẪY — quên đổi mô tả ------------------------------------
    # KHÔNG gọi setJobDescription. Job dưới đây là "đọc lại để verify", chẳng
    # liên quan gì đến việc ghi — nhưng nó sẽ ĐỘI NGUYÊN cái tên của bước 4.
    # Mô tả SAI còn tệ hơn không có mô tả: nó khiến bạn đi điều tra nhầm job.
    n_written = spark.read.parquet(OUT + "/orders_clean").count()

    # ---- BƯỚC 6: cách sửa — trả mô tả về mặc định ------------------------
    # PySpark 3.4: setJobDescription(None) -> job quay lại tên call-site.
    # (Còn setJobGroup thì KHÔNG có clearJobGroup() trong PySpark 3.4 — đừng gọi,
    #  nó không tồn tại; muốn "đóng" nhóm thì mở nhóm khác.)
    sc.setJobDescription(None)
    _ = spark.read.parquet(OUT + "/orders_clean").select("order_id").count()

    # ------------------------------------------------------------- BẢNG
    jobs = sorted(uiprobe.rest(sc.uiWebUrl, "/applications/%s/jobs" % sc.applicationId),
                  key=lambda j: j["jobId"])

    print("\n" + "=" * 100)
    print("TAB JOBS — chính là thứ Spark UI đang vẽ (dán vào report)")
    print("=" * 100 + "\n")
    print("| jobId | Description (setJobDescription) | tên mặc định (call site) | stage | task |")
    print("|---|---|---|---|---|")
    for j in jobs:
        desc = j.get("description") or "*(trống — không đặt tên)*"
        print("| %d | %s | `%s` | %d | %d |" % (
            j["jobId"], desc, j.get("name", "?"),
            len(j.get("stageIds", [])), j.get("numTasks", 0)))

    print("""
--- ĐỌC BẢNG TRÊN ---
Cột "tên mặc định" là thứ bạn ĐƯỢC TẶNG nếu không làm gì:
`count at NativeMethodAccessorImpl.java:0` — mọi job PySpark đều tên na ná vậy,
vì mọi lệnh đều đi qua cùng một cầu Py4J. Vô dụng khi debug.

Cột "Description" là thứ bạn TỰ VIẾT. Giờ tab Jobs đọc như mục lục pipeline.

--- BẪY (BƯỚC 5, nhìn kỹ bảng) ---
Job "đọc lại để verify" KHÔNG hề được đặt tên, nhưng nó vẫn đội cái nhãn
"CP2 · ghi orders_clean partitionBy(order_date)" của bước 4. Vì sao? Vì job
description là TRẠNG THÁI DÍNH của thread driver — set một lần, mọi job sau đó
mang nó cho đến khi bị ghi đè.
=> Mô tả SAI nguy hiểm hơn không có mô tả: 3h sáng bạn sẽ đi tối ưu nhầm job.
=> Kỷ luật: set lại TRƯỚC MỖI action, hoặc setJobDescription(None) khi xong
   (bước 6 — nhìn bảng, job cuối đã trở về tên call-site mặc định).

--- SỐ LIỆU PIPELINE (tiện thể, đối chiếu với Checkpoint 1) ---
dòng đọc được       : {total:,}
dòng hỏng (_corrupt): {n_bad:,}
dòng order_date NULL: {n_null_date:,}
dòng ghi vào bảng   : {n_written:,}
kiểm tra: {total:,} - {n_bad:,} - {n_null_date:,} = {expect:,}  ->  {verdict}
""".format(total=total, n_bad=n_bad, n_null_date=n_null_date, n_written=n_written,
           expect=total - n_bad - n_null_date,
           verdict="KHỚP" if total - n_bad - n_null_date == n_written
                   else "LỆCH — đi tìm lý do, ĐỪNG bỏ qua"))
    print("=" * 100 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
