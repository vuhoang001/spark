"""A11 — Ranh giới stage nằm ở ĐÂU: narrow không cắt, wide thì cắt.

Chạy:
    make run F=labs/mini-project-1/exercises/a11_ranh_gioi_stage.py        # cluster
    make run-local F=labs/mini-project-1/exercises/a11_ranh_gioi_stage.py  # local[2]: cùng số stage, khác số task

Output: 3 DAG dạng chữ (dán vào .md được) + bảng stage/shuffle + công thức tự rút.
Ghi tạm ra: /workspace/data/output/tmp/a11/  (xoá bằng lệnh ở cuối docstring)

Ý tưởng thí nghiệm — 3 pipeline, cùng dữ liệu, tăng dần độ "wide":
    V1  10 withColumn + filter + select                 (narrow thuần)
    V2  V1 + repartition(4) chèn vào GIỮA               (+1 shuffle)
    V3  V2 + groupBy().agg()                            (+1 shuffle nữa)

Mỗi pipeline chạy với HAI action khác nhau — và đây mới là chỗ đắt giá:
    write.parquet()  -> action KHÔNG tự thêm shuffle  => đo được đúng công thức
    count()          -> action TỰ THÊM một Exchange   => công thức "hụt" 1
Ai chỉ đo bằng count() sẽ rút ra công thức sai và không hiểu vì sao.

⚠️ ĐỪNG ghi output ra /tmp: ở cluster mode, task WRITE chạy trên EXECUTOR
(container worker khác), /tmp của nó không phải /tmp của bạn -> file rơi vãi
mỗi nơi một mảnh, driver commit hụt. Chỉ /workspace là volume DÙNG CHUNG cho
cả 3 container (xem docker-compose.spark.yaml).

File Spark ghi ra thuộc quyền root trên host. Dọn bằng:
    docker exec spark-mastery-spark-submit-1 rm -rf /workspace/data/output/tmp/a11
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

import uiprobe

SRC = "/workspace/data/olist/olist_order_items_dataset.csv"
OUT = "/workspace/data/output/tmp/a11"

ITEMS = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType()),
])


def chain_narrow(df):
    """10 withColumn + filter + select — TẤT CẢ đều narrow.

    Narrow = mỗi partition đầu vào chỉ nuôi đúng một partition đầu ra, dữ liệu
    KHÔNG cần bay qua network. Spark "pipeline" cả chuỗi này: mỗi dòng chảy
    xuyên qua 12 phép biến đổi trong MỘT lần chạm, không hề có "bảng trung gian
    sau withColumn thứ 3" nào tồn tại trong bộ nhớ.
    => 12 phép này nằm gọn trong 1 stage. Thêm 100 cái nữa vẫn 1 stage.
    """
    for i in range(10):
        df = df.withColumn("c%d" % i, F.col("price") * (i + 1) + F.col("freight_value"))
    return df.filter(F.col("price") > 10).select(
        "order_id", "seller_id", "price", "c0", "c9")


def v1(df):
    return chain_narrow(df)


def v2(df):
    """repartition(4) = WIDE. Nó băm lại dữ liệu theo hash -> dữ liệu phải bay
    qua network -> DAG Scheduler CHÉM ở đây."""
    for i in range(5):
        df = df.withColumn("d%d" % i, F.col("price") + i)
    df = df.repartition(4)          # <-- vết chém số 1
    return chain_narrow(df)


def v3(df):
    """Thêm groupBy -> vết chém số 2."""
    return v2(df).groupBy("seller_id").agg(
        F.sum("price").alias("revenue"), F.count("*").alias("n"))


VARIANTS = [
    ("v1", "10×withColumn + filter + select (narrow thuần)", v1),
    ("v2", "v1 + repartition(4) ở giữa", v2),
    ("v3", "v2 + groupBy(seller_id).agg()", v3),
]


def main():
    spark = SparkSession.builder.appName("a11-ranh-gioi-stage").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    # Tắt AQE: AQE gộp partition sau shuffle và có thể đổi cả hình dạng DAG
    # -> đang học đếm stage thì phải tắt cho sạch sân (lesson 3, §3.7).
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    uiprobe.wait_for_executors(spark, expected=2)
    items = spark.read.csv(SRC, header=True, schema=ITEMS)

    print("\n" + "=" * 100)
    print("A11 — RANH GIỚI STAGE (AQE=off)")
    print("=" * 100)
    print("master=%s | defaultParallelism=%d | items đọc vào %d partition" % (
        sc.master, sc.defaultParallelism, items.rdd.getNumPartitions()))
    print("Spark UI: %s   (mở tab Stages -> DAG Visualization để CHỤP ẢNH 3 DAG)" % sc.uiWebUrl)

    rows = []
    for key, label, build in VARIANTS:
        for action in ("write", "count"):
            group = "%s-%s" % (key, action)
            sc.setJobGroup(group, "A11 %s [%s]" % (label, action))

            df = build(items)   # dựng LẠI df mỗi lần: df mới = ShuffleDependency
                                # mới -> không bị tái dùng shuffle file của lần
                                # trước (nếu tái dùng sẽ thấy SKIPPED và phép
                                # đếm stage của bài này hỏng hết).
            if action == "write":
                # write.parquet: action "thật thà" — nó KHÔNG thêm shuffle nào,
                # chỉ có bao nhiêu partition thì ghi bấy nhiêu file.
                df.write.mode("overwrite").parquet("%s/%s" % (OUT, key))
            else:
                # count(): action "gian" — tự cài thêm một Exchange SinglePartition
                # để gom partial count. Chính nó làm mọi người đếm hụt 1 shuffle.
                df.count()

            s = uiprobe.summarize_group(spark, group)
            s["label"], s["action"], s["key"] = label, action, key
            rows.append(s)

            print("\n" + "-" * 100)
            print(">>> %s  |  action = %s()" % (label, action))
            print("-" * 100)
            uiprobe.print_ascii_dag(s)
            uiprobe.print_stage_table(s)

    # --------------------------------------------------------------- BẢNG
    print("\n" + "=" * 100)
    print("BẢNG TỔNG — dán vào report (A11)")
    print("=" * 100 + "\n")
    print("| pipeline | action | job | stage | shuffle | stage == shuffle+1 ? | số task từng stage |")
    print("|---|---|---|---|---|---|---|")
    for s in rows:
        tasks = " → ".join(str(r["numTasks"]) for r in s["stage_rows"])
        print("| %s | `%s()` | %d | %d | %d | %s | %s |" % (
            s["label"], s["action"], s["jobs"], s["stages_run"], s["shuffles"],
            "ĐÚNG" if s["stages_run"] == s["shuffles"] + 1 else "SAI",
            tasks))

    # -------------------------------------------------------------- KẾT LUẬN
    w = {s["key"]: s for s in rows if s["action"] == "write"}
    c = {s["key"]: s for s in rows if s["action"] == "count"}
    print("""
--- CÔNG THỨC TỰ RÚT RA ---
    số stage = số shuffle + 1

Bằng chứng (cột `write` — action không tự thêm shuffle):
    v1  0 shuffle -> %d stage      (10 withColumn + filter + select vẫn 1 stage!)
    v2  1 shuffle -> %d stage      (chèn repartition(4) = +1 shuffle = +1 stage)
    v3  2 shuffle -> %d stage      (thêm groupBy       = +1 shuffle = +1 stage)

--- CÔNG THỨC SAI KHI NÀO? (câu hỏi thật của bài) ---
1. Khi ACTION tự thêm shuffle. Nhìn cùng pipeline, đổi action:
       v1: write -> %d stage / %d shuffle   |   count -> %d stage / %d shuffle
   count() cài thêm Exchange SinglePartition để gom partial count. Công thức
   vẫn đúng, chỉ là bạn đếm THIẾU một shuffle mà mắt không nhìn thấy trong code.
2. Khi có nhiều action -> nhiều job, phải cộng từng job (bài A10 q6).
3. Khi có stage SKIPPED (shuffle reuse) -> tab Jobs hiện số stage lớn hơn số
   stage thật sự chạy (bài A12).
4. Khi BẬT AQE -> AQE có thể gộp partition/đổi join -> DAG khác (bài A14).
5. Khi join 2 nguồn: DAG rẽ nhánh, 2 stage scan chạy SONG SONG. Công thức
   (stage = shuffle + 1) vẫn ĐÚNG, nhưng đừng tưởng tượng DAG là 1 đường thẳng.

--- ĐỌC SỐ TASK ---
Số task của một stage = số partition mà stage đó xử lý:
    stage scan       -> partition lúc đọc file (do maxPartitionBytes, bài A15)
    stage sau repartition(4) -> đúng 4 task
    stage sau groupBy        -> %s task (spark.sql.shuffle.partitions — bài A16:
                                200 task cho vài nghìn seller là chuyện khác,
                                nhưng nếu chỉ có 8 nhóm thì 192 task chạy KHÔNG)
""" % (
        w["v1"]["stages_run"], w["v2"]["stages_run"], w["v3"]["stages_run"],
        w["v1"]["stages_run"], w["v1"]["shuffles"],
        c["v1"]["stages_run"], c["v1"]["shuffles"],
        spark.conf.get("spark.sql.shuffle.partitions"),
    ))
    print("Ảnh DAG: mở %s -> tab Stages -> DAG Visualization, chụp 3 cái (v1/v2/v3)." % sc.uiWebUrl)
    print("Nếu job đã xong mất UI: xem lại ở Spark History, hoặc thêm time.sleep() trước spark.stop().")
    print("=" * 100 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
