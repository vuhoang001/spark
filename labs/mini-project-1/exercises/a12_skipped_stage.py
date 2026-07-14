"""A12 — Săn "skipped stage": vì sao job thứ hai nhanh bất thường.

Chạy:
    make run F=labs/mini-project-1/exercises/a12_skipped_stage.py        # cluster (khuyến nghị:
                                                                        #   thấy shuffle file nằm trên
                                                                        #   ĐĨA CỦA EXECUTOR ở container khác)
    make run-local F=labs/mini-project-1/exercises/a12_skipped_stage.py  # local[2]: vẫn có skipped,
                                                                        #   nhưng "đĩa executor" = đĩa driver

Output: bảng job (job 2 có stage xám SKIPPED) + vị trí THẬT của shuffle file trên đĩa executor.

────────────────────────────────────────────────────────────────────────
Ý tưởng: ShuffleMapStage ghi shuffle file ra LOCAL DISK của executor và
KHÔNG xoá sau khi job xong. Job sau cần đúng đoạn tính toán đó -> DAG
Scheduler thấy "output còn nguyên trên đĩa" -> BỎ QUA, không chạy lại.
Trên UI: stage màu xám, chữ (skipped). Bạn được tặng một dạng cache mà
không hề gọi .cache().

3 thí nghiệm:
    J1  joined.count()                      -> chạy đủ mọi stage
    J2  joined.groupBy(...).count().show()  -> CÙNG object `joined`
                                               -> các stage trước join: SKIPPED
    J3  dựng LẠI joined từ đầu (df mới) rồi count()
                                               -> KHÔNG skipped. Đây là bằng chứng
                                                  reuse gắn với ĐỐI TƯỢNG plan,
                                                  không phải với "dữ liệu giống nhau".
────────────────────────────────────────────────────────────────────────

Tắt broadcast join: items ~15MB, Spark có thể chọn BroadcastHashJoin -> KHÔNG
có shuffle -> không có gì để mà skip, cả bài tập tan thành mây khói.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

import uiprobe

SRC = "/workspace/data/olist"

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


def find_shuffle_files(sc):
    """Chạy TRÊN EXECUTOR để xem shuffle file nằm ở đâu, tên gì.

    Đây là bằng chứng vật lý cho câu "shuffle file nằm trên local disk của
    executor". Hàm chạy trong task -> os.uname() trả hostname của CONTAINER
    WORKER, không phải container của bạn.

    Mặc định spark.local.dir = /tmp -> block manager tạo /tmp/blockmgr-<uuid>/.
    File .data/.index trong đó chính là shuffle output.

    Lưu ý: dùng parallelize(range(N), N) với N nhỏ -> mỗi executor gần như chắc
    chắn nhận ít nhất 1 task, nhưng KHÔNG có gì bảo đảm tuyệt đối (task có thể
    dồn hết vào 1 executor). Nếu kết quả chỉ ra 1 host, đó là chuyện bình thường,
    đừng kết luận "executor kia không có shuffle file".
    """
    def probe(_):
        # import BÊN TRONG hàm: hàm này bị serialize rồi gửi sang executor. Mọi
        # thứ nó tham chiếu phải tồn tại Ở ĐÓ. Import trong thân hàm là cách
        # chắc ăn nhất (đây cũng là mầm mống của lỗi "Task not serializable"
        # huyền thoại — lesson 3 §4).
        import glob
        import os
        import socket
        host = socket.gethostname()

        # ĐỪNG hardcode "/tmp/blockmgr-*" — LẦN ĐẦU CHẠY BÀI NÀY TÔI ĐÃ SAI ĐÚNG
        # Ở ĐÂY và bảng in ra toàn số 0 (0 thư mục, 0 file). Sự thật đo được:
        # blockmgr KHÔNG nằm ngay dưới /tmp, nó nằm sâu hơn HAI tầng —
        #     /tmp/spark-<uuid>/executor-<uuid>/blockmgr-<uuid>/
        # vì Worker standalone không để executor tự chọn chỗ: nó tạo sẵn thư mục
        # riêng cho từng executor rồi ĐƯA đường dẫn qua biến môi trường
        # SPARK_LOCAL_DIRS. Nên cách đúng là HỎI EXECUTOR (đọc env), đừng đoán.
        #
        # Thư mục này bị xoá khi app chết (DiskBlockManager có shutdown hook) ->
        # muốn nhìn thấy file thì phải hỏi TRONG LÚC APP CÒN SỐNG, đúng như hàm
        # này đang làm. Chạy `docker exec ... ls` sau khi job xong = thấy rỗng,
        # rồi tưởng "làm gì có shuffle file nào".
        roots = [d for d in os.environ.get("SPARK_LOCAL_DIRS", "").split(",") if d]
        if not roots:                       # local mode: không có biến này
            roots = ["/tmp"]
        dirs = []
        for r in roots:
            dirs += glob.glob(r + "/blockmgr-*")     # SPARK_LOCAL_DIRS trỏ vào CHA
            if os.path.basename(r).startswith("blockmgr-"):
                dirs.append(r)                        # ...hoặc trỏ thẳng vào blockmgr
        dirs = sorted(set(dirs))
        n_data = sum(len(glob.glob(d + "/**/*.data", recursive=True)) for d in dirs)
        n_index = sum(len(glob.glob(d + "/**/*.index", recursive=True)) for d in dirs)
        sample = dirs[0] if dirs else "(không thấy)"
        return [(host, sample, len(dirs), n_data, n_index)]

    n = max(sc.defaultParallelism, 2) * 2
    return sorted(set(sc.parallelize(range(n), n).mapPartitions(probe).collect()))


def main():
    spark = SparkSession.builder.appName("a12-skipped-stage").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # bắt buộc phải có shuffle

    uiprobe.wait_for_executors(spark, expected=2)

    orders = spark.read.csv(SRC + "/olist_orders_dataset.csv", header=True, schema=ORDERS)
    items = spark.read.csv(SRC + "/olist_order_items_dataset.csv", header=True, schema=ITEMS)

    print("\n" + "=" * 100)
    print("A12 — SĂN SKIPPED STAGE (AQE=off, broadcast=off)")
    print("=" * 100)
    print("master=%s | Spark UI: %s (tab Jobs -> job J2 sẽ có stage xám)" % (sc.master, sc.uiWebUrl))

    # MỘT object duy nhất, dùng lại cho J1 và J2. Chính chữ "một object" này là
    # linh hồn của bài: reuse xảy ra vì hai job cùng trỏ tới CÙNG MỘT
    # ShuffleDependency trong DAG, chứ không phải vì "dữ liệu trông giống nhau".
    joined = orders.join(items, "order_id")

    # ---------------- J1: chạy đủ ----------------
    sc.setJobGroup("j1", "A12/J1 joined.count() — lần đầu, chạy đủ")
    n1 = joined.count()
    s1 = uiprobe.summarize_group(spark, "j1")

    # ---------------- J2: CÙNG object -> mong đợi SKIPPED ----------------
    sc.setJobGroup("j2", "A12/J2 joined.groupBy().count() — dùng lại shuffle của J1")
    joined.groupBy("order_status").count().show()
    s2 = uiprobe.summarize_group(spark, "j2")

    # ---------------- J3: dựng LẠI từ đầu -> đối chứng ----------------
    sc.setJobGroup("j3", "A12/J3 df MỚI, cùng logic — có skip không?")
    orders2 = spark.read.csv(SRC + "/olist_orders_dataset.csv", header=True, schema=ORDERS)
    items2 = spark.read.csv(SRC + "/olist_order_items_dataset.csv", header=True, schema=ITEMS)
    n3 = orders2.join(items2, "order_id").count()
    s3 = uiprobe.summarize_group(spark, "j3")

    for tag, s in (("J1", s1), ("J2", s2), ("J3", s3)):
        print("\n" + "-" * 100)
        print(">>> %s — %d job | %d stage chạy | %d stage SKIPPED | %d shuffle" % (
            tag, s["jobs"], s["stages_run"], s["stages_skipped"], s["shuffles"]))
        print("-" * 100)
        uiprobe.print_stage_table(s)

    print("\n" + "=" * 100)
    print("BẢNG DÁN VÀO REPORT (A12)")
    print("=" * 100 + "\n")
    print("| job | code | stage trong job | stage CHẠY | stage **SKIPPED** | task đã chạy | thời gian |")
    print("|---|---|---|---|---|---|---|")
    for tag, code, s in (
        ("J1", "`joined.count()`", s1),
        ("J2", "`joined.groupBy(status).count().show()` — CÙNG object `joined`", s2),
        ("J3", "`orders2.join(items2).count()` — df dựng LẠI từ đầu", s3),
    ):
        print("| %s | %s | %d | %d | **%d** | %d | %.0f ms |" % (
            tag, code, s["stages_total"], s["stages_run"], s["stages_skipped"],
            s["tasks"], s["duration_ms"]))

    skipped_rows = [r for r in s2["stage_rows"] if r["skipped"]]
    print("\n--- CHI TIẾT STAGE BỊ SKIP Ở J2 ---")
    if skipped_rows:
        for r in skipped_rows:
            print("  stage %d (%s): %d task — KHÔNG chạy lại task nào, đọc thẳng "
                  "shuffle file cũ." % (r["stageId"], r["name"], r["numTasks"]))
    else:
        print("  KHÔNG có stage nào bị skip. Nếu gặp trường hợp này thì ghi đúng như thế "
              "vào report (đừng bịa), rồi đi tìm lý do: broadcast join có bị bật lại "
              "không? AQE có tắt thật không? `joined` có bị dựng lại không?")

    # ------------- Bằng chứng vật lý: shuffle file nằm ở đâu -------------
    print("\n--- SHUFFLE FILE NẰM Ở ĐÂU? (chạy code TRÊN executor để hỏi) ---")
    print("| host (container) | thư mục blockmgr thật | số thư mục | file .data | file .index |")
    print("|---|---|---|---|---|")
    for host, sample, ndirs, ndata, nindex in find_shuffle_files(sc):
        print("| %s | `%s` | %d | %d | %d |" % (host, sample, ndirs, ndata, nindex))
    print("""
Đường dẫn ở cột 2 là do EXECUTOR tự khai (đọc biến môi trường SPARK_LOCAL_DIRS),
không phải tôi đoán. Kiểm chứng bằng tay ở terminal khác, TRONG LÚC APP CÒN SỐNG
(app chết là thư mục bị xoá sạch, chạy sau sẽ không thấy gì — tôi đã dính bẫy này):
    docker exec spark-mastery-spark-worker-1 sh -c 'find /tmp -name "*.data" -path "*blockmgr*" | head'
""")

    print("""=== GIẢI THÍCH (phần phải viết vào report) ===

1. Spark tái dùng CÁI GÌ?
   Output của ShuffleMapStage: các file .data (dữ liệu đã băm sẵn theo partition
   đích) + .index (bản đồ offset). Stage sau không nhận dữ liệu được "đẩy" tới —
   nó tự KÉO (fetch) đúng mảnh của mình từ mọi executor. Vì file còn nguyên trên
   đĩa nên job sau chỉ việc kéo lại, khỏi tính lại từ CSV.

2. Nó nằm ở đâu?
   LOCAL DISK của executor (spark.local.dir, mặc định /tmp/blockmgr-*), tức ổ đĩa
   của container worker — KHÔNG phải HDFS/S3, KHÔNG phải RAM, KHÔNG phải driver.
   Xem bảng host ở trên: đó là hostname của worker container, không phải máy bạn.

3. Vì sao executor chết là mất luôn "skipped"?
   File shuffle nằm trên đĩa RIÊNG của executor đó. Executor chết (OOM, bị thu
   hồi, container restart) -> file đi theo. Job sau fetch không thấy file ->
   FetchFailedException -> DAG Scheduler phải CHẠY LẠI cả ShuffleMapStage đã mất
   (trên UI hiện "Resubmitted"). Đó là lý do:
       skipped stage là MÓN QUÀ CƠ HỘI, KHÔNG PHẢI HỢP ĐỒNG.
   Cần chắc chắn thì .cache()/.persist() (có replication, có Storage tab để nhìn),
   đừng thiết kế pipeline dựa vào việc shuffle file sẽ còn đó.
   (External Shuffle Service giữ file sống sót qua cái chết của executor — nhưng
   đó là chuyện của module 3, cluster này không bật.)

4. Vì sao J3 khác J2? (đối chứng — chỗ 9/10 người hiểu sai)
   J3 dựng DataFrame MỚI với logic Y HỆT, dữ liệu Y HỆT — mà vẫn phải chạy lại
   từ đầu. Vì reuse gắn với ĐỐI TƯỢNG ShuffleDependency trong DAG, không phải với
   "nội dung giống nhau". Muốn dùng lại kết quả thì phải dùng lại chính cái
   DataFrame đó (hoặc cache/ghi ra đĩa). Viết lại query từ đầu = tính lại từ đầu.
""")
    print("Số dòng để đối chiếu: J1 count = %s | J3 count = %s (phải bằng nhau)" % (
        "{:,}".format(n1), "{:,}".format(n3)))
    print("=" * 100 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
