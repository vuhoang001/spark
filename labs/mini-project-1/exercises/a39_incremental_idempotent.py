"""A39 — Ingest incremental: chạy lại MỘT NGÀY mà không giết cả bảng.

MỤC TIÊU (theo đề): trả lời câu hỏi mở rộng #2 bằng CODE CHẠY ĐƯỢC.
Đây là hình dạng của 90% pipeline batch ngoài đời: mỗi sáng nhận file của ngày hôm
qua, nạp vào bảng. Rồi 3 hôm sau phát hiện ngày 07-02 tính sai -> phải nạp lại ĐÚNG
ngày đó, KHÔNG được đụng 599 ngày còn lại.

BỐN CHẾ ĐỘ:
  --prepare          Giả lập nguồn incremental: bổ olist_orders_dataset.csv thành
                     data/incoming/dt=YYYY-MM-DD/*.csv (mỗi ngày một thư mục).
  --full             Backfill: nạp TẤT CẢ các ngày -> dựng bảng lần đầu (~600 partition).
  --date YYYY-MM-DD  Nạp ĐÚNG một ngày (dynamic overwrite). Thêm --repeat N để chạy
                     N lần liên tiếp -> BẰNG CHỨNG IDEMPOTENT.
  --static           Cùng việc như --date nhưng dùng partitionOverwriteMode=static.
                     ⚠️ NÓ SẼ XOÁ SẠCH BẢNG. Đó là mục đích: nhìn tận mắt thảm hoạ A26.

BẢNG ĐÍCH: /workspace/data/output/silver/orders_incremental
  (CỐ Ý tách khỏi silver/orders_clean của A37: bài này ghi đè/xoá bảng liên tục,
   không được phép làm hỏng bảng mà nhóm benchmark A30/A35/A36 đang đo trên đó.)

CHẠY (từ repo root, ĐÚNG THỨ TỰ).
Bài này CẦN THAM SỐ mà `make run` không truyền được -> gọi thẳng spark-submit.
Chạy local[2]: dữ liệu bé, và quan trọng hơn — KHÔNG chiếm 6 core của cluster.

    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit --master 'local[2]' \\
        /workspace/labs/mini-project-1/exercises/a39_incremental_idempotent.py --prepare

    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit --master 'local[2]' \\
        /workspace/labs/mini-project-1/exercises/a39_incremental_idempotent.py --full

    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit --master 'local[2]' \\
        /workspace/labs/mini-project-1/exercises/a39_incremental_idempotent.py --date 2018-07-02 --repeat 3

    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit --master 'local[2]' \\
        /workspace/labs/mini-project-1/exercises/a39_incremental_idempotent.py --date 2018-07-03
    # ^ sau lệnh này, ngày 07-02 phải KHÔNG SUY SUYỂN. Đó là điều phải chứng minh.
"""

import os
import sys
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

SRC_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
INCOMING = "/workspace/data/incoming"
TABLE = "/workspace/data/output/silver/orders_incremental"

# Ngày dùng làm ĐỐI CHỨNG: nó không được thay đổi khi ta nạp lại ngày khác.
CONTROL_DATE = "2017-05-16"

ORDERS_COLS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]
TS_COLS = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

ORDERS_RAW = StructType(
    [StructField(c, StringType(), True) for c in ORDERS_COLS]
    + [StructField("_corrupt_record", StringType(), True)]
)


def read_orders_csv(spark, path):
    """Đọc CSV -> vứt dòng hỏng -> ép kiểu -> derive order_date.
    Dùng chung cho cả --full lẫn --date: MỘT logic biến đổi, hai nguồn khác nhau.
    (Nếu logic full và logic incremental khác nhau thì sớm muộn hai bảng sẽ lệch —
     lỗi kinh điển của kiến trúc lambda.)"""
    df = (
        spark.read.schema(ORDERS_RAW)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(path)
    )
    df.cache()          # BẪY A24: filter 2 nhánh theo _corrupt_record -> phải cache
    df.count()
    clean = df.filter(F.col("_corrupt_record").isNull())

    # ⚠️ BẪY THẬT, ĐÃ SUÝT DÍNH: khi trỏ vào cây `incoming/` (có dt=.../), Spark TỰ
    # PHÁT HIỆN partition và ĐẺ THÊM cột `dt` vào DataFrame. Nhưng khi trỏ thẳng vào
    # `incoming/dt=2018-07-02` (đường dẫn lá) thì KHÔNG có cột `dt` nào cả.
    # => cùng một hàm đọc, hai nguồn, HAI SCHEMA KHÁC NHAU -> bảng Parquet sẽ có
    #    partition này thừa cột, partition kia thiếu cột. Đọc lại là hỏng.
    # Chốt cứng schema bằng select() để full-load và incremental luôn ghi ra ĐÚNG
    # cùng một bộ cột. Đây là loại lỗi chỉ lộ ra sau 3 tháng, khi ai đó query.
    clean = clean.select(*ORDERS_COLS)

    for c in TS_COLS:
        clean = clean.withColumn(c, F.col(c).cast("timestamp"))
    return (
        clean.withColumn("order_date", F.to_date("order_purchase_timestamp"))
             .filter(F.col("order_date").isNotNull())
    )


def part_dirs(path):
    """Số thư mục partition order_date=... hiện có trên đĩa."""
    if not os.path.isdir(path):
        return 0
    return len([d for d in os.listdir(path) if d.startswith("order_date=")])


def n_files(path):
    c = 0
    for _, _, files in os.walk(path):
        c += len([f for f in files if f.startswith("part-")])
    return c


def snapshot(spark, label):
    """Chụp trạng thái bảng: đây là BẰNG CHỨNG idempotent.
    Đọc lại từ đĩa mỗi lần (không tái dùng DataFrame cũ) — nếu tái dùng thì ta đang
    đo cái plan trong RAM chứ không đo file thật vừa ghi ra."""
    spark.catalog.clearCache()      # cẩn thận: file index/cache cũ có thể nói dối
    if not os.path.isdir(TABLE) or part_dirs(TABLE) == 0:
        return (label, 0, 0, 0, 0, 0)
    df = spark.read.parquet(TABLE)
    total = df.count()
    n_target = df.filter(F.col("order_date") == F.lit(TARGET_HOLDER[0])).count() \
        if TARGET_HOLDER[0] else 0
    n_ctrl = df.filter(F.col("order_date") == F.lit(CONTROL_DATE)).count()
    return (label, total, part_dirs(TABLE), n_target, n_ctrl, n_files(TABLE))


TARGET_HOLDER = [None]   # ngày đang nạp (dùng trong snapshot); [None] khi --full


def print_table(rows, target):
    print("\n| lần chạy | count() TOÀN BẢNG | số thư mục partition | count() ngày {} | count() ngày {} (đối chứng) | số file |"
          .format(target or "(n/a)", CONTROL_DATE))
    print("|---|---|---|---|---|---|")
    for label, total, parts, n_t, n_c, nf in rows:
        print("| {} | {:,} | {} | {:,} | {:,} | {} |".format(label, total, parts, n_t, n_c, nf))


def do_prepare(spark):
    """Bổ CSV gốc thành nguồn incremental theo ngày.
    Trong đời thật, thứ này do hệ thống nguồn (hoặc Airflow + sqoop/CDC) đẻ ra;
    ở đây ta tự giả lập để có cái mà nạp."""
    print("\n=== A39 --prepare: bổ orders CSV thành data/incoming/dt=<ngày>/ ===")
    df = read_orders_csv(spark, SRC_CSV).withColumn("dt", F.col("order_date"))
    n = df.count()
    n_days = df.select("dt").distinct().count()

    # partitionBy("dt") + CSV: mỗi ngày một thư mục dt=YYYY-MM-DD chứa 1 file.
    # repartition("dt") để mỗi ngày ra ĐÚNG 1 file thay vì 200 mảnh vụn (A35).
    # Cột order_date bị drop khỏi file: nó sẽ được derive lại lúc nạp, y hệt như
    # nguồn thật (nguồn thật không gửi kèm cột order_date cho bạn).
    (df.drop("order_date").repartition("dt")
       .write.mode("overwrite").partitionBy("dt")
       .option("header", True).csv(INCOMING))

    days = sorted([d for d in os.listdir(INCOMING) if d.startswith("dt=")])
    print("\n| chỉ số | giá trị |")
    print("|---|---|")
    print("| dòng orders sạch | {:,} |".format(n))
    print("| số ngày khác nhau | {:,} |".format(n_days))
    print("| số thư mục dt= tạo ra | {:,} |".format(len(days)))
    print("| ngày đầu / cuối | {} / {} |".format(days[0] if days else "-", days[-1] if days else "-"))
    print("| tổng file CSV | {:,} |".format(n_files(INCOMING)))
    print("\nVí dụ 3 thư mục: {}".format(days[:3]))


def do_full(spark):
    """Backfill toàn bộ lịch sử -> dựng bảng lần đầu.

    Ở ĐÂY DÙNG STATIC (mặc định), và đó là ĐÚNG: full load nghĩa là 'xoá bảng cũ,
    dựng lại từ đầu'. Static overwrite làm đúng việc đó.
    Đừng lẫn: static SAI ở bài toán incremental, ĐÚNG ở bài toán full rebuild.
    Cùng một nút vặn, hai bài toán, hai đáp án."""
    print("\n=== A39 --full: backfill TẤT CẢ các ngày (dựng bảng lần đầu) ===")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    spark.sparkContext.setJobDescription("A39 full backfill -> orders_incremental")

    df = read_orders_csv(spark, INCOMING)   # đọc cả cây incoming/dt=*/
    t = time.time()
    (df.repartition("order_date").write.mode("overwrite")
       .partitionBy("order_date").parquet(TABLE))
    dt = time.time() - t

    rows = [snapshot(spark, "sau --full")]
    print_table(rows, None)
    print("\nThời gian backfill: {:.1f}s".format(dt))


def do_date(spark, target, repeat, static):
    """Nạp ĐÚNG một ngày. Trái tim của bài này."""
    TARGET_HOLDER[0] = target
    mode = "static" if static else "dynamic"

    print("\n=== A39 --date {} · partitionOverwriteMode = {} · lặp {} lần ==="
          .format(target, mode, repeat))
    if static:
        print("""
⚠️  CẢNH BÁO CÓ CHỦ ĐÍCH: bạn đang chạy STATIC.
    Static overwrite hiểu 'overwrite' là: XOÁ SẠCH THƯ MỤC ĐÍCH rồi ghi cái mới vào.
    Bạn đưa cho nó dữ liệu 1 ngày -> nó xoá 600 ngày, để lại đúng 1.
    Hãy nhìn cột 'số thư mục partition' ở bảng dưới. Đó là bài học A26.
    Khôi phục: chạy lại --full.
""")

    # ĐÂY LÀ MỘT DÒNG CODE ĐÁNG GIÁ CẢ BẢNG DỮ LIỆU.
    # dynamic: chỉ ghi đè NHỮNG partition có mặt trong dữ liệu mới. 599 ngày kia
    #          không hề bị đụng tới — Spark thậm chí không mở thư mục của chúng.
    # static : xoá sạch thư mục đích. Không hỏi lại. Không undo.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", mode)

    rows = [snapshot(spark, "TRƯỚC khi nạp")]

    src = INCOMING + "/dt=" + target
    if not os.path.isdir(src):
        print("KHÔNG CÓ nguồn cho ngày {} ({}). Chạy --prepare trước.".format(target, src))
        raise SystemExit(2)

    for i in range(1, repeat + 1):
        spark.sparkContext.setJobDescription(
            "A39 nap ngay {} (lan {}/{}) mode={}".format(target, i, repeat, mode))
        df = read_orders_csv(spark, src)
        n_in = df.count()
        t = time.time()
        # KHÔNG repartition theo order_date ở đây: cả DataFrame chỉ có ĐÚNG 1 ngày,
        # repartition("order_date") sẽ hash 1 giá trị -> 1 partition có dữ liệu + 199
        # partition rỗng -> 199 task vô ích (bài học A16). coalesce(1) là đủ và rẻ:
        # dữ liệu 1 ngày ~ vài trăm dòng, một task xử lý thoải mái.
        (df.coalesce(1).write.mode("overwrite")
           .partitionBy("order_date").parquet(TABLE))
        el = time.time() - t
        rows.append(snapshot(spark, "sau lần {} ({:.1f}s, đọc {} dòng)".format(i, el, n_in)))

    print_table(rows, target)

    # --- Tự kiểm chứng: 3 dòng "sau lần N" có GIỐNG HỆT nhau không? ---
    after = [r for r in rows if r[0].startswith("sau lần")]
    if len(after) >= 2:
        sig = set((r[1], r[2], r[3], r[4]) for r in after)   # (total, parts, n_target, n_ctrl)
        print("\n**Kiểm chứng idempotent:** {} lần ghi -> {} trạng thái khác nhau -> {}".format(
            len(after), len(sig),
            "✅ IDEMPOTENT (mọi lần chạy cho kết quả y hệt)" if len(sig) == 1
            else "❌ KHÔNG idempotent — dữ liệu bị nhân bản/đổi giữa các lần"))

    before = rows[0]
    if not static and before[1] > 0:
        keep = (before[4] == after[-1][4]) if after else False
        print("**Ngày đối chứng {}:** trước = {:,} dòng, sau = {:,} dòng -> {}".format(
            CONTROL_DATE, before[4], after[-1][4] if after else 0,
            "✅ KHÔNG SUY SUYỂN" if keep else "❌ BỊ ĐỤNG (dynamic đã hỏng?)"))

    print("""
### Câu bằng chữ IN HOA mà tôi sẽ không bao giờ quên

    `mode("overwrite")` KHÔNG CÓ NGHĨA LÀ "GHI ĐÈ NHỮNG GÌ TÔI ĐƯA CHO ANH".
    MẶC ĐỊNH (static) NÓ CÓ NGHĨA LÀ "XOÁ SẠCH THƯ MỤC ĐÍCH".
    MUỐN GHI ĐÈ ĐÚNG MẤY PARTITION MÌNH ĐANG CẦM, PHẢI BẬT:
        spark.sql.sources.partitionOverwriteMode = dynamic

### Vì sao chạy 3 lần vẫn ra một kết quả (cơ chế, không phải phép màu)

Idempotent ở đây KHÔNG đến từ việc "Spark thông minh biết dòng nào đã có". Spark
không hề so sánh dòng. Nó đến từ chỗ: **đơn vị ghi là cả một PARTITION, và thao tác
là THAY THẾ chứ không phải CỘNG THÊM.** Nạp lại ngày 07-02 = vứt cả thư mục
`order_date=2018-07-02/` cũ, ghi thư mục mới từ cùng một file nguồn -> cùng input,
cùng logic, cùng output. Nếu dùng `append` thì mỗi lần chạy là một lần CỘNG THÊM ->
chạy 3 lần = dữ liệu ×3 = rubric trừ 15 điểm.

=> Điều kiện để idempotent: (1) partition là đơn vị ghi, (2) overwrite dynamic,
   (3) một ngày dữ liệu nằm gọn trong một partition. Thiếu (3) — ví dụ file nguồn của
   ngày 07-02 lại chứa lẫn dòng của 07-01 — thì dynamic sẽ ghi đè luôn cả partition
   07-01 bằng phần dữ liệu THIẾU đó. **Dynamic không bảo vệ bạn khỏi nguồn bẩn.**
""")


def main():
    args = sys.argv[1:]
    spark = SparkSession.builder.appName("a39-incremental").getOrCreate()

    if "--prepare" in args:
        do_prepare(spark)
    elif "--full" in args:
        do_full(spark)
    elif "--date" in args:
        target = args[args.index("--date") + 1]
        repeat = int(args[args.index("--repeat") + 1]) if "--repeat" in args else 1
        do_date(spark, target, repeat, static="--static" in args)
    else:
        print(__doc__)
        print("Thiếu chế độ. Dùng: --prepare | --full | --date YYYY-MM-DD [--repeat N] [--static]")
        spark.stop()
        raise SystemExit(2)

    spark.stop()


if __name__ == "__main__":
    main()
