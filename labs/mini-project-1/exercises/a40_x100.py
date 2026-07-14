"""A40 — Bài toán ×100: làm THẬT, không nói suông.

MỤC TIÊU (theo đề): mục "Tư duy scale" của rubric (10 điểm). Không được phép viết
"nếu dữ liệu lớn thì thêm executor là xong". Phải CHẠY THẬT trên ~10 triệu đơn và
chỉ ra thứ GÃY ĐẦU TIÊN — bằng số.

HAI CHẾ ĐỘ:
  --gen              Sinh dữ liệu ×100: spark.range(100).crossJoin(orders) -> ~9.94M đơn,
                     làm nhiễu order_id + dịch ngày -> ghi CSV ra data/big/orders_100x/
                     (chạy 1 lần, mất vài phút, tốn ~1.5-2 GB đĩa).
  --round N          Chạy pipeline silver (y hệt A37) trên dữ liệu ×100 với 1 bộ nút vặn.
                     N = 0,1,2,3. Mỗi vòng SỬA ĐÚNG MỘT THỨ so với vòng trước.

  VÒNG 0 — chạy NGUYÊN XI code Olist: shuffle.partitions=200 (mặc định),
           repartition("order_date") -> partitionBy("order_date"). Đây là baseline.
  VÒNG 1 — sửa shuffle.partitions 200 -> 24 (= 4 × 6 core). Chỉ đổi MỘT nút.
  VÒNG 2 — sửa hạt partition: NGÀY -> THÁNG (van an toàn của A20). Vẫn 24 shuffle parts.
  VÒNG 3 — sửa maxPartitionBytes 128m -> 64m (tăng song song lúc ĐỌC).

⚠️ BẮT BUỘC CHẠY CLUSTER. local[2] chỉ có driver JVM heap 1g — 1.7GB CSV + shuffle sẽ
   OOM hoặc chạy hàng chục phút. Bài này cần 6 core + 2 executor thật + spill thật.
⚠️ CHỈ MỘT APP SPARK TẠI MỘT THỜI ĐIỂM. Chạy tuần tự, đừng bật 2 vòng song song
   (app thứ 2 nhận 0 core và TREO VĨNH VIỄN, không báo lỗi).

CHẠY:
    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        /workspace/labs/mini-project-1/exercises/a40_x100.py --gen

    # rồi lần lượt (ĐỢI VÒNG TRƯỚC XONG HẲN):
    ... /workspace/labs/mini-project-1/exercises/a40_x100.py --round 0
    ... --round 1
    ... --round 2
    ... --round 3

SPILL ĐO Ở ĐÂU: script tự gọi REST API /api/v1/applications/<id>/stages và đọc
`memoryBytesSpilled` / `diskBytesSpilled` của TỪNG stage. Không phải chụp màn hình
Spark UI — UI tắt ngay khi app kết thúc, còn số thì phải lưu lại được.
"""

import json
import os
import sys
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

SRC_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
BIG_CSV = "/workspace/data/big/orders_100x"        # dữ liệu ×100 (CSV)
BIG_OUT = "/workspace/data/big/silver_100x"        # output của pipeline ×100

FACTOR = 100

# Olist trải từ 2016-09-04 đến 2018-10-17 (~774 ngày). Dữ liệu ×100 phải trải
# TRÊN CÙNG KHOẢNG THỜI GIAN đó — nếu không, ta chỉ đang nhân đôi mỗi partition-ngày
# lên 100 lần mà không đổi SỐ LƯỢNG partition, và bài toán small-files sẽ tự biến mất
# một cách giả tạo. Giữ nguyên ~774 ngày -> mỗi ngày ~12.800 đơn (thay vì ~128).
BASE_DATE = "2016-09-04"
SPAN_DAYS = 774

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

# Mỗi vòng sửa ĐÚNG MỘT THỨ. Đó là kỷ luật của thí nghiệm: đổi 2 thứ cùng lúc thì
# không bao giờ biết thứ nào có tác dụng.
ROUNDS = {
    0: dict(name="nguyên xi (baseline)", shuffle=200, grain="day", maxpb="128m"),
    1: dict(name="shuffle.partitions 200 -> 24", shuffle=24, grain="day", maxpb="128m"),
    2: dict(name="partition NGÀY -> THÁNG", shuffle=24, grain="month", maxpb="128m"),
    3: dict(name="maxPartitionBytes 128m -> 64m", shuffle=24, grain="month", maxpb="64m"),
}


# ---------------------------------------------------------------------------
# Đo đạc
# ---------------------------------------------------------------------------
def stage_metrics(sc):
    """Đọc metric mọi stage từ REST API. Trả về (list dòng, tổng spill mem, tổng spill disk).

    VÌ SAO KHÔNG DÙNG SPARK UI: UI chết cùng app. Số đo phải sống lâu hơn app, nếu
    không thì không ai tái lập được — mà không tái lập được thì không phải bằng chứng.
    """
    url = "{}/api/v1/applications/{}/stages".format(sc.uiWebUrl, sc.applicationId)
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            stages = json.loads(r.read())
    except Exception as e:                       # noqa: BLE001
        print("(!) Không đọc được REST API: {}".format(e))
        return [], 0, 0

    rows, tot_mem, tot_disk = [], 0, 0
    for s in stages:
        mem = s.get("memoryBytesSpilled", 0) or 0
        disk = s.get("diskBytesSpilled", 0) or 0
        tot_mem += mem
        tot_disk += disk
        rows.append({
            "id": s.get("stageId"),
            "name": (s.get("name") or "")[:44],
            "tasks": s.get("numTasks", 0),
            "in_b": s.get("inputBytes", 0) or 0,
            "sr_b": s.get("shuffleReadBytes", 0) or 0,
            "sw_b": s.get("shuffleWriteBytes", 0) or 0,
            "mem": mem,
            "disk": disk,
        })
    rows.sort(key=lambda r: r["id"])
    return rows, tot_mem, tot_disk


def dir_stats(path):
    n_files, total, parts = 0, 0, 0
    if not os.path.isdir(path):
        return 0, 0, 0
    parts = len([d for d in os.listdir(path) if "=" in d])
    for root, _, files in os.walk(path):
        for f in files:
            if f.startswith("part-"):
                n_files += 1
                total += os.path.getsize(os.path.join(root, f))
    return n_files, total, parts


def gb(n):
    return n / 1024.0 / 1024.0 / 1024.0


def mb(n):
    return n / 1024.0 / 1024.0


# ---------------------------------------------------------------------------
# --gen : sinh dữ liệu ×100
# ---------------------------------------------------------------------------
def do_gen(spark):
    sc = spark.sparkContext
    sc.setJobDescription("A40 gen: crossJoin x100 -> CSV")
    print("\n=== A40 --gen: sinh dữ liệu ×{} ===".format(FACTOR))

    base = (
        spark.read.schema(ORDERS_RAW).option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(SRC_CSV)
        .filter(F.col("_corrupt_record").isNull())
        .select(*ORDERS_COLS)
    )
    n_base = base.count()

    # crossJoin: range(100) × orders. BẪY (đề bài nhắc): đây là WIDE transformation.
    # Thực tế Spark thấy range(100) bé tí -> chọn BroadcastNestedLoopJoin: phát 100
    # con số đi khắp cluster rồi nhân tại chỗ, KHÔNG shuffle 17MB orders. Nhìn số
    # task ở bảng stage bên dưới để tự kiểm chứng (nếu thấy Exchange thì nó đã shuffle).
    dup = spark.range(FACTOR).withColumnRenamed("id", "copy_id").crossJoin(base)

    # Làm nhiễu — mục đích: 10 triệu đơn PHẢI có 10 triệu order_id KHÁC NHAU, nếu
    # không thì check "order_id unique" của A38 sẽ fail và bài toán scale bị lẫn với
    # bài toán data quality.
    dup = dup.withColumn(
        "order_id", F.concat_ws("-", F.col("order_id"), F.col("copy_id").cast("string"))
    )

    # Dịch ngày. DÙNG hash(order_id) CHỨ KHÔNG DÙNG rand():
    # rand() không có seed là KHÔNG XÁC ĐỊNH — task bị retry (rất hay xảy ra ở dữ
    # liệu lớn) sẽ sinh ra ngày KHÁC lần chạy trước -> dữ liệu tự mâu thuẫn với chính
    # nó. hash(order_id) là hàm thuần tuý: cùng input, cùng output, retry bao nhiêu
    # lần cũng vậy. Đây là một bài học production thật, không phải tiểu tiết.
    offset = F.pmod(F.abs(F.hash(F.col("order_id"))), F.lit(SPAN_DAYS))
    new_date = F.expr("date_add(to_date('{}'), offset)".format(BASE_DATE))

    dup = dup.withColumn("offset", offset).withColumn("new_date", new_date)
    # delta = số ngày phải dịch, áp CÙNG MỘT delta cho MỌI cột timestamp để không
    # phá vỡ quan hệ thời gian (đặt -> duyệt -> giao). Phá quan hệ đó là tự tay tạo
    # ra dữ liệu bẩn ngữ nghĩa, rồi A38 sẽ báo FAIL và ta lại tưởng pipeline hỏng.
    dup = dup.withColumn(
        "delta", F.datediff(F.col("new_date"), F.to_date("order_purchase_timestamp"))
    )
    # Dịch timestamp bằng unix_timestamp + delta×86400 giây.
    # (Không dùng `timestampadd`/`make_dt_interval` — chúng phụ thuộc phiên bản Spark;
    #  unix_timestamp/from_unixtime thì có từ đời nào tới giờ. Ở môi trường container
    #  cũ, chọn API cũ mà chắc còn hơn API đẹp mà gãy.)
    # NULL-safe: unix_timestamp(NULL) -> NULL -> from_unixtime(NULL) -> NULL, nên các
    # cột ngày còn thiếu (đơn chưa giao) vẫn giữ nguyên NULL. Đúng nghiệp vụ.
    for c in TS_COLS:
        dup = dup.withColumn(
            c,
            F.to_timestamp(
                F.from_unixtime(F.unix_timestamp(F.col(c)) + F.col("delta") * 86400)
            ),
        )

    out = dup.select(*ORDERS_COLS)

    # repartition(24) trước khi ghi: 24 file CSV × ~70MB. Không repartition thì số
    # file = số partition của crossJoin (có thể rất lệch), và ta muốn dữ liệu NGUỒN
    # trông giống nguồn thật: một nhúm file to, không phải rừng file vụn.
    t = time.time()
    (out.repartition(24).write.mode("overwrite")
        .option("header", True).csv(BIG_CSV))
    el = time.time() - t

    nf, sz, _ = dir_stats(BIG_CSV)
    n_out = spark.read.schema(ORDERS_RAW).option("header", True).csv(BIG_CSV).count()

    print("\n| chỉ số | giá trị |")
    print("|---|---|")
    print("| orders gốc | {:,} dòng |".format(n_base))
    print("| orders ×{} | {:,} dòng |".format(FACTOR, n_out))
    print("| order_id khác nhau | {:,} |".format(
        spark.read.schema(ORDERS_RAW).option("header", True).csv(BIG_CSV)
             .select("order_id").distinct().count()))
    print("| số file CSV | {} |".format(nf))
    print("| dung lượng CSV | {:.2f} GB |".format(gb(sz)))
    print("| thời gian sinh | {:.1f}s |".format(el))
    print("| khoảng ngày | {} .. +{} ngày |".format(BASE_DATE, SPAN_DAYS))

    rows, m, d = stage_metrics(sc)
    print_stages(rows, m, d)
    print("\n(Bẫy crossJoin: nhìn cột `tasks` và `sw_b` — nếu shuffle write ≈ 0 thì "
          "Spark đã broadcast chứ không shuffle. Đó là lý do nó không chết.)")


# ---------------------------------------------------------------------------
# --round N : chạy pipeline silver trên dữ liệu ×100
# ---------------------------------------------------------------------------
def do_round(spark, n):
    cfg = ROUNDS[n]
    sc = spark.sparkContext

    spark.conf.set("spark.sql.shuffle.partitions", cfg["shuffle"])
    spark.conf.set("spark.sql.files.maxPartitionBytes", cfg["maxpb"])
    # Full load -> static overwrite (xoá bảng cũ, dựng lại). Xem A39 để biết vì sao
    # KHÔNG dùng dynamic ở đây.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    print("\n" + "=" * 86)
    print("A40 — VÒNG {}: {}".format(n, cfg["name"]))
    print("=" * 86)
    print("shuffle.partitions = {} · maxPartitionBytes = {} · hạt partition = {} · AQE = {}".format(
        cfg["shuffle"], cfg["maxpb"], cfg["grain"],
        spark.conf.get("spark.sql.adaptive.enabled")))

    sc.setJobDescription("A40 round {}: read x100 CSV".format(n))
    df = (
        spark.read.schema(ORDERS_RAW).option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(BIG_CSV)
    )
    n_read_parts = df.rdd.getNumPartitions()   # số partition LÚC ĐỌC (do maxPartitionBytes)

    # KHÔNG cache ở đây: 1.7GB > tổng RAM-cho-data của cluster (~2.0GB cho CẢ HAI
    # executor). cache() sẽ evict liên tục hoặc spill ra đĩa -> chậm hơn là không cache.
    # Ở Olist 17MB thì cache là lãi; ở ×100 thì cache là LỖ. Cùng một dòng code,
    # đổi kích thước dữ liệu là đổi luôn đáp án. Đây chính là "tư duy scale".
    clean = df.filter(F.col("_corrupt_record").isNull()).select(*ORDERS_COLS)
    for c in TS_COLS:
        clean = clean.withColumn(c, F.col(c).cast("timestamp"))
    clean = clean.withColumn("order_date", F.to_date("order_purchase_timestamp")) \
                 .filter(F.col("order_date").isNotNull())

    if cfg["grain"] == "day":
        key = "order_date"
    else:
        # VAN AN TOÀN của A20: hạt thô hơn -> ít partition hơn -> file to hơn.
        # 774 ngày -> ~26 tháng. Mỗi tháng ~380k đơn -> file Parquet cỡ chục MB,
        # tiến gần chuẩn nghề 64-256MB, thay vì 774 file vụn.
        clean = clean.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
        key = "order_month"

    sc.setJobDescription("A40 round {}: write silver_100x partitionBy({})".format(n, key))
    t = time.time()
    (clean.repartition(key).write.mode("overwrite")
          .partitionBy(key).parquet(BIG_OUT))
    t_write = time.time() - t

    sc.setJobDescription("A40 round {}: count lai".format(n))
    t = time.time()
    n_rows = spark.read.parquet(BIG_OUT).count()
    t_count = time.time() - t

    nf, sz, nparts = dir_stats(BIG_OUT)
    rows, spill_mem, spill_disk = stage_metrics(sc)

    print_stages(rows, spill_mem, spill_disk)

    print("\n### DÒNG BẢNG cho §3.8 (dán thẳng vào PROGRESS.md)\n")
    print("| vòng | đã sửa gì | thời gian ghi | thời gian count | spill mem | spill disk | số file | tổng size | partition |")
    print("|---|---|---|---|---|---|---|---|---|")
    print("| {} | {} | {:.1f}s | {:.1f}s | {:.0f} MB | {:.0f} MB | {:,} | {:.0f} MB | {} |".format(
        n, cfg["name"], t_write, t_count, mb(spill_mem), mb(spill_disk), nf, mb(sz), nparts))

    print("\n| chi tiết | giá trị |")
    print("|---|---|")
    print("| dòng ghi ra | {:,} |".format(n_rows))
    print("| partition LÚC ĐỌC (maxPartitionBytes={}) | {} |".format(cfg["maxpb"], n_read_parts))
    print("| thư mục partition LÚC GHI | {} |".format(nparts))
    print("| số file part-* | {:,} |".format(nf))
    print("| kích thước TB mỗi file | {:.2f} MB |".format(mb(sz) / nf if nf else 0))
    print("| chuẩn nghề | 64–256 MB / file |")
    print("| lệch chuẩn | {:.0f}× nhỏ hơn cận dưới 64MB |".format(
        64.0 / (mb(sz) / nf) if nf and mb(sz) / nf > 0 else 0))


def print_stages(rows, spill_mem, spill_disk):
    if not rows:
        return
    print("\n### Metric từng stage (REST API — không phải ảnh chụp UI)\n")
    print("| stage | tên | tasks | input | shuffle read | shuffle write | spill mem | spill disk |")
    print("|---|---|---|---|---|---|---|---|")
    for r in rows:
        print("| {} | {} | {} | {:.0f} MB | {:.0f} MB | {:.0f} MB | {:.0f} MB | {:.0f} MB |".format(
            r["id"], r["name"], r["tasks"], mb(r["in_b"]), mb(r["sr_b"]),
            mb(r["sw_b"]), mb(r["mem"]), mb(r["disk"])))
    print("\n**TỔNG SPILL: memory {:.0f} MB · disk {:.0f} MB**".format(
        mb(spill_mem), mb(spill_disk)))
    if spill_disk > 0:
        print("""
> Spill > 0 nghĩa là: dữ liệu shuffle KHÔNG VỪA RAM của executor, Spark phải ghi tạm
> ra ĐĨA rồi đọc lại. Mỗi byte spill = 1 lần ghi + 1 lần đọc đĩa mà lẽ ra không cần.
> Nhắc lại cấu hình: mỗi executor chỉ có (2048−300)×0.6 = **1048.8 MB** cho
> execution+storage, tổng CẢ CLUSTER ~2.0 GB. Dữ liệu ×100 là ~1.7 GB CSV — nó KHÔNG
> vừa. Spill ở đây là TẤT YẾU, không phải bug.""")
    else:
        print("\n> Không có spill. (Nếu vòng này ×100 mà vẫn 0 spill, hãy kiểm tra: "
              "có phải AQE đã coalesce, hay dữ liệu chưa đủ lớn?)")


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        raise SystemExit(2)

    spark = (
        SparkSession.builder
        .appName("a40-x100-" + "-".join(args))
        .getOrCreate()
    )

    if "--gen" in args:
        do_gen(spark)
    elif "--round" in args:
        n = int(args[args.index("--round") + 1])
        if n not in ROUNDS:
            print("Vòng không hợp lệ. Có: {}".format(sorted(ROUNDS)))
            spark.stop()
            raise SystemExit(2)
        if not os.path.isdir(BIG_CSV):
            print("Chưa có dữ liệu ×100 ở {}. Chạy --gen trước.".format(BIG_CSV))
            spark.stop()
            raise SystemExit(2)
        do_round(spark, n)
    else:
        print(__doc__)
        spark.stop()
        raise SystemExit(2)

    spark.stop()


if __name__ == "__main__":
    main()
