"""A37 — Layout Bronze -> Silver -> Gold (medallion architecture).

MỤC TIÊU (theo đề): biết mình đang đứng ở TẦNG nào của một lakehouse.

Ba tầng, ba khách hàng khác nhau:
  bronze/  — CSV đọc vào, CHƯA ĐỘNG GÌ. Mọi cột là StringType (kể cả ngày, tiền),
             chỉ thêm `source_file` + `ingest_ts`. Khách hàng: kỹ sư đi REPLAY/AUDIT.
             VÌ SAO all-string? Vì ép kiểu là một hành vi CÓ THỂ LÀM MẤT DỮ LIỆU:
             cast("2017-13-45" AS timestamp) -> NULL, im lặng, không ai biết. Bronze
             mà cast thì bạn đã hủy bằng chứng ngay ở cửa vào. Bronze giữ nguyên văn.
  silver/  — đã ép kiểu, đã vứt dòng hỏng, đã derive order_date, đã partition.
             Khách hàng: analyst chạy SQL. (Đây chính là `orders_clean` của Checkpoint 2.)
  gold/    — bảng mart nhỏ: daily_revenue(order_date, n_orders, revenue, avg_ticket).
             Khách hàng: dashboard. Bé (~600 dòng) -> ghi 1 FILE, KHÔNG partition.

LUỒNG: CSV -> bronze -> silver -> gold. Silver đọc từ BRONZE, không đọc lại CSV.
Đó là điểm mấu chốt của medallion: mỗi tầng chỉ phụ thuộc tầng ngay trước nó, nên
khi silver sai logic, bạn chạy lại silver từ bronze — KHÔNG cần đụng vào file nguồn
(mà file nguồn thì có thể đã bị nhà cung cấp xoá/ghi đè rồi).

CHẠY (một trong hai đều được, dữ liệu bé):
    make run       F=labs/mini-project-1/exercises/a37_bronze_silver_gold.py
    make run-local F=labs/mini-project-1/exercises/a37_bronze_silver_gold.py

OUTPUT: /workspace/data/output/{bronze,silver,quarantine,gold}/
"""

import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

SRC = "/workspace/data/olist"
OUT = "/workspace/data/output"

BRONZE = OUT + "/bronze"
SILVER = OUT + "/silver"
QUAR = OUT + "/quarantine"
GOLD = OUT + "/gold"

# 8 trạng thái hợp lệ của Olist — dùng lại ở A38.
VALID_STATUS = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]

# ---------------------------------------------------------------------------
# SCHEMA — khai báo tường minh, CẤM inferSchema (rubric −10 điểm).
#
# Ở tầng BRONZE mọi cột là String. Không phải lười — là CÓ CHỦ ĐÍCH:
# bronze phải giữ được cả dòng "hôm qua" ở cột timestamp để tầng sau còn soi được.
# Kiểu thật (Timestamp/Double/Integer) chỉ xuất hiện ở SILVER, qua một phép cast
# mà ta ĐẾM ĐƯỢC số dòng chết trong lúc cast (xem hàm cast_report bên dưới).
#
# Ghi chú packaging: schema bị lặp lại ở a38/a39/a40 thay vì import từ src/schemas.py.
# Lý do: `make run` submit ĐÚNG MỘT file lên cluster, không có --py-files -> import
# module cạnh bên sẽ ModuleNotFoundError trên executor. Đề bài cũng nói thẳng:
# "vướng packaging quá 30 phút thì gộp file lại" (mục 4, ghi chú kỹ thuật).
# ---------------------------------------------------------------------------
ORDERS_COLS = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]
ITEMS_COLS = [
    "order_id", "order_item_id", "product_id", "seller_id",
    "shipping_limit_date", "price", "freight_value",
]


def raw_schema(cols):
    """Schema toàn String + _corrupt_record (bắt buộc phải khai, không tự có)."""
    return StructType(
        [StructField(c, StringType(), True) for c in cols]
        + [StructField("_corrupt_record", StringType(), True)]
    )


ORDERS_RAW = raw_schema(ORDERS_COLS)
ITEMS_RAW = raw_schema(ITEMS_COLS)

# Kiểu THẬT ở tầng silver: (tên cột, biểu thức cast)
ORDERS_CAST = {
    "order_id": "string", "customer_id": "string", "order_status": "string",
    "order_purchase_timestamp": "timestamp", "order_approved_at": "timestamp",
    "order_delivered_carrier_date": "timestamp",
    "order_delivered_customer_date": "timestamp",
    "order_estimated_delivery_date": "timestamp",
}
ITEMS_CAST = {
    "order_id": "string",
    "order_item_id": "int",       # SỐ THỨ TỰ item trong đơn -> Integer, không phải id!
    "product_id": "string",       # id giữ String: id có số 0 đứng đầu mà ép Integer là MẤT dữ liệu
    "seller_id": "string",
    "shipping_limit_date": "timestamp",
    "price": "double",            # bài học: production nên Decimal(10,2); Double sai số ~1e-16
    "freight_value": "double",
}


# ---------------------------------------------------------------------------
# Helper đo layout trên đĩa. Driver chạy trong container spark-submit, /workspace
# là volume dùng chung -> driver ĐỌC ĐƯỢC file mà executor vừa ghi.
# ---------------------------------------------------------------------------
def dir_stats(path):
    """(số file part-*, tổng bytes, số thư mục partition) của một thư mục output."""
    n_files, total, parts = 0, 0, 0
    for root, dirs, files in os.walk(path):
        if root == path:
            parts = len([d for d in dirs if "=" in d])
        for f in files:
            if f.startswith("part-"):
                n_files += 1
                total += os.path.getsize(os.path.join(root, f))
    return n_files, total, parts


def mb(n_bytes):
    return n_bytes / 1024.0 / 1024.0


def safe_count(spark, path):
    """count() một thư mục Parquet có thể RỖNG.

    BẪY THẬT: nếu DataFrame không có dòng nào (rất có thể xảy ra với quarantine —
    Olist gốc là CSV khá sạch về CẤU TRÚC), Spark ghi ra thư mục chỉ có `_SUCCESS`
    và KHÔNG có file part-* nào. Đọc lại thư mục đó -> AnalysisException:
    'Unable to infer schema for Parquet. It must be specified manually.'
    -> script chết ở đúng dòng in bằng chứng. Chặn trước bằng cách nhìn đĩa.
    """
    n_files, _, _ = dir_stats(path)
    if n_files == 0:
        return 0
    return spark.read.parquet(path).count()


def main():
    spark = SparkSession.builder.appName("a37-bronze-silver-gold").getOrCreate()
    sc = spark.sparkContext

    # Full load (dựng lại cả bảng) -> overwrite STATIC là ĐÚNG: nó xoá sạch bảng cũ
    # rồi ghi lại. Idempotent theo nghĩa "chạy 2 lần ra y hệt".
    # (dynamic là chuyện của A39 — ghi đè ĐÚNG MỘT ngày. Đừng lẫn hai thứ.)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    t_all = time.time()
    log = []  # gom các dòng bảng markdown để in một thể ở cuối

    # =======================================================================
    # TẦNG 1 — BRONZE: đọc nguyên văn, chỉ dán 2 cái nhãn truy vết
    # =======================================================================
    sc.setJobDescription("A37 bronze: read CSV + source_file + ingest_ts")

    def read_bronze(fname, schema, name):
        df = (
            spark.read.schema(schema)
            .option("header", True)
            .option("mode", "PERMISSIVE")       # dòng hỏng -> _corrupt_record, KHÔNG chết pipeline
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(SRC + "/" + fname)
            .withColumn("source_file", F.input_file_name())   # A29: từ file nào ra
            .withColumn("ingest_ts", F.current_timestamp())   # A29: nhập lúc nào
        )
        # ⚠️ BẪY A24: sắp filter theo _corrupt_record HAI lần (sạch + hỏng). Không cache
        # thì Spark đọc lại CSV cho mỗi nhánh, và ở vài phiên bản còn ném lỗi
        # "Queries from raw JSON/CSV files are disallowed when the referenced columns
        #  only include the internal corrupt record column".
        df.cache()
        total = df.count()  # action này vừa mồi cache vừa cho ta số dòng
        n_bad = df.filter(F.col("_corrupt_record").isNotNull()).count()
        log.append((name, total, n_bad, total - n_bad))
        return df

    orders_b = read_bronze("olist_orders_dataset.csv", ORDERS_RAW, "orders")
    items_b = read_bronze("olist_order_items_dataset.csv", ITEMS_RAW, "order_items")

    # Ghi bronze. Không partition: bronze là "hộp đen", đọc lại theo lô chứ không
    # ai query bronze theo ngày. coalesce(2) để khỏi đẻ rừng file vụn cho 17MB.
    for df, name in [(orders_b, "orders"), (items_b, "order_items")]:
        sc.setJobDescription("A37 bronze: write " + name)
        (df.coalesce(2).write.mode("overwrite")
           .parquet(BRONZE + "/" + name))

    # =======================================================================
    # QUARANTINE — dòng hỏng có nhà riêng, kèm đủ đồ nghề để REPLAY (A29)
    # =======================================================================
    sc.setJobDescription("A37 quarantine: write dong hong")
    for df, name in [(orders_b, "orders"), (items_b, "order_items")]:
        bad = df.filter(F.col("_corrupt_record").isNotNull())
        (bad.coalesce(1).write.mode("overwrite")
            .parquet(QUAR + "/" + name))

    # =======================================================================
    # TẦNG 2 — SILVER: đọc từ BRONZE (không đọc lại CSV), ép kiểu, lọc, partition
    # =======================================================================
    sc.setJobDescription("A37 silver: cast + derive order_date")
    ob = spark.read.parquet(BRONZE + "/orders")
    ib = spark.read.parquet(BRONZE + "/order_items")

    def cast_all(df, castmap):
        out = df
        for col, typ in castmap.items():
            out = out.withColumn(col, F.col(col).cast(typ))
        return out

    orders_typed = (
        cast_all(ob.filter(F.col("_corrupt_record").isNull()), ORDERS_CAST)
        .drop("_corrupt_record")
    )
    items_typed = (
        cast_all(ib.filter(F.col("_corrupt_record").isNull()), ITEMS_CAST)
        .drop("_corrupt_record")
    )

    # ĐẾM SỐ DÒNG CHẾT TRONG LÚC CAST — thứ mà _corrupt_record KHÔNG bắt được.
    # Chuỗi "hôm qua" ở cột timestamp: đúng cấu trúc CSV (Spark thấy đủ cột) nên
    # KHÔNG vào _corrupt_record; nó chỉ lặng lẽ hoá NULL khi cast. Đây chính là
    # "lỗi ngữ nghĩa" mà A23 cảnh báo và A38 sinh ra để chặn.
    n_cast_dead = (
        ob.filter(F.col("_corrupt_record").isNull())
          .filter(
              F.col("order_purchase_timestamp").isNotNull()
              & F.col("order_purchase_timestamp").cast("timestamp").isNull()
          ).count()
    )

    orders_s = orders_typed.withColumn(
        "order_date", F.to_date("order_purchase_timestamp")
    )
    n_null_date = orders_s.filter(F.col("order_date").isNull()).count()

    # QUYẾT ĐỊNH (phải biện luận trong report): dòng NULL order_date bị DROP khỏi
    # silver nhưng KHÔNG bị vứt đi — nó đã nằm nguyên vẹn ở bronze, và ta ĐẾM nó.
    # Vì sao không dồn vào partition '__unknown__'? Vì partition đó sẽ thành cái
    # thùng rác mà mọi query full-scan phải đọc qua, còn analyst thì không bao giờ
    # muốn nó. Bronze giữ bằng chứng; silver giữ sự sạch sẽ. Ranh giới rõ ràng.
    orders_s = orders_s.filter(F.col("order_date").isNotNull())

    # order_items KHÔNG có timestamp mua hàng -> lấy order_date từ orders qua join.
    # VÌ SAO không partition theo month(shipping_limit_date)? Vì mọi query thực tế
    # đều đi kèm orders theo NGÀY ĐẶT; partition theo một cột khác nghĩa là analyst
    # filter order_date thì items vẫn full-scan -> pruning chết. Cùng khoá partition
    # với bảng cha = hai bảng prune CÙNG NHAU.
    sc.setJobDescription("A37 silver: join items <- order_date")
    items_s = items_typed.join(
        orders_s.select("order_id", "order_date"), "order_id", "inner"
    )

    # repartition("order_date") TRƯỚC khi ghi (bài học A17/A35): gom mọi dòng cùng
    # ngày về CÙNG một partition -> mỗi ngày ra 1 file, thay vì 200 shuffle-partition
    # × 600 ngày = rừng file vụn. coalesce KHÔNG làm được việc này (nó không shuffle,
    # không gom được theo khoá).
    sc.setJobDescription("A37 silver: write orders_clean partitioned by order_date")
    t = time.time()
    (orders_s.repartition("order_date").write.mode("overwrite")
        .partitionBy("order_date").parquet(SILVER + "/orders_clean"))
    t_orders_w = time.time() - t

    sc.setJobDescription("A37 silver: write items_clean partitioned by order_date")
    t = time.time()
    (items_s.repartition("order_date").write.mode("overwrite")
        .partitionBy("order_date").parquet(SILVER + "/items_clean"))
    t_items_w = time.time() - t

    # =======================================================================
    # TẦNG 3 — GOLD: daily_revenue. Bé tí, 1 file, KHÔNG partition.
    # =======================================================================
    sc.setJobDescription("A37 gold: daily_revenue")
    o = spark.read.parquet(SILVER + "/orders_clean")
    i = spark.read.parquet(SILVER + "/items_clean")

    # QUYẾT ĐỊNH: chỉ tính đơn 'delivered'. Doanh thu chỉ được ghi nhận khi hàng đã
    # tới tay khách — đơn 'canceled'/'unavailable' mà tính vào revenue là bịa số cho
    # sếp. (Nếu dashboard cần cả GMV đặt-hàng thì đó là một bảng gold KHÁC, không
    # phải nhét thêm cột vào bảng này.)
    gold = (
        i.join(o.filter(F.col("order_status") == "delivered")
                .select("order_id", "order_date"), ["order_id", "order_date"])
         .groupBy("order_date")
         .agg(
             F.countDistinct("order_id").alias("n_orders"),
             F.round(F.sum("price"), 2).alias("revenue"),
         )
         .withColumn("avg_ticket", F.round(F.col("revenue") / F.col("n_orders"), 2))
         .orderBy("order_date")
    )

    t = time.time()
    # coalesce(1): gold ~600 dòng ≈ vài chục KB. Ghi 200 file cho 600 dòng là tội ác.
    # Ở đây coalesce(1) AN TOÀN vì upstream đã aggregate xuống còn 600 dòng —
    # ép 1 task xử lý 600 dòng thì không sao. (coalesce(1) chỉ nguy hiểm khi nó bóp
    # cả upstream ĐANG xử lý dữ liệu lớn về single-thread — xem bẫy A17.)
    (gold.coalesce(1).write.mode("overwrite").parquet(GOLD + "/daily_revenue"))
    t_gold_w = time.time() - t
    n_gold = gold.count()

    # =======================================================================
    # BẰNG CHỨNG — in ra dạng dán thẳng vào Markdown
    # =======================================================================
    print("\n" + "=" * 78)
    print("A37 — BRONZE / SILVER / GOLD")
    print("=" * 78)

    print("\n### Đọc vào (bronze)\n")
    print("| bảng | dòng đọc | dòng hỏng (_corrupt_record) | dòng sạch |")
    print("|---|---|---|---|")
    for name, total, bad, ok in log:
        print("| {} | {:,} | {:,} | {:,} |".format(name, total, bad, ok))

    print("\n### Dòng chết trong lúc CAST (lỗi NGỮ NGHĨA — _corrupt_record KHÔNG bắt)\n")
    print("| hiện tượng | số dòng |")
    print("|---|---|")
    print("| orders: chuỗi timestamp không null nhưng cast -> NULL | {:,} |".format(n_cast_dead))
    print("| orders: order_date NULL -> bị loại khỏi silver (vẫn còn ở bronze) | {:,} |".format(n_null_date))

    print("\n### Layout 3 tầng\n")
    print("| tầng | đường dẫn | dòng | số file part-* | dung lượng | thư mục partition |")
    print("|---|---|---|---|---|---|")
    rows = [
        ("bronze", BRONZE + "/orders", safe_count(spark, BRONZE + "/orders")),
        ("bronze", BRONZE + "/order_items", safe_count(spark, BRONZE + "/order_items")),
        ("quarantine", QUAR + "/orders", safe_count(spark, QUAR + "/orders")),
        ("quarantine", QUAR + "/order_items", safe_count(spark, QUAR + "/order_items")),
        ("silver", SILVER + "/orders_clean", o.count()),
        ("silver", SILVER + "/items_clean", i.count()),
        ("gold", GOLD + "/daily_revenue", n_gold),
    ]
    for tier, path, n in rows:
        nf, sz, np_ = dir_stats(path)
        short = path[len("/workspace/data/output/"):]
        print("| {} | `{}` | {:,} | {} | {:.2f} MB | {} |".format(
            tier, short, n, nf, mb(sz), np_))

    print("\n### Thời gian ghi\n")
    print("| bước | giây |")
    print("|---|---|")
    print("| silver/orders_clean | {:.1f} |".format(t_orders_w))
    print("| silver/items_clean | {:.1f} |".format(t_items_w))
    print("| gold/daily_revenue | {:.1f} |".format(t_gold_w))
    print("| TỔNG pipeline (cả session) | {:.1f} |".format(time.time() - t_all))

    print("\n### Gold — 5 dòng đầu\n")
    gold.show(5, truncate=False)

    print("\n### Cây thư mục (2 mức đầu)\n")
    print("```")
    for tier in ["bronze", "silver", "quarantine", "gold"]:
        base = OUT + "/" + tier
        if not os.path.isdir(base):
            continue
        print(tier + "/")
        for tbl in sorted(os.listdir(base)):
            p = os.path.join(base, tbl)
            if not os.path.isdir(p):
                continue
            kids = sorted(os.listdir(p))
            partdirs = [k for k in kids if "=" in k]
            if partdirs:
                print("  " + tbl + "/            <- {} thư mục partition".format(len(partdirs)))
                for k in partdirs[:2]:
                    inner = [x for x in os.listdir(os.path.join(p, k)) if x.startswith("part-")]
                    print("    " + k + "/  ({} file)".format(len(inner)))
                print("    ...")
            else:
                nf = len([k for k in kids if k.startswith("part-")])
                print("  " + tbl + "/            <- {} file part-*, KHÔNG partition".format(nf))
    print("```")

    print("""
### Mỗi tầng phục vụ ai (3 dòng cho report)

- **bronze** — phục vụ KỸ SƯ: replay & audit. Nguyên văn, all-string, có `source_file`
  + `ingest_ts`. Nguồn CSV có thể bị nhà cung cấp xoá; bronze thì không. Muốn sửa logic
  silver? Chạy lại từ bronze, không cần xin lại file.
- **silver** — phục vụ ANALYST: đã đúng kiểu, đã sạch, đã partition theo `order_date`
  nên `WHERE order_date = ...` prune được. Đây là bảng người ta viết SQL lên hàng ngày.
- **gold** — phục vụ DASHBOARD: đã aggregate sẵn, 1 file, đọc phát ra luôn.
  **Vì sao gold KHÔNG partition:** cả bảng chỉ ~{:,} dòng / {:.0f} KB. Partition theo ngày
  = {:,} thư mục, mỗi thư mục ~{:.0f} byte + một footer Parquet ~1KB -> METADATA TO HƠN
  DỮ LIỆU, và dashboard thì luôn đọc TOÀN BỘ bảng (vẽ đường theo thời gian) nên chẳng
  prune được gì. Partition chỉ có lãi khi query BỎ QUA được phần lớn dữ liệu.
""".format(n_gold, mb(dir_stats(GOLD + "/daily_revenue")[1]) * 1024, n_gold,
           dir_stats(GOLD + "/daily_revenue")[1] / max(n_gold, 1)))

    spark.stop()


if __name__ == "__main__":
    main()
