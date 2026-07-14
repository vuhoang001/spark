"""A38 — Cổng chất lượng dữ liệu (data quality gate).

MỤC TIÊU (theo đề): bắt loại lỗi mà `_corrupt_record` ở A23 KHÔNG bắt được.

Nhắc lại bài học A23/A37: `_corrupt_record` chỉ bắt lỗi **CẤU TRÚC** (sai số cột,
ngoặc kép lệch). Nó mù hoàn toàn trước lỗi **NGỮ NGHĨA**:
    - `order_status = "delivered"` nhưng `order_delivered_customer_date` NULL
    - `price = -1`
    - `order_purchase_timestamp = 1970-01-01` (timestamp hỏng, epoch 0)
    - đơn 'delivered' mà không có item nào
Những dòng đó có ĐÚNG số cột, ĐÚNG kiểu, Spark đọc vào không kêu một tiếng —
rồi tháng sau sếp hỏi "sao doanh thu tháng 9 âm?".

=> Gate này chạy TRƯỚC KHI GHI SILVER, và có quyền GIẾT pipeline.

HAI MỨC (quyết định của tôi, biện luận in ở cuối output):
  BLOCKING — sai là DỪNG. Dùng cho thứ mà nếu sai thì mọi số ở tầng dưới đều SAI
             (khoá trùng -> join nhân bản dòng; price âm -> revenue sai).
             Thà không có dashboard còn hơn có dashboard nói dối.
  WARNING  — sai thì GHI LOG, vẫn chạy. Dùng cho thứ phản ánh SỰ THẬT XẤU XÍ của
             nghiệp vụ (đơn giao xong mà thiếu ngày giao là lỗi của hệ thống nguồn,
             không phải lỗi pipeline). Chặn ở đây = pipeline không bao giờ chạy được.

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a38_quality_gate.py
    # dữ liệu 17MB, local[2] là quá đủ, và KHÔNG chiếm 6 core của cluster.

    # muốn xem hết bảng mà không cho nó exit 1 (ví dụ khi hứng log vào results/):
    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit --master 'local[2]' \\
        /workspace/labs/mini-project-1/exercises/a38_quality_gate.py --warn-only

EXIT CODE LÀ MỘT PHẦN CỦA BẰNG CHỨNG:
    0 = mọi check blocking PASS -> được phép ghi silver
    1 = có blocking FAIL -> pipeline chết CÓ CHỦ ĐÍCH (đây là tính năng, không phải bug)
"""

import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

SRC = "/workspace/data/olist"

VALID_STATUS = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]

# Olist là dữ liệu thương mại điện tử Brazil 2016-2018. Bất kỳ ngày nào ngoài
# khoảng này = timestamp hỏng (epoch 0, năm 2099, hoặc lỗi parse).
DATE_MIN = "2016-01-01"
DATE_MAX = "2018-12-31"

DELIVERED_NULL_RATE_MAX = 0.05   # ngưỡng đề bài đặt: null rate delivered_date < 5%

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
    return StructType(
        [StructField(c, StringType(), True) for c in cols]
        + [StructField("_corrupt_record", StringType(), True)]
    )


def read_typed(spark, fname, cols, castmap):
    """Đọc CSV -> vứt dòng hỏng cấu trúc -> ép kiểu. Đây đúng là DataFrame mà
    a37 sắp ghi vào silver. Gate phải soi CHÍNH NÓ, không phải soi bản khác."""
    df = (
        spark.read.schema(raw_schema(cols))
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(SRC + "/" + fname)
    )
    df.cache()          # BẪY A24 — cache trước khi filter theo _corrupt_record
    df.count()
    clean = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    for c, t in castmap.items():
        clean = clean.withColumn(c, F.col(c).cast(t))
    return clean


# ---------------------------------------------------------------------------
# ĐỊNH NGHĨA CHECK
# Mỗi check trả về (ok, n_vi_pham, chi_tiet, df_mau).
# QUY TẮC VÀNG: check chỉ ĐO. Không check nào được phép "sửa" dữ liệu cho đẹp số.
# ---------------------------------------------------------------------------
def check_not_null(df, col):
    bad = df.filter(F.col(col).isNull())
    n = bad.count()
    return (n == 0, n, "{} dòng có {} = NULL".format(n, col), bad)


def check_unique(df, col):
    total = df.count()
    distinct = df.select(col).distinct().count()
    n = total - distinct
    bad = (df.groupBy(col).count().filter(F.col("count") > 1))
    return (n == 0, n, "{} dòng thừa (total {} vs distinct {})".format(n, total, distinct), bad)


def check_in_set(df, col, allowed):
    bad = df.filter(~F.col(col).isin(allowed) | F.col(col).isNull())
    n = bad.count()
    seen = [r[0] for r in df.select(col).distinct().collect()]
    extra = sorted([s for s in seen if s not in allowed])
    return (n == 0, n, "giá trị lạ: {}".format(extra if extra else "(không có)"), bad)


def check_non_negative(df, col):
    bad = df.filter(F.col(col) < 0)
    n = bad.count()
    return (n == 0, n, "{} dòng có {} < 0".format(n, col), bad)


def check_date_range(df, col, lo, hi):
    bad = df.filter(
        F.col(col).isNotNull()
        & ((F.col(col) < F.lit(lo).cast("timestamp"))
           | (F.col(col) > F.lit(hi).cast("timestamp")))
    )
    n = bad.count()
    return (n == 0, n, "{} dòng có {} ngoài [{} .. {}]".format(n, col, lo, hi), bad)


def check_null_rate(df, col, max_rate):
    total = df.count()
    n_null = df.filter(F.col(col).isNull()).count()
    rate = (n_null / float(total)) if total else 0.0
    ok = rate < max_rate
    return (ok, n_null,
            "null rate = {}/{} = {:.2%} (ngưỡng < {:.0%})".format(n_null, total, rate, max_rate),
            df.filter(F.col(col).isNull()))


def check_cast_survived(spark, fname, cols, col, typ):
    """Lỗi NGỮ NGHĨA kinh điển: chuỗi có giá trị nhưng cast ra NULL.
    Đọc lại bản RAW (all-string) để so: raw có chữ, typed thành NULL -> chết lúc cast.
    Đây chính là loại bẩn #3 của A23 ("hôm qua" ở cột timestamp) — nó KHÔNG vào
    _corrupt_record, nó chỉ lặng lẽ biến mất."""
    raw = (
        spark.read.schema(raw_schema(cols)).option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(SRC + "/" + fname)
        .filter(F.col("_corrupt_record").isNull())
    )
    bad = raw.filter(
        (F.trim(F.col(col)) != "")
        & F.col(col).isNotNull()
        & F.col(col).cast(typ).isNull()
    )
    n = bad.count()
    return (n == 0, n,
            "{} dòng: {} có chữ nhưng cast({}) -> NULL".format(n, col, typ), bad)


def check_delivered_has_date(df):
    """NGỮ NGHĨA: đơn đã 'delivered' thì BẮT BUỘC phải có ngày giao cho khách.
    Không có = hệ thống nguồn mâu thuẫn với chính nó."""
    bad = df.filter(
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_customer_date").isNull()
    )
    n = bad.count()
    return (n == 0, n, "{} đơn 'delivered' mà KHÔNG có ngày giao".format(n), bad)


def check_timeline(df):
    """NGỮ NGHĨA: thời gian phải chảy xuôi. Giao hàng TRƯỚC khi khách đặt = nghịch lý."""
    bad = df.filter(
        F.col("order_delivered_customer_date").isNotNull()
        & (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"))
    )
    n = bad.count()
    return (n == 0, n, "{} đơn giao TRƯỚC lúc đặt (du hành thời gian)".format(n), bad)


def check_orphan_items(orders, items):
    """TOÀN VẸN THAM CHIẾU: item trỏ tới order_id không tồn tại -> join sẽ nuốt mất
    doanh thu mà không ai hay."""
    bad = items.join(orders.select("order_id"), "order_id", "left_anti")
    n = bad.count()
    return (n == 0, n, "{} item mồ côi (order_id không có trong orders)".format(n), bad)


def check_delivered_has_item(orders, items):
    """Đơn 'delivered' mà không có item nào -> revenue của nó = 0. Giao cái gì?"""
    bad = (orders.filter(F.col("order_status") == "delivered")
                 .join(items.select("order_id").distinct(), "order_id", "left_anti"))
    n = bad.count()
    return (n == 0, n, "{} đơn 'delivered' không có item nào".format(n), bad)


def main():
    warn_only = "--warn-only" in sys.argv

    spark = SparkSession.builder.appName("a38-quality-gate").getOrCreate()
    spark.sparkContext.setJobDescription("A38: data quality gate")

    orders = read_typed(spark, "olist_orders_dataset.csv", ORDERS_COLS, {
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp",
    })
    items = read_typed(spark, "olist_order_items_dataset.csv", ITEMS_COLS, {
        "order_item_id": "int", "price": "double", "freight_value": "double",
        "shipping_limit_date": "timestamp",
    })
    # Cache: mỗi check là một action riêng (~12 action). Không cache = đọc lại CSV
    # 12 lần. ĐÂY là chỗ cache LÃI (A9), khác hẳn DataFrame chỉ dùng 1 lần.
    orders.cache(); orders.count()
    items.cache(); items.count()

    # (tên, mức, hàm)  — mức: "blocking" | "warning"
    CHECKS = [
        # --- BLOCKING: sai thì mọi con số tầng dưới đều vô nghĩa -------------
        ("orders.order_id không NULL", "blocking",
         lambda: check_not_null(orders, "order_id")),
        ("orders.order_id UNIQUE", "blocking",
         lambda: check_unique(orders, "order_id")),
        ("orders.order_status ∈ 8 giá trị hợp lệ", "blocking",
         lambda: check_in_set(orders, "order_status", VALID_STATUS)),
        ("items.price >= 0", "blocking",
         lambda: check_non_negative(items, "price")),
        ("items.freight_value >= 0", "blocking",
         lambda: check_non_negative(items, "freight_value")),
        ("items.order_id không mồ côi (FK -> orders)", "blocking",
         lambda: check_orphan_items(orders, items)),
        ("orders.order_purchase_timestamp cast không chết", "blocking",
         lambda: check_cast_survived(spark, "olist_orders_dataset.csv", ORDERS_COLS,
                                     "order_purchase_timestamp", "timestamp")),
        ("items.price cast không chết", "blocking",
         lambda: check_cast_survived(spark, "olist_order_items_dataset.csv", ITEMS_COLS,
                                     "price", "double")),
        # --- WARNING: sự thật xấu xí của nghiệp vụ, không phải lỗi pipeline ---
        ("orders.order_purchase_timestamp ∈ 2016..2018", "warning",
         lambda: check_date_range(orders, "order_purchase_timestamp", DATE_MIN, DATE_MAX)),
        ("null rate của order_delivered_customer_date < 5%", "warning",
         lambda: check_null_rate(orders, "order_delivered_customer_date",
                                 DELIVERED_NULL_RATE_MAX)),
        ("đơn 'delivered' PHẢI có ngày giao", "warning",
         lambda: check_delivered_has_date(orders)),
        ("thời gian chảy xuôi: giao >= đặt", "warning",
         lambda: check_timeline(orders)),
        ("đơn 'delivered' phải có ít nhất 1 item", "warning",
         lambda: check_delivered_has_item(orders, items)),
    ]

    results = []
    for name, level, fn in CHECKS:
        ok, n, detail, sample = fn()
        results.append((name, level, ok, n, detail, sample))

    # =======================================================================
    # BẰNG CHỨNG
    # =======================================================================
    print("\n" + "=" * 86)
    print("A38 — DATA QUALITY GATE trên Olist THẬT")
    print("=" * 86)
    print("\n| Check | Mức | Kết quả | Số dòng vi phạm | Chi tiết |")
    print("|---|---|---|---|---|")
    for name, level, ok, n, detail, _ in results:
        print("| {} | {} | {} | {:,} | {} |".format(
            name, level, "✅ PASS" if ok else "❌ FAIL", n, detail))

    fails_block = [r for r in results if r[1] == "blocking" and not r[2]]
    fails_warn = [r for r in results if r[1] == "warning" and not r[2]]

    print("\n**Tổng kết:** {}/{} check PASS · blocking FAIL = {} · warning FAIL = {}".format(
        sum(1 for r in results if r[2]), len(results), len(fails_block), len(fails_warn)))

    # In mẫu dòng vi phạm — không có mẫu thì không ai tin, và không ai sửa được.
    for name, level, ok, n, detail, sample in results:
        if ok or n == 0:
            continue
        print("\n#### Mẫu vi phạm — {} [{}]\n".format(name, level))
        try:
            sample.show(5, truncate=60)
        except Exception as e:      # noqa: BLE001 — mẫu hỏng không được giết cả gate
            print("(không in được mẫu: {})".format(e))

    print("""
### Biện luận mức blocking / warning (phần ăn điểm)

**Vì sao 8 check đầu là BLOCKING:**
- `order_id` NULL hoặc TRÙNG -> mọi phép `join` ở tầng gold sẽ NHÂN BẢN dòng. Doanh thu
  phồng lên mà không ai biết. Đây là loại lỗi làm SAI TOÀN BỘ số ở hạ nguồn, không phải
  làm thiếu vài dòng. Sai kiểu này thì "chạy được" còn TỆ HƠN "chết".
- `price < 0` -> `sum(price)` sai. Dashboard nói dối sếp.
- item MỒ CÔI -> `inner join` sẽ ÂM THẦM nuốt mất doanh thu của nó. Không exception,
  không log, chỉ là con số nhỏ đi. Loại lỗi đáng sợ nhất: nó im lặng.
- cast CHẾT -> dòng có dữ liệu ở CSV nhưng thành NULL trong bảng. `_corrupt_record`
  KHÔNG bắt được (đúng cấu trúc mà!). Đây chính xác là lỗ hổng mà A23 đã cảnh báo.

**Vì sao 5 check sau là WARNING:**
- Chúng mô tả SỰ THẬT XẤU XÍ của hệ thống NGUỒN, không phải lỗi của pipeline tôi.
  Olist là dữ liệu vận hành thật: có đơn giao xong mà quên đóng ngày giao, có đơn
  chưa giao nên `delivered_date` NULL (đúng nghiệp vụ!).
- Nếu đặt chúng ở mức blocking thì pipeline KHÔNG BAO GIỜ CHẠY ĐƯỢC NGÀY NÀO. Một cái
  gate không bao giờ mở thì người ta sẽ... tắt nó đi. Gate mất uy tín là gate vô dụng.
- Nguyên tắc: **blocking = "số sẽ SAI"; warning = "số sẽ XẤU nhưng ĐÚNG"**.

### Bài học chốt (nối A23 -> A38)

`_corrupt_record` là hàng rào **cấu trúc**: nó hỏi "dòng này có đúng số cột không?".
Data quality gate là hàng rào **ngữ nghĩa**: nó hỏi "dòng này có VÔ LÝ không?".
Một dòng `price = -999` đi qua hàng rào thứ nhất mà không hề hấn gì.
Không có hàng rào thứ hai thì bạn có một pipeline chạy xanh mướt và một cái dashboard nói dối.
""")

    spark.stop()

    if fails_block and not warn_only:
        print("\n>>> GATE ĐÓNG. {} check BLOCKING thất bại -> KHÔNG ghi silver. exit(1)".format(
            len(fails_block)))
        print(">>> (Đây là hành vi CÓ CHỦ ĐÍCH của đề bài, không phải script hỏng.)")
        raise SystemExit(1)

    if fails_block and warn_only:
        print("\n>>> Có {} blocking FAIL nhưng chạy với --warn-only -> exit(0) để hứng log."
              .format(len(fails_block)))
    else:
        print("\n>>> GATE MỞ. Mọi check blocking PASS -> được phép ghi silver. exit(0)")


if __name__ == "__main__":
    main()
