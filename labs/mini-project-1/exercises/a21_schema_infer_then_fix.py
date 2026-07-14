"""A21 — Sinh schema bằng inferSchema (MỘT LẦN ở dev) rồi sửa tay.

Chạy:
    make run-local F=labs/mini-project-1/exercises/a21_schema_infer_then_fix.py
    (local là đủ: bài này đọc 3 file nhỏ, không cần cluster. Chạy cluster cũng được.)

Output: 3 phần dán thẳng vào PROGRESS/report:
    PHẦN 1 — inferSchema đoán gì (treeString thật, chạy thật)
    PHẦN 2 — bảng 3 cột "Spark đoán / tôi sửa / vì sao"  (>= 5 cột bị sửa)
    PHẦN 3 — thí nghiệm nullable=False: lời hứa mà Spark KHÔNG kiểm tra

Ý chính: đây là file DUY NHẤT trong project được phép gọi inferSchema, và nó
KHÔNG phải code pipeline — nó là công cụ sinh mã. Kết quả (schema đã sửa tay)
đã đóng băng trong src/schemas.py; pipeline chỉ ăn cái đã đóng băng.
"""
import os
import sys
import time

# Cho phép import src/schemas.py. Ở cluster mode (deploy-mode client) driver chạy
# trong container spark-submit, /workspace mount đủ cả repo -> đường dẫn này vẫn đúng.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from schemas import (  # noqa: E402
    CUSTOMERS,
    FIX_TABLE,
    NULLABLE_STRICT_ORDERS,
    ORDER_ITEMS,
    ORDERS,
)

SRC = "/workspace/data/olist"
FILES = [
    ("orders", f"{SRC}/olist_orders_dataset.csv", ORDERS),
    ("order_items", f"{SRC}/olist_order_items_dataset.csv", ORDER_ITEMS),
    ("customers", f"{SRC}/olist_customers_dataset.csv", CUSTOMERS),
]


def hr(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def main():
    spark = SparkSession.builder.appName("a21-schema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # =====================================================================
    # PHẦN 1 — chạy inferSchema THẬT, một lần, và bấm giờ nó
    # =====================================================================
    # Bấm giờ để thấy inferSchema KHÔNG miễn phí: nó là action trá hình
    # (Spark quét file để đoán kiểu) trong khi read() lẽ ra phải lazy.
    hr("PHẦN 1 — inferSchema đoán gì? (chạy thật, 1 lần, ở dev)")
    for name, path, _ in FILES:
        t0 = time.time()
        df_infer = (spark.read
                    .option("header", True)
                    .option("inferSchema", True)   # <-- CHỖ DUY NHẤT trong project được dùng
                    .csv(path))
        schema_infer = df_infer.schema           # .schema đã đủ trigger việc đoán
        t_infer = time.time() - t0

        t0 = time.time()
        _ = (spark.read.schema(ORDERS if name == "orders" else
                               (ORDER_ITEMS if name == "order_items" else CUSTOMERS))
             .option("header", True).csv(path)).schema
        t_explicit = time.time() - t0

        print(f"\n--- {name}  ({path})")
        print(f"    thời gian lấy schema: inferSchema = {t_infer*1000:8.1f} ms"
              f"  |  schema tường minh = {t_explicit*1000:6.1f} ms")
        print("    (chênh lệch này = số tiền bạn trả cho sự lười biếng. "
              "Nó là I/O THẬT, xảy ra ngay cả khi bạn chưa gọi action nào.)")
        print("    treeString mà inferSchema đoán ra:")
        for line in df_infer._jdf.schema().treeString().splitlines():
            print("      " + line)

    # =====================================================================
    # PHẦN 2 — bảng "Spark đoán / tôi sửa / vì sao"
    # =====================================================================
    hr("PHẦN 2 — Bảng sửa tay (dán thẳng vào report)")
    print()
    print("| Bảng | Cột | Spark (inferSchema) đoán | Tôi sửa thành | Vì sao |")
    print("|---|---|---|---|---|")
    for tbl, col, guess, fixed, why in FIX_TABLE:
        print(f"| {tbl} | `{col}` | {guess} | **{fixed}** | {why} |")
    print(f"\n=> {len(FIX_TABLE)} cột được xem xét/sửa (đề yêu cầu >= 5).")

    # =====================================================================
    # PHẦN 3 — nullable=False là LỜI HỨA, không phải KIỂM TRA
    # =====================================================================
    hr("PHẦN 3 — Thí nghiệm nullable=False (cái bẫy đáng giá nhất bài này)")
    orders_path = f"{SRC}/olist_orders_dataset.csv"

    # 3a. Đọc bằng schema THẬT THÀ (nullable=True) -> đếm null thật của cột
    honest = (spark.read.schema(ORDERS).option("header", True)
              .option("mode", "PERMISSIVE").csv(orders_path))
    n_null_honest = honest.filter(F.col("order_delivered_customer_date").isNull()).count()
    n_total = honest.count()
    print(f"\n[3a] Schema thật thà (nullable=True):")
    print(f"     tổng dòng                                  = {n_total}")
    print(f"     order_delivered_customer_date IS NULL      = {n_null_honest}")
    print("     -> cột này CÓ null thật (đơn chưa giao xong).")

    # 3b. Đọc CÙNG file bằng schema NÓI DỐI (nullable=False cho đúng cột đó)
    #     Câu hỏi: Spark có ném lỗi không? Có drop dòng không? Có cảnh báo không?
    liar = (spark.read.schema(NULLABLE_STRICT_ORDERS).option("header", True)
            .option("mode", "PERMISSIVE").csv(orders_path))
    print("\n[3b] Schema NÓI DỐI (nullable=False cho order_delivered_customer_date):")
    print("     schema Spark ghi nhận (nullable của cột thứ 7):")
    fld = liar.schema["order_delivered_customer_date"]
    print(f"       {fld.name}: {fld.dataType.simpleString()}, nullable={fld.nullable}")
    try:
        n_null_liar = liar.filter(F.col("order_delivered_customer_date").isNull()).count()
        n_total_liar = liar.count()
        print(f"     count() chạy được, KHÔNG exception: tổng = {n_total_liar}")
        print(f"     số dòng NULL đếm được qua schema nói dối = {n_null_liar}")
        if n_null_liar == 0 and n_null_honest > 0:
            print("     *** KẾT QUẢ SAI IM LẶNG ***: Spark TIN lời hứa nullable=False,")
            print("     Catalyst tối ưu bỏ luôn phép check IS NULL -> đếm ra 0 dù dữ liệu có null.")
            print("     Không exception. Không warning. Chỉ có số sai.")
        else:
            print("     (Ghi nhận đúng như quan sát được, không suy diễn thêm.)")
    except Exception as e:  # noqa: BLE001
        print(f"     CHẠY LỖI khi filter: {type(e).__name__}: {e}")

    # 3c. Ghi ra Parquet — chỗ nullable=False hay nổ muộn nhất (nếu nó nổ)
    tmp = "/workspace/data/bench/a21_nullable_lie"
    try:
        liar.write.mode("overwrite").parquet(tmp)
        back = spark.read.parquet(tmp)
        print(f"\n[3c] Ghi Parquet bằng schema nói dối: GHI ĐƯỢC, {back.count()} dòng.")
        print(f"     nullable của cột sau khi đọc lại từ Parquet: "
              f"{back.schema['order_delivered_customer_date'].nullable}")
        print(f"     số NULL đọc lại được từ Parquet: "
              f"{back.filter(F.col('order_delivered_customer_date').isNull()).count()}")
    except Exception as e:  # noqa: BLE001
        print(f"\n[3c] CHẠY LỖI khi ghi Parquet: {type(e).__name__}: {e}")

    print("\nBÀI HỌC: nullable=False không phải một RÀNG BUỘC (constraint) như trong SQL.")
    print("Nó là một LỜI HỨA gửi cho optimizer. Spark không kiểm tra, chỉ tin và tối ưu theo.")
    print("Ràng buộc thật phải viết bằng một CHECK thật -> đó là data quality gate (A38).")

    spark.stop()


if __name__ == "__main__":
    main()
