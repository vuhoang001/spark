"""A34 — SCHEMA EVOLUTION: Parquet trần chịu được kiểu thay đổi nào, gãy ở đâu?

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a34_schema_evolution.py

PHỤ THUỘC: không. Tự dựng dữ liệu ở /workspace/data/bench/a34/{x1,x2,x3}.

4 KỊCH BẢN (mỗi kịch bản một thư mục RIÊNG — nếu dùng chung 1 thư mục thì kịch bản
sau bị nhiễm schema của kịch bản trước, không quy kết được lỗi cho ai):
    x1: base(N cột) rồi APPEND thêm 1 cột mới  (order_priority)
    x2: base(N cột) rồi APPEND thiếu 1 cột     (bỏ order_status)
    x3: base(N cột) rồi APPEND đổi kiểu 1 cột  (price: Double -> String)
    + với mỗi cái: đọc MẶC ĐỊNH vs đọc mergeSchema=true

⚠️ BẪY LỚN NHẤT — "đọc mặc định" là một TRÒ MAY RỦI:
   Không bật mergeSchema, Spark lấy schema từ MỘT file bất kỳ trong thư mục
   (thực tế: file đầu tiên nó liệt kê được — phụ thuộc thứ tự filesystem trả về,
   KHÔNG có bảo đảm nào). Nên cùng một thư mục, hôm nay đọc thấy cột mới, mai không.
   Đó chính là lý do người ta gọi "Parquet trần + thư mục" là một cái bảng GIẢ:
   nó không có metadata layer -> không có schema chính thức. Iceberg/Delta (module 5)
   sinh ra để vá đúng lỗ này: schema nằm trong metadata của BẢNG, không nằm trong file.
   => Script chạy lại nhiều lần có thể ra kết quả khác nhau ở cột "đọc mặc định".
      KHÔNG phải script sai. Đó LÀ phát hiện. Ghi vào report đúng như thế.

⚠️ BẪY 2: mergeSchema là thao tác ĐẮT — Spark phải mở FOOTER của MỌI file để hợp nhất
   schema. 10.000 file small-files (A35) + mergeSchema = ngồi chờ dài cổ. Đó là lý do
   spark.sql.parquet.mergeSchema mặc định = false.
"""

import sys
import traceback

sys.path.insert(0, "/workspace/labs/mini-project-1/src")
import benchmark as B  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

OUT = f"{B.BENCH}/a34"


def try_read(spark, path, merge):
    """Trả (mô tả kết quả, danh sách cột, số dòng) — hoặc lỗi (nói thật, không nuốt)."""
    try:
        r = spark.read
        if merge:
            r = r.option("mergeSchema", "true")
        df = r.parquet(path)
        cols = df.columns
        n = df.count()  # ép materialize: lỗi type mismatch nhiều khi chỉ nổ lúc ĐỌC DỮ LIỆU,
        #                 không nổ lúc suy luận schema. count() bắt cả hai loại.
        return "OK", cols, n, None
    except Exception as e:  # noqa: BLE001
        first = str(e).strip().split("\n")[0][:180]
        return "LỖI", [], None, f"{type(e).__name__}: {first}"


def main():
    spark = B.new_spark("a34-schema-evolution")
    df, note = B.load_orders_clean(spark)
    # Chỉ giữ vài cột cho dễ nhìn. `price` là DoubleType -> dùng để thử đổi kiểu.
    base = df.select("order_id", "order_status", "order_date", "price").repartition(2).cache()
    n_base = base.count()

    B.section("A34 — CHUẨN BỊ")
    print(f"Nguồn: {note}")
    print(f"base = {n_base:,} dòng, cột: {base.columns} (price: double)")
    print("Mỗi kịch bản: ghi base (overwrite) -> APPEND một biến thể -> đọc 2 kiểu.\n")

    results = []

    # ---------------------------------------------------------------- x1: THÊM cột
    p1 = f"{OUT}/x1_add_col"
    base.write.mode("overwrite").parquet(p1)
    add = base.limit(1000).withColumn("order_priority", F.lit("HIGH"))
    add.write.mode("append").parquet(p1)   # file mới có 5 cột, file cũ 4 cột
    for merge in (False, True):
        st, cols, n, err = try_read(spark, p1, merge)
        results.append([
            "x1. THÊM cột (order_priority)",
            "mergeSchema=true" if merge else "mặc định",
            st,
            "có" if "order_priority" in cols else "KHÔNG",
            f"{n:,}" if n is not None else "—",
            err or ("giá trị ở file CŨ = NULL" if merge and "order_priority" in cols else ""),
        ])
    # Kiểm chứng câu "file cũ = NULL": đếm thẳng
    try:
        d = spark.read.option("mergeSchema", "true").parquet(p1)
        nulls = d.where(F.col("order_priority").isNull()).count()
        nn = d.where(F.col("order_priority").isNotNull()).count()
        x1_note = (f"mergeSchema: {nn:,} dòng có order_priority='HIGH' (file mới), "
                   f"{nulls:,} dòng NULL (file cũ — Parquet KHÔNG viết lại file cũ, "
                   f"reader tự điền NULL cho cột thiếu).")
    except Exception as e:  # noqa: BLE001
        x1_note = f"CHẠY LỖI khi kiểm chứng x1: {type(e).__name__}: {e}"

    # ---------------------------------------------------------------- x2: THIẾU cột
    p2 = f"{OUT}/x2_drop_col"
    base.write.mode("overwrite").parquet(p2)
    base.limit(1000).drop("order_status").write.mode("append").parquet(p2)
    for merge in (False, True):
        st, cols, n, err = try_read(spark, p2, merge)
        results.append([
            "x2. THIẾU cột (bỏ order_status)",
            "mergeSchema=true" if merge else "mặc định",
            st,
            "có" if "order_status" in cols else "KHÔNG",
            f"{n:,}" if n is not None else "—",
            err or "dòng của file mới -> order_status = NULL",
        ])

    # ---------------------------------------------------------------- x3: ĐỔI KIỂU
    p3 = f"{OUT}/x3_type_change"
    base.write.mode("overwrite").parquet(p3)
    changed = base.limit(1000).withColumn("price", F.col("price").cast("string"))
    t3_write = "OK"
    try:
        # LƯU Ý: append KHÔNG bị chặn ở đây! Parquet trần không kiểm tra schema khi ghi
        # (không có metadata layer nào để kiểm). Bom nổ chậm — chỉ nổ lúc ĐỌC.
        changed.write.mode("append").parquet(p3)
    except Exception as e:  # noqa: BLE001
        t3_write = f"LỖI NGAY LÚC GHI: {type(e).__name__}: {str(e).splitlines()[0][:150]}"
    for merge in (False, True):
        st, cols, n, err = try_read(spark, p3, merge)
        results.append([
            "x3. ĐỔI KIỂU (price double->string)",
            "mergeSchema=true" if merge else "mặc định",
            st,
            "có" if "price" in cols else "KHÔNG",
            f"{n:,}" if n is not None else "—",
            err or "đọc được (schema lấy từ 1 file — xem cảnh báo bên dưới)",
        ])

    B.section("BẢNG A34 — 4 KỊCH BẢN × 2 CÁCH ĐỌC")
    B.md(["kịch bản", "cách đọc", "đọc được?", "thấy cột thay đổi?", "số dòng", "ghi chú / lỗi"],
         results)

    B.section("A34 — DIỄN GIẢI")
    print(x1_note)
    print(f"\nx3 — lúc GHI append kiểu lệch: {t3_write}")
    print("   Ghi KHÔNG lỗi = điều đáng sợ nhất của Parquet trần: dữ liệu hỏng chui được vào")
    print("   bảng, không ai chặn. Lỗi chỉ nổ ở người ĐỌC, có thể là 3 tuần sau, ở team khác.")
    print("\nQUY LUẬT RÚT RA (Parquet trần + thư mục):")
    print("  THÊM cột  -> CHỊU ĐƯỢC. mergeSchema=true đọc ra đủ; file cũ điền NULL. An toàn.")
    print("  BỎ cột    -> CHỊU ĐƯỢC. Cột thiếu ở file mới = NULL. Nhưng SILENT: không ai báo")
    print("               cho bạn biết từ hôm nay cột đó toàn NULL -> hỏng số liệu, không hỏng job.")
    print("  ĐỔI KIỂU  -> GÃY. Không hợp nhất được double với string.")
    print("               (int -> long thì merge được; double -> string thì KHÔNG.")
    print("                Quy luật: Spark chỉ merge được khi có kiểu 'rộng hơn' chứa được cả hai.)")
    print("  ĐỔI TÊN cột = BỎ cột cũ + THÊM cột mới -> dữ liệu cũ mất tăm dưới tên mới. Câm lặng.")
    print("\n=> ĐÂY LÀ CHỖ ICEBERG (module 5) SINH RA ĐỂ CỨU:")
    print("   - schema nằm trong METADATA CỦA BẢNG, không phải suy ra từ file nào đó ngẫu nhiên")
    print("     -> đọc mặc định không còn là trò may rủi.")
    print("   - mỗi cột có FIELD ID -> đổi tên cột là thao tác metadata, dữ liệu cũ vẫn đọc được.")
    print("   - ALTER TABLE có kiểm tra tương thích -> append lệch kiểu bị CHẶN NGAY LÚC GHI,")
    print("     không thành bom nổ chậm.")
    spark.stop()


if __name__ == "__main__":
    main()
