"""A36 — PARTITION PRUNING: bằng chứng cuối cùng, VÀ 3 CÁCH PHÁ NÓ.

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a36_partition_pruning.py

PHỤ THUỘC: cần một bảng Parquet partitionBy(order_date). Script tự dò theo thứ tự:
    1. data/output/silver/orders_clean  (Checkpoint 2)
    2. data/bench/cp3/orders_parquet    (src/benchmark.py)
    3. data/bench/a35/fix               (A35)
    ... không có cái nào -> tự dựng data/bench/a36/orders (mất ~30s).

-------------------------------------------------------------------------------
TRỌNG TÂM: partition pruning KHÔNG PHẢI PHÉP MÀU TỰ ĐỘNG.
Nó chỉ chạy khi filter là BIỂU THỨC ĐƠN GIẢN TRÊN CHÍNH CỘT PARTITION.
Vì sao? Vì Spark prune ở giai đoạn LIỆT KÊ FILE — trước khi mở byte nào, nó đọc TÊN
THƯ MỤC (`order_date=2018-07-02`), parse ra giá trị, rồi so với predicate. Nó chỉ làm
được điều đó nếu predicate còn nhìn thấy cột partition ở dạng "thô".
Bọc cột partition vào MỘT HÀM (date_format, cast, UDF, substring...) -> Spark không
biết hàm đó ánh xạ thế nào -> nó buộc phải ĐỌC HẾT rồi mới lọc.
=> Đây là lỗi #1 khiến pipeline production chậm 100× mà không ai hiểu vì sao.
   Job vẫn chạy, kết quả vẫn ĐÚNG, chỉ là đọc 600 file thay vì 1. Không có cảnh báo nào.
-------------------------------------------------------------------------------

DỰ ĐOÁN TRƯỚC: files read của (1) filter thẳng ___ (2) bọc date_format ___ (3) UDF ___
"""

import sys

sys.path.insert(0, "/workspace/labs/mini-project-1/src")
import benchmark as B  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import BooleanType  # noqa: E402

DAY = "2018-07-02"
CANDIDATES = [B.SILVER, f"{B.BENCH}/cp3/orders_parquet", f"{B.BENCH}/a35/fix"]
FALLBACK = f"{B.BENCH}/a36/orders"


def _has_price(path):
    """Bảng ứng viên phải CÓ SẴN cột `price`.

    ⚠️ BẪY ĐÃ DÍNH: silver/orders_clean của nhóm Checkpoint 1+2 KHÔNG có cột `price`
    (price nằm ở order_items — xem docstring benchmark.orders_enriched). A36 đọc bảng
    THẲNG bằng spark.read.parquet(path) rồi agg(sum("price")) -> chọn nhầm silver là
    AnalysisException ngay.
    Vì sao không join price vào cho xong? Vì join đẻ thêm MỘT node Scan nữa (bảng items)
    -> `files read` / `input bytes` của probe sẽ cộng cả file của items vào => hỏng đúng
    con số mà bài này cần đo. A36 phải đọc MỘT bảng, MỘT scan. Nên: chỉ nhận bảng đã có
    sẵn price (cp3 do benchmark.py đẻ ra), không có thì tự dựng FALLBACK."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return True  # không kiểm được thì cứ thử
    files = B.list_part_files(path)
    if not files:
        return False
    return "price" in pq.ParquetFile(files[0]).schema_arrow.names


def ensure_table(spark):
    for c in CANDIDATES:
        if B.count_part_files(c) > 0 and B.count_dirs(c) > 0:
            if not _has_price(c):
                print(f"[a36] BỎ QUA {c}: có partition nhưng KHÔNG có cột `price` "
                      f"(bảng orders trần) -> không dùng cho query doanh thu được.")
                continue
            return c
    print(f"[a36] chưa có bảng partition nào có `price` -> dựng {FALLBACK}")
    df, _ = B.load_orders_clean(spark)
    (df.where(F.col("order_date").isNotNull())
       .repartition("order_date")            # 1 file/ngày — bài học từ A35
       .write.mode("overwrite").partitionBy("order_date").parquet(FALLBACK))
    return FALLBACK


def main():
    spark = B.new_spark("a36-partition-pruning")
    path = ensure_table(spark)

    total_files = B.count_part_files(path)
    total_dirs = B.count_dirs(path)
    total_size = B.du_bytes(path)
    df = spark.read.parquet(path)

    B.section("A36 — BẢNG ĐEM RA MỔ")
    print(f"path        : {path}")
    print(f"tổng số file: {total_files:,}   (đây là mẫu số — mọi con số 'files read' phải so với nó)")
    print(f"tổng partition (thư mục order_date=): {total_dirs}")
    print(f"tổng size   : {B.human(total_size)}")
    print(f"ngày thử    : {DAY}\n")

    # UDF: hộp đen với optimizer. Spark KHÔNG nhìn được vào trong python function
    # -> không đẩy xuống được, không prune được. Đây là 'cách phá' tàn bạo nhất.
    is_day = F.udf(lambda d: d is not None and str(d) == DAY, BooleanType())

    variants = [
        ("A. filter THẲNG cột partition  (order_date == '2018-07-02')",
         lambda: df.where(F.col("order_date") == F.lit(DAY)).agg(F.sum("price")),
         "PRUNE ĐƯỢC"),
        ("B. bọc date_format(order_date)  == '2018-07-02'",
         lambda: df.where(F.date_format("order_date", "yyyy-MM-dd") == DAY).agg(F.sum("price")),
         "PHÁ: cột bị bọc trong hàm"),
        ("C. cast sang string rồi so sánh",
         lambda: df.where(F.col("order_date").cast("string") == DAY).agg(F.sum("price")),
         "PHÁ (hoặc không — xem kết quả)"),
        ("D. UDF python is_day(order_date)",
         lambda: df.where(is_day(F.col("order_date"))).agg(F.sum("price")),
         "PHÁ: UDF là hộp đen"),
        ("E. filter theo cột THƯỜNG (order_purchase_timestamp) — đối chứng",
         lambda: df.where(F.to_date("order_purchase_timestamp") == F.lit(DAY)).agg(F.sum("price")),
         "không prune được (không phải cột partition)"),
    ]

    rows, plans = [], []
    for label, build, expect in variants:
        q = build()
        plan = B.plan_text(q)
        pf = B.grep_plan(plan, "PartitionFilters")
        pf_line = pf[0] if pf else "(không có dòng PartitionFilters)"
        has_pf = "PartitionFilters: []" not in pf_line and bool(pf)

        def act(qq=q):
            return qq.collect()

        best, times = B.timeit(act, runs=3, label=label[:28])
        with B.Probe(spark, label[:30]) as p:
            res = act()

        val = res[0][0] if res and res[0][0] is not None else None
        rows.append([
            label,
            "CÓ" if has_pf else "KHÔNG",
            f"{p.files:,}" if p.files is not None else "?",
            f"{total_files:,}",
            f"{100 * p.files / max(total_files, 1):.1f}%" if p.files is not None else "?",
            B.human(p.files_size),
            B.human(p.input_bytes),
            f"{best:.2f}s",
            f"{val:,.2f}" if val is not None else "NULL",
        ])
        plans.append((label, expect, pf_line, B.grep_plan(plan, "PushedFilters")))

    B.section("BẢNG A36 — 5 CÁCH VIẾT FILTER, CÙNG MỘT KẾT QUẢ, KHÁC NHAU 100× I/O")
    B.md(["cách viết filter", "PartitionFilters?", "files read", "tổng file",
          "% file phải đọc", "size of files read", "input bytes", "min lần 2-3",
          "sum(price) — PHẢI GIỐNG NHAU"], rows)
    print("⚠️ Cột cuối PHẢI giống hệt nhau ở mọi dòng. Nếu khác -> filter viết sai, không phải")
    print("   phát hiện về pruning. (Trừ dòng E: nó lọc theo timestamp mua hàng, về lý thuyết")
    print("   cùng ngày -> cùng kết quả; lệch nhau nghĩa là order_date đã bị derive khác đi.)\n")

    B.section("HAI PLAN ĐẶT CẠNH NHAU — CHỖ ĐỂ KHOANH BÚT ĐỎ")
    for label, expect, pf_line, pushed in plans:
        print(f"\n### {label}   [dự kiến: {expect}]")
        print(f"    {pf_line[:260]}")
        for x in pushed[:1]:
            print(f"    {x[:260]}")

    B.section("A36 — KẾT LUẬN")
    a, b = rows[0], rows[1]
    print(f"Filter thẳng : {a[2]} / {a[3]} file  ({a[4]})  — Spark loại thư mục ngay lúc LIỆT KÊ FILE.")
    print(f"Bọc hàm      : {b[2]} / {b[3]} file  ({b[4]})  — đọc sạch, rồi mới vứt 99.8% dữ liệu đi.")
    print()
    print("BA TẦNG LỌC, phải phân biệt được (nhìn dòng FileScan trong plan):")
    print("  1. PartitionFilters : loại nguyên THƯ MỤC. Rẻ nhất — không mở file nào.")
    print("  2. PushedFilters    : đẩy xuống ĐẦU ĐỌC PARQUET -> bỏ qua row group nhờ min/max (A32).")
    print("                        Đã mở file, nhưng không đọc hết nội dung.")
    print("  3. Filter (node riêng): đọc HẾT lên RAM rồi mới lọc. Đắt nhất.")
    print("  Filter viết sai không LỖI — nó chỉ lặng lẽ tụt từ tầng 1 xuống tầng 3.")
    print()
    print("QUY TẮC SỐNG CÒN:")
    print("  - Filter trên cột partition: để CỘT TRẦN một bên, HẰNG SỐ bên kia.")
    print("      ĐÚNG : order_date = '2018-07-02'   |  order_date BETWEEN a AND b")
    print("      ĐÚNG : order_date >= '2018-07-01' AND order_date < '2018-08-01'  (lọc theo tháng)")
    print("      SAI  : date_format(order_date,'yyyy-MM') = '2018-07'   <- bọc hàm, mất prune")
    print("      SAI  : year(order_date) = 2018                         <- bọc hàm, mất prune")
    print("      SAI  : udf(order_date)                                 <- hộp đen tuyệt đối")
    print("  - Hay lọc theo tháng? -> ĐỪNG bọc hàm để lách; hãy PARTITION THEO THÁNG ngay từ")
    print("    lúc ghi (thêm cột order_month), hoặc viết lại thành khoảng ngày như trên.")
    print("  - Cách kiểm tra 30 giây, làm TRƯỚC KHI merge code: explain() -> tìm dòng FileScan ->")
    print("    PartitionFilters có rỗng [] không? Rỗng = bạn vừa mất 100× tốc độ.")
    spark.stop()


if __name__ == "__main__":
    main()
