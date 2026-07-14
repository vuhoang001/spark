"""A32 — sortWithinPartitions: mồi cho min/max statistics -> BỎ QUA CẢ ROW GROUP.

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a32_sort_within_partitions.py

PHỤ THUỘC: không (đọc thẳng order_items CSV — bảng DUY NHẤT có cột `price` từng dòng).

CƠ CHẾ (lesson 6): mỗi row group của Parquet lưu min/max của TỪNG CỘT trong footer.
Reader nhận filter `price > 1500`, so với max của row group:
    max(price) của row group = 90  ->  90 > 1500 SAI  ->  BỎ QUA cả row group, không mở byte nào.
Dữ liệu KHÔNG sort: giá trị rải ngẫu nhiên -> mọi row group đều có min≈1 max≈6735
-> không row group nào bị loại -> đọc hết. Sort rồi: min/max mỗi row group HẸP lại
-> chỉ vài row group cuối (giá cao) sống sót.

⚠️ CÁI BẪY GIẾT CẢ THÍ NGHIỆM (đề bài KHÔNG nói):
    Row group mặc định = 128 MB. order_items chỉ ~15MB CSV -> ~2-3MB Parquet
    -> TOÀN BỘ dữ liệu nằm trong ĐÚNG 1 ROW GROUP. Một row group thì không có gì để
    "bỏ qua" — sort hay không sort, kết quả bytes Y HỆT NHAU, và bạn sẽ kết luận sai là
    "sortWithinPartitions vô dụng".
    => Ta chạy 2 cấu hình: block mặc định (128MB, chứng minh bẫy là có thật) và
       block 512KB (ép ra nhiều row group, thí nghiệm mới có ý nghĩa).
    Bài học thật: sort chỉ đáng tiền khi file ĐỦ LỚN để có nhiều row group. Ở dữ liệu
    ×100 (A40) thì điều kiện đó tự thoả.

CÁCH ĐỌC KẾT QUẢ: nhìn 2 con số
    - `input bytes`      : byte thật đọc từ đĩa (row group bị skip -> không tính vào đây)
    - `rows scanned`     : số dòng node Scan NHẢ RA. Có pushdown + skip -> nhỏ hơn hẳn
                           tổng số dòng. Đây là bằng chứng trực quan nhất.
    (`size of files read` KHÔNG dùng được: nó là size FILE, skip row group không đổi nó.)
"""

import sys

sys.path.insert(0, "/workspace/labs/mini-project-1/src")
import benchmark as B  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

OUT = f"{B.BENCH}/a32"
THRESHOLD = 1500.0  # price > 1500: hiếm (334/112.650 dòng ~ 0.3%) -> đúng dạng query
#                     mà row-group skipping sinh ra để phục vụ.
BLOCK_SMALL = 512 * 1024  # 512KB/row group -> ép ra nhiều row group trên dữ liệu bé


def rowgroup_report(path):
    """Hỏi thẳng file: mấy row group, min/max price mỗi cái. Đây là 'sự thật vật lý'
    để đối chiếu với metric của Spark."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None, []
    files = sorted(B.list_part_files(path))
    total_rg, ranges = 0, []
    for f in files:
        m = pq.ParquetFile(f).metadata
        total_rg += m.num_row_groups
        for g in range(m.num_row_groups):
            rg = m.row_group(g)
            for i in range(rg.num_columns):
                c = rg.column(i)
                if c.path_in_schema == "price" and c.statistics:
                    ranges.append((c.statistics.min, c.statistics.max, rg.num_rows))
    return total_rg, ranges


def main():
    spark = B.new_spark("a32-sort-within-partitions")
    items = B.read_items_csv(spark).repartition(1).cache()
    # repartition(1): thí nghiệm có kiểm soát. Nhiều partition -> mỗi file sort riêng,
    # dải giá vẫn chồng lấn -> khó quy kết. 1 file = sort toàn cục = tín hiệu rõ nhất.
    n = items.count()

    B.section("A32 — CHUẨN BỊ")
    hi = items.where(F.col("price") > THRESHOLD).count()
    print(f"order_items: {n:,} dòng | price > {THRESHOLD:.0f}: {hi} dòng "
          f"({100 * hi / n:.2f}% — đủ hiếm để skip có ý nghĩa)")
    print(f"parquet.filterPushdown = {spark.conf.get('spark.sql.parquet.filterPushdown')} "
          "(phải là true, không thì reader không nhận được filter -> không skip được gì)\n")

    variants = [
        ("unsorted / block 128MB (mặc định)", "u_default", False, None),
        ("SORTED   / block 128MB (mặc định)", "s_default", True, None),
        ("unsorted / block 512KB", "u_small", False, BLOCK_SMALL),
        ("SORTED   / block 512KB", "s_small", True, BLOCK_SMALL),
    ]

    rows = []
    for label, name, do_sort, block in variants:
        path = f"{OUT}/{name}"
        w = items.sortWithinPartitions("price") if do_sort else items
        writer = w.write.mode("overwrite").option("compression", "snappy")
        if block:
            # parquet.block.size = kích thước row group (byte). Truyền qua option ->
            # Spark đẩy xuống Hadoop conf của ParquetOutputFormat.
            writer = writer.option("parquet.block.size", str(block))
        writer.parquet(path)

        n_rg, ranges = rowgroup_report(path)

        def q(p=path):
            return spark.read.parquet(p).where(F.col("price") > THRESHOLD).count()

        best, times = B.timeit(q, runs=3, label=label)
        with B.Probe(spark, label) as pr:
            got = q()

        rows.append([
            label,
            B.count_part_files(path),
            n_rg if n_rg is not None else "? (thiếu pyarrow)",
            B.human(B.du_bytes(path)),
            B.human(pr.files_size),
            B.human(pr.input_bytes),
            f"{pr.input_bytes:,}" if pr.input_bytes else "?",
            f"{pr.rows_scanned:,}" if pr.rows_scanned else "?",
            got,
            f"{best:.2f}s",
        ])

    B.section("BẢNG A32 — SORT vs KHÔNG SORT (query: price > 1500, action = count)")
    B.md(["biến thể", "file", "row group", "size trên đĩa", "size of files read",
          "input bytes", "input bytes (số)", "rows scanned (Scan nhả ra)",
          "kết quả count", "min lần 2-3"], rows)

    print("ĐỌC BẢNG:")
    print(f"  - Cột 'kết quả count' PHẢI giống nhau cả 4 dòng (= {hi}). Khác nhau = code sai,")
    print("    không phải phát hiện khoa học.")
    print("  - So 2 dòng 'block 128MB': row group = 1 -> input bytes GẦN NHƯ Y HỆT dù đã sort.")
    print("    Đó KHÔNG phải sort vô dụng — đó là 'không có gì để bỏ qua'. (bẫy ở docstring)")
    print("  - So 2 dòng 'block 512KB': đây mới là thí nghiệm thật. sorted đọc ít byte hơn")
    print("    và 'rows scanned' tụt mạnh -> reader đã BỎ QUA cả row group nhờ min/max.")

    B.section("A32 — MIN/MAX CỦA TỪNG ROW GROUP (bằng chứng vật lý, đọc từ footer)")
    for label, name, _s, _b in variants:
        if not name.endswith("small"):
            continue
        n_rg, ranges = rowgroup_report(f"{OUT}/{name}")
        if not ranges:
            print(f"{label}: không đọc được (thiếu pyarrow: "
                  "docker exec spark-mastery-spark-submit-1 pip install pyarrow)")
            continue
        surv = sum(1 for mn, mx, _r in ranges if mx is not None and mx > THRESHOLD)
        print(f"\n{label} — {n_rg} row group. "
              f"Số row group có max(price) > {THRESHOLD:.0f} (tức PHẢI đọc): {surv}/{n_rg}"
              f"  -> lý thuyết bỏ qua được {n_rg - surv}/{n_rg} row group")
        for i, (mn, mx, r) in enumerate(ranges[:12]):
            keep = "ĐỌC " if (mx is not None and mx > THRESHOLD) else "skip"
            print(f"   rg[{i:2d}] rows={r:6,}  min={mn:>9}  max={mx:>9}  -> {keep}")
        if len(ranges) > 12:
            print(f"   ... ({len(ranges) - 12} row group nữa)")

    B.section("A32 — ĐÁNH ĐỔI PHẢI GHI VÀO REPORT")
    print("Chỉ sort được theo MỘT chiều. Sort theo `price` = đánh cược rằng query hay lọc")
    print("theo price. Nếu thực tế query lọc theo `product_id` hay `seller_id` thì công sort")
    print("này VÔ ÍCH (min/max của product_id trong mỗi row group vẫn phủ toàn dải).")
    print("Cái giá phải trả: sortWithinPartitions thêm 1 bước sort (CPU + spill) lúc GHI.")
    print("=> Chỉ sort khi biết rõ query pattern. Ghi vào report: 'tôi sort theo ___ vì")
    print("   query chính của downstream lọc theo ___'.")
    print("Ghi chú: sort KHÔNG gây shuffle (sortWithinPartitions sort trong từng partition),")
    print("khác orderBy() — cái đó shuffle toàn cục, đắt hơn nhiều.")
    spark.stop()


if __name__ == "__main__":
    main()
