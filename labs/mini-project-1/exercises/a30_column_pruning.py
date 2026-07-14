"""A30 — COLUMN PRUNING: đo bằng BYTES, không bằng GIÂY.

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a30_column_pruning.py
(local[2] là đủ và ổn định hơn. Cluster cũng chạy được nhưng không đổi kết luận:
 bytes đọc là thuộc tính của FILE + PLAN, không phải của số executor.)

PHỤ THUỘC: không. Script tự dựng Parquet nguồn ở /workspace/data/bench/a30/orders/.

-------------------------------------------------------------------------------
DỰ ĐOÁN TRƯỚC KHI CHẠY (viết ra trước, sai mới học được):
  - count()            đọc bao nhiêu byte? ____
  - đọc đủ 11 cột      đọc bao nhiêu byte? ____
  - chỉ đọc cột price  đọc bao nhiêu byte? ____   tỉ lệ so với 11 cột? ____
-------------------------------------------------------------------------------

BA CÁI BẪY CỦA BÀI NÀY (không biết là ra số vô nghĩa):

BẪY 1 — `select("*").count()` KHÔNG đọc hết cột.
   Đề bài bảo so `select("*").count()` với `select("price").agg(sum)`. Nhưng
   optimizer thấy count() không cần giá trị cột nào -> prune xuống 0 cột, chỉ đọc
   số dòng trong FOOTER. Nên query đó đọc ÍT byte hơn cả query 1 cột! Nếu bạn chạy
   theo đúng chữ của đề rồi thấy "column pruning không có tác dụng" thì không phải
   Spark sai — là phép đo sai. => Ta thêm query Q_ALL ép đọc thật đủ 11 cột bằng
   `.write.format("noop")` (action thật, ghi vào hư vô).

BẪY 2 — `size of files read` KHÔNG phản ánh column pruning.
   Đề bài bảo đọc metric này ở node Scan. Nó là TỔNG SIZE CÁC FILE ĐƯỢC MỞ. Đọc 1 cột
   hay 11 cột vẫn mở đúng bấy nhiêu file -> metric BẰNG NHAU. Metric này chỉ hữu ích
   cho PARTITION pruning (A36 — cắt ở mức file). Column pruning cắt BÊN TRONG file
   => phải nhìn `input bytes` (Stage -> Input Size), là byte thật qua FileSystem.

BẪY 3 — chỉ có 1 file thì mọi thứ vẫn đúng, nhưng nếu file quá nhỏ, phần footer +
   dictionary chiếm tỉ lệ lớn -> tỉ lệ bytes bị "pha loãng". Ta repartition(4) và
   in luôn sự thật đó ra thay vì giấu.
"""

import os
import sys

sys.path.insert(0, "/workspace/labs/mini-project-1/src")
import benchmark as B  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

OUT = f"{B.BENCH}/a30/orders"


def main():
    spark = B.new_spark("a30-column-pruning")

    # ---------------------------------------------------------------- nguồn
    df_src, src_note = B.load_orders_clean(spark)
    B.section("A30 — CHUẨN BỊ")
    print(f"Nguồn: {src_note}")
    # repartition(4): muốn có vài file để `number of files read` là con số có nghĩa,
    # đồng thời mỗi file vẫn đủ lớn để footer không chiếm tỉ lệ đáng kể.
    (df_src.repartition(4).write.mode("overwrite").parquet(OUT))
    cols = spark.read.parquet(OUT).columns
    print(f"Ghi {OUT}: {B.count_part_files(OUT)} file, "
          f"{B.human(B.du_bytes(OUT))}, {len(cols)} cột: {cols}")

    df = spark.read.parquet(OUT)
    n_rows = df.count()
    print(f"Số dòng: {n_rows:,}")

    # ---------------------------------------------------------------- 3 query
    # Mọi query PHẢI kết thúc bằng action thật, nếu không là đo lazy = 0.001s = bịa.
    def q_count():
        # Spark prune về 0 cột: chỉ đọc footer đếm dòng.
        return df.count()

    def q_all():
        # ÉP đọc đủ 11 cột. noop = DataSource V2 ghi vào hư vô -> action thật,
        # không tốn I/O ghi, không làm nhiễu phép đo I/O đọc.
        return df.write.format("noop").mode("overwrite").save()

    def q_one():
        # Chỉ động vào 1 cột -> Parquet chỉ đọc các column chunk của cột đó.
        return df.select(F.sum("price")).collect()

    queries = [
        ("Q_COUNT  df.count()", q_count, "0 cột (chỉ footer)"),
        ("Q_ALL    đọc đủ 11 cột (noop)", q_all, f"{len(cols)}/{len(cols)} cột"),
        ("Q_ONE    select(price).sum", q_one, f"1/{len(cols)} cột"),
    ]

    rows, ib = [], {}
    for name, fn, note in queries:
        best, times = B.timeit(fn, runs=3, label=name)
        with B.Probe(spark, name) as p:
            fn()
        ib[name] = p.input_bytes
        rows.append([name, note, f"{times[0]:.2f}s", f"{best:.2f}s",
                     p.files if p.files is not None else "?",
                     B.human(p.files_size), B.human(p.input_bytes),
                     f"{p.input_bytes:,}" if p.input_bytes else "?"])

    B.section("BẢNG A30.1 — BẰNG CHỨNG CHÍNH (chú ý 2 cột cuối, KHÔNG phải cột giây)")
    B.md(["Query", "Cột thực đọc", "lần 1 (lạnh)", "min lần 2-3",
          "files read", "size of files read", "input bytes", "input bytes (số nguyên)"],
         rows)

    all_b = ib.get("Q_ALL    đọc đủ 11 cột (noop)")
    one_b = ib.get("Q_ONE    select(price).sum")
    cnt_b = ib.get("Q_COUNT  df.count()")
    print("KẾT LUẬN SỐ:")
    if all_b and one_b:
        print(f"  bytes(11 cột) / bytes(1 cột) = {all_b:,} / {one_b:,} = {all_b / max(one_b, 1):.1f}×")
        print(f"  => đọc 1 cột chỉ tốn {100 * one_b / max(all_b, 1):.1f}% I/O của đọc cả bảng.")
    if cnt_b is not None:
        print(f"  count() đọc {cnt_b:,} byte — nhỏ hơn cả query 1 cột, vì nó đọc 0 cột "
              f"(chứng minh BẪY 1 ở docstring là thật).")
    print("  `size of files read` giữa các query BẰNG NHAU (nếu đúng như dự đoán) ->")
    print("  metric đó KHÔNG đo được column pruning. Đề bài chỉ sai chỗ này.\n")

    # ------------------------------------------------ sự thật nằm trong file
    # Vì sao phải mổ file? Vì input_bytes là số Spark tự khai. Muốn kiểm chứng chéo,
    # ta hỏi thẳng FILE PARQUET: mỗi cột chiếm bao nhiêu byte nén? Nếu tỉ lệ
    # price/tổng khớp với tỉ lệ input_bytes ở trên -> hai nguồn độc lập cùng chỉ 1 sự thật.
    B.section("BẢNG A30.2 — KIỂM CHỨNG CHÉO: kích thước TỪNG CỘT bên trong file Parquet")
    try:
        import pyarrow.parquet as pq
        f0 = sorted(B.list_part_files(OUT))[0]
        meta = pq.ParquetFile(f0).metadata
        per_col = {}
        for g in range(meta.num_row_groups):
            rg = meta.row_group(g)
            for i in range(rg.num_columns):
                c = rg.column(i)
                per_col[c.path_in_schema] = per_col.get(c.path_in_schema, 0) + c.total_compressed_size
        total = sum(per_col.values())
        B.md(["Cột", "byte nén trong file", "% tổng"],
             [[k, f"{v:,}", f"{100 * v / total:.1f}%"]
              for k, v in sorted(per_col.items(), key=lambda x: -x[1])]
             + [["**TỔNG**", f"{total:,}", "100%"]])
        p_ = per_col.get("price", 0)
        print(f"File mổ: {os.path.basename(f0)} ({meta.num_rows:,} dòng, "
              f"{meta.num_row_groups} row group)")
        print(f"Cột price chiếm {100 * p_ / max(total, 1):.1f}% dung lượng file "
              f"-> về lý thuyết đọc 1 cột price tốn ~{total / max(p_, 1):.0f}× ít I/O hơn đọc cả bảng.")
        print("So con số này với tỉ lệ input_bytes ở Bảng A30.1: hai nguồn độc lập, "
              "khớp nhau thì số đo đáng tin.")
    except ImportError:
        print("CHẠY LỖI: container không có pyarrow. Cài rồi chạy lại:")
        print("  docker exec spark-mastery-spark-submit-1 pip install pyarrow")

    B.section("A30 — NGOẠI SUY (câu chốt của đề bài)")
    if all_b and one_b:
        r = all_b / max(one_b, 1)
        print(f"Olist bé (~{B.human(B.du_bytes(OUT))} parquet) nên chênh lệch GIÂY khiêm tốn — "
              f"phần lớn thời gian là overhead cố định (lập plan, gửi task, khởi động JVM).")
        print(f"Nhưng bytes chênh {r:.1f}×. Ở dữ liệu ×100 (bài A40), phần I/O trở thành phần")
        print(f"CHI PHỐI, còn overhead cố định thì không đổi -> thời gian sẽ tiệm cận tỉ lệ bytes ~{r:.1f}×.")
    print("Đó là lý do rubric bắt 'chỉ vào bytes read' chứ không chỉ vào đồng hồ.")
    spark.stop()


if __name__ == "__main__":
    main()
