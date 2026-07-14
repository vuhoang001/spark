"""A35 — SMALL FILES: GÂY ÁN RỒI PHÁ ÁN.  (bài nặng nhất của phụ lục)

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a35_small_files.py
    # nhanh (chỉ để soi cơ chế, KHÔNG dùng số này làm bằng chứng): 32 partition thay vì 200
    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit --master 'local[2]' \
        /workspace/labs/mini-project-1/exercises/a35_small_files.py 32

⏱ CẢNH BÁO THỜI GIAN: chạy đầy đủ (200 partition) sinh ra HÀNG CHỤC NGHÌN file.
   Ghi + đọc + xoá ngần ấy file trên đĩa mất khoảng 5–20 phút. Đó KHÔNG phải script treo,
   đó chính là bài học: filesystem chết vì số file, không phải vì số byte.
   (Nếu chạy cluster thay vì local: nhớ luật sắt — chỉ MỘT app Spark chạy một lúc.)

PHỤ THUỘC: không. Ghi vào /workspace/data/bench/a35/<chiến lược>/

-------------------------------------------------------------------------------
DỰ ĐOÁN TRƯỚC KHI CHẠY:
  crime (200 part, không repartition theo ngày) -> bao nhiêu file? ____
  cùng số DÒNG mà tổng DUNG LƯỢNG có tăng không? ____ (nếu có: vì sao?)
  query đọc 10.000 file so với đọc 600 file: chậm hơn mấy lần? ____
-------------------------------------------------------------------------------

VÌ SAO "GÂY ÁN" LẠI PHẢI repartition(200)?
  Đề bài nói: "ghi partitionBy(order_date) mà KHÔNG repartition trước, để nguyên
  shuffle.partitions=200". Nhưng đọc thẳng orders CSV (17MB) thì DataFrame chỉ có 1
  partition (17MB < maxPartitionBytes 128MB) -> ghi ra đúng 1 file/ngày -> KHÔNG có án
  để gây! Con số 200 chỉ xuất hiện SAU MỘT SHUFFLE (join/groupBy/repartition) — mà
  pipeline thật thì luôn có shuffle trước khi ghi.
  => Ta tái hiện đúng tình huống thật bằng repartition(200) (= "vừa join xong,
     shuffle.partitions=200 mặc định"). Đây là sửa lỗi của đề, không phải bịa tình huống.

CƠ CHẾ GÂY ÁN (phải hiểu, không thì không phá được):
  Mỗi TASK ghi độc lập. Task giữ N dòng rải rác của đủ mọi ngày -> nó phải mở
  MỘT FILE CHO MỖI NGÀY mà nó có dữ liệu.
      số file  =  Σ (số task có chứa dữ liệu của ngày d)   ~  #task × #ngày
  200 task × 634 ngày -> lên tới hàng chục nghìn file, mỗi file vài KB.
CƠ CHẾ PHÁ ÁN:
  repartition("order_date") = shuffle theo hash(order_date) -> MỌI dòng cùng ngày về
  CÙNG MỘT task -> mỗi ngày chỉ còn 1 task ghi -> 1 file/ngày. (Vài ngày có thể chung
  task — không sao, vẫn 1 file mỗi ngày vì file tách theo thư mục partition.)
"""

import os
import sys

sys.path.insert(0, "/workspace/labs/mini-project-1/src")
import benchmark as B  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

OUT = f"{B.BENCH}/a35"
NPARTS = int(sys.argv[1]) if len(sys.argv) > 1 else 200  # 200 = spark.sql.shuffle.partitions mặc định


def footer_overhead(path):
    """Đo CHI PHÍ CỐ ĐỊNH mỗi file: footer + magic + page header. Đây là câu trả lời
    định lượng cho câu hỏi 'vì sao nhiều file nhỏ thì TỔNG DUNG LƯỢNG cũng tăng'."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    files = B.list_part_files(path)
    if not files:
        return None
    sample = files[: min(len(files), 30)]
    fo = sum(pq.ParquetFile(f).metadata.serialized_size for f in sample) / len(sample)
    sz = sum(os.path.getsize(f) for f in sample) / len(sample)
    return fo, sz


def main():
    spark = B.new_spark("a35-small-files")
    df_src, note = B.load_orders_clean(spark)
    df = df_src.where(F.col("order_date").isNotNull()).cache()
    n = df.count()
    n_dates = df.select("order_date").distinct().count()

    B.section(f"A35 — CHUẨN BỊ  (shuffle.partitions giả lập = {NPARTS})")
    print(f"Nguồn: {note} | {n:,} dòng | {n_dates} ngày khác nhau")
    print(f"spark.sql.shuffle.partitions thật = {spark.conf.get('spark.sql.shuffle.partitions')}")
    print(f"Dự báo lý thuyết cho 'crime': tối đa {NPARTS} task × {n_dates} ngày = "
          f"{NPARTS * n_dates:,} file (thực tế thấp hơn vì ngày ít dòng không rơi vào đủ 200 task)\n")

    # 4 chiến lược. Mỗi cái trả về DataFrame ĐÃ ĐỊNH HÌNH partition, sẵn sàng ghi.
    strategies = [
        ("1. GÂY ÁN: repartition(%d) — mô phỏng vừa shuffle xong" % NPARTS,
         "crime", lambda: df.repartition(NPARTS)),
        ("2. PHÁ ÁN: repartition('order_date')",
         "fix", lambda: df.repartition("order_date")),
        ("3. repartition(1)",
         "one", lambda: df.repartition(1)),
        ("4. repartition(%d).coalesce(8)" % NPARTS,
         "coalesce8", lambda: df.repartition(NPARTS).coalesce(8)),
    ]

    rows = []
    for label, name, build in strategies:
        path = f"{OUT}/{name}"
        print(f"\n>>> {label} -> {path}")

        # ---- ghi (đo thời gian) ---------------------------------------------
        # Ghi 69.000 file mất vài phút. Lặp 3 lần theo luật 'bỏ warmup' là không khả thi
        # cho chiến lược 'crime'. => đo 1 lần cho lần ghi ĐẮT, 2 lần (bỏ lần 1) cho lần
        # ghi rẻ. GHI RÕ TRONG BẢNG mỗi ô là mấy lần chạy — không giấu.
        def w(p=path, b=build):
            b().write.mode("overwrite").partitionBy("order_date").parquet(p)

        t1, all_w = B.timeit(w, runs=1, drop=0, label=f"write {name} (lần 1, lạnh)")
        if t1 < 60:
            t2, _ = B.timeit(w, runs=2, drop=1, label=f"write {name} (lặp lại, ấm)")
            t_write, runs_note = t2, "min lần 2-3"
        else:
            t_write, runs_note = t1, "1 lần (quá đắt để lặp)"

        nfile = B.count_part_files(path)
        size = B.du_bytes(path)
        ndir = B.count_dirs(path)

        # ---- query A trên chính layout đó ------------------------------------
        # Query A = doanh thu theo tháng của đơn delivered. Kết thúc bằng collect() = action thật.
        def qA(p=path):
            return (spark.read.parquet(p)
                    .where(F.col("order_status") == "delivered")
                    .groupBy(F.date_format("order_date", "yyyy-MM").alias("m"))
                    .agg(F.sum("price").alias("rev"))
                    .collect())

        t_q, all_q = B.timeit(qA, runs=3, label=f"queryA {name}")
        with B.Probe(spark, f"queryA {name}") as p:
            qA()

        fo = footer_overhead(path)
        rows.append([
            label, f"{nfile:,}", ndir, B.human(size), f"{size:,}",
            f"{size / max(nfile, 1) / 1024:.1f} KB",
            f"{t_write:.1f}s ({runs_note})",
            f"{all_q[0]:.2f}s", f"{t_q:.2f}s",
            f"{p.files:,}" if p.files is not None else "?",
            B.human(p.input_bytes),
            f"{fo[0]:.0f} B ({100 * fo[0] / max(fo[1], 1):.0f}%)" if fo else "?",
        ])

    B.section("BẢNG A35 — BẰNG CHỨNG BEFORE/AFTER (bảng mạnh nhất của cả project)")
    B.md(["chiến lược", "số file", "số thư mục partition", "tổng size", "size (byte)",
          "TB/file", "thời gian GHI", "query A lạnh", "query A ấm (min 2-3)",
          "files read", "input bytes", "footer/file (%file)"], rows)

    B.section("A35 — GIẢI THÍCH (đây là phần lấy điểm, không phải cái bảng)")
    crime = rows[0]
    fix = rows[1]
    print("① VÌ SAO SỐ FILE NỔ RA?")
    print(f"   Mỗi task ghi độc lập -> task nào có dòng của ngày d thì phải MỞ RIÊNG một file")
    print(f"   trong thư mục order_date=d. {NPARTS} task × {n_dates} ngày => {crime[1]} file thật.")
    print(f"   repartition('order_date') gom mọi dòng cùng ngày về 1 task => {fix[1]} file.")
    print()
    print("② VÌ SAO TỔNG DUNG LƯỢNG CŨNG TĂNG DÙ CÙNG SỐ DÒNG?")
    print(f"   crime: {crime[3]}  vs  fix: {fix[3]}")
    print("   Mỗi file Parquet có CHI PHÍ CỐ ĐỊNH: magic bytes + footer (schema + metadata của")
    print("   mọi column chunk + min/max thống kê) + page header của từng cột. Chi phí này gần")
    print("   như KHÔNG phụ thuộc số dòng — file 3 dòng cũng phải mang đủ footer như file 3 triệu dòng.")
    print("   Xem cột 'footer/file (%file)': ở file nhỏ, METADATA chiếm tỉ lệ khổng lồ.")
    print("   Tệ hơn: dictionary encoding và RLE cần NHIỀU dòng mới nén tốt. File 3 dòng thì")
    print("   từ điển to hơn dữ liệu -> nén âm. Nhiều file nhỏ = mất luôn khả năng nén của Parquet.")
    print()
    print("③ VÌ SAO ĐỌC 10.000 FILE NHỎ CHẬM HƠN ĐỌC 600 FILE DÙ GẦN CÙNG SỐ BYTE?")
    print("   - LIỆT KÊ FILE: driver phải listStatus toàn bộ cây thư mục TRƯỚC khi chạy task nào.")
    print("     Trên S3/HDFS đây là hàng chục nghìn lời gọi RPC — thường là phần CHẬM NHẤT.")
    print("   - MỞ FILE: mỗi file = 1 lần open + đọc footer + seek. Chi phí cố định × số file.")
    print("   - LẬP LỊCH: Spark gom file thành task theo openCostInBytes (mặc định 4MB/file —")
    print("     tức là Spark COI mỗi file rẻ nhất cũng 'nặng' 4MB). 10.000 file × 4MB = 40GB")
    print("     'ảo' / 128MB = ~300 task cho vài chục MB dữ liệu thật -> hàng trăm task rỗng,")
    print("     mỗi task ~vài chục ms overhead JVM.")
    print(f"   Số liệu: query A trên crime = {crime[8]} vs trên fix = {fix[8]}")
    print()
    print("④ HAI BIẾN THỂ CÒN LẠI:")
    print("   - repartition(1): 1 task ghi TẤT CẢ -> vẫn 1 file/ngày (đúng), NHƯNG toàn bộ")
    print("     dữ liệu chui qua MỘT core -> ghi chậm, và với dữ liệu ×100 thì task đó OOM.")
    print("     Đúng kết quả, sai kiến trúc: nó không scale.")
    print("   - coalesce(8): coalesce KHÔNG shuffle, chỉ GỘP các partition có sẵn -> dòng của")
    print("     một ngày vẫn nằm rải ở nhiều partition trong số 8 -> vẫn tối đa 8 file/ngày.")
    print("     Số file giảm mạnh so với crime nhưng KHÔNG về 1/ngày. Đây là điểm khác cốt lõi")
    print("     giữa coalesce (gộp, rẻ, không sửa được layout) và repartition (shuffle, đắt,")
    print("     ĐỊNH HÌNH LẠI được dữ liệu theo key).")
    print()
    print("⑤ CHỐT CHO PIPELINE: trước mọi write.partitionBy(K) -> repartition(K).")
    print("   Nếu một partition K quá to (skew) thì repartition(N, K) để tách thành N file/partition.")
    spark.stop()


if __name__ == "__main__":
    main()
