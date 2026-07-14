"""A19 — Soi bên trong từng partition: cái ống nghe dùng cả sự nghiệp.

CHẠY (local là đủ — bài này đo HÌNH DẠNG dữ liệu, không đo tốc độ):
    make run-local F=labs/mini-project-1/exercises/a19_partition_sizes.py
    make run       F=labs/mini-project-1/exercises/a19_partition_sizes.py   # cũng chạy được

HELPER SỐNG Ở ĐÂU: labs/mini-project-1/src/sparkutils.py
    partition_sizes(df)        -> glom().map(len).collect()   (đúng như đề bài)
    partition_sizes_cheap(df)  -> mapPartitions đếm            (bản production-safe)
    partition_stats(sizes)     -> min/max/mean/stddev/rỗng/skew
    histogram(sizes)           -> vẽ '#'
    partition_report(df, nhãn) -> gộp cả ba, in một phiếu khám

Bài này soi 5 trạng thái của cùng một DataFrame. Mục tiêu KHÔNG phải là chạy code —
mà là nhìn thấy: mỗi phép biến đổi làm HÌNH DẠNG dữ liệu méo đi như thế nào.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.sparkutils import banner, md_table, partition_report

ORDERS = "/workspace/data/olist/olist_orders_dataset.csv"


def main():
    spark = SparkSession.builder.appName("a19-partition-sizes").getOrCreate()
    sc = spark.sparkContext
    # Tắt AQE: AQE gộp partition sau shuffle -> ta sẽ không nhìn thấy 200 partition
    # rỗng, tức là không nhìn thấy đúng CĂN BỆNH mà bài này muốn khám.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    print(banner(f"A19 — master={sc.master} | defaultParallelism={sc.defaultParallelism} | "
                 f"shuffle.partitions=200 | AQE=OFF"))
    print("""⚠️ GHI CHÚ PRODUCTION (đề bài bắt ghi, và đúng là phải ghi):
   glom() gom TOÀN BỘ dòng của một partition thành một list trong RAM executor, rồi
   collect() kéo hết về driver. Olist bé (99k dòng) nên vô hại. Với 1 TB thì đây là
   cách tự bắn vào chân: OOM executor ngay lập tức.
   Bản production: partition_sizes_cheap() — mapPartitions(sum(1 for _ in it)) — chỉ
   đếm trên iterator, bộ nhớ O(1), chỉ N con số bay về driver. Cùng kết quả, không rủi ro.
   Script này dùng bản glom() vì đề bài yêu cầu đúng nó, và có kiểm chứng hai bản khớp nhau.""")

    orders = spark.read.csv(ORDERS, header=True)
    orders.cache()
    total = orders.count()

    # ------------------------------------------------------------------
    # ẢNH 1 — lúc mới đọc. Số partition do maxPartitionBytes + dP quyết định (bài A15).
    # ------------------------------------------------------------------
    s1 = partition_report(orders, "ẢNH 1 — orders vừa đọc từ CSV (chưa động vào gì)")

    # Kiểm chứng chéo: bản glom và bản mapPartitions phải cho cùng kết quả.
    from src.sparkutils import partition_sizes, partition_sizes_cheap
    a, b = partition_sizes(orders), partition_sizes_cheap(orders)
    print(f"\n  [tự kiểm] glom == mapPartitions ? {a == b}   (nếu False thì helper sai, dừng lại)")

    # ------------------------------------------------------------------
    # ẢNH 2 — sau FILTER MẠNH (giữ ~1% dữ liệu)
    # order_status == 'shipped' là một nhóm nhỏ có thật trong Olist.
    # ------------------------------------------------------------------
    shipped = orders.filter(F.col("order_status") == "shipped")
    s2 = partition_report(shipped, "ẢNH 2 — sau filter(order_status='shipped') — giữ ~1%")
    print(f"""
   ĐỌC ẢNH 2: số partition KHÔNG ĐỔI ({s1['num_partitions']} -> {s2['num_partitions']}), chỉ có số DÒNG teo lại
   ({s1['total_rows']:,} -> {s2['total_rows']:,}). filter là NARROW transformation: nó chạy TẠI CHỖ, trong
   từng partition, không di chuyển dữ liệu. Vứt 99% dòng đi nhưng cái "hộp" vẫn còn nguyên.
   Ở quy mô này ({s1['num_partitions']} partition) thì vô hại. Vấn đề nằm ở ẢNH 3.""")

    # ------------------------------------------------------------------
    # ẢNH 3 — filter mạnh trên df ĐÃ có 200 partition. Đây mới là cảnh tượng đề bài hỏi.
    # ------------------------------------------------------------------
    big = orders.repartition(200)
    s3a = partition_report(big, "ẢNH 3a — orders.repartition(200) (trước filter)")
    s3 = partition_report(big.filter(F.col("order_status") == "shipped"),
                          "ẢNH 3b — 200 partition đó SAU filter giữ 1%")
    print(f"""
   ĐỌC ẢNH 3 — ĐÂY LÀ CÂU TRẢ LỜI CỦA ĐỀ BÀI:
   "sau filter mạnh thì 200 partition kia ra sao?"
     -> Chúng VẪN Ở ĐÓ. Đủ {s3['num_partitions']} partition, trong đó {s3['empty']} partition RỖNG HOÀN TOÀN,
        và phần còn lại mỗi cái vỏn vẹn ~{s3['mean']:.0f} dòng (max {s3['max']}).
     -> Spark KHÔNG tự dọn. Bước tiếp theo trong pipeline sẽ tạo ra {s3['num_partitions']} TASK để xử lý
        {s3['total_rows']:,} dòng. Mỗi task làm việc vài mili-giây, nhưng phí schedule + khởi tạo +
        báo cáo của nó thì cố định. Tỉ lệ "làm việc thật / tổng chi phí" rơi xuống gần 0.
     -> Nếu bước sau là WRITE: {s3['num_partitions']} task = {s3['num_partitions']} FILE, mỗi file vài KB. Chào mừng đến với
        small files problem — thứ giết chết mọi data lake, kể cả của người giỏi.

   GỢI Ý CỦA ĐỀ: "điều đó gợi ý bạn nên gọi hàm gì tiếp theo?"
     -> coalesce(n). Đây là ĐÚNG chỗ để dùng coalesce (chứ không phải repartition):
        * dữ liệu sau filter đã BÉ -> shuffle là lãng phí thuần tuý.
        * coalesce chỉ DÁN partition lại, không di chuyển dữ liệu qua mạng: gần miễn phí.
        * phần tính toán nặng (đọc + filter) nằm TRƯỚC nó, và coalesce KHÔNG cắt stage
          -> đúng, nó sẽ kéo mức song song của khâu đọc+filter xuống n. Nên đừng
          coalesce(1); chọn n ≈ số core (ở đây {sc.defaultParallelism}) là an toàn.
     -> Nếu sau filter dữ liệu bị LỆCH (vài partition to, phần lớn rỗng) thì lại phải
        repartition(n): chỉ nó mới chia đều được. Quy tắc: bé + đều -> coalesce;
        bé + lệch -> repartition. Xem chính xác điều đó ở ẢNH 4 và 5.""")

    coalesced = big.filter(F.col("order_status") == "shipped").coalesce(sc.defaultParallelism)
    partition_report(coalesced, f"ẢNH 3c — ...sau coalesce({sc.defaultParallelism}): thuốc đã ngấm")

    # ------------------------------------------------------------------
    # ẢNH 4 — repartition(8): round-robin, chia đều tuyệt đối
    # ------------------------------------------------------------------
    s4 = partition_report(orders.repartition(8), "ẢNH 4 — orders.repartition(8) — chia theo SỐ")

    # ------------------------------------------------------------------
    # ẢNH 5 — repartition("order_status"): chia theo KHOÁ -> hash -> LỆCH
    # ------------------------------------------------------------------
    s5 = partition_report(orders.repartition("order_status"),
                          "ẢNH 5 — orders.repartition(\"order_status\") — chia theo KHOÁ")

    print(banner("BẢNG TỔNG — 5 trạng thái, 5 hình dạng"))
    print(md_table(
        ["ảnh", "trạng thái", "partition", "rỗng", "dòng", "min", "max", "mean",
         "stddev", "skew (max/mean)"],
        [[i, name, s["num_partitions"], s["empty"], f"{s['total_rows']:,}",
          f"{s['min']:,}", f"{s['max']:,}", f"{s['mean']:,.0f}",
          f"{s['stddev']:,.0f}", f"{s['skew_ratio']:.2f}×"]
         for i, (name, s) in enumerate([
             ("mới đọc", s1),
             ("sau filter 1%", s2),
             ("repartition(200)", s3a),
             ("repartition(200) + filter 1%", s3),
             ("repartition(8)", s4),
             ('repartition("order_status")', s5),
         ], start=1)]))

    print(f"""
NHẬN XÉT (thứ phải viết vào report):

  * repartition(8) — ẢNH 4: skew {s4['skew_ratio']:.2f}×, stddev {s4['stddev']:,.0f}. Round-robin rải dòng
    lần lượt vào 8 thùng nên gần như hoàn hảo. Đây là ý nghĩa của "chia đều".

  * repartition("order_status") — ẢNH 5: skew {s5['skew_ratio']:.2f}×, {s5['empty']}/{s5['num_partitions']} partition RỖNG.
    Vì sao? Chia theo KHOÁ = hash(khoá) % 200. order_status chỉ có 8 giá trị -> nhiều
    nhất 8 thùng có hàng, 192 thùng rỗng (giống hệt bài A16 — cùng một cơ chế!). Và
    trong 8 thùng có hàng, 'delivered' chiếm ~97% -> một task ôm gần hết dữ liệu.
    ĐÂY LÀ SKEW, do chính tay bạn gây ra bằng một dòng code trông rất vô hại.

  * BÀI HỌC TRUNG TÂM: repartition(n) và repartition(cột) là HAI HÀM KHÁC NHAU đội
    chung một cái tên.
        repartition(8)              -> mục tiêu: CHIA ĐỀU. Dùng khi cần cân tải.
        repartition("order_date")   -> mục tiêu: GOM CÙNG KHOÁ VỀ MỘT CHỖ. Dùng trước
                                       write.partitionBy để mỗi ngày ra đúng 1 file.
    Chọn cái thứ hai là CHẤP NHẬN skew để đổi lấy cấu trúc file đẹp. Đó là một giao
    dịch có ý thức — miễn là bạn biết mình đang giao dịch cái gì. (Bài A17 + A20.)

  * Cái ống nghe này giữ lại: từ nay trước mỗi write, và sau mỗi shuffle đáng ngờ,
    gọi partition_report(df, "nhãn"). Ba giây, và bạn thấy được thứ mà 90% người viết
    Spark không bao giờ nhìn.""")

    spark.stop()


if __name__ == "__main__":
    main()
