"""A20 — Sizing thực chiến: chọn số file cho orders_clean. Bài "senior" nhất track này.

CHẠY (local[2] là ĐỦ và NÊN — bài này đo KÍCH THƯỚC FILE, không đo tốc độ. Chạy local
      còn tránh chuyện mỗi executor ghi một chỗ):
    make run-local F=labs/mini-project-1/exercises/a20_sizing_orders_clean.py

Bài này KHÔNG ghi vào data/output/silver/ (sân của nhóm ingest). Nó ghi thử vào
data/bench/a20/ để ĐO, rồi trả về một QUYẾT ĐỊNH + một đoạn code van an toàn mà
ingest.py sẽ import từ src/sparkutils.py (hàm choose_partition_grain).

CÂU HỎI TRUNG TÂM: quy tắc nghề nói mỗi file Parquet nên 64–256MB. Dữ liệu orders
chỉ ~17MB CSV. Vậy con số "đúng" là 1 FILE. Nhưng đề bài bắt partitionBy("order_date")
-> ~600 ngày -> 600 file vài chục KB. HAI YÊU CẦU NÀY MÂU THUẪN VỚI NHAU.
Đây là mâu thuẫn CÓ THẬT, không phải bạn làm sai. Việc của kỹ sư không phải là giả vờ
nó không tồn tại, mà là ĐO nó, GỌI TÊN nó, rồi thiết kế một lối thoát có điều kiện.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.sparkutils import (TARGET_FILE_BYTES, banner, choose_partition_grain,
                            human_bytes, md_table)

ORDERS_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
BENCH = "/workspace/data/bench/a20"


def dir_stats(path):
    """(số file dữ liệu, tổng byte, list byte của từng file) — đọc từ hệ thống file THẬT.

    Bỏ qua _SUCCESS và các file .crc (checksum của Hadoop) — chúng không phải dữ liệu,
    nhưng LƯU Ý: ở production trên HDFS/S3, mỗi file rác đó vẫn là một object phải
    liệt kê. 600 file dữ liệu thường kéo theo 600 file .crc + 600 entry metadata.
    Chi phí "small files" lớn hơn con số bạn nhìn thấy.
    """
    files = []
    for root, _dirs, names in os.walk(path):
        for n in names:
            if n.startswith(".") or n == "_SUCCESS":
                continue
            files.append(os.path.getsize(os.path.join(root, n)))
    return len(files), sum(files), sorted(files)


def main():
    spark = SparkSession.builder.appName("a20-sizing").getOrCreate()

    # ------------------------------------------------------------------
    # BƯỚC 1 — dữ liệu thật: bao nhiêu dòng, bao nhiêu ngày, bao nhiêu tháng?
    # ------------------------------------------------------------------
    csv_bytes = os.path.getsize(ORDERS_CSV)
    orders = spark.read.csv(ORDERS_CSV, header=True)

    # order_date = ngày (không giờ). schemas.py của bài A21 sẽ làm việc này tử tế hơn;
    # ở đây cast tại chỗ cho bài này tự đứng được một mình.
    clean = (orders
             .withColumn("order_ts", F.to_timestamp("order_purchase_timestamp"))
             .withColumn("order_date", F.to_date("order_ts"))
             .withColumn("order_month", F.date_format("order_ts", "yyyy-MM"))
             .withColumn("order_year", F.date_format("order_ts", "yyyy"))
             .filter(F.col("order_date").isNotNull()))
    clean.cache()

    n_rows = clean.count()
    n_days = clean.select("order_date").distinct().count()
    n_months = clean.select("order_month").distinct().count()
    n_years = clean.select("order_year").distinct().count()

    print(banner("A20 — BƯỚC 1: dữ liệu thật nói gì"))
    print(md_table(
        ["chỉ số", "giá trị"],
        [["CSV gốc", f"{csv_bytes:,} B ({human_bytes(csv_bytes)})"],
         ["số dòng (order_date not null)", f"{n_rows:,}"],
         ["số NGÀY khác nhau", n_days],
         ["số THÁNG khác nhau", n_months],
         ["số NĂM khác nhau", n_years],
         ["trung bình đơn/ngày", f"{n_rows / n_days:,.1f}"]]))

    # ------------------------------------------------------------------
    # BƯỚC 2 — ĐO, ĐỪNG ĐOÁN: Parquet+snappy thì còn bao nhiêu?
    # ------------------------------------------------------------------
    p_one = f"{BENCH}/one_file"
    # repartition(1) chứ KHÔNG coalesce(1): xem bài A17 — coalesce(1) sẽ kéo cả khâu
    # đọc + cast về 1 task. Ở đây dữ liệu bé nên không đau, nhưng viết đúng ngay từ
    # đầu là một thói quen rẻ tiền.
    clean.drop("order_ts").repartition(1).write.mode("overwrite").parquet(p_one)
    f_one, b_one, _ = dir_stats(p_one)

    bytes_per_row = b_one / n_rows
    print(banner("BƯỚC 2: Parquet + snappy — ĐO chứ không đoán"))
    print(md_table(
        ["dạng lưu", "số file", "tổng dung lượng", "so với CSV", "byte / dòng"],
        [["CSV gốc (không nén)", 1, human_bytes(csv_bytes), "1.00×",
          f"{csv_bytes / n_rows:,.1f}"],
         ["Parquet snappy, 1 file", f_one, human_bytes(b_one),
          f"{b_one / csv_bytes:.2f}×", f"{bytes_per_row:,.1f}"]]))
    print(f"""
  -> Parquet nén còn {b_one / csv_bytes * 100:.0f}% của CSV. Vì sao nén tốt thế? Vì Parquet lưu theo CỘT:
     cả cột order_status chỉ có 8 giá trị nằm liền nhau -> dictionary encoding + RLE
     bóp nó xuống gần bằng 0. CSV lưu theo DÒNG nên "delivered" bị lặp lại 96 nghìn lần
     dưới dạng text. (Chi tiết ở lesson 6.)
  -> Toàn bộ orders sau nén = {human_bytes(b_one)}. Quy tắc nghề: mỗi file 64–256 MB.
     Vậy con số ĐÚNG CHUẨN cho dữ liệu này là: **1 file**. Thậm chí 1 file vẫn còn
     bé hơn ngưỡng dưới {human_bytes(TARGET_FILE_BYTES)} tới {TARGET_FILE_BYTES / b_one:.0f} lần.""")

    # ------------------------------------------------------------------
    # BƯỚC 3 — điều đề bài BẮT làm: partitionBy(order_date). Xem hậu quả.
    # ------------------------------------------------------------------
    p_day = f"{BENCH}/by_day"
    (clean.drop("order_ts").repartition("order_date")     # gom cùng ngày về 1 task -> mỗi ngày 1 file
          .write.mode("overwrite").partitionBy("order_date").parquet(p_day))
    f_day, b_day, sizes_day = dir_stats(p_day)

    p_month = f"{BENCH}/by_month"
    (clean.drop("order_ts").repartition("order_month")
          .write.mode("overwrite").partitionBy("order_month").parquet(p_month))
    f_month, b_month, sizes_month = dir_stats(p_month)

    def med(xs):
        return xs[len(xs) // 2] if xs else 0

    print(banner("BƯỚC 3: hậu quả của partitionBy — ba phương án, số thật"))
    print(md_table(
        ["phương án", "số file", "tổng dung lượng", "file bé nhất", "file trung vị",
         "file to nhất", "đạt 64MB/file?"],
        [["1 file (không partitionBy)", f_one, human_bytes(b_one), human_bytes(b_one),
          human_bytes(b_one), human_bytes(b_one),
          "KHÔNG" if b_one < TARGET_FILE_BYTES else "CÓ"],
         [f"partitionBy(order_month) — {n_months} tháng", f_month, human_bytes(b_month),
          human_bytes(min(sizes_month)), human_bytes(med(sizes_month)),
          human_bytes(max(sizes_month)), "KHÔNG"],
         [f"partitionBy(order_date) — {n_days} ngày", f_day, human_bytes(b_day),
          human_bytes(min(sizes_day)), human_bytes(med(sizes_day)),
          human_bytes(max(sizes_day)), "KHÔNG"]]))
    print(f"""
  -> partitionBy(order_date) đẻ ra {f_day} file, trung vị {human_bytes(med(sizes_day))}/file.
     Nhỏ hơn ngưỡng 64MB khoảng {TARGET_FILE_BYTES / max(med(sizes_day), 1):,.0f} lần.
  -> Tổng dung lượng còn PHÌNH LÊN: {human_bytes(b_one)} (1 file) -> {human_bytes(b_day)} ({f_day} file),
     tức +{(b_day / b_one - 1) * 100:.0f}%. Vì sao? Mỗi file Parquet đều phải mang theo footer,
     schema, thống kê min/max cho từng cột, từ điển riêng của nó. Chia càng nhỏ, phần
     "bao bì" càng lấn phần "hàng hoá". Với 600 file thì bao bì thắng.
  -> Và đó mới chỉ là dung lượng. Cái đau thật là ĐỌC: mỗi lần query, Spark phải liệt
     kê {f_day} thư mục, mở {f_day} footer, tạo {f_day} task cho {human_bytes(b_day)} dữ liệu.
     Trên S3 (mỗi lần LIST/GET là một request có độ trễ ~50-100ms) đây là thảm hoạ.""")

    # ------------------------------------------------------------------
    # BƯỚC 4 — Ở KÍCH THƯỚC NÀO thì partitionBy(order_date) BẮT ĐẦU ĐÚNG?
    # ------------------------------------------------------------------
    rows_per_day_now = n_rows / n_days
    rows_needed_per_day = TARGET_FILE_BYTES / bytes_per_row     # để 1 ngày = 64MB
    scale = rows_needed_per_day / rows_per_day_now
    total_needed = rows_needed_per_day * n_days

    print(banner("BƯỚC 4: PHÉP TÍNH — bao giờ thì partitionBy(order_date) mới có lý?"))
    print(f"""  Đặt bài toán: muốn MỖI partition-ngày đạt tối thiểu {human_bytes(TARGET_FILE_BYTES)} (ngưỡng dưới
  của quy tắc 64–256MB). Mọi con số dưới đây đến từ phép đo ở BƯỚC 2, không phải phỏng đoán.

    (1) 1 dòng orders sau Parquet+snappy = {b_one:,} B / {n_rows:,} dòng = {bytes_per_row:,.1f} B/dòng
    (2) Cần mỗi ngày   = {TARGET_FILE_BYTES:,} B / {bytes_per_row:,.1f} B = {rows_needed_per_day:,.0f} đơn/ngày
    (3) Hiện có mỗi ngày = {n_rows:,} / {n_days} ngày = {rows_per_day_now:,.1f} đơn/ngày
    (4) HỆ SỐ CÒN THIẾU  = (2)/(3) = {scale:,.0f}×
    (5) Tức là Olist phải có {total_needed:,.0f} đơn (thay vì {n_rows:,}) trên cùng {n_days} ngày,
        tương đương ~{total_needed * bytes_per_row / 1024 ** 3:,.1f} GB Parquet, thì partitionBy(order_date)
        mới đúng chuẩn nghề.

  ĐỌC CON SỐ {scale:,.0f}× NÀY THẾ NÀO:
    - Olist là một sàn TMĐT Brazil cỡ trung, 2016–2018. Để mỗi ngày sinh {rows_needed_per_day:,.0f} đơn
      thì nó phải to cỡ Shopee/Tiki. Nói cách khác: partitionBy theo NGÀY là thiết kế
      dành cho dữ liệu ở quy mô đó, không dành cho dữ liệu này.
    - Ngược lại KHÔNG có nghĩa partitionBy là sai. Nó chỉ SAI ĐỘ MỊN. Cùng dữ liệu này,
      partitionBy theo THÁNG cho {n_months} file — vẫn còn bé, nhưng đã bớt lố {f_day / max(f_month, 1):.0f} lần.
      partitionBy theo NĂM cho {n_years} file. Càng thô càng gần quy tắc, nhưng càng mất
      khả năng partition pruning (lọc 1 ngày mà phải quét cả tháng/cả năm).
    - Đây là một sự ĐÁNH ĐỔI, không phải một đáp án đúng/sai:
        mịn hơn -> pruning tốt hơn khi query theo ngày, nhưng nhiều file bé
        thô hơn -> file to đẹp, nhưng query 1 ngày phải đọc thừa
      Cân nó bằng CÁCH DÙNG THẬT: nếu 90% query là "lấy đơn hàng ngày X" thì mịn thắng;
      nếu 90% query là "doanh thu theo tháng" thì thô thắng.""")

    # ------------------------------------------------------------------
    # BƯỚC 5 — VAN AN TOÀN (đề bài yêu cầu: hằng số ở đầu file điều khiển độ mịn)
    # ------------------------------------------------------------------
    buckets = {"day": n_days, "month": n_months, "year": n_years}
    grain, table = choose_partition_grain(b_one, buckets)

    print(banner("BƯỚC 5: VAN AN TOÀN — code tự chọn độ mịn theo kích thước"))
    print(md_table(
        ["độ mịn", "số partition-value", "dung lượng TB mỗi value", f"đạt ≥ {human_bytes(TARGET_FILE_BYTES)}?"],
        [[g, n, human_bytes(avg), "CÓ" if ok else "KHÔNG"] for g, n, avg, ok in table]))
    print(f"""
  choose_partition_grain() trả về: **{grain}**
  (Với {human_bytes(b_one)} dữ liệu thì KHÔNG độ mịn nào đạt 64MB/file -> van trả "none",
   nghĩa là: ghi phẳng, đừng partitionBy gì cả. Đó là câu trả lời ĐÚNG về mặt kỹ thuật.)

  Nhưng đề bài BẮT partitionBy(order_date) — và đề bài không sai: mục đích của nó là
  DẠY partition pruning, không phải tối ưu dung lượng. Nên van phải có công tắc tay:

      # --- đầu file ingest.py ---
      from src.sparkutils import choose_partition_grain, TARGET_FILE_BYTES

      PARTITION_GRAIN = "auto"   # "day" | "month" | "year" | "none" | "auto"
      #   "day"   : ép mịn — dùng khi CHẤM BÀI / học partition pruning (mặc định của đề)
      #   "auto"  : để van tự quyết theo kích thước đo được -> dữ liệu bé thì thô hoá
      #   "none"  : ghi phẳng — đúng nhất về dung lượng, mất pruning

      grain = PARTITION_GRAIN
      if grain == "auto":
          grain, _ = choose_partition_grain(est_bytes, {{"day": n_days,
                                                        "month": n_months,
                                                        "year": n_years}})

      w = df.repartition(grain_col(grain)) if grain != "none" else df.repartition(1)
      w = w.write.mode("overwrite")
      if grain != "none":
          w = w.partitionBy(grain_col(grain))     # "order_date" / "order_month" / "order_year"
      w.parquet(SILVER)

  VÌ SAO CÓ repartition(cột) TRƯỚC partitionBy: xem A17. Không có nó, MỖI task ghi
  MỘT file cho MỖI ngày nó gặp -> số file = số_task × số_ngày (hàng chục nghìn).
  Có nó -> mỗi ngày đúng 1 file. Một dòng code, giảm 20× số file.

KẾT LUẬN NỘP BÀI (trả lời luôn câu hỏi mở rộng số 5 ở mục 7 của đề):
  1. orders sau Parquet+snappy = {human_bytes(b_one)} ({bytes_per_row:,.1f} B/dòng, đo được, không đoán).
  2. Theo quy tắc 64–256MB/file, con số đúng là 1 file. partitionBy(order_date) cho
     {f_day} file × trung vị {human_bytes(med(sizes_day))} — lệch chuẩn khoảng {TARGET_FILE_BYTES / max(med(sizes_day), 1):,.0f} lần, và làm tổng dung
     lượng phình +{(b_day / b_one - 1) * 100:.0f}%.
  3. Đây là mâu thuẫn CÓ THẬT giữa yêu cầu SƯ PHẠM (học pruning) và quy tắc VẬN HÀNH
     (file phải to). Tôi giữ partitionBy(order_date) để đúng đề, và ghi nhận rõ nó
     không phải lựa chọn tôi sẽ ra ở production với dữ liệu cỡ này.
  4. partitionBy(order_date) bắt đầu ĐÚNG khi dữ liệu lớn hơn ~{scale:,.0f}× hiện tại
     (≈{rows_needed_per_day:,.0f} đơn/ngày, ≈{total_needed * bytes_per_row / 1024 ** 3:,.1f} GB). Dưới ngưỡng đó, dùng THÁNG.
  5. Van an toàn choose_partition_grain() đã nằm ở src/sparkutils.py, ingest.py import.""")

    spark.stop()


if __name__ == "__main__":
    main()
