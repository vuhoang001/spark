"""A31 — BỐN THUẬT NÉN: none / snappy / gzip / zstd.

CHẠY:
    make run-local F=labs/mini-project-1/exercises/a31_four_codecs.py
BẮT BUỘC local[2]: đây là bài đo THỜI GIAN. Cluster có 2 executor ở 2 container ->
thêm nhiễu (mạng, scheduling) mà dữ liệu chỉ 17MB -> số đo nhảy loạn, không so được.

PHỤ THUỘC: không. Tự dựng nguồn từ CSV (hoặc silver nếu đã có).

DỰ ĐOÁN TRƯỚC (điền trước khi chạy):
  size:  none ___ > snappy ___ > zstd ___ > gzip ___   ?
  ghi:   gzip chậm nhất? zstd đắt hơn snappy bao nhiêu?
  đọc:   none nhanh nhất? (đọc nhiều byte hơn nhưng không phải giải nén — đánh đổi CPU/IO)

Ý NGHĨA THẬT SỰ CỦA BÀI (không nằm ở việc "gzip nén tốt hơn 20%"):
  - Parquet nén theo TỪNG PAGE bên trong column chunk, không nén cả file.
    => file .parquet nén gzip VẪN SPLITTABLE (chia được cho nhiều task), khác hẳn
       file .csv.gz — cái đó nén cả stream, không chia được, 1 file = 1 task.
    => câu "gzip không splittable" là ĐÚNG với CSV.GZ và SAI với PARQUET-GZIP.
       Rất nhiều người (và cả blog) nhầm chỗ này. Đề bài gợi ý "gzip không splittable
       nên một file lớn thành một task" — câu đó chỉ đúng cho file nén nguyên khối.
    => lý do thật để chọn snappy/zstd cho Parquet là CPU giải nén, không phải splittable.
"""

import sys

sys.path.insert(0, "/workspace/labs/mini-project-1/src")
import benchmark as B  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

CODECS = ["none", "snappy", "gzip", "zstd"]
OUT = f"{B.BENCH}/a31"


def main():
    spark = B.new_spark("a31-four-codecs")
    df_src, note = B.load_orders_clean(spark)

    # cache: cùng một DataFrame nguồn cho cả 4 lần ghi. Không cache -> mỗi lần ghi lại
    # đọc + join lại CSV -> thời gian ghi bị cộng thêm thời gian ĐỌC CSV, không còn đo
    # thuần "chi phí nén" nữa. Đây là bẫy làm sai lệch cột "thời gian ghi".
    df = df_src.repartition(4).cache()
    n = df.count()  # ép materialize cache trước khi bấm giờ

    B.section("A31 — CHUẨN BỊ")
    print(f"Nguồn: {note} | {n:,} dòng | {len(df.columns)} cột | 4 partition (=> 4 file/codec)")
    print("df đã cache -> cột 'thời gian ghi' chỉ còn chi phí serialize + nén + ghi đĩa.\n")

    rows = []
    for codec in CODECS:
        path = f"{OUT}/{codec}"

        # ---- ghi -------------------------------------------------------------
        def w(p=path, c=codec):
            (df.write.mode("overwrite").option("compression", c).parquet(p))

        t_write, all_w = B.timeit(w, runs=3, label=f"write {codec}")
        size = B.du_bytes(path)
        nfile = B.count_part_files(path)

        # ---- đọc full (ép đọc MỌI cột) --------------------------------------
        # noop = action thật, không ghi gì -> đo đúng chi phí ĐỌC + GIẢI NÉN.
        # (Nếu dùng count() thì Spark prune về 0 cột -> không giải nén gì -> đo hụt.)
        def rfull(p=path):
            spark.read.parquet(p).write.format("noop").mode("overwrite").save()

        t_full, _ = B.timeit(rfull, runs=3, label=f"read-full {codec}")

        # ---- đọc 1 cột -------------------------------------------------------
        def rone(p=path):
            (spark.read.parquet(p).groupBy("order_status").count().collect())

        t_one, _ = B.timeit(rone, runs=3, label=f"read-1col {codec}")

        with B.Probe(spark, f"read-full {codec}") as p:
            rfull()

        rows.append([codec, nfile, B.human(size), f"{size:,}",
                     f"{t_write:.2f}s", f"{t_full:.2f}s", f"{t_one:.2f}s",
                     B.human(p.input_bytes)])

    base = next((r for r in rows if r[0] == "none"), None)
    base_size = int(base[3].replace(",", "")) if base else None
    for r in rows:
        sz = int(r[3].replace(",", ""))
        r.append(f"{base_size / sz:.2f}×" if base_size else "?")

    B.section("BẢNG A31 — 4 CODEC × 4 SỐ ĐO (mọi thời gian = min lần 2-3, đã bỏ warmup)")
    B.md(["codec", "số file", "size", "size (byte)", "ghi", "đọc-full",
          "đọc-1-cột", "input bytes (đọc-full)", "tỉ lệ nén vs none"], rows)

    B.section("A31 — CÁCH ĐỌC BẢNG (và cái bẫy 'zstd nhanh hơn none')")
    print("1. Cột 'size' là thứ duy nhất chắc chắn ổn định. Các cột giây trên dữ liệu 17MB")
    print("   bị overhead cố định (lập plan ~0.2-0.5s) nuốt mất -> chênh lệch nhỏ có thể là NHIỄU.")
    print("   Nếu 2 codec chênh nhau < 15% thời gian, KHÔNG kết luận cái nào nhanh hơn.")
    print("2. Nghịch lý hay gặp: 'none' đọc CHẬM hơn 'snappy'. Không vô lý:")
    print("   none phải kéo NHIỀU BYTE hơn từ đĩa; snappy giải nén rất rẻ (~GB/s).")
    print("   Đọc dữ liệu = I/O + CPU. Nén = trả CPU để mua I/O. Với đĩa chậm, giao dịch này LỜI.")
    print("3. QUYẾT ĐỊNH CHO PIPELINE (điền vào report, dựa trên số ở bảng trên, không dựa cảm tính):")
    print("   - snappy: nén khá, CPU nén/giải nén rẻ nhất -> mặc định cho tầng SILVER (ghi/đọc nhiều lần).")
    print("   - zstd  : nén tốt gần gzip, giải nén nhanh gần snappy -> tốt cho GOLD/archive (ghi 1 đọc nhiều).")
    print("   - gzip  : nén tốt nhất nhưng CPU đắt; với Parquet nó VẪN splittable (nén theo page,")
    print("             không nén nguyên file) -> lập luận 'gzip = 1 task' chỉ đúng cho .csv.gz.")
    print("   - none  : chỉ dùng khi CPU là nút cổ chai và đĩa/mạng rẻ vô hạn — hiếm.")
    print("   Viết vào report theo mẫu: 'chọn ___ vì nén hơn none ___×, ghi chậm hơn snappy ___%,")
    print("   đọc-1-cột chênh ___s' — có số mới được điểm.")
    spark.stop()


if __name__ == "__main__":
    main()
