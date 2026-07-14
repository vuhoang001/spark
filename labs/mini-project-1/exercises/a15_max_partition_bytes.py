"""A15 — `maxPartitionBytes`: vặn nút, nhìn số task đổi. Và cái bẫy .csv.gz.

CHẠY (bài này NÊN chạy cả 2 nơi để thấy defaultParallelism đổi -> partition đổi):
    make run       F=labs/mini-project-1/exercises/a15_max_partition_bytes.py   # cluster, dP=6
    make run-local F=labs/mini-project-1/exercises/a15_max_partition_bytes.py   # local[2], dP=2

CÂU HỎI CỦA BÀI: ai quyết định số partition lúc ĐỌC?
Trả lời ngắn: KHÔNG PHẢI mình maxPartitionBytes. Công thức thật (lesson 4 §3.2):

    bytesPerCore  = (tổng bytes + số file × openCostInBytes) / defaultParallelism
    maxSplitBytes = min( maxPartitionBytes , max(openCostInBytes, bytesPerCore) )

Nghĩa là có tới BA tay cùng vặn: maxPartitionBytes, openCostInBytes (4MB), và
defaultParallelism (số core!). Đó là lý do cùng một file 58MB, cùng maxPartitionBytes
=128m, mà local[2] và cluster 6 core cho ra số partition KHÁC NHAU. Script in cả
DỰ ĐOÁN (từ công thức) lẫn THỰC TẾ (Spark trả) để bạn tự đối chiếu.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession

from src.sparkutils import (banner, human_bytes, md_table, parse_size,
                            predict_read_partitions, print_stage_summary, timeit,
                            wait_for_executors)

GEO_CSV = "/workspace/data/olist/olist_geolocation_dataset.csv"
GEO_GZ = "/workspace/data/gz/olist_geolocation_dataset.csv.gz"

# Vặn từ to xuống nhỏ. 128m là mặc định; 4m = openCostInBytes (không xuống thấp hơn
# được nữa — max(openCost, ...) trong công thức chặn lại, thử 1m để tự chứng minh).
SIZES = ["128m", "32m", "16m", "8m", "4m", "1m"]


def main():
    spark = SparkSession.builder.appName("a15-maxPartitionBytes").getOrCreate()
    sc = spark.sparkContext

    # ⚠️ PHẢI CHỜ EXECUTOR ĐĂNG KÝ trước khi đọc defaultParallelism.
    # Đọc ngay lập tức thì trên cluster 6 core ta vẫn nhận về 2 (giá trị sàn khi chưa
    # có executor nào) -> cột "DỰ ĐOÁN" bên dưới sẽ tính bằng dP sai và lệch thực tế,
    # dù công thức hoàn toàn đúng. Xem docstring wait_for_executors() trong sparkutils.
    dp_naive = sc.defaultParallelism
    dp = wait_for_executors(sc, min_executors=1)
    if dp != dp_naive:
        print(f"[A15] defaultParallelism đọc NGAY khi khởi động = {dp_naive} (SAI — executor chưa đăng ký)")
        print(f"[A15] defaultParallelism sau khi executor đăng ký xong = {dp} (ĐÚNG — dùng con số này)")

    open_cost = parse_size(spark.conf.get("spark.sql.files.openCostInBytes", "4194304"))

    # Cỡ file THẬT (byte), lấy từ hệ thống file trong container — không đoán.
    geo_bytes = os.path.getsize(GEO_CSV)

    print(banner(f"A15 — master={sc.master}   defaultParallelism={dp}   "
                 f"openCostInBytes={human_bytes(open_cost)}"))
    print(f"File: {GEO_CSV}")
    print(f"Cỡ  : {geo_bytes:,} byte ({human_bytes(geo_bytes)}), 1 file, KHÔNG nén -> SPLITTABLE")

    # ---------------------------------------------------------------------
    # PHẦN 1 — vặn maxPartitionBytes, đo numPartitions + số task + thời gian count()
    # ---------------------------------------------------------------------
    rows = []
    for size in SIZES:
        spark.conf.set("spark.sql.files.maxPartitionBytes", size)

        # header=True, KHÔNG inferSchema: inferSchema là action trá hình (bài A5),
        # nó sẽ quét cả file ngay tại đây và làm bẩn phép đo thời gian bên dưới.
        df = spark.read.csv(GEO_CSV, header=True)
        n_parts = df.rdd.getNumPartitions()

        # Dự đoán từ công thức lesson 4 — in ra để so với thực tế.
        pred = predict_read_partitions(geo_bytes, 1, dp, parse_size(size), open_cost)

        # Đo count(): 3 lần, vứt lần 1 (JVM warmup + page cache lạnh).
        # count() là ACTION thật -> Spark buộc phải đọc hết file.
        group = f"a15-{size}"
        sc.setJobGroup(group, f"count() với maxPartitionBytes={size}")
        _, warm_ms, n_rows = timeit(lambda d=df: d.count(), runs=3, label=size)

        # Số task ở stage đọc file (stage 0 của job count) — lấy từ REST, không nhìn mắt.
        stages = print_stage_summary(sc, group, title=None) or []
        scan_tasks = stages[0]["num_tasks"] if stages else -1

        rows.append([
            size,
            f"{pred['max_split_bytes'] / 1024 / 1024:,.1f} MB",
            pred["predicted_partitions"],
            n_parts,
            scan_tasks,
            f"{warm_ms:,.0f}",
        ])
        print(f"  -> {size}: {n_parts} partition, {scan_tasks} task, "
              f"count={n_rows:,}, ấm={warm_ms:,.0f} ms")

    print(banner("BẢNG 1 — maxPartitionBytes vặn thế nào thì partition đổi thế nào"))
    print(f"(defaultParallelism = {dp}; openCostInBytes = {human_bytes(open_cost)}; "
          f"file = {human_bytes(geo_bytes)} × 1)")
    print(md_table(
        ["maxPartitionBytes", "maxSplitBytes (công thức)", "partition DỰ ĐOÁN",
         "partition THỰC TẾ", "task ở stage đọc", "count() ấm (ms)"],
        rows))
    print("""
ĐỌC BẢNG NÀY THẾ NÀO:
  - Cột "DỰ ĐOÁN" = ceil(tổng bytes / maxSplitBytes). Nếu nó LỆCH cột "THỰC TẾ" 1–2
    partition: thủ phạm là openCostInBytes. Khi Spark bin-pack các khúc file vào
    partition, nó cộng thêm openCost cho MỖI khúc — coi như mỗi lần mở file tốn
    "ảo" 4MB. Vậy một partition chứa được ~ (maxSplitBytes - openCost) byte dữ liệu
    thật, chứ không phải maxSplitBytes. Chia ít hơn -> ra NHIỀU partition hơn dự đoán.
  - Chú ý dòng 128m: maxSplitBytes KHÔNG phải 128MB, vì bytesPerCore kéo nó xuống.
    Spark cố cho mỗi core có việc làm. Đây là lý do cùng file, cùng config, mà
    cluster (dP=6) và local[2] (dP=2) ra số partition khác nhau.
  - Dòng 1m — ĐỌC KỸ, ĐÂY LÀ CHỖ TÔI ĐÃ VIẾT SAI VÀ SỐ ĐO ĐÃ SỬA LƯNG TÔI:
    Tôi từng tin (và viết ra) rằng "maxSplitBytes không xuống dưới openCost 4MB vì
    max(openCost, ...) chặn lại — vặn nhỏ hơn 4m là vặn vào không khí". SAI.
    Nhìn cột "partition THỰC TẾ": 4m -> 15 partition, nhưng 1m -> 59 partition.
    Nếu thật sự bị chặn ở 4MB thì 1m cũng phải cho 15. Nó cho 59.
    Đọc lại công thức cho kỹ:

        maxSplitBytes = min( maxPartitionBytes , max(openCost, bytesPerCore) )
                        ^^^                      ^^^
                        min NGOÀI CÙNG           max chỉ chặn SÀN cho bytesPerCore

    `max(openCost, bytesPerCore)` chỉ đảm bảo cái NGOẶC TRONG không bé hơn 4MB.
    Nhưng `min()` ở ngoài vẫn có toàn quyền kéo kết quả xuống bằng maxPartitionBytes.
    Đặt maxPartitionBytes=1m -> min(1MB, 10.4MB) = 1MB < openCost. Không có gì chặn cả.
    => maxPartitionBytes LUÔN là trần cứng, vặn xuống bao nhiêu cũng có tác dụng.
    (Bài học phụ, đắt hơn: đừng tin lời giải thích — kể cả của chính mình — khi bảng
     số đang nói ngược lại. Số đo thắng. Đó là lý do bài này in cả DỰ ĐOÁN lẫn THỰC TẾ.)
  - Đường cong thời gian: KHÔNG phải cứ nhiều partition là nhanh. Nhiều partition
    -> nhiều task -> mỗi task chỉ vài chục ms trong khi phí schedule + khởi tạo +
    serialize kết quả của một task cũng cỡ đó. Tìm ĐIỂM ĐẢO CHIỀU trong cột cuối:
    trước điểm đó, thêm partition = thêm song song = nhanh hơn; sau điểm đó,
    thêm partition = thêm overhead = chậm đi.""")

    # ---------------------------------------------------------------------
    # PHẦN 2 — CÁI BẪY ĐẮT NHẤT: .csv.gz KHÔNG SPLITTABLE
    # ---------------------------------------------------------------------
    print(banner("PHẦN 2 — file .csv.gz: vặn nút đến mòn cũng vô ích"))
    if not os.path.exists(GEO_GZ):
        print(f"CHẠY LỖI: không thấy {GEO_GZ}. Sinh nó bằng (chạy ở host, repo root):")
        print("    python3 -c \"import gzip,shutil;"
              "shutil.copyfileobj(open('data/olist/olist_geolocation_dataset.csv','rb'),"
              "gzip.open('data/gz/olist_geolocation_dataset.csv.gz','wb'))\"")
    else:
        gz_bytes = os.path.getsize(GEO_GZ)
        gz_rows = []
        for size in ["128m", "16m", "4m"]:
            spark.conf.set("spark.sql.files.maxPartitionBytes", size)
            dfz = spark.read.csv(GEO_GZ, header=True)
            n_parts = dfz.rdd.getNumPartitions()
            group = f"a15-gz-{size}"
            sc.setJobGroup(group, f"count() gz với maxPartitionBytes={size}")
            _, warm_ms, n_rows = timeit(lambda d=dfz: d.count(), runs=3, label=f"gz-{size}")
            stages = print_stage_summary(sc, group, title=None) or []
            scan_tasks = stages[0]["num_tasks"] if stages else -1
            gz_rows.append([size, n_parts, scan_tasks, f"{n_rows:,}", f"{warm_ms:,.0f}"])

        print(f"\nFile: {GEO_GZ}")
        print(f"Cỡ  : {gz_bytes:,} byte ({human_bytes(gz_bytes)}) — nén từ {human_bytes(geo_bytes)}")
        print(md_table(
            ["maxPartitionBytes", "partition THỰC TẾ", "task ở stage đọc",
             "số dòng", "count() ấm (ms)"], gz_rows))
        print("""
BÀI HỌC ĐẮT NHẤT CỦA TRACK NÀY:
  gzip là stream nén tuần tự — muốn giải nén byte thứ 40.000.000 thì PHẢI giải nén
  toàn bộ 39.999.999 byte trước nó. Không có điểm bắt đầu ở giữa. Nên Spark KHÔNG
  THỂ cắt file .gz thành khúc: 1 file .gz = 1 partition = 1 TASK, dù file 10 GB,
  dù cluster có 1000 core. 999 core kia ngồi chơi, và bạn thì đang trả tiền cho chúng.
  Vặn maxPartitionBytes xuống 4m? Vô ích — xem cột "partition THỰC TẾ" ở trên, nó
  đứng im ở 1. Con số đó là toàn bộ bài học.

  Cùng số dòng, nhưng file .gz đọc bằng 1 task còn file .csv đọc bằng N task —
  so cột thời gian của Bảng 1 và Bảng 2 để thấy giá của "tiết kiệm dung lượng".

  Ở production gặp .gz thì làm gì?
    1. Đọc 1 lần bằng 1 task (chịu đau), .repartition(N) NGAY, rồi tính tiếp.
       Vẫn tắc ở khâu đọc nhưng ít nhất phần tính toán sau đó được song song.
    2. Chuyển sang định dạng splittable: bzip2 (splittable nhưng chậm), lz4,
       hoặc tốt nhất — Parquet + snappy (splittable theo row group, lesson 6).
    3. Nếu là nguồn ngoài đổ vào: yêu cầu họ chia nhỏ thành nhiều file .gz.
       N file .gz = N task. Nén vẫn được, mà vẫn song song.""")

    spark.stop()


if __name__ == "__main__":
    main()
