"""A27 — Bốn format, một bảng số: CSV / JSON / Parquet / ORC.

Chạy:
    make run-local F=labs/mini-project-1/exercises/a27_four_formats.py
    (local ĐỦ và TỐT HƠN: số đo ổn định, không có nhiễu mạng giữa 2 executor.)

Output: bảng 4 format × (dung lượng | tỉ lệ so CSV | số file | thời gian ghi |
        đọc full count() | đọc 1 cột) — dán vào PROGRESS §3.4.

GHI CHÚ TRUNG THỰC — 2 chỗ tôi lệch khỏi đề:

1. AVRO: đề nói "+ Avro nếu jar sẵn". Đã kiểm tra image apache/spark:3.4.1:
      docker exec spark-mastery-spark-submit-1 ls /opt/spark/jars | grep spark-avro
   -> KHÔNG CÓ jar `spark-avro_2.12`. (Có avro-1.11.1.jar nhưng đó là thư viện Avro
   thuần, KHÔNG phải data source của Spark — hai thứ khác nhau.) Muốn có phải
   `--packages org.apache.spark:spark-avro_2.12:3.4.1` (cần mạng để tải).
   => Script BỎ QUA Avro và nói rõ, thay vì bịa số.

2. "select(1 cột).sum()": bảng `orders` KHÔNG CÓ CỘT TIỀN nào (tiền nằm ở
   order_items.price). Nên phép đo "đọc 1 cột" ở đây là
   `select(order_estimated_delivery_date).agg(max(...))` — vẫn đúng bản chất
   (bắt engine đọc ĐÚNG 1 cột), chỉ khác phép toán. Không bịa cột price vào orders.
"""
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from schemas import ORDERS  # noqa: E402

SRC = "/workspace/data/olist/olist_orders_dataset.csv"
SRC_CSV_RAW = SRC              # để tính tỉ lệ so với CSV GỐC
BASE = "/workspace/data/bench/a27_format"

ONE_COL = "order_estimated_delivery_date"


def hadoop(spark):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path


def rmrf(spark, path):
    fs, Path = hadoop(spark)
    if fs.exists(Path(path)):
        fs.delete(Path(path), True)


def du_bytes(spark, path):
    """du -sh phiên bản Hadoop FS. Tính CẢ file _SUCCESS và mọi thứ trong thư mục."""
    fs, Path = hadoop(spark)
    p = Path(path)
    if not fs.exists(p):
        return 0
    return fs.getContentSummary(p).getLength()


def n_files(spark, path):
    fs, Path = hadoop(spark)
    st = fs.globStatus(Path(path + "/part-*"))
    return len(st) if st else 0


def mb(b):
    return b / (1024 * 1024)


def bench(fn, runs=3):
    """3 lần, VỨT lần 1 (JVM warmup), lấy min lần 2-3. fn phải kết thúc bằng ACTION."""
    ts, res = [], None
    for _ in range(runs):
        t0 = time.time()
        res = fn()
        ts.append(time.time() - t0)
    return (min(ts[1:]) if len(ts) > 1 else ts[0]), ts[0], res


def main():
    spark = SparkSession.builder.appName("a27-formats").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Cùng MỘT DataFrame cho cả 4 format — nếu không thì đang so táo với cam.
    # coalesce(1)? KHÔNG: giữ nguyên số partition tự nhiên để số file phản ánh
    # đúng hành vi mặc định của từng format (và để A35 có cái mà so).
    df = (spark.read.schema(ORDERS).option("header", True)
          .option("mode", "PERMISSIVE").csv(SRC)
          .withColumn("order_date", F.to_date("order_purchase_timestamp"))
          ).cache()
    n_rows = df.count()
    n_parts = df.rdd.getNumPartitions()
    csv_src_bytes = du_bytes(spark, SRC_CSV_RAW)
    print(f"DataFrame nguồn: {n_rows} dòng, {n_parts} partition trong RAM.")
    print(f"CSV gốc trên đĩa: {mb(csv_src_bytes):.2f} MB  (đây là mốc 1×)\n")

    FORMATS = [
        # (tên, writer_fn, reader_fn, ghi chú)
        ("csv", lambda w, p: w.option("header", True).csv(p),
         lambda p: spark.read.schema(df.schema).option("header", True).csv(p)),
        ("json", lambda w, p: w.json(p),
         lambda p: spark.read.schema(df.schema).json(p)),
        ("parquet(snappy)", lambda w, p: w.option("compression", "snappy").parquet(p),
         lambda p: spark.read.parquet(p)),
        # ORC: KHÔNG set compression -> để mặc định của Spark, và IN RA giá trị mặc định đó
        # thay vì đoán bừa là "zlib" (Spark 3.4 mặc định `spark.sql.orc.compression.codec`
        # = snappy; nhưng đừng tin tôi, script in ra để bạn tự thấy).
        ("orc(mặc định)", lambda w, p: w.orc(p),
         lambda p: spark.read.orc(p)),
    ]
    print(f"codec mặc định — parquet: {spark.conf.get('spark.sql.parquet.compression.codec')}"
          f" | orc: {spark.conf.get('spark.sql.orc.compression.codec')}\n")

    rows = []
    for name, writer, reader in FORMATS:
        path = f"{BASE}/{name.split('(')[0]}"
        print("=" * 78)
        print(f"FORMAT = {name}   -> {path}")
        print("=" * 78)

        # --- GHI (đo 3 lần: mỗi lần phải xoá đích trước, nếu không mode overwrite
        #     sẽ tự xoá và ta đo lẫn cả thời gian xoá — vẫn công bằng vì cả 4 format
        #     đều chịu như nhau; ta dùng overwrite cho đơn giản và nhất quán.)
        t_write, t_write_cold, _ = bench(
            lambda: (writer(df.write.mode("overwrite"), path), "written")[1])

        size = du_bytes(spark, path)
        nf = n_files(spark, path)

        # --- ĐỌC FULL: count() ---
        # LƯU Ý BẪY: với Parquet/ORC, count() KHÔNG đọc data — nó đọc metadata/footer.
        # Nên con số này KHÔNG phải "tốc độ đọc", nó là "tốc độ đếm". Đó chính là
        # điều thú vị và phải nói ra, chứ không phải giấu đi.
        t_count, t_count_cold, c = bench(lambda: reader(path).count())
        assert c == n_rows, f"{name}: count sai! {c} != {n_rows}"

        # --- ĐỌC FULL THẬT SỰ: bắt engine chạm vào MỌI cột ---
        # Cách ép: đếm số dòng có bất kỳ cột nào null -> phải đọc hết cột.
        t_full, _, _ = bench(
            lambda: reader(path).select([F.col(c_) for c_ in df.columns])
                    .where(F.col("order_id").isNotNull()).count())

        # --- ĐỌC 1 CỘT: đây là chỗ columnar ăn tiền ---
        t_1col, _, mx = bench(lambda: reader(path).select(ONE_COL).agg(F.max(ONE_COL)).collect())

        ratio = size / csv_src_bytes if csv_src_bytes else 0
        rows.append((name, size, ratio, nf, t_write, t_count, t_full, t_1col))
        print(f"  dung lượng   = {mb(size):8.2f} MB   ({ratio:.2f}× CSV gốc), {nf} file part-*")
        print(f"  ghi (ấm)     = {t_write*1000:8.1f} ms   (lạnh {t_write_cold*1000:.0f} ms)")
        print(f"  count() (ấm) = {t_count*1000:8.1f} ms")
        print(f"  đọc mọi cột  = {t_full*1000:8.1f} ms")
        print(f"  đọc 1 cột    = {t_1col*1000:8.1f} ms   (max = {mx[0][0] if mx else '?'})")
        print()

    # ---------------- AVRO: nói thật ----------------
    print("=" * 78)
    print("FORMAT = avro -> BỎ QUA")
    print("=" * 78)
    try:
        spark.read.format("avro").load("/workspace/data/does-not-exist")
    except Exception as e:  # noqa: BLE001
        msg = str(e).splitlines()[0][:200]
        print(f"  Bằng chứng: {msg}")
    print("  Image apache/spark:3.4.1 KHÔNG kèm data source spark-avro.")
    print("  Muốn có: --packages org.apache.spark:spark-avro_2.12:3.4.1 (cần mạng).")
    print("  KHÔNG bịa số cho format không chạy được.\n")

    # ---------------- BẢNG ----------------
    print("=" * 78)
    print("BẢNG (dán vào PROGRESS §3.4 'Format')")
    print("=" * 78)
    print()
    print("| Format | Dung lượng | Tỉ lệ so CSV gốc | Số file | Ghi (ms, ấm) | "
          "count() (ms) | Đọc mọi cột (ms) | Đọc 1 cột (ms) |")
    print("|---|---|---|---|---|---|---|---|")
    print(f"| CSV gốc (nguồn) | {mb(csv_src_bytes):.2f} MB | **1×** | 1 | — | — | — | — |")
    for name, size, ratio, nf, tw, tc, tf, t1 in rows:
        print(f"| {name} | {mb(size):.2f} MB | {ratio:.2f}× | {nf} | {tw*1000:.0f} | "
              f"{tc*1000:.0f} | {tf*1000:.0f} | {t1*1000:.0f} |")

    # ---------------- GIẢI THÍCH (đề hỏi đúng 2 câu này) ----------------
    print("""
HAI CÂU ĐỀ HỎI — trả lời bằng cơ chế, không bằng cảm tính:

1) VÌ SAO JSON TO HƠN CSV?
   Vì JSON lặp lại TÊN CỘT ở MỌI DÒNG. CSV ghi tên cột đúng 1 lần (header).
   Một dòng orders có 8 cột; tên cột trung bình ~25 ký tự
   ("order_delivered_customer_date" là 29) -> JSON phải trả thêm ~200 byte/dòng
   chỉ để nói lại những cái tên mà nó đã nói ở dòng trước, 99.441 lần.
   Cộng thêm { } " " : , -> phình. JSON đổi DUNG LƯỢNG lấy TÍNH TỰ MÔ TẢ
   (mỗi dòng tự hiểu được, không cần header, không sợ lệch cột — chính là bệnh
   L4b của bài A23). Không có bữa trưa miễn phí, chỉ có đánh đổi.

2) VÌ SAO PARQUET ĐỌC-1-CỘT NHANH HƠN NHIỀU DÙ CÙNG SỐ DÒNG?
   Vì bố cục vật lý khác nhau, không phải vì "Parquet được tối ưu hơn":
   - CSV/JSON là ROW-ORIENTED: các giá trị của 1 dòng nằm cạnh nhau trên đĩa.
     Muốn lấy cột thứ 8, engine BẮT BUỘC đọc và parse cả 7 cột trước nó, từng dòng một.
     Đọc 1 cột = đọc 100% file. Không có cách nào khác.
   - Parquet/ORC là COLUMNAR: mọi giá trị của CÙNG MỘT CỘT nằm liền kề nhau thành
     một khối (column chunk). Muốn cột thứ 8 -> footer cho biết cột đó nằm ở byte
     nào -> SEEK thẳng tới đó, đọc đúng khối đó. Đọc 1/8 cột ~ đọc ~1/8 bytes.
   - Phần thưởng kèm theo: giá trị cùng cột thì cùng kiểu và hay giống nhau
     -> nén rất tốt (dictionary + RLE). Đó là lý do Parquet vừa NHỎ hơn vừa NHANH hơn,
     hai thứ thường phải đánh đổi cho nhau.

CẢNH BÁO KHI ĐỌC BẢNG TRÊN (trung thực > đẹp):
   - Olist chỉ 17 MB. Ở kích thước này, chi phí CỐ ĐỊNH (khởi động job ~50 ms,
     mở file, đọc footer) LẤN ÁT chi phí đọc dữ liệu. Cột "ms" có thể cho kết quả
     phản trực giác (thậm chí CSV nhanh hơn ở vài phép đo) — ĐỪNG SỬA SỐ, hãy đọc
     cột DUNG LƯỢNG và bảng bytes-read của A30. Bytes không nói dối, giây thì có.
   - count() trên Parquet/ORC gần như tức thời vì nó đọc FOOTER (số dòng ghi sẵn),
     không đọc data. So count() của Parquet với count() của CSV là so "đọc metadata"
     với "parse 17 MB text" — đúng về mặt kết quả, nhưng phải hiểu vì sao mới nói được.
""")
    spark.stop()


if __name__ == "__main__":
    main()
