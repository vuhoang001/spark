"""A25 — Bốn save mode: mode nào idempotent, mode nào nhân đôi dữ liệu (−15 điểm rubric).

Chạy:
    make run-local F=labs/mini-project-1/exercises/a25_four_save_modes.py
    (local đủ.)

Output: bảng 4 mode × (count sau lần 1 | count sau lần 2 | exception?) + số file part-*.

Ý chính: "idempotent" = CHẠY LẠI KHÔNG ĐỔI KẾT QUẢ. Nó không phải tính chất của
pipeline bạn viết, nó là tính chất của SAVE MODE bạn chọn. Chọn sai một chữ
(`append` thay vì `overwrite`) là mọi retry của Airflow đều nhân đôi dữ liệu —
mà retry thì Airflow tự làm, không hỏi bạn.
"""
import os
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from schemas import ORDERS  # noqa: E402

SRC = "/workspace/data/olist/olist_orders_dataset.csv"
BASE = "/workspace/data/bench/a25_savemode"

MODES = ["overwrite", "append", "errorifexists", "ignore"]


def hadoop(spark):
    """Trả về (FileSystem, Path) của Hadoop qua py4j.

    Dùng Hadoop FS thay vì os.listdir vì: (1) code này chạy ở DRIVER, mà đường dẫn
    output có thể là HDFS/S3 chứ không phải đĩa cục bộ; (2) nó là API mà chính Spark
    dùng -> nhìn thấy đúng thứ Spark nhìn thấy.
    """
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path


def rmrf(spark, path):
    fs, Path = hadoop(spark)
    if fs.exists(Path(path)):
        fs.delete(Path(path), True)


def count_part_files(spark, path):
    fs, Path = hadoop(spark)
    st = fs.globStatus(Path(path + "/part-*"))
    return len(st) if st else 0


def main():
    spark = SparkSession.builder.appName("a25-save-modes").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # DataFrame nhỏ, cố định: 1 ngày dữ liệu. Nhỏ để chạy nhanh, nhưng là dữ liệu THẬT.
    df = (spark.read.schema(ORDERS).option("header", True)
          .option("mode", "PERMISSIVE").csv(SRC)
          .withColumn("order_date", F.to_date("order_purchase_timestamp"))
          .filter(F.col("order_date") == F.lit("2018-07-02")))
    n_src = df.cache().count()
    print(f"DataFrame nguồn: {n_src} dòng (đơn ngày 2018-07-02). Ghi cùng DF này 2 LẦN "
          f"vào cùng 1 thư mục, với 4 mode khác nhau.\n")

    rows = []
    for mode in MODES:
        path = f"{BASE}/{mode}"
        rmrf(spark, path)          # dọn sạch: để lần 1 luôn là "thư mục chưa tồn tại"
        print("=" * 78)
        print(f"MODE = {mode}")
        print("=" * 78)

        # ---- LẦN 1: đích chưa tồn tại ----
        c1, e1 = None, "KHÔNG"
        try:
            df.write.mode(mode).parquet(path)
            c1 = spark.read.parquet(path).count()
            f1 = count_part_files(spark, path)
            print(f"  lần 1: ghi OK -> đọc lại {c1} dòng, {f1} file part-*")
        except Exception:  # noqa: BLE001
            e1 = "CÓ"
            tb = traceback.format_exc().strip().splitlines()[-1]
            c1 = f"CHẠY LỖI: {tb[:90]}"
            print(f"  lần 1: NÉM LỖI -> {tb}")

        # ---- LẦN 2: đích ĐÃ tồn tại — đây mới là bài thi ----
        c2, e2 = None, "KHÔNG"
        try:
            df.write.mode(mode).parquet(path)
            c2 = spark.read.parquet(path).count()
            f2 = count_part_files(spark, path)
            print(f"  lần 2: ghi OK -> đọc lại {c2} dòng, {f2} file part-*")
        except Exception:  # noqa: BLE001
            e2 = "CÓ"
            tb = traceback.format_exc().strip().splitlines()[-1]
            # Dù ghi lỗi, thư mục cũ vẫn còn -> đọc lại được, ghi nhận luôn
            try:
                still = spark.read.parquet(path).count()
            except Exception:  # noqa: BLE001
                still = "?"
            c2 = f"{still} (ghi bị CHẶN)"
            print(f"  lần 2: NÉM LỖI -> {tb}")
            print(f"         dữ liệu cũ còn nguyên: {still} dòng")

        idem = "?"
        if isinstance(c1, int) and isinstance(c2, int):
            idem = "CÓ" if c1 == c2 else f"KHÔNG — nhân {c2 / c1:.0f}×"
        elif e2 == "CÓ":
            idem = "CÓ (theo nghĩa 'không làm hỏng gì', nhưng job FAIL)"
        rows.append((mode, c1, c2, f"lần1={e1}, lần2={e2}", idem))

    print("\n" + "=" * 78)
    print("BẢNG (dán vào PROGRESS §3.6 'Save mode')")
    print("=" * 78)
    print()
    print("| mode | count sau lần 1 | count sau lần 2 | có exception? | Idempotent? |")
    print("|---|---|---|---|---|")
    for m, c1, c2, e, idem in rows:
        star = " ⚠️" if m == "append" else (" *(mặc định!)*" if m == "errorifexists" else "")
        print(f"| `{m}`{star} | {c1} | {c2} | {e} | {idem} |")

    print("""
ĐỌC BẢNG:

* `overwrite`  — XOÁ đích rồi ghi lại. Chạy 2 lần ra CÙNG con số => IDEMPOTENT.
                 Nhưng đọc kỹ A26: với bảng CÓ PARTITION, "xoá đích" mặc định
                 nghĩa là xoá CẢ BẢNG, không phải xoá phần bạn đang ghi. Đó mới
                 là chỗ chết người thật sự.
* `append`     — CÁI BẪY −15 ĐIỂM. Không lỗi, không cảnh báo, chỉ là dữ liệu nhân đôi.
                 Nguy hiểm vì nó là mode DUY NHẤT sai mà vẫn "chạy thành công".
                 Airflow retry 3 lần = dữ liệu ×3. Không ai phát hiện cho tới lúc
                 kế toán hỏi vì sao doanh thu gấp ba.
* `errorifexists` — MẶC ĐỊNH. An toàn nhất cho người mới (không im lặng phá gì),
                 nhưng KHÔNG chạy lại được -> mọi retry đều FAIL -> không dùng
                 cho pipeline hằng ngày.
* `ignore`     — Đích tồn tại thì lặng lẽ KHÔNG LÀM GÌ. Nguy hiểm ngầm: bạn sửa
                 bug trong code, chạy lại, và... dữ liệu cũ SAI vẫn nằm nguyên đó.
                 Không lỗi, không đổi, không ai biết. "Idempotent" nhưng theo nghĩa
                 sai: nó bảo toàn cả cái sai.

CÂU CHỐT: chỉ `overwrite` cho idempotency THẬT. Và để `overwrite` không giết
cả bảng partition thì phải bật `spark.sql.sources.partitionOverwriteMode=dynamic`
-> đó chính xác là bài A26, chạy tiếp đi.
""")
    spark.stop()


if __name__ == "__main__":
    main()
