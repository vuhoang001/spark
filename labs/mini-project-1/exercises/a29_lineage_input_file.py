"""A29 — Truy vết nguồn gốc từng dòng: `input_file_name()` + `ingest_ts`.

PHỤ THUỘC: cần data/dirty/orders_dirty.csv -> CHẠY A23 TRƯỚC.
(Có 2 file cùng schema thì cột source_file mới có gì để phân biệt. Đọc 1 file thì
 cột đó chỉ là một hằng số — không chứng minh được điều gì.)

Chạy:
    make run-local F=labs/mini-project-1/exercises/a29_lineage_input_file.py

Output: vài dòng quarantine có đủ source_file + ingest_ts + _corrupt_record,
        + trả lời: cột nào giúp REPLAY được lỗi.

Ý chính: khi sếp hỏi "dòng rác này ở đâu ra?", câu trả lời phải nằm TRONG BẢNG,
không nằm trong trí nhớ của bạn. Cột lineage là thứ rẻ nhất bạn có thể thêm
(2 dòng code) và là thứ đắt nhất khi thiếu (không điều tra được -> phải chạy lại
cả pipeline để tái hiện).
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from schemas import ORDERS_CORRUPT  # noqa: E402

# Đọc NHIỀU file cùng schema trong MỘT lần read -> input_file_name() mới có ý nghĩa.
# (Đề gợi ý glob "olist_order*.csv", nhưng CẨN THẬN: glob đó khớp cả
#  olist_order_items / olist_order_payments / olist_order_reviews — 3 bảng có SCHEMA
#  KHÁC HẲN. Ép chúng vào schema của orders thì ~330k dòng sẽ thành rác corrupt,
#  bài học bị nhiễu và tốn thời gian vô ích. Tôi chọn đọc 2 file CÙNG schema orders:
#  file gốc + file bẩn của A23. Đúng tinh thần bài, sạch về mặt thí nghiệm.
#  Ghi nhận: đây là chỗ tôi CỐ Ý làm khác đề, và đây là lý do.)
PATHS = [
    "/workspace/data/olist/olist_orders_dataset.csv",
    "/workspace/data/dirty/orders_dirty.csv",
]
QUARANTINE = "/workspace/data/output/quarantine/orders_a29"


def main():
    spark = SparkSession.builder.appName("a29-lineage").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # BẪY: input_file_name() chỉ có giá trị khi DataFrame còn nối trực tiếp với
    # FileScan. Sau shuffle/join nó vẫn giữ được (nó là một cột bình thường sau khi
    # đã tính), NHƯNG phải withColumn NGAY SAU read, trước mọi phép biến đổi nặng.
    # Thêm sau khi đã union/agg -> Spark không biết dòng đó từ file nào nữa -> "".
    df = (spark.read.schema(ORDERS_CORRUPT)
          .option("header", True)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .csv(PATHS)
          .withColumn("source_file", F.input_file_name())      # <-- NGAY ĐÂY
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("ingest_run_id", F.lit(spark.sparkContext.applicationId))
          ).cache()          # cache trước khi filter _corrupt_record — bẫy A24

    total = df.count()
    print("=" * 78)
    print("BỘ BỐN LINEAGE: source_file · ingest_ts · ingest_run_id · _corrupt_record")
    print("=" * 78)
    print(f"Đọc {len(PATHS)} file trong 1 lệnh read -> tổng {total} dòng.\n")

    # --- Dòng nào đến từ file nào? ---
    print("--- Phân bố dòng theo file nguồn (bằng chứng input_file_name hoạt động):")
    (df.groupBy("source_file").count().orderBy("source_file")).show(truncate=False)

    # --- Quarantine: dòng hỏng + đầy đủ lineage ---
    bad = df.filter(F.col("_corrupt_record").isNotNull()).select(
        "source_file", "ingest_ts", "ingest_run_id", "_corrupt_record")
    n_bad = bad.count()
    print(f"--- {n_bad} dòng hỏng. Mỗi dòng biết CHÍNH XÁC nó từ file nào ra:")
    bad.show(10, truncate=60)

    # Ghi quarantine ra Parquet. mode overwrite: quarantine của LẦN CHẠY NÀY.
    # (Ở ingest.py thật, cân nhắc partitionBy(ngày chạy) + append để giữ lịch sử —
    #  nhưng append thì phải chấp nhận trùng khi retry. Đây là một đánh đổi có thật.)
    bad.write.mode("overwrite").parquet(QUARANTINE)
    back = spark.read.parquet(QUARANTINE)
    print(f"--- Đã ghi quarantine -> {QUARANTINE} ({back.count()} dòng). Đọc lại 3 dòng:")
    back.show(3, truncate=False, vertical=True)

    # --- input_file_name() bị "mất" khi nào? Chứng minh cái bẫy ---
    print("=" * 78)
    print("BẪY: thêm input_file_name() SAU khi đã shuffle thì được gì?")
    print("=" * 78)
    late = (df.groupBy("order_status").count()
              .withColumn("source_file_late", F.input_file_name()))
    late.show(3, truncate=False)
    print("  Cột source_file_late rỗng (\"\") — vì sau groupBy, một dòng kết quả được")
    print("  tổng hợp từ NHIỀU dòng của NHIỀU file. Không còn 'file nguồn' nào để trỏ tới.")
    print("  => LUÔN thêm lineage NGAY SAU read, không bao giờ thêm sau.")

    print("""
=========================================================================
CÂU ĐỀ HỎI: cột nào trong bộ ba giúp bạn REPLAY được lỗi?
=========================================================================
  * `source_file`  — CỘT DUY NHẤT cho phép REPLAY.
      Nó trả lời "chạy lại cái gì": mở đúng file đó, tìm đúng dòng đó, sửa nguồn,
      chạy lại đúng file đó. Không có nó thì để tái hiện 1 dòng rác bạn phải chạy
      lại TOÀN BỘ pipeline trên TOÀN BỘ nguồn — và cầu trời là lỗi vẫn còn ở đó.
      (Ở data lake thật, đường dẫn có ngày: .../dt=2018-07-02/part-0003.csv
       -> source_file đồng thời cho biết cả lô nào, giờ nào.)

  * `ingest_ts`    — trả lời "LÚC NÀO tôi nuốt dòng này vào".
      Dùng để: (a) khoanh vùng "mọi dòng rác xuất hiện sau 14:00 hôm qua"
      -> khớp với lúc upstream đổi format; (b) tính tuổi dữ liệu (freshness SLA);
      (c) dọn quarantine cũ. Nó KHÔNG replay được — nó là DẤU THỜI GIAN, không phải ĐỊA CHỈ.

  * `_corrupt_record` — trả lời "dòng đó TRÔNG NHƯ THẾ NÀO".
      Nguyên văn byte. Không có nó thì bạn biết file, biết giờ, mà không biết
      dòng nào trong 99.441 dòng. Nó là TANG VẬT.

  * `ingest_run_id` (tôi thêm, đề không yêu cầu) — applicationId của lần chạy.
      Trả lời "lần chạy nào đẻ ra đống rác này" -> tra thẳng Spark UI / log của
      app đó. Rẻ (1 dòng lit()), và là thứ đầu tiên bạn thèm khi debug lúc 3h sáng.

  BỘ BA TỐI THIỂU = ĐỊA CHỈ (source_file) + THỜI GIAN (ingest_ts) + TANG VẬT (_corrupt_record).
  Thiếu địa chỉ thì không replay. Thiếu thời gian thì không khoanh vùng. Thiếu tang
  vật thì không biết mình đang tìm gì.
""")
    spark.stop()


if __name__ == "__main__":
    main()
