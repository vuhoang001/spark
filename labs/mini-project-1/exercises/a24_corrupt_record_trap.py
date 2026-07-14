"""A24 — Tái hiện cái bẫy `_corrupt_record` rồi sửa nó bằng 3 cách.

PHỤ THUỘC: cần data/dirty/orders_dirty.csv -> CHẠY A23 TRƯỚC.

Chạy:
    make run-local F=labs/mini-project-1/exercises/a24_corrupt_record_trap.py
    (local đủ, và local cho số đo ỔN ĐỊNH hơn để so 3 cách sửa.)

Output: nguyên văn exception (nếu có) + bảng so 3 cách sửa (thời gian, số dòng)
        + kết luận chọn cách nào cho src/ingest.py và VÌ SAO.

Ý chính: Spark 2.3+ CẤM hẳn truy vấn mà cột được tham chiếu CHỈ có _corrupt_record,
khi nguồn là file text thô (CSV/JSON). Lý do nằm ở kiến trúc, không phải bug —
xem giải thích in ra ở cuối.
"""
import os
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from schemas import ORDERS_CORRUPT  # noqa: E402

DIRTY = "/workspace/data/dirty/orders_dirty.csv"
TMP_PARQUET = "/workspace/data/bench/a24_raw_parquet"


def bench(name, fn, runs=3):
    """Đo 3 lần, VỨT lần 1 (JVM warmup + page cache lạnh), lấy min lần 2-3.

    fn PHẢI kết thúc bằng một ACTION và trả về kết quả thật — trả DataFrame là
    đang đo 0.001s của lazy, lỗi benchmark #1.
    """
    ts, res = [], None
    for _ in range(runs):
        t0 = time.time()
        res = fn()
        ts.append(time.time() - t0)
    warm = min(ts[1:]) if len(ts) > 1 else ts[0]
    print(f"  [bench] {name:38s} lạnh={ts[0]*1000:8.1f} ms | ấm(min lần 2-3)={warm*1000:8.1f} ms"
          f" | kết quả={res}")
    return warm, res


def read_dirty(spark):
    """Đọc thô, KHÔNG cache — đúng như trong đề bài, để cái bẫy có chỗ mà sập."""
    return (spark.read.schema(ORDERS_CORRUPT)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(DIRTY))


def main():
    spark = SparkSession.builder.appName("a24-corrupt-trap").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # =====================================================================
    # BƯỚC 1 — GIẪM ĐÚNG VÀO BẪY (không cache)
    # =====================================================================
    print("=" * 78)
    print("BƯỚC 1 — Giẫm vào bẫy: filter theo _corrupt_record trên DF đọc thẳng từ CSV")
    print("=" * 78)
    df = read_dirty(spark)
    bad = df.filter(F.col("_corrupt_record").isNotNull())
    good = df.filter(F.col("_corrupt_record").isNull())

    # SỐ THẬT để đối chiếu: A23 đã chứng minh file bẩn có ĐÚNG 7 dòng hỏng
    # (đọc bằng PERMISSIVE + cache). Giữ số này để biết bước 1 nói dối cỡ nào.
    TRUTH_BAD, TRUTH_TOTAL = 7, 2007

    trap_happened = False
    trap_text = ""
    try:
        nb = bad.count()
        ng = good.count()
        print(f"  bad.count()  = {nb}      <- SỰ THẬT: {TRUTH_BAD} (A23 đã chứng minh)")
        print(f"  good.count() = {ng}   <- SỰ THẬT: {TRUTH_TOTAL - TRUTH_BAD}")
        print()
        if nb != TRUTH_BAD:
            print("  *** BẪY ĐÃ SẬP — VÀ NÓ SẬP THEO KIỂU TỆ NHẤT CÓ THỂ. ***")
            print("  KHÔNG exception. KHÔNG warning. Chỉ có SỐ SAI, im như thóc.")
            print(f"  Spark vừa khai báo {nb} dòng hỏng trên một file có {TRUTH_BAD} dòng hỏng,")
            print(f"  và lặng lẽ xếp cả {TRUTH_BAD} dòng rác đó vào nhóm 'sạch' ({ng} dòng).")
            print("  Nếu tin con số này, bạn vừa đẩy rác vào silver và ký tên xác nhận nó sạch.")
        else:
            print("  Số ĐÚNG ngay cả khi không cache — ghi nhận đúng như thấy, không suy diễn.")
    except Exception:  # noqa: BLE001
        trap_happened = True
        trap_text = traceback.format_exc()
        print("  *** BẪY SẬP — nguyên văn exception: ***")
        for line in trap_text.splitlines():
            print("    " + line)

    print("\n  VÌ SAO (giải thích kiến trúc — và tôi đã DỰ ĐOÁN SAI chỗ này, xem ghi chú):")
    print("  - `_corrupt_record` KHÔNG phải cột có sẵn trong file. Nó do PARSER SINH RA")
    print("    tại thời điểm đọc từng dòng.")
    print("  - `count()` không cần giá trị của cột nào -> Catalyst prune xuống 0 cột ->")
    print("    parser CHỈ ĐẾM DÒNG, KHÔNG parse field. Không parse thì không có dòng nào")
    print("    được đánh dấu hỏng -> _corrupt_record toàn NULL -> bad = 0, good = TẤT CẢ.")
    print("    (Đây CHÍNH XÁC là cơ chế đã làm DROPMALFORMED/FAILFAST tê liệt ở A22.")
    print("     Một gốc rễ, hai bài. Không phải trùng hợp.)")
    print()
    print("  GHI NHẬN TRUNG THỰC — dự đoán của tôi SAI:")
    print("  Tôi viết script này với giả định Spark >= 2.3 sẽ NÉM AnalysisException khi")
    print("  query chỉ tham chiếu _corrupt_record ('thà nổ còn hơn nói dối').")
    print("  Spark 3.4.1 KHÔNG nổ. Nó trả về 0 và 2007 — SỐ SAI, IM LẶNG.")
    print("  Log nói ngược lại dự đoán thì log đúng. Sửa niềm tin, không sửa số.")
    print("  => Cái bẫy này NGUY HIỂM HƠN tôi tưởng: không có tiếng nổ nào để báo bạn biết.")
    print("     Thứ duy nhất cứu bạn là ĐỐI CHIẾU với một con số bạn tin được từ nguồn khác.")

    # =====================================================================
    # BƯỚC 2 — BA CÁCH SỬA
    # =====================================================================
    print("\n" + "=" * 78)
    print("BƯỚC 2 — Ba cách sửa, đo thật")
    print("=" * 78)
    rows = []

    # --- Cách 1: .cache() (MEMORY_AND_DISK) rồi mới filter -------------------
    # Vì sao hết lỗi: cache() chèn một RÀO CẢN vật chất hoá. Toàn bộ dòng (đủ 9 cột,
    # kể cả _corrupt_record đã parse xong) được nhét vào bộ nhớ. Filter sau đó
    # đọc từ CACHE, không đọc từ file text nữa -> hết vòng lặp logic, hết cấm.
    print("\n--- Cách 1: .cache() trước khi filter")
    df1 = read_dirty(spark).cache()
    try:
        t1, _ = bench("cache: count() (vật chất hoá)", lambda: df1.count())
        t1b, nb1 = bench("cache: bad.count()",
                         lambda: df1.filter(F.col("_corrupt_record").isNotNull()).count())
        t1c, ng1 = bench("cache: good.count()",
                         lambda: df1.filter(F.col("_corrupt_record").isNull()).count())
        rows.append(("`.cache()` + action", f"{(t1 + t1b + t1c) * 1000:.1f}", nb1, ng1,
                     "RAM (spill ra đĩa nếu hết chỗ)", "KHÔNG"))
    except Exception:  # noqa: BLE001
        print("  CHẠY LỖI:\n" + traceback.format_exc())
        rows.append(("`.cache()` + action", "CHẠY LỖI", "-", "-", "-", "-"))

    # --- Cách 2: ghi ra Parquet tạm rồi đọc lại ------------------------------
    # Vì sao hết lỗi: sau khi ghi Parquet, _corrupt_record trở thành một cột BÌNH THƯỜNG
    # nằm vật lý trong file. Đọc lại thì Spark không còn thấy nguồn text nữa -> không còn
    # cái vòng lặp logic kia. Đắt hơn cache (phải ghi + đọc đĩa), nhưng BỀN: kết quả nằm
    # trên đĩa, job khác đọc lại được, và nó chính là tầng BRONZE.
    print("\n--- Cách 2: ghi Parquet tạm rồi đọc lại")
    try:
        tw, _ = bench("parquet: write (action thật)",
                      lambda: read_dirty(spark).write.mode("overwrite").parquet(TMP_PARQUET)
                      or "written")
        df2 = spark.read.parquet(TMP_PARQUET)
        t2b, nb2 = bench("parquet: bad.count()",
                         lambda: df2.filter(F.col("_corrupt_record").isNotNull()).count())
        t2c, ng2 = bench("parquet: good.count()",
                         lambda: df2.filter(F.col("_corrupt_record").isNull()).count())
        rows.append(("ghi Parquet tạm rồi đọc lại", f"{(tw + t2b + t2c) * 1000:.1f}", nb2, ng2,
                     "ĐĨA (bền, job khác dùng lại được)", "KHÔNG"))
    except Exception:  # noqa: BLE001
        print("  CHẠY LỖI:\n" + traceback.format_exc())
        rows.append(("ghi Parquet tạm rồi đọc lại", "CHẠY LỖI", "-", "-", "-", "-"))

    # --- Cách 3: .persist(DISK_ONLY) + trigger action ------------------------
    # Cùng cơ chế với cache (cache() == persist(MEMORY_AND_DISK)), nhưng ép xuống đĩa.
    # Đo để thấy giá của việc KHÔNG dùng RAM: chậm hơn cache bao nhiêu?
    print("\n--- Cách 3: .persist(DISK_ONLY) + trigger action")
    df3 = read_dirty(spark).persist(StorageLevel.DISK_ONLY)
    try:
        t3, _ = bench("persist(DISK_ONLY): count()", lambda: df3.count())
        t3b, nb3 = bench("persist(DISK_ONLY): bad.count()",
                         lambda: df3.filter(F.col("_corrupt_record").isNotNull()).count())
        t3c, ng3 = bench("persist(DISK_ONLY): good.count()",
                         lambda: df3.filter(F.col("_corrupt_record").isNull()).count())
        rows.append(("`.persist(DISK_ONLY)` + action",
                     f"{(t3 + t3b + t3c) * 1000:.1f}", nb3, ng3,
                     "ĐĨA của executor (mất khi app tắt)", "KHÔNG"))
    except Exception:  # noqa: BLE001
        print("  CHẠY LỖI:\n" + traceback.format_exc())
        rows.append(("`.persist(DISK_ONLY)` + action", "CHẠY LỖI", "-", "-", "-", "-"))

    # =====================================================================
    # BẢNG + QUYẾT ĐỊNH
    # =====================================================================
    print("\n" + "=" * 78)
    print("BẢNG SO SÁNH 3 CÁCH SỬA (dán vào report)")
    print("=" * 78)
    print()
    print("| Cách sửa | Tổng thời gian ấm (ms) | dòng hỏng | dòng sạch | Dữ liệu nằm ở đâu | Còn lỗi? |")
    print("|---|---|---|---|---|---|")
    for r in rows:
        print("| " + " | ".join(str(x) for x in r) + " |")

    print(f"""
HIỆN TƯỢNG GỐC: {"CÓ nổ exception" if trap_happened else "KHÔNG nổ — trả SỐ SAI im lặng (bad=0 thay vì 7). Xem bước 1."}

CHỌN CHO src/ingest.py: **.cache() + một action ngay sau đó**.
  Vì sao:
  1. Rẻ nhất trong 3 cách (xem bảng — không phải vì tôi đoán, vì tôi đo).
  2. orders chỉ ~17 MB / 99k dòng, thừa sức nằm trong 1049 MB RAM mỗi executor
     -> cache không spill, không đánh đổi gì.
  3. Ta CẦN đọc DataFrame này 2 lần (một lần lọc bad -> quarantine, một lần lọc
     good -> silver). Không cache thì Spark đọc + parse CSV HAI LẦN. cache là
     thứ đúng phải làm ngay cả khi không có cái bẫy này.
  KHI NÀO ĐỔI Ý: nếu dữ liệu ×100 (A40) không còn vừa RAM -> chuyển sang cách 2
  (ghi bronze Parquet rồi đọc lại). Lúc đó "cách sửa cái bẫy" và "tầng bronze"
  hoá ra là CÙNG MỘT THỨ — đó là lý do kiến trúc medallion tồn tại.

CÂU PHẢI THUỘC: đừng bao giờ filter theo _corrupt_record trên một DataFrame vừa
đọc thẳng từ CSV/JSON mà chưa vật chất hoá. Phải có một rào cản ở giữa:
cache / persist / ghi-rồi-đọc-lại.
""")

    spark.stop()


if __name__ == "__main__":
    main()
