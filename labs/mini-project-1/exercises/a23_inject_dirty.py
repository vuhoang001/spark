"""A23 — Tự chế dữ liệu bẩn, rồi xem Spark bắt được loại nào.
(Bài quan trọng nhất track L5: bạn không xử lý được dữ liệu bẩn nếu chưa biết
nó TRÔNG NHƯ THẾ NÀO ở mức byte.)

Chạy — HAI BƯỚC, bước 1 KHÔNG cần Spark:

  # bước 1: sinh file bẩn (python thuần, chạy trên host hoặc trong container)
  python3 labs/mini-project-1/exercises/a23_inject_dirty.py --gen-only

  # bước 2: đọc file bẩn bằng Spark, lập bảng "loại nào bị bắt / loại nào lọt"
  make run-local F=labs/mini-project-1/exercises/a23_inject_dirty.py

Output dữ liệu: data/dirty/orders_dirty.csv  (A22 và A24 ĐỀU cần file này -> chạy A23 TRƯỚC)

Ý chính (phần đắt nhất): file được đọc HAI LẦN, bằng HAI schema:
  - schema CÓ KIỂU (timestamp)  -> Spark bắt được dòng hỏng, ném vào _corrupt_record
  - schema TOÀN STRING ("bronze cho an toàn") -> dòng LỆCH CỘT lọt sạch, không dấu vết
So hai lần đọc mới thấy: _corrupt_record bắt lỗi CẤU TRÚC + lỗi KIỂU,
nhưng KHÔNG bắt lỗi NGỮ NGHĨA. Đó là lý do tồn tại của data quality gate (A38).
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))


def repo_root():
    """Trong container thì repo nằm ở /workspace; trên host thì suy từ __file__."""
    if os.path.isdir("/workspace/data/olist"):
        return "/workspace"
    return os.path.abspath(os.path.join(HERE, "..", "..", ".."))


ROOT = repo_root()
SRC_CSV = os.path.join(ROOT, "data", "olist", "olist_orders_dataset.csv")
DIRTY_DIR = os.path.join(ROOT, "data", "dirty")
DIRTY_CSV = os.path.join(DIRTY_DIR, "orders_dirty.csv")

HEADER = ('"order_id","customer_id","order_status","order_purchase_timestamp",'
          '"order_approved_at","order_delivered_carrier_date",'
          '"order_delivered_customer_date","order_estimated_delivery_date"')

# ---------------------------------------------------------------------------
# SÁU LOẠI BẨN — MỘT nguồn sự thật duy nhất: src/make_dirty.py
# ---------------------------------------------------------------------------
# TRƯỚC ĐÂY file này có generator RIÊNG, ghi đè lên cùng data/dirty/orders_dirty.csv
# mà src/make_dirty.py cũng ghi — hai generator, hai kết quả khác nhau (2.009 dòng
# vs 99.450 dòng), cùng một đường dẫn. Ai chạy sau thì thắng, và bảng bằng chứng
# results/a23_injected.md (số dòng 224, 447, ... 1785) lặng lẽ trỏ sai chỗ.
#
# Hai generator cho cùng một file là cách bằng chứng phân kỳ mà không ai hay.
# Giờ CHỈ CÒN MỘT: make_dirty.generate(). File này chỉ gọi lại nó.
#
# VÌ SAO 2.000 dòng chứ không phải 99.441: bằng chứng phải KIỂM TRA ĐƯỢC BẰNG TAY.
# 2.000 dòng thì `sed -n '224p' data/dirty/orders_dirty.csv` cho bạn xem tận mắt
# dòng bẩn số 1. 99.441 dòng thì bạn chỉ còn cách tin lời script. Cần bản đầy đủ
# để test scale: python3 labs/mini-project-1/src/make_dirty.py --full
from make_dirty import DIRTY_ROWS, MARKERS, generate as _make_dirty_generate  # noqa: E402


def generate():
    """Sinh data/dirty/orders_dirty.csv qua make_dirty.py (nguồn sự thật DUY NHẤT).

    Deterministic: chạy lại bao nhiêu lần cũng ra file y hệt -> A22/A24/A29 đọc
    được đúng cái file mà bảng bằng chứng đang mô tả.
    """
    dst, report, (n_data, n_total) = _make_dirty_generate()

    print(f"[gen] nguồn    : {SRC_CSV}")
    print(f"[gen] đã ghi   : {dst}")
    print(f"[gen]   tổng   : {n_total} dòng = 1 header + {n_data} data sạch "
          f"+ {len(report)} dòng bẩn")
    print(f"[gen]   (generator = src/make_dirty.py — xem results/a23_injected.md)")
    print()
    print("| # | Loại bẩn | Dòng số (1-based, kể cả header) | Token thô | Token parser | Nguyên văn (60 ký tự đầu) |")
    print("|---|---|---|---|---|---|")
    for i, (code, desc, lineno, ntok, ptok, raw) in enumerate(report, start=1):
        tok = "?" if ntok == -1 else str(ntok)
        pt = "?" if ptok == -1 else str(ptok)
        shown = (raw[:60] if raw else "(dòng trống)")
        print(f"| {i} | `{code}` — {desc} | {lineno} | {tok} | {pt} | `{shown}` |")
    print()
    print("Schema đúng có 8 field. Mọi con số khác 8 ở cột 'token thô' là một cái bẫy CẤU TRÚC.")
    print("Nguy hiểm nhất là dòng có ĐÚNG 8 token mà vẫn sai: L4b (lệch cột).")
    return dst


# ---------------------------------------------------------------------------
# PHẦN SPARK — đọc file bẩn 2 lần bằng 2 schema, xem loại nào bị bắt
# ---------------------------------------------------------------------------
# MARKERS import từ make_dirty (ở trên) — không định nghĩa lại, tránh lệch.


def analyze():
    # import muộn: để `--gen-only` chạy được bằng python3 thuần trên host (không cần pyspark)
    from pyspark.sql import SparkSession, functions as F
    from schemas import ORDERS_ALL_STRING_CORRUPT, ORDERS_CORRUPT

    dirty_uri = "/workspace/data/dirty/orders_dirty.csv"

    spark = SparkSession.builder.appName("a23-dirty").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def read(schema, label):
        # .cache() NGAY tại đây: xem A24 — filter theo _corrupt_record trên một
        # DataFrame đọc thẳng từ file text là cái bẫy Spark cấm.
        df = (spark.read.schema(schema)
              .option("header", True)
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .csv(dirty_uri)).cache()
        n = df.count()          # action -> vật chất hoá cache
        n_bad = df.filter(F.col("_corrupt_record").isNotNull()).count()
        print(f"\n[{label}] tổng dòng đọc được = {n} | dòng vào _corrupt_record = {n_bad}")
        return df

    print("\n" + "=" * 78)
    print("LẦN ĐỌC 1 — schema CÓ KIỂU (timestamp thật) + _corrupt_record")
    print("=" * 78)
    typed = read(ORDERS_CORRUPT, "typed")

    print("\n" + "=" * 78)
    print("LẦN ĐỌC 2 — schema TOÀN STRING (kiểu 'bronze cho an toàn') + _corrupt_record")
    print("=" * 78)
    bronze = read(ORDERS_ALL_STRING_CORRUPT, "bronze")

    # ---- Bảng 6 dòng: loại bẩn nào bị bắt, ở schema nào ----
    print("\n" + "=" * 78)
    print("BẢNG BẰNG CHỨNG — loại bẩn nào Spark BẮT ĐƯỢC, loại nào LỌT?")
    print("=" * 78)
    print()
    print("| Loại bẩn | typed: có trong _corrupt_record? | typed: dòng còn sống? | "
          "bronze(all-string): có trong _corrupt_record? | bronze: order_status đọc ra là gì |")
    print("|---|---|---|---|---|")

    for m in MARKERS:
        # Dòng bẩn có thể: (a) nằm nguyên văn trong _corrupt_record,
        # (b) sống sót ở order_id, (c) biến mất hoàn toàn. Kiểm cả ba.
        t_corrupt = typed.filter(F.col("_corrupt_record").contains(m)).count()
        t_alive = typed.filter(F.col("order_id").contains(m)).count()
        b_corrupt = bronze.filter(F.col("_corrupt_record").contains(m)).count()
        b_status = (bronze.filter(F.col("order_id").contains(m))
                    .select("order_status").limit(1).collect())
        b_status_val = repr(b_status[0][0]) if b_status else "(không có dòng nào)"
        print(f"| `{m}` | {'CÓ (%d)' % t_corrupt if t_corrupt else 'không'} "
              f"| {'CÒN (%d)' % t_alive if t_alive else 'MẤT'} "
              f"| {'CÓ (%d)' % b_corrupt if b_corrupt else 'không'} "
              f"| {b_status_val} |")

    # Dòng trống + header lặp: không có marker id -> đếm kiểu khác
    n_header_dup = typed.filter(F.col("order_id") == "order_id").count()
    # BẪY ĐO LƯỜNG (đã dính, sửa rồi): bản đầu dò bằng contains("order_id,customer_id")
    # -> LUÔN ra 0, vì header thật CÓ NGOẶC KÉP: "order_id","customer_id",...
    # Bảng bằng chứng khi đó in "không" trong khi phần dump nguyên văn ngay bên dưới
    # lại CHO THẤY dòng header nằm trong _corrupt_record. Số trong bảng mâu thuẫn với
    # tang vật thô -> tang vật đúng, bảng sai.
    # Dò bằng tên cột cuối: chuỗi này CHỈ xuất hiện ở dòng header, và không phụ thuộc
    # vào việc file có quote hay không.
    n_header_dup_corrupt = typed.filter(
        F.col("_corrupt_record").contains("order_estimated_delivery_date")).count()
    # Tính sẵn ra BIẾN, không nhét vào trong f-string: Python < 3.12 CẤM dấu \
    # bên trong phần biểu thức của f-string, mà chuỗi này có \" -> SyntaxError.
    # (Python trong container apache/spark:3.4.1 là bản cũ. Đã dính thật, không phải phòng xa.)
    hdr_corrupt_txt = "CÓ" if n_header_dup_corrupt else "không"
    if n_header_dup:
        hdr_alive_txt = 'CÒN (order_id == "order_id": %d dòng)' % n_header_dup
    else:
        hdr_alive_txt = "MẤT"
    print(f"| `L6b_header_lap` | {hdr_corrupt_txt} | {hdr_alive_txt} | — | — |")

    # ---- Nguyên văn vài dòng corrupt: bằng chứng thô ----
    print("\n--- Nguyên văn _corrupt_record (schema typed), tối đa 10 dòng:")
    (typed.filter(F.col("_corrupt_record").isNotNull())
          .select("_corrupt_record").show(10, truncate=False))

    # ---- Cú đấm cuối: dòng L4b ở schema bronze ----
    print("\n--- L4b (lệch cột) đọc bằng schema BRONZE toàn String — nhìn kỹ:")
    bronze.filter(F.col("order_id").contains("dirty04b")).show(1, truncate=False, vertical=True)

    print("\nKẾT LUẬN (đọc bảng trên rồi tự đối chiếu, đừng chép nếu số không khớp):")
    print("  * _corrupt_record bắt được lỗi CẤU TRÚC (thiếu/thừa token) và lỗi KIỂU")
    print("    (chuỗi không ép được sang timestamp) — VÌ hai lỗi đó làm PARSER gãy.")
    print("  * Nó KHÔNG bắt được lỗi NGỮ NGHĨA: 'Sao Paulo' nằm trong order_status là")
    print("    một chuỗi hoàn toàn hợp lệ với parser. Chỉ có CON NGƯỜI biết order_status")
    print("    phải thuộc 8 giá trị {delivered, shipped, canceled, ...}.")
    print("  * => Càng nhiều cột String, _corrupt_record càng vô dụng. Schema chặt là")
    print("    tuyến phòng thủ SỐ MỘT; quality gate (A38) là tuyến SỐ HAI. Cần cả hai.")

    spark.stop()


if __name__ == "__main__":
    generate()                      # luôn sinh lại -> file bẩn là DETERMINISTIC, chạy lại vẫn y hệt
    if "--gen-only" not in sys.argv:
        analyze()
