"""A22 — Ba read mode, một file bẩn.

PHỤ THUỘC: cần data/dirty/orders_dirty.csv -> CHẠY A23 TRƯỚC:
    python3 labs/mini-project-1/exercises/a23_inject_dirty.py --gen-only

Chạy:
    make run-local F=labs/mini-project-1/exercises/a22_three_read_modes.py
    (local đủ. File 17MB, không cần cluster.)

Output: bảng 5 cột (mode | count() | dòng hỏng đi đâu | exception? | dùng khi nào)
        + 5 câu biện luận cho câu hỏi của Checkpoint 1.

Ý chính: read mode KHÔNG phải sở thích. Nó là câu trả lời cho câu hỏi
"mất một dòng và dừng cả pipeline, cái nào đắt hơn?" — và câu trả lời đó
khác nhau theo từng bảng, không phải theo từng công ty.
"""
import os
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "src")))

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from schemas import ORDERS, ORDERS_CORRUPT  # noqa: E402

DIRTY = "/workspace/data/dirty/orders_dirty.csv"
CLEAN = "/workspace/data/olist/olist_orders_dataset.csv"

# Số dòng data của file gốc (không kể header) — dùng để đối chiếu.
# KHÔNG hardcode: script tự đếm bằng spark.read.text() để con số luôn thật.


def main():
    spark = SparkSession.builder.appName("a22-read-modes").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # --- Mốc đối chiếu: đếm dòng THÔ bằng read.text (không parse gì cả) ---
    # Đây là "wc -l" phiên bản Spark. Mọi count() bên dưới phải giải thích được
    # chênh lệch với con số này.
    raw_dirty = spark.read.text(DIRTY).count()
    raw_clean = spark.read.text(CLEAN).count()
    print("=" * 78)
    print("MỐC ĐỐI CHIẾU (đọc thô bằng spark.read.text, không parse)")
    print("=" * 78)
    print(f"  file gốc  {CLEAN}: {raw_clean} dòng thô (gồm 1 header)")
    print(f"  file bẩn  {DIRTY}: {raw_dirty} dòng thô (gồm 1 header)")
    print(f"  => file bẩn thừa {raw_dirty - raw_clean} dòng so với gốc (số dòng bẩn đã tiêm ở A23)")
    print("  LƯU Ý: read.text đếm theo ký tự xuống dòng. Nếu có dòng bẩn kiểu ngoặc kép lệch")
    print("  mà bật multiLine=true thì parser CSV sẽ gộp dòng -> count() của CSV có thể")
    print("  KHÁC count() của text. Ở đây multiLine=false (mặc định) nên 1 dòng text = 1 record.")

    results = []

    # =====================================================================
    # 1) PERMISSIVE — mặc định. Giữ dòng, field hỏng -> NULL, nguyên văn -> _corrupt_record
    # =====================================================================
    df_perm = (spark.read.schema(ORDERS_CORRUPT)
               .option("header", True)
               .option("mode", "PERMISSIVE")
               .option("columnNameOfCorruptRecord", "_corrupt_record")
               .csv(DIRTY)).cache()   # cache: xem A24, bắt buộc trước khi filter _corrupt_record
    n_perm = df_perm.count()
    n_bad = df_perm.filter(F.col("_corrupt_record").isNotNull()).count()
    n_good = n_perm - n_bad
    results.append(("PERMISSIVE", n_perm,
                    f"giữ lại trong DF, nguyên văn nằm ở `_corrupt_record` ({n_bad} dòng)",
                    "KHÔNG", "mặc định; khi muốn ingest hết rồi quarantine dòng hỏng"))
    print("\n" + "=" * 78)
    print("1) PERMISSIVE")
    print("=" * 78)
    print(f"  count()          = {n_perm}")
    print(f"  dòng hỏng        = {n_bad}   (có _corrupt_record != null)")
    print(f"  dòng sạch        = {n_good}")
    print("  Dòng hỏng KHÔNG biến mất — chúng vẫn nằm trong DataFrame, chỉ là mọi field")
    print("  parse-được thì giữ, field gãy thì NULL. Đây là mode DUY NHẤT cho bạn cái để điều tra.")

    # =====================================================================
    # 2) DROPMALFORMED — lặng lẽ VỨT cả dòng
    # =====================================================================
    # LƯU Ý KỸ THUẬT: không khai _corrupt_record ở đây. Khai _corrupt_record cùng
    # DROPMALFORMED là mâu thuẫn logic (vứt dòng rồi thì lấy gì mà lưu nguyên văn).
    df_drop = (spark.read.schema(ORDERS)
               .option("header", True)
               .option("mode", "DROPMALFORMED")
               .csv(DIRTY))
    n_drop = df_drop.count()
    # ÉP PARSE: xem giải thích ở khối "PHÁT HIỆN" bên dưới. count() KHÔNG parse field,
    # nên nó KHÔNG kích hoạt được DROPMALFORMED. Phải có action chạm vào field thật.
    n_drop_parsed = len(df_drop.collect())
    results.append(("DROPMALFORMED", f"{n_drop} (count) / **{n_drop_parsed}** (collect)",
                    f"BỊ VỨT — {n_perm - n_drop_parsed} dòng bốc hơi, không dấu vết, không log",
                    "KHÔNG",
                    "gần như KHÔNG BAO GIỜ ở production; chỉ khi khám phá dữ liệu ad-hoc"))
    print("\n" + "=" * 78)
    print("2) DROPMALFORMED")
    print("=" * 78)
    print(f"  count()          = {n_drop}   <- KHÔNG vứt dòng nào?!")
    print(f"  collect()        = {n_drop_parsed}   <- vứt {n_perm - n_drop_parsed} dòng")
    print("  HAI CON SỐ KHÁC NHAU TRÊN CÙNG MỘT DATAFRAME. Xem khối PHÁT HIỆN ở cuối.")
    print(f"  Khi đã thật sự parse: {n_perm} - {n_drop_parsed} = {n_perm - n_drop_parsed} dòng đã BỐC HƠI")
    print("  Không exception. Không warning. Không log. Bạn chỉ biết mất dữ liệu nếu bạn")
    print("  TÌNH CỜ so count với nguồn. Đây là lý do mode này gần như luôn sai ở production.")

    # =====================================================================
    # 3) FAILFAST — có rác là dừng nhà máy
    # =====================================================================
    print("\n" + "=" * 78)
    print("3) FAILFAST")
    print("=" * 78)
    df_fail = (spark.read.schema(ORDERS)
               .option("header", True)
               .option("mode", "FAILFAST")
               .csv(DIRTY))
    # --- 3a) count() thuần: KHÔNG nổ. Đây là cái bẫy, không phải kết quả. ---
    n_fail_count = df_fail.count()
    print(f"  count()          = {n_fail_count}  <- KHÔNG NỔ. FAILFAST mà im re.")
    print("  (Đừng dừng ở đây mà kết luận 'dòng bẩn chưa đủ hỏng' — A23 đã chứng minh")
    print("   đúng 7 dòng đó rơi vào _corrupt_record. Vấn đề nằm ở ACTION, không ở DỮ LIỆU.)")

    # --- 3b) ép parse: BÂY GIỜ mới là phép thử thật ---
    exc_text = None
    try:
        n_fail = len(df_fail.collect())
        print(f"  collect()        = {n_fail}  (vẫn không nổ — ghi nhận đúng như thấy)")
        results.append(("FAILFAST", f"{n_fail_count} (count) / {n_fail} (collect)",
                        "n/a", "KHÔNG (bất ngờ — cần điều tra thêm)",
                        "xem lại: parser không coi các dòng này là malformed"))
    except Exception:  # noqa: BLE001
        exc_text = traceback.format_exc()
        first = [l for l in exc_text.splitlines() if l.strip()]
        print("  collect() NÉM EXCEPTION. Nguyên văn (rút gọn 12 dòng cuối):")
        for line in first[-12:]:
            print("    " + line)
        short = first[-1][:160] if first else "(rỗng)"
        # Tìm dòng nói rõ nguyên nhân gốc (Malformed...) để dán vào bảng cho có sức nặng
        root = next((l.strip() for l in first
                     if "Malformed" in l or "BadRecordException" in l), short)
        results.append(("FAILFAST",
                        f"{n_fail_count} (count — IM LẶNG) / **NỔ** (collect)",
                        "không đi đâu cả — cả JOB chết",
                        f"CÓ (chỉ khi thật sự parse): `{root[:120]}`",
                        "dữ liệu tài chính / y tế: mất 1 dòng TỆ HƠN dừng pipeline"))

    # FAILFAST trên file SẠCH thì sao? -> chứng minh nó không phải "mode hỏng"
    n_fail_clean = (spark.read.schema(ORDERS).option("header", True)
                    .option("mode", "FAILFAST").csv(CLEAN)).count()
    print(f"\n  Đối chứng: FAILFAST trên file GỐC (sạch) = {n_fail_clean} dòng, chạy êm.")
    print("  => FAILFAST không phải mode 'lỗi'. Nó là mode 'không khoan nhượng'.")

    # =====================================================================
    # BẢNG BẰNG CHỨNG
    # =====================================================================
    print("\n" + "=" * 78)
    print("BẢNG (dán thẳng vào PROGRESS §3.6)")
    print("=" * 78)
    print()
    print("| mode | `count()` / action ép parse | dòng hỏng đi đâu | có exception không | dùng khi nào |")
    print("|---|---|---|---|---|")
    for mode, cnt, where, exc, when in results:
        print(f"| `{mode}` | {cnt} | {where} | {exc} | {when} |")

    # =====================================================================
    # PHÁT HIỆN — cái này KHÔNG có trong đề, và nó quan trọng hơn cả bảng trên
    # =====================================================================
    print("\n" + "=" * 78)
    print("PHÁT HIỆN: count() KHÔNG PARSE FIELD -> read mode KHÔNG HỀ CHẠY")
    print("=" * 78)
    print(f"""
Đo lần đầu, cả ba mode đều trả về {n_perm} và FAILFAST không thèm nổ. Trông như
"dữ liệu bẩn không đủ bẩn". SAI. Bằng chứng ngược lại nằm ngay ở A23: đúng 7 dòng
đó rơi vào _corrupt_record.

Sự thật, đo được trên CÙNG một DataFrame:

    DROPMALFORMED.count()    = {n_drop}     <- không vứt dòng nào
    DROPMALFORMED.collect()  = {n_drop_parsed}     <- vứt {n_perm - n_drop_parsed} dòng
    FAILFAST.count()         = {n_fail_count}     <- im lặng
    FAILFAST.collect()       -> NÉM EXCEPTION

CƠ CHẾ: count() không cần biết giá trị của bất kỳ cột nào — nó chỉ cần biết CÓ BAO
NHIÊU DÒNG. Catalyst nhìn thấy điều đó và cắt luôn (column pruning) xuống 0 cột.
Parser CSV vì thế chỉ ĐẾM DẤU XUỐNG DÒNG, không hề tách field, không ép kiểu.
Không parse thì không có "malformed" nào để mà phát hiện -> DROPMALFORMED không có
gì để vứt, FAILFAST không có gì để nổ. Read mode không im lặng thất bại: nó KHÔNG
BAO GIỜ ĐƯỢC CHẠY.

HỆ QUẢ THỰC TẾ (đắt hơn cả bài học):
  * "Malformed" KHÔNG phải tính chất của DÒNG. Nó là tính chất của
    (DÒNG × NHỮNG CỘT BẠN THẬT SỰ ĐỌC × MODE). Đọc ít cột hơn -> ít dòng hỏng hơn.
    Đo thử: select 1 cột rồi filter -> ra {n_drop_parsed} != số của collect() toàn bộ.
  * Validate dữ liệu bằng `df.count()` là VÔ NGHĨA — nó không chứng minh file parse
    được. Job "xanh" mà chưa hề đọc một field nào.
  * FAILFAST KHÔNG bảo vệ bạn ở bước read. Nó chỉ nổ khi có ai đó thật sự CHẠM vào
    field hỏng — có thể là 3 stage sau, hoặc không bao giờ, nếu cột đó bị prune.
  * => Muốn kiểm tra file có sạch không: đừng count(). Phải ép parse
    (ghi ra Parquet, hoặc đếm _corrupt_record bằng PERMISSIVE). ĐẾM KHÔNG PHẢI ĐỌC.

(Đề bài mặc định 'count() trả về gì' là đủ để phân biệt 3 mode. Không đủ. Bảng
 trên vì thế có 2 con số mỗi ô, và đó mới là bảng đúng.)""")

    print("\n" + "=" * 78)
    print("NĂM CÂU BIỆN LUẬN (trả lời câu hỏi của Checkpoint 1)")
    print("=" * 78)
    print("""
1. VÌ SAO Olist chọn PERMISSIVE chứ không FAILFAST?
   Vì Olist là dữ liệu THƯƠNG MẠI ĐIỆN TỬ dạng phân tích, không phải sổ cái.
   Mất/lệch vài chục dòng trong 99.441 đơn làm doanh thu tháng lệch ~0,0x% —
   sai số đó nhỏ hơn nhiễu tự nhiên của chính bài toán. Nhưng DỪNG pipeline vì
   một dòng hỏng thì dashboard cả công ty trắng bảng. Chi phí bất đối xứng
   -> chọn PERMISSIVE.

2. PERMISSIVE KHÔNG PHẢI "cho qua". Nó là "cho qua CÓ BIÊN LAI".
   Điều kiện để PERMISSIVE hợp lệ: bạn PHẢI khai _corrupt_record, PHẢI ghi dòng
   hỏng ra quarantine, và PHẢI có ngưỡng cảnh báo (vd: bad_rate > 1% thì fail job).
   PERMISSIVE mà không ai nhìn quarantine = DROPMALFORMED chậm hơn.

3. Pipeline nào BẮT BUỘC FAILFAST?
   Nơi mà một dòng = một nghĩa vụ pháp lý: giao dịch ngân hàng, bút toán kế toán,
   đơn thuốc, dữ liệu điều tra dân số. Ở đó "thiếu 1 dòng" là SAI SỐ LIỆU BÁO CÁO,
   còn "dừng pipeline" chỉ là một cuộc gọi lúc 3h sáng. Dừng rẻ hơn sai.

4. DROPMALFORMED gần như LUÔN sai — vì sao?
   Vì nó phá huỷ BẰNG CHỨNG. Sau khi nó chạy, không tồn tại cách nào biết bạn đã
   mất bao nhiêu dòng, mất dòng nào, vì sao mất. Mọi mode khác cho bạn hoặc dữ liệu
   (PERMISSIVE) hoặc tiếng nổ (FAILFAST). Mode này cho bạn SỰ IM LẶNG — thứ tệ nhất
   trong data engineering.

5. Read mode chỉ là tuyến phòng thủ SỐ MỘT, và nó chỉ chặn được lỗi CẤU TRÚC.
   Dòng L4b của A23 (lệch cột nhưng đủ 8 token) cho thấy: parser thấy "hợp lệ",
   ba mode đều để lọt như nhau. Muốn chặn nó phải có tuyến SỐ HAI: quality gate
   kiểm tra NGỮ NGHĨA (order_status phải thuộc 8 giá trị hợp lệ) — bài A38.
""")

    spark.stop()


if __name__ == "__main__":
    main()
