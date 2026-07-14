"""A8 — "Filter càng sớm càng tốt" — TÍN ĐIỀU NÀY CÓ ĐÚNG KHÔNG? Kiểm chứng bằng plan.

Chạy (local là đủ; cluster cũng chạy được nhưng số giây nhiễu hơn):

    make run-local F=labs/mini-project-1/exercises/a08_order_of_transforms.py

BỐN BIẾN THỂ, cùng một kết quả:
  (a) read -> filter(status='delivered') -> join(items) -> groupBy     [filter SỚM, native]
  (b) read -> join(items) -> filter(status='delivered') -> groupBy     [filter MUỘN, native]
  (c) read -> filter(UDF) -> join(items) -> groupBy                    [filter SỚM, Python UDF]
  (d) read -> join(items) -> filter(UDF) -> groupBy                    [filter MUỘN, Python UDF]

CÁCH ĐO — bằng chứng chính KHÔNG phải giây, mà là PLAN:
  * So `optimizedPlan` của (a) và (b): nếu hai chuỗi GIỐNG HỆT NHAU thì Catalyst đã tự
    kéo filter xuống dưới join hộ bạn -> thứ tự bạn gõ KHÔNG quan trọng.
  * Lặp lại với (c)/(d). ĐÂY MỚI LÀ BÀI HỌC THẬT.

TÔI KHÔNG BIẾT TRƯỚC ĐÁP ÁN CỦA (c)/(d) — và cố tình không đoán. Script in plan ra,
plan nói gì thì ghi đúng thế vào report. Đó là điểm khác nhau giữa thí nghiệm và
tụng kinh. (Dự đoán của tôi: Catalyst KHÔNG đẩy được filter chứa PythonUDF xuống dưới
join, vì UDF phải chạy ở tiến trình Python qua node `BatchEvalPython`. Nếu plan cho
thấy ngược lại -> tôi đoán sai, và ghi rõ là mình đã đoán sai.)
"""

import re
import time
import contextlib
import io

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
ITEMS_CSV = "/workspace/data/olist/olist_order_items_dataset.csv"
RUNS = 3

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])

ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType()),
])

# UDF: một hàm Python tầm thường, làm ĐÚNG việc mà `col == 'delivered'` làm.
# Khác biệt duy nhất: Catalyst KHÔNG đọc hiểu được ruột của nó.
is_delivered_udf = F.udf(lambda s: s == "delivered", BooleanType())


def explain_str(df, mode="formatted"):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def opt_plan(df):
    """Optimized logical plan dạng chuỗi — đây là plan SAU khi Catalyst đã xoay xong."""
    return df._jdf.queryExecution().optimizedPlan().toString()


def canon(p):
    """Bỏ id nội bộ (#123L) để so 2 plan về mặt CẤU TRÚC, không phải về mặt số hiệu."""
    return re.sub(r"#\d+L?", "#x", p).strip()


def node_order(plan_text):
    """Trả thứ tự các node đáng quan tâm trong physical plan (từ trên xuống).

    Trong `explain(mode="formatted")`, cái cây ở đầu output đọc từ TRÊN XUỐNG = từ
    NGOÀI VÀO TRONG. Node nằm CÀNG DƯỚI (thụt càng sâu) thì chạy CÀNG SỚM.
    Nên: nếu `BatchEvalPython` (UDF) nằm DƯỚI node join -> UDF chạy TRƯỚC join (được đẩy
    xuống). Nếu nằm TRÊN -> UDF chạy SAU join (không đẩy được).
    """
    keep = ("BatchEvalPython", "SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin",
            "Filter", "HashAggregate", "Exchange")
    out = []
    for line in plan_text.splitlines():
        if line.startswith("=="):          # hết phần cây, sang phần chi tiết
            if out:
                break
            continue
        # BUG ĐÃ SỬA: nhánh TRÁI của join được explain() vẽ bằng tiền tố ':' và ':-'
        # (vd "   :  +- BatchEvalPython (3)"). Bản cũ chỉ lstrip("+-* ") -> ký tự ':'
        # chặn lại, mọi node nhánh trái BỊ BỎ SÓT IM LẶNG — kể cả BatchEvalPython, đúng
        # cái node mà bài này đi tìm. Phải strip cả ':'.
        s = line.strip().lstrip(":+-* ").strip()
        for k in keep:
            if s.startswith(k):
                out.append(k)
                break
    return out


def bench(fn, runs=RUNS):
    times, res = [], None
    for _ in range(runs):
        t0 = time.time()
        res = fn()
        times.append(time.time() - t0)
    return times[0], min(times[1:]), res, times


def main():
    spark = SparkSession.builder.appName("a08-order-of-transforms").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    orders = spark.read.schema(ORDERS_SCHEMA).option("header", True).csv(ORDERS_CSV)
    items = spark.read.schema(ITEMS_SCHEMA).option("header", True).csv(ITEMS_CSV)

    def agg(df):
        """Đuôi chung của cả 4 biến thể: doanh thu theo tháng."""
        return (df
                .groupBy(F.date_format("order_purchase_timestamp", "yyyy-MM").alias("thang"))
                .agg(F.round(F.sum("price"), 2).alias("revenue")))

    # Mỗi biến thể là một HÀM DỰNG, không phải một DataFrame dựng sẵn.
    #
    # VÌ SAO? (bug đã sửa — bản cũ dựng sẵn q_a..q_d rồi collect() 3 lần trên CÙNG một
    # object). Spark GIỮ LẠI file shuffle của một DataFrame và DÙNG LẠI cho action sau
    # nếu đó vẫn là cùng một plan/RDD. Nên lần 2-3 bỏ qua sạch scan+join+partial-agg,
    # chỉ đọc lại vài chục dòng đã shuffle sẵn -> ra 0.04s. Con số đó KHÔNG phải chi phí
    # của query, nó là chi phí đọc file shuffle cũ. Đo lại 4 biến thể như thế thì cả 4
    # đều ~0.04s và bảng trở thành vô nghĩa.
    # Dựng MỚI mỗi lần chạy -> plan mới -> RDD mới -> không có shuffle cũ để bám vào.
    def b_a():   # filter SỚM, biểu thức native
        return agg(orders.filter(F.col("order_status") == "delivered").join(items, "order_id"))

    def b_b():   # filter MUỘN, biểu thức native
        return agg(orders.join(items, "order_id").filter(F.col("order_status") == "delivered"))

    def b_c():   # filter SỚM, Python UDF
        return agg(orders.filter(is_delivered_udf(F.col("order_status"))).join(items, "order_id"))

    def b_d():   # filter MUỘN, Python UDF
        return agg(orders.join(items, "order_id").filter(is_delivered_udf(F.col("order_status"))))

    q_a, q_b, q_c, q_d = b_a(), b_b(), b_c(), b_d()   # chỉ dùng để SO PLAN (không đo giây)

    # -----------------------------------------------------------------------
    # BẰNG CHỨNG 1 — PLAN. Catalyst có tự xếp lại hộ không?
    # -----------------------------------------------------------------------
    pa, pb, pc, pd = opt_plan(q_a), opt_plan(q_b), opt_plan(q_c), opt_plan(q_d)
    native_same = canon(pa) == canon(pb)
    udf_same = canon(pc) == canon(pd)

    print("\n" + "=" * 78)
    print("BẰNG CHỨNG 1 — Optimized Logical Plan: Catalyst có xếp lại hộ bạn không?")
    print("=" * 78)
    print("\n--- (a) filter SỚM (native) ---\n" + pa)
    print("\n--- (b) filter MUỘN (native) ---\n" + pb)
    print("\n>>> plan(a) == plan(b) ? **{}**".format("GIỐNG HỆT" if native_same else "KHÁC"))
    print("\n--- (c) filter SỚM (Python UDF) ---\n" + pc)
    print("\n--- (d) filter MUỘN (Python UDF) ---\n" + pd)
    print("\n>>> plan(c) == plan(d) ? **{}**".format("GIỐNG HỆT" if udf_same else "KHÁC"))

    # -----------------------------------------------------------------------
    # BẰNG CHỨNG 2 — vị trí node UDF so với node JOIN trong PHYSICAL plan.
    # -----------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("BẰNG CHỨNG 2 — `BatchEvalPython` (nơi UDF chạy) nằm TRÊN hay DƯỚI join?")
    print("=" * 78)
    print("\n--- physical plan của (d): join TRƯỚC, UDF SAU ---")
    phys_d = explain_str(q_d)
    print(phys_d)
    print("\n| query | thứ tự node (đọc từ NGOÀI vào TRONG) |")
    print("|---|---|")
    for label, q in [("(a) native sớm", q_a), ("(b) native muộn", q_b),
                     ("(c) UDF sớm", q_c), ("(d) UDF muộn", q_d)]:
        print("| {} | `{}` |".format(label, " → ".join(node_order(explain_str(q)))))
    print("""
Cách đọc bảng trên: node đứng SAU trong danh sách = nằm SÂU HƠN trong cây = chạy SỚM HƠN.
  - Nếu ở (b) `Filter` xuất hiện SAU (= dưới) node join  -> Catalyst ĐÃ đẩy filter xuống
    trước join, dù bạn gõ nó sau.
  - Nếu ở (d) `BatchEvalPython` xuất hiện TRƯỚC (= trên) node join -> UDF KHÔNG đẩy
    xuống được, nó buộc phải chạy trên toàn bộ kết quả join.
""")

    # -----------------------------------------------------------------------
    # BẰNG CHỨNG 3 — giây đồng hồ (phụ thôi, plan mới là chính)
    # -----------------------------------------------------------------------
    print("=" * 78)
    print("BẰNG CHỨNG 3 — thời gian (3 lần, vứt lần 1, lấy min lần 2-3)")
    print("=" * 78)
    print("(Mỗi lần chạy DỰNG LẠI query từ đầu — nếu không, Spark dùng lại file shuffle")
    print(" của lần trước và cả 4 biến thể đều ra ~0.04s giả tạo. Xem chú thích ở b_a().)\n")
    results = {}
    rows = []
    for label, builder, tag in [("(a) filter SỚM · native", b_a, "a08-a"),
                                ("(b) filter MUỘN · native", b_b, "a08-b"),
                                ("(c) filter SỚM · Python UDF", b_c, "a08-c"),
                                ("(d) filter MUỘN · Python UDF", b_d, "a08-d")]:
        sc.setJobGroup(tag, label)
        # dựng MỚI rồi mới collect -> đo đúng chi phí query, không đo chi phí đọc shuffle cũ
        cold, warm, res, allt = bench(lambda f=builder: f().collect())   # ACTION thật
        results[label] = sorted([(r["thang"], r["revenue"]) for r in res])
        rows.append((label, cold, allt, warm))

    base = rows[0][3]
    print("\n| biến thể | lần 1 (lạnh) | lần 2 | lần 3 | ẤM (min 2-3) | so với (a) |")
    print("|---|---|---|---|---|---|")
    for label, cold, allt, warm in rows:
        print("| {} | {:.2f}s | {:.2f}s | {:.2f}s | **{:.2f}s** | {:.2f}× |".format(
            label, cold, allt[1], allt[2], warm, warm / base))

    keys = list(results)
    all_same = all(results[k] == results[keys[0]] for k in keys)
    print("\n4 biến thể ra CÙNG kết quả? **{}**  (nếu KHÔNG thì mọi số trên đây là rác)".format(
        "CÓ" if all_same else "KHÔNG"))
    print("Số tháng: {} · 3 tháng đầu: {}".format(
        len(results[keys[0]]), results[keys[0]][:3]))

    # -----------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("KẾT LUẬN TRUNG THỰC")
    print("=" * 78)
    print("""
1. VỚI BIỂU THỨC NATIVE: plan(a) {} plan(b).
   Nghĩa là quy tắc `PushDownPredicate` của Catalyst đã tự kéo filter xuống DƯỚI join.
   Bạn gõ filter ở đâu cũng thế. Tín điều "filter càng sớm càng tốt" ở đây là ĐÚNG VỀ
   KẾT QUẢ nhưng THỪA VỀ CÔNG SỨC — Spark làm hộ rồi.

2. VỚI PYTHON UDF: plan(c) {} plan(d).
   Vì sao? Vì Catalyst KHÔNG BIẾT ruột con lambda đó làm gì. Nó không dám khẳng định
   hàm này chỉ đụng vào cột `order_status` và không có side-effect, nên KHÔNG dám đổi
   chỗ. UDF bị bọc vào một node riêng (`BatchEvalPython`) — dữ liệu phải rời JVM, sang
   tiến trình Python, quay về. Đẩy một node như thế xuống dưới join là chuyện Catalyst
   né. Kết quả: ở (d), UDF phải chạy trên TOÀN BỘ kết quả join (~112k dòng) thay vì chỉ
   trên bảng orders (~99k dòng) trước khi join.

3. VẬY BÀI HỌC THẬT LÀ GÌ?
   KHÔNG phải "filter sớm đi". Mà là: **BIẾT KHI NÀO CATALYST KHÔNG LÀM HỘ BẠN.**
   Catalyst chỉ tối ưu được thứ nó ĐỌC HIỂU ĐƯỢC. Cứ mỗi lần bạn nhét một hộp đen
   (Python UDF, `df.rdd.map`) vào giữa pipeline, bạn vừa dựng một BỨC TƯỜNG mà tối ưu
   hoá không xuyên qua được. Mọi thứ nằm dưới bức tường đó, bạn phải TỰ tối ưu.

   Hệ quả thực chiến cho `ingest.py`:
     - Ưu tiên tuyệt đối `pyspark.sql.functions` (300+ hàm) trước khi nghĩ tới UDF.
     - Buộc phải dùng UDF -> đặt nó CÀNG MUỘN CÀNG TỐT trong pipeline (sau khi đã filter
       và pruning bằng native), và TỰ TAY filter trước nó. Đừng chờ Catalyst.
     - Đây cũng chính là lý do `PushedFilters` ở bài A6 không bao giờ chứa UDF.
""".format("GIỐNG HỆT NHAU" if native_same else "KHÁC NHAU",
           "GIỐNG HỆT NHAU (tôi đã đoán SAI — ghi rõ vào report!)" if udf_same
           else "KHÁC NHAU"))

    spark.stop()


if __name__ == "__main__":
    main()
