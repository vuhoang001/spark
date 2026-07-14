"""A14 — AQE bật/tắt: cùng một query, hai số phận.

Chạy:
    make run F=labs/mini-project-1/exercises/a14_aqe_on_off.py        # CLUSTER (khuyến nghị — số khớp
                                                                     #   môi trường đo §3.0: 6 core)
    make run-local F=labs/mini-project-1/exercises/a14_aqe_on_off.py  # local[2]: kết luận giống,
                                                                     #   thời gian khác

Output: bảng 2 dòng cho PROGRESS.md §3.3 (số task sau shuffle | số task xử lý 0 record | thời gian).

────────────────────────────────────────────────────────────────────────
THÍ NGHIỆM
    groupBy("order_status") — chỉ có ~8 giá trị khác nhau trong toàn bộ Olist.
    spark.sql.shuffle.partitions = 200 (mặc định của Spark, KHÔNG ai chỉnh).

    AQE OFF: Spark quyết số partition sau shuffle NGAY LÚC LẬP PLAN, khi chưa
             biết dữ liệu to nhỏ ra sao -> nó dùng con số cứng 200. Kết quả:
             200 task cho 8 nhóm. ~192 task nhận 0 record: vẫn được schedule,
             vẫn khởi tạo, vẫn mở file shuffle, vẫn báo cáo về driver — để làm
             đúng con số 0.
    AQE ON:  Spark chạy xong stage map, NHÌN kích thước THẬT của từng partition
             (thống kê MapOutputStatistics), rồi mới quyết: "200 mảnh này bé
             tí, gộp lại" -> AQEShuffleRead / coalesced. Số task sau shuffle
             tụt xuống còn vài cái.

    => AQE = hoãn quyết định cho đến khi có số liệu thật. Đó là toàn bộ ý tưởng.
────────────────────────────────────────────────────────────────────────

Đo thời gian theo luật của repo: chạy 3 lần, VỨT lần 1 (JVM warmup), lấy min
lần 2–3. DataFrame được dựng LẠI mỗi lần chạy — nếu dùng lại cùng một object,
Spark sẽ tái dùng shuffle file cũ (stage SKIPPED, bài A12) và số đo thành rác.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StringType, StructField, StructType, TimestampType)

import uiprobe

SRC = "/workspace/data/olist/olist_orders_dataset.csv"
RUNS = 3          # lần 1 là warmup, chỉ lấy min(lần 2, lần 3)
SHUFFLE_PARTS = "200"

ORDERS = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])


def build(spark):
    """Dựng LẠI query từ đầu mỗi lần gọi — xem docstring (chống shuffle reuse)."""
    orders = spark.read.csv(SRC, header=True, schema=ORDERS)
    return orders.groupBy("order_status").count()


def shuffle_read_stage(summary):
    """Stage NẰM SAU shuffle = stage có đọc shuffle. Đây là stage ta cần soi.

    (Stage trước shuffle luôn có số task = số partition lúc đọc file — không
    liên quan gì đến shuffle.partitions, đừng nhìn nhầm chỗ.)
    """
    cands = [r for r in summary["stage_rows"]
             if not r["skipped"] and r["shuffleReadBytes"] > 0]
    return cands[-1] if cands else None


def zero_record_tasks(spark, stage_row):
    """Đếm task xử lý ĐÚNG 0 record — nguồn: taskList, chính là cột
    'Shuffle Read Size / Records' của từng task trên tab Stages.

    BẪY: taskList mặc định chỉ trả 100 task. Stage có 200 task mà không truyền
    length -> đếm được 100, kết luận "chỉ 92 task rỗng" -> SAI. uiprobe đã xin
    sẵn length=2000.
    """
    tasks = uiprobe.task_list(spark, stage_row["stageId"], stage_row["attemptId"])
    n_zero = n_seen = 0
    for t in tasks:
        m = t.get("taskMetrics") or {}
        srm = m.get("shuffleReadMetrics") or {}
        recs = srm.get("recordsRead", 0)
        n_seen += 1
        if recs == 0:
            n_zero += 1
    return n_zero, n_seen


def measure(spark, aqe):
    """Chạy query RUNS lần với một chế độ AQE. Trả về số đo + stage sau shuffle."""
    spark.conf.set("spark.sql.adaptive.enabled", "true" if aqe else "false")
    tag = "aqe-on" if aqe else "aqe-off"

    walls = []
    last_df = None
    for i in range(RUNS):
        group = "%s-run%d" % (tag, i)
        spark.sparkContext.setJobGroup(group, "A14 %s lần %d" % (tag, i))
        df = build(spark)
        t = time.time()
        rows = df.collect()          # ACTION THẬT. Trả DataFrame về = đo lazy = 0.001s = sai.
        walls.append((time.time() - t) * 1000)
        last_df = df
        last_group = group

    summary = uiprobe.summarize_group(spark, last_group)
    st = shuffle_read_stage(summary)
    n_zero, n_seen = zero_record_tasks(spark, st) if st else (0, 0)

    # Plan CUỐI CÙNG: chỉ có sau khi query đã CHẠY. Trước khi chạy, AQE chỉ hiện
    # "AdaptiveSparkPlan isFinalPlan=false" — chưa có gì để xem. Đây là lý do
    # explain() của query AQE phải đọc SAU action.
    plan = last_df._jdf.queryExecution().executedPlan().toString()

    return {
        "tag": tag, "walls": walls,
        "warm_ms": min(walls[1:]) if len(walls) > 1 else walls[0],
        "stage": st, "n_zero": n_zero, "n_seen": n_seen,
        "duration_ms": summary["duration_ms"],
        "summary": summary, "plan": plan, "n_groups": len(rows),
    }


def main():
    spark = (SparkSession.builder.appName("a14-aqe-on-off")
             .config("spark.sql.shuffle.partitions", SHUFFLE_PARTS)
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    uiprobe.wait_for_executors(spark, expected=2)

    print("\n" + "=" * 100)
    print("A14 — AQE ON/OFF  (groupBy order_status: ~8 nhóm, shuffle.partitions=%s)" % SHUFFLE_PARTS)
    print("=" * 100)
    print("master=%s | defaultParallelism=%d | Spark UI: %s" % (
        sc.master, sc.defaultParallelism, sc.uiWebUrl))

    # THỨ TỰ ĐO LÀ MỘT BIẾN GÂY NHIỄU — chạy `... a14_aqe_on_off.py reverse` để
    # đo AQE-ON trước, OFF sau.
    #
    # Vì sao phải có cờ này: chế độ nào chạy TRƯỚC sẽ gánh toàn bộ chi phí khởi
    # động (JVM JIT chưa ấm, page cache của OS chưa có file CSV, executor vừa
    # sinh). Lần đo đầu tiên của tôi cho OFF=695ms, ON=157ms — nhưng OFF chạy
    # trước và ôm trọn cú cold-start (lần 1 của nó: 4028ms, trong khi lần 1 của
    # ON đã là 217ms). Nếu tin luôn con số đó thì ta đang tính công của JVM
    # warmup thành công của AQE.
    #
    # Đảo thứ tự là cách rẻ nhất để tách hai thứ đó ra. Con số ĐÁNG TIN của bài
    # này là SỐ TASK (200 -> 1) — nó không đổi dù chạy thứ tự nào. Còn số giây
    # thì phải kiểm tra rồi mới được phát biểu.
    reverse = "reverse" in sys.argv
    if reverse:
        print("*** THỨ TỰ ĐẢO: đo AQE-ON trước, AQE-OFF sau (kiểm tra nhiễu warmup) ***")
        on = measure(spark, aqe=True)
        off = measure(spark, aqe=False)
    else:
        off = measure(spark, aqe=False)
        on = measure(spark, aqe=True)

    for r in (off, on):
        print("\n" + "-" * 100)
        print(">>> %s | wall 3 lần: %s ms | ẤM (min lần 2-3): %.0f ms" % (
            r["tag"].upper(),
            ", ".join("%.0f" % w for w in r["walls"]), r["warm_ms"]))
        print("-" * 100)
        uiprobe.print_stage_table(r["summary"])

    # ------------------------------------------------------- BẢNG §3.3
    print("\n" + "=" * 100)
    print("BẢNG DÁN VÀO PROGRESS.md §3.3 (phần shuffle.partitions)")
    print("=" * 100 + "\n")
    print("| Giá trị | Số task sau shuffle | Số task xử lý **0 record** | Thời gian (ấm) | 🎯 |")
    print("|---|---|---|---|---|")
    for r, note in ((off, "*lãng phí*"), (on, "← AQE làm hộ bạn")):
        st = r["stage"]
        n_tasks = st["numTasks"] if st else 0
        print("| 200 %s | %d | %d / %d task lấy được | %.0f ms | %s |" % (
            "*(mặc định, AQE off)*" if r is off else "+ **AQE on**",
            n_tasks, r["n_zero"], r["n_seen"], r["warm_ms"], note))

    # -------------------------------------------------- BẰNG CHỨNG AQE
    plan = on["plan"]
    has_aqe_read = "AQEShuffleRead" in plan
    has_coalesced = "coalesced" in plan
    print("\n--- BẰNG CHỨNG TRONG PLAN (AQE ON, đọc SAU khi chạy) ---")
    print("  node `AQEShuffleRead` : %s" % ("CÓ" if has_aqe_read else "KHÔNG THẤY"))
    print("  chữ `coalesced`       : %s" % ("CÓ" if has_coalesced else "KHÔNG THẤY"))
    for line in plan.split("\n"):
        if "AQEShuffleRead" in line or "AdaptiveSparkPlan" in line:
            print("      " + line.strip())
    if not has_aqe_read:
        print("""  (!) Không thấy AQEShuffleRead. ĐỪNG sửa số — đi tìm lý do:
      - dữ liệu bé đến mức chỉ có 1 partition thật -> không có gì để coalesce?
      - spark.sql.adaptive.coalescePartitions.enabled bị tắt?
      Ghi đúng hiện tượng vào report kèm plan thật.""")

    off_tasks = off["stage"]["numTasks"] if off["stage"] else 0
    on_tasks = on["stage"]["numTasks"] if on["stage"] else 0
    print("""
=== KẾT LUẬN (viết vào report) ===

AQE ĐÃ LÀM GÌ: nó COALESCE (gộp) các partition sau shuffle SAU KHI đã nhìn thấy
kích thước THẬT của chúng. Số task ở stage sau shuffle: %d -> %d.

Vì sao AQE off không làm được? Vì lúc lập plan, Spark chưa chạy gì cả — nó
không biết groupBy này ra 8 nhóm hay 8 triệu nhóm. Không biết thì dùng số cứng
`spark.sql.shuffle.partitions = 200`. AQE thì hoãn quyết định: chạy xong stage
map, đọc thống kê kích thước từng partition (MapOutputStatistics), rồi mới gộp
sao cho mỗi task đạt ~spark.sql.adaptive.advisoryPartitionSizeInBytes (64MB).

%d task đọc 0 record thì có hại gì? Mỗi task rỗng vẫn phải: được driver lên lịch,
serialize + gửi đi, deserialize + khởi tạo trên executor, mở/đóng shuffle reader,
báo metrics về driver. Vài ms mỗi cái, nhân với hàng trăm, nhân với hàng nghìn
job/ngày. Trên dữ liệu Olist bé xíu, phần OVERHEAD này có thể lớn hơn cả phần
tính toán thật — nhìn cột thời gian ở bảng trên.

TRUNG THỰC VỚI SỐ ĐO: Olist quá bé, chênh lệch thời gian giữa 2 dòng có thể nhỏ,
thậm chí ngược dấu (AQE tự nó cũng tốn công lập kế hoạch lại). Nếu số của bạn
như vậy thì GHI ĐÚNG NHƯ VẬY và giải thích. Thứ chắc chắn đúng và đáng chỉ vào
là SỐ TASK (%d -> %d), không phải số giây.

LIÊN HỆ CÁC BÀI KHÁC: A16 giải quyết đúng vấn đề này bằng tay (chỉnh
shuffle.partitions = 8). AQE làm việc đó TỰ ĐỘNG và động theo từng lần chạy —
đó là lý do production BẬT AQE (mặc định từ Spark 3.2), và chỉ TẮT khi đang
học đếm stage (bài A10, A11).
""" % (off_tasks, on_tasks, off["n_zero"], off_tasks, on_tasks))
    print("Số nhóm order_status thật sự: %d (đây chính là số task 'có việc làm' tối đa)"
          % on["n_groups"])
    print("=" * 100 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
