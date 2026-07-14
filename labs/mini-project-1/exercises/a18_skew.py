"""A18 — Tự tay chế skew: gặp kẻ thù trước khi nó gặp bạn ở production.

CHẠY (BẮT BUỘC CLUSTER — skew là chuyện "cả cluster chờ một task", local[2] thì
      chẳng có ai để mà chờ):
    make run F=labs/mini-project-1/exercises/a18_skew.py

Ý TƯỞNG: join orders × customers rồi repartition("customer_state").
Bang São Paulo (SP) chiếm ~42% khách hàng Olist. Hash-partition theo customer_state
nghĩa là TẤT CẢ dòng của SP phải chui vào ĐÚNG MỘT partition -> đúng một task ->
đúng một core. 41% dữ liệu, một core. Đó là skew.

⚠️ ĐỀ BÀI DẶN: KHÔNG "sửa" skew ở bài này (salting là module 3). Nhiệm vụ ở đây là
ĐO và MÔ TẢ cho chính xác. Đo được thì mới sửa được.

⚠️ HAI ĐIỀU CHỈNH ĐỂ PHÉP ĐO CÓ NGHĨA (nói thẳng, không giấu):
 1. TẮT AQE. AQE có coalescePartitions (gộp partition bé) — nó làm số task sau shuffle
    không còn là con số ta đặt, và làm quartile Duration méo đi. Tắt để nhìn bệnh thô.
    (AQE skewJoin CHỈ chữa skew trong JOIN, không chữa skew do repartition() tay.)
 2. THÊM VIỆC NẶNG (sha2 lồng) sau shuffle. Vì sao? Vì nếu chỉ count() thì mỗi task
    chạy ~20ms, và 20ms đó bị nhiễu bởi phí khởi động task — quartile Duration sẽ
    toàn nhiễu, không phản ánh khối lượng dữ liệu. Job thật LÀM VIỆC trên từng dòng.
    sha2 mô phỏng điều đó: task nào nhiều dòng thì lâu hơn, tỉ lệ thuận. Đây là cách
    làm cho tín hiệu (số dòng) nổi lên trên nhiễu (phí task).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.sparkutils import (banner, md_table, partition_sizes, partition_stats,
                            histogram, stage_summary, timeit)

ORDERS = "/workspace/data/olist/olist_orders_dataset.csv"
CUSTOMERS = "/workspace/data/olist/olist_customers_dataset.csv"


def main():
    spark = SparkSession.builder.appName("a18-skew").getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    orders = spark.read.csv(ORDERS, header=True)
    customers = spark.read.csv(CUSTOMERS, header=True)

    # ------------------------------------------------------------------
    # BƯỚC 1 — nhìn mặt kẻ thù: phân bố customer_state (nguồn gốc của skew)
    # ------------------------------------------------------------------
    joined = orders.join(customers, on="customer_id", how="inner")
    dist = (joined.groupBy("customer_state").count()
            .orderBy(F.desc("count")).collect())
    total = sum(r["count"] for r in dist)
    n_states = len(dist)

    print(banner(f"A18 — orders ⋈ customers = {total:,} dòng, {n_states} bang"))
    print(md_table(["hạng", "customer_state", "số dòng", "% tổng"],
                   [[i + 1, r["customer_state"], f"{r['count']:,}",
                     f"{r['count'] / total * 100:.1f}%"]
                    for i, r in enumerate(dist[:8])]))
    top = dist[0]
    print(f"\n  -> Bang {top['customer_state']} một mình ôm {top['count'] / total * 100:.1f}% dữ liệu. "
          f"Trong khi chia đều {n_states} bang thì mỗi bang chỉ nên có {100 / n_states:.1f}%.")
    print(f"  -> Mất cân bằng gốc: {top['count'] / (total / n_states):.1f}× so với trung bình.")
    print("     Skew KHÔNG do Spark tạo ra. Nó có sẵn trong THẾ GIỚI THẬT (dân số Brazil).")
    print("     Spark chỉ trung thành ánh xạ sự bất công đó lên các core của bạn.")

    # ------------------------------------------------------------------
    # BƯỚC 2 — repartition("customer_state") rồi ĐO stage sau shuffle
    # ------------------------------------------------------------------
    # shuffle.partitions = 32 (xấp xỉ số bang, không phải 200): với 200 thì 173
    # partition rỗng sẽ làm loãng quartile — min/p25/median đều = task rỗng, và bảng
    # quartile trở nên vô nghĩa. 32 giữ cho hầu hết partition có hàng, để quartile nói
    # về DỮ LIỆU chứ không nói về SỰ TRỐNG RỖNG. (Va chạm hash vẫn xảy ra: vài bang
    # rơi chung một partition — đó là chuyện bình thường, ghi nhận chứ không sửa.)
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    skewed = joined.repartition("customer_state").select(
        F.sha2(F.sha2(F.sha2(F.col("order_id"), 256), 256), 256).alias("h"),
        F.col("customer_state"),
    )

    group = "a18-skew"
    sc.setJobGroup(group, "repartition(customer_state) + việc nặng")
    _, warm_ms, _ = timeit(lambda: skewed.count(), runs=3, label="skew")

    # Phân bố SỐ DÒNG thật trong từng partition sau shuffle (ống nghe của A19).
    #
    # ⚠️ PHẢI ĐỔI jobGroup TRƯỚC KHI GỌI glom. Đã dính bug này rồi:
    # partition_sizes() là một ACTION (glom + collect) -> nó đẻ ra job MỚI. Nếu vẫn
    # đang mang nhãn "a18-skew" thì job glom cũng bị gắn nhãn đó, mà stage_id của nó
    # LỚN HƠN mọi stage của count() -> stages[-1] ở BƯỚC 3 sẽ tóm nhầm STAGE CỦA GLOM,
    # không phải stage của count(). Triệu chứng lộ ra: "wall của stage" (1.052 ms) LỚN
    # HƠN "wall của cả action" (490 ms) — một điều bất khả thi, vì stage nằm TRONG action.
    # Con số vô lý đó chính là thứ tố cáo phép đo sai. Đổi nhãn -> mỗi job một sổ riêng.
    sc.setJobGroup("a18-glom", "đo số dòng mỗi partition (glom) — KHÔNG tính vào phép đo skew")
    sizes = partition_sizes(joined.repartition("customer_state"))
    st = partition_stats(sizes)
    print(banner("BƯỚC 2 — số DÒNG mỗi partition sau repartition(\"customer_state\")"))
    print(histogram(sizes))
    print(f"\n  partition: {st['num_partitions']} | rỗng: {st['empty']} | "
          f"min: {st['min']:,} | median-ish mean: {st['mean']:,.0f} | max: {st['max']:,} | "
          f"skew (max/mean): {st['skew_ratio']:.2f}×")

    # ------------------------------------------------------------------
    # BƯỚC 3 — Summary Metrics THẬT của stage sau shuffle (đúng thứ đề bài đòi)
    # ------------------------------------------------------------------
    # ⚠️ KHÔNG lấy stages[-1]. Bug đã dính: count() kết thúc bằng một stage GOM TỔNG
    # chỉ có ĐÚNG 1 TASK (cộng các partial count lại). Lấy stage cuối là lấy đúng cái
    # stage 1-task đó -> Max/Median = 1.00× -> "không có skew", một kết luận SAI HOÀN TOÀN.
    # Kế hoạch thật của skewed.count() có 3 stage:
    #     scan+join -> shuffle write | shuffle READ + sha2 + partial count (32 task) | gom tổng (1 task)
    # Skew sống ở stage GIỮA — stage ĐỌC shuffle. Nhận diện nó bằng ĐẶC ĐIỂM, không
    # bằng vị trí: nó là stage có shuffle_read > 0 và nhiều task nhất (== shuffle.partitions).
    def post_shuffle_stage(g):
        st = [s for s in stage_summary(sc, g) if s["num_tasks"]]
        cand = [s for s in st if s["shuffle_read_bytes"] > 0]
        if not cand:
            return None
        mx_tasks = max(s["num_tasks"] for s in cand)
        return [s for s in cand if s["num_tasks"] == mx_tasks][-1]

    post = post_shuffle_stage(group)
    # Stage sau shuffle của job GLOM: cùng một phép chia partition, nhưng VIỆC TRÊN MỖI
    # DÒNG nặng hơn nhiều (dựng list Python + serialize về driver). Dùng nó làm phép đo
    # đối chứng — xem BẢNG SO SÁNH bên dưới.
    post_glom = post_shuffle_stage("a18-glom")

    print(banner("BƯỚC 3 — Summary Metrics: Duration theo quartile (stage SAU shuffle)"))
    if not post or not post["task_duration_q"]:
        print("CHẠY LỖI: REST không trả về task metrics cho stage sau shuffle.")
    else:
        mn, p25, med, p75, mx = post["task_duration_q"]
        ratio = post["skew_ratio"]
        print(md_table(
            ["chỉ số", "Min", "25th", "Median", "75th", "Max"],
            [["Duration (ms)", f"{mn:,.0f}", f"{p25:,.0f}", f"{med:,.0f}",
              f"{p75:,.0f}", f"{mx:,.0f}"]]))
        # Hai cột "wall" đến từ HAI lần chạy khác nhau (stage = lần chạy cuối;
        # warm = min của lần 2-3) -> ghi rõ, đừng để người đọc tưởng cùng một lần.
        print(md_table(
            ["stage", "số task", "Max/Median",
             "wall stage sau shuffle (lần cuối, ms)", "count() ấm = min lần 2-3 (ms)"],
            [[post["stage_id"], post["num_tasks"], f"{ratio:.2f}×",
              f"{post.get('wall_ms', 0):,.0f}", f"{warm_ms:,.0f}"]]))

        verdict = ("SKEW — đã nhìn thấy nó" if ratio > 3
                   else "chưa vượt ngưỡng 3× (xem giải thích bên dưới)")
        print(f"\n  Tỉ lệ Max/Median = {ratio:.2f}×  ->  {verdict}")

        # ------------------------------------------------------------------
        # BẢNG ĐỐI CHỨNG — CÙNG một phép chia partition (cùng skew DỮ LIỆU 13.43×),
        # nhưng VIỆC TRÊN MỖI DÒNG khác nhau. Đây là câu trả lời cho "vì sao Max/Median
        # có lúc 1.9× có lúc 11.7×" — và nó chứng minh skew là THẬT, không phải nhiễu.
        # ------------------------------------------------------------------
        if post_glom and post_glom["task_duration_q"]:
            g = post_glom["task_duration_q"]
            print(banner("BẢNG ĐỐI CHỨNG — skew DỮ LIỆU không đổi, skew THỜI GIAN thì đổi"))
            print(md_table(
                ["job (việc làm trên mỗi dòng)", "số task", "Min", "Median", "Max",
                 "Max/Median"],
                [[f"count() + sha2 — việc NHẸ", post["num_tasks"],
                  f"{mn:,.0f}", f"{med:,.0f}", f"{mx:,.0f}", f"{ratio:.2f}×"],
                 [f"glom() — việc NẶNG (dựng list + serialize)", post_glom["num_tasks"],
                  f"{g[0]:,.0f}", f"{g[2]:,.0f}", f"{g[4]:,.0f}",
                  f"{post_glom['skew_ratio']:.2f}×"]]))
            print(f"""
  ĐỌC BẢNG NÀY — ĐÂY LÀ PHÁT HIỆN QUAN TRỌNG NHẤT CỦA BÀI:

  Hai job trên chia partition Y HỆT NHAU: cùng repartition("customer_state"), cùng 32
  partition, cùng một task ôm trọn 41.746 dòng của bang SP. Skew DỮ LIỆU giống hệt nhau
  ({st['skew_ratio']:.2f}× — đo ở BƯỚC 2). Nhưng skew THỜI GIAN thì khác một trời một vực:
  {ratio:.2f}× với việc nhẹ, {post_glom['skew_ratio']:.2f}× với việc nặng.

  Vì sao? Mỗi task có một PHÍ CỐ ĐỊNH (khởi động, deserialize closure, hỏi shuffle
  block, báo cáo về driver) — ở cluster này khoảng {mn:,.0f} ms, và nó CỘNG VÀO MỌI TASK,
  bất kể task đó có 0 dòng hay 41.746 dòng.

      Duration(task) ≈ PHÍ CỐ ĐỊNH  +  (số dòng × việc trên mỗi dòng)
                       ^^^^^^^^^^^^     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                       giống nhau       CHỖ SKEW SỐNG

  Việc trên mỗi dòng càng NHẸ, số hạng thứ hai càng bé, và PHÍ CỐ ĐỊNH càng át tín hiệu
  -> Max/Median bị NÉN xuống gần 1. Việc càng NẶNG, số hạng thứ hai càng thống trị, và
  Max/Median càng TIẾN VỀ tỉ lệ số dòng thật ({st['skew_ratio']:.2f}×). Nhìn bảng: {post_glom['skew_ratio']:.2f}× đã bám
  khá sát {st['skew_ratio']:.2f}× rồi.

  KẾT LUẬN TRUNG THỰC (không sửa số để chiều ngưỡng 3× của đề):
    - Với việc nhẹ, Max/Median = {ratio:.2f}× < 3 -> theo đúng chữ của đề thì "chưa có skew".
      Nhưng kết luận đó SAI. Skew có thật và đo được: {st['skew_ratio']:.2f}× ở số DÒNG.
    - Ngưỡng "Max/Median > 3" là một chỉ báo TỐT ở production (nơi task chạy hàng chục
      giây, phí cố định không đáng kể) nhưng nó NÓI DỐI trên dữ liệu bé/việc nhẹ.
      Muốn chẩn đoán skew cho chắc thì nhìn CẢ HAI: tỉ lệ số DÒNG và tỉ lệ THỜI GIAN.
    - Job thật (clean, cast, hash, join, ghi Parquet) nặng hơn count() nhiều — nó nằm
      về phía dòng thứ hai của bảng. Nên ở A40 (dữ liệu ×100), cái task ôm SP sẽ là
      kẻ nổ đầu tiên, đúng như dự đoán.""")

    print("""
CẢ JOB PHẢI CHỜ AI? (3 dòng đề bài yêu cầu)

  1. Một stage chỉ XONG khi TASK CHẬM NHẤT của nó xong — không phải khi task trung
     bình xong. Wall time của stage ≈ Max, không phải Median. Nhìn bảng trên: 31 task
     kia đã ngồi chơi từ lâu, cả cluster nín thở chờ đúng cái task ôm bang SP.
  2. Trong lúc chờ, 5/6 core RỖNG. Bạn vẫn trả tiền cho 6 core. Hiệu suất thực =
     tổng thời gian task / (số core × wall) — với skew nặng, con số này rơi xuống dưới 30%.
  3. Tệ hơn: task to nhất còn là ứng viên số một cho SPILL (dữ liệu không vừa RAM
     executor -> ghi tạm ra đĩa) và cho OOM. Skew không chỉ làm CHẬM, nó làm CHẾT.
     Với cluster này (executor maxMemory ~1049 MB), Olist còn quá bé để OOM — nhưng
     nhân dữ liệu lên 100 lần (bài A40) thì cái task SP đó là kẻ nổ đầu tiên.

NẾU Max/Median CỦA BẠN < 3 THÌ SAO? Đừng sửa số. Giải thích:
  - Olist quá bé: mỗi task chỉ vài trăm ms, phí khởi động task (~50-100ms) là một
    HẰNG SỐ CỘNG THÊM vào MỌI task. Nó nâng Median lên và kéo tỉ lệ Max/Median xuống.
    Với dữ liệu thật (mỗi task hàng chục giây), hằng số đó biến mất và tỉ lệ nhảy vọt.
  - Hãy đối chiếu với tỉ lệ số DÒNG (skew max/mean ở BƯỚC 2). Nếu số dòng lệch 10×
    mà Duration chỉ lệch 2× thì kết luận đúng là: "skew DỮ LIỆU có thật và đo được,
    nhưng ở quy mô này nó chưa kịp biến thành skew THỜI GIAN vì phí cố định của task
    đang át tín hiệu". Đó là một câu trả lời trung thực và ĐÚNG — quan trọng hơn một
    con số đẹp.

KHÔNG SỬA SKEW Ở ĐÂY (đề dặn). Nhưng ghi lại 3 hướng cho module 3:
  - salting: thêm hậu tố ngẫu nhiên vào khoá SP -> tách 1 task thành N task.
  - AQE skewJoin: Spark tự chẻ partition to trong JOIN (không cứu repartition tay).
  - broadcast join: nếu một bên đủ bé, không shuffle thì không có skew.""")

    spark.stop()


if __name__ == "__main__":
    main()
