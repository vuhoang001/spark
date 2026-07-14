"""A17 — repartition vs coalesce: hai anh em không giống nhau.

CHẠY (BẮT BUỘC CLUSTER cho phần 2 — phải có >1 core mới thấy coalesce(1) giết song song):
    make run F=labs/mini-project-1/exercises/a17_repartition_vs_coalesce.py

Đây là quyết định bạn sẽ ra HÀNG NGÀY: trước khi write, gọi cái nào?
Câu trả lời rơi thẳng vào 25 điểm rubric "Thiết kế ghi". Nên bài này phải có SỐ.

⚠️ AQE TẮT trong bài này. Vì sao? AQE có thể tự gộp shuffle partition, làm số
partition thực tế không còn khớp với con số ta đặt -> không so sánh được sòng phẳng
repartition(8) và coalesce(8). Tắt AQE = nhìn thấy đúng hành vi của HAI HÀM, không
lẫn với hành vi của thằng thứ ba.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.sparkutils import (banner, md_table, partition_stats, partition_sizes,
                            stage_summary, timeit)

ORDERS = "/workspace/data/olist/olist_orders_dataset.csv"
OUT = "/workspace/data/bench/a17"   # KHÔNG ghi vào silver/ — đó là sân của nhóm ingest


def main():
    spark = SparkSession.builder.appName("a17-repartition-vs-coalesce").getOrCreate()
    sc = spark.sparkContext
    spark.conf.set("spark.sql.adaptive.enabled", "false")   # xem hành vi thô

    # ------------------------------------------------------------------
    # Dựng một DataFrame 200 partition (đúng đề bài) rồi giảm về 8 bằng 2 cách.
    # repartition(200) ở đây là RoundRobin -> chia đều tuyệt đối, để "trước khi giảm"
    # là một điểm xuất phát SẠCH, mọi lệch lạc sau đó là do hàm giảm gây ra.
    # ------------------------------------------------------------------
    base = spark.read.csv(ORDERS, header=True).repartition(200)
    base.cache()
    n_rows = base.count()          # ép vật chất hoá cache TRƯỚC khi đo -> so sánh công bằng

    s200 = partition_stats(partition_sizes(base))
    print(banner(f"A17 — điểm xuất phát: orders {n_rows:,} dòng, "
                 f"{s200['num_partitions']} partition "
                 f"(min {s200['min']:,} / max {s200['max']:,} / mean {s200['mean']:,.0f})"))

    # ------------------------------------------------------------------
    # TIÊU CHÍ 1 — explain(): có Exchange (shuffle) không?
    # ------------------------------------------------------------------
    df_rep = base.repartition(8)
    df_coal = base.coalesce(8)

    print("\n--- explain() của base.repartition(8) ---")
    df_rep.explain()
    print("\n--- explain() của base.coalesce(8) ---")
    df_coal.explain()
    print("""
ĐỌC EXPLAIN THẾ NÀO:
  repartition(8) -> có node `Exchange RoundRobinPartitioning(8)`. Exchange = SHUFFLE
    = ghi dữ liệu ra đĩa, gửi qua mạng, đọc lại. Đắt, nhưng chia lại ĐỀU.
  coalesce(8)    -> KHÔNG có Exchange mới. Chỉ là `Coalesce 8`. Nó chỉ DÁN các
    partition sẵn có lại với nhau (partition 0..24 -> task 0, 25..49 -> task 1, ...).
    Gần như miễn phí. Nhưng "dán" nghĩa là kích thước phụ thuộc hoàn toàn vào
    partition cũ, và — quan trọng nhất — nó KHÔNG CẮT STAGE (xem PHẦN 2).""")

    # ------------------------------------------------------------------
    # TIÊU CHÍ 2 + 3 — phân bố dữ liệu (glom) + thời gian (action thật)
    # ------------------------------------------------------------------
    rows = []
    for name, df in [("repartition(8)", df_rep), ("coalesce(8)", df_coal)]:
        group = f"a17-{name}"
        sc.setJobGroup(group, name)
        sizes = partition_sizes(df)                      # <- yêu cầu của đề: in list 8 số
        st = partition_stats(sizes)
        _, warm_ms, _ = timeit(lambda d=df: d.count(), runs=3, label=name)
        stages = [s for s in stage_summary(sc, group) if s["num_tasks"]]
        rows.append([
            name,
            "CÓ" if "repartition" in name else "KHÔNG",
            st["num_partitions"],
            str(sizes),
            f"{st['min']:,}",
            f"{st['max']:,}",
            f"{st['stddev']:,.0f}",
            f"{st['skew_ratio']:.2f}x",
            f"{warm_ms:,.0f}",
        ])
        print(f"\n{name}: {sizes}")

    print(banner("BẢNG 1 — repartition(8) vs coalesce(8) trên cùng df 200 partition"))
    print(md_table(
        ["cách giảm", "có Exchange?", "số partition", "phân bố (số dòng mỗi partition)",
         "min", "max", "stddev", "skew max/mean", "count() ấm (ms)"], rows))
    # Số partition CHA mà mỗi partition con của coalesce đã nuốt (mỗi cha ~497 dòng).
    # Tính MỘT LẦN (mỗi lần gọi partition_sizes là một action thật — đừng gọi 2 lần).
    parent_mean = s200["mean"]
    coal_sizes = partition_sizes(df_coal)          # partition_stats() KHÔNG trả về "sizes"
    coal_st = partition_stats(coal_sizes)
    parents_eaten = [round(x / parent_mean) for x in coal_sizes]

    print(f"""
ĐỌC BẢNG NÀY — VÀ MỘT PHÁT HIỆN NGƯỢC VỚI TRỰC GIÁC:

  * repartition(8): stddev ~6 dòng. Đều đến mức gần như tuyệt đối. Đó là thứ bạn MUA
    được bằng cái giá của một shuffle: quyền chia lại dữ liệu từ đầu, theo round-robin.

  * coalesce(8): KHÔNG ĐỀU — dù điểm xuất phát ĐÃ ĐỀU TUYỆT ĐỐI.
    Đây là chỗ dễ hiểu sai nhất của cả bài. Điểm xuất phát là repartition(200)
    round-robin: 200 partition, mỗi cái ~{parent_mean:,.0f} dòng, đều như đong bằng cân.
    Nếu coalesce(8) chỉ đơn giản "dán 25 cha liền nhau thành 1 con" thì 8 partition con
    phải bằng nhau chằn chặn. NHƯNG ĐO RA THÌ KHÔNG.

    Lấy số dòng mỗi partition con chia cho ~{parent_mean:,.0f} (cỡ một partition cha), ta biết mỗi
    con đã nuốt bao nhiêu cha:

        {parents_eaten}   <- cộng lại đúng {sum(parents_eaten)} cha
        nhưng KHÔNG phải {[s200['num_partitions'] // 8] * 8}

    VÌ SAO? Vì coalesce KHÔNG chia theo số lượng. Nó dùng DefaultPartitionCoalescer —
    một bộ gom NHẬN BIẾT VỊ TRÍ (locality-aware). Dữ liệu đang nằm cache trên 2 executor;
    coalesce cố gom những partition cha ĐANG NẰM CÙNG MỘT EXECUTOR vào cùng một partition
    con, để khỏi phải kéo dữ liệu qua mạng. Nó tối ưu cho "ĐỪNG DI CHUYỂN DỮ LIỆU",
    KHÔNG tối ưu cho "CHIA CHO ĐỀU". Kết quả: nhóm nào cũng hợp lệ về locality, nhưng
    số dòng thì lệch — ở đây lệch {coal_st['skew_ratio']:.2f}× (stddev {coal_st['stddev']:,.0f} dòng, so với 6 của repartition).

    BÀI HỌC: coalesce KHÔNG HỨA GÌ VỀ ĐỘ ĐỀU — kể cả khi đầu vào hoàn hảo. Nếu đầu vào
    đã lệch sẵn (sau filter, sau join skew) thì nó KẾ THỪA NGUYÊN VẸN sự lệch đó, thậm
    chí khuếch đại. Muốn đều thì phải trả tiền shuffle. Không có bữa trưa miễn phí.

  * coalesce nhanh hơn ở phép count() này vì nó không shuffle. Nhưng thời gian
    count() KHÔNG phải câu chuyện thật. Câu chuyện thật ở PHẦN 2.""")

    # ------------------------------------------------------------------
    # PHẦN 2 — BẪY CHÍ MẠNG: coalesce(1) kéo TOÀN BỘ UPSTREAM về 1 task
    # ------------------------------------------------------------------
    print(banner("PHẦN 2 — bẫy coalesce(1): chứng minh bằng SỐ TASK Ở STAGE TRƯỚC"))
    print("""Kịch bản: có một phép tính NẶNG (sha2 lồng 5 lần trên mỗi dòng), rồi ghi ra 1 file.
Hai cách viết, cùng cho ra 1 file, nhưng số phận khác nhau hoàn toàn.""")

    # ĐỌC LẠI TỪ CSV, KHÔNG dùng `base` đã cache: vì `base` đã bị repartition(200) —
    # tức là đã có sẵn một Exchange ở giữa, nó sẽ CẮT STAGE và làm hỏng thí nghiệm
    # (coalesce(1) khi đó chỉ bóp stage sau, stage đọc file vẫn song song -> không
    # thấy được bẫy). Muốn thấy bẫy thì chuỗi tính toán phải LIỀN MẠCH, không Exchange.
    src = spark.read.csv(ORDERS, header=True)
    print(f"\nsố partition lúc đọc thẳng CSV (không repartition): {src.rdd.getNumPartitions()}")
    print("   ^ đây là mức song song TỐI ĐA mà stage đầu có thể đạt. Nhớ con số này.")

    # Việc nặng: sha2 lồng nhau -> ép CPU làm việc thật, để thời gian có ý nghĩa.
    # Nếu chỉ ghi thẳng thì cả hai cách đều nhanh và ta không thấy được gì.
    heavy = src.select(
        F.sha2(F.sha2(F.sha2(F.sha2(F.sha2(F.col("order_id"), 256), 256), 256), 256), 256).alias("h"),
        F.col("order_status"),
    )

    trap_rows = []
    for name in ["repartition(1)", "coalesce(1)"]:
        df = heavy.repartition(1) if name.startswith("repartition") else heavy.coalesce(1)
        group = f"a17-trap-{name}"
        sc.setJobGroup(group, f"write 1 file bằng {name}")
        path = f"{OUT}/{'rep1' if name.startswith('repartition') else 'coal1'}"

        # ACTION thật: ghi Parquet. Đây mới là thứ ta quan tâm (write là đích đến).
        _, warm_ms, _ = timeit(
            lambda d=df, p=path: d.write.mode("overwrite").parquet(p), runs=3, label=name)

        stages = [s for s in stage_summary(sc, group) if s["num_tasks"]]
        # Stage ĐẦU = nơi việc nặng (sha2) được làm. Số task của nó = mức song song THẬT.
        first = stages[0] if stages else None
        last = stages[-1] if stages else None
        trap_rows.append([
            name,
            len(stages),
            first["num_tasks"] if first else -1,
            last["num_tasks"] if last else -1,
            f"{warm_ms:,.0f}",
        ])

    print(md_table(
        ["cách", "số stage trong job", "task ở stage ĐẦU (nơi sha2 chạy)",
         "task ở stage CUỐI (ghi file)", "write ấm (ms)"], trap_rows))
    print(f"""
ĐÂY LÀ TOÀN BỘ BÀI HỌC:

  repartition(1): job có 2 STAGE.
      stage 1 = sha2, chạy song song trên NHIỀU task (xem cột "task ở stage ĐẦU")
      stage 2 = 1 task gom lại và ghi 1 file
      Giá phải trả: 1 lần shuffle toàn bộ dữ liệu qua mạng.

  coalesce(1): job có 1 STAGE DUY NHẤT, đúng 1 TASK.
      coalesce KHÔNG CẮT STAGE. Nó không phải một phép biến đổi dữ liệu, nó là một
      lời khai với Spark rằng "stage này chỉ cần 1 partition". Mà số task của một
      stage = số partition của nó. Nên toàn bộ chuỗi tính toán TRƯỚC coalesce —
      đọc CSV, sha2 100 nghìn lần — bị nhét hết vào ĐÚNG MỘT TASK, chạy trên ĐÚNG
      MỘT CORE. Cluster 6 core thì 5 core ngồi chơi. Và trên Spark UI KHÔNG có
      Exchange nào để bạn nghi ngờ — đó là lý do bẫy này giết người: nó im lặng.

  Kết luận nghề: coalesce chỉ AN TOÀN khi (a) giảm nhẹ (200 -> 100, không phải -> 1),
  và (b) phần tính toán nặng nằm SAU nó, hoặc không có phần nào nặng.
  Muốn 1 file mà vẫn tính song song -> repartition(1). Muốn nhanh nhất -> đừng đòi 1 file.

QUYẾT ĐỊNH CHO ingest.py (câu này vào thẳng rubric "Thiết kế ghi", 25 điểm):

  Trước `write.partitionBy("order_date")` tôi dùng **repartition("order_date")** —
  KHÔNG dùng coalesce, cũng không dùng repartition(n) theo số.

  Ba lý do, mỗi lý do có số đỡ lưng:
  1. TRÁNH SMALL FILES NHÂN BẢN. Không repartition, mỗi task đang giữ dữ liệu của
     NHIỀU ngày sẽ ghi ra một file cho MỖI ngày nó gặp -> số file = số_task × số_ngày.
     Với {s200['num_partitions']} partition × ~600 ngày, con số đó là hàng chục nghìn file vài KB.
     repartition("order_date") gom mọi dòng cùng ngày về CÙNG MỘT task -> mỗi ngày
     đúng 1 file. Từ hàng chục nghìn file xuống ~600.
  2. KHÔNG coalesce, vì coalesce không đảm bảo "cùng ngày về cùng task" (nó chỉ dán
     partition bừa) -> vẫn đẻ file trùng ngày ở nhiều task. Và như PHẦN 2 vừa chứng
     minh, coalesce sâu còn giết song song của khâu clean phía trước.
  3. Giá của repartition là 1 shuffle. Với ~17MB CSV thì shuffle này gần như miễn phí.
     Đổi 1 shuffle rẻ lấy việc giảm 20× số file: món hời không cần đắn đo.

  (Còn chuyện ~600 file × vài chục KB VẪN là quá bé so với quy tắc 64–256MB/file —
   đó là mâu thuẫn thật của đề bài, xử lý ở bài A20 bằng "van an toàn" đổi độ mịn.)""")

    spark.stop()


if __name__ == "__main__":
    main()
