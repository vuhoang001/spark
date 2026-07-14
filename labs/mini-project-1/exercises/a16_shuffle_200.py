"""A16 — Con số 200 định mệnh: 8 nhóm, 200 task, 192 kẻ thất nghiệp.

CHẠY (cluster để thấy phí schedule thật; local[2] cũng ra được kết luận):
    make run F=labs/mini-project-1/exercises/a16_shuffle_200.py

CHUYỆN GÌ ĐANG XẢY RA:
Sau MỌI wide transformation (groupBy/join/distinct/orderBy), Spark chia lại dữ liệu
thành đúng `spark.sql.shuffle.partitions` phần. Mặc định 200 — bất kể dữ liệu là 5MB
hay 5TB, bất kể có 8 nhóm hay 8 triệu nhóm. Con số 200 này không phải kết quả tính
toán gì cả, nó là một hằng số ai đó gõ vào Spark năm 2014 và không ai dám đổi.

orders có ĐÚNG 8 giá trị order_status. Hash 8 khoá vào 200 thùng -> nhiều nhất 8 thùng
có hàng, ÍT NHẤT 192 thùng rỗng (thực tế còn tệ hơn: hash có thể va chạm, 2 status
rơi cùng thùng -> số thùng có hàng < 8).

⚠️ AQE PHẢI TẮT ở phép đo chính. Vì sao? AQE (bật mặc định) nhìn thấy 192 partition
rỗng sau shuffle rồi TỰ GỘP chúng lại (coalesce) — nghĩa là nó CHỮA đúng căn bệnh mà
bài này muốn cho bạn xem. Tắt AQE = xem bệnh. Bật AQE = xem thuốc. Script chạy CẢ HAI:
  - Bảng 1 (AQE OFF): 200 vs 8 vs 1 -> thấy 192 task rỗng bằng mắt.
  - Bảng 2 (AQE ON) : cùng shuffle.partitions=200 -> thấy AQE gộp còn mấy task.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession

from src.sparkutils import banner, md_table, stage_summary, timeit

ORDERS = "/workspace/data/olist/olist_orders_dataset.csv"


def measure(spark, sc, n_shuffle, aqe, df):
    """Chạy groupBy("order_status").count() với một cấu hình, trả về số liệu THẬT.

    Phải kết thúc bằng ACTION. Ở đây dùng .collect() trên kết quả groupBy (chỉ 8 dòng,
    an toàn) — .count() cũng được nhưng .collect() ép Spark vật chất hoá đủ 8 nhóm,
    đúng với thứ ta muốn đo. Trả DataFrame về = đo lazy = 0.001s = sai (luật sắt #5).
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(n_shuffle))
    spark.conf.set("spark.sql.adaptive.enabled", "true" if aqe else "false")

    tag = f"sp{n_shuffle}-aqe{'ON' if aqe else 'OFF'}"
    group = f"a16-{tag}"
    sc.setJobGroup(group, f"groupBy(order_status).count() | shuffle.partitions={n_shuffle} | AQE={aqe}")

    _, warm_ms, rows = timeit(
        lambda: df.groupBy("order_status").count().collect(), runs=3, label=tag)

    # Stage SAU shuffle = stage cuối cùng của job (stage_id lớn nhất).
    # Stage TRƯỚC shuffle (đọc CSV + agg cục bộ) không liên quan tới con số 200.
    stages = [s for s in stage_summary(sc, group) if s["num_tasks"]]
    post = stages[-1] if stages else None
    return {
        "tag": tag,
        "n_shuffle": n_shuffle,
        "aqe": aqe,
        "warm_ms": warm_ms,
        "groups": len(rows),
        "post_tasks": post["num_tasks"] if post else -1,
        "zero_tasks": post["zero_input_tasks"] if post else -1,
        "post_wall": post.get("wall_ms", 0) if post else 0,
        "shuffle_write": stages[0]["shuffle_write_bytes"] if stages else 0,
    }


def main():
    spark = SparkSession.builder.appName("a16-shuffle-200").getOrCreate()
    sc = spark.sparkContext

    df = spark.read.csv(ORDERS, header=True)
    # .cache() + một action mồi: để phép đo tập trung vào PHẦN SHUFFLE, không lẫn
    # thời gian đọc CSV lại từ đầu ở mỗi lần chạy. Đọc CSV là hằng số chung cho cả
    # 4 cấu hình -> giữ nó cố định thì so sánh mới công bằng.
    df.cache()
    n_orders = df.count()
    n_status = df.select("order_status").distinct().count()

    print(banner(f"A16 — master={sc.master} | orders = {n_orders:,} dòng | "
                 f"order_status có ĐÚNG {n_status} giá trị khác nhau"))

    results = []
    # AQE OFF: xem BỆNH. 200 (mặc định) -> 8 (bằng số nhóm) -> 1 (cực đoan).
    for n in [200, 8, 1]:
        results.append(measure(spark, sc, n, aqe=False, df=df))
    # AQE ON: xem THUỐC. Vẫn để shuffle.partitions=200, để AQE tự dọn.
    aqe_on = measure(spark, sc, 200, aqe=True, df=df)

    print(banner("BẢNG 1 — AQE TẮT: con số 200 hiện nguyên hình"))
    print(md_table(
        ["shuffle.partitions", "task ở stage SAU shuffle", "task đọc 0 byte (thất nghiệp)",
         "số nhóm ra được", "thời gian ấm (ms)", "wall stage sau (ms)"],
        [[r["n_shuffle"], r["post_tasks"], r["zero_tasks"], r["groups"],
          f"{r['warm_ms']:,.0f}", f"{r['post_wall']:,.0f}"] for r in results]))

    r200 = results[0]
    print(f"""
TRẢ LỜI CÂU HỎI CỦA ĐỀ: "200 task cho 8 nhóm thì 192 task làm gì?"

  Đo được: {r200['zero_tasks']}/{r200['post_tasks']} task ở stage sau shuffle đọc về ĐÚNG 0 BYTE.
  Chúng không làm gì cả. Nhưng "không làm gì" KHÔNG có nghĩa là "miễn phí". Mỗi task
  rỗng vẫn phải:
    1. Được driver TẠO RA (một object TaskDescription, serialize lại)
    2. Được driver SCHEDULE (chọn executor, đưa vào hàng đợi)
    3. Được GỬI QUA MẠNG tới executor
    4. Được executor DESERIALIZE, cấp một thread, chạy, deserialize cả closure Python
    5. Đi HỎI mọi map-task ở stage trước: "có block nào cho tôi không?" -> mạng
    6. Báo cáo kết quả (rỗng) về driver, driver cập nhật trạng thái, ghi vào event log

  Sáu bước đó × 192 = một đống việc bookkeeping cho ZERO byte dữ liệu. Ở quy mô này
  (Olist bé) thiệt hại chỉ là vài trăm ms — nhìn cột "thời gian ấm" mà so 200 vs 8.
  Ở production 5000 job/ngày × mỗi job vài chục stage, đây là hàng giờ CPU driver và
  hàng triệu task rỗng. Driver là SINGLE POINT: nó schedule tuần tự. Task rỗng làm
  NGHẼN CỔ CHAI ở driver chứ không phải ở executor — đó là chỗ đau thật.

  Còn shuffle.partitions=1? Nó chữa được lãng phí nhưng đẻ ra bệnh ngược lại:
  TOÀN BỘ dữ liệu sau shuffle chui vào MỘT task, chạy trên MỘT core. Ở đây 8 nhóm
  nên vô hại. Với dữ liệu thật, 1 = tự sát (OOM + mất sạch song song).
  Quy tắc: shuffle.partitions nên ≈ 2–3 lần số core, hoặc để AQE lo.""")

    print(banner("BẢNG 2 — AQE BẬT: thuốc chữa"))
    print(md_table(
        ["cấu hình", "shuffle.partitions ĐẶT", "task THẬT ở stage sau shuffle",
         "task 0 byte", "thời gian ấm (ms)"],
        [["AQE OFF", 200, r200["post_tasks"], r200["zero_tasks"], f"{r200['warm_ms']:,.0f}"],
         ["AQE ON", 200, aqe_on["post_tasks"], aqe_on["zero_tasks"], f"{aqe_on['warm_ms']:,.0f}"]]))
    print(f"""
AQE ĐÃ LÀM GÌ: nó KHÔNG sửa con số 200 trong config. Nó để shuffle ghi ra đủ 200
partition, RỒI nhìn kích thước THẬT của từng partition (map output statistics), thấy
192 cái rỗng và 8 cái bé tí, và GỘP (coalesce) chúng lại thành {aqe_on['post_tasks']} partition trước khi
stage sau đọc. Từ khoá phải nhớ: *coalesce partition sau khi nhìn thấy kích thước thật*.
Trên tab SQL của UI, node `AQEShuffleRead` sẽ có chữ `coalesced`.

Hệ quả cho đời sống hàng ngày: với Spark 3.x + AQE bật, bạn KHÔNG cần chỉnh tay
shuffle.partitions cho phần lớn job. Nhưng phải HIỂU nó, vì:
  - AQE chữa tốt chiều "quá nhiều partition", chữa kém chiều "quá ít".
  - AQE không cứu được nếu bạn tự tay .repartition(1).
  - Nhiều cluster production vẫn tắt AQE (legacy), và bạn sẽ là người bật nó lên.""")

    spark.stop()


if __name__ == "__main__":
    main()
