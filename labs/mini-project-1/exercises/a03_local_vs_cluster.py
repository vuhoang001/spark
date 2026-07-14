"""A3 — "Cluster nhanh hơn local" là NIỀM TIN SAI. Chứng minh bằng số.

Cùng một `count()` trên olist_customers (8.6 MB), chạy ở 3 nơi:
    local[1]   — 1 thợ, không mạng, không serialize
    local[*]   — hết core của máy, vẫn 1 JVM
    cluster    — 2 executor ở 2 container khác

Makefile chỉ có `run` (cluster) và `run-local` (local[2]) nên script tự set master.
Chạy (từ thư mục gốc repo):

    docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
        /workspace/labs/mini-project-1/exercises/a03_local_vs_cluster.py local1
    ... đổi `local1` thành `localstar` / `cluster`

Hai đồng hồ, hai con số KHÁC NHAU — đó là trọng tâm bài này:
    1. time.time() quanh action  = thời gian DRIVER chờ  (gồm cả lập plan, gửi task)
    2. Duration của job trên UI  = thời gian EXECUTOR làm (chỉ tính lúc task chạy)
Chênh lệch giữa hai cái = phần overhead mà bạn KHÔNG thấy nếu chỉ nhìn Spark UI.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime

from pyspark.sql import SparkSession

CSV = "/workspace/data/olist/olist_customers_dataset.csv"
RUNS = 3  # lần 1 là warmup, chỉ lấy lần 2-3

MASTERS = {
    "local1": "local[1]",
    "localstar": "local[*]",
    "cluster": "spark://spark-master:7077",
}


def _ts(s: str) -> datetime:
    """'2026-07-14T07:35:02.123GMT' -> datetime."""
    return datetime.strptime(s.replace("GMT", "").strip(), "%Y-%m-%dT%H:%M:%S.%f")


def job_duration_ms(ui_url: str, app_id: str, group: str) -> float:
    """Duration của job thuộc jobGroup `group` — chính là cột Duration trên Spark UI.

    BẪY (đã dính): REST /jobs trả về job theo thứ tự MỚI NHẤT TRƯỚC, và
    `spark.read.csv(header=True)` cũng đẻ ra một job (đọc header) chứ không chỉ
    count(). Lấy jobs[-3:] là lấy nhầm job -> ra số vô nghĩa (job > wall).
    Cách chắc chắn: gắn nhãn jobGroup cho từng lần chạy rồi lọc theo nhãn.
    """
    url = f"{ui_url}/api/v1/applications/{app_id}/jobs"
    with urllib.request.urlopen(url, timeout=10) as r:
        jobs = json.loads(r.read())
    total = 0.0
    for j in jobs:
        if j.get("jobGroup") != group:
            continue
        if j.get("submissionTime") and j.get("completionTime"):
            total += (_ts(j["completionTime"]) - _ts(j["submissionTime"])).total_seconds() * 1000
    return total


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "cluster"
    master = MASTERS[mode]

    # ---------- Đồng hồ 0: khởi động SparkSession ----------
    # Đây là overhead mà KHÔNG BAO GIỜ hiện trên Spark UI. UI chỉ tồn tại SAU khi
    # session đã dựng xong. Ở cluster mode, đây là lúc master cấp executor, JVM
    # executor bật lên, đăng ký về driver — tốn hàng giây.
    t = time.time()
    spark = (
        SparkSession.builder.appName(f"a03-{mode}").master(master).getOrCreate()
    )
    sc = spark.sparkContext
    startup_s = time.time() - t

    # Chờ executor đăng ký xong (cluster mode). Không chờ thì lần count() đầu
    # sẽ gánh luôn thời gian executor khởi động -> số đo nói dối.
    if mode == "cluster":
        sc.parallelize(range(10), 2).count()
        time.sleep(2)

    df = spark.read.csv(CSV, header=True)  # KHÔNG inferSchema (nó là action trá hình — bài A5)

    # ---------- Đồng hồ 1 + 2: đo song song, gắn nhãn từng lần ----------
    walls, count_jobs = [], []
    for i in range(RUNS):
        sc.setJobGroup(f"count-{i}", f"count() lan {i}")
        t = time.time()
        n = df.count()
        walls.append((time.time() - t) * 1000)
        count_jobs.append(job_duration_ms(sc.uiWebUrl, sc.applicationId, f"count-{i}"))

    am_wall = min(walls[1:])          # lấy lần 2-3, bỏ lần 1 (JVM warmup + page cache lạnh)
    am_job = min(count_jobs[1:])

    print("\n" + "=" * 78)
    print(f"A3 — {mode.upper()}  ({master})")
    print("=" * 78)
    print(f"""
Số dòng đọc được       : {n:,}
defaultParallelism     : {sc.defaultParallelism}
Số partition của df    : {df.rdd.getNumPartitions()}

--- ĐỒNG HỒ 0: dựng SparkSession (KHÔNG hiện trên UI) ---
startup                : {startup_s * 1000:,.0f} ms

--- ĐỒNG HỒ 1: time.time() quanh count() — DRIVER chờ bao lâu ---
lần 1 (lạnh)           : {walls[0]:,.0f} ms   <-- vứt đi, JVM warmup
lần 2                  : {walls[1]:,.0f} ms
lần 3                  : {walls[2]:,.0f} ms
ẤM (min lần 2-3)       : {am_wall:,.0f} ms

--- ĐỒNG HỒ 2: Duration job trên Spark UI — EXECUTOR làm bao lâu ---
job count() lần 1-3    : {[f'{d:,.0f}' for d in count_jobs]} ms
ẤM (min lần 2-3)       : {am_job:,.0f} ms

--- CHÊNH LỆCH: cái Spark UI KHÔNG kể cho bạn ---
wall (ấm) - job (ấm)   : {am_wall - am_job:,.0f} ms
    = lập plan + tối ưu Catalyst + serialize task + gửi qua mạng + gom kết quả về
    Nhìn Spark UI thấy job chỉ mất {am_job:,.0f} ms -> tưởng nhanh.
    Nhưng bạn THỰC SỰ chờ {am_wall:,.0f} ms.
""")
    print(f"DÒNG BẢNG| {mode} | {master} | {startup_s*1000:.0f} | {walls[0]:.0f} | "
          f"{am_wall:.0f} | {am_job:.0f} | {am_wall - am_job:.0f} |")
    print("=" * 78 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
