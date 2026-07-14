"""A1 — Vẽ bản đồ cluster của chính mình.

Chạy:
    make run       F=labs/mini-project-1/exercises/a01_cluster_map.py   # cluster (standalone)
    make run-local F=labs/mini-project-1/exercises/a01_cluster_map.py   # local[2]

Output: một bảng markdown dán thẳng vào PROGRESS.md §3.0 — driver/executor ở đâu,
mấy core, bao nhiêu RAM, và RAM *thật sự* dùng được cho dữ liệu là bao nhiêu.

Ý chính của bài: mọi con số trên tab Executors của Spark UI đều truy được về
MỘT dòng cấu hình. Script này đi ngược từ số về config, thay vì chép tay từ UI.
"""

import json
import time
import urllib.request

from pyspark.sql import SparkSession

# Bật/tắt: giữ session sống để kịp mở Spark UI ở localhost:4040
KEEP_UI_ALIVE_SEC = 0

# Hằng số nội bộ của Spark — không phải config, hardcode trong UnifiedMemoryManager
RESERVED_MEMORY_MB = 300

# ============================================================================
# CHỖ CHIA TÀI NGUYÊN — đây là toàn bộ thứ bạn cần sửa
# ============================================================================
# Đặt None = dùng mặc định của Spark (executor ăn HẾT core worker, heap 1g).
#
# Trần cứng: xem worker cho thuê bao nhiêu bằng  curl -s localhost:8080/json/
# Công thức: số executor = min(worker_cores // EXEC_CORES,
#                              worker_mem_mb // EXEC_MEMORY_MB)
# Xin quá trần -> app TREO vô hạn (không báo lỗi). Script sẽ cảnh báo trước.
#
# Cluster hiện tại: 2 worker x 3 core x 3G (xem docker-compose.spark.yaml).
# Thử lần lượt để thấy reserved 300MB bị trừ TRÊN MỖI executor:
#   ("3", "2g")    -> 2 executor x 3 core, maxMemory 1049 MB mỗi con  (tổng 2.0 GB)
#   ("1", "512m")  -> 6 executor x 1 core, maxMemory  127 MB mỗi con  (tổng 0.7 GB!)
# Cùng 6 core, nhưng chẻ vụn thì mất gần hết RAM vào reserved.
EXEC_CORES = "3"
EXEC_MEMORY = "2g"

# LƯU Ý: spark.driver.memory KHÔNG set được ở đây. Ở client mode JVM driver đã
# khởi động xong trước khi dòng Python này chạy -> phải dùng --driver-memory.
# ============================================================================


def rest(base_url: str, path: str):
    """Gọi REST API của Spark UI — chính là nguồn dữ liệu mà tab Executors đang vẽ."""
    with urllib.request.urlopen(f"{base_url}/api/v1{path}", timeout=10) as r:
        return json.loads(r.read())


def mb(byte_count) -> float:
    return byte_count / 1024 / 1024


def parse_mem_mb(text: str) -> float:
    """'1g' -> 1024.0, '512m' -> 512.0. Spark chấp nhận k/m/g/t, mặc định là MB."""
    text = text.strip().lower().rstrip("b")
    unit, number = text[-1], text[:-1]
    factor = {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 * 1024}
    return float(number) * factor[unit] if unit in factor else float(text)


def worker_capacity(master_url: str):
    """Trần cứng của cluster: worker cho thuê tối đa bao nhiêu core / MB.

    Nguồn: REST của Spark *master* (cổng 8080), khác với REST của app (4040).
    Đây chính là SPARK_WORKER_CORES / SPARK_WORKER_MEMORY trong docker-compose.
    """
    if not master_url.startswith("spark://"):
        return None  # local mode: không có worker
    host = master_url[len("spark://"):].split(":")[0]
    with urllib.request.urlopen(f"http://{host}:8080/json/", timeout=10) as r:
        data = json.loads(r.read())
    alive = [w for w in data["workers"] if w["state"] == "ALIVE"]
    return [(w["cores"], float(w["memory"])) for w in alive]


def plan(workers, exec_cores, exec_mem_mb):
    """Tính TRƯỚC xem sẽ nhận được mấy executor — trước khi Spark treo im lặng."""
    total = 0
    for cores, mem_mb in workers:
        by_core = cores // exec_cores if exec_cores else 1
        by_mem = int(mem_mb // exec_mem_mb)
        total += min(by_core, by_mem)
    return total


def main():
    builder = SparkSession.builder.appName("a01-cluster-map")
    if EXEC_CORES:
        builder = builder.config("spark.executor.cores", EXEC_CORES)
    if EXEC_MEMORY:
        builder = builder.config("spark.executor.memory", EXEC_MEMORY)

    spark = builder.getOrCreate()
    sc = spark.sparkContext
    conf = dict(sc.getConf().getAll())

    # ---------- 0. Đối chiếu XIN vs TRẦN, trước khi kịp treo ----------
    req_cores = int(EXEC_CORES) if EXEC_CORES else None
    req_mem_mb = parse_mem_mb(conf.get("spark.executor.memory", "1g"))
    workers = worker_capacity(sc.master)

    if workers:
        cap_cores = sum(c for c, _ in workers)
        cap_mem = sum(m for _, m in workers)
        n_exec = plan(workers, req_cores, req_mem_mb)
        print(f"""
--- TRẦN CỨNG vs ĐƠN XIN ---
Worker cho thuê : {len(workers)} worker, tổng {cap_cores} core / {cap_mem:.0f} MB
                  (= SPARK_WORKER_CORES / SPARK_WORKER_MEMORY trong docker-compose)
App đang xin    : {req_cores or 'HẾT core của worker'} core, {req_mem_mb:.0f} MB mỗi executor
Dự kiến nhận    : {n_exec} executor
    = min(core: {cap_cores}//{req_cores or 1}, RAM: {cap_mem:.0f}//{req_mem_mb:.0f})""")
        if n_exec == 0:
            print("\n!!! XIN QUÁ TRẦN — Spark sẽ chờ executor mãi mãi. Sửa EXEC_* rồi chạy lại.")
            spark.stop()
            return

    # Ép Spark khởi động executor thật trước khi hỏi REST API.
    # Không có action này, ở cluster mode danh sách executor có thể còn rỗng.
    sc.parallelize(range(100), 4).count()
    time.sleep(2)

    ui = sc.uiWebUrl
    app_id = sc.applicationId

    print("\n" + "=" * 78)
    print("A1 — BẢN ĐỒ CLUSTER")
    print("=" * 78)

    # ---------- 1. Danh tính phiên chạy ----------
    print(f"""
Spark version   : {spark.version}
Application id  : {app_id}
Master URL      : {sc.master}
Deploy mode     : {conf.get('spark.submit.deployMode', '?')}
Driver host     : {conf.get('spark.driver.host', '?')}   (container nào đang chạy driver?)
Spark UI        : {ui}
defaultParallelism : {sc.defaultParallelism}   <-- số task Spark chạy song song khi không được chỉ định
""")

    # ---------- 2. Bảng executor: lấy thẳng từ REST API ----------
    execs = rest(ui, f"/applications/{app_id}/executors")

    print("| vai trò | id | địa chỉ | cores | RAM cho data (maxMemory) | disk used |")
    print("|---|---|---|---|---|---|")

    total_cores = 0
    total_data_mem = 0.0
    for e in execs:
        is_driver = e["id"] == "driver"
        role = "driver" if is_driver else "executor"

        # maxMemory = unified pool: phần RAM cho storage + execution
        # (đã trừ reserved 300MB và nhân memory.fraction). KHÔNG phải JVM heap.
        data_mem = mb(e["maxMemory"])

        if not is_driver:
            total_cores += e["totalCores"]
            total_data_mem += data_mem

        print(
            f"| {role} | {e['id']} | {e['hostPort']} | {e['totalCores']} "
            f"| {data_mem:.0f} MB | {mb(e['diskUsed']):.0f} MB |"
        )

    # ---------- 3. Truy ngược: số trên UI đến từ dòng config nào ----------
    exec_mem = conf.get("spark.executor.memory", "1g (mặc định)")
    exec_cores = conf.get("spark.executor.cores", "(không set → standalone cấp HẾT core của worker)")
    driver_mem = conf.get("spark.driver.memory", "1g (mặc định)")
    mem_fraction = float(conf.get("spark.memory.fraction", 0.6))
    storage_fraction = float(conf.get("spark.memory.storageFraction", 0.5))

    # local mode KHÔNG có executor riêng — driver JVM đóng cả hai vai.
    # Đây không phải lỗi, đây là bài học: nhìn cột cores của driver là biết ngay.
    real_execs = [e for e in execs if e["id"] != "driver"]
    is_local = not real_execs
    if is_local:
        driver = execs[0]
        total_cores = driver["totalCores"]
        total_data_mem = mb(driver["maxMemory"])

    # Kiểm chứng công thức: heap - reserved, nhân fraction, có ra đúng maxMemory không?
    # Đọc heap từ conf, KHÔNG hardcode — nếu không, vừa tune một cái là báo LỆCH oan.
    heap_key = "spark.driver.memory" if is_local else "spark.executor.memory"
    heap_mb = parse_mem_mb(conf.get(heap_key, "1g"))
    predicted = (heap_mb - RESERVED_MEMORY_MB) * mem_fraction
    actual = total_data_mem / max(len(real_execs), 1)
    who = "driver (local mode: driver KIÊM executor)" if is_local else "executor"
    print(f"""
--- KIỂM CHỨNG CÔNG THỨC (đây là phần đắt giá của bài) ---
Dự đoán maxMemory của {who}
    = ({heap_mb:.0f} - {RESERVED_MEMORY_MB}) x {mem_fraction} = {predicted:.1f} MB
Thực tế = {actual:.1f} MB   -> {'KHỚP' if abs(predicted - actual) < 2 else 'LỆCH, đi tìm lý do!'}""")

    print(f"""
--- TRUY NGƯỢC: mỗi số trên UI đến từ đâu? ---
spark.executor.memory        = {exec_mem}
spark.executor.cores         = {exec_cores}
spark.driver.memory          = {driver_mem}
spark.memory.fraction        = {mem_fraction}
spark.memory.storageFraction = {storage_fraction}

Công thức RAM (đây là chỗ 9/10 người hiểu sai):
    executor heap                    = spark.executor.memory
    - reserved                       = {RESERVED_MEMORY_MB} MB (hằng số, KHÔNG chỉnh được)
    x spark.memory.fraction ({mem_fraction})   = unified pool (execution + storage)  <-- 'maxMemory' ở bảng trên
    x storageFraction ({storage_fraction})       = phần storage ĐƯỢC BẢO ĐẢM (cache không bị đuổi)
    phần còn lại                     = user memory (object của bạn, UDF, ...)

=> Xin 1GB executor KHÔNG có nghĩa có 1GB để chứa dữ liệu.
""")

    # ---------- 4. Câu chốt của bài ----------
    print(f"""--- KẾT LUẬN ---
Tổng số task chạy SONG SONG tối đa = tổng core của các executor = {total_cores}
Tổng RAM thật sự dùng cho dữ liệu  = {total_data_mem:.0f} MB
Task thứ {total_cores + 1} trở đi phải XẾP HÀNG đợi (wave) — xem lesson 3, mục 3.5.
""")
    print("=" * 78 + "\n")

    if KEEP_UI_ALIVE_SEC:
        print(f"Giữ UI sống {KEEP_UI_ALIVE_SEC}s — mở {ui} (host: http://localhost:4040)")
        time.sleep(KEEP_UI_ALIVE_SEC)

    spark.stop()


if __name__ == "__main__":
    main()
