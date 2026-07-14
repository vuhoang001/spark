"""A2 — `make run` vs `make run-local`: deploy mode bằng trải nghiệm.

Chạy CÙNG file này bằng CẢ HAI lệnh, rồi so output:
    make run       F=labs/mini-project-1/exercises/a02_run_vs_local.py   # cluster (standalone)
    make run-local F=labs/mini-project-2/exercises/a02_run_vs_local.py   # local[2]

Câu hỏi cả bài xoay quanh đúng một chữ: dòng code này chạy trên MÁY NÀO?
Ở local mode driver kiêm executor -> một máy. Ở cluster mode chúng là hai
container khác nhau — và đó là lúc print() phản bội bạn.
"""

import socket
import time

from pyspark.sql import SparkSession


def banner(title: str):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def dem_executor(sc) -> int:
    """Số executor THẬT (không tính driver).

    PySpark không có API public -> phải thò tay xuống JVM.
    BẪY: getExecutorMemoryStatus() ĐẾM CẢ DRIVER, nên phải trừ 1.
    Ở local mode driver kiêm executor => trừ xong ra 0. Đó không phải lỗi,
    đó chính là định nghĩa của local mode.
    """
    tong = sc._jsc.sc().getExecutorMemoryStatus().size()
    return tong - 1


def main():
    spark = SparkSession.builder.appName("a02-run-vs-local").getOrCreate()
    sc = spark.sparkContext

    # Ép executor đăng ký xong rồi mới đếm. Không có action này, ở cluster mode
    # executor còn đang khởi động -> đếm ra số nhỏ hơn thật. (Bẫy đã gặp ở A1.)
    sc.parallelize(range(10), 2).count()
    time.sleep(2)

    # ---------- BƯỚC 2: tôi đang ở đâu? ----------
    banner("BƯỚC 2 — TÔI ĐANG Ở ĐÂU?")
    print(f"""
sc.master                 : {sc.master}
spark.submit.deployMode   : {sc.getConf().get("spark.submit.deployMode", "?")}
socket.gethostname()      : {socket.gethostname()}   <-- DRIVER chạy ở đây
    (dòng print này nằm trong main(), tức là code của DRIVER — luôn thấy ở terminal)
Số executor (đã trừ driver): {dem_executor(sc)}
Application id            : {sc.applicationId}
""")

    # ---------- BƯỚC 4: cái bẫy — print() bên trong transformation ----------
    banner("BƯỚC 4 — PRINT() TRONG TRANSFORMATION ĐI ĐÂU?")

    def soi(x):
        # Dòng print này KHÔNG chạy ở driver. Nó được đóng gói, gửi qua mạng,
        # và chạy trong tiến trình Python của EXECUTOR.
        print(f"[TRANSFORMATION] xu ly {x} tren host {socket.gethostname()}")
        return x * 2

    rdd = sc.parallelize(range(1, 6), 2)
    ket_qua = rdd.map(soi).collect()

    print(f"\nKết quả collect() (cái này chạy ở DRIVER, luôn thấy): {ket_qua}")
    print("""
Nếu bên trên KHÔNG có dòng [TRANSFORMATION] nào -> code KHÔNG hỏng.
Nó đã chạy rồi, chỉ là chạy ở container khác. Log nằm trên executor.""")

    # ---------- BƯỚC 5: truy tìm log bị mất ----------
    banner("BƯỚC 5 — TRUY TÌM LOG BỊ MẤT")
    print(f"""
Cluster mode: log của executor nằm trong container worker, đào lên bằng:

    docker exec spark-mastery-spark-worker-1 \\
        sh -c 'cat /opt/spark/work/{sc.applicationId}/*/stdout'
    docker exec spark-mastery-spark-worker-2 \\
        sh -c 'cat /opt/spark/work/{sc.applicationId}/*/stdout'

Local mode: không có thư mục work/ nào cả — vì không có executor nào.
""")

    spark.stop()


if __name__ == "__main__":
    main()
