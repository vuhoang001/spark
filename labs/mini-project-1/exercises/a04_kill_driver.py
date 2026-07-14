"""A4 — Giết driver có chủ đích, rồi sửa 3 cách và CHỨNG MINH cách nào an toàn.

Ý tưởng của bài: driver KHÔNG phải chỗ chứa dữ liệu. Nó là kỹ sư trưởng đọc bản vẽ,
không phải cái xe tải. `collect()` = bắt kỹ sư trưởng vác hết gạch về phòng làm việc.
Muốn nhớ điều đó cả đời thì phải làm nó SẬP một lần, tự tay.

File này có 4 mode, mỗi mode là 1 lần spark-submit RIÊNG (mode kill sẽ chết thật,
không chạy tiếp được):

  1) kill-oom        collect() cả olist_geolocation (58MB, ~1.0 triệu dòng) với driver
                     bị bóp còn 512m  -> kỳ vọng java.lang.OutOfMemoryError
  2) kill-maxresult  y hệt nhưng thêm spark.driver.maxResultSize=32m
                     -> kỳ vọng "Total size of serialized results ... is bigger than
                        spark.driver.maxResultSize" (một lỗi KHÁC, ở một chốt chặn KHÁC)
  3) fix             take(20) / limit(20).collect() / show(20) + explain() cả ba,
                     ĐẾM SỐ TASK thật của từng action qua REST API -> bằng chứng
                     cách nào không kéo hết dữ liệu về
  4) danger          df.collect()[:20] — cái bẫy của đề bài. Nhìn giống limit(20)
                     trong Python, khác hoàn toàn trong Spark. Mode này CỐ TÌNH chết.

LỆNH CHẠY (từ repo root) — xem cuối file.

Vì sao KHÔNG set driver memory trong code? Vì ở client mode, JVM driver đã bật xong
TRƯỚC khi dòng SparkSession.builder chạy. `.config("spark.driver.memory", "512m")`
lúc đó là lời nói vào hư không — heap đã cấp phát rồi. Bắt buộc phải truyền
--driver-memory cho spark-submit. Đây là bẫy số 1 của bài này.
"""

import inspect
import json
import sys
import time
import traceback
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

CSV = "/workspace/data/olist/olist_geolocation_dataset.csv"  # 58MB — file to nhất bộ Olist


# ---------------------------------------------------------------------------
# Bằng chứng cứng: đếm SỐ TASK mà mỗi action thực sự chạy.
# Đây là thứ không cãi được. Một action "kéo hết dữ liệu về" buộc phải đụng
# TẤT CẢ partition -> N task. Một action "chỉ lấy 20 dòng" chỉ cần đụng
# partition đầu tiên -> 1 task.
# ---------------------------------------------------------------------------
def job_stats(ui_url: str, app_id: str, group: str) -> dict:
    """Tổng numTasks + số job của các job thuộc jobGroup `group`.

    BẪY (đã dính ở A3): REST /jobs trả job MỚI NHẤT TRƯỚC, và bản thân
    spark.read.csv(header=True) cũng đẻ ra một job riêng để đọc dòng header.
    Lấy jobs[-1] là lấy nhầm. Cách chắc: sc.setJobGroup() rồi lọc theo nhãn.
    """
    url = f"{ui_url}/api/v1/applications/{app_id}/jobs"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            jobs = json.loads(r.read())
    except Exception as e:  # UI có thể chưa sẵn sàng — thà thiếu số còn hơn bịa số
        return {"jobs": -1, "tasks": -1, "err": str(e)}
    mine = [j for j in jobs if j.get("jobGroup") == group]
    return {
        "jobs": len(mine),
        "tasks": sum(j.get("numTasks", 0) for j in mine),
        "stages": sum(len(j.get("stageIds", [])) for j in mine),
    }


def run_action(sc, tag: str, fn):
    """Chạy 1 action dưới nhãn jobGroup riêng, trả về (kết quả, ms, thống kê job)."""
    sc.setJobGroup(tag, tag)
    t = time.time()
    out = fn()
    ms = (time.time() - t) * 1000
    # PySpark 3.4 KHÔNG có sc.clearJobGroup() -> không gọi, chỉ đặt nhãn mới ở lần sau.
    return out, ms, job_stats(sc.uiWebUrl, sc.applicationId, tag)


def build(app: str, extra: dict = None) -> SparkSession:
    b = SparkSession.builder.appName(app)
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    return b.getOrCreate()


# ===========================================================================
# MODE 1 + 2 + 4 — làm chết driver
# ===========================================================================
def mode_kill(which: str):
    extra = {}
    if which == "kill-maxresult":
        # Chốt chặn THỨ HAI, khác OOM: driver đếm tổng kích thước kết quả
        # serialize từ các task trả về; vượt ngưỡng là nó CHỦ ĐỘNG huỷ job.
        # Đây là cơ chế "phanh tay" — nó bảo vệ driver TRƯỚC KHI heap vỡ.
        extra["spark.driver.maxResultSize"] = "32m"

    spark = build(f"a04-{which}", extra)
    sc = spark.sparkContext

    print("=" * 78)
    print(f"A4 — MODE {which}")
    print("=" * 78)
    print(f"master                     : {sc.master}")
    print(f"spark.driver.memory (conf) : {sc.getConf().get('spark.driver.memory', '(mặc định)')}")
    print(f"spark.driver.maxResultSize : {sc.getConf().get('spark.driver.maxResultSize', '(mặc định 1g)')}")
    # Heap THẬT của JVM driver — con số này mới nói lên sự thật, không phải cái conf ở trên.
    # Nếu bạn quên --driver-memory thì dòng này sẽ tố cáo bạn ngay.
    try:
        rt = sc._jvm.java.lang.Runtime.getRuntime()
        print(f"JVM driver maxMemory THẬT  : {rt.maxMemory() / 1024 / 1024:,.0f} MB")
    except Exception as e:
        print(f"JVM driver maxMemory THẬT  : không đọc được ({e})")

    df = spark.read.csv(CSV, header=True)  # không inferSchema (nó là action trá hình — A5)
    print(f"file                       : {CSV}")
    print(f"số partition               : {df.rdd.getNumPartitions()}")
    print()
    print(">>> Sắp gọi:", "df.collect()[:20]" if which == "danger" else "df.collect()")
    print(">>> KỲ VỌNG: driver chết. Nếu nó KHÔNG chết, ghi lại đúng như vậy — đừng bịa lỗi.")
    print()
    sys.stdout.flush()  # xả buffer TRƯỚC khi chết, nếu không mất sạch log ở trên

    t = time.time()
    try:
        if which == "danger":
            # BẪY CỦA ĐỀ BÀI. Trông giống limit(20) nhưng: Python slice `[:20]`
            # chỉ chạy SAU KHI collect() đã trả về một list đầy đủ 1 triệu Row.
            # Nghĩa là toàn bộ dữ liệu đã nằm trong RAM driver rồi mới bị cắt.
            # Spark không hề biết bạn chỉ cần 20 dòng.
            rows = df.collect()[:20]
        else:
            rows = df.collect()
        # Nếu tới được đây nghĩa là KHÔNG chết -> phải nói thật, không được giả vờ.
        print("!!! KHÔNG CHẾT — driver nuốt trôi.")
        print(f"    số dòng kéo về driver: {len(rows):,}")
        print(f"    thời gian            : {(time.time() - t) * 1000:,.0f} ms")
        # VÌ SAO KHÔNG CHẾT? Đo RSS của tiến trình PYTHON driver.
        # --driver-memory chỉ chặn HEAP CỦA JVM. List Row mà collect() trả về
        # nằm trong tiến trình PYTHON — một vùng nhớ mà cờ đó KHÔNG hề quản.
        # Nếu RSS Python >> heap JVM thì giả thuyết này đúng.
        import resource
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        print(f"    RSS đỉnh của PYTHON  : {rss_mb:,.0f} MB  <- KHÔNG bị --driver-memory chặn")
        try:
            rt2 = sc._jvm.java.lang.Runtime.getRuntime()
            print(f"    heap JVM (max/used)  : {rt2.maxMemory()/1024/1024:,.0f} MB / "
                  f"{(rt2.totalMemory()-rt2.freeMemory())/1024/1024:,.0f} MB")
        except Exception:
            pass
    except BaseException:
        print("=" * 78)
        print("NGUYÊN VĂN LỖI (dán thẳng vào report, không sửa một chữ):")
        print("=" * 78)
        traceback.print_exc(file=sys.stdout)
        print("=" * 78)
        print(f"chết sau: {(time.time() - t) * 1000:,.0f} ms")
        sys.stdout.flush()
        # Lưu ý: nếu JVM driver vỡ heap thật sự, Py4J có thể đứt kết nối và
        # traceback bạn thấy ở đây chỉ là hệ quả. Dòng OutOfMemoryError gốc nằm
        # trong STDERR của spark-submit -> nhớ chạy với 2>&1 khi hứng vào results/.
        sys.exit(1)
    finally:
        try:
            spark.stop()
        except BaseException:
            pass


# ===========================================================================
# MODE 3 — sửa 3 cách + chứng minh
# ===========================================================================
def mode_fix():
    # Vì sao ép maxPartitionBytes=8m? Vì file 58MB < 128MB mặc định -> Spark đọc
    # thành 1 partition duy nhất. Mà 1 partition thì MỌI action đều 1 task, so
    # sánh vô nghĩa. Ép 8m -> ~8 partition -> lúc đó "1 task" vs "N task" mới
    # tách bạch và bằng chứng mới có giá trị.
    spark = build("a04-fix", {"spark.sql.files.maxPartitionBytes": "8m"})
    sc = spark.sparkContext

    df = spark.read.csv(CSV, header=True)
    nparts = df.rdd.getNumPartitions()

    print("=" * 78)
    print("A4 — MODE fix: ba cách sửa + bằng chứng cách nào KHÔNG kéo hết về driver")
    print("=" * 78)
    print(f"master              : {sc.master}")
    print(f"file                : {CSV} (58 MB)")
    print(f"maxPartitionBytes   : 8m (ép nhỏ để nhìn rõ 1 task vs N task)")
    print(f"số partition của df : {nparts}")
    print()

    # ---- 0) Mốc so sánh: một action ĐỌC HẾT dữ liệu ----
    # count() phải chạm mọi partition -> N task. Đây là dáng hình của "full scan".
    # collect() cũng có đúng dáng hình đó (khác ở chỗ nó còn kéo dữ liệu về driver
    # nữa) — nhưng collect() thì đã chết ở mode kill rồi, không chạy lại ở đây.
    n, ms_cnt, st_cnt = run_action(sc, "a04-count", lambda: df.count())

    # ---- 1) take(20) ----
    # PySpark: DataFrame.take(n) == self.limit(n).collect(). In hẳn source ra cho
    # thấy — không cần tin lời ai.
    r_take, ms_take, st_take = run_action(sc, "a04-take", lambda: df.take(20))

    # ---- 2) limit(20).collect() ----
    r_lim, ms_lim, st_lim = run_action(sc, "a04-limit", lambda: df.limit(20).collect())

    # ---- 3) show(20) ----
    # show() gọi xuống JVM showString(n) -> bên trong cũng là take(n) -> limit.
    # Nó in ra stdout nên không trả về gì; ta chỉ cần đo task của nó.
    _, ms_show, st_show = run_action(sc, "a04-show", lambda: df.show(20, truncate=False))

    # ---------------- Bằng chứng 1: source của take() ----------------
    print()
    print("-" * 78)
    print("BẰNG CHỨNG 1 — take() thực chất LÀ limit().collect() (source PySpark trong container):")
    print("-" * 78)
    try:
        print(inspect.getsource(DataFrame.take).rstrip())
    except Exception as e:
        print(f"(không lấy được source: {e})")

    # ---------------- Bằng chứng 2: explain() cả ba ----------------
    print()
    print("-" * 78)
    print("BẰNG CHỨNG 2 — explain(): tìm CollectLimit / LocalLimit / GlobalLimit")
    print("-" * 78)
    print("\n### (a) plan của df — cái mà collect() và collect()[:20] chạy:")
    df.explain(mode="formatted")
    print("\n### (b) plan của df.limit(20) — cái mà limit().collect() chạy:")
    df.limit(20).explain(mode="formatted")
    print("\n### (c) plan của df.take(20) / df.show(20) — take = limit(20).collect()")
    print("###     nên plan y hệt (b). In lại bằng đường vòng để tự kiểm chứng:")
    df.limit(20).explain()  # chế độ mặc định, ngắn, thấy ngay CollectLimit
    print("""
ĐỌC PLAN THẾ NÀO:
  (a) chỉ có FileScan csv -> action nào chạy trên plan này đều ĐỌC HẾT mọi partition
      và (với collect) kéo HẾT về driver. Không có nút nào chặn.
  (b)/(c) có CollectLimit 20 (dưới nó là LocalLimit 20 rồi GlobalLimit 20 khi
      qua shuffle). LocalLimit = mỗi partition tự cắt còn 20 dòng NGAY TẠI EXECUTOR
      -> dữ liệu bị chặn ở nguồn, không bao giờ có 1 triệu dòng bay về driver.
      CollectLimit còn khôn hơn: nó chạy partition ĐẦU TIÊN trước, đủ 20 dòng thì
      DỪNG, không đụng partition còn lại (nếu thiếu mới nhân lên theo
      spark.sql.limit.scaleUpFactor, mặc định 4). Đó là lý do numTasks = 1 ở dưới.
""")

    # ---------------- Bằng chứng 3: số task thật ----------------
    print("-" * 78)
    print("BẰNG CHỨNG 3 — SỐ TASK THẬT (đọc từ REST API, lọc theo jobGroup)")
    print("-" * 78)
    print(f"df có {nparts} partition. Action nào đụng {nparts} task = đọc hết dữ liệu.")
    print("Action nào chỉ 1 task = chỉ đọc 1 partition -> KHÔNG kéo hết về driver.")
    print()
    print("| Cách gọi | Dòng về driver | #job | #task thật | Thời gian (ms) | Có đọc hết dữ liệu? |")
    print("|---|---|---|---|---|---|")
    print(f"| `df.count()` *(mốc so sánh: full scan)* | 1 (con số) | {st_cnt['jobs']} | "
          f"{st_cnt['tasks']} | {ms_cnt:,.0f} | CÓ |")
    print(f"| `df.take(20)` | {len(r_take)} | {st_take['jobs']} | {st_take['tasks']} | "
          f"{ms_take:,.0f} | KHÔNG |")
    print(f"| `df.limit(20).collect()` | {len(r_lim)} | {st_lim['jobs']} | {st_lim['tasks']} | "
          f"{ms_lim:,.0f} | KHÔNG |")
    print(f"| `df.show(20)` | 20 (in ra) | {st_show['jobs']} | {st_show['tasks']} | "
          f"{ms_show:,.0f} | KHÔNG |")
    print(f"| `df.collect()` | ~1.000.163 | — | — | — | CÓ → **CHẾT** (xem mode kill) |")
    print(f"| `df.collect()[:20]` | ~1.000.163 rồi mới cắt | — | — | — | CÓ → **CHẾT** (mode danger) |")
    print()
    print("(#task = numTasks tổng các job cùng jobGroup, lấy từ REST /api/v1/applications/<id>/jobs)")
    print("(count() có 2 stage: partial aggregate trên N partition + final aggregate 1 task,")
    print(" nên numTasks của nó là N+1 chứ không phải N — đừng ngạc nhiên.)")

    # ---------------- Kết luận 3 dòng mà đề bài yêu cầu ----------------
    print()
    print("-" * 78)
    print("BA DÒNG KẾT LUẬN (đề bài: cách nào an toàn ở dữ liệu 1000×, cách nào chỉ may mắn)")
    print("-" * 78)
    print("""
1. take(20)            — AN TOÀN ở mọi cỡ dữ liệu. Nó = limit(20).collect(), có CollectLimit,
                         chỉ đọc partition đầu, tối đa 20 dòng về driver. Dữ liệu ×1000 vẫn 20 dòng.
2. limit(20).collect() — AN TOÀN, y hệt (1) — cùng một plan. Chỗ khác biệt duy nhất là bạn tự
                         gõ .collect(); nếu lỡ tay gõ limit(1_000_000).collect() thì lại chết,
                         nên nó an toàn nhờ CON SỐ bạn đặt, không phải nhờ bản thân limit.
3. show(20)            — AN TOÀN. Bên trong cũng là take(n) -> limit. Thêm một điểm cộng: nó
                         không giữ list Row trong RAM Python, chỉ in chuỗi ra rồi vứt.
   df.collect()[:20]   — KHÔNG AN TOÀN, và đây mới là bài học. Slice của Python chạy SAU khi
                         1 triệu Row đã nằm trong RAM driver. Ở dữ liệu 1000× nó chết chắc.
                         Nó "chạy được" trên máy bạn hôm nay chỉ là MAY MẮN vì dữ liệu còn nhỏ.

Nguyên tắc rút ra: giới hạn phải nằm trong PLAN (Spark biết), không nằm trong Python (Spark
không biết). Cứ nhìn explain(): thấy CollectLimit/LocalLimit là an toàn, không thấy là bom hẹn giờ.
""")
    spark.stop()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "fix"
    if mode in ("kill-oom", "kill-maxresult", "danger"):
        mode_kill(mode)
    elif mode == "fix":
        mode_fix()
    else:
        print(f"mode lạ: {mode}. Chọn: kill-oom | kill-maxresult | danger | fix")
        sys.exit(2)

# =============================================================================
# LỆNH CHẠY (từ repo root). LUẬT SẮT: chỉ MỘT app Spark trên cluster tại một thời điểm.
# Ba mode kill nên chạy LOCAL cho nhanh và khỏi tranh core (driver ở đâu cũng chết như nhau
# — driver luôn nằm ở client, cluster không cứu được nó; đó chính là điều bài này muốn dạy).
#
#   R=/workspace/labs/mini-project-1/exercises/a04_kill_driver.py
#
#   # 1) OOM thật
#   docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
#       --master 'local[2]' --driver-memory 512m $R kill-oom            > results/a04_kill_oom.txt 2>&1
#
#   # 2) maxResultSize
#   docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
#       --master 'local[2]' --driver-memory 512m $R kill-maxresult      > results/a04_kill_maxresult.txt 2>&1
#
#   # 3) cái bẫy collect()[:20]
#   docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit \
#       --master 'local[2]' --driver-memory 512m $R danger              > results/a04_danger.txt 2>&1
#
#   # 4) ba cách sửa (chạy được cả local lẫn cluster; cluster đẹp hơn vì task chạy thật ở xa)
#   make run F=labs/mini-project-1/exercises/a04_kill_driver.py         # -> mặc định mode fix
# =============================================================================
