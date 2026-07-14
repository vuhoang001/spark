"""uiprobe — đọc Spark UI bằng CODE thay vì bằng MẮT.

Dùng chung cho nhóm bài L3 (A10..A14). File này KHÔNG phải sparkutils.py
(sparkutils.py là của nhóm L4 — partition_sizes()/glom()). Đừng gộp hai file.

VÌ SAO có file này:
    Đề bài A10–A14 bắt "vào tab Jobs / tab Stages đọc số". Đọc bằng mắt thì
    không dán được vào report, không tái lập được, và dễ chép nhầm. Nhưng mọi
    con số trên Spark UI đều do một REST API sinh ra:

        http://<driver>:4040/api/v1/applications/<appId>/jobs
        http://<driver>:4040/api/v1/applications/<appId>/stages
        .../stages/<stageId>/<attemptId>/taskList

    UI chỉ là HTML vẽ lại JSON đó. Ta gọi thẳng JSON -> số đo tự động, thô,
    truy ngược được. (Vẫn nên mở UI bằng mắt một lần để thấy DAG — hình ảnh
    thì API không trả về.)

BẪY đã dính (ghi lại để không dính lần 2):
    1. /jobs trả job MỚI NHẤT TRƯỚC. Lấy jobs[-1] là lấy job CŨ NHẤT, không
       phải job vừa chạy. => luôn lọc theo jobGroup, đừng lấy theo vị trí.
    2. spark.read.csv(header=True) mà KHÔNG truyền schema thì Spark phải đọc
       dòng header -> đẻ ra MỘT JOB RIÊNG, dính vào jobGroup đang mở. => trong
       các bài này luôn truyền schema tường minh, và đọc file TRƯỚC khi
       setJobGroup lần đầu.
    3. PySpark 3.4 KHÔNG có sc.clearJobGroup(). Chỉ có setJobGroup(). Muốn
       "đóng" một nhóm thì mở nhóm mới.
    4. taskList mặc định chỉ trả 100 task. Stage 200 task -> phải truyền
       length=... nếu không sẽ đếm thiếu (và kết luận sai về AQE).
"""

import json
import time
import urllib.request
from datetime import datetime

# taskList mặc định 100 -> thiếu với shuffle.partitions=200. Xin dư cho chắc.
TASK_LIST_LIMIT = 2000


# ---------------------------------------------------------------- REST cơ bản
def rest(ui_url, path):
    """GET {ui}/api/v1{path} -> JSON. Đây chính là nguồn dữ liệu của Spark UI."""
    with urllib.request.urlopen(ui_url + "/api/v1" + path, timeout=15) as r:
        return json.loads(r.read())


def _ts(s):
    """'2026-07-14T07:35:02.123GMT' -> datetime."""
    return datetime.strptime(s.replace("GMT", "").strip(), "%Y-%m-%dT%H:%M:%S.%f")


def _dur_ms(job):
    if job.get("submissionTime") and job.get("completionTime"):
        return (_ts(job["completionTime"]) - _ts(job["submissionTime"])).total_seconds() * 1000
    return 0.0


def wait_for_executors(spark, expected=1, timeout_s=30):
    """Ép executor đăng ký XONG rồi mới đo.

    Ở cluster mode, action đầu tiên gánh luôn thời gian bật JVM executor ->
    số đo lần đầu nói dối. Chạy một job rác trước để "mồi" cluster.
    """
    sc = spark.sparkContext
    sc.parallelize(range(8), 2).count()
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        execs = rest(sc.uiWebUrl, "/applications/%s/executors" % sc.applicationId)
        alive = [e for e in execs if e["id"] != "driver" and e.get("isActive")]
        if len(alive) >= expected or sc.master.startswith("local"):
            break
        time.sleep(1)
    time.sleep(1)


# --------------------------------------------------------------- Job & Stage
def jobs_of_group(spark, group):
    """Mọi job thuộc jobGroup `group`, sắp xếp theo jobId tăng dần.

    Lọc theo nhãn thay vì theo vị trí -> miễn nhiễm với bẫy số 1 ở docstring.
    """
    sc = spark.sparkContext
    jobs = rest(sc.uiWebUrl, "/applications/%s/jobs" % sc.applicationId)
    mine = [j for j in jobs if j.get("jobGroup") == group]
    return sorted(mine, key=lambda j: j["jobId"])


def stage_index(spark):
    """{stageId: stage} — với stage có nhiều attempt thì lấy attempt mới nhất."""
    sc = spark.sparkContext
    stages = rest(sc.uiWebUrl, "/applications/%s/stages" % sc.applicationId)
    out = {}
    for s in stages:
        sid = s["stageId"]
        if sid not in out or s.get("attemptId", 0) >= out[sid].get("attemptId", 0):
            out[sid] = s
    return out


def task_list(spark, stage_id, attempt_id=0):
    """Danh sách task của một stage — nguồn của Summary Metrics trên UI."""
    sc = spark.sparkContext
    path = "/applications/%s/stages/%d/%d/taskList?length=%d" % (
        sc.applicationId, stage_id, attempt_id, TASK_LIST_LIMIT)
    try:
        return rest(sc.uiWebUrl, path)
    except Exception as e:  # stage bị skip hoàn toàn thì không có task
        print("   (!) không lấy được taskList stage %d: %s" % (stage_id, e))
        return []


def summarize_group(spark, group):
    """Tóm tắt một nhóm job: mấy job, mấy stage, mấy shuffle, mấy task.

    CÁCH ĐẾM SHUFFLE (đây là chỗ dễ cãi nhau, nên nói rõ):
        Ta đếm số stage có shuffleWriteBytes > 0, tức số stage đã GHI shuffle
        file ra đĩa. Mỗi `Exchange` trong physical plan sinh ra đúng một stage
        như vậy => số stage ghi shuffle = số Exchange thực sự CHẠY.

        Vì sao không đếm chữ "Exchange" trong explain()? Vì với action count(),
        cái Exchange do chính count() sinh ra (gom partial count về 1 partition)
        KHÔNG nằm trong plan của DataFrame bạn gọi explain() — nó chỉ xuất hiện
        lúc chạy. Đếm bằng plan sẽ thiếu đúng cái shuffle hay bị quên nhất.

    Stage SKIPPED không tính vào `stages_run` (nó không chạy) nhưng vẫn nằm
    trong job.stageIds -> đó là lý do hai con số này lệch nhau, và chính chỗ
    lệch đó là bài A12.
    """
    jobs = jobs_of_group(spark, group)
    sidx = stage_index(spark)

    stage_ids, rows = [], []
    for j in jobs:
        for sid in sorted(j.get("stageIds", [])):
            if sid not in stage_ids:
                stage_ids.append(sid)

    n_skipped = n_run = n_shuffle_write = 0
    for sid in stage_ids:
        s = sidx.get(sid)
        if s is None:
            continue
        skipped = s.get("status") == "SKIPPED"
        wrote = (s.get("shuffleWriteBytes") or 0) > 0
        if skipped:
            n_skipped += 1
        else:
            n_run += 1
            if wrote:
                n_shuffle_write += 1
        rows.append({
            "stageId": sid,
            "attemptId": s.get("attemptId", 0),
            "status": s.get("status"),
            "name": (s.get("name") or "").split("(")[0].strip(),
            "numTasks": s.get("numTasks", 0),
            "inputRecords": s.get("inputRecords", 0),
            "shuffleWriteBytes": s.get("shuffleWriteBytes") or 0,
            "shuffleWriteRecords": s.get("shuffleWriteRecords") or 0,
            "shuffleReadBytes": s.get("shuffleReadBytes") or 0,
            "shuffleReadRecords": s.get("shuffleReadRecords") or 0,
            "skipped": skipped,
        })

    return {
        "group": group,
        "jobs": len(jobs),
        "job_ids": [j["jobId"] for j in jobs],
        "stages_total": len(stage_ids),   # gồm cả skipped (đúng như tab Jobs hiện)
        "stages_run": n_run,
        "stages_skipped": n_skipped,
        "shuffles": n_shuffle_write,
        "tasks": sum(j.get("numTasks", 0) for j in jobs),
        "duration_ms": sum(_dur_ms(j) for j in jobs),
        "stage_rows": rows,
    }


# ------------------------------------------------------------------- In ấn
def print_stage_table(summary, indent="  "):
    """Bảng stage — dán thẳng vào Markdown được."""
    print(indent + "| stage | trạng thái | tên (operator cuối) | task | input rec | shuffle WRITE | shuffle READ |")
    print(indent + "|---|---|---|---|---|---|---|")
    for r in summary["stage_rows"]:
        print(indent + "| %d | %s | %s | %d | %s | %s B / %s rec | %s B / %s rec |" % (
            r["stageId"],
            "SKIPPED" if r["skipped"] else r["status"],
            r["name"],
            r["numTasks"],
            "{:,}".format(r["inputRecords"]),
            "{:,}".format(r["shuffleWriteBytes"]), "{:,}".format(r["shuffleWriteRecords"]),
            "{:,}".format(r["shuffleReadBytes"]), "{:,}".format(r["shuffleReadRecords"]),
        ))


def print_ascii_dag(summary, indent="  "):
    """DAG dạng chữ: mỗi hộp một stage, mũi tên là shuffle.

    Đây là bản thay thế cho "chụp ảnh DAG Visualization" — ảnh thì không dán
    được vào file .md trong git, còn cái này thì được. Vẫn nên mở UI ngắm
    hình thật một lần cho quen mặt.
    """
    rows = summary["stage_rows"]
    if not rows:
        print(indent + "(không có stage)")
        return
    for i, r in enumerate(rows):
        tag = " [SKIPPED - dùng lại shuffle file cũ]" if r["skipped"] else ""
        print(indent + "┌─ stage %-3d %-28s task=%-4d%s" % (
            r["stageId"], r["name"][:28], r["numTasks"], tag))
        if i < len(rows) - 1:
            if r["shuffleWriteBytes"] > 0:
                print(indent + "└─▶ SHUFFLE (%s bytes ghi ra local disk executor)" % (
                    "{:,}".format(r["shuffleWriteBytes"])))
            else:
                print(indent + "└─▶ (không shuffle — stage kế tiếp thuộc job khác)")
