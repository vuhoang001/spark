"""sparkutils — bộ đồ nghề dùng chung cho cả Mini Project 1.

Chủ sở hữu: nhóm L4 (bài A15–A20). Các nhóm khác CỨ IMPORT, ĐỪNG GHI ĐÈ file này.

Ba nhóm công cụ:
  1. Ống nghe partition  : partition_sizes() / partition_report()   <- A19
  2. Đồng hồ bấm giờ     : timeit()                                  <- luật "3 lần, bỏ lần 1"
  3. Máy đọc Spark UI    : stage_summary() qua REST API              <- lấy SỐ TASK THẬT

VÌ SAO cần máy đọc REST? Vì đề bài bắt "vào tab Stages, đếm số task, đọc quartile
Duration". Đọc bằng mắt thì (a) chậm, (b) không lưu được bằng chứng, (c) UI biến mất
khi app kết thúc. REST API `/api/v1/applications/<id>/stages/...` trả về ĐÚNG những
con số mà UI vẽ ra — vì UI cũng chỉ là một client của API đó. Ta lấy số, in ra
Markdown, dán vào report. Không bịa một chữ nào.

Cách dùng trong exercises/aXX_*.py:

    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.sparkutils import partition_report, timeit, stage_summary, md_table
"""

import json
import math
import statistics
import time
import urllib.error
import urllib.request
from datetime import datetime

# ---------------------------------------------------------------------------
# 0. Tiện ích in ấn — mọi thứ phải dán thẳng vào Markdown được
# ---------------------------------------------------------------------------

def md_table(headers, rows):
    """Trả về một bảng Markdown dạng chuỗi. Không căn lề cầu kỳ — Markdown không cần."""
    out = ["| " + " | ".join(str(h) for h in headers) + " |",
           "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def banner(text):
    line = "=" * 78
    return f"\n{line}\n{text}\n{line}"


def human_bytes(n):
    if n is None:
        return "?"
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,.0f} B"
        n /= 1024.0


# ---------------------------------------------------------------------------
# 1. ỐNG NGHE PARTITION  (bài A19 — "đồng hồ đo bạn sẽ dùng cả sự nghiệp")
# ---------------------------------------------------------------------------

def partition_sizes(df):
    """Số DÒNG trong từng partition của `df`, theo đúng thứ tự partition index.

        df.rdd.glom()        -> mỗi partition thành MỘT list các Row
          .map(len)          -> đếm độ dài list đó (chạy trên executor)
          .collect()         -> kéo N con số về driver

    ⚠️ CẢNH BÁO PRODUCTION (đề bài bắt ghi lại, và đúng là phải ghi):
    `glom()` GOM TOÀN BỘ dòng của một partition thành một list trong RAM executor.
    Với Olist (vài chục MB) thì vô hại. Với 1 TB thì đây là cách tự bắn vào chân:
    OOM executor ngay. Ở production dùng `partition_sizes_cheap()` bên dưới —
    nó chỉ đếm rồi trả về N con số, không vật chất hoá dữ liệu.

    Giữ hàm này vì đề bài yêu cầu đúng nó, và vì trên dữ liệu nhỏ nó ngắn gọn nhất.
    """
    return df.rdd.glom().map(len).collect()


def partition_sizes_cheap(df):
    """Bản production-safe của partition_sizes(): đếm bằng mapPartitions.

    Khác biệt DUY NHẤT nhưng sống còn: không bao giờ giữ cả partition trong RAM.
    `sum(1 for _ in it)` chạy trên iterator — bộ nhớ O(1), không phải O(số dòng).
    Kết quả trả về giống hệt glom().map(len).
    """
    return df.rdd.mapPartitions(lambda it: iter([sum(1 for _ in it)])).collect()


def partition_stats(sizes):
    """Thống kê sức khoẻ partition từ list số dòng."""
    n = len(sizes)
    total = sum(sizes)
    empty = sum(1 for s in sizes if s == 0)
    mean = total / n if n else 0.0
    stdev = statistics.pstdev(sizes) if n > 1 else 0.0
    mx = max(sizes) if sizes else 0
    return {
        "num_partitions": n,
        "total_rows": total,
        "empty": empty,
        "min": min(sizes) if sizes else 0,
        "max": mx,
        "mean": mean,
        "stddev": stdev,
        # skew_ratio = max/mean. >3 là ốm. =1 là hoàn hảo (không bao giờ có thật).
        "skew_ratio": (mx / mean) if mean else 0.0,
    }


def histogram(sizes, width=50, max_bars=40):
    """Histogram thô bằng ký tự '#': mỗi dòng = một partition.

    Vì sao không vẽ chart? Vì output phải dán được vào Markdown trong code block,
    và vì cái ta cần nhìn là HÌNH DẠNG (đều hay lệch), không phải mỹ thuật.
    """
    if not sizes:
        return "(rỗng)"
    mx = max(sizes) or 1
    lines = []
    shown = sizes[:max_bars]
    for i, s in enumerate(shown):
        bar = "#" * int(round(s / mx * width))
        lines.append(f"  p{i:<3} |{bar:<{width}}| {s:,}")
    if len(sizes) > max_bars:
        rest = sizes[max_bars:]
        lines.append(f"  ... còn {len(rest)} partition nữa "
                     f"(tổng {sum(rest):,} dòng, rỗng: {sum(1 for s in rest if s == 0)})")
    return "\n".join(lines)


def partition_report(df, label, cheap=False, show_hist=True):
    """In "phiếu khám sức khoẻ" của một DataFrame. Trả về dict stats để dùng tiếp.

    ⚠️ Mỗi lần gọi = một ACTION (collect) = Spark tính lại df từ đầu.
    Đó là cái giá phải trả để có số thật. Nếu df đắt, .cache() trước khi gọi.
    """
    sizes = partition_sizes_cheap(df) if cheap else partition_sizes(df)
    st = partition_stats(sizes)
    print(f"\n### {label}")
    print(f"    partition: {st['num_partitions']}   |   dòng: {st['total_rows']:,}   "
          f"|   rỗng: {st['empty']}   |   min: {st['min']:,}   max: {st['max']:,}   "
          f"|   mean: {st['mean']:,.1f}   stddev: {st['stddev']:,.1f}   "
          f"|   skew(max/mean): {st['skew_ratio']:.2f}x")
    if show_hist:
        print(histogram(sizes))
    st["sizes"] = sizes
    return st


# ---------------------------------------------------------------------------
# 2. ĐỒNG HỒ BẤM GIỜ  (luật sắt #4: chạy 3 lần, vứt lần 1, lấy min lần 2-3)
# ---------------------------------------------------------------------------

def timeit(fn, runs=3, label=""):
    """Chạy fn() `runs` lần, trả về (list_ms, warm_min_ms, ket_qua_lan_cuoi).

    Lần 1 bị VỨT khi tính warm_min vì nó gánh: JIT của JVM chưa nóng, page cache
    của OS còn lạnh, executor có thể còn đang đăng ký. Nó nói dối, một cách hệ thống.

    ⚠️ fn PHẢI kết thúc bằng ACTION thật (count/collect/write). Trả về DataFrame
    là đo lazy = đo con số 0.001s vô nghĩa (luật sắt #5).
    """
    times, result = [], None
    for i in range(runs):
        t0 = time.time()
        result = fn()
        dt = (time.time() - t0) * 1000
        times.append(dt)
        if label:
            print(f"    [{label}] lần {i + 1}: {dt:,.0f} ms" + ("   <- warmup, vứt" if i == 0 else ""))
    warm = min(times[1:]) if len(times) > 1 else times[0]
    return times, warm, result


# ---------------------------------------------------------------------------
# 3. MÁY ĐỌC SPARK UI qua REST API
# ---------------------------------------------------------------------------
# Nguyên tắc: gắn NHÃN (jobGroup) cho mỗi phép đo, rồi lọc theo nhãn.
#
# BẪY (đã dính rồi, ghi lại để đừng dính nữa):
#   - /jobs trả về job MỚI NHẤT TRƯỚC. Lấy jobs[-1] là lấy job CŨ NHẤT.
#   - spark.read.csv(header=True) tự đẻ ra một job riêng (đi đọc dòng header).
#     Không lọc theo jobGroup thì bạn đang đo nhầm job đó.
#   - PySpark 3.4 KHÔNG có sc.clearJobGroup(). Chỉ có setJobGroup(). Muốn "xoá"
#     nhãn thì set sang một nhãn rác khác.

_TIMEOUT = 15


def _api(sc, path):
    """GET {uiWebUrl}/api/v1/applications/{appId}{path} -> object JSON."""
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}{path}"
    try:
        with urllib.request.urlopen(url, timeout=_TIMEOUT) as r:
            return json.loads(r.read())
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
        print(f"    [sparkutils] REST lỗi ({url}): {e}")
        return None


def _ts(s):
    """'2026-07-14T07:35:02.123GMT' -> datetime."""
    return datetime.strptime(s.replace("GMT", "").strip(), "%Y-%m-%dT%H:%M:%S.%f")


def quantiles(values):
    """(min, p25, median, p75, max) — đúng 5 con số mà Summary Metrics của UI hiện.

    Dùng nội suy tuyến tính giống numpy; UI Spark dùng thuật toán xấp xỉ khác một
    chút nên có thể lệch vài %. Không sao — cái ta cần là TỈ LỆ max/median.
    """
    if not values:
        return (0, 0, 0, 0, 0)
    v = sorted(values)
    def q(p):
        if len(v) == 1:
            return v[0]
        idx = p * (len(v) - 1)
        lo, hi = math.floor(idx), math.ceil(idx)
        return v[lo] + (v[hi] - v[lo]) * (idx - lo)
    return (v[0], q(0.25), q(0.5), q(0.75), v[-1])


def jobs_of_group(sc, group):
    """Các job mang nhãn jobGroup == group (đã sắp lại theo thứ tự chạy)."""
    jobs = _api(sc, "/jobs") or []
    mine = [j for j in jobs if j.get("jobGroup") == group]
    return sorted(mine, key=lambda j: j.get("jobId", 0))


def job_duration_ms(sc, group):
    """Tổng Duration (theo đúng nghĩa cột Duration trên UI) của các job trong nhóm."""
    total = 0.0
    for j in jobs_of_group(sc, group):
        if j.get("submissionTime") and j.get("completionTime"):
            total += (_ts(j["completionTime"]) - _ts(j["submissionTime"])).total_seconds() * 1000
    return total


def _stage_attempts(sc, stage_id):
    data = _api(sc, f"/stages/{stage_id}") or []
    return data if isinstance(data, list) else [data]


def _tasks(sc, stage_id, attempt_id):
    data = _api(sc, f"/stages/{stage_id}/{attempt_id}/taskList?length=10000") or []
    return data if isinstance(data, list) else []


def stage_summary(sc, group, with_tasks=True):
    """Số liệu THẬT của mọi stage thuộc jobGroup `group`.

    Trả về list dict (theo thứ tự stageId tăng dần), mỗi dict:
        stage_id, name, num_tasks, duration_ms,
        input_bytes, shuffle_read_bytes, shuffle_write_bytes,
        zero_input_tasks   <- số task KHÔNG đọc được byte nào (input + shuffle-read = 0)
                              == chính là "192 task làm gì?" của bài A16
        task_duration_q    <- (min, p25, median, p75, max) ms  == Summary Metrics của UI
        skew_ratio         <- max/median của Duration. > 3 là skew (bài A18)

    Lưu ý: stage bị SKIP (nhờ cache/shuffle reuse) vẫn xuất hiện trong /jobs.stageIds
    nhưng REST trả status SKIPPED và numTasks có thể = 0 — ta bỏ qua chúng.
    """
    stage_ids = sorted({sid for j in jobs_of_group(sc, group) for sid in j.get("stageIds", [])})
    out = []
    for sid in stage_ids:
        for att in _stage_attempts(sc, sid):
            if att.get("status") == "SKIPPED":
                continue
            rec = {
                "stage_id": sid,
                "attempt_id": att.get("attemptId", 0),
                "name": (att.get("name") or "")[:45],
                "num_tasks": att.get("numTasks", 0),
                "duration_ms": att.get("executorRunTime", 0),
                "input_bytes": att.get("inputBytes", 0),
                "shuffle_read_bytes": att.get("shuffleReadBytes", 0),
                "shuffle_write_bytes": att.get("shuffleWriteBytes", 0),
                "zero_input_tasks": None,
                "task_duration_q": None,
                "skew_ratio": None,
            }
            if att.get("submissionTime") and att.get("completionTime"):
                rec["wall_ms"] = (_ts(att["completionTime"]) - _ts(att["submissionTime"])).total_seconds() * 1000
            if with_tasks and rec["num_tasks"]:
                tasks = _tasks(sc, sid, rec["attempt_id"])
                durs, zero = [], 0
                for t in tasks:
                    m = t.get("taskMetrics") or {}
                    durs.append(t.get("duration", 0))
                    read = ((m.get("inputMetrics") or {}).get("bytesRead", 0)
                            + (m.get("shuffleReadMetrics") or {}).get("localBytesRead", 0)
                            + (m.get("shuffleReadMetrics") or {}).get("remoteBytesRead", 0))
                    if read == 0:
                        zero += 1
                rec["zero_input_tasks"] = zero
                rec["tasks_seen"] = len(tasks)
                q = quantiles(durs)
                rec["task_duration_q"] = q
                rec["skew_ratio"] = (q[4] / q[2]) if q[2] else 0.0
            out.append(rec)
    return out


def print_stage_summary(sc, group, title=None):
    """In bảng Markdown các stage của một jobGroup. Đây là "bằng chứng" nộp được."""
    st = stage_summary(sc, group)
    if title:
        print(f"\n{title}")
    if not st:
        print("  (REST không trả về stage nào — app đã kết thúc? jobGroup sai tên?)")
        return st
    rows = []
    for s in st:
        q = s["task_duration_q"] or (0, 0, 0, 0, 0)
        rows.append([
            s["stage_id"], s["name"], s["num_tasks"],
            s["zero_input_tasks"] if s["zero_input_tasks"] is not None else "-",
            f"{s.get('wall_ms', 0):,.0f}",
            f"{q[0]:.0f}/{q[1]:.0f}/{q[2]:.0f}/{q[3]:.0f}/{q[4]:.0f}",
            f"{s['skew_ratio']:.2f}x" if s["skew_ratio"] else "-",
            human_bytes(s["input_bytes"]),
            human_bytes(s["shuffle_write_bytes"]),
        ])
    print(md_table(
        ["stage", "tên", "task", "task 0 byte", "wall ms",
         "task dur min/p25/med/p75/max (ms)", "max/med", "input", "shuffle write"],
        rows))
    return st


# ---------------------------------------------------------------------------
# 4. CÔNG THỨC PARTITION LÚC ĐỌC (lesson 4 §3.2) — để đối chiếu dự đoán vs thực tế
# ---------------------------------------------------------------------------

def wait_for_executors(sc, min_executors=1, timeout=60, poll=0.5):
    """Chờ executor ĐĂNG KÝ XONG rồi mới đọc defaultParallelism. Trả về dP đã ổn định.

    BẪY CÓ THẬT, ĐÃ DÍNH (A15): ở standalone/client mode, `spark-submit` trả quyền
    điều khiển về cho code Python NGAY khi SparkContext dựng xong — nhưng lúc đó
    executor CHƯA kịp đăng ký với master (mất ~2-5 giây). Mà:

        defaultParallelism (standalone) = tổng số core của các executor ĐANG đăng ký

    Chưa có executor nào -> tổng = 0 -> Spark lấy giá trị sàn = 2. Nên nếu bạn đọc
    sc.defaultParallelism ở dòng đầu tiên, trên cluster 6 core bạn vẫn nhận về **2**,
    và mọi phép tính dựa trên nó đều sai — trong khi Spark, lúc lập kế hoạch đọc file
    vài giây sau đó, lại dùng dP=6 THẬT. Kết quả: dự đoán lệch thực tế, và bạn đi tìm
    lỗi ở công thức trong khi công thức hoàn toàn đúng.

    Hàm này chờ đến khi (a) đủ min_executors executor đã đăng ký, và (b) dP không đổi
    qua 3 lần đo liên tiếp — rồi mới trả về. local[*] thì trả ngay (không có gì để chờ).
    """
    if sc.master.startswith("local"):
        return sc.defaultParallelism
    t0, last, stable = time.time(), -1, 0
    while time.time() - t0 < timeout:
        # getExecutorMemoryStatus đếm cả DRIVER -> trừ 1 để ra số executor thật.
        n_exec = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
        dp = sc.defaultParallelism
        if n_exec >= min_executors and dp == last:
            stable += 1
            if stable >= 3:
                return dp
        else:
            stable = 0
        last = dp
        time.sleep(poll)
    print(f"    [sparkutils] CẢNH BÁO: hết {timeout}s chờ executor; "
          f"defaultParallelism={sc.defaultParallelism} có thể chưa ổn định.")
    return sc.defaultParallelism


def predict_read_partitions(total_bytes, num_files, default_parallelism,
                            max_partition_bytes, open_cost_bytes=4 * 1024 * 1024):
    """Dự đoán số partition khi ĐỌC file splittable, theo đúng công thức lesson 4.

        bytesPerCore  = (tổng bytes + số file × openCost) / defaultParallelism
        maxSplitBytes = min(maxPartitionBytes, max(openCost, bytesPerCore))
        numPartitions ≈ ceil(tổng bytes / maxSplitBytes)

    Dòng cuối là XẤP XỈ: Spark còn bin-pack các khúc và cộng openCost cho MỖI khúc
    khi đóng gói, nên số thật có thể lệch 1–2 partition. Chỗ lệch đó chính là bài
    tập của A15 — script sẽ in cả hai và bắt bạn giải thích.
    """
    bytes_per_core = (total_bytes + num_files * open_cost_bytes) / max(default_parallelism, 1)
    max_split = min(max_partition_bytes, max(open_cost_bytes, bytes_per_core))
    return {
        "bytes_per_core": bytes_per_core,
        "max_split_bytes": max_split,
        "predicted_partitions": max(1, math.ceil(total_bytes / max_split)),
    }


def parse_size(s):
    """'128m' -> 134217728. Chấp nhận '4m', '512k', '1g' hoặc số byte trần."""
    s = str(s).strip().lower()
    mult = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}
    if s and s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(s)


# ---------------------------------------------------------------------------
# 5. VAN AN TOÀN CHO partitionBy  (bài A20 — ingest.py sẽ import hàm này)
# ---------------------------------------------------------------------------
# Quy tắc nghề: mỗi file Parquet nên nằm trong 64–256 MB.
# Đề bài bắt partitionBy("order_date") -> ~600 ngày -> 600 file vài chục KB.
# Đó là MÂU THUẪN CÓ THẬT giữa yêu cầu học và quy tắc nghề, không phải bạn làm sai.
# Van này là cách trưởng thành để sống chung với nó: chọn ĐỘ MỊN theo KÍCH THƯỚC.

TARGET_FILE_BYTES = 64 * 1024 * 1024      # ngưỡng dưới của quy tắc 64–256MB
GRAIN_OPTIONS = ("day", "month", "year", "none")


def choose_partition_grain(total_bytes, num_buckets_by_grain,
                           target_bytes=TARGET_FILE_BYTES):
    """Chọn độ mịn partitionBy sao cho MỖI partition-value đạt >= target_bytes.

    total_bytes           : cỡ dữ liệu (ước lượng sau nén Parquet — ĐO, đừng đoán)
    num_buckets_by_grain  : {"day": 610, "month": 25, "year": 3} — đếm distinct thật
    Trả về: (grain, bảng giải thích) — grain mịn nhất mà vẫn đạt ngưỡng;
            nếu không grain nào đạt, trả "none" (ghi phẳng, không partitionBy).

    VÌ SAO đi từ mịn đến thô: partition mịn = filter theo ngày nhanh nhất (partition
    pruning). Ta chỉ thô hoá KHI BỊ ÉP — khi file đã nhỏ đến mức phí mở file và phí
    metadata ăn hết lợi ích pruning.
    """
    table = []
    chosen = "none"
    for grain in ("day", "month", "year"):
        n = num_buckets_by_grain.get(grain)
        if not n:
            continue
        avg = total_bytes / n
        ok = avg >= target_bytes
        table.append((grain, n, avg, ok))
    for grain, n, avg, ok in table:   # day -> month -> year: lấy cái MỊN NHẤT mà đạt
        if ok:
            chosen = grain
            break
    return chosen, table
