"""benchmark.py — Checkpoint 3 + thư viện đo đạc dùng chung cho track L6 (A30..A36).

File này có HAI vai trò:

  1) THƯ VIỆN. Các bài A30..A36 `import benchmark as B` để dùng chung:
       - B.Probe(...)      : đo 1 action -> wall time + metric THẬT lấy từ Spark REST API
       - B.timeit(...)     : chạy N lần, VỨT lần 1 (JVM warmup), lấy min các lần sau
       - B.du_bytes(...)   : kích thước thư mục (byte, chính xác — không phải `du -sh` làm tròn)
       - B.count_part_files(...) : đếm file part-*.parquet
       - B.md(...)         : in bảng Markdown dán thẳng vào report

  2) CHECKPOINT 3 (chạy trực tiếp). So sánh CSV vs Parquet trên 2 query:
       Query A (aggregate)     : doanh thu đơn 'delivered' theo THÁNG      -> quét toàn bảng
       Query B (điểm rơi pruning): doanh thu đúng ngày 2018-07-02          -> chỉ 1 partition

CHẠY (Checkpoint 3):
    make run-local F=labs/mini-project-1/src/benchmark.py
    # local[2] cho số đo ổn định (đề bài yêu cầu). Cluster cũng chạy được nhưng
    # 2 executor ở 2 container -> thêm nhiễu mạng, không giúp gì với 121MB dữ liệu.

OUTPUT: stdout (bảng Markdown). Dữ liệu phụ trợ ghi vào /workspace/data/bench/cp3/

-------------------------------------------------------------------------------
VÌ SAO PHẢI TỰ VIẾT LỚP ĐO METRIC (Probe) THAY VÌ NHÌN MẮT VÀO SPARK UI?
Đề bài bảo "vào tab SQL, click query, đọc `number of files read` / `size of files read`".
Đúng, nhưng: (a) app tắt là UI biến mất, không truy ngược được -> không có bằng chứng;
(b) chép tay thì bịa số lúc nào không biết. Spark UI thực chất chỉ là HTML render lại
REST API `/api/v1/applications/<id>/sql?details=true`. Ta gọi thẳng API đó -> con số
IN RA FILE, kiểm tra lại được. Đây chính là "bằng chứng truy ngược được" mà project đòi.
-------------------------------------------------------------------------------

⚠️ BA CÁI BẪY VỀ METRIC — đọc trước khi tin bất kỳ con số nào:

  BẪY 1. `size of files read` (metric `filesSize` của node Scan) là TỔNG KÍCH THƯỚC
         CÁC FILE ĐƯỢC MỞ, **không phải** số byte thực sự đọc từ đĩa. Nghĩa là:
         column pruning (A30) KHÔNG làm metric này nhỏ đi — đọc 1 cột hay 11 cột thì
         `size of files read` vẫn bằng nhau, vì vẫn mở đúng bấy nhiêu file!
         Đề bài chỉ dẫn đọc metric này cho A30 -> ĐỀ BÀI SAI Ở CHỖ NÀY. Nó chỉ đúng cho
         PARTITION pruning (A36 — prune ở mức FILE) chứ không đúng cho COLUMN pruning.
         => muốn thấy column pruning phải nhìn `input_bytes` (bên dưới).

  BẪY 2. `input_bytes` (Stage -> Input Size trên UI) là byte THẬT đọc qua Hadoop
         FileSystem (đếm ở tầng InputStream). Column pruning + row-group skipping
         hiện ra ở ĐÂY. Đây mới là con số để "chỉ vào bytes read" như rubric đòi.

  BẪY 3. `df.count()` KHÔNG đọc cột nào cả — Spark prune xuống 0 cột, chỉ đọc metadata
         số dòng trong footer. Nên `select("*").count()` KHÔNG phải query "đọc hết cột"
         như đề bài tưởng. Muốn ép đọc thật đủ 11 cột phải dùng `.write.format("noop")`
         (action thật, không ghi gì ra đĩa) hoặc aggregate trên mọi cột.
"""

import json
import os
import re
import sys
import time
import urllib.request

# Tự đưa thư mục src/ vào sys.path -> `from schemas import ...` bên dưới chạy được
# dù file này được import từ exercises/ hay được spark-submit chạy trực tiếp.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------- đường dẫn
SRC = "/workspace/data/olist"
ORDERS_CSV = f"{SRC}/olist_orders_dataset.csv"
ITEMS_CSV = f"{SRC}/olist_order_items_dataset.csv"
SILVER = "/workspace/data/output/silver/orders_clean"  # do nhóm Checkpoint 1+2 đẻ ra
BENCH = "/workspace/data/bench"  # sân chơi riêng của L6 — ĐỪNG ghi vào silver/

# ---------------------------------------------------------------- schema
# Vì sao khai schema tường minh mà không inferSchema?
#   inferSchema = Spark đọc TOÀN BỘ file 1 lượt chỉ để đoán kiểu -> một action trá hình
#   (bài A5). Trong file benchmark thì càng chết: nó cộng thêm 1 job vào phép đo,
#   và phép đo của bạn hoá ra đang đo... việc đoán kiểu.
# Nguồn schema: src/schemas.py (Checkpoint 1). Import nó thay vì chép lại — chép lại là
# mầm mống của "hai bảng cùng tên, khác kiểu". Chưa có schemas.py -> fallback tại chỗ
# để bài L6 vẫn ĐỨNG MỘT MÌNH ĐƯỢC.
sys_path_note = "schemas.py"
try:
    from schemas import ORDERS as ORDERS_SCHEMA, ORDER_ITEMS as ITEMS_SCHEMA
except ImportError:  # pragma: no cover
    sys_path_note = "fallback nội bộ (schemas.py chưa có)"
    ORDERS_SCHEMA = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
    ])
    ITEMS_SCHEMA = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
    ])


# =============================================================================
# 1. ĐỌC DỮ LIỆU
# =============================================================================
def read_orders_csv(spark):
    """orders thô từ CSV + cột dẫn xuất order_date (DateType)."""
    return (
        spark.read.schema(ORDERS_SCHEMA).option("header", True).csv(ORDERS_CSV)
        .withColumn("order_date", F.to_date("order_purchase_timestamp"))
    )


def read_items_csv(spark):
    """order_items thô từ CSV. Đây là bảng DUY NHẤT có cột `price`."""
    return spark.read.schema(ITEMS_SCHEMA).option("header", True).csv(ITEMS_CSV)


def orders_enriched(spark):
    """Bảng làm việc chung của cả track L6: orders + doanh thu mỗi đơn.

    VÌ SAO PHẢI GHÉP? Đề bài A30 bảo "trên orders_clean chạy select('price').agg(sum)".
    Nhưng bảng orders của Olist KHÔNG CÓ cột `price` (kiểm tra: header chỉ có 8 cột,
    price nằm ở order_items). => ĐỀ BÀI THIẾU MỘT BƯỚC. Ta vá bằng cách join sẵn
    doanh thu của đơn vào orders, ra bảng 11 cột:
        8 cột gốc + order_date + price (tổng tiền hàng của đơn) + freight_value
    Bảng này dùng cho A30 (column pruning), A31 (nén), A34 (evolution),
    A35 (small files), A36 (partition pruning) và cả Checkpoint 3.

    Lưu ý: đơn KHÔNG có item nào (đơn huỷ) -> price NULL. Không drop, giữ nguyên để
    số dòng vẫn khớp 99.441 (LEFT join).
    """
    o = read_orders_csv(spark)
    rev = (
        read_items_csv(spark)
        .groupBy("order_id")
        .agg(F.sum("price").alias("price"), F.sum("freight_value").alias("freight_value"))
    )
    return o.join(rev, "order_id", "left")


def load_orders_clean(spark):
    """Nếu nhóm Checkpoint 1+2 đã đẻ ra silver/orders_clean thì DÙNG LẠI (đúng tinh thần
    pipeline chung). Chưa có thì tự dựng từ CSV — để bài L6 không bị block."""
    if os.path.isdir(SILVER) and count_part_files(SILVER) > 0:
        df = spark.read.parquet(SILVER)
        if "price" not in df.columns:  # silver chỉ có orders trần -> vẫn phải ghép giá
            rev = (read_items_csv(spark).groupBy("order_id")
                   .agg(F.sum("price").alias("price"),
                        F.sum("freight_value").alias("freight_value")))
            df = df.join(rev, "order_id", "left")
        return df, f"silver ({SILVER})"
    return orders_enriched(spark), "CSV gốc (silver chưa có)"


# =============================================================================
# 2. ĐO ĐẠC — Probe: bọc quanh MỘT action, moi metric thật từ REST API
# =============================================================================
def _get(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())


_SIZE_RE = re.compile(r"([\d.]+)\s*(B|KiB|MiB|GiB|TiB)")
_UNIT = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def metric_total(v):
    """Lấy con số TỔNG từ giá trị metric của REST API.

    ⚠️ BẪY ĐÃ DÍNH (làm sai toàn bộ cột `size of files read` ở lần chạy đầu):
    metric của Spark có HAI DẠNG.
      - Dạng 1 (một dòng):  '112,650'
      - Dạng 2 (HAI dòng):  'total (min, med, max (stageId: taskId))\\n16.8 MiB (0.0 B, 0.0 B, 16.8 MiB (driver))'

    Dạng 2 là dạng phổ biến của metric kiểu size/timing. Lấy `.split("\\n")[0]` là lấy
    đúng cái DÒNG TIÊU ĐỀ 'total (min, med, max ...)' — trong đó KHÔNG có số nào cả
    -> parse ra None -> cộng 0 -> báo cáo hiện '0 B' dù thật ra đọc 16.8 MiB.
    Số tổng nằm ở ĐẦU DÒNG CUỐI. Vậy: luôn lấy dòng cuối."""
    lines = [ln.strip() for ln in str(v or "").split("\n") if ln.strip()]
    if not lines:
        return ""
    return lines[-1]


_INT_RE = re.compile(r"-?[\d,]+")


def parse_int(s):
    """'112,650 (0, 0, 112,650)' -> 112650. Lấy số nguyên ĐẦU TIÊN."""
    m = _INT_RE.search(str(s or ""))
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


def parse_size(s):
    """'9.6 MiB' -> 10066329 byte. Spark format metric size thành chuỗi -> phải parse.
    CẢNH BÁO: Spark làm tròn 1 chữ số thập phân => con số này chỉ CHÍNH XÁC ~1%.
    Cần chính xác tuyệt đối thì dùng input_bytes (số nguyên) hoặc du_bytes()."""
    if not s:
        return None
    m = _SIZE_RE.search(str(s))
    if not m:
        return None
    return int(float(m.group(1)) * _UNIT[m.group(2)])


class Probe:
    """Bọc quanh 1 action. Sau khi thoát, có:
        .wall        thời gian driver chờ (giây)
        .input_bytes byte THẬT đọc từ FileSystem (cộng qua các stage)  <-- CON SỐ VÀNG
        .scan        dict metric của các node Scan (files read / size of files read...)
        .rows_scanned số dòng node Scan nhả ra (thấy row-group skipping ở đây — A32)

    CƠ CHẾ: đặt jobGroup trước khi chạy -> sau đó lọc job/stage theo đúng nhãn đó.
    BẪY (đã dính): REST /jobs trả job MỚI NHẤT TRƯỚC, và spark.read.csv(header=True)
    cũng đẻ một job riêng. Lấy "3 job cuối" là lấy nhầm. Lọc theo jobGroup mới chắc.
    (PySpark 3.4 KHÔNG có sc.clearJobGroup() — nên mỗi Probe đặt một nhãn mới đè lên.)
    """

    _seq = 0

    def __init__(self, spark, label):
        Probe._seq += 1
        self.spark = spark
        self.sc = spark.sparkContext
        self.label = label
        self.group = f"probe-{Probe._seq:03d}-{re.sub('[^a-zA-Z0-9]', '_', label)[:40]}"
        self.ui = self.sc.uiWebUrl
        self.app = self.sc.applicationId
        self.wall = None
        self.input_bytes = None
        self.input_records = None
        self.scan = {}
        self.rows_scanned = None

    def __enter__(self):
        self.sc.setJobGroup(self.group, self.label)
        self._sql_seen = self._sql_ids()
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        self.wall = time.time() - self.t0
        if exc[0] is not None:
            return False
        # Listener bus của Spark là BẤT ĐỒNG BỘ: action xong không có nghĩa là
        # metric đã được ghi vào store. Không ngủ -> đọc REST ra rỗng/thiếu.
        time.sleep(1.2)
        try:
            self._collect()
        except Exception as e:  # noqa: BLE001 — thà mất metric còn hơn hỏng cả script
            print(f"[Probe] KHÔNG lấy được metric ({type(e).__name__}: {e})")
        return False

    # -- REST helpers ---------------------------------------------------------
    def _sql_ids(self):
        if not self.ui:
            return set()
        try:
            return {e["id"] for e in _get(f"{self.ui}/api/v1/applications/{self.app}/sql?details=false&length=1000")}
        except Exception:
            return set()

    def _collect(self):
        if not self.ui:
            return
        base = f"{self.ui}/api/v1/applications/{self.app}"

        # (a) input bytes: job -> stageIds -> stage.inputBytes
        jobs = [j for j in _get(f"{base}/jobs") if j.get("jobGroup") == self.group]
        want = {sid for j in jobs for sid in j.get("stageIds", [])}
        stages = _get(f"{base}/stages")
        seen, ib, ir = set(), 0, 0
        for s in stages:
            sid = s["stageId"]
            if sid in want and sid not in seen:  # mỗi stage có thể có nhiều attempt
                seen.add(sid)
                ib += s.get("inputBytes", 0)
                ir += s.get("inputRecords", 0)
        self.input_bytes, self.input_records = ib, ir

        # (b) metric node Scan: lấy các SQL execution MỚI xuất hiện trong probe này
        execs = _get(f"{base}/sql?details=true&length=1000")
        new = [e for e in execs if e["id"] not in self._sql_seen]
        agg, rows = {}, 0
        for e in new:
            for node in e.get("nodes", []):
                if "Scan" not in node.get("nodeName", ""):
                    continue
                for m in node.get("metrics", []):
                    n = m.get("name")
                    v = metric_total(m.get("value"))
                    if n == "number of files read":
                        agg["files"] = agg.get("files", 0) + (parse_int(v) or 0)
                    elif n == "size of files read":
                        agg["files_size"] = (agg.get("files_size") or 0) + (parse_size(v) or 0)
                        agg["files_size_str"] = v
                    elif n == "number of partitions read":
                        agg["partitions"] = agg.get("partitions", 0) + (parse_int(v) or 0)
                    elif n == "number of output rows":
                        rows += (parse_int(v) or 0)
        self.scan = agg
        self.rows_scanned = rows or None

    # -- tiện in ra -----------------------------------------------------------
    @property
    def files(self):
        return self.scan.get("files")

    @property
    def files_size(self):
        return self.scan.get("files_size")

    def row(self):
        return [
            f"{self.wall:.2f}s",
            str(self.files if self.files is not None else "?"),
            human(self.files_size),
            human(self.input_bytes),
        ]


def timeit(fn, runs=3, drop=1, label=""):
    """Chạy fn() `runs` lần, VỨT `drop` lần đầu (JVM warmup + page cache lạnh),
    trả (best, all_times). LUẬT: fn PHẢI kết thúc bằng action thật.

    Vì sao vứt lần 1: JIT chưa nóng, code gen của Spark chưa cache, OS page cache
    chưa có file -> lần 1 luôn chậm 2-5×. Báo cáo lấy lần 1 = báo cáo sai bản chất.
    Nhưng vẫn IN CẢ lần 1 ra để người đọc thấy chênh lệch nóng/lạnh (đề bài đòi)."""
    times = []
    for i in range(runs):
        t0 = time.time()
        fn()
        times.append(time.time() - t0)
    kept = times[drop:] or times
    if label:
        print(f"  [{label}] các lần chạy: "
              + ", ".join(f"{t:.2f}s" for t in times)
              + f"  -> lấy min(sau warmup) = {min(kept):.2f}s")
    return min(kept), times


# =============================================================================
# 3. ĐO TRÊN ĐĨA
# =============================================================================
def du_bytes(path):
    """Tổng byte thật của thư mục. Dùng os.walk chứ không `du -sh` vì du làm tròn
    ('4.0K') -> mất độ phân giải khi so 4 codec chênh nhau vài phần trăm."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def list_part_files(path):
    out = []
    for root, _dirs, files in os.walk(path):
        for f in files:
            if f.startswith("part-"):
                out.append(os.path.join(root, f))
    return out


def count_part_files(path):
    return len(list_part_files(path))


def count_dirs(path, prefix="order_date="):
    n = 0
    for root, dirs, _files in os.walk(path):
        n += sum(1 for d in dirs if d.startswith(prefix))
    return n


def human(n):
    if n is None:
        return "?"
    n = float(n)
    for u in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or u == "GB":
            return f"{n:.1f} {u}" if u != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} GB"


def md(headers, rows):
    """In bảng Markdown — dán thẳng vào report.md không cần sửa."""
    print("| " + " | ".join(headers) + " |")
    print("|" + "|".join("---" for _ in headers) + "|")
    for r in rows:
        print("| " + " | ".join(str(c) for c in r) + " |")
    print()


def section(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def plan_text(df):
    """Lấy plan dưới dạng CHUỖI (df.explain() chỉ in ra stdout, không trả về gì
    -> không grep được PartitionFilters trong code)."""
    return df._jdf.queryExecution().toString()


def grep_plan(plan, *keys):
    return [ln.strip() for ln in plan.split("\n")
            if any(k in ln for k in keys)]


def new_spark(app):
    s = SparkSession.builder.appName(app).getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    return s


# =============================================================================
# 4. CHECKPOINT 3 — chạy trực tiếp file này
# =============================================================================
CP3 = f"{BENCH}/cp3/orders_parquet"
DAY = "2018-07-02"  # ngày có thật trong dữ liệu (195 đơn) — đã kiểm bằng cut+grep trên CSV


def _build_cp3(spark):
    """Dựng bảng Parquet phân vùng theo order_date (nếu chưa có).
    repartition("order_date") TRƯỚC khi ghi: gom mọi dòng cùng ngày về 1 task
    -> mỗi partition đúng 1 file. Không làm thế -> small files (chính là bài A35)."""
    if count_part_files(CP3) > 0:
        print(f"[cp3] dùng lại {CP3} ({count_part_files(CP3)} file)")
        return
    print(f"[cp3] dựng {CP3} ...")
    df = orders_enriched(spark).where(F.col("order_date").isNotNull())
    (df.repartition("order_date").write.mode("overwrite")
       .partitionBy("order_date").parquet(CP3))


def main():
    spark = new_spark("mp1-cp3-benchmark")
    _build_cp3(spark)

    section("CHECKPOINT 3 — CSV vs PARQUET (4 ô: 2 query x 2 format)")
    print(f"Query A: sum(price) đơn 'delivered' theo THÁNG  (quét toàn bảng)")
    print(f"Query B: sum(price) đúng ngày {DAY}          (điểm rơi partition pruning)\n")

    # --- các query, mỗi cái kết thúc bằng ACTION thật (collect) ---------------
    def qA_csv():
        return (orders_enriched(spark)
                .where(F.col("order_status") == "delivered")
                .groupBy(F.date_format("order_date", "yyyy-MM").alias("m"))
                .agg(F.sum("price").alias("rev")).collect())

    def qA_pq():
        return (spark.read.parquet(CP3)
                .where(F.col("order_status") == "delivered")
                .groupBy(F.date_format("order_date", "yyyy-MM").alias("m"))
                .agg(F.sum("price").alias("rev")).collect())

    def qB_csv():
        return (orders_enriched(spark)
                .where(F.col("order_date") == F.lit(DAY))
                .agg(F.sum("price")).collect())

    def qB_pq():
        return (spark.read.parquet(CP3)
                .where(F.col("order_date") == F.lit(DAY))
                .agg(F.sum("price")).collect())

    rows = []
    for name, fn in [("A — CSV", qA_csv), ("A — Parquet", qA_pq),
                     ("B — CSV", qB_csv), ("B — Parquet (partition filter)", qB_pq)]:
        best, times = timeit(fn, runs=3, label=name)
        with Probe(spark, name) as p:   # lần đo cuối để moi metric
            fn()
        rows.append([name, f"{times[0]:.2f}s", f"{best:.2f}s",
                     p.files if p.files is not None else "?",
                     human(p.files_size), human(p.input_bytes)])

    section("BẢNG 1 — 4 ô bắt buộc của Checkpoint 3")
    md(["Query", "lần 1 (lạnh)", "min lần 2-3 (ấm)", "files read",
        "size of files read", "input bytes (THẬT)"], rows)
    print("Đọc bảng: cột `files read` và `input bytes` là bằng chứng KHÔNG CÃI ĐƯỢC.")
    print("Cột giây có thể chênh ít vì Olist chỉ ~121MB — đừng bán giây, hãy bán bytes.\n")

    # --- kích thước trên đĩa --------------------------------------------------
    csv_bytes = os.path.getsize(ORDERS_CSV) + os.path.getsize(ITEMS_CSV)
    pq_bytes = du_bytes(CP3)
    section("BẢNG 2 — Kích thước trên đĩa")
    md(["Dạng lưu", "Đường dẫn", "Kích thước", "Số file"],
       [["CSV gốc (orders + order_items)", "data/olist/*.csv", human(csv_bytes), 2],
        ["Parquet partitionBy(order_date)", "data/bench/cp3/orders_parquet",
         human(pq_bytes), count_part_files(CP3)]])
    print(f"Tỉ lệ nén: CSV / Parquet = {csv_bytes / max(pq_bytes, 1):.2f}×")
    print(f"Số partition (thư mục order_date=): {count_dirs(CP3)}\n")

    # --- explain của Query B: khoanh PartitionFilters --------------------------
    section("BẢNG 3 — explain() Query B: chứng minh PARTITION PRUNING")
    pl = plan_text(spark.read.parquet(CP3).where(F.col("order_date") == F.lit(DAY))
                   .agg(F.sum("price")))
    for ln in grep_plan(pl, "PartitionFilters", "PushedFilters", "FileScan", "Location"):
        print("    " + ln[:300])
    print("\n=> `PartitionFilters: [... (order_date = 2018-07-02)]` = Spark đã loại thư mục")
    print("   NGAY LÚC LIỆT KÊ FILE, chưa mở byte nào. Đó là lý do files read tụt về ~1.")
    print("\nHẾT CHECKPOINT 3. Chi tiết từng cơ chế: xem A30 (column) / A35 (small files) / A36 (phá pruning).")
    spark.stop()


if __name__ == "__main__":
    main()
