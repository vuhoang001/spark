"""A9 — `cache()`: ĐO, đừng đoán. Đọc thẳng tab Storage bằng REST API.

Chạy — NÊN chạy CLUSTER (tab Storage mới có ý nghĩa: bộ nhớ nằm trên 2 executor):

    make run F=labs/mini-project-1/exercises/a09_cache_measured.py

Local cũng chạy được (block nằm trong BlockManager của driver):

    make run-local F=labs/mini-project-1/exercises/a09_cache_measured.py

⚠️ LƯU Ý CLUSTER: chỉ MỘT app Spark chạy một lúc. App khác đang chiếm 6 core thì app
này nhận 0 core và TREO IM LẶNG vĩnh viễn. Đứng im > 90s = đúng bệnh đó.

4 PHẦN:
  A. DataFrame nặng (join + derive), count() 3 lần, KHÔNG cache  -> mỗi lần tính lại từ đầu
  B. Thêm .cache() + mồi, count() 3 lần                          -> nhanh hơn bao nhiêu?
  C. Tab Storage: chiếm bao nhiêu MB? (đọc bằng REST, không chép tay từ UI)
  D. So các StorageLevel                                          -> ⚠️ ĐỀ BÀI SAI, xem dưới
  E. BẪY của Checkpoint 1: 2 action trên 1 plan chưa cache = ĐỌC FILE 2 LẦN.
     Chứng minh bằng BYTES đọc từ đĩa, không phải bằng cảm giác.

⚠️ ĐỀ BÀI SAI Ở PHẦN D — phải nói thẳng:
   Đề bảo "thử `persist(StorageLevel.MEMORY_ONLY_SER)`". Hằng số này KHÔNG TỒN TẠI
   trong PySpark (đã kiểm tra trên chính container: pyspark 3.4.1 chỉ có NONE, DISK_ONLY,
   DISK_ONLY_2/3, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2,
   MEMORY_AND_DISK_DESER, OFF_HEAP). `MEMORY_ONLY_SER` là tên bên SCALA.
   Lý do: trong PySpark, dữ liệu RDD LUÔN được serialize (Python object không nằm trần
   trong JVM heap được) -> `StorageLevel.MEMORY_ONLY` của PySpark in ra đúng chữ
   "Memory Serialized 1x Replicated" — nó CHÍNH LÀ MEMORY_ONLY_SER rồi.
   Script vẫn dò bằng getattr() để nếu bản Spark khác có hằng số đó thì tự động đo.
"""

import json
import time
import urllib.request

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_CSV = "/workspace/data/olist/olist_orders_dataset.csv"
ITEMS_CSV = "/workspace/data/olist/olist_order_items_dataset.csv"
RUNS = 3

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])

ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType()),
])


# ---------------------------------------------------------------------------
# Đọc Spark UI bằng REST thay vì chép tay -> số không thể bịa được
# ---------------------------------------------------------------------------
def _rest(sc, path):
    url = "{}/api/v1/applications/{}{}".format(sc.uiWebUrl, sc.applicationId, path)
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def storage_rdds(sc, wait_s=12):
    """Nội dung tab STORAGE. Đợi tới khi block cache hiện ra (listener bus bất đồng bộ)."""
    deadline = time.time() + wait_s
    while time.time() < deadline:
        try:
            rdds = _rest(sc, "/storage/rdd")
        except Exception:
            rdds = []
        if rdds:
            return rdds
        time.sleep(0.5)
    return []


def wait_storage_empty(sc, wait_s=8):
    """Chờ tab Storage rỗng hẳn sau unpersist.

    Không chờ thì lần đo StorageLevel kế tiếp có thể CỘNG NHẦM block cũ chưa kịp xoá
    -> ra số MB to gấp đôi và bạn tưởng level đó tốn RAM hơn. Bẫy đo lường thuần tuý.
    """
    deadline = time.time() + wait_s
    while time.time() < deadline:
        try:
            if not _rest(sc, "/storage/rdd"):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def input_bytes_of_group(sc, group):
    """Tổng số BYTE ĐỌC TỪ ĐĨA của các job thuộc jobGroup `group`.

    Đây là bằng chứng cứng nhất của bài này: nếu Spark đọc lại file thì con số này
    nhảy lên bằng kích thước file; nếu nó lấy từ cache thì con số này ~0.
    Cách lấy: /jobs -> lọc theo jobGroup -> lấy stageIds -> /stages -> cộng inputBytes.
    """
    time.sleep(1.2)   # chờ listener bus ghi xong, không thì đếm thiếu
    jobs = [j for j in _rest(sc, "/jobs") if j.get("jobGroup") == group]
    want = set()
    for j in jobs:
        want.update(j.get("stageIds", []))
    total = 0
    for s in _rest(sc, "/stages"):
        if s.get("stageId") in want and s.get("status") == "COMPLETE":
            total += s.get("inputBytes", 0)
    return total


def mb(x):
    return x / 1024.0 / 1024.0


def bench(fn, runs=RUNS):
    times, res = [], None
    for _ in range(runs):
        t0 = time.time()
        res = fn()
        times.append(time.time() - t0)
    return times[0], min(times[1:]), res, times


# ---------------------------------------------------------------------------
def build_heavy(spark):
    """DataFrame "nặng": đọc 2 CSV -> join (WIDE, có shuffle) -> vài cột dẫn xuất.

    Nặng ở chỗ nào? Ở cái `join`. Mỗi lần có action mà không cache, Spark phải:
      đọc lại 17MB + 15MB CSV -> parse text -> shuffle cả hai bảng -> merge.
    Đó là thứ ta muốn ĐỪNG PHẢI LÀM LẠI. Cache sinh ra để cắt đúng chỗ này.
    """
    orders = spark.read.schema(ORDERS_SCHEMA).option("header", True).csv(ORDERS_CSV)
    items = spark.read.schema(ITEMS_SCHEMA).option("header", True).csv(ITEMS_CSV)
    return (orders
            .filter(F.col("order_status") == "delivered")
            .join(items, "order_id")                                  # WIDE -> shuffle
            .withColumn("order_date", F.to_date("order_purchase_timestamp"))
            .withColumn("total", F.col("price") + F.col("freight_value"))
            .withColumn("wait_days", F.datediff("order_delivered_customer_date",
                                                "order_purchase_timestamp"))
            .select("order_id", "order_date", "seller_id", "product_id",
                    "price", "freight_value", "total", "wait_days"))


def main():
    spark = SparkSession.builder.appName("a09-cache-measured").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print("\nmaster={} | defaultParallelism={}".format(sc.master, sc.defaultParallelism))

    # =======================================================================
    # PHẦN A — KHÔNG cache: 3 lần count() trên cùng một DataFrame
    # =======================================================================
    df = build_heavy(spark)
    sc.setJobGroup("A-nocache", "A9 A: 3x count() KHONG cache")
    a_cold, a_warm, n, a_all = bench(lambda: df.count())
    print("\n" + "=" * 78)
    print("PHẦN A — KHÔNG cache · {:,} dòng".format(n))
    print("=" * 78)
    print("3 lần count(): {}".format(["{:.2f}s".format(t) for t in a_all]))
    print("Cả 3 lần đều xấp xỉ nhau -> lần 2 và lần 3 KHÔNG hề rẻ hơn lần 1.")
    print("Vì lazy = KHÔNG NHỚ GÌ CẢ. Mỗi action tính lại từ đầu: đọc 2 file, join, shuffle.")

    # =======================================================================
    # PHẦN B — CÓ cache
    # =======================================================================
    df.cache()                      # LƯỜI: mới chỉ ĐÁNH DẤU, chưa có gì vào RAM
    sc.setJobGroup("B-prime", "A9 B: action moi cache")
    t0 = time.time()
    df.count()                      # action MỒI: vừa tính vừa nạp vào RAM
    t_prime = time.time() - t0

    sc.setJobGroup("B-cached", "A9 B: 3x count() CO cache")
    b_cold, b_warm, _, b_all = bench(lambda: df.count())

    print("\n" + "=" * 78)
    print("PHẦN B — CÓ cache")
    print("=" * 78)
    print("action MỒI (vừa tính vừa nạp cache): {:.2f}s  <- lần này ĐẮT HƠN bình thường,".format(t_prime))
    print("   vì ngoài việc tính, Spark còn phải ghi kết quả vào bộ nhớ. Đây là VỐN BỎ RA.")
    print("3 lần count() sau đó: {}".format(["{:.2f}s".format(t) for t in b_all]))

    print("\n| lần chạy | KHÔNG cache | CÓ cache | nhanh hơn |")
    print("|---|---|---|---|")
    for i in range(RUNS):
        print("| lần {} | {:.2f}s | {:.2f}s | {:.1f}× |".format(
            i + 1, a_all[i], b_all[i], a_all[i] / b_all[i] if b_all[i] else float("nan")))
    print("| **ẤM (min lần 2-3)** | **{:.2f}s** | **{:.2f}s** | **{:.1f}×** |".format(
        a_warm, b_warm, a_warm / b_warm if b_warm else float("nan")))
    print("\nHOÀ VỐN SAU BAO NHIÊU LẦN DÙNG? Vốn bỏ ra = {:.2f}s (mồi) − {:.2f}s (một lần tính".format(
        t_prime, a_warm))
    print("bình thường) = {:.2f}s phụ trội. Mỗi lần dùng lại tiết kiệm {:.2f}s.".format(
        t_prime - a_warm, a_warm - b_warm))
    if a_warm - b_warm > 0:
        print("=> Cache HOÀ VỐN sau ~{:.1f} lần dùng lại. Dùng ÍT hơn thế thì cache là LỖ.".format(
            max(0.0, (t_prime - a_warm)) / (a_warm - b_warm) + 1))

    # =======================================================================
    # PHẦN C — TAB STORAGE: cache tốn bao nhiêu MB?
    # =======================================================================
    print("\n" + "=" * 78)
    print("PHẦN C — TAB STORAGE (đọc bằng REST /api/v1/applications/<id>/storage/rdd)")
    print("=" * 78)
    print("StorageLevel mà .cache() thực sự dùng: **{}**".format(df.storageLevel))
    rdds = storage_rdds(sc)
    print("\n| tên block cache | partition đã cache | RAM chiếm | Đĩa chiếm | StorageLevel |")
    print("|---|---|---|---|---|")
    total_mem = 0
    for r in rdds:
        total_mem += r.get("memoryUsed", 0)
        print("| {} | {}/{} | {:.1f} MB | {:.1f} MB | {} |".format(
            r.get("name", "?")[:40], r.get("numCachedPartitions"), r.get("numPartitions"),
            mb(r.get("memoryUsed", 0)), mb(r.get("diskUsed", 0)), r.get("storageLevel")))
    if not rdds:
        print("| (REST không trả về gì — ghi 'KHÔNG ĐO ĐƯỢC', đừng bịa số) | | | | |")

    print("""
VÌ SAO CACHE THƯỜNG *TO HƠN* FILE GỐC TRÊN ĐĨA? (đề hỏi đúng câu này)
  1. CSV trên đĩa là TEXT ĐÃ NÉN VỀ MẶT NGỮ NGHĨA: số 58.90 chỉ tốn 5 ký tự = 5 byte.
     Trong bộ nhớ nó là DoubleType = 8 byte. Timestamp trong CSV là 19 ký tự text,
     trong bộ nhớ là 8 byte long — chỗ này thì cache lại NHỎ hơn.
  2. Cache giữ CẢ CẤU TRÚC: con trỏ, độ dài chuỗi, bitmap null cho từng cột.
  3. Ở đây DataFrame đã cache là kết quả JOIN (bảng rộng hơn, {:,} dòng) chứ không phải
     file gốc — so trực tiếp với 17MB CSV là so nhầm đối tượng.
  => Nên nhìn con số MB đo được ở bảng trên và đối chiếu với ngân sách RAM THẬT:
     mỗi executor chỉ có ~1049 MB cho storage+execution (xem PROGRESS §3.0).
     Cache vượt ngân sách -> Spark âm thầm ĐUỔI block ra (evict) -> lần sau lại tính lại,
     và bạn ngồi thắc mắc "sao cache mà vẫn chậm".
""".format(n))

    # =======================================================================
    # PHẦN D — so các StorageLevel
    # =======================================================================
    print("=" * 78)
    print("PHẦN D — so StorageLevel (⚠️ MEMORY_ONLY_SER KHÔNG TỒN TẠI trong PySpark)")
    print("=" * 78)
    levels = [
        ("MEMORY_AND_DISK  (= cái .cache() dùng)", StorageLevel.MEMORY_AND_DISK),
        ("MEMORY_ONLY      (PySpark: đã là SER)", StorageLevel.MEMORY_ONLY),
        ("DISK_ONLY", StorageLevel.DISK_ONLY),
    ]
    ser = getattr(StorageLevel, "MEMORY_ONLY_SER", None)
    if ser is not None:
        levels.append(("MEMORY_ONLY_SER (đề bài yêu cầu)", ser))
    else:
        print("\n>> Đã dò: `StorageLevel.MEMORY_ONLY_SER` KHÔNG có trong pyspark {}.".format(
            sc.version))
        print(">> PySpark MEMORY_ONLY in ra: '{}' -> nó CHÍNH LÀ bản serialized.".format(
            StorageLevel.MEMORY_ONLY))
        print(">> Ghi vào report là ĐỀ SAI, đừng bịa ra một dòng số cho nó.\n")

    print("| StorageLevel | RAM chiếm | Đĩa chiếm | count() ẤM |")
    print("|---|---|---|---|")
    for label, lvl in levels:
        df.unpersist(blocking=True)
        wait_storage_empty(sc)      # phải sạch hẳn rồi mới đo level tiếp theo
        df.persist(lvl)
        sc.setJobGroup("D-prime-" + label[:8], "moi cache " + label)
        df.count()                                  # mồi
        _, warm, _, _ = bench(lambda: df.count())   # đo khi đã ấm
        rr = storage_rdds(sc)
        m = sum(x.get("memoryUsed", 0) for x in rr)
        d = sum(x.get("diskUsed", 0) for x in rr)
        print("| {} | {:.1f} MB | {:.1f} MB | {:.2f}s |".format(label, mb(m), mb(d), warm))

    df.unpersist(blocking=True)
    spark.catalog.clearCache()      # dọn sạch trước phần E, không thì phần E ăn nhầm cache
    time.sleep(1.0)

    # =======================================================================
    # PHẦN E — BẪY CỦA CHECKPOINT 1, đo bằng BYTES
    # =======================================================================
    print("\n" + "=" * 78)
    print("PHẦN E — BẪY `_corrupt_record`: 2 action trên 1 plan chưa cache = ĐỌC FILE 2 LẦN")
    print("=" * 78)
    print("""
Ở Checkpoint 1 bạn sẽ viết đúng hình dạng này:
    df   = spark.read....csv(dirty)
    bad  = df.filter(col('_corrupt_record').isNotNull())     # nhánh 1
    good = df.filter(col('_corrupt_record').isNull())        # nhánh 2
    bad.count(); good.count()                                # 2 ACTION
Hai action = hai job = ĐỌC FILE HAI LẦN (và với _corrupt_record, một số bản Spark còn
ném thẳng lỗi). Dưới đây tái hiện đúng cơ chế đó bằng cột `order_status` cho dễ thấy,
và ĐO BẰNG SỐ BYTE ĐỌC TỪ ĐĨA — thứ không cãi được.
""")
    src = spark.read.schema(ORDERS_SCHEMA).option("header", True).csv(ORDERS_CSV)
    br1 = src.filter(F.col("order_status") == "delivered")
    br2 = src.filter(F.col("order_status") != "delivered")

    sc.setJobGroup("E-nocache-1", "nhanh 1, chua cache")
    c1 = br1.count()
    sc.setJobGroup("E-nocache-2", "nhanh 2, chua cache")
    c2 = br2.count()
    e1 = input_bytes_of_group(sc, "E-nocache-1")
    e2 = input_bytes_of_group(sc, "E-nocache-2")

    src.cache()
    sc.setJobGroup("E-prime", "moi cache")
    src.count()
    sc.setJobGroup("E-cache-1", "nhanh 1, da cache")
    br1.count()
    sc.setJobGroup("E-cache-2", "nhanh 2, da cache")
    br2.count()
    f1 = input_bytes_of_group(sc, "E-cache-1")
    f2 = input_bytes_of_group(sc, "E-cache-2")

    print("| tình huống | action | bytes ĐỌC TỪ ĐĨA |")
    print("|---|---|---|")
    print("| KHÔNG cache | nhánh 1 (`delivered`, {:,} dòng) | {:.1f} MB |".format(c1, mb(e1)))
    print("| KHÔNG cache | nhánh 2 (`!= delivered`, {:,} dòng) | {:.1f} MB |".format(c2, mb(e2)))
    print("| **tổng** | | **{:.1f} MB** ← file chỉ có ~17 MB! |".format(mb(e1 + e2)))
    print("| CÓ cache | nhánh 1 | {:.1f} MB |".format(mb(f1)))
    print("| CÓ cache | nhánh 2 | {:.1f} MB |".format(mb(f2)))
    print("| **tổng** | | **{:.1f} MB** |".format(mb(f1 + f2)))
    print("""
Đọc bảng: không cache thì MỖI nhánh tự đi đọc lại nguyên file. Cache xong thì cả hai
nhánh ăn chung một lần đọc — nhánh sau đọc ~0 byte từ đĩa.
""")

    # =======================================================================
    print("=" * 78)
    print("KẾT LUẬN — 3 dòng cho `ingest.py` của tôi (đề yêu cầu đúng 3 dòng này)")
    print("=" * 78)
    print("""
1. ĐÁNG cache: DataFrame đọc từ file bẩn ngay TRƯỚC khi tách 2 nhánh sạch/hỏng
   (Checkpoint 1). Nó được dùng ĐÚNG 2 LẦN (bad + good) -> phần E chứng minh không cache
   là đọc file 2 lần. Đây là chỗ duy nhất trong ingest.py mà cache chắc chắn LÃI.

2. PHÍ cache: DataFrame orders sau khi đã làm sạch, chỉ để `write.parquet()` một lần.
   Dùng đúng 1 lần -> cache là LỖ RÒNG: trả tiền RAM + trả thời gian ghi vào bộ nhớ,
   đổi lấy 0 lần dùng lại. Cache ở đây chỉ làm chậm job và ăn mất RAM của shuffle.

3. LUẬT NGÓN TAY CÁI: cache khi và chỉ khi (số lần DÙNG LẠI ≥ 2) VÀ (dữ liệu vừa ngân
   sách RAM: ~1049 MB/executor × 2 = ~2.0 GB ở cụm này). Không thoả -> đừng cache.
   Và đừng quên `unpersist()` khi xong: cache không tự chết cho tới khi session tắt,
   nó cứ nằm đó chiếm RAM mà đáng lẽ shuffle được dùng.

MỘT CÁI BẪY NỮA (không có trong đề): `cache()` LÀ LƯỜI. `df.cache()` rồi bỏ đó, không
có action nào theo sau -> KHÔNG CÓ GÌ trong RAM cả, tab Storage rỗng, và bạn vẫn tưởng
mình đã cache. Luôn phải có một action MỒI.
""")

    src.unpersist(blocking=True)
    spark.stop()


if __name__ == "__main__":
    main()
