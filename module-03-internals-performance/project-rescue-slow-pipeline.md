# Project 3 (Tuần 11) — "Cứu pipeline chậm"

> Module 3 · Internals & Performance Tuning · Tuần 11 · Thời lượng: 5–7 ngày (mỗi ngày 1–2 giờ)

---

## 1. Bối cảnh (kịch bản thật đến từng chi tiết)

Bạn vừa vào công ty. Người tiền nhiệm nghỉ việc, để lại pipeline "daily sales report" chạy trên dữ liệu Olist. Dữ liệu tăng trưởng, pipeline giờ chạy lâu đến mức trễ SLA mỗi sáng. Ticket trên bàn bạn ghi đúng một dòng:

> *"Job `daily_report.py` chậm. Fix it. — Manager"*

Không docs, không tuning notes, không ai biết gì. Đây chính là ngày làm việc điển hình của một Data Engineer — và là bài thi tổng hợp toàn bộ Module 3 (lesson 15–22).

**Mục tiêu**: cải thiện tổng thời gian pipeline **≥ 5×**, có bằng chứng, theo đúng playbook lesson 22.

**Luật chơi:**

1. **Không được đổi KẾT QUẢ nghiệp vụ** — output cuối (các con số báo cáo) phải khớp bản gốc. Nhanh mà sai = 0 điểm.
2. **Đo trước, sửa sau** — chưa có báo cáo chẩn đoán thì chưa được sửa dòng code nào.
3. **Mỗi lần một thay đổi**, mỗi thay đổi một dòng tuning notes, mỗi optimization một ngày (theo checkpoint 3).
4. Cluster giữ nguyên: Docker `apache/spark:3.4.1`, worker **1 GB / 1 core** — cấm "tuning bằng cách mua máy". Đây là chủ ý: cluster nhỏ làm mọi bệnh phát tác rõ.

---

## 2. Setup

```bash
make up          # master UI :8080, app UI khi job chạy :4040
mkdir -p labs/project-rescue
```

Dataset Olist tại `data/olist/*.csv` (trong container: `/workspace/data/olist/`). Olist gốc hơi nhỏ để "đau thật", nên bước đầu tiên là phóng to + cấy skew — chạy script này MỘT lần:

### `labs/project-rescue/00_generate_data.py`

```python
"""Phóng to Olist ~40x va cấy 1 mega-seller (skew). Chạy 1 lần duy nhất."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("rescue-00-generate").getOrCreate()
SRC = "/workspace/data/olist"
DST = "/workspace/labs/project-rescue/data"

items = spark.read.csv(f"{SRC}/olist_order_items_dataset.csv", header=True, inferSchema=True)

# Phóng to 40x, order_id được đánh hậu tố để giữ tính duy nhất
big = (items.crossJoin(spark.range(40).select(F.col("id").alias("dup")))
            .withColumn("order_id", F.concat("order_id", F.lit("-"), "dup"))
            .drop("dup"))

# Cấy skew: seller lớn nhất được nhân thêm 250 lần nữa (hot key ~60-70% số dòng)
top = big.groupBy("seller_id").count().orderBy(F.desc("count")).first()["seller_id"]
hot = (big.filter(F.col("seller_id") == top)
          .crossJoin(spark.range(250).select(F.col("id").alias("d")))
          .withColumn("order_id", F.concat("order_id", F.lit("h"), "d")).drop("d"))
skewed = big.unionByName(hot)

skewed.repartition(8).write.mode("overwrite").parquet(f"{DST}/order_items_big")
print(f"order_items_big: {skewed.count():,} dòng | mega-seller: {top}")
spark.stop()
```

```bash
make run F=labs/project-rescue/00_generate_data.py
```

---

## 3. Bệnh nhân — `labs/project-rescue/daily_report.py`

Copy nguyên văn file dưới đây và chạy được ngay. Code này viết tệ **có chủ đích** — mọi dòng đều "chạy đúng", nhưng gần như mọi quyết định đều sai. Nhiệm vụ của bạn KHÔNG phải viết lại từ đầu, mà là chẩn đoán và chữa từng bệnh có bằng chứng.

```python
"""daily_report.py — pipeline cua nguoi tien nhiem. DO NOT TOUCH (truoc checkpoint 2)."""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType

t_start = time.time()

spark = (SparkSession.builder
         .appName("daily-sales-report")
         # nguoi tien nhiem: "200 la mac dinh, chac la chuan"
         .config("spark.sql.shuffle.partitions", "200")
         # "AQE la gi khong biet, tat cho giong bai blog nam 2019"
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")
         .getOrCreate())

DATA = "/workspace/labs/project-rescue/data"
OLIST = "/workspace/data/olist"

# ---- doc du lieu ("inferSchema cho tien, khoi phai go schema") ----
items    = spark.read.parquet(f"{DATA}/order_items_big")
orders   = spark.read.csv(f"{OLIST}/olist_orders_dataset.csv",
                          header=True, inferSchema=True)
sellers  = spark.read.csv(f"{OLIST}/olist_sellers_dataset.csv",
                          header=True, inferSchema=True)          # ~3k dong
products = spark.read.csv(f"{OLIST}/olist_products_dataset.csv",
                          header=True, inferSchema=True)          # ~33k dong

# ---- "cache het cho nhanh" ----
items.cache()                      # cache bang LON NHAT... roi dung dung 1 lan
orders.cache().count()             # count chi de "warm cache"

# ---- UDF ("python quen tay hon may cai F.* kho hieu") ----
@udf(DoubleType())
def price_with_freight(price, freight):
    if price is None:
        return 0.0
    return float(price) + (float(freight) if freight is not None else 0.0)

@udf(StringType())
def month_of(ts):
    if ts is None:
        return "unknown"
    return str(ts)[0:7]            # "2017-10" tu "2017-10-02 10:56:33"

# ---- lam giau du lieu ----
enriched = (items
    .withColumn("gross", price_with_freight("price", "freight_value"))
    .join(orders.select("order_id", "order_status", "order_purchase_timestamp"),
          "order_id")                                   # orders da phong to? khong — join lech
    .filter(F.col("order_status") == "delivered")       # filter SAU join
    .withColumn("month", month_of("order_purchase_timestamp"))
    .join(sellers, "seller_id")                          # bang 3k dong, sort-merge join!
    .join(products.select("product_id", "product_category_name"), "product_id"))

# ---- "kiem tra du lieu" giua chung ----
n = enriched.count()                                    # action thua
all_months = [r["month"] for r in
              enriched.select("month").distinct().collect()]   # collect ve driver
print(f"rows={n}, months={len(all_months)}")

# ---- bao cao 1: doanh thu theo seller (groupBy tren key SKEW) ----
by_seller = (enriched.groupBy("seller_id", "seller_state")
             .agg(F.sum("gross").alias("revenue"),
                  F.countDistinct("order_id").alias("orders")))
by_seller.write.mode("overwrite").parquet(
    "/workspace/labs/project-rescue/out/by_seller")

# ---- bao cao 2: doanh thu theo thang x bang x category ----
by_month = (enriched.groupBy("month", "seller_state", "product_category_name")
            .agg(F.sum("gross").alias("revenue")))
# "partition theo 3 cot cho de tra cuu" → HANG NGHIN thu muc/file nho
by_month.write.mode("overwrite") \
        .partitionBy("month", "seller_state") \
        .parquet("/workspace/labs/project-rescue/out/by_month")

# ---- bao cao 3: top category — doc lai chinh enriched (khong cache nhanh nay) ----
top_cat = (enriched.groupBy("product_category_name")
           .agg(F.sum("gross").alias("revenue"))
           .orderBy(F.desc("revenue")).limit(20))
for row in top_cat.collect():                           # collect + in tung dong
    print(row["product_category_name"], row["revenue"])

print(f"TOTAL WALL TIME: {time.time() - t_start:.0f}s")
spark.stop()
```

```bash
make run F=labs/project-rescue/daily_report.py
```

> Chạy trên cluster (1G/1core) để "đau" giống production. Nếu quá lâu để lặp thí nghiệm, được phép chạy `make run-local F=...` khi ĐO SO SÁNH TƯƠNG ĐỐI — nhưng số liệu nộp ở checkpoint 4 phải đo trên cluster, cùng điều kiện với baseline.

**Danh sách bệnh được cấy (để bạn đối chiếu SAU KHI tự tìm — đừng đọc trước khi làm checkpoint 1... nghiêm túc đấy):** UDF thay vì built-in (×2) · `inferSchema` trên CSV (×3) · bảng nhỏ không broadcast (×2 join) · filter sau join · groupBy/join trên key skew tự cấy · ghi `partitionBy` 2 tầng ra hàng nghìn small files với 200 shuffle partitions · `collect()`/`count()` thừa (×3) · cache sai chỗ (cache bảng gốc dùng 1 lần, KHÔNG cache `enriched` dùng 3 lần) · AQE bị tắt · shuffle partitions mặc định cho mọi cỡ dữ liệu.

---

## 4. Checkpoint — 4 chặng theo ROADMAP

### Checkpoint 1 (Ngày 1) — Profile: tìm top 3 bottleneck

Chạy baseline 2–3 lần, KHÔNG sửa gì. Dùng đúng lộ trình UI của lesson 22 §11 (Jobs → Stages → Summary Metrics → SQL → Executors). Giao nộp `01_profile.md`:

- Bảng baseline: tổng wall time, số job, top 5 stage theo duration (kèm stage đó là dòng code nào).
- **Top 3 bottleneck** xếp hạng theo thời gian, mỗi cái kèm ≥ 2 bằng chứng UI (screenshot + con số: duration max/median, shuffle read/write, spill, số file, plan node...).
- Đếm file output: `find labs/project-rescue/out -name '*.parquet' | wc -l` (tool `file_health.py` của lab 21 dùng được ở đây).

### Checkpoint 2 (Ngày 2) — Báo cáo chẩn đoán

`02_diagnosis.md` — bảng "vấn đề × giải pháp", MỖI bệnh một dòng:

| # | Bệnh | Bằng chứng (UI/plan/số) | Lesson liên quan | Giải pháp đề xuất | Dự đoán tác động |
|---|------|--------------------------|------------------|-------------------|------------------|

Yêu cầu tìm được **≥ 7 bệnh**. Cột "dự đoán tác động" (cao/trung/thấp + 1 câu lý do) quyết định THỨ TỰ sửa ở checkpoint 3 — sửa món ăn nhiều nhất trước. Đây là kỹ năng ăn điểm phỏng vấn: chẩn đoán có cấu trúc, không phải liệt kê lỗi.

### Checkpoint 3 (Ngày 3–6) — Tối ưu từng vấn đề, mỗi ngày một optimization

Tạo `daily_report_v1.py`, `v2.py`, ... Mỗi version = **một** thay đổi so với version trước. Sau mỗi version:

1. Chạy trên cluster, ghi wall time.
2. Kiểm tra kết quả khớp: tổng `revenue` của 3 báo cáo phải bằng baseline (viết `99_verify.py` so sánh — sai số float chấp nhận < 0.01%).
3. Điền 1 dòng tuning notes (khung lesson 22 §5.4): thay đổi / giả thuyết / trước / sau / giữ hay rollback.

Có thay đổi KHÔNG cải thiện (hoặc âm điểm)? Ghi lại và rollback — dòng đó có giá trị chấm điểm ngang dòng thành công.

Khung `99_verify.py` để khởi động (hoàn thiện thêm cho báo cáo 2 và 3):

```python
"""So ket qua nghiep vu giua 2 ban output — nhanh ma sai = 0 diem."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("rescue-verify").getOrCreate()
OUT = "/workspace/labs/project-rescue"

def total(path, col="revenue"):
    return spark.read.parquet(path).agg(F.sum(col)).first()[0]

base  = total(f"{OUT}/out_baseline/by_seller")   # copy output baseline sang out_baseline
fixed = total(f"{OUT}/out/by_seller")
diff  = abs(base - fixed) / base
print(f"by_seller: baseline={base:,.2f} fixed={fixed:,.2f} diff={diff:.6%}")
assert diff < 1e-4, "KET QUA LECH — optimization lam sai nghiep vu!"
spark.stop()
```

> Mẹo vòng lặp thí nghiệm: trước lần chạy baseline cuối cùng, copy `out/` thành `out_baseline/` để verify mãi về sau; và giữa các lần đo, chạy `make ps` xác nhận cluster đủ container (worker chết giữa chừng làm số đo vô nghĩa).

### Checkpoint 4 (Ngày 7) — Chứng minh cải thiện ≥ 5×

`04_results.md` chốt hạ:

- **Metrics table**: baseline vs final — wall time, số job, tổng shuffle read/write, spill, số file output, duration max/median của stage nặng nhất. Đo cùng cluster, cùng dữ liệu, ≥ 2 lần lấy median.
- **Before/after explain**: `explain("formatted")` của nhánh join chính ở cả 2 bản — chỉ ra thay đổi (SortMergeJoin → BroadcastHashJoin, biến mất BatchEvalPython, xuất hiện AQEShuffleRead...).
- **UI screenshots** before/after: trang Stages (Summary Metrics stage nặng nhất) + SQL plan.
- Không đạt 5×? Phân tích trung thực vì sao + ước tính trần cải thiện còn lại (Amdahl!) — phân tích tốt vẫn được điểm; con số đẹp không bằng chứng thì không.

---

## 5. Deliverable (nộp đủ mới tính hoàn thành)

```
labs/project-rescue/
├── 00_generate_data.py
├── daily_report.py            # bản gốc, không sửa
├── daily_report_v1.py ... vN.py
├── 99_verify.py               # chứng minh kết quả khớp baseline
├── 01_profile.md              # checkpoint 1
├── 02_diagnosis.md            # checkpoint 2 — bảng vấn đề × giải pháp
├── 03_tuning_notes.md         # checkpoint 3 — mỗi thay đổi 1 dòng, cả rollback
├── 04_results.md              # checkpoint 4 — metrics table + explain + phân tích
└── screenshots/               # UI before/after
```

---

## 6. Rubric chấm theo chuẩn Senior

| Tiêu chí | Trọng số | Junior (đạt) | Senior (xuất sắc) |
|---|---|---|---|
| Chẩn đoán | 25% | Tìm được 4–5 bệnh, có bằng chứng cơ bản | ≥ 7 bệnh, mỗi bệnh ≥ 2 bằng chứng định lượng, xếp hạng theo tác động dự đoán và dự đoán gần khớp thực tế |
| Quy trình | 25% | Có sửa có đo | Đúng playbook: baseline trước, 1 thay đổi/version, có rollback được ghi lại, thứ tự sửa theo Pareto |
| Kết quả | 25% | ≥ 3× + kết quả nghiệp vụ khớp | ≥ 5×, verify tự động, kèm phân tích trần cải thiện còn lại |
| Bằng chứng & trình bày | 15% | Có bảng metrics + screenshots | Before/after explain được chú giải từng khác biệt; báo cáo người ngoài đọc hiểu trong 10 phút |
| Hiểu sâu | 10% | Trả lời được "sửa gì" | Trả lời được "vì sao nó chậm ở tầng cơ chế" (shuffle/skew/memory/file) cho TỪNG fix |

**Câu hỏi vấn đáp sau khi nộp** (chuẩn bị sẵn): fix nào ăn nhiều nhất và vì sao bạn dự đoán được/không được? Nếu dữ liệu tăng 10× nữa, bệnh nào tái phát đầu tiên? Nếu chỉ được giữ MỘT fix, chọn gì? Cache `enriched` giúp bao nhiêu — và khi nào nó sẽ phản chủ trên cluster 1 GB này?

---

## 7. Gợi ý khi bí (mở từng nấc, đừng mở hết)

<details><summary>Nấc 1 — bí hướng profile</summary>

Đếm số job trên UI và map từng job về từng action trong code (`count`, `collect`, 2 lần `write`, `collect` cuối — và job lén của `inferSchema`). Job nào nặng nhất? Trong nó, stage nào? Nhớ: `enriched` được tính LẠI cho mỗi action vì không cache — nhìn DAG các job có giống nhau không?
</details>

<details><summary>Nấc 2 — bí thứ tự sửa</summary>

Thứ tự tham khảo theo tác động trên cluster này: (1) cache/persist `enriched` (nó chạy lại ~4 lần!), hoặc bỏ hẳn action thừa; (2) broadcast 2 bảng nhỏ + bật lại AQE; (3) gỡ 2 UDF bằng `F.col("price") + F.coalesce(F.col("freight_value"), F.lit(0))` và `F.date_format`/`F.substring`; (4) filter `delivered` TRƯỚC join; (5) skew: sau khi broadcast thì join hết skew — nhưng `groupBy(seller_id)` thì sao? kiểm tra bằng số; (6) writer: repartition theo cột partition + bỏ bớt tầng partitionBy + schema tường minh thay inferSchema.
</details>

<details><summary>Nấc 3 — không đạt 5×</summary>

Kiểm tra: (a) baseline đo có sạch không (cluster nguội/nóng lẫn lộn?); (b) còn action thừa nào không — mỗi action thừa là một lần chạy lại cả DAG; (c) số file output còn bao nhiêu — nghìn file nhỏ ăn cả phút ghi; (d) `count()` "warm cache" của orders — bỏ chưa? (e) wall time còn lại nằm ở đâu: cộng tổng duration các stage trên UI rồi so với wall time — phần chênh là thời gian driver (collect, plan, listing), sửa ở tầng code chứ không phải config.
</details>

---

## 8. Sau project — nhìn lại Module 3

Điền nốt checklist tự đánh giá vào cuối `04_results.md`:

- [ ] Tôi chẩn đoán bằng SỐ LIỆU, không bằng cảm giác — mọi kết luận đều chỉ được vào screenshot/metric.
- [ ] Tôi phân biệt được 4 họ bệnh: shuffle/skew (lesson 15, 19), memory/spill (17), plan/join (11, 20), file layout (16, 21).
- [ ] Tôi có bảng tuning notes mà người khác đọc là tái hiện được toàn bộ hành trình.
- [ ] Tôi biết ĐIỂM DỪNG: đạt mục tiêu là dừng, ghi lại trần cải thiện còn lại thay vì đào mãi.
- [ ] Tôi kể lại được project này trong 3 phút như một câu chuyện phỏng vấn: bối cảnh → chẩn đoán → hành động → con số.

---

## 9. Next Lesson

**Module 4 · Lesson 23 — Streaming 101: micro-batch vs continuous, unbounded table.**

Module 3 khép lại: bạn đã cầm trong tay bộ kỹ năng làm chủ hiệu năng batch — shuffle, memory, skew, AQE, file layout, và quan trọng nhất là quy trình. Module 4 đổi hẳn hệ quy chiếu: dữ liệu **không bao giờ kết thúc**. Kafka topic của pipeline Olist-CDC không có dòng cuối cùng — vậy `groupBy().count()` nghĩa là gì khi bảng dài vô tận? Spark trả lời bằng một ý tưởng thanh lịch: coi stream là **unbounded table** và chạy query lặp lại trên phần mới đến (micro-batch). Lesson 23 dựng nền: micro-batch vs continuous, event time vs processing time, và chạy stream đầu tiên của bạn — nơi mọi kiến thức tuning module 3 vẫn nguyên giá trị, chỉ thêm chiều thời gian.

> Gõ **"Continue"** khi sẵn sàng.
