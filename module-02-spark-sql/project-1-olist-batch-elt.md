# Project 1 (FULL) — Olist Batch ELT chuẩn production

> Module 2 · Spark SQL & DataFrame Mastery · Tuần 7 · Thời lượng: 12–16 giờ (trải trên 4–6 ngày, cố tình — vì có deliverable "run log 3 ngày")

---

## 1. Mục tiêu

Đây không phải bài tập — đây là **mô phỏng một task được giao ở công ty**: "dựng pipeline batch ELT cho dữ liệu đơn hàng, chạy hàng ngày, có kiểm soát chất lượng, phục vụ BI". Bạn nộp như nộp cho tech lead, và được chấm bằng rubric như một buổi design review thật.

Sau project này bạn phải:

- Dựng trọn **medallion architecture** (bronze → silver → gold) trên bộ Olist, mỗi tầng có lý do tồn tại rõ ràng.
- Áp dụng **toàn bộ Module 1 + 2**: schema tường minh (L5), Parquet/Iceberg (L6), transformations (L7), aggregation (L8), join + broadcast (L9), window (L10), QC engine (L14), đọc plan để tự review (L13), không một `@udf` nào lọt lưới (L12).
- Viết pipeline **idempotent**: chạy lại cùng ngày ra cùng kết quả, không nhân đôi dữ liệu — tính chất số 1 của batch production.
- Đóng gói vận hành: **Airflow DAG** daily 2AM, design doc, test, run log.

Yêu cầu đầu vào: đã xong lesson 1–14. Dataset: `data/olist/*.csv` (trong container: `/workspace/data/olist/`). Nếu bạn có hạ tầng repo `../kafka-flink` (Iceberg REST catalog + MinIO + Trino), dùng nó cho checkpoint 4–5; nếu chưa, có **phương án B bằng Parquet** ghi chú ở từng checkpoint — làm được 100% project chỉ với cluster `spark-mastery`.

---

## 2. Kiến trúc

```
                     OLIST BATCH ELT — daily 2AM
┌──────────────┐
│ data/olist/  │  9 file CSV (orders, items, customers, sellers,
│ *.csv        │  products, payments, reviews, geolocation, translation)
└──────┬───────┘
       │ ① INGEST (as-is + audit columns)
       ▼
┌────────────────────────────────────────────────────────────────┐
│ BRONZE  — sự thật thô, immutable                               │
│ bronze.orders, bronze.order_items, ... (1 bảng / 1 file nguồn) │
│ + ingestion_date (partition), + _source_file, + _ingested_at   │
│ schema TƯỜNG MINH (không inferSchema), giữ nguyên giá trị bẩn  │
└──────┬─────────────────────────────────────────────────────────┘
       │ ② CLEAN: QC gate → dedup → chuẩn hóa kiểu → surrogate key
       ▼
┌────────────────────────────────────────────────────────────────┐
│ SILVER — sự thật sạch, 1 dòng = 1 thực thể                     │
│ silver.orders, silver.order_items, silver.customers, ...       │
│ + bảng qc_results (report mọi lần chạy)  + bảng _rejects       │
└──────┬─────────────────────────────────────────────────────────┘
       │ ③ MODEL: star schema + business metrics
       ▼
┌────────────────────────────────────────────────────────────────┐
│ GOLD — sự thật phục vụ nghiệp vụ                               │
│ fact_order_items (grain: 1 dòng = 1 order item)                │
│ dim_customers, dim_sellers, dim_products, dim_date             │
│ agg_revenue_seller_daily (metric: revenue/seller/day)          │
└──────┬─────────────────────────────────────────────────────────┘
       │ ④ MAINTENANCE (Iceberg: compaction + expire snapshots)
       │ ⑤ SERVE (Trino / BI)          ⑥ ORCHESTRATE (Airflow 2AM)
       ▼
   Analyst gõ SQL vào Trino — KHÔNG BAO GIỜ đụng bronze
```

Quy ước bài nộp: code đặt tại `labs/project1/` (`ingest_bronze.py`, `build_silver.py`, `build_gold.py`, `maintenance.py`, `qc_lib.py`, `dags/olist_elt_dag.py`), chạy bằng `make run F=labs/project1/ingest_bronze.py -- <args>` (hoặc `run-local`). Mọi job nhận tham số `--run-date YYYY-MM-DD` — cấm hardcode ngày (lý do ở checkpoint 6).

---

## 3. Checkpoint chi tiết + tiêu chí chấm

### Checkpoint 1 — Bronze ingest + ingestion_date

**Việc**: đọc 9 CSV với **schema tường minh khai tay** (StructType — đúng, gõ hết, một lần trong đời cũng đáng), thêm 3 cột audit: `ingestion_date` (= `--run-date`), `_source_file` (`input_file_name()`), `_ingested_at` (`current_timestamp()`). Ghi partition theo `ingestion_date`, mode ghi đè **đúng partition của run-date** (idempotent — chạy lại không nhân đôi).

Phương án A (Iceberg): `writeTo(...).overwritePartitions()`. Phương án B (Parquet): `partitionBy("ingestion_date")` + `spark.sql.sources.partitionOverwriteMode=dynamic` + `mode("overwrite")`.

Khung khởi động (điền tiếp cho đủ 9 bảng — cấu trúc, không phải đáp án):

```python
# labs/project1/ingest_bronze.py
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

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
TABLES = {"orders": ORDERS_SCHEMA}   # + 8 bảng còn lại, bạn tự khai

def main(run_date: str):
    spark = (SparkSession.builder.appName(f"bronze-ingest-{run_date}")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    for name, schema in TABLES.items():
        df = (spark.read.csv(f"/workspace/data/olist/olist_{name}_dataset.csv",
                             header=True, schema=schema)
                .withColumn("ingestion_date", F.lit(run_date).cast("date"))
                .withColumn("_source_file", F.input_file_name())
                .withColumn("_ingested_at", F.current_timestamp()))
        (df.write.mode("overwrite").partitionBy("ingestion_date")
           .parquet(f"/workspace/labs/project1/warehouse/bronze/{name}"))
        print(f"[bronze] {name}: {df.count():,} dòng cho {run_date}")
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--run-date", required=True)
    main(p.parse_args().run_date)
```

**Tiêu chí chấm**:
- [ ] Không có `inferSchema=True` ở bất cứ đâu (grep được là trừ điểm thẳng).
- [ ] Chạy 2 lần cùng `--run-date` → row count không đổi (chứng minh bằng log 2 lần chạy).
- [ ] Bronze KHÔNG sửa dữ liệu (không cast ép, không dropna) — bẩn để nguyên, bẩn là chứng cứ.
- [ ] `printSchema()` của bronze.orders khớp thiết kế trong design doc.
- [ ] Lưu ý ngầm: `header=True` + schema tường minh vẫn parse fail ra NULL với ô sai kiểu — đó là VIỆC CỦA SILVER phát hiện qua QC, bronze cứ ghi.

### Checkpoint 2 — Silver: dedup + QC + surrogate key

**Việc**, theo đúng thứ tự:
1. **QC gate đầu vào** (dùng lại `qc_engine` lesson 14, tách thành `qc_lib.py`): fail-hard cho PK null/trùng, status ngoài danh mục, row count < ngưỡng; warn cho null % bất thường; rule chéo cột `delivered_no_date`. Vi phạm fail-hard cấp dòng → **quarantine** vào `silver._rejects` kèm `reject_reasons`; vi phạm cấp bảng → raise, dừng pipeline.
2. **Dedup**: theo business key mỗi bảng (orders: `order_id`; items: `order_id + order_item_id`...) — dùng window `row_number()` theo key, order by `_ingested_at` desc, giữ bản mới nhất (L10). Ghi số dòng bị loại vào qc_results.
3. **Chuẩn hóa**: cast timestamp tường minh, trim/lowercase các cột phân loại, thống nhất xử lý NULL theo bảng quyết định trong design doc (drop/fill/keep — L14).
4. **Surrogate key**: `sha2(concat_ws('||', các_cột_business_key), 256)` → `order_sk`, `customer_sk`... Vì sao hash thay vì `monotonically_increasing_id`: hash **ổn định giữa các lần chạy** (chạy lại ra cùng key → idempotent, join lại được), id tự tăng thì mỗi lần chạy một kiểu — phá idempotency. Ghi câu trả lời này vào design doc bằng lời của bạn.

Hai viên gạch trung tâm của silver (dedup + key — phần còn lại bạn tự ráp):

```python
from pyspark.sql import Window

# Dedup: giữ bản ghi mới nhất theo business key (L10)
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
orders_dedup = (bronze_orders
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1).drop("_rn"))
dropped = bronze_orders.count() - orders_dedup.count()   # → ghi vào qc_results

# Surrogate key ổn định — coalesce để NULL trong key không làm concat_ws sập khác nhau
orders_silver = orders_dedup.withColumn(
    "order_sk", F.sha2(F.concat_ws("||", F.coalesce("order_id", F.lit("∅"))), 256))
```

**Tiêu chí chấm**:
- [ ] QC chạy 1-agg-N-rules (mở Spark UI đếm job — QC nhiều hơn 2-3 job/bảng là chưa đạt).
- [ ] `clean + rejects = bronze` (số học phải khớp, in trong log).
- [ ] qc_results được append mỗi lần chạy, có run_date — chạy 3 ngày ra 3 lát cắt.
- [ ] Không UDF. Toàn bộ bằng built-in (L12 — mọi thứ ở đây đều làm được bằng built-in).

### Checkpoint 3 — Gold: star schema + revenue per seller per day

**Việc**:
1. **Thiết kế star schema** (vẽ trong design doc): `fact_order_items` grain **order-item** (không phải order — giải trình vì sao trong doc: price/freight nằm ở item, seller gắn với item), FK là các surrogate key; `dim_customers/sellers/products` (products join thêm bảng translation cho category tiếng Anh — broadcast join, L9); `dim_date` sinh bằng `sequence()` + `explode` (L11!).
2. **Metric bắt buộc**: `agg_revenue_seller_daily` — mỗi dòng = 1 seller × 1 ngày (theo `order_purchase_timestamp`, chỉ đơn delivered): `revenue = sum(price)`, `freight = sum(freight_value)`, `orders = countDistinct(order_id)`, `items = count(*)`, và `revenue_7d_avg` bằng window (L10).
3. **Tự review plan** (L13): `explain(formatted)` của job build fact — dim phải BroadcastHashJoin, PushedFilters phải có mặt; dán 2 bằng chứng vào design doc.

Hình dạng metric bắt buộc (đủ để bạn kiểm tra mình đi đúng hướng):

```python
revenue_daily = (fact_order_items
    .join(F.broadcast(dim_date), "date_sk")                 # hoặc dùng thẳng cột date
    .filter(F.col("order_status") == "delivered")
    .groupBy("seller_sk", "order_date")
    .agg(F.round(F.sum("price"), 2).alias("revenue"),
         F.round(F.sum("freight_value"), 2).alias("freight"),
         F.countDistinct("order_id").alias("orders"),
         F.count("*").alias("items")))

w7 = (Window.partitionBy("seller_sk").orderBy(F.col("order_date").cast("timestamp").cast("long"))
      .rangeBetween(-6 * 86400, 0))                          # 7 ngày LỊCH, không phải 7 dòng (L10)
revenue_daily = revenue_daily.withColumn("revenue_7d_avg",
                                         F.round(F.avg("revenue").over(w7), 2))
```

**Tiêu chí chấm**:
- [ ] Đối soát chéo tầng: `sum(price)` của fact == của silver.order_items (sau trừ rejects) — sai 1 xu cũng phải giải trình được vì đâu.
- [ ] Query "top 10 seller theo revenue tháng 2017-11" chạy trên gold trả kết quả < vài giây và KHÔNG cần join lại từ silver.
- [ ] Grain được phát biểu thành văn ("1 dòng của bảng X là ...") cho từng bảng gold.

### Checkpoint 4 — Iceberg maintenance

*(Cần hạ tầng Iceberg từ `../kafka-flink`. Nếu chưa có: phương án B — viết job compact Parquet: đọc partition → `coalesce` về file ~128MB → ghi đè; và viết 10 dòng mô tả bạn SẼ làm gì với Iceberg thật — vẫn được chấm.)*

**Việc**: viết `maintenance.py` gọi Iceberg procedures cho các bảng gold + silver:

```python
spark.sql("CALL catalog.system.rewrite_data_files(table => 'db.fact_order_items', "
          "options => map('target-file-size-bytes','134217728'))")
spark.sql("CALL catalog.system.expire_snapshots(table => 'db.fact_order_items', "
          "older_than => TIMESTAMP '...', retain_last => 7)")
```

**Tiêu chí chấm**:
- [ ] Trước/sau compaction: số file giảm (query `db.table.files`), thời gian scan cải thiện — có số đo.
- [ ] Expire giữ lại ≥7 snapshot (còn đường time-travel/rollback).
- [ ] Giải thích trong doc: vì sao chạy pipeline daily sinh small files, và tần suất maintenance hợp lý.

### Checkpoint 5 — Trino / BI

*(Cần Trino từ `../kafka-flink`. Phương án B: dùng `spark-sql` shell làm "analyst" và ghi lại các câu SQL.)*

**Việc**: từ Trino, query gold trả lời 4 câu nghiệp vụ: top seller theo revenue; xu hướng doanh thu theo tuần; category doanh thu cao nhất; seller có revenue_7d_avg tăng nhanh nhất. Nếu có Superset trong hạ tầng → dựng 1 dashboard nhỏ (không bắt buộc).

**Tiêu chí chấm**:
- [ ] 4 câu SQL chỉ đụng gold (đụng silver/bronze = thiết kế gold hỏng — quay lại CP3).
- [ ] So thời gian 1 câu chạy trên gold vs chạy tương đương từ silver — con số nói thay bạn vì sao gold tồn tại.

### Checkpoint 6 — Airflow DAG daily 2AM

*(Có Airflow trong `../kafka-flink` thì deploy thật; chưa có thì code DAG vẫn phải viết đầy đủ + chạy giả lập bằng script bash gọi tuần tự — nêu rõ trong doc.)*

**Việc**: `dags/olist_elt_dag.py`:

```
bronze_ingest ──▶ silver_build ──▶ gold_build ──▶ maintenance
                     │ (QC fail-hard → task fail → DỪNG, gold hôm qua vẫn nguyên)
schedule="0 2 * * *", catchup=True, max_active_runs=1
mỗi task = SparkSubmitOperator (hoặc BashOperator gọi spark-submit)
truyền --run-date {{ ds }}  ← đây là lý do cấm hardcode ngày:
                              backfill 30 ngày = airflow tự truyền 30 ds khác nhau
retries=2, retry_delay=5min, sla / on_failure_callback (in ra log là đủ)
```

Khung DAG (Airflow 2.x):

```python
# labs/project1/dags/olist_elt_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SUBMIT = ("docker exec spark-mastery-spark-submit-1 /opt/spark/bin/spark-submit "
          "--master spark://spark-master:7077 /workspace/labs/project1/{job} "
          "--run-date {{{{ ds }}}}")

default_args = dict(owner="data-eng", retries=2, retry_delay=timedelta(minutes=5))

with DAG(dag_id="olist_batch_elt", schedule="0 2 * * *",
         start_date=datetime(2026, 7, 1), catchup=True,
         max_active_runs=1, default_args=default_args, tags=["olist", "elt"]) as dag:
    bronze = BashOperator(task_id="bronze_ingest",
                          bash_command=SUBMIT.format(job="ingest_bronze.py"))
    silver = BashOperator(task_id="silver_build",
                          bash_command=SUBMIT.format(job="build_silver.py"))
    gold   = BashOperator(task_id="gold_build",
                          bash_command=SUBMIT.format(job="build_gold.py"))
    maint  = BashOperator(task_id="maintenance",
                          bash_command=SUBMIT.format(job="maintenance.py"))
    bronze >> silver >> gold >> maint
```

(Dùng `SparkSubmitOperator` nếu Airflow của bạn cài provider — BashOperator là mẫu số chung chạy được với hạ tầng Docker hiện tại. Chi tiết operator học sâu ở lesson 36.)

**Tiêu chí chấm**:
- [ ] `--run-date` chảy từ `{{ ds }}` xuyên suốt 4 job — re-run một ngày cũ ra đúng kết quả ngày đó (idempotent, chứng minh bằng chạy lại ngày N-1).
- [ ] QC fail được mô phỏng (tiêm dữ liệu hỏng vào 1 bản CSV copy) → DAG dừng ở silver, gold không bị vấy bẩn — có log chứng minh.
- [ ] Giải trình: vì sao `max_active_runs=1` (hai run ghi đè cùng bảng = race), vì sao 2AM (sau ngày nghiệp vụ đóng, trước giờ analyst).

---

## 4. Deliverable

Nộp đủ 4 món — thiếu món nào rubric trừ thẳng mục đó:

1. **Design doc** (`labs/project1/DESIGN.md`, 2–4 trang):
   - Sơ đồ kiến trúc + star schema (ASCII được).
   - Bảng quyết định NULL per cột quan trọng (drop/fill/keep + lý do — L14).
   - Grain từng bảng gold, định nghĩa metric (revenue tính theo giá nào, đơn nào được tính).
   - SLA: pipeline xong trước mấy giờ, dữ liệu trễ tối đa bao lâu, QC fail thì ai làm gì.
   - 2 bằng chứng plan (broadcast join, pushed filters) + 1 đoạn "rủi ro khi dữ liệu ×100".
2. **Code**: 4 job + `qc_lib.py` + DAG. Chuẩn: schema tường minh, không UDF, không action thừa, config không hardcode, có docstring đầu file nói job làm gì/nhận tham số gì.
3. **Test cases** (`labs/project1/tests/`): tối thiểu 5 — dedup giữ bản mới nhất; surrogate key ổn định qua 2 lần chạy; QC bắt được PK trùng; quarantine + clean = tổng; revenue đối soát silver↔gold. (pytest với SparkSession local là đủ; chưa học CI — L41 sẽ nâng cấp.)
4. **Run log 3 ngày** (`labs/project1/RUNLOG.md`): chạy pipeline 3 "ngày" liên tiếp (`--run-date` 3 giá trị, mô phỏng bằng cách chia orders theo `order_purchase_timestamp` hoặc đơn giản là 3 lần ingest 3 ngày khác nhau). Mỗi ngày ghi: thời gian từng job, row count từng tầng, QC pass/warn/fail, sự cố + cách xử. Ngày 2 hoặc 3 PHẢI có một lần re-run để chứng minh idempotency.

---

## 5. Rubric Senior (100 điểm)

| Hạng mục | Điểm | Đạt tối đa khi |
|---|---|---|
| **Đúng đắn dữ liệu** | 25 | Đối soát tầng khớp tuyệt đối hoặc lệch có giải trình; dedup/NULL/key xử lý có chủ đích, không "cho chắc"; re-run không đổi kết quả. |
| **Thiết kế** | 20 | Grain phát biểu rõ và nhất quán; mỗi tầng có lý do; NULL decision table đầy đủ; SLA thực tế. |
| **Chất lượng code Spark** | 20 | Schema tường minh; 0 UDF; QC 1-scan; broadcast đúng chỗ có bằng chứng plan; không action rác; tham số hóa sạch. |
| **Vận hành** | 20 | DAG idempotent + backfill được; QC gate chặn đúng tầng; maintenance có số đo; run log trung thực (log "đẹp không tì vết" 3 ngày liền là dấu hiệu đáng ngờ — sự cố + cách xử mới là điểm). |
| **Truyền đạt** | 15 | Design doc đọc 10 phút hiểu toàn hệ thống; trade-off có lý do; trả lời được chất vấn của mentor khi defense. |

**Thang**: ≥85 — Senior-ready, đem đi phỏng vấn được; 70–84 — đạt, sửa theo review rồi mới sang Module 3; 50–69 — làm lại checkpoint yếu; <50 — quay lại lesson tương ứng, project không phải chỗ học bù.

**Defense (bắt buộc, qua chat)**: mentor sẽ hỏi xoáy 5 câu kiểu — "chạy lại ngày hôm kia thì gold hôm qua có hỏng không, vì sao?", "seller đổi thành phố thì dim của em ghi đè hay giữ lịch sử, trade-off?", "dữ liệu ×100 thì chỗ nào chết trước?", "vì sao dedup TRƯỚC surrogate key mà SAU QC gate?", "nếu 2AM job fail mà 8AM sếp cần số thì runbook của em là gì?". Không có đáp án duy nhất — có lập luận là có điểm.

---

## 6. Common Mistakes của project (mentor đã chấm đủ nhiều để liệt kê trước)

1. **Code trước, design sau** — ngày 4 phát hiện grain sai, đập lại gold. Design doc nháp NGÀY 1, code từ ngày 2.
2. **Bronze "sạch quá"** — cast, dropna, dedup ngay ở bronze. Bronze mà sạch thì khi silver có bug, bạn mất bản gốc để đối chiếu và mất luôn khả năng replay.
3. **Idempotency giả** — `mode("append")` cho bronze: chạy lại một ngày là dữ liệu nhân đôi, và bạn chỉ phát hiện khi đối soát CP3 lệch. Overwrite đúng partition, luôn.
4. **QC viết xong để đó** — có qc_lib nhưng silver không GỌI nó, hoặc gọi mà kết quả fail không chặn pipeline. QC không có răng = trang trí.
5. **Metric mơ hồ** — "revenue" không định nghĩa: có tính freight không? đơn canceled thì sao? tính theo ngày mua hay ngày giao? Mỗi câu chưa trả lời trong design doc là một câu mentor sẽ hỏi lúc defense.
6. **`countDistinct` ở nơi `count` là đủ** (và ngược lại) — orders phải distinct `order_id` vì fact grain là item; items thì count(*). Nhầm là số sai lệch ngay.
7. **Run log viết lại từ trí nhớ vào ngày nộp** — lộ liễu hơn bạn tưởng (không có sự cố nào trong 3 ngày? thời gian tròn trịa?). Ghi ngay sau mỗi lần chạy, xấu cũng ghi — vận hành trung thực là kỹ năng được chấm.
8. **Ôm Iceberg/Trino/Airflow khi hạ tầng chưa sẵn** — kẹt 2 ngày ở docker-compose thay vì làm xong phương án B. Ưu tiên pipeline chạy được end-to-end, hạ tầng xịn nâng cấp sau.

---

## 7. Gợi ý lộ trình 6 ngày (tham khảo, không bắt buộc)

```
Ngày 1: DESIGN.md nháp (kiến trúc, schema, NULL table, grain) — THIẾT KẾ TRƯỚC KHI CODE
Ngày 2: CP1 bronze + chạy run-date đầu tiên
Ngày 3: CP2 silver (qc_lib tách riêng — tái dùng từ lab14)
Ngày 4: CP3 gold + đối soát + tự review plan
Ngày 5: CP4-5 maintenance + Trino; CP6 DAG
Ngày 6: chạy đủ 3 run-date, re-run 1 ngày, tiêm lỗi test QC gate, chốt RUNLOG + tests, nộp
```

Quy tắc cầu cứu giữ nguyên: stuck >2h thì hỏi mentor kèm (đã thử gì / log gì / nghi ngờ gì) — hỏi đúng cách cũng là kỹ năng được chấm.

---

## 8. Next

**Module 3 — Lesson 15: Shuffle internals — shuffle write/read, spill.**

Trong project này bạn đã join, groupBy, dedup bằng window — tức là đã tạo ra hàng loạt shuffle và (có thể) chưa từng hỏi chúng đắt bao nhiêu. Với 100k đơn Olist, Spark tha cho bạn. Với 100 triệu đơn, shuffle là nơi pipeline sống hay chết: dữ liệu được ghi xuống đĩa ở mapper thế nào, bay qua network ra sao, spill là gì và vì sao nó biến job 10 phút thành 2 giờ. Module 3 mở nắp cỗ máy — từ "viết Spark chạy đúng" sang "viết Spark chạy nhanh", và mọi con số trong tab Stages mà bạn từng lướt qua sẽ bắt đầu biết nói.

> Gõ **"Continue"** khi sẵn sàng.
