# Lesson 36 — Orchestration với Airflow: SparkSubmitOperator, idempotency

> Module 5 · Lakehouse & Iceberg · Tuần 19 · Thời lượng: 5–6 giờ (lý thuyết 2h, lab 3–4h)

---

## 1. Learning Objective

Hôm nay bạn học:

- Tại sao **cron không đủ** cho data pipeline: dependency, retry, backfill, alert — 4 thứ cron không có.
- Bộ khái niệm Airflow: **DAG, task, operator, schedule, catchup, data_interval / logical_date**.
- Ba cách submit Spark từ Airflow: **SparkSubmitOperator vs BashOperator vs KubernetesPodOperator** — bảng so sánh.
- Viết DAG hoàn chỉnh: **bronze → QC → silver → gold → maintenance** cho pipeline Olist của lesson 34–35.
- Kỹ năng đắt giá nhất bài: **idempotency** — thiết kế Spark job re-run bao nhiêu lần cũng cho cùng kết quả (overwrite partition theo `ds`, MERGE theo key, cấm append mù).
- **Backfill**: `airflow dags backfill`, `catchup=True`, template `{{ ds }}` truyền vào Spark job làm parameter.
- Sensor, SLA, retry + alert callback.

Sau bài này bạn phải làm được:

- Vẽ DAG cho một pipeline bất kỳ và chỉ ra task nào được chạy song song.
- Trả lời "job này chạy lại 3 lần thì dữ liệu có bị nhân 3 không?" — và chứng minh bằng thiết kế.
- Backfill 30 ngày quá khứ mà không phá dữ liệu hiện có.

Kiến thức dùng trong thực tế: mọi pipeline production đều sống trong một orchestrator (Airflow chiếm thị phần lớn nhất; Dagster/Prefect cùng khái niệm). Và **idempotency là câu hỏi phỏng vấn Senior DE kinh điển** — vì nó phân biệt người từng trực on-call với người mới chỉ chạy notebook.

---

## 2. Why

### Cron: đủ cho 1 job, sụp đổ ở job thứ 5

Tuần 18 bạn có 5 job: bronze, qc_gate, silver, gold, maintenance. Thử vận hành bằng cron:

```cron
0 2 * * * spark-submit bronze.py     # 02:00
30 2 * * * spark-submit qc_gate.py   # 02:30 — CẦU NGUYỆN bronze xong trước 02:30
0 3 * * * spark-submit silver.py     # 03:00 — cầu nguyện tiếp...
0 4 * * * spark-submit gold.py
0 5 * * * spark-submit maintenance.py
```

Bốn cái chết được báo trước:

1. **Dependency bằng... khoảng cách thời gian**: hôm dữ liệu to gấp đôi, bronze chạy 40 phút → qc_gate check dữ liệu cũ, silver build thiếu, gold sai — mà **mọi job đều xanh**. Đây là kiểu lỗi tồi tệ nhất: sai không có tiếng động.
2. **Retry**: bronze chết vì network chớp — cron không thử lại. Sáng ra dashboard trống, bạn chạy tay 5 job theo đúng thứ tự, bằng trí nhớ.
3. **Backfill**: sếp muốn tính lại 30 ngày với logic mới → bạn viết vòng lặp bash, tự quản lý ngày nào xong ngày nào fail, tự chạy lại chỗ đứt... tức là bạn đang viết một orchestrator dởm.
4. **Alert & audit**: cron fail thì ghi mail vào `/var/mail` không ai đọc. Ai chạy gì lúc nào, lần nào fail, log đâu — không có.

Orchestrator (Airflow) cho đúng 4 thứ đó: **dependency thật** (silver chạy khi bronze *xong và thành công*), **retry có chính sách**, **backfill là lệnh 1 dòng**, **alert + UI + log tập trung**. Còn `enforce()` của lesson 35 raise exception → exit code ≠ 0 → Airflow đánh dấu task fail → chuỗi downstream dừng đúng như thiết kế fail-hard của bạn. Hai bài học vừa khớp vào nhau.

### Trade-off (Senior phải thuộc lòng)

| Được | Mất |
|---|---|
| Dependency, retry, backfill, alert, UI, log tập trung | Thêm một hệ thống phải vận hành (scheduler, metadata DB, worker) |
| Lịch sử chạy = audit trail | Learning curve: logical_date/catchup gây nhầm lẫn kinh niên |
| Idempotency + backfill = sửa quá khứ được | Airflow là **orchestrator, không phải compute** — nhét pandas nặng vào worker là sai kiến trúc |
| Chuẩn ngành, tuyển người dễ | Scheduler latency: không dành cho realtime (streaming job không sống TRONG Airflow) |

> Bài học Senior: Airflow chỉ trả lời "chạy CÁI GÌ, KHI NÀO, THEO THỨ TỰ nào, FAIL thì sao". Dữ liệu không chảy qua Airflow — nó chảy qua Spark/Iceberg. Airflow là nhạc trưởng cầm đũa, không phải nhạc công.

---

## 3. Theory

### 3.1. Bộ khái niệm Airflow

| Khái niệm | Nghĩa |
|---|---|
| **DAG** | Directed Acyclic Graph — bản nhạc: tập task + quan hệ phụ thuộc + lịch chạy. Định nghĩa bằng file Python. |
| **Task** | Một nút trong DAG — đơn vị được schedule, retry, log riêng. |
| **Operator** | Khuôn tạo task: `BashOperator` chạy lệnh shell, `SparkSubmitOperator` submit Spark, `PythonOperator` gọi hàm Python... |
| **DAG Run** | Một lần thực thi DAG cho một **data interval** cụ thể. |
| **Task Instance** | Task X trong DAG Run Y — thứ thực sự chạy, có state (queued/running/success/failed/up_for_retry). |
| **Sensor** | Task chờ điều kiện (file xuất hiện, bảng có partition, DAG khác xong) rồi mới cho downstream chạy. |
| **XCom** | Kênh truyền **metadata bé** giữa các task (đường dẫn, count). KHÔNG truyền DataFrame! |

### 3.2. `data_interval` và `logical_date` — cái bẫy số 1 của người mới

Airflow tư duy theo **khoảng dữ liệu (data interval)**, không phải "giờ bấm nút". DAG `@daily` xử lý dữ liệu của một ngày — và nó chỉ chạy **SAU KHI ngày đó kết thúc** (dữ liệu mới đầy đủ):

```
   Dữ liệu của ngày 2026-07-07 (data interval [07-07 00:00 → 07-08 00:00))
   ────────────────────────────────────────────────────────────────────────
   07-07 00:00                                    07-08 00:00
        │  events của ngày 07 rơi vào đây             │
        └──────────────────────────────────────────────┘
                                                       ▲
                                              DAG RUN chạy TẠI ĐÂY (sau interval)
                                              logical_date = 2026-07-07  ← "đại diện"
                                              {{ ds }}     = "2026-07-07"
```

Quy tắc bỏ túi: **`{{ ds }}` = ngày của DỮ LIỆU đang xử lý, không phải ngày đồng hồ treo tường**. Run lúc 02:00 sáng 08/07 xử lý dữ liệu ngày 07/07 → `ds = 2026-07-07`. Ai quên điều này sẽ viết job "xử lý hôm nay" và vĩnh viễn thiếu một ngày.

- **schedule**: `"@daily"`, `"0 2 * * *"` (cron string), hoặc timetable tùy biến.
- **start_date**: mốc bắt đầu tính các data interval.
- **catchup**: `True` → khi bật DAG, Airflow tự tạo run cho **mọi interval quá khứ** từ start_date đến nay (backfill tự động). `False` → chỉ chạy từ nay về sau. Quên đặt `False` khi start_date là 2 năm trước = 730 run ập vào cluster sáng thứ hai.

### 3.3. Ba cách submit Spark từ Airflow

| Tiêu chí | `SparkSubmitOperator` | `BashOperator` (gọi spark-submit/docker) | `KubernetesPodOperator` |
|---|---|---|---|
| Cách hoạt động | Worker Airflow chạy `spark-submit` qua provider `apache-airflow-providers-apache-spark` | Worker chạy lệnh shell bất kỳ do bạn viết | Airflow tạo pod K8s riêng, pod tự spark-submit |
| Yêu cầu trên worker | Cài Spark binaries + connection `spark_default` | Có gì gọi nấy (kể cả `docker exec`) | Chỉ cần quyền gọi K8s API |
| Tham số hóa | `application_args`, `conf`, `packages` — có cấu trúc, template được | Tự nối chuỗi lệnh — dễ sai quote/escape | Full container spec |
| Cách ly môi trường | Kém — mọi job chung env của worker | Kém | **Tốt nhất** — mỗi job 1 image, 1 version Spark riêng |
| Khi dùng | Chuẩn mực khi worker với tới spark-submit (YARN/standalone) | Glue nhanh, môi trường học/lab, gọi Makefile/docker | Production trên K8s (chuẩn hiện đại) |
| Nhược điểm chính | Version Spark bị đóng đinh theo worker | Chuỗi lệnh khó review, khó test | Phức tạp setup ban đầu |

Trong lab hôm nay: cluster Spark của ta sống trong Docker (`spark-mastery-spark-submit-1`), nên ta viết DAG theo **cả hai kiểu** — SparkSubmitOperator (dạng chuẩn mực để thuộc bài) và BashOperator gói `docker exec` (dạng chạy được ngay với hạ tầng hiện có). Ngoài đời trên K8s, bạn chỉ đổi khuôn operator — cấu trúc DAG giữ nguyên.

### 3.4. Idempotency — trái tim của bài

**Định nghĩa**: task chạy 1 lần hay 10 lần (retry, backfill, chạy tay) đều để hệ thống ở **cùng một trạng thái cuối**. `f(f(x)) = f(x)`.

Vì sao bắt buộc? Vì **re-run không phải tình huống hiếm — nó là chuyện mỗi tuần**: retry tự động sau lỗi mạng, backfill sau khi sửa logic, chạy tay sau sự cố. Job không idempotent + orchestrator hay retry = máy nhân bản dữ liệu.

```
  APPEND MÙ (❌)                          IDEMPOTENT (✅)
  run 1: append 1000 dòng ngày 07-07     run 1: overwrite partition 07-07 → 1000 dòng
  retry: append 1000 dòng NỮA            retry: overwrite partition 07-07 → 1000 dòng
  → 2000 dòng, doanh thu x2, không       → 1000 dòng, chạy 100 lần vẫn 1000
    ai biết cho đến khi BI hỏi
```

Ba chiến thuật, xếp theo tình huống:

1. **Overwrite partition theo `ds`** — cho bảng fact/append theo ngày. Job nhận `--ds 2026-07-07`, chỉ đọc dữ liệu ngày đó, và **thay thế đúng partition đó**:
   ```python
   df_one_day.writeTo("lake.gold.fact_order_items").overwritePartitions()
   # Iceberg dynamic overwrite: thay thế CHỈ các partition xuất hiện trong df
   # Hoặc tường minh, an toàn hơn nữa:
   # spark.sql(f"DELETE FROM lake.gold.fact WHERE order_date = DATE'{ds}'") rồi append
   ```
2. **MERGE theo key** — cho dimension/SCD (lesson 34 đã làm): chạy lại, `hash_diff` không đổi → không ghi gì. MERGE là idempotent by design nếu điều kiện ON đúng.
3. **Overwrite toàn bảng** (`createOrReplace`) — cho bảng nhỏ derive được từ nguồn (dim_date, aggregate bé). Thô nhưng idempotent tuyệt đối.

**Cấm kỵ**: `append` mà không có cơ chế chống trùng; xóa/ghi dựa trên `current_date()` (chạy bù ngày hôm qua sẽ ghi nhầm chỗ — mọi thứ phải theo `ds` được truyền vào); random/UUID trong key (mỗi lần chạy sinh key khác — nhớ lesson 34: hash key deterministic).

Idempotency + Iceberg còn có lưới an toàn thứ hai: mỗi lần ghi là 1 snapshot — lỡ tay ghi bậy vẫn `rollback_to_snapshot` được (lesson 31). Nhưng lưới không thay được thiết kế đúng.

### 3.5. Backfill

Backfill = bảo Airflow chạy DAG cho một dải interval quá khứ:

```bash
airflow dags backfill olist_medallion \
    --start-date 2026-06-01 --end-date 2026-06-30
```

Airflow tạo 30 DAG run, mỗi run có `ds` riêng (2026-06-01 → 2026-06-30), chạy theo dependency, fail cái nào retry cái đó, `--rerun-failed-tasks` chạy tiếp chỗ đứt. **Toàn bộ giá trị của backfill đứng trên hai chân**: (a) job nhận `ds` làm tham số và chỉ đụng dữ liệu của `ds` đó; (b) job idempotent. Thiếu một chân là backfill = thảm họa nhân bản.

`catchup=True` là backfill tự động cho DAG mới bật; production thường để `False` và backfill chủ động bằng lệnh — kiểm soát được tải.

### 3.6. Sensor, SLA, retry & alert callback

- **Sensor**: "đợi đến khi X". Ví dụ `ExternalTaskSensor` đợi DAG ingest của team khác xong. Luôn dùng `mode="reschedule"` (nhả worker slot khi chờ) và đặt `timeout` — sensor treo vô hạn là rò rỉ tài nguyên kinh điển.
- **SLA**: khai `sla=timedelta(hours=2)` — task xong muộn hơn 2h sau data interval → Airflow ghi SLA miss + gọi callback. Cho câu hỏi "dashboard PHẢI sẵn sàng trước 8h sáng".
- **Retry**: `retries=2, retry_delay=timedelta(minutes=5), retry_exponential_backoff=True` — chống lỗi thoáng qua (network, executor mất). Lưu ý: QC fail-hard của lesson 35 mà retry 5 lần thì vẫn fail 5 lần — retry chỉ cứu lỗi *transient*, không cứu lỗi *deterministic*; đừng đặt retries cao cho task QC.
- **Alert callback**: `on_failure_callback=fn` nhận `context` (dag_id, task_id, ds, exception, log_url) → bắn Slack/webhook. Đặt ở `default_args` để mọi task thừa hưởng.

---

## 4. Internal

Airflow vận hành DAG của bạn thế nào — biết để debug:

```
① Bạn đặt file dag_olist.py vào thư mục dags/
② DAG PROCESSOR parse file định kỳ (import Python thật!)   ← code cấp module
   chạy MỖI LẦN PARSE (~30s/lần) → cấm gọi API/DB/Spark ở cấp module
③ SCHEDULER: mỗi khi một data interval kết thúc → tạo DAG RUN
   → xét task nào đủ điều kiện (upstream success) → đẩy vào hàng đợi
④ EXECUTOR/WORKER nhặt task instance, chạy operator:
   SparkSubmitOperator → subprocess spark-submit → Spark DRIVER khởi động
   (compute nặng nằm ở Spark cluster, KHÔNG ở worker Airflow)
⑤ Task xong → exit code/exception quyết định state:
   0 → success → downstream được mở khóa
   ≠0 → up_for_retry (còn lượt) → failed → on_failure_callback + downstream skip
⑥ METADATA DB ghi lại mọi state — đây là nguồn của UI, backfill biết
   ngày nào đã xong, và là lý do "xóa DAG run = Airflow quên ngày đó đã chạy"
```

Hai hệ quả thực chiến:

1. **Code cấp module trong file DAG chạy hàng nghìn lần/ngày** (mỗi lần parse). `spark = SparkSession...` ở cấp module là tự sát. File DAG chỉ nên *khai báo*, mọi việc nặng nằm trong file Spark job riêng (`labs/lab36/jobs/`).
2. **SparkSubmitOperator giữ một process trên worker suốt đời Spark job** (client-mode-style chờ log). 50 Spark job dài đồng thời = 50 slot worker bị chiếm — một lý do lớn để production chuyển sang KubernetesPodOperator / submit qua API. (Chi tiết client vs cluster mode: lesson 37.)

---

## 5. API

### Khung DAG (Airflow 2.x)

```python
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,     # định nghĩa ở dưới
}
with DAG(
    dag_id="olist_medallion",
    start_date=datetime(2026, 7, 1),
    schedule="0 2 * * *",          # 02:00 sáng, xử lý dữ liệu ngày hôm trước
    catchup=False,                 # backfill chủ động bằng CLI, không tự động
    max_active_runs=1,             # các ngày không giẫm chân nhau
    default_args=default_args,
    tags=["olist", "lakehouse"],
) as dag:
    ...
```
- **Pitfall**: `max_active_runs=1` quan trọng khi backfill — 30 run cùng MERGE một bảng dim là mời gọi conflict (Iceberg optimistic concurrency sẽ retry/fail commit).

### `SparkSubmitOperator`

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

silver = SparkSubmitOperator(
    task_id="silver_transform",
    application="/workspace/labs/lab36/jobs/silver_transform.py",
    conn_id="spark_default",                     # Admin → Connections: spark://spark-master:7077
    packages="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
    conf={"spark.sql.shuffle.partitions": "8"},
    application_args=["--ds", "{{ ds }}"],       # ← template: ngày của DỮ LIỆU
)
```
- `{{ ds }}` được render lúc chạy thành `2026-07-07`. Anh em của nó: `{{ ds_nodash }}`, `{{ data_interval_start }}`, `{{ data_interval_end }}`.

### `BashOperator` gói docker exec (chạy được với hạ tầng lab)

```python
from airflow.operators.bash import BashOperator

silver = BashOperator(
    task_id="silver_transform",
    bash_command=(
        "docker exec spark-mastery-spark-submit-1 "
        "/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
        "/workspace/labs/lab36/jobs/silver_transform.py --ds {{ ds }}"
    ),
)
```

### Khai dependency

```python
bronze >> qc_bronze >> silver >> qc_silver >> [gold_dims, gold_fact]
gold_dims >> gold_fact >> qc_gold >> maintenance     # dims xong mới build fact (FK!)
```

### Spark job nhận `--ds` + ghi idempotent (phía Spark)

```python
import argparse
from pyspark.sql import functions as F
from session import iceberg_session

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)          # KHÔNG bao giờ dùng current_date()
ds = parser.parse_args().ds

spark = iceberg_session(f"silver-transform-{ds}")
one_day = (spark.table("lake.bronze.orders")
    .filter(F.to_date("order_purchase_timestamp") == F.lit(ds).cast("date")))
# ... clean như lab 34 ...
cleaned.writeTo("lake.silver.orders_daily").overwritePartitions()   # idempotent
```

### Sensor & callback

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_ingest = ExternalTaskSensor(
    task_id="wait_upstream_ingest", external_dag_id="cdc_ingest",
    mode="reschedule", timeout=3600, poke_interval=120)

def notify_failure(context):
    import requests
    ti = context["task_instance"]
    requests.post(WEBHOOK_URL, json={"text":
        f"[FAIL] {ti.dag_id}.{ti.task_id} ds={context['ds']}\n"
        f"exception: {context.get('exception')}\nlog: {ti.log_url}"})
```

---

## 6. Demo nhỏ

Chứng minh idempotency bằng chính Spark, chưa cần Airflow — mô phỏng "Airflow retry 3 lần":

```
Input : 5 event của ngày 2026-07-07
Chạy  : ghi kiểu APPEND 3 lần  vs  ghi kiểu overwritePartitions 3 lần
Output: bảng A 15 dòng (hỏng), bảng B 5 dòng (đúng)
```

```python
import sys
from pyspark.sql import functions as F
sys.path.insert(0, "/workspace/labs/lab34"); from session import iceberg_session

spark = iceberg_session("demo36-idempotency")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.demo")
for t in ["evt_append", "evt_idem"]:
    spark.sql(f"DROP TABLE IF EXISTS lake.demo.{t}")
    spark.sql(f"""CREATE TABLE lake.demo.{t} (event_id STRING, amount DOUBLE, d DATE)
                  USING iceberg PARTITIONED BY (d)""")

ds = "2026-07-07"
df = spark.createDataFrame(
    [(f"e{i}", 10.0 * i) for i in range(1, 6)], ["event_id", "amount"]
).withColumn("d", F.lit(ds).cast("date"))

for attempt in range(3):                       # "Airflow retry" 3 lần
    df.writeTo("lake.demo.evt_append").append()             # ❌ append mù
    df.writeTo("lake.demo.evt_idem").overwritePartitions()  # ✅ thay đúng partition ds

for t in ["evt_append", "evt_idem"]:
    n, s = spark.table(f"lake.demo.{t}").agg(F.count("*"), F.sum("amount")).first()
    print(f"{t:12s} → {n} dòng, tổng {s}")
# evt_append   → 15 dòng, tổng 450.0   ← doanh thu x3, thảm họa im lặng
# evt_idem     → 5 dòng,  tổng 150.0   ← chạy 100 lần vẫn vậy
spark.stop()
```

Bonus: `SELECT * FROM lake.demo.evt_idem.snapshots` — 3 snapshot (mỗi lần ghi 1 cái) nhưng trạng thái cuối như nhau. Idempotent về **trạng thái**, còn lịch sử ghi vẫn đầy đủ để audit.

---

## 7. Production Example

DAG production hoàn chỉnh cho pipeline Olist — nối trọn lesson 34 + 35 + 32 (maintenance):

```python
# dags/olist_medallion.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK = ("docker exec spark-mastery-spark-submit-1 "
         "/opt/spark/bin/spark-submit --master spark://spark-master:7077 ")
JOBS = "/workspace/labs/lab36/jobs"

def notify_failure(context):
    ti = context["task_instance"]
    print(f"[ALERT] {ti.dag_id}.{ti.task_id} ds={context['ds']} failed "
          f"-> {ti.log_url}")           # lab: print; prod: requests.post(WEBHOOK...)

default_args = {"owner": "data-eng", "retries": 2,
                "retry_delay": timedelta(minutes=5),
                "on_failure_callback": notify_failure}

with DAG(dag_id="olist_medallion",
         start_date=datetime(2026, 7, 1),
         schedule="0 2 * * *", catchup=False, max_active_runs=1,
         default_args=default_args, tags=["olist"]) as dag:

    bronze = BashOperator(task_id="bronze_ingest",
        bash_command=SPARK + f"{JOBS}/bronze_ingest.py --ds {{{{ ds }}}}")

    qc_bronze = BashOperator(task_id="qc_bronze",        # QC deterministic: retry vô ích
        retries=0,
        bash_command=SPARK + f"{JOBS}/qc_gate.py --layer bronze --ds {{{{ ds }}}}")

    silver = BashOperator(task_id="silver_transform",
        bash_command=SPARK + f"{JOBS}/silver_transform.py --ds {{{{ ds }}}}")

    qc_silver = BashOperator(task_id="qc_silver", retries=0,
        bash_command=SPARK + f"{JOBS}/qc_gate.py --layer silver --ds {{{{ ds }}}}")

    gold_dims = BashOperator(task_id="gold_dims",        # SCD1/SCD2 MERGE — idempotent
        bash_command=SPARK + f"{JOBS}/gold_dims.py --ds {{{{ ds }}}}")

    gold_fact = BashOperator(task_id="gold_fact",        # overwritePartitions theo ds
        bash_command=SPARK + f"{JOBS}/gold_fact.py --ds {{{{ ds }}}}",
        sla=timedelta(hours=4))                          # phải xong trước 06:00

    qc_gold = BashOperator(task_id="qc_gold", retries=0,
        bash_command=SPARK + f"{JOBS}/qc_gate.py --layer gold --ds {{{{ ds }}}}")

    maintenance = BashOperator(task_id="iceberg_maintenance",   # lesson 32
        bash_command=SPARK + f"{JOBS}/maintenance.py --ds {{{{ ds }}}}")

    bronze >> qc_bronze >> silver >> qc_silver >> gold_dims >> gold_fact \
           >> qc_gold >> maintenance
```

Đọc DAG này như một Senior đọc code review:

- **QC là task RIÊNG, retries=0**: fail là fail thật (deterministic), retry chỉ tốn 10 phút vô ích; và tách riêng thì UI hiện đúng "chết ở cổng QC nào".
- **gold_dims TRƯỚC gold_fact**: fact cần resolve `seller_key` từ dim SCD2 mới nhất (lesson 34) — dependency nghiệp vụ mã hóa thành dependency DAG.
- **maintenance cuối chuỗi, mỗi ngày**: compaction + expire_snapshots (lesson 32) — chính là "Assignment Hard" tuần 17, giờ thành task tự động.
- Mọi job nhận `--ds` → backfill 30 ngày là một lệnh: `airflow dags backfill olist_medallion --start-date 2026-06-01 --end-date 2026-06-30`.

---

## 8. Hands-on Lab

**Mục tiêu**: chạy Airflow thật (standalone trong Docker), deploy DAG trên, re-run và backfill để TỰ TAY chứng kiến idempotency.

### Bước 0 — dựng Airflow standalone

Không cần cả cụm — một container đủ để học (nếu repo `../kafka-flink` của bạn đã có Airflow, dùng luôn và bỏ qua bước này):

```bash
docker run -d --name airflow-lab \
  -p 8080:8080 \
  -v "$PWD/labs/lab36/dags:/opt/airflow/dags" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --network spark-mastery_default \
  apache/airflow:2.9.2 standalone
# user/password: xem log — docker logs airflow-lab 2>&1 | grep -i password
# cần docker CLI trong container để BashOperator gọi docker exec:
docker exec -u root airflow-lab bash -c "apt-get update && apt-get install -y docker.io"
```

Mount `docker.sock` để BashOperator trong Airflow điều khiển được container Spark — mẹo lab, không phải pattern production (production dùng SparkSubmitOperator/K8s).

### Bước 1 — tách pipeline lab 34–35 thành job nhận `--ds`

Tạo `labs/lab36/jobs/`: copy `session.py`, rồi viết `bronze_ingest.py`, `silver_transform.py`, `gold_dims.py`, `gold_fact.py`, `qc_gate.py`, `maintenance.py` từ code lab 34–35 với 2 thay đổi bắt buộc:

1. Mỗi job có `argparse --ds` (qc_gate thêm `--layer`); mọi filter thời gian dùng `ds`, không dùng `current_date()`. (Olist là dataset tĩnh 2016–2018 — để mỗi ngày backfill có việc làm, filter bronze theo `to_date(order_purchase_timestamp) == ds` và backfill dải ngày 2017.)
2. Mọi lệnh ghi theo đúng chiến thuật idempotent: silver/fact → `overwritePartitions()` (bảng partition theo ngày), dims → MERGE (đã sẵn từ lab 34), `maintenance.py` → gọi `rewrite_data_files` + `expire_snapshots` (lesson 32).

### Bước 2 — viết DAG

Chép DAG ở Section 7 vào `labs/lab36/dags/olist_medallion.py` (sửa `schedule` thành `"@daily"`, `start_date=datetime(2017, 10, 1)` để backfill dữ liệu Olist thật). Mở `http://localhost:8080` → thấy DAG → **Graph view**: đối chiếu hình DAG với chuỗi `>>` bạn viết.

### Bước 3 — trigger 1 run và quan sát

UI → Trigger DAG w/ config → chọn logical date `2017-10-05`. Theo dõi Graph view chuyển màu theo dependency; mở log task `silver_transform` thấy `--ds 2017-10-05` được render. Kiểm tra bằng Spark: partition `2017-10-05` của silver có dữ liệu.

### Bước 4 — nghiệm idempotency + fail hard

1. **Clear** task `gold_fact` của run vừa xong (UI → task → Clear) → Airflow chạy lại nó. Đếm `fact` trước/sau: **không đổi** → overwritePartitions thắng.
2. Tiêm rác vào bronze ngày `2017-10-06` (script step2 của lab 35), trigger run cho ngày đó → `qc_bronze` đỏ, mọi task sau bị **upstream_failed** — fail hard của lesson 35 hiện nguyên hình trên UI. Sửa rác, Clear `qc_bronze` → chuỗi tự chạy tiếp.

### Bước 5 — backfill 1 tuần

```bash
docker exec airflow-lab airflow dags backfill olist_medallion \
    --start-date 2017-10-07 --end-date 2017-10-13
```

Xem Grid view: 7 run xếp hàng (max_active_runs=1 → tuần tự). Xong, kiểm chứng: `SELECT d, count(*) FROM lake.silver.orders_daily GROUP BY d ORDER BY d` — mỗi ngày một partition, không ngày nào double. Chạy lại đúng lệnh backfill lần nữa → số liệu **y nguyên**. Đó là khoảnh khắc "à ha" của bài này. Ghi quan sát vào `labs/lab36/NOTES.md`.

---

## 9. Assignment

**Easy** — Vẽ (ASCII) DAG cho pipeline có thêm nguồn thứ 2 (payments): `bronze_payments` chạy song song `bronze_orders`, cả hai xong mới `qc_bronze`. Viết đoạn khai báo dependency tương ứng. Đặt lịch 2AM hằng ngày: `schedule` viết thế nào, và với run lúc 2AM ngày 08/07 thì `{{ ds }}` là gì? Giải thích.

**Medium** — Idempotency audit: rà cả 6 job trong `labs/lab36/jobs/` của bạn, lập bảng: job → lệnh ghi → chiến thuật idempotent (overwrite partition/MERGE/replace) → chứng minh (chạy 2 lần, so count + sum). Tìm ít nhất 1 chỗ bạn đã vô tình phụ thuộc `current_date()` hoặc append — sửa nó.

**Hard** — Backfill 30 ngày (2017-10-01 → 2017-10-30) với một cú lừa: trước khi backfill, hãy sửa logic silver (ví dụ đổi định nghĩa chuẩn hóa status). Chạy backfill, chứng minh: (a) 30 partition đều theo logic MỚI; (b) tổng doanh thu gold khớp lại từ silver; (c) dim_sellers không mọc version rác (SCD2 vẫn đúng khi chạy lại quá khứ — vì sao? gợi ý: hash_diff). Đo tổng thời gian; nếu bỏ `max_active_runs=1` thì nhanh hơn bao nhiêu và rủi ro gì xuất hiện?

**Production Challenge** — Viết `on_failure_callback` production-grade: gửi webhook kèm dag_id/task_id/ds/exception/log_url + **runbook link** theo từng task (dict TASK_RUNBOOK), và một `sla_miss_callback` riêng cho `gold_fact`. Thêm `ExternalTaskSensor` giả lập chờ DAG `cdc_ingest` (tự viết DAG dummy này) — chỉnh `execution_delta` cho đúng khi 2 DAG khác giờ chạy. Trả lời: vì sao sensor phải `mode="reschedule"` và chuyện gì xảy ra với worker slot nếu để mặc định `poke`?

> Nộp bài bằng cách paste code + câu trả lời vào chat. Mentor review theo chuẩn Senior.

---

## 10. Performance

| Quyết định | Ảnh hưởng | Tại sao |
|---|---|---|
| `max_active_runs=1` khi backfill | Chậm nhưng an toàn | Nhiều run cùng MERGE 1 bảng dim → Iceberg commit conflict, retry storm. Fact overwrite partition khác ngày thì song song được — tách DAG dims/fact nếu cần tốc độ |
| Job đọc theo `--ds` + partition pruning | Backfill 30 ngày = 30 lần đọc NHỎ | Filter `d = ds` trên bảng partition theo ngày → mỗi run chỉ chạm 1 partition (lesson 33) |
| Sensor `mode="poke"` | Chiếm 1 worker slot suốt lúc chờ | `reschedule` nhả slot, chỉ thức dậy mỗi poke_interval |
| Task quá nhỏ, DAG 200 task li ti | Scheduler overhead > compute | Mỗi task Spark có chi phí khởi động driver ~10–30s; gom việc cùng tầng vào 1 job, để Spark song song bên trong |
| Task quá to (cả pipeline 1 task) | Retry = chạy lại tất cả | Điểm cắt task = điểm bạn muốn retry/resume riêng — đó là lý do QC đứng riêng |
| Compute trong PythonOperator (pandas to) | Nghẹt worker Airflow | Airflow điều phối, Spark tính toán — đừng đảo vai |

Tự vấn khi thiết kế DAG: *"nếu task này fail lúc 3 giờ sáng, người trực chỉ được phép làm MỘT thao tác — Clear task — thì hệ thống có tự lành không?"* Nếu chưa, thiết kế lại tính idempotent trước khi nghĩ đến chuyện gì khác.

---

## 11. Spark UI

Bài này bạn có **hai UI** — và kỹ năng là đối chiếu chúng:

- **Airflow UI (8080)**: Grid view = lịch sử run theo ngày (mỗi ô = 1 task instance); Graph view = dependency; Gantt = task nào chiếm thời gian trong run (bronze 2 phút mà silver 20 phút → biết đầu tư tối ưu đâu); Log của task = stdout của spark-submit.
2. **Spark Master UI (8081) / Application UI (4040)**: trong lúc `gold_fact` đang chạy, mở Spark UI thấy application `gold-fact-2017-10-05` — đúng job mà Airflow task đang chờ. Chuỗi truy vết sự cố production luôn là: **Airflow task đỏ → đọc log task lấy applicationId/exception → mở Spark UI/History Server của application đó → tab Stages tìm thủ phạm**.
- Khi backfill chạy: Spark Master UI thấy các application nối đuôi nhau từng ngày (max_active_runs=1). Mỗi application ngắn và nhỏ — bằng chứng partition pruning theo `ds` hoạt động; nếu mỗi run vẫn scan full bảng, bạn quên filter theo `ds`.

---

## 12. Common Mistakes

1. **Hiểu sai `{{ ds }}`** — dùng như "hôm nay". Run 02:00 ngày 08/07 có `ds=2026-07-07` (ngày của dữ liệu). Job viết theo `current_date()` sẽ lệch một ngày và không backfill được.
2. **Append mù trong job được Airflow retry** — cặp đôi hủy diệt: retry tự động × append = nhân bản dữ liệu. Mọi job dưới orchestrator PHẢI idempotent.
3. **`catchup=True` + start_date xa xưa + quên nghĩ** — bật DAG phát, 700 run ập vào cluster. Đặt `catchup=False`, backfill chủ động bằng CLI.
4. **Code nặng ở cấp module file DAG** — tạo SparkSession/gọi API khi parse → scheduler è cổ mỗi 30 giây. File DAG chỉ khai báo.
5. **Truyền DataFrame qua XCom** — XCom là metadata DB, nhét vài trăm MB vào là nghẹt. Task trao nhau **tên bảng/đường dẫn/ds**, dữ liệu nằm ở lakehouse.
6. **Retry cao cho task QC deterministic** — dữ liệu bẩn không tự sạch sau 5 phút; retry chỉ dành cho lỗi transient. `retries=0` cho QC, có `on_failure_callback` là đủ.
7. **Sensor `poke` không timeout** — chiếm worker slot vô hạn khi upstream chết. `mode="reschedule"` + `timeout` luôn luôn.
8. **Không đặt `max_active_runs` khi các run ghi chung bảng** — backfill 30 run đồng thời MERGE một dim → commit conflict, kết quả phụ thuộc may rủi.
9. **Coi Airflow là chỗ chạy transform** — pandas 10GB trong PythonOperator → worker OOM. Nhạc trưởng không chơi kèn.

---

## 13. Interview

**Junior:**

1. *Tại sao cần Airflow khi đã có cron?* — Cron chỉ biết "chạy X lúc Y". Pipeline dữ liệu cần: dependency thật (B chạy khi A *thành công*, không phải "sau A 30 phút"), retry có chính sách, backfill quá khứ, alert khi fail, UI + log + lịch sử tập trung. Cron xử lý dependency bằng khoảng cách thời gian — vỡ ngay khi dữ liệu phình.
2. *DAG, task, operator khác nhau thế nào?* — DAG là đồ thị định nghĩa tập task + phụ thuộc + lịch. Task là một nút được schedule/retry/log riêng. Operator là khuôn tạo task (BashOperator, SparkSubmitOperator...). DAG Run là một lần thực thi DAG cho một data interval; task instance là task trong run đó.
3. *`{{ ds }}` là gì và tại sao quan trọng?* — Template render thành ngày của data interval (ngày của DỮ LIỆU đang xử lý), không phải ngày đồng hồ. Truyền vào job làm tham số để job xử lý đúng lát dữ liệu của mình — điều kiện tiên quyết để backfill hoạt động.
4. *catchup là gì?* — Khi bật DAG, `catchup=True` tự tạo run cho mọi data interval từ start_date đến nay (backfill tự động); `False` chỉ chạy từ nay. Production thường `False` và backfill chủ động bằng `airflow dags backfill` để kiểm soát tải.

**Mid:**

5. *Idempotency là gì, tại sao bắt buộc với job dưới orchestrator?* — Chạy 1 lần hay N lần cho cùng trạng thái cuối. Bắt buộc vì re-run là chuyện thường nhật: retry tự động, backfill, chạy tay sau sự cố. Job append mù + retry = nhân bản dữ liệu im lặng. Ba chiến thuật: overwrite partition theo ds, MERGE theo key, replace toàn bảng nhỏ.
6. *SparkSubmitOperator vs BashOperator vs KubernetesPodOperator?* — SparkSubmitOperator: chuẩn mực, worker cần Spark binaries + connection, tham số có cấu trúc. BashOperator: tự nối lệnh, linh hoạt nhưng khó review, hợp lab/glue. KubernetesPodOperator: mỗi job một pod/image riêng, cách ly tốt nhất, chuẩn production trên K8s. Khác biệt cốt lõi: môi trường thực thi và mức cách ly.
7. *Backfill 30 ngày cần job thỏa điều kiện gì?* — (a) Tham số hóa theo ds — chỉ đọc/ghi dữ liệu của interval đó, không dùng current_date(); (b) idempotent — chạy lại không nhân bản; (c) cân nhắc max_active_runs nếu các run ghi chung bảng (conflict). Có đủ thì backfill = một lệnh CLI.
8. *Task fail lúc 3h sáng — Airflow làm gì, bạn thiết kế gì để hỗ trợ?* — Airflow: retry theo policy (retries, retry_delay, backoff), hết lượt → failed → on_failure_callback bắn alert, downstream thành upstream_failed. Thiết kế: alert kèm ds/log_url/runbook; task idempotent để người trực chỉ cần Clear; retries=0 cho lỗi deterministic (QC) để không lãng phí.

**Senior:**

9. *Backfill 30 ngày trong khi DAG hằng ngày vẫn chạy — những rủi ro nào và bạn xử lý sao?* — Rủi ro: (a) tranh tài nguyên cluster với run hằng ngày → chạy backfill vào pool/queue riêng, giới hạn max_active_runs; (b) hai run cùng MERGE một bảng → Iceberg optimistic concurrency conflict → tuần tự hóa task ghi dim, hoặc backfill fact (overwrite partition độc lập theo ngày — song song an toàn) tách khỏi dims; (c) logic mới vs dữ liệu cũ — schema evolution phải tương thích; (d) SCD2: replay quá khứ có thể chèn version sai thứ tự thời gian → dim nên rebuild từ đầu dải backfill hoặc dùng effective_date từ dữ liệu (không phải ngày chạy). Điểm ăn tiền: phân biệt bảng backfill song song được (fact partition theo ngày) và bảng phải tuần tự (dim SCD).
10. *Streaming job (lesson 27–29) có nên chạy trong Airflow không?* — Không chạy TRONG Airflow — streaming job sống 24/7, không có "khoảnh khắc hoàn thành" để task success; Airflow tư duy theo batch interval. Pattern đúng: Airflow deploy/giám sát/restart streaming job (task submit rồi thoát, sensor/health-check định kỳ), hoặc streaming do hệ khác quản (K8s deployment, Spark Operator) còn Airflow orchestrate phần batch hạ nguồn (compaction, gold aggregate) — chính là mô hình Project 3 tuần này.

---

## 14. Summary

### Mindmap

```
                    LESSON 36 — AIRFLOW & IDEMPOTENCY
                                  │
   ┌──────────────┬───────────────┼────────────────┬───────────────────┐
   ▼              ▼               ▼                ▼                   ▼
 VÌ SAO        KHÁI NIỆM       SUBMIT SPARK    IDEMPOTENCY         VẬN HÀNH
   │              │               │                │                   │
 cron thiếu:   DAG/task/       SparkSubmit-    f(f(x))=f(x)        backfill =
 dependency    operator        Operator        1. overwrite         1 lệnh CLI
 retry         data_interval   (chuẩn mực)        partition (ds)   (nhờ ds +
 backfill      {{ ds }} = ngày Bash (glue,     2. MERGE theo key   idempotent)
 alert         CỦA DỮ LIỆU     lab)            3. replace bảng bé  sensor:
 → Airflow =   catchup         K8sPod (prod,   cấm: append mù,     reschedule
 nhạc trưởng,  max_active_runs cách ly nhất)   current_date(),     +timeout
 không phải                                    random key          retry≠QC fail
 nhạc công                                                         callback+SLA
```

### Checklist trước khi gõ "Continue"

- [ ] Kể được 4 thứ cron không có mà pipeline cần.
- [ ] Giải thích `{{ ds }}` của run 02:00 sáng 08/07 là ngày nào và vì sao.
- [ ] So sánh 3 operator submit Spark và chọn đúng cho từng môi trường.
- [ ] Viết được DAG bronze→QC→silver→gold→maintenance với dependency đúng nghiệp vụ.
- [ ] Thuộc 3 chiến thuật idempotent + 3 điều cấm kỵ; demo append vs overwritePartitions đã tự chạy.
- [ ] Backfill 7 ngày thành công, chạy lại lần 2 số liệu y nguyên.
- [ ] Biết vì sao QC task nên retries=0 còn bronze ingest thì retries=2.
- [ ] Trả lời được 10 câu interview không xem đáp án.

---

## 15. Next Lesson

**Project 3 (FULL) — Clickstream Analytics.**

Bạn đã gom đủ đồ nghề của cả Module 4 + 5: Kafka source, Structured Streaming + stateful processing, Iceberg + time travel + maintenance, medallion + modeling, QC, và giờ là Airflow. Project 3 bắt bạn lắp TẤT CẢ thành một hệ thống sống: tự sinh clickstream 1000 events/giây → Kafka → Spark Streaming sessionization (state 30 phút — bài toán stateful khó nhất khóa cho đến nay) → bronze/silver/gold trên Iceberg → Airflow chạy compaction + metrics hằng đêm → Trino vẽ funnel. Đây là project "dựng cả nhà máy" đầu tiên — và là thứ bạn kể trong phỏng vấn thay vì kể tên công nghệ.

> Gõ **"Continue"** khi sẵn sàng.
