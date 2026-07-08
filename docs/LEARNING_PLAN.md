# LEARNING_PLAN — Spark Mastery (cheat-sheet & checklist)

Mục tiêu: file này là hướng dẫn ngắn gọn, từng bước để bạn thực hiện labs, assignments và projects trong khoá `spark-mastery`.

---

## Quick start (local)

1. System prerequisites

- Java 17+ (Spark needs JDK 17 or newer)
- Docker & docker-compose
- Python 3.9+ (virtualenv)

2. Setup môi trường cơ bản

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk docker.io docker-compose python3-venv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pyspark==3.2.2 psycopg2-binary sqlalchemy confluent-kafka chispa
```

3. Bật infra (nếu có repo infra bên cạnh)

```bash
cd ../realtime-data-streaming    # hoặc repo infra của bạn
docker compose up -d
docker compose ps
```

Nếu không có infra, bạn có thể làm labs batch (CSV/Parquet) local mà không cần Kafka/Debezium.

---

## Cấu trúc học (nhiệm vụ tuần) — checklist rút gọn

- Module 1 (weeks 1–3): Foundations
  - [ ] Tuần 1: Lesson1 + Lesson2 — SparkSession, RDD/DataFrame/Dataset, WordCount lab
  - [ ] Tuần 2: Lesson3 + Lesson4 — Job/Stage/Task, partition experiments
  - [ ] Tuần 3: Lesson5 + Lesson6 — JDBC read, Parquet, Mini Project 1 (CSV→Parquet→Iceberg)

- Module 2 (weeks 4–7): Spark SQL & DataFrame
  - [ ] Tuần 4: Transformations & aggregations
  - [ ] Tuần 5: Joins & window functions
  - [ ] Tuần 6: Complex types & UDFs
  - [ ] Tuần 7: Catalyst/explain + Project 1 full

- Module 3 (weeks 8–11): Internals & Performance
  - [ ] Tuần 8–11: Shuffle, memory, skew, AQE, small-files, Project 3

- Module 4 (weeks 12–15): Structured Streaming
  - [ ] Tuần 12–15: Kafka source, watermark, stateful ops, CDC MERGE, Project 2

- Module 5 (weeks 16–19): Lakehouse & Iceberg
  - [ ] Tuần 16–19: Iceberg internals, compaction, partitioning, dbt tests

- Module 6 (weeks 20–24): Production & Capstone
  - [ ] Tuần 20–24: Deployment, monitoring, CI/CD, Capstone project

---

## Lab workflow (repeatable)

1. Create lab folder: `labs/labXX/` and files: `README.md`, `requirements.txt`, `script.py`.
2. Write small, runnable script (PySpark) that reads/writes from `data/` or JDBC.
3. Run locally with `spark-submit --master local[*] labs/labXX/script.py`.
4. Open Spark UI at `http://localhost:4040` to inspect jobs/stages/tasks.
5. Save important logs/screenshots to `labs/labXX/logs/` and note results in `labs/labXX/README.md`.

---

## Mini-project / Project checklist

- Deliverables (per project):
  - `scripts/` — spark scripts used
  - `design.md` — design decisions, schema, SLA
  - `run.log` or Spark UI screenshots
  - `airflow/dags/` if orchestration required
  - `tests/` — simple validation or chispa tests
- Testing steps (example Project 1):
  - Run `batch_ingest.py` → confirm Parquet files + partitions
  - Create Iceberg table → confirm `SELECT` in Trino
  - Write short report with timing & file sizes

---

## Useful snippets

- JDBC read with partitions

```python
df = spark.read.format("jdbc").options(
  url="jdbc:postgresql://localhost:5432/olist",
  dbtable="public.orders",
  user="postgres",
  password="postgres",
  partitionColumn="order_id",
  lowerBound="1",
  upperBound="100000",
  numPartitions="4"
).load()
```

- Write partitioned Parquet

```python
(df
  .withColumn("ingest_date", current_timestamp())
  .write.mode("overwrite")
  .partitionBy("order_date")
  .parquet("data/parquet/orders"))
```

- Simple `explain` (formatted)

```python
df.select("order_id").where("amount > 100").explain(True)
```

---

## How to record progress

- Edit `ROADMAP.md` checkboxes manually when bạn hoàn thành item.
- Save short notes to `labs/labXX/README.md` (commands you ran, output sample, issues)
- Use `docs/LEARNING_PLAN.md` as your personal checklist — tick items locally or mark as done.

---

## Debug & troubleshooting checklist

- If `spark-submit` fails: xem logs printed to console; xem driver logs; check JAVA_HOME và PYSPARK_PYTHON
- If JDBC slow: tăng `numPartitions` hoặc dùng server-side partition key
- If OOM executor: giảm partition parallelism, tăng executor memory, hoặc persist less
- If small files: run compaction with Spark rewrite or Iceberg `optimize` job

---

## Where to store artifacts

- scripts: `labs/labXX/*.py`
- configs: `infra/connectors/*.json`
- DAGs: `airflow/dags/*.py`
- reports: `labs/labXX/report.md` or `docs/reports/`

---

## Next steps (recommended immediate actions)

1. Tạo `labs/lab01/` theo checklist Module 1, chạy WordCount lab.
2. Khi xong lab01, upload `labs/lab01/README.md` với kết quả, tôi sẽ review và gợi ý.

---

Nếu bạn muốn, tôi sẽ sinh ngay skeleton files cho `labs/lab01/` (README + starter scripts).