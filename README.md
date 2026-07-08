# Spark Mastery — Chương trình học Apache Spark hướng Production Data Engineering

> Mentor: Senior Staff Data Engineer (Claude). Học viên: Backend engineer chuyển hướng Data Engineering,
> đã có SQL, PostgreSQL, Docker, Kafka, Debezium, Iceberg, Trino.

## Mục tiêu cuối khóa

Sau 4–6 tháng, bạn phải:

- Hiểu Spark từ nền tảng đến internals (Catalyst, DAG Scheduler, Shuffle, Memory).
- Thiết kế được pipeline ETL/ELT production.
- Thành thạo Spark SQL, DataFrame API, Structured Streaming.
- Tự tin performance tuning và debug Spark job qua Spark UI.
- Thiết kế Data Lakehouse với Kafka + Spark + Iceberg + Trino + Airflow.
- Đủ năng lực phỏng vấn Mid/Senior Data Engineer.

## Cấu trúc thư mục

```
spark-mastery/
├── README.md            ← file này
├── ROADMAP.md           ← roadmap 24 tuần, đọc trước khi bắt đầu
├── module-01-foundations/
│   └── lesson-01-why-spark-architecture.md   ← BẮT ĐẦU TẠI ĐÂY
├── module-02-spark-sql/
├── module-03-internals-performance/
├── module-04-structured-streaming/
├── module-05-lakehouse/
├── module-06-production/
└── labs/                ← code lab, mỗi lesson một thư mục
```

## Cách học

1. Đọc `ROADMAP.md` để nắm toàn cảnh.
2. Học từng lesson theo thứ tự — **không nhảy cóc**.
3. Mỗi lesson có 15 phần cố định: Objective → Why → Theory → Internal → API →
   Demo → Production Example → Lab → Assignment → Performance → Spark UI →
   Common Mistakes → Interview → Summary → Next Lesson.
4. Làm Lab trên stack của repo `../kafka-flink` bên cạnh (`docker-compose.yaml`
   ở root repo đó: Kafka 7.4, Iceberg REST catalog, Trino 435, PostgreSQL, Airflow).
5. Làm Assignment, nộp lại cho mentor review như code review thật.
6. Gõ **"Continue"** để mentor dạy bài tiếp theo.

## Quy tắc

- Hiểu **TẠI SAO** trước khi học **LÀM THẾ NÀO**.
- Mỗi 3–4 bài có một Mini Project; cuối mỗi module có một Project hoàn chỉnh.
- Assignment được review theo chuẩn Senior: correctness → performance → readability → production-readiness.
