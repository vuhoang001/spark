# Chạy Lab01 bằng Docker

### 1. Khởi động cluster Spark

```bash
docker compose -f docker-compose.spark.yaml up -d
```

### 2. Chạy lab01 bằng `spark-submit`

```bash
docker compose -f docker-compose.spark.yaml run --rm spark-submit spark-submit --master spark://spark-master:7077 labs/lab01/lab01.py
```

### 3. Nếu muốn mở Spark UI

Mở trình duyệt:

```
http://localhost:8080
```

### 4. Dừng container khi xong

```bash
docker compose -f docker-compose.spark.yaml down
```
