# Spark Mastery — lệnh tắt cho cluster Docker
#
# Dùng:
#   make up                          # bật cluster
#   make down                        # tắt cluster
#   make run F=labs/lab01/bai_42.py  # submit 1 file lên cluster
#   make run-local F=labs/...        # chạy local mode (không qua cluster)
#   make shell                       # mở PySpark shell tương tác
#   make ps                          # trạng thái các container

COMPOSE   = docker compose -f docker-compose.spark.yaml
CONTAINER = spark-mastery-spark-submit-1
MASTER    = spark://spark-master:7077

.PHONY: up down ps run run-local shell logs

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

ps:
	docker ps --filter name=spark-mastery --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

run:
ifndef F
	$(error Thieu duong dan file. Dung: make run F=labs/lab01/bai_42.py)
endif
	docker exec $(CONTAINER) /opt/spark/bin/spark-submit --master $(MASTER) /workspace/$(F)

run-local:
ifndef F
	$(error Thieu duong dan file. Dung: make run-local F=labs/lab01/bai_42.py)
endif
	docker exec $(CONTAINER) /opt/spark/bin/spark-submit --master 'local[2]' /workspace/$(F)

shell:
	docker exec -it $(CONTAINER) /opt/spark/bin/pyspark --master $(MASTER)

logs:
	$(COMPOSE) logs -f --tail=50
