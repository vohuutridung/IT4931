.PHONY: up up-full monitoring debug orchestration warehouse-stack enrichment anomaly down \
        replay-raw replay test simulate object-store-writer batch index-batch \
        index-batch-docker warehouse stream api build ps logs logs-all \
        minio-ls-raw spark-batch clean core-up app-up


up:
	docker compose up --build -d

up-full:
	docker compose \
	  --profile monitoring --profile debug --profile orchestration \
	  --profile warehouse --profile enrichment --profile anomaly \
	  up --build -d

monitoring:
	docker compose --profile monitoring up -d prometheus grafana

debug:
	docker compose --profile debug up -d kafka-ui kibana

orchestration:
	docker compose --profile orchestration up -d postgres airflow-init airflow-webserver airflow-scheduler

warehouse-stack:
	docker compose --profile warehouse up -d clickhouse

enrichment:
	docker compose --profile enrichment up -d cassandra

anomaly:
	docker compose --profile anomaly up -d cassandra ml-anomaly

replay-raw:
	@echo "Starting simulators in background (reddit=9101, facebook=9102, instagram=9103)..."
	@nohup python -m ingestion.simulator \
	  --platform reddit \
	  --source data/reddit_data/raw_data \
	  --rate $${REPLAY_RATE_PER_SEC:-20} \
	  --kafka-bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
	  --metrics-port 9101 \
	  > logs/reddit_simulator.log 2>&1 & \
	nohup python -m ingestion.simulator \
	  --platform facebook \
	  --source data/facebook_data/raw_data \
	  --rate $${REPLAY_RATE_PER_SEC:-20} \
	  --kafka-bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
	  --metrics-port 9102 \
	  > logs/facebook_simulator.log 2>&1 & \
	nohup python -m ingestion.simulator \
	  --platform instagram \
	  --source data/instagram_data/raw_data \
	  --rate $${REPLAY_RATE_PER_SEC:-20} \
	  --kafka-bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
	  --metrics-port 9103 \
	  > logs/instagram_simulator.log 2>&1 & \
	echo "All simulators started in background."

down:
	docker compose --profile "*" down

kill-simulators:
	@pkill -f "ingestion.simulator" && echo "Simulators stopped." || echo "No simulators running."

test:
	pytest tests/unit

simulate:
	python -m ingestion.simulator \
	  --platform $${PLATFORM:-reddit} \
	  --source $${SOURCE:-data/$${PLATFORM:-reddit}_data/raw_data} \
	  --rate $${RATE:-20} \
	  --kafka-bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

object-store-writer:
	python -m batch.object_store_writer

batch:
	spark-submit batch/spark_batch_job.py

index-batch:
	spark-submit batch/index_batch_views.py

warehouse:
	spark-submit warehouse/clickhouse_loader.py

stream:
	spark-submit speed/streaming_job.py

api:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

build:
	docker compose build

ps:
	docker compose ps

logs:
	docker compose logs -f object-store-writer speed api

logs-all:
	docker compose logs --tail=200 api speed object-store-writer elasticsearch

# Chạy replay qua Docker (không lo port conflict, không cần venv)
replay:
	docker compose --profile replay up -d replay-reddit replay-facebook replay-instagram

core-up:
	docker compose up -d zookeeper kafka kafka-init minio minio-init redis elasticsearch serving-init

app-up:
	docker compose up -d spark-master spark-worker speed object-store-writer api dashboard

spark-batch:
	docker compose exec -T spark-master \
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /app/batch/spark_batch_job.py \
	  --input-partitions 64 \
	  --shuffle-partitions 64

index-batch-docker:
	docker compose exec -T spark-master \
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /app/batch/index_batch_views.py

minio-ls-raw:
	docker compose run --rm --entrypoint sh minio-init -c \
	  'mc alias set local http://minio:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" \
	   && mc ls -r local/$${S3_BUCKET:-social-lake}/data/raw | head'

clean:
	docker compose --profile "*" down -v --remove-orphans