.PHONY: up up-full monitoring debug orchestration warehouse-stack enrichment anomaly down replay-raw test simulate object-store-writer batch index-batch warehouse stream api

up:
	docker compose up --build -d

up-full:
	docker compose --profile monitoring --profile debug --profile orchestration --profile warehouse --profile enrichment --profile anomaly up --build -d

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
	docker compose --profile replay up -d replay-reddit replay-facebook replay-instagram

down:
	docker compose down

test:
	pytest

simulate:
	python -m ingestion.simulator --platform $${PLATFORM:-reddit} --source $${SOURCE:-data/$${PLATFORM:-reddit}_data/raw_data} --rate $${RATE:-20} --kafka-bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

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
