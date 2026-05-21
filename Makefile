.PHONY: up down test simulate object-store-writer batch index-batch warehouse stream api

up:
	docker compose up --build -d

down:
	docker compose down

test:
	pytest

simulate:
	python -m ingestion.simulator --source data/reddit_data/sample_data/post.json --platform reddit --rate $${RATE:-50} --kafka-bootstrap $${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

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
