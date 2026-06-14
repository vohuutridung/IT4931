.PHONY: build build-core build-airflow build-api-ml test download-data apply delete forward warehouse batch \
        ps logs logs-writer logs-speed logs-simulator \
        reset-data replay dag-trigger dag-status

build: build-core build-airflow

build-core: build-api-ml
	@echo "Building core Docker images..."
	docker build -t social-python:0.1.0 -f Dockerfile .
	docker build -t social-spark:3.5.1 -f Dockerfile.spark .
	docker build -t social-ml:0.1.0 --build-arg REQUIREMENTS_FILE=requirements.ml.txt -f Dockerfile .

build-api-ml:
	@echo "Building API image with ML dependencies..."
	docker build -t social-api-ml:0.1.0 --build-arg REQUIREMENTS_FILE=requirements.ml.txt -f Dockerfile .

build-airflow:
	@echo "Building Airflow images (optional, replicas=0)..."
	docker build -t social-pipeline-airflow-webserver:latest -f Dockerfile.airflow .
	docker tag social-pipeline-airflow-webserver:latest social-pipeline-airflow-init:latest
	docker tag social-pipeline-airflow-webserver:latest social-pipeline-airflow-scheduler:latest

test:
	pytest tests/unit

download-data:
	python3 scripts/download_data.py

# Kubernetes targets (Shortened)
apply:
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/01-config/
	kubectl apply -f k8s/02-storage/
	kubectl apply -f k8s/03-infrastructure/
	kubectl apply -f k8s/04-apps/
	kubectl apply -f k8s/05-orchestration/
	kubectl apply -f k8s/07-simulators/

delete:
	kubectl delete namespace social-pipeline

forward:
	@echo "Forwarding: Dashboard:8084 | API:8000 | MinIO S3 API:9000 Console:9001 | Airflow:8085 | ClickHouse:8123 | Spark Master:8080 | ES:9200 | Redis:6379"
	@echo "Press Ctrl+C to stop."
	@_pf() { while true; do kubectl port-forward -n social-pipeline svc/$$1 $$2 2>&1 | grep -v 'Handling'; sleep 2; done; }; \
	 _pf dashboard-service   8084:80    & \
	 _pf api-service         8000:8000  & \
	 _pf minio-service       9000:9000 9001:9001 & \
	 _pf airflow-webserver-service 8085:8080 & \
	 _pf clickhouse-service  8123:8123  & \
	 _pf spark-master-service 8080:8080 & \
	 _pf elasticsearch-service 9200:9200 & \
	 _pf redis-service       6379:6379  & \
	 wait


forward-kill:
	@echo "Stopping all port-forwards..."
	@pkill -f 'kubectl port-forward' 2>/dev/null || true
	@echo "Done."

warehouse:
	kubectl exec -n social-pipeline -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_POD_IP \
	  --total-executor-cores 1 \
	  /app/warehouse/clickhouse_loader.py'

batch:
	kubectl exec -n social-pipeline -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_POD_IP \
	  --total-executor-cores 1 \
	  /app/batch/spark_batch_job.py \
	  --input-partitions 64 \
	  --shuffle-partitions 64'

ps:
	kubectl get pods -n social-pipeline

logs:
	kubectl logs -n social-pipeline deployments/api --tail=100

logs-writer:
	kubectl logs -n social-pipeline deployments/object-store-writer --tail=100

logs-speed:
	kubectl logs -n social-pipeline deployments/speed-streaming --tail=100

logs-simulator:
	kubectl logs -n social-pipeline job/replay-reddit --tail=50
	kubectl logs -n social-pipeline job/replay-facebook --tail=50
	kubectl logs -n social-pipeline job/replay-instagram --tail=50

# ── Reset & Replay ────────────────────────────────────────────────────────────

reset-data:
	@echo "[1/3] Tạm dừng speed-streaming và object-store-writer..."
	kubectl scale deployment -n social-pipeline speed-streaming object-store-writer --replicas=0
	@echo "[2/3] Xóa sạch ClickHouse..."
	kubectl exec -n social-pipeline deployments/clickhouse -- clickhouse-client -d social -q \
	  "TRUNCATE TABLE dim_platform; \
	   TRUNCATE TABLE fact_platform_daily_stats; \
	   TRUNCATE TABLE fact_top_hashtags_weekly; \
	   TRUNCATE TABLE fact_author_activity; \
	   TRUNCATE TABLE fact_sentiment_time_series; \
	   TRUNCATE TABLE fact_top_posts; \
	   TRUNCATE TABLE IF EXISTS realtime_posts; \
	   TRUNCATE TABLE IF EXISTS merged_posts;"
	@echo "[2/3] Xóa raw data, batch views và checkpoints trong MinIO..."
	kubectl exec -n social-pipeline deployments/minio -- \
	  bash -c "mc alias set l http://localhost:9000 minioadmin minioadmin 2>/dev/null && \
	           mc rm -r --force l/social-lake/data/ ; \
	           mc rm -r --force l/social-lake/checkpoints/ ; true"
	@echo "[2/3] Xóa sạch Elasticsearch và Redis..."
	kubectl exec -n social-pipeline deployments/airflow-scheduler -- python3 -c "import requests; [requests.delete(f'http://elasticsearch-service:9200/{idx}') for idx in ('social_batch_views','social_realtime_views','social_network','social_topics')]" || true
	kubectl exec -n social-pipeline deployments/redis -- redis-cli flushall || true
	@echo "[2/3] Restart Kafka và tạo lại topics..."

	kubectl rollout restart deployment/kafka -n social-pipeline
	kubectl rollout status deployment/kafka -n social-pipeline
	kubectl delete job -n social-pipeline kafka-init 2>/dev/null || true
	kubectl apply -f k8s/03-infrastructure/kafka.yaml
	@echo "[3/3] Khôi phục speed-streaming và object-store-writer..."
	kubectl scale deployment -n social-pipeline speed-streaming object-store-writer --replicas=1
	@echo "✅ Reset hoàn tất! Chạy 'make replay' để phát lại dữ liệu."

replay:
	@echo "Xóa Simulator Jobs cũ..."
	kubectl delete job -n social-pipeline replay-reddit replay-facebook replay-instagram 2>/dev/null || true
	@echo "Chạy lại Simulator Jobs..."
	kubectl apply -f k8s/07-simulators/
	@echo "✅ Simulators đang chạy! Kiểm tra: make ps"

dag-trigger:
	@echo "Bật DAG và trigger chạy ngay..."
	kubectl exec -n social-pipeline deployments/airflow-scheduler -- \
	  airflow dags unpause social_lambda_batch_pipeline
	kubectl exec -n social-pipeline deployments/airflow-scheduler -- \
	  airflow dags trigger social_lambda_batch_pipeline
	@echo "✅ DAG đã được trigger! Kiểm tra tại http://localhost:8085"

dag-status:
	kubectl exec -n social-pipeline deployments/airflow-scheduler -- \
	  airflow dags list-runs -d social_lambda_batch_pipeline --no-backfill -o table