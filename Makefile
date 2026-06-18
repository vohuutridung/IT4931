NAMESPACE ?= social-pipeline
S3_BUCKET ?= social-lake
AIRFLOW_DAG_ID ?= social_lambda_batch_pipeline
KUBECTL ?= kubectl
MINIKUBE_MEMORY ?= 10240
MINIKUBE_CPUS ?= 4
MINIKUBE_MOUNT ?= $(PWD):/social-pipeline
DOCKER_BUILD ?= DOCKER_BUILDKIT=0 docker build

.PHONY: minikube-start minikube-build minikube-build-core minikube-build-airflow \
        build build-core build-airflow build-api-ml test download-data apply apply-simulators delete forward forward-kill warehouse batch \
        ps logs logs-writer logs-speed logs-simulator \
        ensure-kafka-topics reset-data reset-streaming replay dag-trigger dag-status health

build: build-core build-airflow

minikube-start:
	minikube start --memory=$(MINIKUBE_MEMORY) --cpus=$(MINIKUBE_CPUS) --mount --mount-string="$(MINIKUBE_MOUNT)"
	minikube ssh -- sudo sysctl -w vm.max_map_count=262144

minikube-build:
	/bin/bash -lc 'eval "$$(minikube docker-env)" && $(MAKE) build'

minikube-build-core:
	/bin/bash -lc 'eval "$$(minikube docker-env)" && $(MAKE) build-core'

minikube-build-airflow:
	/bin/bash -lc 'eval "$$(minikube docker-env)" && $(MAKE) build-airflow'

build-core: build-api-ml
	@echo "Building core Docker images..."
	$(DOCKER_BUILD) -t social-python:0.1.0 -f Dockerfile .
	$(DOCKER_BUILD) -t social-spark:3.5.1 -f Dockerfile.spark .

build-api-ml:
	@echo "Building API image with ML dependencies..."
	$(DOCKER_BUILD) -t social-api-ml:0.1.0 --build-arg REQUIREMENTS_FILE=requirements.ml.txt -f Dockerfile .

build-airflow:
	@echo "Building Airflow images (optional, replicas=0)..."
	$(DOCKER_BUILD) -t social-pipeline-airflow-webserver:latest -f Dockerfile.airflow .
	docker tag social-pipeline-airflow-webserver:latest social-pipeline-airflow-init:latest
	docker tag social-pipeline-airflow-webserver:latest social-pipeline-airflow-scheduler:latest

test:
	uv run pytest tests/unit

download-data:
	python3 scripts/download_data.py

apply:
	@test -f k8s/01-config/secrets.yaml || (echo "Missing k8s/01-config/secrets.yaml. Copy k8s/01-config/secrets.yaml.example to secrets.yaml and fill real values first."; exit 1)
	$(KUBECTL) apply -f k8s/00-namespace.yaml
	$(KUBECTL) apply -f k8s/01-config/configmap.yaml
	$(KUBECTL) apply -f k8s/01-config/secrets.yaml
	$(KUBECTL) apply -f k8s/02-storage/
	$(KUBECTL) apply -f k8s/03-infrastructure/
	$(MAKE) ensure-kafka-topics
	$(KUBECTL) apply -f k8s/04-apps/
	$(KUBECTL) delete job -n $(NAMESPACE) airflow-init --ignore-not-found=true
	$(KUBECTL) apply -f k8s/05-orchestration/

apply-simulators:
	$(KUBECTL) apply -f k8s/07-simulators/

delete:
	$(KUBECTL) delete namespace $(NAMESPACE) --ignore-not-found=true

forward:
	@echo "Forwarding: Dashboard:8084 | API:8000 | MinIO S3 API:9000 Console:9001 | Airflow:8085 | Kafka UI:8086 | ClickHouse:8123 | Spark Master:8080 | ES:9200 | Redis:6379"
	@echo "Press Ctrl+C to stop."
	@_pf() { while true; do $(KUBECTL) port-forward -n $(NAMESPACE) svc/$$1 $$2 2>&1 | grep -v 'Handling'; sleep 2; done; }; \
	 _pf dashboard-service   8084:80    & \
	 _pf api-service         8000:8000  & \
	 _pf minio-service       9000:9000  & \
	 _pf minio-service       9001:9001  & \
	 _pf airflow-webserver-service 8085:8080 & \
	 _pf clickhouse-service  8123:8123  & \
	 _pf spark-master-service 8080:8080 & \
	 _pf elasticsearch-service 9200:9200 & \
	 _pf redis-service       6379:6379  & \
	 _pf kafka-ui-service    8086:8086  & \
	 wait


forward-kill:
	@echo "Stopping all port-forwards..."
	@pkill -f 'kubectl port-forward' 2>/dev/null || true
	@echo "Done."

warehouse:
	$(KUBECTL) exec -n $(NAMESPACE) -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_POD_IP \
	  --total-executor-cores 1 \
	  /app/warehouse/clickhouse_loader.py'

batch:
	$(KUBECTL) exec -n $(NAMESPACE) -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_POD_IP \
	  --total-executor-cores 1 \
	  /app/batch/spark_batch_job.py \
	  --input-partitions 8 \
	  --shuffle-partitions 4'

ps:
	$(KUBECTL) get pods -n $(NAMESPACE)

logs:
	$(KUBECTL) logs -n $(NAMESPACE) deployments/api --tail=100

logs-writer:
	$(KUBECTL) logs -n $(NAMESPACE) deployments/object-store-writer --tail=100

logs-speed:
	$(KUBECTL) logs -n $(NAMESPACE) deployments/speed-streaming --tail=100

logs-simulator:
	$(KUBECTL) logs -n $(NAMESPACE) job/replay-reddit --tail=50
	$(KUBECTL) logs -n $(NAMESPACE) job/replay-facebook --tail=50
	$(KUBECTL) logs -n $(NAMESPACE) job/replay-instagram --tail=50

ensure-kafka-topics:
	@echo "Đảm bảo Kafka topics tồn tại..."
	$(KUBECTL) rollout status deployment/kafka -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) delete job -n $(NAMESPACE) kafka-init --ignore-not-found=true
	$(KUBECTL) apply -f k8s/03-infrastructure/kafka.yaml
	$(KUBECTL) wait -n $(NAMESPACE) --for=condition=complete job/kafka-init --timeout=180s

health:
	@echo "API:"
	@curl -s http://127.0.0.1:8000/health || true
	@echo "\nDashboard:"
	@curl -s -I http://127.0.0.1:8084/ | head -5 || true
	@echo "Airflow:"
	@curl -s -I http://127.0.0.1:8085/ | head -5 || true
	@echo "Realtime stats:"
	@curl -s http://127.0.0.1:8000/api/v1/stats/realtime | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('stats', [])))" || true

# ── Reset & Replay ────────────────────────────────────────────────────────────

reset-data:
	@echo "[1/7] Dừng producer/consumer đang ghi dữ liệu..."
	$(KUBECTL) delete job -n $(NAMESPACE) replay-reddit replay-facebook replay-instagram --ignore-not-found=true
	$(KUBECTL) scale deployment -n $(NAMESPACE) speed-streaming object-store-writer --replicas=0
	@echo "[2/7] Xóa sạch ClickHouse batch tables..."
	$(KUBECTL) exec -n $(NAMESPACE) deployments/clickhouse -- clickhouse-client -d social -q \
	  "TRUNCATE TABLE IF EXISTS dim_platform; \
	   TRUNCATE TABLE IF EXISTS fact_platform_daily_stats; \
	   TRUNCATE TABLE IF EXISTS fact_top_hashtags_weekly; \
	   TRUNCATE TABLE IF EXISTS fact_author_activity; \
	   TRUNCATE TABLE IF EXISTS fact_sentiment_time_series; \
	   TRUNCATE TABLE IF EXISTS fact_top_posts;"
	@echo "[3/7] Xóa raw data, batch views và checkpoints trong MinIO..."
	$(KUBECTL) exec -n $(NAMESPACE) deployments/minio -- \
	  sh -ec 'mc alias set local http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null; \
	          mc rm -r --force local/$(S3_BUCKET)/data/ || true; \
	          mc rm -r --force local/$(S3_BUCKET)/checkpoints/ || true'
	@echo "[4/7] Xóa Elasticsearch indices và Redis cache..."
	$(KUBECTL) exec -n $(NAMESPACE) deployments/airflow-scheduler -- python3 -c "import requests; [requests.delete(f'http://elasticsearch-service:9200/{idx}') for idx in ('social_batch_views','social_realtime_views','social_network','social_topics')]" || true
	$(KUBECTL) exec -n $(NAMESPACE) deployments/redis -- redis-cli flushall || true
	@echo "[5/7] Restart Kafka và tạo lại topics..."
	$(KUBECTL) rollout restart deployment/kafka -n $(NAMESPACE)
	$(KUBECTL) rollout status deployment/kafka -n $(NAMESPACE) --timeout=180s
	$(MAKE) ensure-kafka-topics
	@echo "[6/7] Xóa lịch sử Airflow (best effort)..."
	$(KUBECTL) exec -n $(NAMESPACE) deployments/airflow-scheduler -- \
	  airflow db clean \
	    --tables dag_run,task_instance,log,job,xcom,task_fail,task_reschedule \
	    --clean-before-timestamp "2100-01-01" \
	    --skip-archive \
	    --yes || true
	@echo "[7/7] Khôi phục speed-streaming và object-store-writer..."
	$(KUBECTL) scale deployment -n $(NAMESPACE) speed-streaming object-store-writer --replicas=1
	$(KUBECTL) rollout status deployment/speed-streaming -n $(NAMESPACE) --timeout=180s
	$(KUBECTL) rollout status deployment/object-store-writer -n $(NAMESPACE) --timeout=180s
	@echo "Reset hoàn tất. Chạy 'make replay' để phát lại dữ liệu."

reset-streaming:
	@echo "Dừng speed-streaming để reset checkpoint và cache realtime..."
	$(KUBECTL) scale deployment -n $(NAMESPACE) speed-streaming --replicas=0
	$(KUBECTL) exec -n $(NAMESPACE) deployments/minio -- \
	  sh -ec 'mc alias set local http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null; \
	          mc rm -r --force local/$(S3_BUCKET)/checkpoints/speed/ || true'
	$(KUBECTL) exec -n $(NAMESPACE) deployments/redis -- \
	  sh -ec 'redis-cli --scan --pattern "rt:*" | xargs -r redis-cli del >/dev/null || true'
	$(MAKE) ensure-kafka-topics
	@echo "Khởi động lại speed-streaming..."
	$(KUBECTL) scale deployment -n $(NAMESPACE) speed-streaming --replicas=1
	$(KUBECTL) rollout status deployment/speed-streaming -n $(NAMESPACE) --timeout=240s

replay:
	@echo "Xóa Simulator Jobs cũ..."
	$(KUBECTL) delete job -n $(NAMESPACE) replay-reddit replay-facebook replay-instagram --ignore-not-found=true
	@echo "Chạy lại Simulator Jobs..."
	$(MAKE) apply-simulators
	@echo "Simulators đang chạy. Kiểm tra: make ps"

dag-trigger:
	@echo "Bật DAG và trigger chạy ngay..."
	$(KUBECTL) exec -n $(NAMESPACE) deployments/airflow-scheduler -- \
	  airflow dags unpause $(AIRFLOW_DAG_ID)
	$(KUBECTL) exec -n $(NAMESPACE) deployments/airflow-scheduler -- \
	  airflow dags trigger $(AIRFLOW_DAG_ID)
	@echo "DAG đã được trigger. Kiểm tra tại http://localhost:8085"

dag-status:
	$(KUBECTL) exec -n $(NAMESPACE) deployments/airflow-scheduler -- \
	  airflow dags list-runs -d $(AIRFLOW_DAG_ID) --no-backfill -o table
