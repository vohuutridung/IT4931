.PHONY: build test download-data apply delete forward warehouse batch index-batch \
        ps logs logs-writer logs-speed

build:
	@echo "Building custom Docker images for Kubernetes..."
	docker build -t social-python:0.1.0 -f Dockerfile .
	docker build -t social-spark:3.5.3 -f Dockerfile.spark .
	docker build -t social-ml:0.1.0 --build-arg REQUIREMENTS_FILE=requirements.ml.txt -f Dockerfile .
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
	kubectl apply -f k8s/06-monitoring/
	kubectl apply -f k8s/07-simulators/

delete:
	kubectl delete namespace social-pipeline

forward:
	@echo "Forwarding Ports (Dashboard:8084, API:8000)..."
	@echo "Press Ctrl+C to stop forwarding."
	@kubectl port-forward -n social-pipeline svc/dashboard-service 8084:80 & \
	 kubectl port-forward -n social-pipeline svc/api-service 8000:8000 & \
	 wait

warehouse:
	kubectl exec -n social-pipeline -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_LOCAL_IP \
	  --total-executor-cores 1 \
	  /app/warehouse/clickhouse_loader.py'

batch:
	kubectl exec -n social-pipeline -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_LOCAL_IP \
	  --total-executor-cores 1 \
	  /app/batch/spark_batch_job.py \
	  --input-partitions 64 \
	  --shuffle-partitions 64'

index-batch:
	kubectl exec -n social-pipeline -it deployments/spark-master -- /bin/sh -c '\
	  /opt/spark/bin/spark-submit \
	  --master spark://spark-master-service:7077 \
	  --conf spark.driver.host=$$SPARK_LOCAL_IP \
	  --total-executor-cores 1 \
	  /app/batch/index_batch_views.py'

ps:
	kubectl get pods -n social-pipeline

logs:
	kubectl logs -n social-pipeline deployments/api --tail=100

logs-writer:
	kubectl logs -n social-pipeline deployments/object-store-writer --tail=100

logs-speed:
	kubectl logs -n social-pipeline deployments/speed-streaming --tail=100