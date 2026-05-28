# Social Media Lambda Pipeline

Pipeline xử lý dữ liệu mạng xã hội theo mô hình Lambda Architecture. Nhận dữ liệu từ Reddit, Facebook và Instagram, chuẩn hóa về canonical schema, ghi raw data vào MinIO, tạo batch views bằng Spark, xử lý realtime bằng Spark Structured Streaming, lưu serving data vào Elasticsearch/Redis và expose qua FastAPI + dashboard tĩnh.

Dự án sử dụng **Kubernetes (k8s) local** bằng Minikube làm hạ tầng chính.

## Mục Lục

- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Cấu hình các dịch vụ k8s](#cấu-hình-các-dịch-vụ-k8s)
- [Yêu cầu môi trường](#yêu-cầu-môi-trường)
- [Chạy dự án từ đầu (Kubernetes)](#chạy-dự-án-từ-đầu-kubernetes)
- [Kiểm tra kết quả](#kiểm-tra-kết-quả)
- [Makefile Reference](#makefile-reference)
- [Luồng dữ liệu chi tiết](#luồng-dữ-liệu-chi-tiết)
- [Cấu hình môi trường](#cấu-hình-môi-trường)

---

## Kiến Trúc Hệ Thống

```text
data/*
  → ingestion.simulator (replay-simulators)
  → Kafka  social.<platform>.posts
  → batch.object_store_writer (object-store-writer pod)
  → MinIO  s3a://social-lake/data/raw/<platform>/...
  → batch.spark_batch_job (Spark Job)
  → MinIO  s3a://social-lake/data/batch_views/...
  → batch.index_batch_views (Spark Job)
  → Elasticsearch  social_batch_views

Kafka  social.<platform>.posts
  → speed.streaming_job (speed-streaming pod)
  → Redis       rt:stats:* / rt:hashtags:*
  → Elasticsearch  social_realtime_views
  → Kafka       social.enriched.posts

Elasticsearch + Redis
  → serving.merge_service (api pod)
  → api.main  (FastAPI)
  → dashboard/index.html (dashboard pod)
```

| Layer | Thành phần | Vai trò |
|---|---|---|
| Ingestion | `ingestion.simulator` | Đọc sample data, normalize, publish Kafka |
| Raw/Object | `batch.object_store_writer` | Consume Kafka → raw Parquet → MinIO |
| Batch | `batch.spark_batch_job` | Đọc raw Parquet, tạo batch views |
| Speed | `speed.streaming_job` | Consume Kafka, enrich NLP, ghi Redis + ES |
| Serving | `serving.merge_service` | Merge batch + realtime cho API |

---

## Cấu Hình Các Dịch Vụ K8s

Khi kích hoạt Port-forward (`make forward`), các dịch vụ sẽ được mapping ra máy Host tại các cổng sau:

| Service | Vai trò | URL / Cổng Host | Cổng k8s Service |
|---|---|---|---|
| Kafka | Message broker | `localhost:9092` | `kafka-service:9092` |
| MinIO Console | Object storage UI | http://localhost:9001 | `minio-service:9001` |
| Spark Master | Cluster UI | http://localhost:8080 | `spark-master-service:8080` |
| Redis | Realtime cache | `localhost:6379` | `redis-service:6379` |
| Elasticsearch | Serving indexes | http://localhost:9200 | `elasticsearch-service:9200` |
| FastAPI | Serving API | http://localhost:8000 | `api-service:8000` |
| Dashboard | UI tĩnh | http://localhost:8084 | `dashboard-service:80` |
| Grafana | Metrics dashboard | http://localhost:3000 | `grafana-service:3000` |
| Airflow | Workflow orchestration | http://localhost:8082 | `airflow-webserver-service:8080` |

---

## Yêu Cầu Môi Trường

- **Minikube** (hoặc cụm k8s local tương đương)
- Tài nguyên khuyến nghị cho Minikube: Tối thiểu **8 GB RAM** và **4 CPU** (Ví dụ: `minikube start --memory=8192 --cpus=4`).
- CLI **kubectl** đã kết nối thành công tới cluster.
- **Docker Engine** (được sử dụng làm driver cho Minikube).

---

## Chạy Dự Án Từ Đầu (Kubernetes)

### 1. Khởi động Minikube & Mount thư mục

> **Lưu ý (Docker driver trên Linux):** Minikube với Docker driver không hỗ trợ `minikube mount` sau khi đã start. Cần truyền `--mount` ngay lúc khởi động.

```bash
# Khởi động Minikube và mount thư mục dự án vào /social-pipeline trong cluster
# Thay /path/to/IT4931 bằng đường dẫn tuyệt đối đến thư mục dự án
minikube start --memory=8192 --cpus=4 \
  --mount --mount-string="/path/to/IT4931:/social-pipeline"
```

Giữ tiến trình `minikube` đang chạy (không Ctrl+C) trong suốt quá trình sử dụng để mount luôn active.

### 2. Build Docker Images vào Minikube

Tất cả custom images phải được build **bên trong Docker daemon của Minikube** (không phải Docker host) vì các manifest dùng `imagePullPolicy: Never`.

```bash
# Trỏ Docker CLI vào daemon của Minikube
eval $(minikube docker-env)

# Build các core images (social-python, social-spark, social-ml)
make build-core

# (Tùy chọn) Build Airflow image nếu cần orchestration
make build-airflow
```

> **Lưu ý:** `make build-airflow` mất 5–10 phút do tải JAR Spark và cài pyspark. Bỏ qua nếu không cần Airflow.

### 3. Triển khai các Manifests lên k8s
```bash
# Áp dụng tất cả manifests (Namespace, Configs, Storage, Apps, Simulators...)
make apply
```
Kiểm tra trạng thái các Pod cho tới khi tất cả đều ở trạng thái `Running`:
```bash
make ps
```

Thứ tự khởi động dự kiến (mỗi bước có thể mất 1–3 phút):

1. `kafka`, `zookeeper`, `minio`, `redis`, `elasticsearch`, `cassandra` — Infrastructure
2. `kafka-init`, `minio-init` — Init Jobs (chuyển sang `Completed`)
3. `object-store-writer`, `speed-streaming`, `api`, `dashboard` — Apps
4. `replay-reddit`, `replay-facebook`, `replay-instagram` — Simulators bắt đầu publish dữ liệu
5. `prometheus`, `grafana`, `airflow-*` — Monitoring & Orchestration

> Simulators chạy dưới dạng **Job** (một lần duy nhất). Sau khi publish xong dữ liệu, Pod chuyển sang `Completed`. Muốn chạy lại: `kubectl delete job -n social-pipeline -l app=replay-reddit && make apply`.

### 4. Thiết lập Port-forward ra máy Host
```bash
# Giữ terminal này chạy ở background
make forward
```

### 5. Chạy các tác vụ Batch và kiểm tra kết quả
1. Đợi các simulators phát dữ liệu khoảng 30-60 giây để `object-store-writer` ghi Parquet vào MinIO.
2. Chạy Spark Batch Job để recompute dữ liệu lô:
   ```bash
   make batch
   ```
3. Đồng bộ hóa batch views đã xử lý vào Elasticsearch:
   ```bash
   make index-batch
   ```
4. Nạp dữ liệu vào ClickHouse Data Warehouse:
   ```bash
   make warehouse
   ```

---

## Kiểm Tra Kết Quả

Bạn có thể truy cập các UI hoặc kiểm tra nhanh qua API:
- **Dashboard:** http://localhost:8084
- **API Health:** http://localhost:8000/health
- **Kiểm tra API dữ liệu:**
  ```bash
  # Lấy danh sách post đã chuẩn hóa (chứa cả batch và realtime)
  curl -fsS "http://localhost:8000/api/v1/posts?limit=5&start=2025-01-01T00:00:00Z" | python3 -m json.tool

  # Xem thống kê realtime
  curl -fsS "http://localhost:8000/api/v1/stats/realtime" | python3 -m json.tool
  ```

Kiểm tra logs simulator để xác nhận dữ liệu đang được publish:
```bash
make logs-simulator
```

> Simulators chạy một lần duy nhất (Job), nên `post_count` trên dashboard phản ánh đúng số bài thực tế. Redis stats key có TTL 2 giờ — sau đó cần chạy lại Job để refresh dữ liệu realtime.

---

## Makefile Reference

| Lệnh | Mô tả |
|---|---|
| `make build-core` | Build core images: `social-python`, `social-spark`, `social-ml` |
| `make build-airflow` | Build Airflow image (tùy chọn, mất ~5–10 phút) |
| `make build` | Build tất cả images (core + airflow) |
| `make test` | Chạy unit tests cục bộ |
| `make download-data` | Tải và giải nén dữ liệu mẫu lớn từ Drive |
| `make apply` | Apply toàn bộ manifests lên Kubernetes |
| `make delete` | Xóa sạch namespace `social-pipeline` trên k8s |
| `make forward` | Port-forward 8 dịch vụ ra host (Dashboard, API, MinIO, Spark, ES, Redis, Grafana, Airflow) |
| `make batch` | Chạy Spark batch job trên k8s |
| `make index-batch` | Đồng bộ batch views vào Elasticsearch trên k8s |
| `make warehouse` | Chạy Spark job nạp ClickHouse trên k8s |
| `make ps` | Xem trạng thái các Pods trong namespace `social-pipeline` |
| `make logs` | Xem logs của Serving API pod |
| `make logs-writer` | Xem logs của Object Store Writer pod |
| `make logs-speed` | Xem logs của Speed Streaming pod |
| `make logs-simulator` | Xem logs của cả 3 simulator pods (reddit, facebook, instagram) |

---

## Luồng Dữ Liệu Chi Tiết

### Kafka topics

| Topic | Mô tả |
|---|---|
| `social.reddit.posts` | Post Reddit đã normalize |
| `social.facebook.posts` | Post Facebook đã normalize |
| `social.instagram.posts` | Post Instagram đã normalize |
| `social.enriched.posts` | Post sau NLP enrichment từ speed layer |
| `social.dlq` | Record lỗi validation |

### Raw data (MinIO)

`object-store-writer` ghi Parquet partition theo platform/ngày:
```
s3a://social-lake/data/raw/<platform>/year=YYYY/month=MM/day=DD/
```

### Batch views (MinIO → Elasticsearch)

| View | Nội dung |
|---|---|
| `platform_daily_stats` | Post count, avg sentiment, total engagement theo ngày |
| `top_hashtags_weekly` | Top 100 hashtag theo tuần |
| `author_activity` | Hoạt động theo author |
| `sentiment_time_series` | Avg sentiment theo giờ |
| `top_posts` | Top 1000 posts theo engagement |

Output: `s3a://social-lake/data/batch_views/<view_name>/`
Index: `social_batch_views`

### Realtime views (Redis + Elasticsearch)

Speed layer ghi:

| Store | Key pattern | Nội dung |
|---|---|---|
| Redis | `rt:stats:<platform>:<hour>` | Post count, sentiment sum |
| Redis | `rt:hashtags:<platform>:<hour>` | Sorted set hashtag count |
| Elasticsearch | `social_realtime_views` | Post với enrichment fields |

---

## Cấu Hình Môi Trường

Các thông số điều chỉnh thông qua ConfigMap ở [k8s/01-config/configmap.yaml](k8s/01-config/configmap.yaml):
- `CONSUMER_FLUSH_SIZE`: Số lượng record flush raw Parquet (Mặc định: 500)
- `CONSUMER_FLUSH_INTERVAL`: Thời gian tối đa flush raw writer (Mặc định: 30 giây)
- `STREAM_TRIGGER_SECS`: Thời gian trigger Spark Streaming (Mặc định: 5 giây)
- `NLP_MODEL_NAME`: HuggingFace Sentiment analysis model sử dụng ở speed-streaming.
