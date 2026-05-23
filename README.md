# Social Media Lambda Pipeline

Pipeline xử lý dữ liệu mạng xã hội theo mô hình Lambda Architecture. Nhận dữ liệu từ Reddit, Facebook và Instagram, chuẩn hóa về canonical schema, ghi raw data vào MinIO, tạo batch views bằng Spark, xử lý realtime bằng Spark Structured Streaming, lưu serving data vào Elasticsearch/Redis và expose qua FastAPI + dashboard tĩnh.

## Mục Lục

- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Service và cổng](#service-và-cổng)
- [Yêu cầu môi trường](#yêu-cầu-môi-trường)
- [Chạy dự án từ đầu](#chạy-dự-án-từ-đầu)
- [Replay và pipeline thủ công](#replay-và-pipeline-thủ-công)
- [Kiểm tra kết quả](#kiểm-tra-kết-quả)
- [Reset và chạy lại từ đầu](#reset-và-chạy-lại-từ-đầu)
- [Tắt dự án](#tắt-dự-án)
- [Luồng dữ liệu chi tiết](#luồng-dữ-liệu-chi-tiết)
- [Cấu hình](#cấu-hình)
- [Debug thường gặp](#debug-thường-gặp)
- [Test](#test)

---

## Kiến Trúc Hệ Thống

```text
data/*
  → ingestion.simulator
  → Kafka  social.<platform>.posts
  → batch.object_store_writer
  → MinIO  s3a://social-lake/data/raw/<platform>/...
  → batch.spark_batch_job
  → MinIO  s3a://social-lake/data/batch_views/...
  → batch.index_batch_views
  → Elasticsearch  social_batch_views

Kafka  social.<platform>.posts
  → speed.streaming_job  +  speed.nlp_pipeline
  → Redis       rt:stats:* / rt:hashtags:*
  → Elasticsearch  social_realtime_views
  → Kafka       social.enriched.posts

Elasticsearch + Redis
  → serving.merge_service
  → api.main  (FastAPI)
  → dashboard/index.html
```

| Layer | Thành phần | Vai trò |
|---|---|---|
| Ingestion | `ingestion.simulator` | Đọc sample data, normalize, publish Kafka |
| Raw/Object | `batch.object_store_writer` | Consume Kafka → raw Parquet → MinIO |
| Batch | `batch.spark_batch_job` | Đọc raw Parquet, tạo batch views |
| Speed | `speed.streaming_job` | Consume Kafka, enrich NLP, ghi Redis + ES |
| Serving | `serving.merge_service` | Merge batch + realtime cho API |

---

## Service Và Cổng

### Core stack (mặc định)

| Service | Vai trò | URL / Cổng |
|---|---|---|
| Kafka | Message broker | `localhost:9092` |
| MinIO Console | Object storage UI | http://localhost:9001 |
| Spark Master | Cluster UI | http://localhost:8081 |
| Spark Worker | Worker UI | http://localhost:8083 |
| Redis | Realtime cache | `localhost:6379` |
| Elasticsearch | Serving indexes | http://localhost:9200 |
| FastAPI | Serving API | http://localhost:8000 |
| Dashboard | UI tĩnh | http://localhost:8084 |

MinIO mặc định: `minioadmin` / `minioadmin`

### Profile bổ sung

| Profile | Service | URL / Cổng |
|---|---|---|
| `debug` | Kafka UI | http://localhost:8080 |
| `debug` | Kibana | http://localhost:5601 |
| `orchestration` | Airflow | http://localhost:8082 |
| `warehouse` | ClickHouse | http://localhost:8123 (Native: 9002) |
| `monitoring` | Prometheus | http://localhost:9090 |
| `monitoring` | Grafana | http://localhost:3000 |
| `enrichment` | Cassandra | `localhost:9042` |
| `anomaly` | Cassandra + anomaly detector | `localhost:9042` |

---

## Yêu Cầu Môi Trường

- Docker Engine và Docker Compose plugin (`docker compose`, không phải `docker-compose` cũ)
- RAM trống: tối thiểu **6 GB** cho core stack, **8 GB+** khi bật thêm profile
- Port chưa bị chiếm: `8000`, `8081`, `8083`, `8084`, `9000`, `9001`, `9200`, `9092`, `6379`

---

## Chạy Dự Án Từ Đầu

### 1. Chuẩn bị

```bash
git clone <repo-url>
cd social-pipeline
cp .env.example .env   # tùy chọn — giữ default nếu không cần đổi port/credential

# Tải và giải nén tự động toàn bộ dữ liệu mẫu lớn từ Google Drive
make download-data
```

### 2. Build image

```bash
docker compose build
```

Chỉ build service cụ thể khi sửa code:

```bash
# Sửa API/serving code
docker compose build api serving-init

# Sửa Spark/batch/speed code
docker compose build spark-master spark-worker speed
```

### 3. Khởi động hạ tầng lõi

```bash
make core-up
```

Lệnh này start: Zookeeper, Kafka, Kafka-init, MinIO, MinIO-init, Redis, Elasticsearch, serving-init.

Kiểm tra tất cả đã `healthy`:

```bash
make ps
```

Kiểm tra Elasticsearch index đã được tạo:

```bash
docker compose exec -T elasticsearch \
  curl -fsS http://localhost:9200/_cat/indices?v
```

### 4. Khởi động Spark, speed layer, API và dashboard

```bash
make app-up
```

Kiểm tra API:

```bash
curl -fsS http://localhost:8000/health
# {"status":"ok"}
```

### 5. Bật Airflow orchestration

Airflow là orchestrator chính điều phối batch pipeline. **Khuyến nghị bật Airflow** thay vì chạy Spark thủ công.

```bash
make orchestration
```

Truy cập Airflow UI: http://localhost:8082 — `admin` / `admin`

### 6. Replay dữ liệu mẫu

```bash
make replay
```

Lệnh này start các container replay để publish sample messages vào Kafka. Đợi **30–60 giây** để `object-store-writer` và `speed` consume và flush dữ liệu.

### 7. Trigger batch pipeline

Vào Airflow UI, **unpause** DAG `social_lambda_batch_pipeline` rồi bấm **Trigger DAG** (▶).

Hoặc trigger bằng CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger social_lambda_batch_pipeline
```

DAG sẽ:
1. Kiểm tra raw data mới trên MinIO
2. Chạy Spark batch job
3. Index batch views lên Elasticsearch
4. Đánh dấu dữ liệu đã xử lý
5. Gửi Slack alert (nếu cấu hình)

### 8. Xem kết quả

Mở dashboard: http://localhost:8084

Kiểm tra nhanh qua API:

```bash
curl -fsS "http://localhost:8000/api/v1/posts?limit=5&start=2023-01-01T00:00:00Z"
curl -fsS "http://localhost:8000/api/v1/stats/realtime"
```

> **Lưu ý:** Dữ liệu mẫu có timestamp trong quá khứ. Luôn truyền `start=2023-01-01T00:00:00Z` khi query posts/trend để chắc chắn thấy data.

### 9. Nạp dữ liệu vào ClickHouse Data Warehouse (Tùy chọn)

Nếu bạn muốn phân tích dữ liệu qua kho lưu trữ ClickHouse:

1. Khởi động ClickHouse server:
   ```bash
   make warehouse-stack
   ```
2. Chạy Spark job nạp dữ liệu tổng hợp từ MinIO sang ClickHouse:
   ```bash
   make warehouse
   ```


---

## Replay Và Pipeline Thủ Công

### Replay qua Docker (khuyến nghị)

```bash
make replay
```

### Replay trực tiếp trên host (dev)

```bash
make replay-raw
```

Mỗi platform dùng port Prometheus riêng (9101/9102/9103) để tránh conflict. Dừng khi cần:

```bash
make kill-simulators
```

### Chạy batch thủ công (fallback, không khuyến nghị trong production)

Dùng khi debug hoặc Airflow chưa bật:

```bash
# Spark batch job
make spark-batch

# Index batch views vào Elasticsearch
make index-batch-docker
```

---

## Kiểm Tra Kết Quả

```bash
# Health check
curl -fsS http://localhost:8000/health

# Elasticsearch indices
docker compose exec -T elasticsearch \
  curl -fsS http://localhost:9200/_cat/indices?v

# API endpoints
curl -fsS "http://localhost:8000/api/v1/posts?limit=5&start=2023-01-01T00:00:00Z" | python3 -m json.tool
curl -fsS "http://localhost:8000/api/v1/sentiment/trend?start=2023-01-01T00:00:00Z" | python3 -m json.tool
curl -fsS "http://localhost:8000/api/v1/hashtags/top?window_hours=24&top_n=10" | python3 -m json.tool
curl -fsS "http://localhost:8000/api/v1/stats/realtime" | python3 -m json.tool

# Raw data trên MinIO
make minio-ls-raw

# Log service chính
make logs
make logs-all
```

---

## Reset Và Chạy Lại Từ Đầu

Xóa toàn bộ dữ liệu volume rồi khởi động lại từ đầu:

```bash
# 1. Dừng và xóa volume
make clean

# 2. Khởi động lại hạ tầng
make core-up
make app-up
make orchestration

# 3. Replay dữ liệu mẫu
make replay

# 4. Đợi 30-60 giây, sau đó chạy batch
make spark-batch && make index-batch-docker
```

---

## Tắt Dự Án

```bash
# Dừng sạch tất cả các container của mọi profiles (giữ lại dữ liệu volume)
make down

# Dừng sạch container của mọi profiles và xóa toàn bộ dữ liệu volume
make clean
```

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

`batch.object_store_writer` ghi Parquet partition theo platform/ngày:

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

### Serving merge

| Endpoint | Logic |
|---|---|
| `/api/v1/posts` | Realtime nếu query chạm window 24h, batch nếu ngoài window; dedupe theo `post_id` |
| `/api/v1/sentiment/trend` | Merge batch/realtime theo time bucket UTC |
| `/api/v1/hashtags/top` | Ưu tiên Redis trong window, fallback batch ES |
| `/api/v1/stats/realtime` | Đọc Redis, tính `avg_sentiment` |

---

## Cấu Hình

Biến môi trường trong `.env.example` và `docker-compose.yml`:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `API_HOST_PORT` | `8000` | Port FastAPI trên host |
| `DASHBOARD_HOST_PORT` | `8084` | Port dashboard |
| `MINIO_API_HOST_PORT` | `9000` | Port MinIO S3 API |
| `MINIO_CONSOLE_HOST_PORT` | `9001` | Port MinIO Console |
| `ES_HOST_PORT` | `9200` | Port Elasticsearch |
| `SPARK_MASTER_UI_HOST_PORT` | `8081` | Spark master UI |
| `REPLAY_RATE_PER_SEC` | `20` | Tốc độ replay (records/s) |
| `REPLAY_DEDUPE` | `true` | Dedupe khi replay |
| `STREAM_STARTING_OFFSETS` | `latest` | Offset bắt đầu streaming |
| `STREAM_TRIGGER_SECS` | `5` | Trigger interval Spark Streaming (giây) |
| `SPEED_WRITE_BATCH_SIZE` | `500` | Số record ghi mỗi micro-batch |
| `CONSUMER_FLUSH_SIZE` | `500` | Số record flush raw Parquet |
| `CONSUMER_FLUSH_INTERVAL` | `30` | Flush interval raw writer (giây) |
| `BATCH_INPUT_PARTITIONS` | `64` | Partition input batch job |
| `BATCH_SHUFFLE_PARTITIONS` | `64` | Shuffle partition Spark SQL |
| `REALTIME_WINDOW_HOURS` | `24` | Window realtime khi serving merge |
| `NLP_MODEL_NAME` | `distilbert-base-uncased-finetuned-sst-2-english` | Sentiment model |

---

## Debug Thường Gặp

### API trả về rỗng

```bash
# Kiểm tra index ES có data không
docker compose exec -T elasticsearch \
  curl -fsS http://localhost:9200/_cat/indices?v

# Nếu social_batch_views rỗng → chạy lại batch
make spark-batch && make index-batch-docker

# Nếu social_realtime_views rỗng → kiểm tra speed layer
docker compose logs --tail=200 speed
make replay
```

### Batch job báo không có raw data

```bash
# Kiểm tra object-store-writer đã flush chưa
docker compose logs --tail=200 object-store-writer

# Kiểm tra MinIO
make minio-ls-raw
```

Nguyên nhân thường gặp: replay chưa chạy, hoặc writer chưa đủ thời gian flush (cần 30–60 giây sau replay).

### Simulator không dừng được (port conflict)

```bash
make kill-simulators
```

### Dashboard hiện dữ liệu cũ

```bash
docker compose build api serving-init
docker compose up -d --force-recreate serving-init api
```

Nếu vẫn cũ → xóa browser cache hoặc thử incognito.

### Elasticsearch status yellow

Bình thường với single-node local — replica không được assign. Index vẫn query được, không cần xử lý.

### Spark batch chậm

Giảm partition cho máy yếu:

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/spark_batch_job.py \
  --input-partitions 32 \
  --shuffle-partitions 32
```

### Airflow DAG không tìm thấy

```bash
# Kiểm tra DAG đã load chưa
docker compose exec airflow-scheduler airflow dags list | grep social

# Xem import errors
docker compose exec airflow-scheduler airflow dags list-import-errors
```

---

## Test

### Chạy Unit Tests

```bash
.venv/bin/pytest tests/unit
```

Chạy theo file unit test cụ thể:

```bash
.venv/bin/pytest tests/unit/test_anomaly_detector.py
.venv/bin/pytest tests/unit/test_serving_merge.py
.venv/bin/pytest tests/unit/test_realtime_stores.py
```

### Chạy Integration & E2E Tests (Yêu cầu Docker stack đang chạy)

Để chạy kiểm thử tích hợp (Integration) và E2E trên local, sử dụng lệnh:

```bash
RUN_INTEGRATION=1 RUN_E2E=1 API_URL=http://localhost:8000 .venv/bin/pytest
```

Hoặc chạy integration/e2e qua Docker:

```bash
docker compose run --rm --no-deps \
  -e RUN_E2E=1 \
  -e API_URL=http://api:8000 \
  -v "$PWD:/app" \
  api \
  python -m pytest tests/e2e/test_pipeline_contract.py
```

Compile nhanh kiểm tra syntax:

```bash
python3 -m py_compile \
  batch/spark_batch_job.py \
  batch/index_batch_views.py \
  serving/merge_service.py \
  serving/es_indexer.py \
  speed/realtime_stores.py \
  speed/streaming_job.py
```

---

## Makefile Reference

| Lệnh | Mô tả |
|---|---|
| `make up` | Build và start toàn bộ core stack |
| `make core-up` | Start hạ tầng lõi (không có Spark/speed/API) |
| `make app-up` | Start Spark, speed layer, API và dashboard |
| `make orchestration` | Start Airflow |
| `make monitoring` | Start Prometheus + Grafana |
| `make debug` | Start Kafka UI + Kibana |
| `make warehouse-stack` | Start ClickHouse |
| `make warehouse` | Chạy Spark job nạp dữ liệu từ MinIO vào ClickHouse |
| `make enrichment` | Start Cassandra |
| `make anomaly` | Start Cassandra + anomaly detector |
| `make replay` | Replay qua Docker container |
| `make replay-raw` | Replay trực tiếp trên host (dev) |
| `make kill-simulators` | Dừng tất cả simulator đang chạy |
| `make spark-batch` | Chạy Spark batch job |
| `make index-batch-docker` | Index batch views vào Elasticsearch |
| `make minio-ls-raw` | Liệt kê raw data trên MinIO |
| `make logs` | Xem log object-store-writer, speed, api |
| `make logs-all` | Xem log chi tiết tất cả service |
| `make ps` | Trạng thái container |
| `make down` | Tắt container, giữ volume |
| `make clean` | Tắt container và xóa toàn bộ volume |
| `make test` | Chạy unit tests |
| `make build` | Build image |
| `make download-data` | Tải và giải nén tự động dữ liệu mẫu lớn từ Drive |
