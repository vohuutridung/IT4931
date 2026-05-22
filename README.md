# Social Media Lambda Pipeline

Pipeline xử lý dữ liệu mạng xã hội theo mô hình Lambda Architecture. Dự án chạy local bằng Docker Compose, nhận dữ liệu mẫu Reddit/Facebook/Instagram, ghi raw data vào MinIO, tạo batch views bằng Spark, xử lý realtime bằng Spark Structured Streaming, lưu serving data vào Elasticsearch/Redis và expose qua FastAPI + dashboard.

Tài liệu này mô tả cách chạy thực tế của repo hiện tại, không dựa máy móc vào README cũ.

## Mục Lục

- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Service và cổng](#service-và-cổng)
- [Yêu cầu môi trường](#yêu-cầu-môi-trường)
- [Chạy dự án từ đầu](#chạy-dự-án-từ-đầu)
- [Chạy lại batch layer](#chạy-lại-batch-layer)
- [Kiểm tra kết quả](#kiểm-tra-kết-quả)
- [Xóa dữ liệu để chạy lại từ đầu](#xóa-dữ-liệu-để-chạy-lại-từ-đầu)
- [Tắt dự án](#tắt-dự-án)
- [Luồng dữ liệu chi tiết](#luồng-dữ-liệu-chi-tiết)
- [Cấu hình quan trọng](#cấu-hình-quan-trọng)
- [Debug thường dùng](#debug-thường-dùng)
- [Test](#test)

## Kiến Trúc Hệ Thống

```text
data/*
  -> ingestion.simulator
  -> Kafka social.<platform>.posts
  -> batch.object_store_writer
  -> MinIO s3a://social-lake/data/raw/<platform>/...
  -> batch.spark_batch_job
  -> MinIO s3a://social-lake/data/batch_views/...
  -> batch.index_batch_views
  -> Elasticsearch social_batch_views

Kafka social.<platform>.posts
  -> speed.streaming_job
  -> speed.nlp_pipeline
  -> Redis rt:stats:* / rt:hashtags:* / rt:recent:*
  -> Elasticsearch social_realtime_views
  -> Kafka social.enriched.posts

Elasticsearch + Redis
  -> serving.merge_service
  -> api.main
  -> dashboard/index.html
```

Các layer chính:

- **Ingestion layer**: đọc sample data trong `data/`, normalize theo từng platform, publish vào Kafka.
- **Raw/object-store layer**: `batch.object_store_writer` consume source topics và ghi raw Parquet vào MinIO.
- **Batch layer**: Spark đọc raw Parquet, tạo các batch views như daily stats, top hashtags, sentiment time series, top posts.
- **Speed layer**: Spark Structured Streaming consume Kafka, enrich sentiment/keyword/language, ghi realtime views vào Redis và Elasticsearch.
- **Serving layer**: FastAPI merge batch + realtime data để dashboard/API đọc thống nhất.

## Service Và Cổng

Core stack mặc định:

| Service | Vai trò | URL/Cổng |
| --- | --- | --- |
| Kafka | Message broker | `localhost:9092` |
| MinIO | Object storage cho raw data và batch views | http://localhost:9001 |
| Spark Master | Spark cluster UI | http://localhost:8081 |
| Spark Worker | Worker UI | http://localhost:8083 |
| Redis | Realtime aggregate cache | `localhost:6379` |
| Elasticsearch | Serving indexes | http://localhost:9200 |
| FastAPI | Serving API | http://localhost:8000 |
| Dashboard | UI tĩnh | http://localhost:8084 |

Thông tin đăng nhập MinIO:

```text
Username: minioadmin
Password: minioadmin
```

Service theo profile:

| Profile | Service | URL/Cổng |
| --- | --- | --- |
| `debug` | Kafka UI | http://localhost:8080 |
| `debug` | Kibana | http://localhost:5601 |
| `orchestration` | Airflow | http://localhost:8082 |
| `warehouse` | ClickHouse | http://localhost:8123 |
| `monitoring` | Prometheus | http://localhost:9090 |
| `monitoring` | Grafana | http://localhost:3000 |
| `anomaly` | Cassandra + anomaly detector | `localhost:9042` |

## Yêu Cầu Môi Trường

- Docker Engine.
- Docker Compose plugin (`docker compose`, không phải `docker-compose` cũ).
- RAM khuyến nghị:
  - Core stack: 6 GB trống.
  - Core + debug/orchestration/monitoring: 8 GB trở lên.
- Port host mặc định chưa bị chiếm: `8000`, `8081`, `8083`, `8084`, `9000`, `9001`, `9200`, `9092`, `6379`.

Tạo file `.env` nếu cần override cấu hình:

```bash
cp .env.example .env
```

Không bắt buộc tạo `.env` nếu dùng toàn bộ default.

## Chạy Dự Án Từ Đầu

Phần này dành cho người mới clone repo lần đầu và muốn chạy toàn bộ luồng local để thấy dashboard có dữ liệu.

### 1. Clone repo và tạo file env

```bash
git clone <repo-url>
cd social-pipeline
cp .env.example .env
```

Nếu không cần override port hoặc credential local, có thể bỏ qua bước `cp .env.example .env` vì Docker Compose đã có default. Tuy vậy, nên tạo `.env` để cấu hình chạy nhất quán giữa các máy.

### 2. Build image

```bash
docker compose build
```

Nếu chỉ sửa API/serving code:

```bash
docker compose build api serving-init
```

Nếu sửa Spark/batch/speed code:

```bash
docker compose build spark-master spark-worker speed
```

### 3. Start core infrastructure

Ngắn gọn (dùng Makefile):

```bash
make core-up
```

Lệnh này tương đương với:

```bash
docker compose up -d zookeeper kafka kafka-init minio minio-init redis elasticsearch serving-init
```

Kiểm tra container:

```bash
make ps
```

Kiểm tra Elasticsearch index đã được tạo:

```bash
docker compose exec -T elasticsearch curl -fsS http://localhost:9200/_cat/indices?v
```

### 4. Start Spark, speed layer, object-store writer, API và dashboard


```bash
docker compose up -d spark-master spark-worker speed object-store-writer api dashboard
```

Kiểm tra API:

```bash
curl -fsS http://localhost:8000/health
```

Kết quả đúng:

```json
{"status":"ok"}
```

### 5. Chạy pipeline

Trong dự án này, luồng production và toàn bộ orchestration được điều phối bởi Airflow. Phần hướng dẫn dưới đây là cách chính thức để chạy pipeline; các lệnh `spark-submit` thủ công chỉ là fallback cho debug và không được khuyến khích khi vận hành.


1) Bật Airflow orchestration (bắt buộc):

```bash
make orchestration
```

- `airflow-init` khởi tạo metadata DB, tạo user admin và đăng ký connection `spark_default`.
- Truy cập Airflow UI: `http://localhost:8082` (Username: `admin`, Password: `admin`).
- Trong Airflow UI bật (unpause) DAG `social_lambda_batch_pipeline` hoặc trigger thủ công DAG này.


2) Replay sample data (nếu cần) — trước khi trigger DAG:

```bash
make replay
```

- Lệnh trên khởi các container replay để publish sample messages vào Kafka. Đợi vài chục giây để `object-store-writer` và `speed` consume và flush raw Parquet/realtime aggregates.


Kiểm tra logs (tuỳ chọn):

```bash
make logs
```

3) Trigger DAG

- Vào Airflow UI, trigger DAG `social_lambda_batch_pipeline` hoặc đợi DAG chạy theo lịch (5 phút 1 lần).
- DAG sẽ kiểm tra dữ liệu raw mới trên MinIO, chạy Spark batch, refresh serving layer và đánh dấu dữ liệu đã xử lý.

Ghi chú vận hành:

- Không nên chạy `spark_batch_job.py` hay `index_batch_views.py` thủ công trong môi trường vận hành; chỉ dùng khi debug offline.
- Nếu cần chạy thủ công (fallback), xem phần "Fallback: chạy thủ công" bên dưới.

### 6. Fallback: chạy thủ công (không khuyến khích)

Chỉ dùng khi debug hoặc môi trường dev đơn giản. Thực hiện các bước theo thứ tự:

- Replay dữ liệu mẫu vào Kafka (nếu cần):

```bash
make replay
```

- Chạy batch Spark job (reads raw Parquet từ MinIO):

```bash
make spark-batch
```

- Index batch views vào Elasticsearch:

```bash
make index-batch-docker
```

### 7. Mở dashboard và kiểm tra API

Truy cập dashboard:

```text
http://localhost:8084
```

Các endpoint quan trọng:

```text
http://localhost:8000/health
http://localhost:8000/api/v1/posts
http://localhost:8000/api/v1/sentiment/trend
http://localhost:8000/api/v1/hashtags/top
http://localhost:8000/api/v1/stats/realtime
```

Kiểm tra nhanh bằng API:

```bash
curl -fsS http://localhost:8000/health
curl -fsS "http://localhost:8000/api/v1/posts?limit=5&start=2023-01-01T00:00:00Z"
curl -fsS "http://localhost:8000/api/v1/sentiment/trend?start=2023-01-01T00:00:00Z"
```

Ghi chú: dữ liệu mẫu có timestamp trải dài trong quá khứ, vì vậy khi kiểm tra posts/trend nên truyền `start=2023-01-01T00:00:00Z` để chắc chắn nhìn thấy dữ liệu.

## Chạy Lại Batch Layer

Dùng khi đã có raw data trong MinIO và chỉ muốn build lại batch views/index.

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/spark_batch_job.py \
  --input-partitions 64 \
  --shuffle-partitions 64
```

Sau đó index lại:

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/index_batch_views.py
```

Recreate API nếu vừa sửa serving/API code:

```bash
docker compose build api serving-init
docker compose up -d --force-recreate serving-init api
```

Recreate Spark services nếu vừa sửa Spark/speed/batch code:

```bash
docker compose build spark-master spark-worker speed
docker compose up -d --force-recreate spark-master spark-worker speed
```

## Kiểm Tra Kết Quả

Health:

```bash
curl -fsS http://localhost:8000/health
```

Elasticsearch indices:

```bash
docker compose exec -T elasticsearch curl -fsS http://localhost:9200/_cat/indices?v
```

Sample API check:

```bash
curl -fsS "http://localhost:8000/api/v1/sentiment/trend" | python -m json.tool
curl -fsS "http://localhost:8000/api/v1/posts?limit=5&start=2023-01-01T00:00:00Z" | python -m json.tool
curl -fsS "http://localhost:8000/api/v1/hashtags/top?window_hours=24&top_n=10" | python -m json.tool
curl -fsS "http://localhost:8000/api/v1/stats/realtime" | python -m json.tool
```

Kiểm tra raw data trong MinIO:

```bash
docker compose run --rm --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls -r local/social-lake/data/raw | head'
```

Kiểm tra batch views trong MinIO:

```bash
docker compose run --rm --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls -r local/social-lake/data/batch_views | head'
```

Log các service chính:

```bash
docker compose logs --tail=100 api
docker compose logs --tail=100 speed
docker compose logs --tail=100 object-store-writer
docker compose logs --tail=100 elasticsearch
```

## Xóa Dữ Liệu Để Chạy Lại Từ Đầu

Khi dữ liệu đã chạy hết và muốn xem pipeline chạy lại từ đầu, dùng luồng dưới đây. Luồng này xóa dữ liệu Docker volume của dự án rồi replay lại sample data. Không cần `docker compose build` nếu không sửa Dockerfile hoặc requirements.

```bash
docker compose down -v --remove-orphans
docker compose up -d zookeeper kafka kafka-init minio minio-init redis elasticsearch serving-init
docker compose up -d spark-master spark-worker speed object-store-writer api dashboard
docker compose --profile replay up replay-reddit replay-facebook replay-instagram
```

Sau replay, đợi khoảng 30-60 giây để `object-store-writer` và `speed` xử lý dữ liệu. Sau đó chạy batch job và index batch views:

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/spark_batch_job.py \
  --input-partitions 64 \
  --shuffle-partitions 64

docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/index_batch_views.py
```

Mở lại dashboard:

```text
http://localhost:8084
```

## Tắt Dự Án

Tắt container nhưng giữ dữ liệu volume:

```bash
docker compose down
```

Tắt cả profile đang bật:

```bash
docker compose --profile replay --profile debug --profile orchestration --profile monitoring --profile warehouse --profile anomaly down
```

Tắt và xóa volume dữ liệu:

```bash
docker compose down -v --remove-orphans
```

Xóa image tự build nếu muốn build lại hoàn toàn:

```bash
docker image rm social-python:0.1.0 social-api:0.1.0 social-spark:3.5.3 social-ml:0.1.0
```

## Luồng Dữ Liệu Chi Tiết

### Kafka topics

`kafka-init` tạo các topic:

- `social.reddit.posts`
- `social.facebook.posts`
- `social.instagram.posts`
- `social.enriched.posts`
- `social.dlq`

Source topics chứa post đã normalize. `social.enriched.posts` chứa post sau NLP enrichment từ speed layer.

### Raw data

`batch.object_store_writer` consume source topics và ghi raw Parquet vào:

```text
s3a://social-lake/data/raw/<platform>/...
```

Raw schema không chứa enrichment fields. Sentiment/keyword/language được tính ở batch job hoặc speed layer.

### Batch views

`batch.spark_batch_job` tạo:

- `platform_daily_stats`
- `top_hashtags_weekly`
- `author_activity`
- `sentiment_time_series`
- `top_posts`

Output:

```text
s3a://social-lake/data/batch_views/<view_name>
```

`batch.index_batch_views` đọc các view này và index vào:

```text
social_batch_views
```

### Realtime views

`speed.streaming_job` consume source Kafka topics, gọi `speed.nlp_pipeline.enrich_post()`, rồi ghi:

- Redis:
  - `rt:stats:<platform>:<hour>`
  - `rt:hashtags:<platform>:<hour>`
  - `rt:hashtags:__all__:<hour>`
  - `rt:recent:<platform>`
- Elasticsearch:
  - `social_realtime_views`
- Kafka:
  - `social.enriched.posts`

### Serving merge

`serving.merge_service` merge dữ liệu:

- `/api/v1/posts`: lấy realtime nếu query chạm realtime window, lấy batch nếu query vượt ngoài realtime window, sort theo `event_ts desc`, dedupe theo `post_id`.
- `/api/v1/sentiment/trend`: merge batch/realtime theo time bucket đã normalize UTC để tránh duplicate point cùng giờ.
- `/api/v1/hashtags/top`: ưu tiên Redis trong window, fallback batch.
- `/api/v1/stats/realtime`: đọc Redis stats và tính `avg_sentiment`.

## Cấu Hình Quan Trọng

Các biến nằm trong `.env.example` và `docker-compose.yml`.

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `API_HOST_PORT` | `8000` | Port FastAPI trên host |
| `DASHBOARD_HOST_PORT` | `8084` | Port dashboard |
| `MINIO_API_HOST_PORT` | `9000` | Port MinIO API |
| `MINIO_CONSOLE_HOST_PORT` | `9001` | Port MinIO Console |
| `ES_HOST_PORT` | `9200` | Port Elasticsearch |
| `SPARK_MASTER_UI_HOST_PORT` | `8081` | Spark master UI |
| `SPARK_WORKER_UI_HOST_PORT` | `8083` | Spark worker UI |
| `REPLAY_RATE_PER_SEC` | `20` | Tốc độ replay sample data |
| `REPLAY_DEDUPE` | `true` | Dedupe khi replay |
| `STREAM_STARTING_OFFSETS` | `latest` | Offset bắt đầu của speed streaming |
| `STREAM_TRIGGER_SECS` | `5` | Trigger interval của Spark streaming |
| `SPEED_WRITE_BATCH_SIZE` | `500` | Số record ghi mỗi lần trong speed layer |
| `CONSUMER_FLUSH_SIZE` | `500` | Số record flush raw Parquet mỗi lần |
| `CONSUMER_FLUSH_INTERVAL` | `30` | Flush interval raw writer, giây |
| `BATCH_INPUT_PARTITIONS` | `64` | Số partition input batch job |
| `BATCH_SHUFFLE_PARTITIONS` | `64` | Số shuffle partition Spark SQL |
| `REALTIME_WINDOW_HOURS` | `24` | Window realtime khi serving merge |

## Debug Thường Dùng

### API trả rỗng

Kiểm tra Elasticsearch có data:

```bash
docker compose exec -T elasticsearch curl -fsS http://localhost:9200/_cat/indices?v
```

Nếu `social_batch_views` chưa có docs, chạy lại batch job và indexer.

Nếu `social_realtime_views` chưa có docs, kiểm tra `speed` và replay data:

```bash
docker compose logs --tail=200 speed
docker compose --profile replay up replay-reddit replay-facebook replay-instagram
```

### Batch job báo không có raw data

Raw writer có thể chưa flush hoặc replay chưa chạy xong.

Kiểm tra log:

```bash
docker compose logs --tail=200 object-store-writer
```

Kiểm tra MinIO:

```bash
docker compose run --rm --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls -r local/social-lake/data/raw | head'
```

### Dashboard vẫn hiện dữ liệu cũ

Các nguyên nhân thường gặp:

- API container chưa recreate sau khi sửa code.
- Elasticsearch index vẫn còn dữ liệu cũ.
- Browser cache.

Lệnh xử lý nhanh:

```bash
docker compose build api serving-init
docker compose up -d --force-recreate serving-init api
```

Nếu muốn dữ liệu sạch, dùng reset mức 2 hoặc mức 3.

### Elasticsearch yellow

Local single-node Elasticsearch thường có status `yellow` vì replica không được assign. Với dev local, điều này chấp nhận được nếu index vẫn query được.

### Spark job chậm hoặc quá nhiều task

Giảm partition cho local:

```bash
docker compose exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/spark_batch_job.py \
  --input-partitions 32 \
  --shuffle-partitions 32
```

Tăng lên `128` hoặc hơn nếu dataset lớn và máy đủ CPU/RAM.

## Test

Chạy unit tests:

```bash
uv run pytest tests/unit
```

Chạy test theo file:

```bash
uv run pytest tests/unit/test_serving_merge.py
uv run pytest tests/unit/test_es_indexer.py
uv run pytest tests/unit/test_realtime_stores.py
```

Compile nhanh các file Python chính:

```bash
python3 -m py_compile \
  batch/spark_batch_job.py \
  batch/index_batch_views.py \
  serving/merge_service.py \
  serving/es_indexer.py \
  speed/realtime_stores.py \
  speed/streaming_job.py
```

Một số integration/e2e test yêu cầu stack Docker đang chạy.

## Build Theo Profile Tùy Chọn

Debug UI:

```bash
docker compose --profile debug up -d kafka-ui kibana
```

Airflow:

```bash
docker compose --profile orchestration up -d airflow-init airflow-webserver airflow-scheduler
```

Airflow login:

```text
Username: admin
Password: admin
```

Monitoring:

```bash
docker compose --profile monitoring up -d prometheus grafana
```

Warehouse ClickHouse:

```bash
docker compose --profile warehouse up -d clickhouse
```

Anomaly detector:

```bash
docker compose --profile anomaly up -d cassandra ml-anomaly
```

## Ghi Chú Vận Hành

- `Dockerfile.spark` dùng `apache/spark:3.5.3`. Không cần nâng version nếu không có lý do cụ thể; 3.5.x ổn cho local và tương thích với JAR Kafka/S3A hiện tại.
- Raw writer consume source topics, còn enrichment realtime nằm ở `social.enriched.posts` và `social_realtime_views`.
- Batch job tự tính `sentiment_score` nếu raw chưa có field này.
- Serving layer đã xử lý duplicate time bucket giữa batch/realtime cho sentiment trend.
