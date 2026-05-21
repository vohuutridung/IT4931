# Social Media Lambda Pipeline

Dự án xây dựng pipeline xử lý dữ liệu mạng xã hội theo mô hình Lambda Architecture. Pipeline nhận dữ liệu từ Reddit, Facebook và Instagram, chuẩn hóa về một schema chung, ghi dữ liệu thô vào MinIO, tạo batch views bằng Spark, load dữ liệu phân tích vào ClickHouse warehouse, xử lý realtime bằng Spark Streaming, làm giàu NLP, lưu serving data vào Elasticsearch/Redis/Cassandra và cung cấp API/dashboard để truy vấn.

## Mục Tiêu

- Chuẩn hóa dữ liệu social media từ nhiều nền tảng về một canonical schema.
- Publish dữ liệu chuẩn vào Kafka và record lỗi vào DLQ.
- Lưu raw data dạng Parquet trên MinIO, partition theo `platform` và `date`.
- Tạo batch views bằng Spark theo hướng idempotent.
- Xử lý realtime stream, enrich sentiment/keyword/entity/language.
- Serving qua FastAPI, Elasticsearch, Redis và Cassandra.
- Theo dõi hệ thống bằng Prometheus/Grafana.
- Điều phối batch pipeline bằng Airflow.
- Cung cấp test unit, integration, e2e, load test và script validation theo từng phase SOP.

## Kiến Trúc Tổng Quan

```text
data/*
  -> ingestion/simulator.py
  -> Kafka social.<platform>.posts
  -> batch/object_store_writer.py
  -> MinIO s3a://social-lake/data/raw/platform/year=yyyy/month=mm/day=dd
  -> batch/spark_batch_job.py
  -> MinIO s3a://social-lake/data/batch_views
  -> batch/index_batch_views.py
  -> Elasticsearch social_batch_views
  -> warehouse/clickhouse_loader.py
  -> ClickHouse social_warehouse fact/dim tables

Kafka social.<platform>.posts
  -> speed/streaming_job.py
  -> speed/nlp_pipeline.py
  -> Redis rt:*
  -> Cassandra enrichments
  -> Elasticsearch social_realtime_views

serving/merge_service.py
  -> api/main.py
  -> dashboard/index.html

orchestration/dags/batch_pipeline_dag.py
  -> Airflow hourly/manual batch refresh

ml/anomaly_detector.py
  -> Cassandra alerts
```

## Canonical Post Schema

Sau khi normalize, mỗi post hợp lệ có schema chính:

```json
{
  "post_id": "reddit_abc123",
  "platform": "reddit",
  "source_id": "datascience",
  "author_id": "sha256_hash",
  "content": "Post title and body",
  "title": "Post title",
  "media_urls": [],
  "hashtags": ["data"],
  "comments": [
    {
      "comment_id": "c1",
      "post_id": "reddit_abc123",
      "parent_id": "t3_abc123",
      "author_id": "t2_comment_author",
      "author": "commenter",
      "text": "Great update",
      "likes": 3,
      "depth": 0,
      "created_at": 1700000010000,
      "extra": "{\"reply_count\": 0}"
    }
  ],
  "created_at": "2023-11-14T22:13:20Z",
  "ingested_at": "2026-05-20T10:00:00Z",
  "metrics": {
    "likes": 7,
    "comments": 2,
    "shares": 1,
    "views": 0
  }
}
```

Ghi chú:

- `author_id` được hash SHA-256 để tránh lưu định danh raw.
- `platform` là một trong `reddit`, `facebook`, `instagram`.
- `comments` là danh sách comment đã normalize. Với post không có comment hoặc platform không cung cấp comment tree, field này là `[]`.
- `metrics.comments` là số lượng comment của post.
- `metrics` luôn gồm đủ `likes`, `comments`, `shares`, `views`.
- Record không hợp lệ được publish vào Kafka topic `social.dlq`.

## Cấu Trúc Thư Mục

```text
.
├── api/                  FastAPI endpoints và Prometheus metrics endpoint
├── batch/                Object store writer, Spark batch job, Elasticsearch batch indexer
├── config/               Cấu hình tập trung từ environment variables
├── dashboard/            Static dashboard chạy bằng nginx
├── data/                 Sample data Reddit/Facebook/Instagram
├── ingestion/            Simulator và normalizers cho từng platform
├── ml/                   Anomaly detection service
├── monitoring/           Prometheus config
├── orchestration/        Airflow DAGs
├── scripts/              Validation scripts theo từng phase SOP
├── serving/              Elasticsearch mapping/indexing và merge query service
├── speed/                Spark streaming, NLP enrichment, realtime stores
├── tests/                Unit, integration, e2e và load tests
├── Dockerfile            Python service image
├── Dockerfile.airflow    Airflow image
├── Dockerfile.spark      Spark image
├── docker-compose.yml    Local full stack
├── Makefile              Lệnh tiện ích
└── requirements.txt      Python dependencies
```

## Thành Phần Dịch Vụ

Docker Compose mặc định chỉ khởi động core pipeline để nhẹ máy hơn. Các service
monitoring/debug/orchestration/warehouse được đưa vào profile riêng.

| Service | Vai trò | URL/Cổng |
| --- | --- | --- |
| Kafka | Message broker cho ingestion, batch và speed layer | `localhost:9092` |
| MinIO | Raw data và batch view storage | http://localhost:9001 |
| Spark Master | Spark cluster master | http://localhost:8081 |
| Spark Worker | Spark worker UI | http://localhost:8083 |
| Redis | Realtime aggregate cache | `localhost:6379` |
| Elasticsearch | Serving indexes | http://localhost:9200 |
| FastAPI | Serving API | http://localhost:8000 |
| Dashboard | Static dashboard | http://localhost:8084 |

Các service theo profile:

| Profile | Service | Vai trò | URL/Cổng |
| --- | --- | --- | --- |
| `debug` | Kafka UI | UI xem topic/message Kafka | http://localhost:8080 |
| `debug` | Kibana | Elasticsearch UI | http://localhost:5601 |
| `monitoring` | Prometheus | Metrics scraping | http://localhost:9090 |
| `monitoring` | Grafana | Metrics dashboard UI | http://localhost:3000 |
| `orchestration` | Airflow | Batch orchestration | http://localhost:8082 |
| `warehouse` | ClickHouse | OLAP data warehouse | http://localhost:8123 |
| `enrichment` | Cassandra | Lưu enrichment tùy chọn từ speed layer | `localhost:9042` |
| `anomaly` | Cassandra + ml-anomaly | Phát hiện bất thường sau serving layer | `localhost:9042` |

Airflow mặc định:

```text
Username: admin
Password: admin
```

## Yêu Cầu Môi Trường

- Docker
- Docker Compose plugin
- Tối thiểu khoảng 6 GB RAM trống cho core stack; 8 GB+ nếu bật thêm nhiều profile.
- Python 3.10+ nếu chạy test/script ngoài container.

Khuyến nghị chạy bằng Docker Compose vì Spark, Kafka, MinIO và Elasticsearch cần nhiều service phụ thuộc.

## Cấu Hình

Tạo file `.env` từ mẫu:

```bash
cp .env.example .env
```

Các biến quan trọng:

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `API_HOST_PORT` | `8000` | Cổng FastAPI trên host |
| `DASHBOARD_HOST_PORT` | `8084` | Cổng dashboard |
| `KAFKA_HOST_PORT` | `9092` | Cổng Kafka host listener |
| `MINIO_API_HOST_PORT` | `9000` | Cổng MinIO S3 API |
| `MINIO_CONSOLE_HOST_PORT` | `9001` | Cổng MinIO Console |
| `CLICKHOUSE_HTTP_HOST_PORT` | `8123` | Cổng ClickHouse HTTP API |
| `CLICKHOUSE_NATIVE_HOST_PORT` | `9002` | Cổng ClickHouse native protocol trên host |
| `SPARK_MASTER_UI_HOST_PORT` | `8081` | Cổng Spark Master UI |
| `AIRFLOW_WEBSERVER_HOST_PORT` | `8082` | Cổng Airflow UI |
| `ES_HOST_PORT` | `9200` | Cổng Elasticsearch |
| `KIBANA_HOST_PORT` | `5601` | Cổng Kibana |
| `PROMETHEUS_HOST_PORT` | `9090` | Cổng Prometheus |
| `GRAFANA_HOST_PORT` | `3000` | Cổng Grafana |
| `NLP_MODEL_NAME` | `distilbert-base-uncased-finetuned-sst-2-english` | Tên sentiment model |
| `CONSUMER_FLUSH_SIZE` | `500` | Số record trước khi Object store writer flush |
| `CONSUMER_FLUSH_INTERVAL` | `30` | Thời gian flush tối đa theo giây |

Trong Docker network, các service dùng host nội bộ như `kafka:29092`, `redis`, `elasticsearch`. Nếu bật profile `enrichment` hoặc `anomaly` thì Cassandra dùng host nội bộ `cassandra`. Từ host machine, dùng `localhost:<port>`.

MinIO mặc định:

```text
Console:  http://localhost:9001
S3 API:   http://localhost:9000
Username: minioadmin
Password: minioadmin
Bucket:   social-lake
```

ClickHouse mặc định:

```text
HTTP API: http://localhost:8123
Native:   localhost:9002
Database: social_warehouse
User:     social
Password: social
```

## Khởi Động Dự Án

### Chạy dự án nhanh từ đầu
Nếu bạn muốn khởi động ngay lập tức toàn bộ hệ thống (dữ liệu chảy liên tục và cập nhật lên Dashboard), hãy chạy 3 bước sau:

```bash
# 1. Khởi động toàn bộ hạ tầng lõi và UI (Dashboard, API, Spark, Kafka, Elasticsearch,...)
docker compose up --build -d

# 2. Khởi động các luồng sinh dữ liệu (Giả lập đẩy data liên tục vào hệ thống)
make replay-raw

# 3. Chạy batch job để tạo batch views (chạy sau 30-60 giây khi đã có dữ liệu)
sleep 60 && docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/spark_batch_job.py

# (Tùy chọn) Index batch views vào Elasticsearch để query qua API
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/index_batch_views.py
```

**Lưu ý:**
- Bước 1-2: Realtime data chảy qua Redis + Elasticsearch ngay, có thể truy cập `/api/v1/stats/realtime` 
- Bước 3: Batch views được tạo và indexed, kích hoạt `/api/v1/posts` và `/api/v1/sentiment/trend`
- Nếu muốn batch chạy tự động theo lịch, hãy bật Airflow orchestration (xem phần Airflow bên dưới)

Sau khi chạy xong, hãy truy cập [http://localhost:8084](http://localhost:8084) để xem Dashboard trực quan hóa realtime + batch data.

### Xóa Dữ Liệu Chạy Lại Từ Đầu (Reset Data)
Nếu bạn muốn xóa trắng toàn bộ dữ liệu (Kafka, Spark checkpoints, Elasticsearch, Database, MinIO, Redis) để bắt đầu lại một môi trường hoàn toàn mới:

```bash
# Lệnh này sẽ stop toàn bộ container và xóa các docker volumes gắn kèm
docker compose down -v
```
Sau đó bạn có thể lặp lại 2 lệnh ở phần **Chạy dự án nhanh từ đầu** để khởi động lại một hệ thống sạch.

### Khởi động từng phần (Manual)
Build và start core stack cơ bản:

```bash
docker compose up --build -d
```

Hoặc dùng Makefile:

```bash
make up
```

Bật các nhóm phụ trợ khi cần:

```bash
make monitoring      # Prometheus + Grafana
make debug           # Kafka UI + Kibana
make orchestration   # Postgres + Airflow
make warehouse-stack # ClickHouse
make enrichment      # Cassandra cho enrichment persistence tùy chọn
make anomaly         # Cassandra + anomaly detector
make up-full         # tất cả profile
```

Kiểm tra trạng thái:

```bash
docker compose ps
```

Kiểm tra API:

```bash
curl http://localhost:8000/health
```

Kết quả mong đợi:

```json
{"status":"ok"}
```

Lưu ý: `http://localhost:8000/` có thể trả `404 Not Found` vì route root `/` chưa được định nghĩa. Dùng `/health`, `/docs` hoặc `/api/v1/...`.

## API Endpoints

FastAPI docs:

```text
http://localhost:8000/docs
```

Các endpoint chính:

| Method | Endpoint | Mục đích |
| --- | --- | --- |
| `GET` | `/health` | Health check |
| `GET` | `/api/v1/posts` | Query posts từ batch + realtime serving layer |
| `GET` | `/api/v1/sentiment/trend` | Query sentiment trend theo giờ/ngày |
| `GET` | `/api/v1/hashtags/top` | Query top hashtags |
| `GET` | `/api/v1/stats/realtime` | Query realtime stats từ Redis |
| `GET` | `/metrics` | Prometheus metrics |

Ví dụ:

```bash
curl "http://localhost:8000/api/v1/stats/realtime"
curl "http://localhost:8000/api/v1/posts?platform=reddit&limit=10"
curl "http://localhost:8000/api/v1/hashtags/top?platform=reddit&window_hours=24&top_n=10"
```

## Chạy Ingestion Simulator

Replay toàn bộ raw data Reddit. Nếu không truyền `--source`, simulator tự dùng
`data/<platform>_data/raw_data`:

```bash
python -m ingestion.simulator \
  --platform reddit \
  --rate 20 \
  --kafka-bootstrap localhost:9092
```

Replay một thư mục hoặc một file cụ thể:

```bash
python -m ingestion.simulator \
  --source data/facebook_data/raw_data \
  --platform facebook \
  --rate 20 \
  --kafka-bootstrap localhost:9092
```

Replay bằng container API để dùng network nội bộ Docker:

```bash
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  api \
  python -m ingestion.simulator \
    --platform reddit \
    --rate 20 \
    --kafka-bootstrap kafka:29092
```

Replay cả ba nền tảng như luồng giả realtime trong Docker Compose:

```bash
make replay-raw
```

Lệnh này bật profile `replay`, chạy ba service `replay-reddit`,
`replay-facebook`, `replay-instagram`, đọc mặc định từ các thư mục:

```text
data/reddit_data/raw_data
data/facebook_data/raw_data
data/instagram_data/raw_data
```

Điều chỉnh tốc độ publish:

```bash
REPLAY_RATE_PER_SEC=20 make replay-raw
```

Mặc định replay có dedupe trong mỗi lượt chạy để tránh cùng một `post_id`
bị publish lặp khi xuất hiện ở nhiều file raw. Có thể tắt khi cần test duplicate:

```bash
REPLAY_DEDUPE=false make replay-raw
```

Speed layer mặc định chỉ đọc message mới từ lúc stream chạy. Nếu cần replay lại
topic cũ vào realtime store trong môi trường dev:

```bash
STREAM_STARTING_OFFSETS=earliest docker compose up -d speed
```

Kafka topics được tạo tự động:

```text
social.reddit.posts
social.facebook.posts
social.instagram.posts
social.dlq
```

## Batch Layer

Batch layer gồm hai bước chính:

1. `batch/object_store_writer.py`: consume Kafka source topics và ghi raw Parquet vào MinIO.
2. `batch/spark_batch_job.py`: đọc raw Parquet và tạo batch views.

Batch views hiện có:

```text
platform_daily_stats
top_hashtags_weekly
author_activity
sentiment_time_series
top_posts
```

Chạy batch job thủ công:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/spark_batch_job.py
```

Index batch views vào Elasticsearch:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/batch/index_batch_views.py
```

## Data Warehouse Layer

ClickHouse là warehouse OLAP của dự án. MinIO vẫn là data lake/object storage, còn ClickHouse chứa các bảng phân tích dạng fact/dimension để query aggregate nhanh hơn Elasticsearch.

Loader chính:

```text
warehouse/clickhouse_loader.py
```

Luồng:

```text
MinIO batch views
  -> Spark warehouse loader
  -> ClickHouse social_warehouse
```

Các bảng hiện có:

```text
dim_platform
fact_platform_daily_stats
fact_top_hashtags_weekly
fact_author_activity
fact_sentiment_time_series
fact_top_posts
```

Chạy load warehouse thủ công:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master local[2] \
  /app/warehouse/clickhouse_loader.py
```

Query kiểm tra:

```bash
curl "http://localhost:8123/?user=social&password=social&database=social_warehouse" \
  --data-binary "SELECT count() FROM fact_platform_daily_stats"
```

## Speed Layer

Speed layer chạy service `speed` trong Docker Compose:

```text
speed/streaming_job.py
  -> đọc Kafka source topics
  -> validate schema
  -> enrich NLP qua speed/nlp_pipeline.py
  -> ghi Redis/Cassandra/Elasticsearch qua speed/realtime_stores.py
```

NLP enrichment của từng post là một phần của core stream và luôn nằm trong
service `speed`. Service `ml-anomaly` không nằm trực tiếp trên đường Kafka
stream; nó là detector phụ trợ đọc realtime stats qua serving layer và ghi alert.

Redis key realtime có dạng:

```text
rt:stats:<platform>:<window_start>
```

Cassandra là optional. Nếu bật profile `enrichment` hoặc `anomaly`, enrichment
được lưu trong keyspace:

```text
social_lambda.enrichments
```

Elasticsearch realtime index:

```text
social_realtime_views
```

## NLP Enrichment

File chính:

```text
speed/nlp_pipeline.py
```

Output enrichment gồm:

```json
{
  "post_id": "reddit_abc123",
  "sentiment_score": 0.75,
  "sentiment_label": "positive",
  "keywords": ["analytics", "pipeline"],
  "entities": [],
  "language": "en",
  "processed_at": "2026-05-20T10:00:00Z",
  "model_version": "distilbert-base-uncased-finetuned-sst-2-english"
}
```

Hiện trạng quan trọng:

- Code có logic dùng Transformer sentiment model nếu dependency/model có sẵn.
- Local image hiện chưa cài đầy đủ `transformers`, `torch`, `spacy`, `en_core_web_sm`.
- Vì vậy trong môi trường local hiện tại NLP thường chạy fallback deterministic để pipeline không bị chết.
- Nếu cần bám SOP nghiêm ngặt, cần bổ sung dependency/model và bật chế độ strict để thiếu model thì fail sớm.

## Serving Layer

Serving layer merge dữ liệu từ:

- Elasticsearch batch index: `social_batch_views`
- Elasticsearch realtime index: `social_realtime_views`
- Redis realtime stats

File chính:

```text
serving/merge_service.py
serving/es_indexer.py
api/main.py
```

## Airflow

Airflow là optional orchestrator chạy batch pipeline theo lịch. Nếu không chạy Airflow, bạn phải chạy batch job thủ công.

**Bật Airflow** (ngoài core stack):

```bash
make orchestration
```

Sau đó start lại simulator:

```bash
make replay-raw
```

Airflow sẽ tự động trigger DAG `social_lambda_batch_pipeline` theo lịch được cấu hình.

Airflow UI:

```text
http://localhost:8082
Username: admin
Password: admin
```

DAG chính:

```text
orchestration/dags/batch_pipeline_dag.py
```

Kiểm tra import errors:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Trigger DAG thủ công:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger social_lambda_batch_pipeline
```

Xem DAG runs:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d social_lambda_batch_pipeline
```

## Monitoring

Folder:

```text
monitoring/prometheus.yml
```

Prometheus scrape các target:

- Prometheus self metrics: `localhost:9090`
- FastAPI metrics: `api:8000/metrics`
- Simulator metrics: `host.docker.internal:9101`

URL:

```text
Prometheus: http://localhost:9090
Grafana:    http://localhost:3000
```

## Validation Theo SOP

Các script validation nằm trong `scripts/`.

### Phase 2: Ingestion, Kafka, DLQ, Metrics

```bash
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  api \
  python scripts/validate_phase2.py
```

Kiểm tra:

- Good record được publish vào `social.reddit.posts`.
- Bad record được publish vào `social.dlq`.
- Canonical schema đúng.
- `author_id` được hash.
- Metrics simulator đúng.

### Phase 3: Batch View Idempotency

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/scripts/validate_phase3_idempotency.py
```

Kiểm tra:

- Đọc tất cả batch views hiện có.
- Chạy lại batch job cho ngày `2023-11-14`.
- So sánh output trước/sau.
- Pass nếu kết quả không đổi.

### Phase 4: Speed Layer, Redis, Cassandra

```bash
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  api \
  python scripts/validate_phase4.py
```

Kiểm tra:

- Publish realtime record vào Kafka.
- Speed layer xử lý record.
- Redis có realtime stats.
- Cassandra có enrichment row với `sentiment_score`.

### Phase 5: Serving Layer, Elasticsearch, Kibana

```bash
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  api \
  python scripts/validate_phase5.py
```

Kiểm tra:

- `social_batch_views` không rỗng.
- Realtime document được index vào `social_realtime_views`.
- Merge query trả historical và realtime result.
- Kibana còn phản hồi.

## Tests

Folder `tests/` gồm:

```text
tests/unit/          Unit tests cho module riêng lẻ
tests/integration/   Integration tests cần service ngoài
tests/e2e/           End-to-end contract tests
tests/load/          Locust load tests
```

Chạy unit tests ngoài host:

```bash
pytest tests/unit
```

Chạy unit tests trong container:

```bash
docker compose run --rm --no-deps \
  -v "$PWD:/app" \
  api \
  python -m pytest tests/unit
```

Chạy e2e khi full stack đang chạy:

```bash
docker compose run --rm --no-deps \
  -e RUN_E2E=1 \
  -e API_URL=http://api:8000 \
  -v "$PWD:/app" \
  api \
  python -m pytest tests/e2e/test_pipeline_contract.py
```

Load test bằng Locust:

```bash
locust -f tests/load/locustfile.py --host http://localhost:8000
```

## Lệnh Thường Dùng

Start stack:

```bash
docker compose up --build -d
```

Stop stack nhưng giữ volumes (giữ lại dữ liệu):

```bash
docker compose down
```



Xem logs API:

```bash
docker compose logs --tail=100 api
```

Xem logs speed layer:

```bash
docker compose logs --tail=100 speed
```

Xem logs Object store writer:

```bash
docker compose logs --tail=100 object-store-writer
```

Xem Kafka topics:

```bash
docker compose exec kafka \
  kafka-topics --bootstrap-server kafka:29092 --list
```

Xem message trong topic Reddit:

```bash
docker compose exec kafka \
  kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic social.reddit.posts \
    --from-beginning \
    --max-messages 5
```

## Troubleshooting

### `http://localhost:8000/` trả 404

Đây không phải lỗi server. Root route `/` chưa được định nghĩa. Dùng:

```text
http://localhost:8000/health
http://localhost:8000/docs
http://localhost:8000/api/v1/stats/realtime
```

### API không truy cập được

Kiểm tra:

```bash
docker compose ps api
docker compose logs --tail=100 api
curl http://localhost:8000/health
```

### Kafka chưa sẵn sàng

Kiểm tra:

```bash
docker compose ps kafka zookeeper kafka-init
docker compose logs --tail=100 kafka
```

### MinIO chưa có dữ liệu

Kiểm tra Object store writer:

```bash
docker compose logs --tail=100 object-store-writer
```

Kiểm tra MinIO Console:

```text
http://localhost:9001
```

### Batch view rỗng

Nguyên nhân thường gặp:

1. **Chưa replay dữ liệu vào Kafka** → Chạy `make replay-raw` trước
2. **`object-store-writer` chưa flush Parquet** → Đợi 30-60 giây hoặc xem logs
3. **Spark batch job chưa chạy** → ❌ **PHỔ BIẾN NHẤT** 
   - Fix: Chạy `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/batch/spark_batch_job.py`
   - Hoặc: Bật Airflow với `make orchestration` để tự động
4. **Đường dẫn `STORAGE_RAW_BASE` hoặc `STORAGE_BATCH_VIEWS_BASE` sai** → Check `.env`

Kiểm tra:

```bash
# 1. Xem dữ liệu raw trên MinIO
docker compose exec minio mc ls minio/social-lake/data/raw --recursive | head

# 2. Xem batch views trên MinIO
docker compose exec minio mc ls minio/social-lake/data/batch_views --recursive | head

# 3. Xem batch index trên Elasticsearch
curl http://localhost:9200/social_batch_views/_search | jq '.hits.total'
```

### NLP không dùng DistilBERT/spaCy thật

Hiện image local chưa cài model nặng. Pipeline fallback để vẫn chạy end-to-end. Nếu cần strict SOP production, cần thêm:

- `torch`
- `transformers`
- `spacy`
- `en_core_web_sm`

và tải/cache model trong Docker build.

## Trạng Thái SOP

Đã có các phần chính:

- Docker Compose full stack.
- Kafka source topics và DLQ.
- Ingestion simulator + canonical schema.
- MinIO raw Parquet writer.
- Spark batch views.
- Airflow DAG.
- Speed layer realtime processing.
- Redis/Cassandra/Elasticsearch serving outputs.
- FastAPI + dashboard.
- Prometheus/Grafana monitoring.
- Validation scripts Phase 2, 3, 4, 5.
- Unit/e2e/load tests.

Các điểm còn cần lưu ý nếu chấm SOP nghiêm ngặt:

- NLP hiện có fallback; chưa bắt buộc DistilBERT + spaCy thật trong Docker image.
- F1 test NLP hiện dùng synthetic sample, chưa phải benchmark dataset thực.
- Monitoring dùng `prometheus-client`; chưa dùng `starlette-exporter`.
- Serving layer dùng Elasticsearch/Kibana thay vì Druid/Superset.
- Anomaly detector hiện thiên về rolling/fallback baseline, chưa train strict Isolation Forest từ historical batch view ở startup.

## Dọn Dẹp

Xóa container/network nhưng giữ dữ liệu volume:

```bash
docker compose down
```

Xóa cả dữ liệu volume:

```bash
docker compose down -v
```

Xóa cache Python sinh ra khi chạy test:

```bash
find . -type d -name __pycache__ -prune -exec rm -rf {} +
find . -type d -name .pytest_cache -prune -exec rm -rf {} +
```
