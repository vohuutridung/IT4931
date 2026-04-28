# 🚀 Social Media Data Pipeline — IT4931

Hệ thống **data pipeline** xử lý dữ liệu mạng xã hội (Facebook, Instagram, Reddit) theo kiến trúc **Lambda Architecture** — kết hợp batch processing và real-time streaming vào một hệ thống thống nhất.

---

## 📑 Mục lục

- [Tổng quan kiến trúc](#tổng-quan-kiến-trúc)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt và khởi chạy](#cài-đặt-và-khởi-chạy)
- [Cấu hình](#cấu-hình)
- [Các component](#các-component)
- [Data Flow](#data-flow)
- [Schema dữ liệu](#schema-dữ-liệu)
- [Vận hành](#vận-hành)
- [Phát triển](#phát-triển)
- [Troubleshooting](#troubleshooting)

---

## Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│          Facebook          Instagram          Reddit                    │
│       (post.json files)  (posts.jsonl)    (posts.jsonl)                │
└────────────────┬───────────────┬───────────────┬────────────────────────┘
                 │               │               │
                 ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Python)                              │
│  • Normalize → Canonical Schema (schema_version=1)                      │
│  • Validate (post_id, event_time required)                              │
│  • Produce → Kafka (batch topic / realtime topic)                       │
└────────────────┬───────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         APACHE KAFKA                                    │
│   dev.social-raw-batch        dev.social-raw-realtime                  │
│   (3 partitions)              (3 partitions)                            │
└────────────┬──────────────────────────────┬────────────────────────────┘
             │                              │
    ┌────────▼────────┐            ┌────────▼────────┐
    │  BATCH LAYER    │            │  SPEED LAYER    │
    │                 │            │                 │
    │ Kafka→MinIO     │            │ Spark Streaming │
    │ Consumer        │            │ (Structured     │
    │ (Parquet files) │            │  Streaming)     │
    └────────┬────────┘            └────────┬────────┘
             │                              │
    ┌────────▼────────┐            ┌────────▼────────┐
    │    MinIO        │            │  Parquet Sinks  │
    │  (social-raw)   │            │  • clean/       │
    │   s3a://        │            │  • aggregated/  │
    └────────┬────────┘            └─────────────────┘
             │
    ┌────────▼────────┐
    │  Spark ETL      │  ← Apache Airflow (schedule 02:00 UTC)
    │  (Batch Job)    │
    │  • Clean        │
    │  • Enrich       │
    │  • Deduplicate  │
    └────────┬────────┘
             │
    ┌────────▼────────┐
    │   MinIO         │   MongoDB
    │  (social-clean) │   (posts_clean)
    └─────────────────┘
```

### Stack công nghệ

| Layer | Công nghệ | Version |
|-------|-----------|---------|
| Message Broker | Apache Kafka | 7.6.0 (Confluent) |
| Object Storage | MinIO | latest |
| Batch Processing | Apache Spark | 3.5.3 |
| Stream Processing | Spark Structured Streaming | 3.5.3 |
| Orchestration | Apache Airflow | 2.9.3 |
| Database | MongoDB | 7.0 |
| Language | Python | 3.8 (Spark/Airflow), 3.10+ (Ingestion) |
| Container | Docker Compose | - |

---

## Cấu trúc thư mục

```
IT4931/
├── shared/
│   └── config.py               # ★ Config trung tâm — source of truth
│
├── ingestion/                  # Layer 1: Thu thập & chuẩn hoá dữ liệu
│   ├── config/
│   │   └── settings.py         # Import từ shared/config.py
│   ├── normalizers/
│   │   ├── facebook.py         # Chuẩn hoá dữ liệu Facebook
│   │   ├── instagram.py        # Chuẩn hoá dữ liệu Instagram
│   │   └── reddit.py           # Chuẩn hoá dữ liệu Reddit
│   ├── producer/
│   │   ├── social_producer.py  # Kafka producer (batch + realtime)
│   │   └── readers.py          # Đọc file JSONL/folder
│   └── main.py                 # Entrypoint ingestion
│
├── batch/
│   ├── consumer/
│   │   └── kafka_to_minio.py   # Kafka → MinIO (Parquet)
│   └── etl/
│       ├── spark_etl.py        # Spark batch ETL job
│       └── airflow_dag.py      # Airflow DAG định nghĩa pipeline
│
├── streaming/
│   ├── config/
│   │   └── spark_settings.py   # Cấu hình Spark Streaming
│   ├── processors/
│   │   ├── social_processor.py # Transform pipeline (clean/enrich)
│   │   └── engagement_agg.py   # Windowed aggregation
│   ├── sinks/
│   │   ├── parquet_sink.py     # Ghi Parquet (ParquetSink, KafkaSink)
│   │   ├── kafka_sink.py       # write_to_kafka()
│   │   └── console_sink.py     # Debug console sink
│   └── main.py                 # Entrypoint streaming job
│
├── data/                       # Raw data (gitignored)
│   ├── facebook_data/
│   │   ├── batch_data/         # Historical posts (post.json per folder)
│   │   └── stream_data/        # Realtime posts
│   ├── instagram_data/
│   │   ├── batch_data/posts.jsonl
│   │   └── stream_data/posts.jsonl
│   └── reddit_data/
│       ├── batch_data/posts.jsonl
│       └── stream_data/posts.jsonl
│
├── Dockerfile                  # Image cho ingestion + batch consumer
├── Dockerfile.spark            # Image cho Spark jobs
├── Dockerfile.airflow          # Image custom cho Airflow (cài sẵn Java & PySpark)
├── docker-compose.yml          # Toàn bộ hạ tầng
├── requirements.txt            # Deps chung (không có pyspark)
├── requirements-spark.txt      # Deps cho Spark jobs (pyspark==3.5.3)
└── .env                        # Biến môi trường local
```

---

## Yêu cầu hệ thống

- **Docker** ≥ 24.0 và **Docker Compose** ≥ 2.20
- **RAM** tối thiểu 8 GB (khuyến nghị 16 GB vì cụm Spark đã được scale lên 4G RAM)
- **CPU** đa nhân (khuyến nghị tối thiểu 4 Cores để xử lý song song)
- **Disk** tối thiểu 10 GB
- **Python** 3.10+ (cho local development)

---

## Cài đặt và khởi chạy

### 1. Clone và cấu hình môi trường

```bash

git clone <repo-url>
cd IT4931

# Tạo file .env từ template
cp .env.example .env
# Chỉnh sửa các giá trị nếu cần (MinIO credentials, MongoDB URI, v.v.)
```

### 2. Khởi động toàn bộ hạ tầng

```bash
docker compose up --build -d
```

Thứ tự khởi động (tự động theo `depends_on`):

1. `zookeeper` → `kafka` → `kafka-init` (tạo topics)
2. `minio` → `minio-init` (tạo buckets)
3. `mongodb`
4. `postgres` → `airflow-init` (init DB + Airflow Variables)
5. `spark-master` → `spark-worker-1`
6. `ingestion`, `batch-consumer`, `streaming`, `airflow-webserver`, `airflow-scheduler`

### 3. Kiểm tra trạng thái

```bash
# Xem tất cả container
docker compose ps

# Xem log ingestion
docker compose logs -f ingestion

# Xem log streaming
docker compose logs -f streaming
```

### 4. Chạy Batch ETL

Cách tốt nhất là sử dụng **Airflow UI (http://localhost:8085)**:
- Bật (unpause) DAG `social_batch_pipeline` để nó tự động chạy hàng ngày.
- Bấm nút **Trigger** DAG `social_batch_init_pipeline` để chạy khởi tạo toàn bộ dữ liệu lịch sử.

Hoặc nếu muốn chạy thủ công bằng lệnh (Local Testing):
```bash
# Xử lý ngày hôm qua (tất cả sources)
spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,\
             com.amazonaws:aws-java-sdk-bundle:1.12.262,\
             org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  batch/etl/spark_etl.py --date $(date -d yesterday +%Y-%m-%d)

# Chỉ xử lý source cụ thể
spark-submit ... batch/etl/spark_etl.py \
  --date 2026-04-27 \
  --source reddit
```

---

## Cấu hình

### Biến môi trường (`.env`)

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `ENV` | `dev` | Môi trường (`dev`/`prod`) — ảnh hưởng tên Kafka topic |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP` | `batch-consumer-group` | Consumer group ID |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_USE_SSL` | `false` | Bật HTTPS cho MinIO |
| `MINIO_BUCKET_RAW` | `social-raw` | Bucket lưu raw Parquet |
| `MINIO_BUCKET_CLEAN` | `social-clean` | Bucket lưu clean Parquet |
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB` | `social_db` | Database name |
| `MONGO_COLLECTION` | `posts_clean` | Collection name |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_APP_NAME` | `SocialBatchETL` | Tên Spark application |
| `S3A_ENDPOINT` | `http://localhost:9000` | S3A endpoint cho Spark→MinIO |
| `FB_DATA_DIR` | `./data/facebook_data` | Thư mục dữ liệu Facebook |
| `IG_DATA_DIR` | `./data/instagram_data` | Thư mục dữ liệu Instagram |
| `REDDIT_DATA_DIR` | `./data/reddit_data` | Thư mục dữ liệu Reddit |
| `CONSUMER_FLUSH_SIZE` | `5000` | Flush buffer sau N records |
| `CONSUMER_FLUSH_INTERVAL` | `60` | Flush buffer sau N giây |
| `ETL_TRACKER_FILE` | `/opt/etl_state/processed_dates.json` | File lưu trạng thái ETL |

### Kafka Topics

| Topic | Mục đích | Partitions |
|-------|----------|-----------|
| `{ENV}.social-raw-batch` | Dữ liệu batch (historical) | 3 |
| `{ENV}.social-raw-realtime` | Dữ liệu realtime | 3 |
| `{ENV}.social-processed` | Dữ liệu đã xử lý (downstream) | 3 |

### MinIO Bucket Structure

```
social-raw/
└── raw/
    └── {source}/
        └── {year}/{month}/{day}/{hour}/
            └── {timestamp}.parquet

social-clean/
└── clean/
    └── source={source}/
        └── event_year={year}/event_month={month}/event_date={date}/
            └── *.parquet
```

---

## Các component

### 1. Ingestion Layer (`ingestion/`)

**Chức năng:** Đọc raw data từ file, normalize về canonical schema, gửi vào Kafka.

**Normalizers** — Mỗi platform có normalizer riêng với các tính năng:
- Parse timestamp (ISO-8601 / unix-s / unix-ms → UTC ms)
- Làm sạch text (HTML unescape, xóa URL, collapse whitespace)
- Lọc spam (phone number, drug keywords, bot accounts)
- Flatten comment tree (DFS, inject `_parent_id` / `_depth`)
- Deduplication theo comment ID

| Platform | File | Post ID field | Timestamp field |
|----------|------|---------------|-----------------|
| Facebook | `facebook.py` | `post_id` hoặc `id` | `createdAt` |
| Instagram | `instagram.py` | `id` | `timestamp` / `timestamp_raw` |
| Reddit | `reddit.py` | `post_id` | `created_utc_raw` / `created_utc` |

**Producer** — `SocialProducer`:
- 2 producer riêng biệt: batch (`acks=all`, idempotent) và realtime (`acks=1`, low-latency)
- Backpressure handling với `BufferError` retry
- Delivery callback cho mỗi message
- Compression `lz4` cho cả hai
- **Mô phỏng Realtime**: Tích hợp độ trễ ngẫu nhiên (`delay`) khi đọc dữ liệu stream để mô phỏng dòng chảy realtime rỉ rả liên tục, phục vụ test Spark Streaming.

**CLI:**
```bash
python -m ingestion.main [--source {facebook,instagram,reddit}] [--dry-run]
```

---

### 2. Batch Consumer (`batch/consumer/kafka_to_minio.py`)

**Chức năng:** Consume từ `social-raw-batch` → buffer theo `(source, event_date_hour)` → flush Parquet lên MinIO.

**Chi tiết:**
- **Grouping:** Buffer theo `(source, YYYY-MM-DD-HH)` → mỗi file Parquet = 1 source × 1 giờ
- **Flush trigger:** Đạt `CONSUMER_FLUSH_SIZE` records HOẶC `CONSUMER_FLUSH_INTERVAL` giây
- **Schema:** PyArrow schema cố định (16 columns) đảm bảo type consistency
- **Compression:** Snappy
- **Offset commit:** Sau khi flush thành công (at-least-once)
- **Graceful shutdown:** SIGINT/SIGTERM → flush buffer còn lại → commit → close

**MinIO path pattern:**
```
raw/{source}/{year}/{month}/{day}/{hour}/{timestamp}.parquet
```

---

### 3. Spark ETL (`batch/etl/spark_etl.py`)

**Chức năng:** Đọc Parquet từ MinIO → clean & enrich → ghi Parquet sạch về MinIO → upsert vào MongoDB.

**Pipeline:**
```
Read Raw (MinIO s3a://)
    ↓
Deduplicate (Window by post_id, keep latest ingest_time)
    ↓
Filter Empty (content length > 5 chars)
    ↓
Normalize Text (strip URLs, collapse whitespace)
    ↓
Add Derived Columns (event_date, event_year, hashtag_count, total_engagement)
    ↓
Add Engagement Tier (viral/high/medium/low)
    ↓
Write Clean Parquet (MinIO social-clean, partition by source/year/month/date)
    ↓
Upsert MongoDB (post_id as _id, upsertDocument=true)
```

**Incremental processing:**
- Tracker file tại `/opt/etl_state/processed_dates.json`
- Chế độ xử lý 1 lần (Static data mode): Bỏ qua hoàn toàn các partition đã xử lý để tránh quét lại dữ liệu cũ, tối ưu cho 1 bộ dataset tĩnh.
- Volume Docker `etl_state` đảm bảo persistent qua container restart

**CLI:**
```bash
spark-submit batch/etl/spark_etl.py [--date YYYY-MM-DD] [--source SOURCE] [--skip-mongo]
```

---

### 4. Airflow DAG (`batch/etl/airflow_dag.py`)

Hệ thống có 2 DAG riêng biệt cho 2 mục đích khác nhau:

#### 1. `social_batch_pipeline` (Incremental Load - Chạy hàng ngày)
**Schedule:** `0 2 * * *` (02:00 UTC hàng ngày)

**Flow:**
```
check_minio_has_data (ShortCircuit)
    ↓ (skip nếu không có data của ngày hôm qua)
spark_etl (BashOperator — spark-submit --date)
    ↓
verify_mongo_count (PythonOperator)
    ↓
log_done (BashOperator)
```
**Cấu hình:**
- `catchup=False` — không backfill tự động
- `start_date=2026-04-01`

#### 2. `social_batch_init_pipeline` (Historical Full Load - Khởi tạo 1 lần)
**Schedule:** `None` (Chỉ trigger thủ công qua UI)

**Mục đích:** Khi mới deploy hệ thống, trigger DAG này bằng tay để quét toàn bộ dữ liệu lịch sử từ đầu đến cuối (bỏ qua filter `--date`).
**Flow:**
```
spark_etl_full_load (BashOperator — spark-submit quét toàn bộ)
    ↓
log_init_done (BashOperator)
```
> **⚡ Hiệu năng:** Với cấu hình mặc định (1 Core CPU), việc tải toàn bộ 9.000+ files có thể mất 10-20 phút. Tuy nhiên, hệ thống đã được cấu hình chạy **2 Cores**, thời gian xử lý sẽ được rút ngắn xuống đáng kể.
**Airflow Variables** (được init tự động bởi `airflow-init` container):
- `minio_endpoint`, `minio_access_key`, `minio_secret_key`, `mongo_uri`

---

### 5. Spark Structured Streaming (`streaming/main.py`)

**Chức năng:** Consume từ cả hai Kafka topics → transform → ghi vào multiple sinks.

**Pipeline:**
```
Kafka Source (batch + realtime topics)
    ↓
Deserialize JSON (POST_JSON_SCHEMA)
    ↓
Apply Watermark (5 minutes late arrival tolerance)
    ↓
SocialProcessor.apply()
│   ├── flatten_engagement (nested → flat columns)
│   ├── normalize_time_columns (ms → TimestampType)
│   ├── deduplicate (dropDuplicatesWithinWatermark)
│   ├── filter_empty (content > 5 chars)
│   ├── normalize_text (clean URLs, hashtags lowercase)
│   ├── add_derived_columns (event_date/year/month/hour)
│   └── add_engagement_tier (viral/high/medium/low)
    ↓
┌───────────────────────┐
│    Output Sinks       │
├───────────────────────┤
│ Console (debug)       │ → stdout, append mode
│ Parquet clean/        │ → /tmp/streaming-output/clean
│ Parquet aggregated/   │ → /tmp/streaming-output/aggregated
└───────────────────────┘
```

**Engagement Aggregation** (10-minute tumbling window):
- `num_posts`, `avg_likes`, `avg_comments`, `avg_shares`
- `max_likes`, `min_likes`, `stddev_likes`
- Grouped by `source`

**Sinks:**

| Sink | Mode | Checkpoint | Partition |
|------|------|-----------|-----------|
| Console | append | `/tmp/spark-checkpoints/console` | - |
| Parquet (clean) | append | `/tmp/spark-checkpoints/parquet` | source, event_year, event_month, event_date |
| Parquet (aggregated) | append | `/tmp/spark-checkpoints/aggregated` | source |

---

## Data Flow

### Canonical Post Schema (v1)

```json
{
  "schema_version": 1,
  "post_id": "string",
  "source": "facebook|instagram|reddit",
  "event_time": 1234567890000,    // Unix ms UTC
  "ingest_time": 1234567890000,   // Unix ms UTC
  "author_id": "string",
  "author_name": "string",
  "content": "string",
  "url": "string",
  "hashtags": ["tag1", "tag2"],
  "engagement": {
    "likes": 0,
    "comments": 0,
    "shares": 0,
    "score": 0,
    "video_views": 0,
    "comments_normalized_count": 0
  },
  "comments": [
    {
      "comment_id": "string",
      "post_id": "string",
      "parent_id": "string|null",
      "author_id": "string",
      "author": "string",
      "text": "string",
      "likes": 0,
      "created_at": 1234567890000,
      "depth": 0,
      "extra": "{...}"
    }
  ],
  "extra": {...}
}
```

### Parquet Schema (sau Batch Consumer)

| Column | Type | Mô tả |
|--------|------|-------|
| `post_id` | string | ID bài viết |
| `source` | string | Platform (facebook/instagram/reddit) |
| `event_time` | timestamp(ms, UTC) | Thời điểm đăng |
| `ingest_time` | timestamp(ms, UTC) | Thời điểm ingest |
| `author_id` | string | ID tác giả |
| `author_name` | string | Tên tác giả |
| `content` | string | Nội dung bài viết |
| `url` | string | URL bài viết |
| `hashtags` | list<string> | Danh sách hashtag |
| `likes` | int64 | Lượt thích |
| `comments` | int64 | Số comment |
| `shares` | int64 | Lượt chia sẻ |
| `score` | int64 | Score/upvote |
| `video_views` | int64 | Lượt xem video |
| `extra` | string (JSON) | Metadata platform-specific |

---

## Vận hành

### Web UIs

| Service | URL / URI | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8082 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master | http://localhost:8080 | - |
| Spark Worker | http://localhost:8083 | - |
| Airflow | http://localhost:8085 | admin / admin |
| MongoDB Compass | `mongodb://localhost:27018` | (Sử dụng cổng 27018) |

### Lệnh hữu ích

```bash
# Xem tất cả containers
docker compose ps

# Xem log của service cụ thể
docker compose logs -f <service>

# Restart 1 service
docker compose restart streaming

# Scale worker (nếu cần)
docker compose up -d --scale spark-worker-1=2

# Kiểm tra Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Kiểm tra consumer group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group batch-consumer-group \
  --describe

# Dừng toàn bộ
docker compose down

# Dừng và xóa volumes (NGUY HIỂM — mất data)
docker compose down -v
```

### Monitoring

```bash
# Batch consumer: kiểm tra số records đã flush
docker compose logs batch-consumer | grep "Flushed"

# ETL tracker: xem ngày đã xử lý
docker compose exec airflow-scheduler \
  cat /opt/etl_state/processed_dates.json

# Streaming: xem throughput
docker compose logs streaming | grep "Streaming query"
```

---

## Phát triển

### Setup local

```bash
# Tạo virtual environment
python -m venv .venv
source .venv/bin/activate

# Cài dependencies
pip install -r requirements.txt

# Nếu cần chạy Spark locally
pip install -r requirements-spark.txt

# Set PYTHONPATH
export PYTHONPATH=$(pwd)
```

### Chạy tests / smoke tests

```bash
# Test facebook normalizer
python ingestion/normalizers/facebook.py

# Test instagram normalizer
python ingestion/normalizers/instagram.py

# Test reddit normalizer
python ingestion/normalizers/reddit.py

# Ingestion dry-run (không cần Kafka)
python -m ingestion.main --dry-run --source facebook
```

### Thêm nguồn dữ liệu mới

1. Tạo normalizer tại `ingestion/normalizers/<platform>.py` với hàm `normalize(raw: dict) -> dict`
2. Thêm reader function vào `ingestion/producer/readers.py`
3. Đăng ký trong `SOURCES` dict tại `ingestion/main.py`
4. Thêm data paths vào `shared/config.py` (`DATA_PATHS`) và `ingestion/config/settings.py` (`PATHS`)

### Cấu trúc dữ liệu đầu vào

**Facebook:** Folder `batch_data/{post_id}/post.json` — mỗi folder là 1 bài viết
```
facebook_data/batch_data/
└── 123456789/
    └── post.json
```

**Instagram/Reddit:** File JSONL — mỗi dòng là 1 bài viết
```
instagram_data/batch_data/posts.jsonl
reddit_data/batch_data/posts.jsonl
```

---

## Schema dữ liệu

### Engagement Tiers

| Tier | Điều kiện |
|------|-----------|
| `viral` | `total_engagement > 1000` |
| `high` | `total_engagement > 100` |
| `medium` | `total_engagement > 10` |
| `low` | Còn lại |

### Comment Extra Schema (cross-platform)

```json
{
  "parent_id": "string|null",
  "depth": 0,
  "reply_count": 0,
  "is_truncated": false,
  "platform_meta": {}
}
```

---

## Troubleshooting

### Kafka: Producer queue full

```
BufferError: Local: Queue full
```
**Giải pháp:** Tăng `BATCH_LINGER_MS` hoặc giảm throughput ingestion. Consumer group lag quá lớn thì thêm partition.

---

### MinIO: Connection refused

```
urllib3.exceptions.MaxRetryError: ... connect to minio:9000
```
**Giải pháp:** Đợi MinIO healthcheck pass:
```bash
docker compose logs minio | tail -20
docker compose ps minio
```

---

### Spark: Class not found

```
ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
```
**Giải pháp:** JARs chưa được download vào image. Rebuild:
```bash
docker compose build spark-master
```

---

### Airflow: Variable not found

```
KeyError: Variable minio_endpoint does not exist
```
**Giải pháp:** `airflow-init` chưa chạy xong hoặc bị lỗi:
```bash
docker compose logs airflow-init
# Chạy lại:
docker compose run --rm airflow-init
```

---

### Streaming: AnalysisException

```
AnalysisException: Queries with streaming sources must be executed with writeStream
```
**Giải pháp:** Không gọi `count()`, `orderBy()`, hay các action không hỗ trợ trên streaming DataFrame.

---

### ETL: Incremental skip không đúng

**Giải pháp:** Xóa tracker file để re-process:
```bash
docker compose exec airflow-scheduler \
  rm /opt/etl_state/processed_dates.json
```

---

## License

MIT License — xem [LICENSE](LICENSE)
