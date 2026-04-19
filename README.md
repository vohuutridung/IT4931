## Overview

This pipeline reads pre-split data (by date) from three social media sources and produces messages to Kafka topics:
- **Input**: Split data files (before/after April 10, 2026)
- **Output**: Kafka topics with Avro-serialized records
- **Topics**: 
  - `social-raw-batch` - Historical data (created before April 10, 2026)
  - `social-raw-realtime` - Recent data (created after April 10, 2026)

## Quick Start

### 1. Prerequisites

Ensure Kafka and Schema Registry are running:
```bash
cd /XXX/IT4931
docker-compose up -d
```

Verify status:
```bash
docker-compose ps
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

Copy and configure `.env`:
```bash
cp .env.example .env
```

Edit `.env`:
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
DATA_DIR=../  # Points to IT4931 root containing crawl_facebook, crawl_reddit, crawl_instagram
```

### 4. Test Pipeline (Dry-run)

Test without sending to Kafka:
```bash
python main.py --dry-run
python main.py --dry-run --source reddit     # Single source test
```

### 5. Run Pipeline

Send data to Kafka:
```bash
python main.py
```

Expected output:
```
SUMMARY
========
reddit       | social-raw-batch       | 2,636 records
reddit       | social-raw-realtime    | 11 records
instagram    | social-raw-batch       | 1 record
facebook     | social-raw-batch       | 4,233 records
facebook     | social-raw-realtime    | 764 records
TOTAL sent: 7,645 records
```

## Data Sources

### Facebook
- **Batch**: `crawl_facebook/data_before_2026_04_10/` (4,236+ folders)
- **Realtime**: `crawl_facebook/data_after_2026_04_10/` (764+ folders)
- Format: Nested JSON in `post.json` files

### Reddit
- **Batch**: `crawl_reddit/posts_before_2026_04_10.jsonl` (2,636+ posts)
- **Realtime**: `crawl_reddit/posts_after_2026_04_10.jsonl` (11+ posts)
- Format: JSONL (one record per line)

### Instagram
- **Batch**: `crawl_instagram/posts_before_2026_04_10.jsonl` (1+ post)
- **Realtime**: `crawl_instagram/posts_after_2026_04_10.jsonl` (0 posts)
- Format: JSONL (one record per line)

## Utilities

### Check Kafka Messages

View messages in topics:
```bash
python check_kafka.py           # Show first 3 messages from each topic
```

### Check Topic Metadata

View topic partitions and configuration:
```bash
python check_topics.py
```

Expected output:
```
Topic: social-raw-batch        | Partitions: 3
Topic: social-raw-realtime     | Partitions: 3
```

## Project Structure

```
kafka_pipeline/
├── main.py                    # Main producer entry point
├── producer/
│   ├── readers.py            # Data readers (Reddit, Instagram, Facebook)
│   └── social_producer.py    # Kafka producer with Avro serialization
├── normalizers/              # Data normalization for each source
├── config/
│   └── settings.py           # Configuration & paths
├── schema/
│   └── social_post.avsc      # Avro schema
├── check_kafka.py            # Message checker utility
├── check_topics.py           # Topic metadata viewer
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## Configuration Details

### Base Configuration (`config/settings.py`)

```python
# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Topics
TOPIC_BATCH = "social-raw-batch"
TOPIC_REALTIME = "social-raw-realtime"

# Data paths (auto-resolved relative to kafka_pipeline)
BASE_DIR = Path(os.getenv("DATA_DIR", "..")).resolve()
```

### Producer Configuration

- **Batch topic**: Acks='all', high durability
- **Realtime topic**: Acks=1, low latency
- **Both**: Idempotence enabled, Snappy compression

## ⚙️ Common Issues & Solutions

### Issue: "File not found" errors
**Solution**: Ensure `DATA_DIR` points to `../` (IT4931 root)

### Issue: "UTF-8 BOM" errors (Facebook data)
**Solution**: Already fixed - using `utf-8-sig` encoding

### Issue: No messages in Kafka
**Solution**: Check offsets with `check_kafka.py`

### Issue: Schema Registry connection error
**Solution**: Verify Schema Registry URL in `.env`

## Performance

- **Throughput**: ~7,645 records in ~9 seconds (~850 rec/sec)
- **Data Format**: Avro binary (not human-readable)
- **Partitions**: 3 per topic for parallel consumption

## Testing

### Unit tests (future)
```bash
pytest tests/
```

### Manual integration test
```bash
python main.py --dry-run --source reddit
python main.py --dry-run --source instagram
python main.py --dry-run --source facebook
```

## Kafka Topics Schema

Both `social-raw-batch` and `social-raw-realtime` use the same Avro schema defined in `schema/social_post.avsc`.

Record structure includes:
- `post_id`, `source` (reddit/instagram/facebook)
- `content`, `title`, `caption`
- `event_time`, `created_utc`
- `engagement_metrics` (likes, comments, shares, etc.)

## Next Steps

1. **Consumer**: Build consumer to read from Kafka topics
2. **Streaming**: Process with Spark/Flink for real-time aggregations
3. **Storage**: Load into data warehouse (PostgreSQL, Snowflake, etc.)
4. **Analytics**: Build dashboards from processed data

## Notes

- Data is **pre-split by date** (before/after April 10, 2026)
- Producer runs **once** - messages persist in Kafka (~7,645 records)
- Use consumer offsets to track consumption
- Avro serialization ensures schema compatibility

## Support

For issues, check:
1. Kafka/Schema Registry logs: `docker-compose logs`
2. Application logs: See stdout/stderr
3. Topic status: Run `check_topics.py`
4. Message content: Run `check_kafka.py`

---
