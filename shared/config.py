"""
shared/config.py
─────────────────
Config tập trung cho toàn bộ pipeline:
  - ingestion  (producer → Kafka)
  - streaming  (Spark Structured Streaming)
  - batch      (kafka_to_minio consumer + spark_etl)

Tất cả giá trị đều có thể override qua biến môi trường.
"""

import os
from pathlib import Path

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL     = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

KAFKA_TOPIC_BATCH    = "social-raw-batch"
KAFKA_TOPIC_REALTIME = "social-raw-realtime"
KAFKA_TOPIC_PROCESSED = "social-processed"

KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "batch-consumer-group")

# ── MinIO (Object Storage) ────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_USE_SSL    = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

MINIO_BUCKET_RAW   = os.getenv("MINIO_BUCKET_RAW",   "social-raw")
MINIO_BUCKET_CLEAN = os.getenv("MINIO_BUCKET_CLEAN", "social-clean")
MINIO_RAW_PREFIX   = "raw"
MINIO_CLEAN_PREFIX = "clean"

# S3A endpoint cho Spark ↔ MinIO (dùng http://<host>:<port>, không phải host:port)
S3A_ENDPOINT = os.getenv("S3A_ENDPOINT", "http://localhost:9000")

# ── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB",         "social_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "posts_clean")

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SocialBatchETL")
SPARK_MASTER   = os.getenv("SPARK_MASTER",   "local[*]")

# ── Batch Consumer tuning ─────────────────────────────────────────────────────
# Flush khi đạt N records HOẶC sau M giây, tùy cái nào đến trước
CONSUMER_FLUSH_SIZE     = int(os.getenv("CONSUMER_FLUSH_SIZE",     "1000"))
CONSUMER_FLUSH_INTERVAL = int(os.getenv("CONSUMER_FLUSH_INTERVAL", "60"))

# ── Data paths (ingestion) ────────────────────────────────────────────────────
_BASE = Path(os.getenv("DATA_DIR", Path(__file__).parent.parent / "data")).resolve()

FB_DATA_DIR     = Path(os.getenv("FB_DATA_DIR",     str(_BASE / "facebook_data"))).resolve()
IG_DATA_DIR     = Path(os.getenv("IG_DATA_DIR",     str(_BASE / "instagram_data"))).resolve()
REDDIT_DATA_DIR = Path(os.getenv("REDDIT_DATA_DIR", str(_BASE / "reddit_data"))).resolve()

DATA_PATHS = {
    "facebook": {
        "batch":    FB_DATA_DIR / "data_before_2026_04_10",
        "realtime": FB_DATA_DIR / "data_after_2026_04_10",
    },
    "instagram": {
        "batch":    IG_DATA_DIR / "posts_before_2026_04_10.jsonl",
        "realtime": IG_DATA_DIR / "posts_after_2026_04_10.jsonl",
    },
    "reddit": {
        "batch":    REDDIT_DATA_DIR / "posts_before_2026_04_10.jsonl",
        "realtime": REDDIT_DATA_DIR / "posts_10k.jsonl",
    },
}

# ── Producer tuning (ingestion) ───────────────────────────────────────────────
PRODUCER_BATCH = {
    "acks":             "all",
    "linger.ms":        500,
    "compression.type": "snappy",
    "retries":          5,
    "retry.backoff.ms": 300,
    "batch.size":       65536,
}

PRODUCER_REALTIME = {
    "acks":             1,
    "linger.ms":        10,
    "compression.type": "snappy",
    "retries":          3,
    "retry.backoff.ms": 100,
    "batch.size":       16384,
}
