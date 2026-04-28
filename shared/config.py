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

ENV = os.getenv("ENV", "dev")
KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH", f"{ENV}.social-raw-batch")
KAFKA_TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME", f"{ENV}.social-raw-realtime")
KAFKA_TOPIC_PROCESSED = os.getenv("KAFKA_TOPIC_PROCESSED", f"{ENV}.social-processed")

KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", f"{ENV}-batch-consumer-group")

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
protocol = "https" if MINIO_USE_SSL else "http"
S3A_ENDPOINT = os.getenv("S3A_ENDPOINT", f"{protocol}://{MINIO_ENDPOINT}")

# ── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB",         "social_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "posts_clean")

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SocialBatchETL")
SPARK_MASTER   = os.getenv("SPARK_MASTER") or "spark://spark-master:7077"

# ── Batch Consumer tuning ─────────────────────────────────────────────────────
# Flush khi đạt N records HOẶC sau M giây, tùy cái nào đến trước
CONSUMER_FLUSH_SIZE     = int(os.getenv("CONSUMER_FLUSH_SIZE",     "5000"))  # Increased to reduce small files
CONSUMER_FLUSH_INTERVAL = int(os.getenv("CONSUMER_FLUSH_INTERVAL", "60"))

# ── Data paths (ingestion) ────────────────────────────────────────────────────
_BASE = Path(os.getenv("DATA_DIR", Path(__file__).parent.parent / "data")).resolve()

FB_DATA_DIR     = Path(os.getenv("FB_DATA_DIR",     str(_BASE / "facebook_data"))).resolve()
IG_DATA_DIR     = Path(os.getenv("IG_DATA_DIR",     str(_BASE / "instagram_data"))).resolve()
REDDIT_DATA_DIR = Path(os.getenv("REDDIT_DATA_DIR", str(_BASE / "reddit_data"))).resolve()

# Fix #1: Đồng nhất cấu trúc thư mục với ingestion/config/settings.py và .gitignore
# Thư mục thực tế: batch_data/ (historical) và stream_data/ (realtime)
DATA_PATHS = {
    "facebook": {
        "batch":    FB_DATA_DIR / "batch_data",
        "realtime": FB_DATA_DIR / "stream_data",
    },
    "instagram": {
        "batch":    IG_DATA_DIR / "batch_data" / "posts.jsonl",
        "realtime": IG_DATA_DIR / "stream_data" / "posts.jsonl",
    },
    "reddit": {
        "batch":    REDDIT_DATA_DIR / "batch_data" / "posts.jsonl",
        "realtime": REDDIT_DATA_DIR / "stream_data" / "posts.jsonl",
    },
}

# ── Producer tuning (ingestion) ───────────────────────────────────────────────
# Fix #18: Source of truth duy nhất — ingestion/config/settings.py import từ đây
PRODUCER_BATCH = {
    "acks":             "all",
    "linger.ms":        int(os.getenv("BATCH_LINGER_MS", "20")),    # 20ms hợp lý hơn 500ms
    "compression.type": "lz4",                                     # lz4 nhanh hơn snappy
    "retries":          int(os.getenv("BATCH_RETRIES", "5")),
    "retry.backoff.ms": int(os.getenv("BATCH_RETRY_BACKOFF_MS", "300")),
    "batch.size":       int(os.getenv("BATCH_SIZE", "65536")),
}

PRODUCER_REALTIME = {
    "acks":             os.getenv("REALTIME_ACKS", "1"),
    "linger.ms":        int(os.getenv("REALTIME_LINGER_MS", "5")),   # 5ms cho low-latency
    "compression.type": "lz4",
    "retries":          int(os.getenv("REALTIME_RETRIES", "3")),
    "retry.backoff.ms": int(os.getenv("REALTIME_RETRY_BACKOFF_MS", "100")),
    "batch.size":       int(os.getenv("REALTIME_BATCH_SIZE", "16384")),
}


def validate_paths():
    """Validate that configured data paths exist."""
    missing = []
    for source, paths in DATA_PATHS.items():
        for mode, path in paths.items():
            if not Path(path).exists():
                missing.append((source, mode, path))
    if missing:
        missing_str = ", ".join(f"{source}:{mode}={path}" for source, mode, path in missing)
        raise FileNotFoundError(f"Missing configured data paths: {missing_str}")
