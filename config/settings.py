"""Central configuration for the SOP Lambda Architecture pipeline."""

import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ENV = os.getenv("ENV", "dev")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "social.dlq")
KAFKA_TOPIC_ENRICHED = os.getenv("KAFKA_TOPIC_ENRICHED", "social.enriched.posts")
KAFKA_SOURCE_TOPICS = {
    "reddit": os.getenv("KAFKA_TOPIC_REDDIT", "social.reddit.posts"),
    "facebook": os.getenv("KAFKA_TOPIC_FACEBOOK", "social.facebook.posts"),
    "instagram": os.getenv("KAFKA_TOPIC_INSTAGRAM", "social.instagram.posts"),
}
KAFKA_ALL_SOURCE_TOPICS = tuple(KAFKA_SOURCE_TOPICS.values())

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SocialBatchETL")
SPARK_MASTER   = os.getenv("SPARK_MASTER") or "spark://spark-master:7077"

# ── Object Storage (MinIO) ────────────────────────────────────────────────────
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY") or "minioadmin"
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY") or "minioadmin"
S3_BUCKET = os.getenv("S3_BUCKET", "social-lake")
if ENV == "production" and ("minioadmin" in S3_ACCESS_KEY or "minioadmin" in S3_SECRET_KEY):
    raise ValueError("ERROR: Using default S3 credentials in production! Set S3_ACCESS_KEY and S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true")
STORAGE_RAW_BASE = os.getenv("STORAGE_RAW_BASE", f"s3a://{S3_BUCKET}/data/raw")
STORAGE_DISCARDED_BASE = os.getenv("STORAGE_DISCARDED_BASE", f"s3a://{S3_BUCKET}/data/discarded")
STORAGE_BATCH_VIEWS_BASE = os.getenv(
    "STORAGE_BATCH_VIEWS_BASE",
    f"s3a://{S3_BUCKET}/data/batch_views",
)

# ── Serving ───────────────────────────────────────────────────────────────────
REALTIME_WINDOW_HOURS = int(os.getenv("REALTIME_WINDOW_HOURS", "24"))

# ── ClickHouse ────────────────────────────────────────────────────────────────
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123")
if not CLICKHOUSE_HOST.startswith("http://") and not CLICKHOUSE_HOST.startswith("https://"):
    CLICKHOUSE_HOST = f"http://{CLICKHOUSE_HOST}:8123"
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "social")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "social")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "social")
CLICKHOUSE_WRITE_TIMEOUT = int(os.getenv("CLICKHOUSE_WRITE_TIMEOUT", "10"))

# ── Retry & Resilience ────────────────────────────────────────────────────────
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))  # exponential backoff
S3_WRITE_TIMEOUT = int(os.getenv("S3_WRITE_TIMEOUT", "60"))

# ── Speed Layer tuning ────────────────────────────────────────────────────────
STREAM_TRIGGER_SECS = int(os.getenv("STREAM_TRIGGER_SECS", "5"))
STREAM_STARTING_OFFSETS = os.getenv("STREAM_STARTING_OFFSETS", "latest")
SPEED_WRITE_BATCH_SIZE = int(os.getenv("SPEED_WRITE_BATCH_SIZE", "500"))
REPLAY_RATE_PER_SEC = int(os.getenv("REPLAY_RATE_PER_SEC", "20"))
REPLAY_DEDUPE = os.getenv("REPLAY_DEDUPE", "true").lower() in {"1", "true", "yes", "y", "on"}

# ── NLP / ML ──────────────────────────────────────────────────────────────────
NLP_MODEL_NAME = os.getenv(
    "NLP_MODEL_NAME",
    "vinai/phobert-base",
)
SENTIMENT_ARTIFACTS_DIR = os.getenv(
    "SENTIMENT_ARTIFACTS_DIR",
    "ml/sentiment/artifacts",
)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Batch Consumer tuning ─────────────────────────────────────────────────────
# Flush khi đạt N records HOẶC sau M giây, tùy cái nào đến trước
CONSUMER_FLUSH_SIZE     = int(os.getenv("CONSUMER_FLUSH_SIZE",     "500"))
CONSUMER_FLUSH_INTERVAL = int(os.getenv("CONSUMER_FLUSH_INTERVAL", "60"))
