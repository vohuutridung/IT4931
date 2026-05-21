"""Central configuration for the SOP Lambda Architecture pipeline."""

import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ENV = os.getenv("ENV", "dev")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "social.dlq")
KAFKA_SOURCE_TOPICS = {
    "reddit": os.getenv("KAFKA_TOPIC_REDDIT", "social.reddit.posts"),
    "facebook": os.getenv("KAFKA_TOPIC_FACEBOOK", "social.facebook.posts"),
    "instagram": os.getenv("KAFKA_TOPIC_INSTAGRAM", "social.instagram.posts"),
}
KAFKA_ALL_SOURCE_TOPICS = tuple(KAFKA_SOURCE_TOPICS.values())

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "SocialBatchETL")
SPARK_MASTER   = os.getenv("SPARK_MASTER") or "spark://spark-master:7077"

# ── Lambda Architecture stores ────────────────────────────────────────────────
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "social-lake")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true")
STORAGE_RAW_BASE = os.getenv("STORAGE_RAW_BASE", f"s3a://{S3_BUCKET}/data/raw")
STORAGE_BATCH_VIEWS_BASE = os.getenv(
    "STORAGE_BATCH_VIEWS_BASE",
    f"s3a://{S3_BUCKET}/data/batch_views",
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "social_lambda")
CASSANDRA_ENRICHMENTS_TABLE = os.getenv("CASSANDRA_ENRICHMENTS_TABLE", "enrichments")
CASSANDRA_ALERTS_TABLE = os.getenv("CASSANDRA_ALERTS_TABLE", "alerts")
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_BATCH_INDEX = os.getenv("ES_BATCH_INDEX", "social_batch_views")
ES_REALTIME_INDEX = os.getenv("ES_REALTIME_INDEX", "social_realtime_views")
ES_BATCH_ALIAS = os.getenv("ES_BATCH_ALIAS", "batch_current")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "http://localhost:8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "social_warehouse")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "social")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "social")
REPLAY_RATE_PER_SEC = int(os.getenv("REPLAY_RATE_PER_SEC", "50"))
STREAM_TRIGGER_SECS = int(os.getenv("STREAM_TRIGGER_SECS", "5"))
REALTIME_WINDOW_HOURS = int(os.getenv("REALTIME_WINDOW_HOURS", "4"))
NLP_MODEL_NAME = os.getenv(
    "NLP_MODEL_NAME",
    "distilbert-base-uncased-finetuned-sst-2-english",
)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Batch Consumer tuning ─────────────────────────────────────────────────────
# Flush khi đạt N records HOẶC sau M giây, tùy cái nào đến trước
CONSUMER_FLUSH_SIZE     = int(os.getenv("CONSUMER_FLUSH_SIZE",     "5000"))  # Increased to reduce small files
CONSUMER_FLUSH_INTERVAL = int(os.getenv("CONSUMER_FLUSH_INTERVAL", "60"))
