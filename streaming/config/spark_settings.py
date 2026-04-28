"""
Spark Streaming configuration for social media pipeline.

Environment variables:
  - KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
  - OUTPUT_DIR: Output directory for Parquet files (default: /tmp/streaming-output)
  - CHECKPOINT_DIR: Checkpoint directory (default: /tmp/spark-checkpoints)
  - SPARK_MASTER: Spark master URL (default: local[*])
"""

import os
from pathlib import Path

# ── Kafka Configuration ───────────────────────────────────────────────────────
ENV = os.getenv("ENV", "dev")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics
SOURCE_TOPICS = [
    os.getenv("KAFKA_TOPIC_BATCH", f"{ENV}.social-raw-batch"),
    os.getenv("KAFKA_TOPIC_REALTIME", f"{ENV}.social-raw-realtime"),
]
PROCESSED_TOPIC = os.getenv("KAFKA_TOPIC_PROCESSED", f"{ENV}.social-processed")

# ── Spark Configuration ───────────────────────────────────────────────────────
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

SPARK_CONFIG = {
    # Lưu ý: spark.app.name được set qua .appName() trong create_spark_session()
    # Không đặt ở đây để tránh nhầm lẫn

    # Streaming Configuration
    "spark.sql.streaming.schemaInference": "false",
    "spark.sql.streaming.minBatchesToRetain": "100",
    "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints",

    # Kafka Configuration
    "spark.sql.kafka.maxOffsetsPerTrigger": "50000",

    # Memory Configuration (must match spark-submit args)
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.cores": "1",
    "spark.cores.max": "1",

    # Shuffle Configuration
    "spark.sql.shuffle.partitions": "200",

    # Serialization
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max": "512m",
}

# ── Directory Configuration ───────────────────────────────────────────────────
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/tmp/streaming-output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints"))
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# ── Processing Configuration ──────────────────────────────────────────────────
WINDOW_DURATION = "10 minutes"  # Aggregation window
WATERMARK_DELAY = "5 minutes"   # Late data tolerance

# ── Producer Tuning (from ingestion) ──────────────────────────────────────────
PRODUCER_CONFIG = {
    "acks": "all",
    "linger.ms": 20,           # được cập nhật theo shared.config
    "compression.type": "lz4",  # l4 thay vì snappy
    "retries": 5,
    "retry.backoff.ms": 300,
    "batch.size": 65536,
}

# ── Batch Processing ──────────────────────────────────────────────────────────
BATCH_INTERVAL = "10 seconds"  # Micro-batch interval
OUTPUT_MODE = "append"  # append, update, complete

# ── Sink Paths ────────────────────────────────────────────────────────────────
SINK_PATHS = {
    "aggregated": OUTPUT_DIR / "aggregated",
    "clean_posts": OUTPUT_DIR / "clean",
    "viral_posts": OUTPUT_DIR / "viral_posts",
}

# Create sink directories
for sink_dir in SINK_PATHS.values():
    sink_dir.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
