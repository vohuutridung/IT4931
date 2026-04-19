"""
Spark Streaming configuration for social media pipeline.

Environment variables:
  - KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
  - SCHEMA_REGISTRY_URL: Schema Registry URL (default: http://localhost:8081)
  - OUTPUT_DIR: Output directory for Parquet files (default: /tmp/streaming-output)
  - CHECKPOINT_DIR: Checkpoint directory (default: /tmp/spark-checkpoints)
  - SPARK_MASTER: Spark master URL (default: local[*])
"""

import os
from pathlib import Path

# ── Kafka Configuration ───────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Topics
SOURCE_TOPICS = ["social-raw-batch", "social-raw-realtime"]
PROCESSED_TOPIC = "social-processed"

# ── Spark Configuration ───────────────────────────────────────────────────────
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

SPARK_CONFIG = {
    "spark.app.name": "SocialMediaStreaming",
    "spark.master": SPARK_MASTER,
    
    # Streaming Configuration
    "spark.sql.streaming.schemaInference": "false",
    "spark.sql.streaming.minBatchesToRetain": "100",
    "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints",
    
    # Kafka Configuration
    "spark.sql.kafka.maxOffsetsPerTrigger": "50000",
    
    # Memory Configuration
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    
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
    "linger.ms": 500,
    "compression.type": "snappy",
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
    "raw_posts": OUTPUT_DIR / "raw_posts",
    "viral_posts": OUTPUT_DIR / "viral_posts",
}

# Create sink directories
for sink_dir in SINK_PATHS.values():
    sink_dir.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
