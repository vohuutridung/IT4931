import os
from pathlib import Path

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL     = os.getenv("SCHEMA_REGISTRY_URL",     "http://localhost:8081")

TOPIC_BATCH    = "social-raw-batch"
TOPIC_REALTIME = "social-raw-realtime"

# ── Data paths ───────────────────────────────────────────────────────────────
# BASE_DIR trỏ đến thư mục IT4931 (chứa crawl_facebook / crawl_instagram / crawl_reddit)
# Default: ../ (từ kafka_pipeline trỏ lên 1 level về IT4931)
BASE_DIR = Path(os.getenv("DATA_DIR", "..")).resolve()

PATHS = {
    "facebook": {
        "batch":    BASE_DIR / "crawl_facebook" / "data_before_2026_04_10",
        "realtime": BASE_DIR / "crawl_facebook" / "data_after_2026_04_10",
    },
    "instagram": {
        "batch":    BASE_DIR / "crawl_instagram" / "posts_before_2026_04_10.jsonl",
        "realtime": BASE_DIR / "crawl_instagram" / "posts_after_2026_04_10.jsonl",
    },
    "reddit": {
        "batch":    BASE_DIR / "crawl_reddit" / "posts_before_2026_04_10.jsonl",
        "realtime": BASE_DIR / "crawl_reddit" / "posts_after_2026_04_10.jsonl",
    },
}

# ── Producer tuning ──────────────────────────────────────────────────────────
PRODUCER_BATCH = {
    "acks":             "all",   # durability cao nhất
    "linger.ms":        500,
    "compression.type": "snappy",
    "retries":          5,
    "retry.backoff.ms": 300,
    "batch.size":       65536,   # 64KB — gom nhiều record / flush
}

PRODUCER_REALTIME = {
    "acks":             1,       # ưu tiên latency thấp
    "linger.ms":        10,
    "compression.type": "snappy",
    "retries":          3,
    "retry.backoff.ms": 100,
    "batch.size":       16384,
}
