import os
from pathlib import Path

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL     = os.getenv("SCHEMA_REGISTRY_URL",     "http://localhost:8081")

TOPIC_BATCH    = "social-raw-batch"
TOPIC_REALTIME = "social-raw-realtime"

# ── Data paths ────────────────────────────────────────────────────────────────
# Cách 1 — tất cả crawl_* nằm chung 1 thư mục cha:
#   DATA_DIR=/home/khang/.../IT4931
#
# Cách 2 — mỗi platform 1 thư mục riêng (override cách 1):
#   FB_DATA_DIR=/path/to/crawl_facebook
#   IG_DATA_DIR=/path/to/crawl_instagram
#   REDDIT_DATA_DIR=/path/to/crawl_reddit

_BASE = Path(os.getenv("DATA_DIR", Path(__file__).parent.parent.parent / "data")).resolve()

FB_DATA_DIR     = Path(os.getenv("FB_DATA_DIR",     str(_BASE / "facebook_data"))).resolve()
IG_DATA_DIR     = Path(os.getenv("IG_DATA_DIR",     str(_BASE / "instagram_data"))).resolve()
REDDIT_DATA_DIR = Path(os.getenv("REDDIT_DATA_DIR", str(_BASE / "reddit_data"))).resolve()

PATHS = {
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
        "realtime": REDDIT_DATA_DIR / "posts_10k.jsonl",  # Use large dataset from reddit_all_topics.json
    },
}

# ── Producer tuning ───────────────────────────────────────────────────────────
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
