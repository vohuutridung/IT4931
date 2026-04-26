#!/usr/bin/env python3

import os
import re
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────

ENV = os.getenv("ENV", "dev")


# ── Kafka ────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH", f"{ENV}.social-raw-batch")
TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME", f"{ENV}.social-raw-realtime")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", f"{ENV}.social-raw-dlq")

# ── Data paths (FIXED) ───────────────────────────────────

_BASE = Path(
    os.getenv(
        "DATA_DIR",
        str(Path(__file__).parent.parent.parent / "data"),
    )
).resolve()


def _build_paths() -> dict:
    fb_dir     = Path(os.getenv("FB_DATA_DIR",     str(_BASE / "facebook_data"))).resolve()
    ig_dir     = Path(os.getenv("IG_DATA_DIR",     str(_BASE / "instagram_data"))).resolve()
    reddit_dir = Path(os.getenv("REDDIT_DATA_DIR", str(_BASE / "reddit_data"))).resolve()

    return {
        "facebook": {
            "batch":    fb_dir / "batch_data",
            "realtime": fb_dir / "stream_data",
        },
        "instagram": {
            "batch":    ig_dir / "batch_data" / "posts.jsonl",
            "realtime": ig_dir / "stream_data" / "posts.jsonl",
        },
        "reddit": {
            "batch":    reddit_dir / "batch_data" / "posts.jsonl",
            "realtime": reddit_dir / "stream_data" / "posts.jsonl",
        },
    }


PATHS = _build_paths()

# ── Path validation ──────────────────────────────────────

def _validate_paths() -> None:
    for platform, cfg in PATHS.items():
        for mode, path in cfg.items():
            if not path.exists():
                logger.warning(
                    "[%s] Missing %s path: %s",
                    platform, mode, path,
                )


if ENV != "test":
    _validate_paths()

# ── Producer tuning (FIXED) ─────────────────────────────

PRODUCER_BATCH = {
    "acks":             "all",
    "linger.ms":        int(os.getenv("BATCH_LINGER_MS", "20")),   # ↓ từ 500 → hợp lý hơn
    "compression.type": "lz4",                                    # tốt hơn snappy
    "retries":          int(os.getenv("BATCH_RETRIES", "5")),
    "retry.backoff.ms": int(os.getenv("BATCH_RETRY_BACKOFF_MS", "300")),
    "batch.size":       int(os.getenv("BATCH_SIZE", "65536")),
}

PRODUCER_REALTIME = {
    "acks":             os.getenv("REALTIME_ACKS", "1"),
    "linger.ms":        int(os.getenv("REALTIME_LINGER_MS", "5")),
    "compression.type": "lz4",
    "retries":          int(os.getenv("REALTIME_RETRIES", "3")),
    "retry.backoff.ms": int(os.getenv("REALTIME_RETRY_BACKOFF_MS", "100")),
    "batch.size":       int(os.getenv("REALTIME_BATCH_SIZE", "16384")),
}
