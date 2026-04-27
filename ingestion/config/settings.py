#!/usr/bin/env python3
"""
ingestion/config/settings.py
─────────────────────────────
Config ingestion-specific: PATHS, DLQ topic, và producer tuning.

Fix #18: Các giá trị chung (KAFKA_BOOTSTRAP_SERVERS, PRODUCER_BATCH/REALTIME)
được import từ shared.config để tránh duplicate và không nhất quán.
"""

import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Re-export từ shared.config ──────────────────────────────────────────────
# Fix #18: import tập trung, không duplicate
from shared.config import (
    ENV,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BATCH as TOPIC_BATCH,
    KAFKA_TOPIC_REALTIME as TOPIC_REALTIME,
    PRODUCER_BATCH,
    PRODUCER_REALTIME,
    FB_DATA_DIR,
    IG_DATA_DIR,
    REDDIT_DATA_DIR,
)

# ── Ingestion-only: DLQ topic ───────────────────────────────────────────────
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", f"{ENV}.social-raw-dlq")

# ── Data paths (ingestion) ──────────────────────────────────────────────────
# Fix #1: Đồng nhất với shared/config.py — dùng batch_data/ và stream_data/
def _build_paths() -> dict:
    return {
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


PATHS = _build_paths()


# ── Path validation ─────────────────────────────────────────────────────────
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
