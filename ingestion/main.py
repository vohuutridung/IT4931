#!/usr/bin/env python3
"""
main.py — đọc toàn bộ pre-split data và đẩy vào Kafka.
"""

import argparse
import logging
import time
from collections import defaultdict
import concurrent.futures
import threading

from ingestion.producer.readers import (
    reddit_records,
    instagram_records,
    facebook_records,
)
from ingestion.producer.social_producer import SocialProducer
from ingestion.normalizers import reddit, instagram, facebook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Sources mapping ──────────────────────────────────────

SOURCES = {
    "reddit":    (reddit_records,    reddit),
    "instagram": (instagram_records, instagram),
    "facebook":  (facebook_records,  facebook),
}


# ── Main runner ──────────────────────────────────────────

def run(sources: list[str], dry_run: bool = False) -> None:
    producer = None if dry_run else SocialProducer()

    try:
        stats = defaultdict(lambda: defaultdict(int))
        stats_lock = threading.Lock()

        def process_source(source_name: str, phase: str):
            reader_fn, normalizer = SOURCES[source_name]
            logger.info("=== Processing %s: %s ===", phase.upper(), source_name)

            for topic, raw in reader_fn(phase=phase):
                try:
                    post = normalizer.normalize(raw)

                    # Facebook normalizer trả None khi thiếu post_id / timestamp
                    if not post:
                        with stats_lock:
                            stats[source_name]["skipped"] += 1
                        continue

                    if not _validate(post, source_name):
                        with stats_lock:
                            stats[source_name]["skipped"] += 1
                        continue

                    if dry_run:
                        logger.info(
                            "[DRY-RUN] %s → %s | post_id=%s | event=%s",
                            source_name,
                            topic,
                            post["post_id"],
                            _ms_to_iso(post["event_time"]),
                        )
                    else:
                        producer.send(topic, post)

                    with stats_lock:
                        stats[source_name][topic] += 1

                except ValueError as e:
                    # Instagram/Reddit raise ValueError khi thiếu id hoặc timestamp
                    # → Đây là invalid data, đếm là skipped chứ không phải lỗi hệ thống
                    logger.warning("[%s] Invalid record (skip): %s", source_name, e)
                    with stats_lock:
                        stats[source_name]["skipped"] += 1

                except Exception:
                    logger.exception("[%s] Error processing record", source_name)
                    with stats_lock:
                        stats[source_name]["errors"] += 1

        # Phase 1: Chạy song song dữ liệu Batch (không có delay)
        logger.info("================ PHASE 1: BATCH ==================")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(sources)) as executor:
            futures = [executor.submit(process_source, s, "batch") for s in sources]
            concurrent.futures.wait(futures)

        # Phase 2: Chạy song song dữ liệu Realtime (có delay)
        logger.info("================ PHASE 2: REALTIME ===============")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(sources)) as executor:
            futures = [executor.submit(process_source, s, "realtime") for s in sources]
            concurrent.futures.wait(futures)

        # ── Summary ────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)

        total = 0
        for source, counts in stats.items():
            for key, n in counts.items():
                logger.info("  %-12s | %-30s | %d records", source, key, n)
                if key not in {"errors", "skipped"}:
                    total += n

        logger.info("  TOTAL sent: %d", total)

    finally:
        if producer:
            logger.info("Flushing producers...")
            producer.flush()


# ── Validation ───────────────────────────────────────────

def _validate(post: dict, source: str) -> bool:
    required = {"post_id", "source", "event_time", "ingest_time"}
    missing = required - post.keys()

    if missing:
        logger.warning("[%s] Missing fields %s", source, missing)
        return False

    if not isinstance(post["event_time"], int):
        logger.warning("[%s] Invalid event_time type", source)
        return False

    return True


# ── Utils ────────────────────────────────────────────────

def _ms_to_iso(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)\
        .strftime("%Y-%m-%d %H:%M:%S UTC")


# ── CLI ──────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Social Media → Kafka Pipeline")

    parser.add_argument(
        "--source",
        choices=list(SOURCES.keys()),
        help="Chỉ chạy 1 source (mặc định: tất cả)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="In ra log mà không gửi Kafka",
    )

    args = parser.parse_args()

    active_sources = [args.source] if args.source else list(SOURCES.keys())

    if args.dry_run:
        logger.info("DRY-RUN mode — không gửi Kafka")

    t0 = time.monotonic()
    run(active_sources, dry_run=args.dry_run)
    logger.info("Total time: %.1fs", time.monotonic() - t0)