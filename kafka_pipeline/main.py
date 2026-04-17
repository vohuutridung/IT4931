"""
main.py — đọc toàn bộ pre-split data và đẩy vào Kafka.

Usage:
  python main.py                        # tất cả sources
  python main.py --source reddit        # chỉ reddit
  python main.py --source instagram
  python main.py --source facebook
  python main.py --dry-run              # in ra mà không gửi Kafka
"""

import argparse
import logging
import sys
import time
from collections import defaultdict

from producer.readers import reddit_records, instagram_records, facebook_records
from producer.social_producer import SocialProducer
from normalizers import reddit, instagram, facebook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Map source → (reader_fn, normalizer_module)
SOURCES = {
    "reddit":    (reddit_records,    reddit),
    "instagram": (instagram_records, instagram),
    "facebook":  (facebook_records,  facebook),
}


def run(sources: list[str], dry_run: bool = False) -> None:
    producer = None if dry_run else SocialProducer()

    stats = defaultdict(lambda: defaultdict(int))
    # stats[source][topic] = count

    for source_name in sources:
        reader_fn, normalizer = SOURCES[source_name]
        logger.info("=== Processing: %s ===", source_name)

        for topic, raw in reader_fn():
            try:
                post = normalizer.normalize(raw)

                if not _validate(post, source_name):
                    stats[source_name]["skipped"] += 1
                    continue

                if dry_run:
                    logger.info("[DRY-RUN] %s → %s | post_id=%s | event=%s",
                                source_name, topic, post["post_id"],
                                _ms_to_iso(post["event_time"]))
                else:
                    producer.send(topic, post)

                stats[source_name][topic] += 1

            except Exception as e:
                logger.error("[%s] Error processing record: %s", source_name, e)
                stats[source_name]["errors"] += 1

    if producer:
        logger.info("Flushing all producers...")
        producer.flush()

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    total = 0
    for source, counts in stats.items():
        for topic_or_key, n in counts.items():
            logger.info("  %-12s | %-30s | %d records", source, topic_or_key, n)
            if topic_or_key != "errors" and topic_or_key != "skipped":
                total += n
    logger.info("  TOTAL sent: %d", total)


def _validate(post: dict, source: str) -> bool:
    required = {"post_id", "source", "event_time", "ingest_time", "content"}
    missing  = required - post.keys()
    if missing:
        logger.warning("[%s] Missing fields %s", source, missing)
        return False
    if not post.get("content", "").strip():
        logger.debug("[%s] Skip empty content: %s", source, post.get("post_id"))
        return False
    return True


def _ms_to_iso(ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Social Media → Kafka Pipeline")
    parser.add_argument("--source", choices=list(SOURCES.keys()),
                        help="Chỉ chạy 1 source (mặc định: tất cả)")
    parser.add_argument("--dry-run", action="store_true",
                        help="In record ra log mà không gửi Kafka")
    args = parser.parse_args()

    active = [args.source] if args.source else list(SOURCES.keys())

    if args.dry_run:
        logger.info("DRY-RUN mode — không kết nối Kafka")

    t0 = time.monotonic()
    run(active, dry_run=args.dry_run)
    logger.info("Total time: %.1fs", time.monotonic() - t0)
