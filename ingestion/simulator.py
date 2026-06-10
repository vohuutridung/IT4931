#!/usr/bin/env python3
"""File replay simulator required by SOP-LAMBDA-001.

The project normalizers keep richer platform-specific fields internally; this
module adapts their output to the SOP ingestion schema before publishing to
Kafka source topics.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from confluent_kafka import Producer

from ingestion.normalizers import facebook, instagram, reddit
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SOURCE_TOPICS,
    KAFKA_TOPIC_DLQ,
    REPLAY_DEDUPE,
    REPLAY_RATE_PER_SEC,
)

try:
    from prometheus_client import Counter, Histogram, start_http_server
except Exception:  # pragma: no cover - optional dependency fallback
    Counter = Histogram = None
    start_http_server = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
)
logger = logging.getLogger("simulator")


class OutOfRangeError(ValueError):
    """Exception raised when a post timestamp is outside the valid range [2026-01-01, 2026-04-30]."""
    pass


NORMALIZERS = {
    "reddit": reddit.normalize,
    "facebook": facebook.normalize,
    "instagram": instagram.normalize,
}
SUPPORTED_SOURCE_SUFFIXES = {".csv", ".json", ".jsonl", ".ndjson"}

REQUIRED_POST_FIELDS = {
    "post_id",
    "platform",
    "source_id",
    "author_id",
    "content",
    "title",
    "media_urls",
    "hashtags",
    "comments",
    "created_at",
    "ingested_at",
    "metrics",
}

METRIC_FIELDS = {"likes", "comments", "shares", "views"}

records_published_total = (
    Counter("records_published_total", "Records published", ["platform"])
    if Counter else None
)
publish_errors_total = (
    Counter("publish_errors_total", "Publish or validation errors", ["platform"])
    if Counter else None
)
publish_latency_seconds = (
    Histogram("publish_latency_seconds", "Kafka publish latency", ["platform"])
    if Histogram else None
)


class TokenBucket:
    def __init__(self, rate: float, capacity: float | None = None) -> None:
        self.rate = max(float(rate), 0.1)
        self.capacity = float(capacity or max(rate, 1))
        self.tokens = self.capacity
        self.updated_at = time.monotonic()

    def wait(self) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.updated_at = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1:
                self.tokens -= 1
                return
            time.sleep((1 - self.tokens) / self.rate)


def read_records(path: Path) -> Iterator[dict]:
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SOURCE_SUFFIXES:
        raise ValueError(f"Unsupported source file type: {path}")
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            yield from csv.DictReader(handle)
        return

    if suffix in {".jsonl", ".ndjson"}:
        with path.open(encoding="utf-8-sig") as handle:
            yield from _read_json_lines(handle, path)
        return

    with path.open(encoding="utf-8-sig") as handle:
        first = handle.read(1)
        handle.seek(0)
        if first in {"[", "{"}:
            try:
                data = json.load(handle)
            except json.JSONDecodeError as exc:
                if "Extra data" not in str(exc):
                    raise
                handle.seek(0)
                yield from _read_json_lines(handle, path)
                return
            if isinstance(data, dict):
                yield data
                return
            if not isinstance(data, list):
                raise ValueError(f"JSON array expected in {path}")
            for row in data:
                if isinstance(row, dict):
                    yield row
            return

        yield from _read_json_lines(handle, path)


def _read_json_lines(handle, path: Path) -> Iterator[dict]:
    for lineno, line in enumerate(handle, 1):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON line {lineno} in {path}: {exc}") from exc
        if isinstance(row, dict):
            yield row


def iter_source_files(source: Path) -> Iterator[Path]:
    if source.is_file():
        yield source
        return
    if not source.exists():
        raise FileNotFoundError(f"Source does not exist: {source}")
    if not source.is_dir():
        raise ValueError(f"Source must be a file or directory: {source}")

    found = False
    import os
    for root, _, files in os.walk(str(source)):
        for file in sorted(files):
            ext = os.path.splitext(file)[1].lower()
            if ext in SUPPORTED_SOURCE_SUFFIXES:
                found = True
                yield Path(root) / file
    if not found:
        raise ValueError(f"No supported source files found under: {source}")


def default_source_for_platform(platform: str) -> Path:
    return Path("data") / f"{platform}_data" / "raw_data"


def normalise(platform: str, raw: dict) -> dict:
    post = NORMALIZERS[platform](raw)
    if not post:
        raise ValueError("normalizer returned no post")
    return to_sop_schema(platform, raw, post)


def to_sop_schema(platform: str, raw: dict, post: dict) -> dict:
    missing = {"post_id", "event_time", "ingest_time"} - post.keys()
    if missing:
        raise ValueError(f"missing normalized fields: {sorted(missing)}")
    if post["event_time"] in {None, ""}:
        raise ValueError("missing normalized event_time")

    try:
        event_time_sec = float(post["event_time"])
        if event_time_sec > 10_000_000_000:
            event_time_sec = event_time_sec / 1000
        # Filter: Jan 1, 2026 is 1767225600.0, Apr 30, 2026 is 1777593599.0
        if not (1767225600.0 <= event_time_sec <= 1777593599.0):
            raise OutOfRangeError(f"timestamp {event_time_sec} outside range 2026-01-01 to 2026-04-30")
    except (TypeError, ValueError) as exc:
        if isinstance(exc, OutOfRangeError):
            raise
        raise ValueError(f"invalid timestamp for filtering: {exc}") from exc

    metrics = _metrics(post.get("engagement") or {})
    canonical = {
        "post_id": _global_post_id(platform, post["post_id"]),
        "platform": platform,
        "source_id": _source_id(platform, raw, post),
        "author_id": _hash_author_id(post.get("author_id")),
        "content": str(post.get("content") or "")[:10000],
        "title": _title(raw),
        "media_urls": _media_urls(raw, post),
        "hashtags": _string_list(post.get("hashtags")),
        "comments": _comments(post.get("comments")),
        "created_at": _iso_utc(post["event_time"], shift=False),
        "ingested_at": _iso_utc(post["ingest_time"], shift=False),
        "metrics": metrics,
    }
    validate_sop_post(canonical)
    return canonical


def validate_sop_post(post: dict) -> None:
    missing = REQUIRED_POST_FIELDS - post.keys()
    if missing:
        raise ValueError(f"missing SOP fields: {sorted(missing)}")
    nullable_fields = {"title"}
    for field in REQUIRED_POST_FIELDS - nullable_fields:
        if post[field] is None or post[field] == "":
            raise ValueError(f"empty SOP field: {field}")
    if post["platform"] not in NORMALIZERS:
        raise ValueError(f"invalid platform: {post['platform']}")
    if not isinstance(post["media_urls"], list):
        raise ValueError("media_urls must be a list")
    if not isinstance(post["hashtags"], list):
        raise ValueError("hashtags must be a list")
    if not isinstance(post["comments"], list):
        raise ValueError("comments must be a list")
    if not isinstance(post["metrics"], dict):
        raise ValueError("metrics must be an object")
    if set(post["metrics"]) != METRIC_FIELDS:
        raise ValueError("metrics must contain likes/comments/shares/views")
    for key, value in post["metrics"].items():
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"metrics.{key} must be a non-negative integer")


def _global_post_id(platform: str, post_id: Any) -> str:
    post_id = str(post_id)
    prefix = f"{platform}_"
    return post_id if post_id.startswith(prefix) else f"{prefix}{post_id}"


def _hash_author_id(author_id: Any) -> str:
    if author_id in {None, ""}:
        raise ValueError("missing author_id")
    return hashlib.sha256(str(author_id).encode("utf-8")).hexdigest()


def _iso_utc(value: Any, shift: bool = False) -> str:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc
    if timestamp > 10_000_000_000:
        timestamp = timestamp / 1000
    if shift:
        max_raw_ts = 1777562263.0
        offset = time.time() - max_raw_ts
        timestamp = timestamp + offset
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _metrics(engagement: dict) -> dict:
    return {
        "likes": _non_negative_int(engagement.get("likes")),
        "comments": _non_negative_int(engagement.get("comments")),
        "shares": _non_negative_int(engagement.get("shares")),
        "views": _non_negative_int(engagement.get("views", engagement.get("video_views"))),
    }


def _non_negative_int(value: Any) -> int:
    try:
        return max(int(float(value or 0)), 0)
    except (TypeError, ValueError):
        return 0


def _source_id(platform: str, raw: dict, post: dict) -> str:
    extra = post.get("extra") if isinstance(post.get("extra"), dict) else {}
    author = raw.get("author") if isinstance(raw.get("author"), dict) else {}
    candidates = [
        raw.get("source_id"),
        raw.get("subreddit"),
        extra.get("subreddit"),
        raw.get("page_id"),
        raw.get("pageId"),
        raw.get("ownerId"),
        raw.get("ownerUsername"),
        author.get("id"),
        author.get("name"),
        platform,
    ]
    for value in candidates:
        if value not in {None, ""}:
            return str(value)
    raise ValueError("missing source_id")


def _title(raw: dict) -> str | None:
    value = raw.get("title") or raw.get("headline")
    return str(value) if value not in {None, ""} else None


def _media_urls(raw: dict, post: dict) -> list[str]:
    values: list[Any] = []
    for key in ("media_urls", "mediaUrls", "images", "displayUrl", "videoUrl", "thumbnail"):
        value = raw.get(key)
        if isinstance(value, list):
            values.extend(value)
        elif value:
            values.append(value)
    media_urls: list[str] = []
    for value in values:
        if isinstance(value, dict):
            value = value.get("url") or value.get("src")
        if value:
            media_urls.append(str(value))
    return list(dict.fromkeys(media_urls))


def _string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value) for value in values if value not in {None, ""}]


def _comments(values: Any) -> list[dict]:
    if not isinstance(values, list):
        return []
    comments: list[dict] = []
    for value in values:
        if isinstance(value, dict):
            cmt = dict(value)
            author_id = cmt.get("author_id")
            if author_id not in {None, "", "unknown"}:
                cmt["author_id"] = _hash_author_id(author_id)
            comments.append(cmt)
    return comments


def encode(post: dict) -> bytes:
    return json.dumps(post, ensure_ascii=False, default=str).encode("utf-8")


def publish(producer: Producer, topic: str, post: dict, platform: str) -> None:
    started = time.monotonic()
    producer.produce(topic=topic, key=str(post["post_id"]).encode(), value=encode(post))
    producer.poll(0)
    if records_published_total:
        records_published_total.labels(platform=platform).inc()
        publish_latency_seconds.labels(platform=platform).observe(time.monotonic() - started)


def publish_dlq(producer: Producer, platform: str, raw: dict, error: Exception) -> None:
    payload = {
        "platform": platform,
        "error": str(error),
        "raw": raw,
        "failed_at": int(time.time() * 1000),
    }
    producer.produce(KAFKA_TOPIC_DLQ, value=encode(payload))
    producer.poll(0)
    if publish_errors_total:
        publish_errors_total.labels(platform=platform).inc()


def replay(
    source: Path,
    platform: str,
    rate: int,
    loop: bool,
    kafka_bootstrap: str,
    dedupe: bool = REPLAY_DEDUPE,
    max_records: int | None = None,
) -> None:
    producer = Producer({"bootstrap.servers": kafka_bootstrap, "acks": "all"})
    bucket = TokenBucket(rate)
    topic = KAFKA_SOURCE_TOPICS[platform]
    files = list(iter_source_files(source))
    total = 0

    try:
        while True:
            emitted = 0
            skipped_duplicates = 0
            seen_post_ids: set[str] = set()
            for file_path in files:
                for raw in read_records(file_path):
                    if max_records is not None and total >= max_records:
                        break
                    bucket.wait()
                    try:
                        post = normalise(platform, raw)
                        post_id = str(post["post_id"])
                        if dedupe and post_id in seen_post_ids:
                            skipped_duplicates += 1
                            continue
                        seen_post_ids.add(post_id)
                        publish(producer, topic, post, platform)
                        emitted += 1
                        total += 1
                    except OutOfRangeError:
                        continue
                    except Exception as exc:
                        logger.warning("Routing malformed record to DLQ: %s | source=%s", exc, file_path)
                        publish_dlq(producer, platform, raw, exc)
                if max_records is not None and total >= max_records:
                    break
            producer.flush()
            logger.info(
                "Replay pass complete | platform=%s source=%s files=%d emitted=%d duplicates=%d total=%d",
                platform,
                source,
                len(files),
                emitted,
                skipped_duplicates,
                total,
            )
            if not loop or (max_records is not None and total >= max_records):
                break
    finally:
        producer.flush()


def parse_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay static social files to Kafka")
    parser.add_argument(
        "--source",
        type=Path,
        help="Source file or directory. Defaults to data/<platform>_data/raw_data.",
    )
    parser.add_argument("--platform", required=True, choices=sorted(NORMALIZERS))
    parser.add_argument("--rate", type=int, default=REPLAY_RATE_PER_SEC)
    parser.add_argument("--loop", type=parse_bool, default=False)
    parser.add_argument("--kafka-bootstrap", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--dedupe", type=parse_bool, default=REPLAY_DEDUPE)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--metrics-port", type=int, default=9101)
    parser.add_argument("--metrics-hold-seconds", type=int, default=0)
    args = parser.parse_args()

    if start_http_server:
        start_http_server(args.metrics_port)
        logger.info("Prometheus metrics listening on :%d", args.metrics_port)

    source = args.source or default_source_for_platform(args.platform)
    replay(source, args.platform, args.rate, args.loop, args.kafka_bootstrap, args.dedupe, args.max_records)
    if args.metrics_hold_seconds > 0:
        logger.info("Keeping metrics endpoint alive for %d seconds", args.metrics_hold_seconds)
        time.sleep(args.metrics_hold_seconds)


if __name__ == "__main__":
    main()
