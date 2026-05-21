#!/usr/bin/env python3
"""Kafka to MinIO raw Parquet writer for the Batch Layer."""

from __future__ import annotations

import io
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

from config.settings import (
    CONSUMER_FLUSH_INTERVAL,
    KAFKA_ALL_SOURCE_TOPICS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_ENRICHED,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_KEY,
    S3_WRITE_TIMEOUT,
    STORAGE_RAW_BASE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("object_store_writer")

RUNNING = True
SCHEMA = pa.schema([
    pa.field("post_id", pa.string()),
    pa.field("platform", pa.string()),
    pa.field("source_id", pa.string()),
    pa.field("created_at", pa.timestamp("ms", tz="UTC")),
    pa.field("ingested_at", pa.timestamp("ms", tz="UTC")),
    pa.field("author_id", pa.string()),
    pa.field("content", pa.string()),
    pa.field("title", pa.string()),
    pa.field("media_urls", pa.list_(pa.string())),
    pa.field("hashtags", pa.list_(pa.string())),
    pa.field("comments_json", pa.string()),
    pa.field("likes", pa.int64()),
    pa.field("comments", pa.int64()),
    pa.field("shares", pa.int64()),
    pa.field("views", pa.int64()),
    pa.field("sentiment_score", pa.float64()),
    pa.field("sentiment_label", pa.string()),
    pa.field("keywords", pa.list_(pa.string())),
    pa.field("language", pa.string()),
    pa.field("raw_json", pa.string()),
])


def stop(_sig, _frame) -> None:
    global RUNNING
    RUNNING = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def ensure_bucket(client) -> None:
    try:
        client.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        client.create_bucket(Bucket=S3_BUCKET)


def parse_object_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"}:
        raise ValueError(f"Expected s3:// or s3a:// URI, got {uri}")
    return parsed.netloc, parsed.path.strip("/")


def flatten(record: dict) -> dict:
    metrics = record.get("metrics") or record.get("engagement") or {}
    created_at = _parse_timestamp(record.get("created_at"), record.get("event_time"))
    ingested_at = _parse_timestamp(record.get("ingested_at"), record.get("ingest_time"))
    enrichment = record.get("enrichment") or {}
    return {
        "post_id": str(record.get("post_id") or ""),
        "platform": str(record.get("platform") or record.get("source") or ""),
        "source_id": str(record.get("source_id") or ""),
        "created_at": created_at,
        "ingested_at": ingested_at,
        "author_id": str(record.get("author_id") or ""),
        "content": str(record.get("content") or ""),
        "title": str(record.get("title") or ""),
        "media_urls": _string_list(record.get("media_urls")),
        "hashtags": _string_list(record.get("hashtags")),
        "comments_json": json.dumps(record.get("comments") or [], ensure_ascii=False, default=str),
        "likes": _non_negative_int(metrics.get("likes")),
        "comments": _non_negative_int(metrics.get("comments")),
        "shares": _non_negative_int(metrics.get("shares")),
        "views": _non_negative_int(metrics.get("views", metrics.get("video_views"))),
        "sentiment_score": float(enrichment["sentiment_score"]) if "sentiment_score" in enrichment else None,
        "sentiment_label": str(enrichment.get("sentiment_label") or "neutral"),
        "keywords": _string_list(enrichment.get("keywords")),
        "language": str(enrichment.get("language") or "en"),
        "raw_json": json.dumps(record, ensure_ascii=False, default=str),
    }


def partition_key(row: dict) -> tuple[str, int, int, int]:
    dt = row["created_at"]
    return row["platform"], dt.year, dt.month, dt.day


def _parse_timestamp(iso_value, epoch_value=None) -> datetime:
    if iso_value:
        if isinstance(iso_value, datetime):
            return iso_value.astimezone(timezone.utc)
        value = str(iso_value)
        if value.endswith("Z"):
            value = f"{value[:-1]}+00:00"
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    if epoch_value:
        epoch = float(epoch_value)
        if epoch > 10_000_000_000:
            epoch = epoch / 1000
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    raise ValueError("missing timestamp")


def _non_negative_int(value) -> int:
    try:
        return max(int(float(value or 0)), 0)
    except (TypeError, ValueError):
        return 0


def _string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value) for value in values if value not in {None, ""}]


def _write_parquet_bytes(rows: list[dict]) -> bytes:
    cols = {field.name: [] for field in SCHEMA}
    for row in rows:
        for field in SCHEMA:
            cols[field.name].append(row.get(field.name))
    buf = io.BytesIO()
    pq.write_table(pa.table(cols, schema=SCHEMA), buf, compression="snappy")
    return buf.getvalue()


def write_rows(client, rows: list[dict], key: tuple[str, int, int, int]) -> None:
    platform, year, month, day = key
    bucket, prefix = parse_object_uri(STORAGE_RAW_BASE)
    out_prefix = f"{prefix.rstrip('/')}/{platform}/year={year:04d}/month={month:02d}/day={day:02d}"
    filename = f"part-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}.parquet"
    object_key = f"{out_prefix}/{filename}"
    
    body = _write_parquet_bytes(rows)
    
    # Retry logic with exponential backoff
    for attempt in range(MAX_RETRIES):
        try:
            client.put_object(Bucket=bucket, Key=object_key, Body=body)
            logger.info("Wrote %d rows to s3://%s/%s", len(rows), bucket, object_key)
            return
        except Exception as exc:
            if attempt < MAX_RETRIES - 1:
                backoff = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning("S3 write failed (attempt %d/%d), retrying in %.1fs: %s", attempt + 1, MAX_RETRIES, backoff, exc)
                time.sleep(backoff)
            else:
                logger.error("S3 write failed after %d attempts: %s", MAX_RETRIES, exc)
                raise


def flush(client, buffers: dict[tuple[str, int, int, int], list[dict]]) -> int:
    count = 0
    for key, rows in list(buffers.items()):
        if rows:
            write_rows(client, rows, key)
            count += len(rows)
            del buffers[key]
    return count


def run() -> None:
    client = s3_client()
    ensure_bucket(client)
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": os.getenv("KAFKA_CONSUMER_GROUP", "batch-consumer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(KAFKA_ALL_SOURCE_TOPICS))
    buffers: dict[tuple[str, int, int, int], list[dict]] = defaultdict(list)
    last_flush = time.monotonic()
    total = 0
    flush_size = int(os.getenv("CONSUMER_FLUSH_SIZE", "500"))

    try:
        while RUNNING:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
            else:
                try:
                    row = flatten(json.loads(msg.value().decode("utf-8")))
                    if not row["platform"] or not row["created_at"]:
                        raise ValueError("missing platform or created_at")
                    buffers[partition_key(row)].append(row)
                except Exception as exc:
                    logger.warning("Skipping malformed Kafka message: %s", exc)

            buffered = sum(len(rows) for rows in buffers.values())
            if buffered >= flush_size or (buffered and time.monotonic() - last_flush >= CONSUMER_FLUSH_INTERVAL):
                total += flush(client, buffers)
                consumer.commit()
                last_flush = time.monotonic()
                logger.info("Committed offsets after object store write | total=%d", total)
    finally:
        total += flush(client, buffers)
        consumer.commit()
        consumer.close()
        logger.info("Stopped | total=%d", total)


if __name__ == "__main__":
    run()
