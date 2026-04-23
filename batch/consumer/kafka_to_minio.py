import io
import json
import logging
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from minio import Minio

from shared.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_BATCH, KAFKA_CONSUMER_GROUP,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET_RAW, MINIO_RAW_PREFIX, MINIO_USE_SSL,
    CONSUMER_FLUSH_SIZE, CONSUMER_FLUSH_INTERVAL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger("kafka_to_minio")


# ── PyArrow schema ────────────────────────────────────────────────
ARROW_SCHEMA = pa.schema([
    pa.field("post_id",     pa.string()),
    pa.field("source",      pa.string()),
    pa.field("event_time",  pa.timestamp("ms", tz="UTC")),
    pa.field("ingest_time", pa.timestamp("ms", tz="UTC")),
    pa.field("author_id",   pa.string()),
    pa.field("author_name", pa.string()),
    pa.field("content",     pa.string()),
    pa.field("url",         pa.string()),
    pa.field("hashtags",    pa.list_(pa.string())),
    pa.field("likes",       pa.int64()),  # Fix: int64 to prevent overflow
    pa.field("comments",    pa.int64()),  # Fix: int64 to prevent overflow
    pa.field("shares",      pa.int64()),  # Fix: int64 to prevent overflow
    pa.field("score",       pa.int64()),  # Fix: int64 to prevent overflow
    pa.field("extra",       pa.string()),
])

_shutdown = False


def _on_signal(sig, _):
    global _shutdown
    _shutdown = True
    logger.info("Signal %s — shutting down…", sig)


signal.signal(signal.SIGINT,  _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


# ── MinIO ────────────────────────────────────────────────────────
def make_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_USE_SSL,
    )
    if not client.bucket_exists(MINIO_BUCKET_RAW):
        client.make_bucket(MINIO_BUCKET_RAW)
        logger.info("Created bucket: %s", MINIO_BUCKET_RAW)
    return client


# ── Flatten record ───────────────────────────────────────────────
def flatten(record: dict) -> dict:
    eng = record.get("engagement") or {}
    return {
        "post_id":     record.get("post_id", ""),
        "source":      record.get("source", ""),
        "event_time":  record.get("event_time"),
        "ingest_time": record.get("ingest_time"),
        "author_id":   record.get("author_id") or "",
        "author_name": record.get("author_name") or "",
        "content":     record.get("content", ""),
        "url":         record.get("url") or "",
        "hashtags":    record.get("hashtags") or [],
        "likes":       int(eng.get("likes", 0)),
        "comments":    int(eng.get("comments", 0)),
        "shares":      int(eng.get("shares", 0)),
        "score":       int(eng.get("score", 0)),
        "extra":       json.dumps(record.get("extra") or {}),  # Fix: JSON serialize extra dict
    }


# ── Flush → MinIO ───────────────────────────────────────────────
def flush_to_minio(minio_client, buffer, source, event_date_str):
    if not buffer:
        return

    # Parse event_date_str (YYYY-MM-DD-HH) back to datetime
    event_date = datetime.strptime(event_date_str, "%Y-%m-%d-%H")

    cols = defaultdict(list)
    for row in buffer:
        for k, v in row.items():
            cols[k].append(v)

    for ts_col in ("event_time", "ingest_time"):
        # Fix: Convert int ms to datetime objects before creating Arrow array
        cols[ts_col] = pa.array(
            [datetime.fromtimestamp(x / 1000, tz=timezone.utc) for x in cols[ts_col]],
            type=pa.timestamp("ms", tz="UTC")
        )

    table = pa.table(dict(cols), schema=ARROW_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    size = buf.getbuffer().nbytes
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

    obj_key = (
        f"{MINIO_RAW_PREFIX}/{source}/"
        f"{event_date.year:04d}/{event_date.month:02d}/{event_date.day:02d}/{event_date.hour:02d}/"
        f"{ts_str}.parquet"
    )

    minio_client.put_object(
        MINIO_BUCKET_RAW,
        obj_key,
        buf,
        size,
    )

    logger.info("Flushed %d records → %s", len(buffer), obj_key)


# ── Main loop ───────────────────────────────────────────────────
def run():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC_BATCH])
    minio = make_minio_client()

    buffers = defaultdict(list)
    last_flush = time.monotonic()
    total_sent = 0

    logger.info("Consumer started")

    try:
        while not _shutdown:
            msg = consumer.poll(1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
            else:
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    row = flatten(record)

                    # Fix: Skip records with null event_time to prevent crash
                    if row["event_time"] is None:
                        logger.warning("Skipping record with null event_time: %s", record.get("post_id"))
                        continue

                    source = row["source"]
                    evt_dt = datetime.fromtimestamp(
                        row["event_time"] / 1000,
                        tz=timezone.utc
                    )

                    buffers[(source, evt_dt.strftime("%Y-%m-%d-%H"))].append(row)

                except Exception as e:
                    logger.warning("Parse error: %s", e)

            # Flush condition
            elapsed = time.monotonic() - last_flush
            total_buf = sum(len(v) for v in buffers.values())

            if (
                total_buf >= CONSUMER_FLUSH_SIZE or
                elapsed >= CONSUMER_FLUSH_INTERVAL
            ) and total_buf > 0:

                for (src, date_str), rows in list(buffers.items()):
                    flush_to_minio(
                        minio,
                        rows,
                        src,
                        date_str
                    )
                    total_sent += len(rows)
                    del buffers[(src, date_str)]

                consumer.commit()
                last_flush = time.monotonic()
                logger.info("Committed | total_sent=%d", total_sent)

    finally:
        for (src, date_str), rows in buffers.items():
            if rows:
                flush_to_minio(
                    minio,
                    rows,
                    src,
                    date_str
                )

        consumer.commit()
        consumer.close()
        logger.info("Shutdown | total_sent=%d", total_sent)


if __name__ == "__main__":
    run()