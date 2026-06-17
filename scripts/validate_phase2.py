#!/usr/bin/env python3
"""SOP Phase 2 validation for simulator Kafka and DLQ behavior."""

from __future__ import annotations

import json
import re
import tempfile
import time
import uuid
from pathlib import Path
from typing import Callable

from confluent_kafka import Consumer

from config.settings import KAFKA_SOURCE_TOPICS, KAFKA_TOPIC_DLQ
from ingestion import simulator


KAFKA_BOOTSTRAP = "kafka:29092"


def _write_jsonl(path: Path, record: dict) -> None:
    path.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")


def _find_message(topic: str, predicate: Callable[[dict], bool], run_id: str) -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"phase2-validator-{run_id}-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    deadline = time.time() + 30
    try:
        while time.time() < deadline:
            msg = consumer.poll(1)
            if msg is None or msg.error():
                continue
            value = json.loads(msg.value().decode("utf-8"))
            if predicate(value):
                return value
    finally:
        consumer.close()
    raise RuntimeError(f"Timed out waiting for validation message on {topic}")


def main() -> None:
    run_id = f"phase2_{uuid.uuid4().hex[:10]}"
    good = {
        "post_id": run_id,
        "title": f"SOP validation {run_id}",
        "selftext": "Canonical #Validation",
        "author_fullname": f"t2_{run_id}",
        "author": "validator",
        "subreddit": "sop_validation",
        "created_utc_raw": 1_700_000_000,
        "upvotes": 3,
        "comment_count": 1,
        "crossposts_count": 0,
        "comments": [],
    }
    bad = {
        "title": f"bad {run_id}",
        "selftext": "missing post id",
        "author_fullname": f"t2_bad_{run_id}",
        "subreddit": "sop_validation",
        "created_utc_raw": 1_700_000_000,
    }

    good_path = Path(tempfile.gettempdir()) / f"{run_id}_good.jsonl"
    bad_path = Path(tempfile.gettempdir()) / f"{run_id}_bad.jsonl"
    _write_jsonl(good_path, good)
    _write_jsonl(bad_path, bad)

    simulator.replay(good_path, "reddit", 100, False, KAFKA_BOOTSTRAP)
    simulator.replay(bad_path, "reddit", 100, False, KAFKA_BOOTSTRAP)

    source = _find_message(
        KAFKA_SOURCE_TOPICS["reddit"],
        lambda value: value.get("post_id") == f"reddit_{run_id}",
        run_id,
    )
    dlq = _find_message(
        KAFKA_TOPIC_DLQ,
        lambda value: value.get("raw", {}).get("title") == f"bad {run_id}",
        run_id,
    )

    required = {
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
    assert set(source) == required, source
    assert source["platform"] == "reddit", source
    assert source["source_id"] == "sop_validation", source
    assert re.fullmatch(r"[0-9a-f]{64}", source["author_id"]), source
    assert isinstance(source["comments"], list), source
    assert source["metrics"] == {"likes": 3, "comments": 1, "shares": 0, "views": 0}, source
    assert dlq["platform"] == "reddit", dlq
    assert "Missing post_id" in dlq["error"], dlq

    print(
        json.dumps(
            {
                "source": source,
                "dlq_error": dlq["error"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
