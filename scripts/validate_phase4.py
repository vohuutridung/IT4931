#!/usr/bin/env python3
"""SOP Phase 4 validation for streaming, Redis, and Cassandra outputs."""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from pathlib import Path

import redis
from cassandra.cluster import Cluster

from config.settings import (
    CASSANDRA_ENRICHMENTS_TABLE,
    CASSANDRA_KEYSPACE,
    REDIS_HOST,
    REDIS_PORT,
)
from ingestion import simulator


KAFKA_BOOTSTRAP = "kafka:29092"


def main() -> None:
    run_id = f"phase4_{uuid.uuid4().hex[:10]}"
    post_id = f"reddit_{run_id}"
    raw = {
        "post_id": run_id,
        "title": f"Phase 4 validation {run_id}",
        "selftext": "Great amazing streaming analytics #Phase4Validation",
        "author_fullname": f"t2_{run_id}",
        "author": "validator",
        "subreddit": "sop_validation",
        "created_utc_raw": int(time.time()),
        "upvotes": 8,
        "comment_count": 2,
        "crossposts_count": 1,
        "comments": [],
    }
    source = Path(tempfile.gettempdir()) / f"{run_id}.jsonl"
    source.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")
    simulator.replay(source, "reddit", 100, False, KAFKA_BOOTSTRAP)

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    session = Cluster(["cassandra"]).connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)

    redis_match = None
    cassandra_match = None
    deadline = time.time() + 120
    while time.time() < deadline:
        for key in redis_client.scan_iter("rt:stats:reddit:*"):
            fields = redis_client.hgetall(key)
            if fields and fields.get("top_hashtag") == "phase4validation":
                redis_match = {"key": key, "fields": fields}
                break
        row = session.execute(
            f"SELECT post_id, sentiment_score FROM {CASSANDRA_ENRICHMENTS_TABLE} WHERE post_id = %s",
            (post_id,),
        ).one()
        if row and row.sentiment_score is not None:
            cassandra_match = {"post_id": row.post_id, "sentiment_score": row.sentiment_score}
        if redis_match and cassandra_match:
            break
        time.sleep(5)

    assert redis_match, "Redis rt:stats:reddit:* key with expected top_hashtag was not found"
    assert cassandra_match, "Cassandra enrichment row with non-null sentiment_score was not found"
    print(json.dumps({"redis": redis_match, "cassandra": cassandra_match}, sort_keys=True))


if __name__ == "__main__":
    main()
