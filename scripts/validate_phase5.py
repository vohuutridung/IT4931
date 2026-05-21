#!/usr/bin/env python3
"""SOP Phase 5 validation for Elasticsearch serving and merge queries."""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from config.settings import ES_BATCH_INDEX, ES_REALTIME_INDEX
from ingestion import simulator
from serving.merge_service import ServeQuery


ES_HOST = "http://elasticsearch:9200"
KAFKA_BOOTSTRAP = "kafka:29092"


def _count(index: str) -> int:
    response = requests.get(f"{ES_HOST}/{index}/_count", timeout=5)
    response.raise_for_status()
    return int(response.json()["count"])


def main() -> None:
    batch_count = _count(ES_BATCH_INDEX)
    assert batch_count > 0, "social_batch_views is empty"

    run_id = f"phase5_{uuid.uuid4().hex[:10]}"
    post_id = f"reddit_{run_id}"
    raw = {
        "post_id": run_id,
        "title": f"Phase 5 realtime {run_id}",
        "selftext": "Great serving merge validation #Phase5Validation",
        "author_fullname": f"t2_{run_id}",
        "author": "validator",
        "subreddit": "sop_validation",
        "created_utc_raw": int(time.time()),
        "upvotes": 9,
        "comment_count": 3,
        "crossposts_count": 1,
        "comments": [],
    }
    source = Path(tempfile.gettempdir()) / f"{run_id}.jsonl"
    source.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")

    started = time.monotonic()
    simulator.replay(source, "reddit", 100, False, KAFKA_BOOTSTRAP)
    realtime_doc = None
    while time.monotonic() - started <= 120:
        response = requests.get(f"{ES_HOST}/{ES_REALTIME_INDEX}/_doc/realtime:{post_id}", timeout=5)
        if response.status_code == 200 and response.json().get("found"):
            realtime_doc = response.json()["_source"]
            break
        time.sleep(1)
    assert realtime_doc, "social_realtime_views did not receive the published record within 120 seconds"

    requests.post(f"{ES_HOST}/{ES_REALTIME_INDEX}/_refresh")
    
    service = ServeQuery(es_host=ES_HOST, redis_host="redis", redis_port=6379)
    historical = service.query_posts(
        "reddit",
        "2023-11-14T00:00:00+00:00",
        "2023-11-15T00:00:00+00:00",
        10,
    )
    assert historical, "query_posts returned no historical batch results"

    now = datetime.now(timezone.utc)
    recent = service.query_posts("reddit", now - timedelta(minutes=5), now + timedelta(minutes=5), 10)
    assert any(doc.get("post_id") == post_id for doc in recent), "query_posts did not return the realtime post"

    kibana = requests.get("http://kibana:5601/api/status", timeout=10)
    assert kibana.status_code < 500, "Kibana status endpoint is unavailable"

    print(
        json.dumps(
            {
                "batch_count": batch_count,
                "realtime_post_id": post_id,
                "historical_results": len(historical),
                "recent_results": len(recent),
                "kibana_status": kibana.status_code,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
