#!/usr/bin/env python3
"""Elasticsearch indexing utilities for the Serving Layer."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from typing import Iterable

import requests

from config.settings import ES_BATCH_ALIAS, ES_BATCH_INDEX, ES_HOST, ES_REALTIME_INDEX

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("es_indexer")


class ElasticsearchIndexer:
    def __init__(self, host: str = ES_HOST) -> None:
        self.host = host.rstrip("/")

    def put_json(self, path: str, payload: dict) -> None:
        response = requests.put(f"{self.host}/{path.lstrip('/')}", json=payload, timeout=10)
        response.raise_for_status()

    def post_json(self, path: str, payload: dict | str, *, ndjson: bool = False) -> None:
        headers = {"Content-Type": "application/x-ndjson"} if ndjson else None
        response = requests.post(f"{self.host}/{path.lstrip('/')}", data=payload if ndjson else None, json=None if ndjson else payload, headers=headers, timeout=30)
        response.raise_for_status()

    def ensure_indices(self) -> None:
        mapping = {
            "mappings": {
                "properties": {
                    "view": {"type": "keyword"},
                    "post_id": {"type": "keyword"},
                    "source": {"type": "keyword"},
                    "platform": {"type": "keyword"},
                    "event_ts": {"type": "date"},
                    "event_date": {"type": "date"},
                    "event_hour": {"type": "date"},
                    "event_week": {"type": "date"},
                    "author_id": {"type": "keyword"},
                    "hashtag": {"type": "keyword"},
                    "content": {"type": "text"},
                    "sentiment_score": {"type": "float"},
                    "sentiment_label": {"type": "keyword"},
                    "engagement_score": {"type": "long"},
                    "processed_at": {"type": "date"},
                }
            }
        }
        for index in (ES_BATCH_INDEX, ES_REALTIME_INDEX):
            response = requests.head(f"{self.host}/{index}", timeout=5)
            if response.status_code == 404:
                self.put_json(index, mapping)
                logger.info("Created ES index %s", index)

        try:
            self.put_json(
                "_ilm/policy/social_realtime_24h",
                {"policy": {"phases": {"hot": {"actions": {}}, "delete": {"min_age": "24h", "actions": {"delete": {}}}}}},
            )
        except Exception as exc:
            logger.warning("Could not create realtime ILM policy: %s", exc)
        self.post_json("_aliases", {"actions": [{"add": {"index": ES_BATCH_INDEX, "alias": ES_BATCH_ALIAS}}]})

    @staticmethod
    def _bulk_payload(index: str, docs: Iterable[dict]) -> str:
        lines: list[str] = []
        import json

        for doc in docs:
            doc_id = doc.get("doc_id") or doc.get("post_id")
            action = {"index": {"_index": index}}
            if doc_id:
                action["index"]["_id"] = str(doc_id)
            lines.append(json.dumps(action, ensure_ascii=False, default=str))
            lines.append(json.dumps(doc, ensure_ascii=False, default=str))
        return "\n".join(lines) + "\n" if lines else ""

    def bulk_index(self, index: str, docs: list[dict]) -> None:
        if not docs:
            return
        payload = self._bulk_payload(index, docs)
        response = requests.post(
            f"{self.host}/_bulk",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        
        if body.get("errors"):
            failures = []
            items = body.get("items", [])
            for i, item in enumerate(items):
                op = item.get("index", {})
                if op.get("error"):
                    doc_id = op.get("_id", i)
                    failures.append({
                        "doc_id": doc_id,
                        "error": op.get("error"),
                        "doc": docs[i] if i < len(docs) else None
                    })
            if failures:
                logger.error("ES bulk index had %d failures:", len(failures))
                for f in failures[:5]:
                    logger.error("  %s: %s", f["doc_id"], f["error"])
                raise RuntimeError(f"Bulk index: {len(failures)}/{len(docs)} docs failed: {failures[:5]}")
            else:
                raise RuntimeError(f"Elasticsearch bulk index reported errors: {body}")

        logger.info("Indexed %d docs into %s", len(docs), index)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ensure", action="store_true", help="Create required indices and aliases")
    parser.add_argument("--retries", type=int, default=30)
    args = parser.parse_args()
    indexer = ElasticsearchIndexer()
    if args.ensure:
        for attempt in range(1, args.retries + 1):
            try:
                indexer.ensure_indices()
                return
            except Exception as exc:
                if attempt == args.retries:
                    raise
                logger.warning("Elasticsearch not ready (%s/%s): %s", attempt, args.retries, exc)
                time.sleep(2)


if __name__ == "__main__":
    main()
