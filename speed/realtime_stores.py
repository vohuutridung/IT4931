"""Realtime view writer — Writes streaming data to ClickHouse serving layer."""

from __future__ import annotations

import json
import logging
import requests
import time
from datetime import datetime, timezone
from config.settings import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_WRITE_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)


def _normalize_ts(ts: str | None, fallback: str) -> str:
    """Convert ISO 8601 timestamp to ClickHouse DateTime format (YYYY-MM-DD HH:MM:SS UTC)."""
    if not ts:
        return fallback
    try:
        s = str(ts).strip()
        # Replace trailing Z with +00:00 for fromisoformat compatibility
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        # Convert to UTC if timezone-aware, strip timezone for CH DateTime
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return fallback


class RealtimeViewWriter:
    """Writes enriched realtime posts to ClickHouse realtime_posts table."""

    def __init__(self) -> None:
        self.session = requests.Session()
        logger.info("RealtimeViewWriter: initialized writing to ClickHouse at %s", CLICKHOUSE_HOST)

    def write(self, posts: list[dict]) -> None:
        """Insert a batch of enriched posts into ClickHouse."""
        if not posts:
            return

        docs = []
        loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for post in posts:
            enrichment = post.get("enrichment") or {}
            # Flatten/map structure to match ClickHouse realtime_posts schema
            doc = {
                "post_id": post.get("post_id"),
                "platform": post.get("platform"),
                "author_id": post.get("author_id"),
                "content": post.get("content") or "",
                "hashtags": post.get("hashtags") or [],
                "sentiment": float(enrichment.get("sentiment_score", 0.0)),
                "event_ts": _normalize_ts(post.get("created_at"), loaded_at),
                "loaded_at": loaded_at,
            }
            docs.append(doc)

        payload = "\n".join(json.dumps(doc, ensure_ascii=False) for doc in docs).encode("utf-8")
        sql = "INSERT INTO realtime_posts FORMAT JSONEachRow\n"
        body = sql.encode("utf-8") + payload

        params = {
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "database": CLICKHOUSE_DATABASE,
        }

        # Retry logic with exponential backoff
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(
                    CLICKHOUSE_HOST,
                    params=params,
                    data=body,
                    timeout=CLICKHOUSE_WRITE_TIMEOUT,
                )
                response.raise_for_status()
                logger.info("RealtimeViewWriter: inserted %d records to ClickHouse", len(docs))
                return
            except requests.RequestException as exc:
                if attempt < MAX_RETRIES - 1:
                    backoff = RETRY_BACKOFF_BASE ** (attempt + 1)
                    logger.warning("RealtimeViewWriter ClickHouse write failed (attempt %d/%d), retrying in %.1fs: %s", attempt + 1, MAX_RETRIES, backoff, exc)
                    time.sleep(backoff)
                else:
                    logger.error("RealtimeViewWriter ClickHouse write failed after %d attempts: %s", MAX_RETRIES, exc)
                    # Graceful fallback: do not crash the stream
                    return
