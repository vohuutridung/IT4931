"""Realtime view writer — Writes streaming data to Redis and Elasticsearch."""

from __future__ import annotations

import json
import logging
from collections import defaultdict, Counter
from datetime import datetime, timezone

from config.settings import (
    ES_HOST,
    ES_REALTIME_INDEX,
    REDIS_HOST,
    REDIS_PORT,
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


def _created_at_to_dt(ts: str | None) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    try:
        s = str(ts).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


class RealtimeViewWriter:
    """Writes enriched realtime posts to Redis and Elasticsearch."""

    def __init__(self) -> None:
        # Initialize Redis
        self.redis = None
        try:
            import redis as redis_lib
            self.redis = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.redis.ping()
            logger.info("RealtimeViewWriter: connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)
        except Exception as exc:
            logger.warning("Redis client unavailable for RealtimeViewWriter: %s", exc)

        # Initialize Elasticsearch
        from serving.es_indexer import ElasticsearchIndexer
        self.es = ElasticsearchIndexer(host=ES_HOST)
        try:
            self.es.ensure_indices()
            logger.info("RealtimeViewWriter: verified Elasticsearch indices at %s", ES_HOST)
        except Exception as exc:
            logger.warning("Elasticsearch not ready for RealtimeViewWriter: %s", exc)

    def write(self, posts: list[dict]) -> None:
        """Insert a batch of enriched posts into Redis and Elasticsearch."""
        if not posts:
            return

        # 2. Write to Redis
        if self.redis:
            try:
                self._write_redis(posts)
            except Exception as exc:
                logger.error("Realtime Redis cache update failed: %s", exc)

        # 3. Write to Elasticsearch
        try:
            self._write_elasticsearch(posts)
        except Exception as exc:
            logger.error("Realtime ES indexing failed: %s", exc)

    def _write_redis(self, posts: list[dict]) -> None:
        pipe = self.redis.pipeline(transaction=False)
        stats: dict[tuple[str, str], list[float]] = defaultdict(list)
        stats_hashtags: dict[tuple[str, str], Counter] = defaultdict(Counter)
        hashtag_counts: dict[tuple[str, str], Counter] = defaultdict(Counter)

        for post in posts:
            platform = post.get("platform") or post.get("source") or "unknown"
            created = _created_at_to_dt(post.get("created_at") or post.get("event_time"))
            window_start = created.replace(minute=0, second=0, microsecond=0).isoformat()
            enrichment = post.get("enrichment") or {}
            stats[(platform, window_start)].append(float(enrichment.get("sentiment_score") or 0.0))
            tags = [tag.lower() for tag in post.get("hashtags") or []]
            stats_hashtags[(platform, window_start)].update(tags)
            hashtag_counts[(platform, window_start)].update(tags)
            hashtag_counts[("__all__", window_start)].update(tags)

        for (platform, window_start), sentiments in stats.items():
            key = f"rt:stats:{platform}:{window_start}"
            top_hashtag = ""
            if stats_hashtags[(platform, window_start)]:
                top_hashtag = stats_hashtags[(platform, window_start)].most_common(1)[0][0]
            sentiment_sum = sum(sentiments)
            sentiment_count = len(sentiments)
            pipe.hincrby(key, "post_count", sentiment_count)
            pipe.hincrbyfloat(key, "sentiment_sum", sentiment_sum)
            pipe.hincrby(key, "sentiment_count", sentiment_count)
            pipe.hset(
                key,
                mapping={
                    "top_hashtag": top_hashtag,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            pipe.expire(key, 7200)

        for (platform, window_start), counts in hashtag_counts.items():
            key = f"rt:hashtags:{platform}:{window_start}"
            if counts:
                pipe.zincrby(key, 0, "__init__")
                for tag, count in counts.items():
                    pipe.zincrby(key, count, tag)
                pipe.zrem(key, "__init__")
                pipe.expire(key, 3600)
        pipe.execute()

    def _write_elasticsearch(self, posts: list[dict]) -> None:
        docs = []
        for post in posts:
            enrichment = post.get("enrichment") or {}
            platform = post.get("platform") or post.get("source")
            event_dt = _created_at_to_dt(post.get("created_at") or post.get("event_time"))
            metrics = post.get("metrics") or post.get("engagement") or {}
            docs.append(
                {
                    "doc_id": f"realtime:{post.get('post_id')}",
                    "view": "recent_posts",
                    "post_id": post.get("post_id"),
                    "source": platform,
                    "platform": platform,
                    "author_id": post.get("author_id"),
                    "content": post.get("content"),
                    "hashtags": post.get("hashtags") or [],
                    "event_ts": event_dt.isoformat(),
                    "engagement_score": int(metrics.get("likes") or 0)
                    + int(metrics.get("comments") or 0) * 2
                    + int(metrics.get("shares") or 0) * 3,
                    "sentiment_score": enrichment.get("sentiment_score"),
                    "sentiment_label": enrichment.get("sentiment_label"),
                    "keywords": enrichment.get("keywords") or [],
                    "entities": enrichment.get("entities") or [],
                    "language": enrichment.get("language"),
                    "processed_at": enrichment.get("processed_at") or datetime.now(timezone.utc).isoformat(),
                }
            )
        self.es.bulk_index(ES_REALTIME_INDEX, docs)

