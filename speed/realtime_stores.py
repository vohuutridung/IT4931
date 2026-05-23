"""Realtime view writers for Redis, Cassandra, and Elasticsearch."""

from __future__ import annotations

import json
import logging
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Iterable

from serving.es_indexer import ElasticsearchIndexer
from config.settings import (
    CASSANDRA_ENRICHMENTS_TABLE,
    CASSANDRA_HOSTS,
    CASSANDRA_KEYSPACE,
    ES_REALTIME_INDEX,
    REDIS_HOST,
    REDIS_PORT,
)

logger = logging.getLogger(__name__)


def _created_at_to_dt(value) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


class RealtimeViewWriter:
    def __init__(self) -> None:
        self.redis = self._connect_redis()
        self.cassandra = self._connect_cassandra()
        self.es = ElasticsearchIndexer()
        try:
            self.es.ensure_indices()
        except Exception as exc:
            logger.warning("Elasticsearch not ready: %s", exc)

    def __del__(self) -> None:
        """Close Cassandra session properly."""
        if hasattr(self, "cassandra") and self.cassandra:
            try:
                self.cassandra.shutdown()
            except Exception as exc:
                logger.warning("Error closing Cassandra session: %s", exc)

    @staticmethod
    def _connect_redis():
        try:
            import redis

            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            client.ping()
            return client
        except Exception as exc:
            logger.warning("Redis unavailable: %s", exc)
            return None

    @staticmethod
    def _connect_cassandra():
        try:
            from cassandra.cluster import Cluster

            hosts = [host.strip() for host in CASSANDRA_HOSTS.split(",") if host.strip()]
            session = Cluster(hosts).connect()
            session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                """
            )
            session.set_keyspace(CASSANDRA_KEYSPACE)
            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_ENRICHMENTS_TABLE} (
                    post_id TEXT PRIMARY KEY,
                    sentiment_score FLOAT,
                    sentiment_label TEXT,
                    keywords LIST<TEXT>,
                    entities TEXT,
                    language TEXT,
                    processed_at TIMESTAMP,
                    model_version TEXT
                )
                """
            )
            return session
        except Exception as exc:
            logger.warning("Cassandra unavailable: %s", exc)
            return None

    def write(self, posts: list[dict]) -> None:
        if not posts:
            return
        if self.redis:
            self._write_redis(posts)
        if self.cassandra:
            self._write_cassandra(posts)
        self._write_elasticsearch(posts)

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

    def _write_cassandra(self, posts: list[dict]) -> None:
        query = (
            f"INSERT INTO {CASSANDRA_ENRICHMENTS_TABLE} "
            "(post_id, sentiment_score, sentiment_label, keywords, entities, language, processed_at, model_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        prepared = self.cassandra.prepare(query)
        for post in posts:
            enrichment = post.get("enrichment") or {}
            processed_at = enrichment.get("processed_at")
            try:
                processed_dt = datetime.fromisoformat(str(processed_at).replace("Z", "+00:00"))
            except Exception:
                processed_dt = datetime.now(timezone.utc)
            self.cassandra.execute(
                prepared,
                (
                    str(post.get("post_id")),
                    float(enrichment.get("sentiment_score") or 0.0),
                    str(enrichment.get("sentiment_label") or "neutral"),
                    [str(item) for item in enrichment.get("keywords") or []],
                    json.dumps(enrichment.get("entities") or [], ensure_ascii=False),
                    str(enrichment.get("language") or "unknown"),
                    processed_dt,
                    str(enrichment.get("model_version") or "unknown"),
                ),
            )

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
        try:
            self.es.bulk_index(ES_REALTIME_INDEX, docs)
        except Exception as exc:
            logger.error("Realtime ES index failed: %s", exc)
            raise
