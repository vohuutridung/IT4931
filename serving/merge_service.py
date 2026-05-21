"""Serving Layer merge logic for batch and real-time views."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import logging

import requests

from config.settings import ES_HOST, REALTIME_WINDOW_HOURS, REDIS_HOST, REDIS_PORT

logger = logging.getLogger(__name__)


class ServeQuery:
    def __init__(self, es_host: str = ES_HOST, redis_host: str = REDIS_HOST, redis_port: int = REDIS_PORT) -> None:
        self.es_host = es_host.rstrip("/")
        self.redis_host = redis_host
        self.redis_port = redis_port
        self._redis = None
        try:
            import redis

            self._redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        except Exception:
            self._redis = None

    def _search(self, index: str, query: dict, size: int = 100) -> list[dict]:
        try:
            response = requests.post(
                f"{self.es_host}/{index}/_search",
                json={"query": query, "size": size},
                timeout=3,
            )
            response.raise_for_status()
            return [hit.get("_source", {}) for hit in response.json().get("hits", {}).get("hits", [])]
        except Exception:
            return []

    @staticmethod
    def _parse_dt(value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _dedupe(posts: list[dict], limit: int) -> list[dict]:
        seen: set[str] = set()
        merged: list[dict] = []
        for post in posts:
            post_id = str(post.get("post_id") or "")
            if not post_id or post_id in seen:
                continue
            seen.add(post_id)
            merged.append(post)
            if len(merged) >= limit:
                break
        return merged

    def query_posts(self, platform: str | None, start_dt: datetime | str, end_dt: datetime | str, limit: int = 100) -> list[dict]:
        start = self._parse_dt(start_dt)
        end = self._parse_dt(end_dt)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=REALTIME_WINDOW_HOURS)
        filters: list[dict[str, Any]] = [{"range": {"event_ts": {"gte": start.isoformat(), "lte": end.isoformat()}}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        query = {"bool": {"filter": filters}}

        posts: list[dict] = []
        if end >= cutoff:
            posts.extend(self._search("social_realtime_views", query, limit))
        if start < cutoff:
            posts.extend(self._search("social_batch_views", query, limit))
        return self._dedupe(posts, limit)

    def query_sentiment_trend(self, platform: str | None, granularity: str, start_dt: datetime | str, end_dt: datetime | str) -> list[dict]:
        start = self._parse_dt(start_dt)
        end = self._parse_dt(end_dt)

        # 1. Fetch from batch views
        filters: list[dict[str, Any]] = [{"term": {"view": "sentiment_time_series"}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        batch_results = self._search("social_batch_views", {"bool": {"filter": filters}}, 1000)

        # 2. Fetch realtime aggregations
        rt_filters: list[dict[str, Any]] = [
            {"range": {"event_ts": {"gte": start.isoformat(), "lte": end.isoformat()}}}
        ]
        if platform:
            rt_filters.append({"term": {"platform": platform}})

        aggs = {
            "trend": {
                "date_histogram": {
                    "field": "event_ts",
                    "calendar_interval": granularity,
                },
                "aggs": {
                    "avg_sentiment": {
                        "avg": {"field": "sentiment_score"}
                    }
                }
            }
        }

        rt_results = []
        try:
            response = requests.post(
                f"{self.es_host}/social_realtime_views/_search",
                json={"query": {"bool": {"filter": rt_filters}}, "size": 0, "aggs": aggs},
                timeout=3,
            )
            response.raise_for_status()
            buckets = response.json().get("aggregations", {}).get("trend", {}).get("buckets", [])
            for b in buckets:
                val = b.get("avg_sentiment", {}).get("value")
                if val is not None:
                    rt_results.append({
                        "event_hour": b["key_as_string"],
                        "avg_sentiment": val,
                        "platform": platform or "all",
                        "view": "sentiment_time_series"
                    })
        except Exception as exc:
            logger.warning("Failed to fetch realtime sentiment aggregation: %s", exc)

        # 3. Merge: Realtime overrides Batch for the same time bucket
        merged = {r.get("event_hour"): r for r in batch_results if r.get("event_hour")}
        merged.update({r.get("event_hour"): r for r in rt_results if r.get("event_hour")})
        
        return list(merged.values())

    def query_top_hashtags(self, platform: str | None, window_hours: int, top_n: int) -> list[dict]:
        if self._redis:
            keys = self._redis.keys("rt:hashtags:*")
            counts: dict[str, float] = {}
            for key in keys:
                for tag, score in self._redis.zrevrange(key, 0, top_n - 1, withscores=True):
                    counts[tag] = counts.get(tag, 0.0) + float(score)
            if counts:
                return [
                    {"hashtag": tag, "frequency": score}
                    for tag, score in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:top_n]
                ]
        filters: list[dict[str, Any]] = [{"term": {"view": "top_hashtags_weekly"}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        return self._search("social_batch_views", {"bool": {"filter": filters}}, top_n)

    def query_realtime_stats(self, platform: str | None = None) -> dict:
        if not self._redis:
            return {"platform": platform, "stats": []}
        pattern = f"rt:stats:{platform}:*" if platform else "rt:stats:*"
        return {"platform": platform, "stats": [self._redis.hgetall(key) | {"key": key} for key in self._redis.keys(pattern)]}
