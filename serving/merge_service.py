"""Serving Layer merge logic for batch and real-time views."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import logging

import requests

from config.settings import (
    ES_HOST,
    REALTIME_WINDOW_HOURS,
    REDIS_HOST,
    REDIS_PORT,
)

logger = logging.getLogger(__name__)


class ServeQuery:
    def __init__(self, es_host: str = ES_HOST, redis_host: str = REDIS_HOST, redis_port: int = REDIS_PORT) -> None:
        self.es_host = es_host.rstrip("/")
        self.redis_host = redis_host
        self.redis_port = redis_port
        self._redis = None
        self._session = requests.Session()
        try:
            import redis

            self._redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        except Exception as exc:
            logger.warning("Redis client could not be initialized: %s", exc)
            self._redis = None

    def _search(self, index: str, query: dict, size: int = 100, sort: list[dict] | None = None) -> list[dict]:
        payload: dict[str, Any] = {"query": query, "size": size}
        if sort:
            payload["sort"] = sort
        try:
            response = self._session.post(
                f"{self.es_host}/{index}/_search",
                json=payload,
                timeout=3,
            )
            response.raise_for_status()
            return [hit.get("_source", {}) for hit in response.json().get("hits", {}).get("hits", [])]
        except Exception as exc:
            logger.error("Elasticsearch search failed for index %s: %s", index, exc)
            return []

    @staticmethod
    def _parse_dt(value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _parse_event_ts(post: dict) -> datetime:
        ts = post.get("event_ts")
        if not ts:
            return datetime.min.replace(tzinfo=timezone.utc)
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    @classmethod
    def _parse_time_bucket(cls, value: Any) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

    @classmethod
    def _bucket_key(cls, row: dict, granularity: str) -> str | None:
        ts = cls._parse_time_bucket(row.get("event_hour") or row.get("event_ts"))
        if ts is None:
            return None
        if granularity == "day":
            ts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            ts = ts.replace(minute=0, second=0, microsecond=0)
        return ts.isoformat()

    @staticmethod
    def _parse_redis_window_key(key: str, prefix: str) -> datetime | None:
        if not key.startswith(prefix):
            return None
        try:
            value = key[len(prefix):]
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    @classmethod
    def _dedupe(cls, posts: list[dict], limit: int) -> list[dict]:
        seen: dict[str, dict] = {}
        for post in posts:
            post_id = str(post.get("post_id") or "")
            if not post_id:
                continue
            if post_id not in seen:
                seen[post_id] = post
            else:
                existing = seen[post_id]
                existing_ts = cls._parse_event_ts(existing)
                current_ts = cls._parse_event_ts(post)
                if current_ts > existing_ts:
                    seen[post_id] = post

        unique_ordered: list[dict] = []
        seen_ids = set()
        for post in posts:
            post_id = str(post.get("post_id") or "")
            if post_id and post_id not in seen_ids:
                seen_ids.add(post_id)
                unique_ordered.append(seen[post_id])
                if len(unique_ordered) >= limit:
                    break
        return unique_ordered

    def query_posts(self, platform: str | None, start_dt: datetime | str, end_dt: datetime | str, limit: int = 100) -> list[dict]:
        start = self._parse_dt(start_dt)
        end = self._parse_dt(end_dt)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=REALTIME_WINDOW_HOURS)
        filters: list[dict[str, Any]] = [{"range": {"event_ts": {"gte": start.isoformat(), "lte": end.isoformat()}}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        query = {"bool": {"filter": filters}}
        sort = [{"event_ts": {"order": "desc", "missing": "_last"}}]

        posts: list[dict] = []
        if end >= cutoff:
            posts.extend(self._search("social_realtime_views", query, limit, sort=sort))
        if start < cutoff:
            posts.extend(self._search("social_batch_views", query, limit, sort=sort))
        
        return self._dedupe(posts, limit)

    def query_sentiment_trend(self, platform: str | None, granularity: str, start_dt: datetime | str, end_dt: datetime | str) -> list[dict]:
        start = self._parse_dt(start_dt)
        end = self._parse_dt(end_dt)

        # 1. Fetch from batch views
        filters: list[dict[str, Any]] = [
            {"term": {"view": "sentiment_time_series"}},
            {"range": {"event_hour": {"gte": start.isoformat(), "lte": end.isoformat()}}},
        ]
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
                    "avg_sentiment": {"avg": {"field": "sentiment_score"}},
                    "post_count": {"value_count": {"field": "sentiment_score"}},
                    "positive_count": {"filter": {"range": {"sentiment_score": {"gt": 0.03}}}},
                    "negative_count": {"filter": {"range": {"sentiment_score": {"lt": -0.03}}}},
                },
            }
        }

        rt_results = []
        try:
            response = self._session.post(
                f"{self.es_host}/social_realtime_views/_search",
                json={"query": {"bool": {"filter": rt_filters}}, "size": 0, "aggs": aggs},
                timeout=3,
            )
            response.raise_for_status()
            buckets = response.json().get("aggregations", {}).get("trend", {}).get("buckets", [])
            for b in buckets:
                val = b.get("avg_sentiment", {}).get("value")
                if val is not None:
                    pc = int(b.get("post_count", {}).get("value") or 0)
                    pos = int(b.get("positive_count", {}).get("doc_count") or 0)
                    neg = int(b.get("negative_count", {}).get("doc_count") or 0)
                    rt_results.append({
                        "event_hour": b["key_as_string"],
                        "avg_sentiment": val,
                        "platform": platform or "all",
                        "view": "sentiment_time_series",
                        "post_count": pc,
                        "positive_count": pos,
                        "negative_count": neg,
                        "neutral_count": max(0, pc - pos - neg),
                    })
        except requests.RequestException as exc:
            logger.error("Failed to fetch realtime sentiment aggregation from Elasticsearch: %s", exc)

        # 3. Merge by normalized time bucket. Realtime overrides batch only for
        # the same platform bucket, which avoids duplicate points like
        # "2026-05-21T22:00:00+00:00" and "2026-05-21T22:00:00.000Z".
        merged: dict[tuple[str, str], dict] = {}
        for row in batch_results:
            bucket = self._bucket_key(row, granularity)
            if not bucket:
                continue
            row["event_hour"] = bucket
            key = (str(platform or row.get("platform") or "all"), bucket) if platform else ("all", bucket)
            merged[key] = row

        for row in rt_results:
            bucket = self._bucket_key(row, granularity)
            if not bucket:
                continue
            row["event_hour"] = bucket
            key = (str(platform or row.get("platform") or "all"), bucket) if platform else ("all", bucket)
            merged[key] = row

        # 4. Enrich each bucket with percentage breakdown and inter-bucket velocity.
        for row in merged.values():
            pc = int(row.get("post_count") or 0)
            pos = int(row.get("positive_count") or 0)
            neg = int(row.get("negative_count") or 0)
            neu = int(row.get("neutral_count") or max(0, pc - pos - neg))
            if pc > 0:
                row["positive_pct"] = round(pos / pc * 100, 1)
                row["negative_pct"] = round(neg / pc * 100, 1)
                row["neutral_pct"] = round(neu / pc * 100, 1)

        sorted_rows = sorted(merged.values(), key=lambda r: str(r.get("event_hour") or ""))
        for i, row in enumerate(sorted_rows):
            prev = float(sorted_rows[i - 1].get("avg_sentiment") or 0) if i > 0 else None
            curr = float(row.get("avg_sentiment") or 0)
            row["velocity"] = round(curr - prev, 4) if prev is not None else 0.0

        return sorted_rows

    @staticmethod
    def trend_direction(data: list[dict]) -> str:
        if len(data) < 2:
            return "neutral"
        recent = [float(r.get("avg_sentiment") or 0) for r in data[-6:]]
        slope = recent[-1] - recent[0]
        if slope > 0.05:
            return "bullish"
        if slope < -0.05:
            return "bearish"
        return "neutral"

    def query_hashtag_weeks(self, platform: str | None = None) -> list[str]:
        filters: list[dict[str, Any]] = [{"term": {"view": "top_hashtags_weekly"}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        
        payload = {
            "query": {"bool": {"filter": filters}},
            "size": 0,
            "aggs": {
                "weeks": {
                    "terms": {
                        "field": "event_week",
                        "size": 1000,
                        "order": {"_key": "desc"}
                    }
                }
            }
        }
        try:
            response = self._session.post(
                f"{self.es_host}/social_batch_views/_search",
                json=payload,
                timeout=3,
            )
            response.raise_for_status()
            buckets = response.json().get("aggregations", {}).get("weeks", {}).get("buckets", [])
            return [b.get("key_as_string") or str(b.get("key")) for b in buckets if b.get("key_as_string") or b.get("key") is not None]
        except Exception as exc:
            logger.error("Failed to query hashtag weeks: %s", exc)
            return []

    def query_top_hashtags(self, platform: str | None, window_hours: int, top_n: int, week: str | None = None) -> list[dict]:
        if not week and self._redis:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=window_hours)
            redis_platform = platform or "__all__"
            prefix = f"rt:hashtags:{redis_platform}:"
            keys = [
                key for key in self._redis.scan_iter(f"{prefix}*")
                if (window_start := self._parse_redis_window_key(key, prefix)) is not None
                and cutoff <= window_start <= now
            ]
            counts: dict[str, float] = {}
            for key in keys:
                for tag, score in self._redis.zrevrange(key, 0, top_n - 1, withscores=True):
                    counts[tag] = counts.get(tag, 0.0) + float(score)
            if counts:
                return [
                    {"hashtag": tag, "frequency": score, "week": "realtime"}
                    for tag, score in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:top_n]
                ]
        
        filters: list[dict[str, Any]] = [{"term": {"view": "top_hashtags_weekly"}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        
        target_week = week
        if not target_week:
            # Fallback to the latest week from ES
            weeks = self.query_hashtag_weeks(platform)
            if weeks:
                target_week = weeks[0]
        
        if target_week:
            filters.append({"term": {"event_week": target_week}})
            sort = [{"frequency": {"order": "desc"}}, {"event_week": {"order": "desc"}}]
            raw_results = self._search("social_batch_views", {"bool": {"filter": filters}}, size=1000, sort=sort)
            
            counts: dict[str, int] = {}
            for row in raw_results:
                tag = row.get("hashtag")
                if tag:
                    counts[tag] = counts.get(tag, 0) + int(row.get("frequency") or 0)
            return [
                {"hashtag": tag, "frequency": freq, "week": target_week}
                for tag, freq in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
            ]
        
        return []

    def query_realtime_stats(self, platform: str | None = None) -> dict:
        if not self._redis:
            return {"platform": platform, "stats": []}
        pattern = f"rt:stats:{platform}:*" if platform else "rt:stats:*"
        stats = []
        for key in self._redis.scan_iter(pattern):
            row = self._redis.hgetall(key)
            try:
                sentiment_sum = float(row.get("sentiment_sum") or 0.0)
                sentiment_count = int(float(row.get("sentiment_count") or 0))
                row["avg_sentiment"] = sentiment_sum / sentiment_count if sentiment_count else 0.0
            except (TypeError, ValueError):
                row["avg_sentiment"] = 0.0
            stats.append(row | {"key": key})
        return {"platform": platform, "stats": stats}
