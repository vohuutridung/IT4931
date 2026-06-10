"""Serving Layer merge logic using ClickHouse HTTP API."""

from __future__ import annotations

import logging
import requests
from datetime import datetime, timezone
from config.settings import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_WRITE_TIMEOUT,
    REALTIME_WINDOW_HOURS,
)

logger = logging.getLogger(__name__)


def _query_clickhouse(sql: str) -> list[dict]:
    """Execute a query against ClickHouse and return rows as list of dicts."""
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DATABASE,
    }
    # Append FORMAT JSON if not present
    if "FORMAT " not in sql.upper():
        sql = sql.rstrip(" ;") + " FORMAT JSON"

    try:
        response = requests.post(
            CLICKHOUSE_HOST,
            params=params,
            data=sql.encode("utf-8"),
            timeout=CLICKHOUSE_WRITE_TIMEOUT or 10,
        )
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as exc:
        logger.error("ClickHouse query error: %s", exc)
        return []


class ServeQuery:
    def __init__(self, **_kwargs) -> None:
        pass

    @staticmethod
    def _parse_dt(value: datetime | str) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    def query_posts(
        self,
        platform: str | None,
        start_dt: datetime | str,
        end_dt: datetime | str,
        limit: int = 100,
    ) -> list[dict]:
        start = self._parse_dt(start_dt).strftime("%Y-%m-%d %H:%M:%S")
        end = self._parse_dt(end_dt).strftime("%Y-%m-%d %H:%M:%S")

        platform_filter = f"AND platform = '{platform}'" if platform else ""

        # Query batch view (historical top posts)
        sql_batch = f"""
        SELECT post_id, platform, author_id, content, 0.0 AS sentiment_score, 
               formatDateTime(event_ts, '%Y-%m-%d %H:%i:%S') AS event_ts, loaded_at
        FROM fact_top_posts
        WHERE event_ts >= '{start}' AND event_ts <= '{end}' {platform_filter}
        ORDER BY engagement_score DESC, post_id ASC
        LIMIT {limit}
        """

        # Query speed view (realtime posts)
        sql_speed = f"""
        SELECT post_id, platform, author_id, content, sentiment AS sentiment_score, 
               formatDateTime(event_ts, '%Y-%m-%d %H:%i:%S') AS event_ts, loaded_at
        FROM realtime_posts
        WHERE event_ts >= '{start}' AND event_ts <= '{end}' {platform_filter}
        ORDER BY event_ts DESC
        LIMIT {limit}
        """

        batch_posts = _query_clickhouse(sql_batch)
        speed_posts = _query_clickhouse(sql_speed)

        # Merge by post_id: prefer batch (source of truth) if it exists, otherwise speed
        merged = {}
        for post in speed_posts:
            pid = post.get("post_id")
            if pid:
                post["sentiment"] = post.get("sentiment_score", 0.0)
                merged[pid] = post

        for post in batch_posts:
            pid = post.get("post_id")
            if pid:
                post["sentiment"] = post.get("sentiment_score", 0.0)
                merged[pid] = post

        # Sort merged posts by event_ts DESC
        sorted_posts = sorted(
            merged.values(),
            key=lambda x: x.get("event_ts", ""),
            reverse=True
        )
        return sorted_posts[:limit]

    def query_sentiment_trend(
        self,
        platform: str | None,
        granularity: str,
        start_dt: datetime | str,
        end_dt: datetime | str,
    ) -> list[dict]:
        start = self._parse_dt(start_dt).strftime("%Y-%m-%d %H:%M:%S")
        end = self._parse_dt(end_dt).strftime("%Y-%m-%d %H:%M:%S")

        platform_filter = f"AND platform = '{platform}'" if platform else ""

        time_func = "toStartOfHour" if granularity == "hour" else "toStartOfDay"

        # Query batch view (historical hourly stats)
        sql_batch = f"""
        SELECT platform, formatDateTime({time_func}(event_hour), '%Y-%m-%d %H:%i:%S') AS event_hour, 
               avg(avg_sentiment) AS avg_sentiment, sum(post_count) AS post_count
        FROM fact_sentiment_time_series
        WHERE event_hour >= '{start}' AND event_hour <= '{end}' {platform_filter}
        GROUP BY platform, event_hour
        ORDER BY event_hour
        """

        # Query speed view (realtime posts grouped by hour/day)
        sql_speed = f"""
        SELECT platform, formatDateTime({time_func}(event_ts), '%Y-%m-%d %H:%i:%S') AS event_hour, 
               avg(sentiment) AS avg_sentiment, count() AS post_count
        FROM realtime_posts
        WHERE event_ts >= '{start}' AND event_ts <= '{end}' {platform_filter}
        GROUP BY platform, event_hour
        ORDER BY event_hour
        """

        batch_data = _query_clickhouse(sql_batch)
        speed_data = _query_clickhouse(sql_speed)

        # Merge batch & speed:
        # Key by (platform, event_hour)
        merged = {}
        for row in speed_data:
            key = (row["platform"], row["event_hour"])
            merged[key] = {
                "platform": row["platform"],
                "event_hour": row["event_hour"],
                "avg_sentiment": float(row["avg_sentiment"] or 0.0),
                "post_count": int(row["post_count"] or 0),
            }

        for row in batch_data:
            key = (row["platform"], row["event_hour"])
            merged[key] = {
                "platform": row["platform"],
                "event_hour": row["event_hour"],
                "avg_sentiment": float(row["avg_sentiment"] or 0.0),
                "post_count": int(row["post_count"] or 0),
            }

        # Convert to list and sort by event_hour
        sorted_data = sorted(
            merged.values(),
            key=lambda x: x.get("event_hour", "")
        )

        # Compute velocity
        for i, row in enumerate(sorted_data):
            prev = float(sorted_data[i - 1].get("avg_sentiment") or 0) if i > 0 else None
            curr = float(row.get("avg_sentiment") or 0)
            row["velocity"] = round(curr - prev, 4) if prev is not None else 0.0

        return sorted_data

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
        platform_filter = f"AND platform = '{platform}'" if platform else ""
        sql = f"""
        SELECT DISTINCT formatDateTime(event_week, '%Y-%m-%d %H:%i:%S') AS event_week
        FROM fact_top_hashtags_weekly
        WHERE 1=1 {platform_filter}
        ORDER BY event_week DESC
        """
        rows = _query_clickhouse(sql)
        return [row["event_week"] for row in rows]

    def query_top_hashtags(
        self,
        platform: str | None,
        window_hours: int,
        top_n: int,
        week: str | None = None,
    ) -> list[dict]:
        platform_filter = f"AND platform = '{platform}'" if platform else ""

        if not week:
            # Get latest week from table
            sql_latest = f"""
            SELECT formatDateTime(max(event_week), '%Y-%m-%d %H:%i:%S') AS latest_week
            FROM fact_top_hashtags_weekly
            WHERE 1=1 {platform_filter}
            """
            res = _query_clickhouse(sql_latest)
            if res and res[0].get("latest_week"):
                week = res[0]["latest_week"]
            else:
                return []

        sql = f"""
        SELECT hashtag, sum(frequency) AS frequency
        FROM fact_top_hashtags_weekly
        WHERE formatDateTime(event_week, '%Y-%m-%d %H:%i:%S') = '{week}' {platform_filter}
        GROUP BY hashtag
        ORDER BY frequency DESC
        LIMIT {top_n}
        """

        rows = _query_clickhouse(sql)
        return [
            {
                "hashtag": row["hashtag"],
                "frequency": int(row["frequency"] or 0),
                "week": str(week)
            }
            for row in rows
        ]

    def query_realtime_stats(self, platform: str | None = None) -> dict:
        platform_filter = f"AND platform = '{platform}'" if platform else ""
        sql = f"""
        SELECT platform, count() AS post_count, avg(sentiment) AS avg_sentiment
        FROM realtime_posts
        WHERE event_ts >= (SELECT max(event_ts) FROM realtime_posts) - INTERVAL {REALTIME_WINDOW_HOURS} HOUR {platform_filter}
        GROUP BY platform
        """
        rows = _query_clickhouse(sql)
        stats = [
            {
                "platform": row["platform"],
                "post_count": int(row["post_count"] or 0),
                "avg_sentiment": round(float(row["avg_sentiment"] or 0.0), 4),
            }
            for row in rows
        ]
        return {"platform": platform, "stats": stats}
