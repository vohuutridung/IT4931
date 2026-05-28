"""Topic Modeling serving layer.

Queries the ``social_topics`` Elasticsearch index populated by
``batch/network_analysis.py`` (BERTopic output).  When the index is
empty or unreachable the service returns **deterministic simulated data**
so the dashboard always renders.

Schema written by network_analysis.py:
  {
    "topic_id":      int,
    "topic_label":   str,        # human-readable label
    "platform":      str,        # reddit / facebook / instagram / all
    "post_count":    int,
    "keywords":      list[str],
    "avg_sentiment": float,      # -1..1
    "week":          str,        # ISO week date "YYYY-Www" or event_week ISO
    "x":             float,      # UMAP dim 1
    "y":             float,      # UMAP dim 2
  }

Trend records (view == "topic_trend"):
  {
    "view":        "topic_trend",
    "topic_id":    int,
    "topic_label": str,
    "platform":    str,
    "event_week":  str,          # ISO date
    "post_count":  int,
  }
"""

from __future__ import annotations

import logging
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from config.settings import ES_HOST

logger = logging.getLogger(__name__)

ES_TOPICS_INDEX = "social_topics"

# ---------------------------------------------------------------------------
# Simulated data seed (deterministic – no random.seed call in class init)
# ---------------------------------------------------------------------------

_SIMULATED_TOPICS = [
    {"topic_id": 0,  "topic_label": "AI & Machine Learning",        "keywords": ["ai", "gpt", "model", "llm", "neural", "training", "data"]},
    {"topic_id": 1,  "topic_label": "Crypto & Finance",             "keywords": ["bitcoin", "crypto", "market", "token", "defi", "blockchain"]},
    {"topic_id": 2,  "topic_label": "Politics & Society",           "keywords": ["government", "election", "policy", "vote", "president", "law"]},
    {"topic_id": 3,  "topic_label": "Sports & Gaming",              "keywords": ["game", "match", "team", "player", "score", "esports"]},
    {"topic_id": 4,  "topic_label": "Health & Wellness",            "keywords": ["health", "diet", "mental", "fitness", "medicine", "wellness"]},
    {"topic_id": 5,  "topic_label": "Entertainment & Pop Culture",  "keywords": ["movie", "music", "celebrity", "streaming", "series", "album"]},
    {"topic_id": 6,  "topic_label": "Tech Products & Startups",     "keywords": ["startup", "product", "launch", "apple", "google", "samsung"]},
    {"topic_id": 7,  "topic_label": "Climate & Environment",        "keywords": ["climate", "carbon", "renewable", "energy", "green", "solar"]},
    {"topic_id": 8,  "topic_label": "Travel & Lifestyle",           "keywords": ["travel", "vacation", "food", "lifestyle", "trip", "culture"]},
    {"topic_id": 9,  "topic_label": "Education & Research",         "keywords": ["research", "study", "university", "science", "paper", "degree"]},
    {"topic_id": 10, "topic_label": "Memes & Humor",                "keywords": ["meme", "funny", "lol", "joke", "viral", "reddit"]},
    {"topic_id": 11, "topic_label": "Business & Economy",           "keywords": ["economy", "stock", "company", "investment", "revenue", "profit"]},
]

_UMAP_POSITIONS = [
    (0.12,  0.87), (-0.45, 0.31), (-0.78, -0.22), (0.53, -0.61),
    (0.91,  0.14), (-0.30, 0.72), (0.67,  0.45),  (-0.55, -0.80),
    (0.21, -0.93), (-0.88, 0.58), (0.39,  0.00),  (-0.10, -0.47),
]

_PLATFORMS = ["reddit", "facebook", "instagram"]

_SENTIMENTS = [0.35, -0.12, -0.28, 0.18, 0.42, 0.25, 0.30, 0.15, 0.50, 0.60, 0.22, 0.08]

_POST_COUNTS = [3200, 2850, 1950, 2100, 1750, 2600, 3100, 1400, 2200, 1600, 4100, 2750]


def _simulated_topic_distribution(platform: str | None) -> list[dict]:
    """Return simulated per-topic distribution (all-time aggregation)."""
    topics = []
    divisor = 1.0 if not platform else {"reddit": 2.5, "facebook": 2.2, "instagram": 3.0}.get(platform, 2.0)
    for i, t in enumerate(_SIMULATED_TOPICS):
        count = int(_POST_COUNTS[i] / divisor)
        x, y = _UMAP_POSITIONS[i]
        topics.append({
            "topic_id":      t["topic_id"],
            "topic_label":   t["topic_label"],
            "keywords":      t["keywords"],
            "platform":      platform or "all",
            "post_count":    count,
            "avg_sentiment": _SENTIMENTS[i],
            "x":             x,
            "y":             y,
        })
    return topics


def _simulated_topic_trend(platform: str | None, weeks: int = 8) -> list[dict]:
    """Return simulated weekly trend per topic."""
    rng = random.Random(42)
    now = datetime.now(timezone.utc)
    # Align to Monday
    monday = now - timedelta(days=now.weekday())
    trend: list[dict] = []
    for w in range(weeks - 1, -1, -1):
        week_dt = monday - timedelta(weeks=w)
        week_str = week_dt.date().isoformat()
        for t in _SIMULATED_TOPICS:
            base = _POST_COUNTS[t["topic_id"]]
            jitter = rng.gauss(1.0, 0.25)
            count = max(10, int(base * jitter / (8 if platform else 1)))
            trend.append({
                "topic_id":    t["topic_id"],
                "topic_label": t["topic_label"],
                "platform":    platform or "all",
                "event_week":  week_str,
                "post_count":  count,
            })
    return trend


def _simulated_sentiment_heatmap(platform: str | None) -> list[dict]:
    """Return sentiment per-topic per-platform matrix."""
    rng = random.Random(7)
    result: list[dict] = []
    plats = [platform] if platform else _PLATFORMS
    for t in _SIMULATED_TOPICS:
        for p in plats:
            base = _SENTIMENTS[t["topic_id"]]
            jitter = rng.gauss(0, 0.12)
            result.append({
                "topic_id":      t["topic_id"],
                "topic_label":   t["topic_label"],
                "platform":      p,
                "avg_sentiment": max(-1.0, min(1.0, base + jitter)),
            })
    return result


# ---------------------------------------------------------------------------
# TopicService
# ---------------------------------------------------------------------------

class TopicService:
    """Query topic data from Elasticsearch with simulated-data fallback."""

    def __init__(self, es_host: str = ES_HOST) -> None:
        self.es_host = es_host.rstrip("/")

    # ── internal helpers ────────────────────────────────────────────────────

    def _search(self, query: dict, size: int = 500, sort: list[dict] | None = None) -> list[dict]:
        payload: dict[str, Any] = {"query": query, "size": size}
        if sort:
            payload["sort"] = sort
        try:
            resp = requests.post(
                f"{self.es_host}/{ES_TOPICS_INDEX}/_search",
                json=payload,
                timeout=3,
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            return [hit.get("_source", {}) for hit in resp.json().get("hits", {}).get("hits", [])]
        except Exception as exc:
            logger.warning("ES topics search failed, using simulated data: %s", exc)
            return []

    def _index_exists(self) -> bool:
        try:
            resp = requests.head(f"{self.es_host}/{ES_TOPICS_INDEX}", timeout=2)
            return resp.status_code == 200
        except Exception:
            return False

    # ── public API ──────────────────────────────────────────────────────────

    def query_topic_distribution(self, platform: str | None = None) -> list[dict]:
        """Per-topic post count + keywords + UMAP position."""
        filters: list[dict] = [{"term": {"view": "topic_distribution"}}]
        if platform:
            filters.append({"term": {"platform": platform}})

        results = self._search({"bool": {"filter": filters}}, size=100)
        if results:
            return results

        logger.info("topic_distribution: using simulated data")
        return _simulated_topic_distribution(platform)

    def query_topic_trend(self, platform: str | None = None, weeks: int = 8) -> list[dict]:
        """Weekly post count per topic."""
        filters: list[dict] = [{"term": {"view": "topic_trend"}}]
        if platform:
            filters.append({"term": {"platform": platform}})

        sort = [{"event_week": {"order": "asc"}}]
        results = self._search({"bool": {"filter": filters}}, size=1000, sort=sort)
        if results:
            return results

        logger.info("topic_trend: using simulated data")
        return _simulated_topic_trend(platform, weeks)

    def query_sentiment_heatmap(self, platform: str | None = None) -> list[dict]:
        """Avg sentiment per topic × platform."""
        filters: list[dict] = [{"term": {"view": "topic_sentiment_heatmap"}}]
        if platform:
            filters.append({"term": {"platform": platform}})

        results = self._search({"bool": {"filter": filters}}, size=200)
        if results:
            return results

        logger.info("topic_sentiment_heatmap: using simulated data")
        return _simulated_sentiment_heatmap(platform)

    def query_topic_network(self, platform: str | None = None) -> dict:
        """Topic co-occurrence network (nodes + edges)."""
        filters: list[dict] = [{"term": {"view": "topic_network"}}]
        if platform:
            filters.append({"term": {"platform": platform}})

        results = self._search({"bool": {"filter": filters}}, size=500)
        if results:
            # Expect results to contain serialised nodes/edges lists
            nodes = [r for r in results if r.get("record_type") == "node"]
            edges = [r for r in results if r.get("record_type") == "edge"]
            return {"nodes": nodes, "edges": edges}

        logger.info("topic_network: using simulated data")
        return _simulated_topic_network(platform)


def _simulated_topic_network(platform: str | None) -> dict:
    """Build a simulated topic-topic co-occurrence network."""
    rng = random.Random(13)
    nodes = []
    for t in _SIMULATED_TOPICS:
        nodes.append({
            "id":    t["topic_id"],
            "label": t["topic_label"],
            "value": _POST_COUNTS[t["topic_id"]],
        })

    # Build sparse edges between semantically close topics
    edges_def = [
        (0, 6, 0.82), (0, 9, 0.68), (1, 11, 0.75), (1, 6, 0.55),
        (2, 11, 0.60), (2, 4, 0.42), (3, 10, 0.50), (3, 5, 0.38),
        (4, 9, 0.70), (5, 10, 0.65), (5, 8, 0.48), (6, 9, 0.58),
        (7, 4, 0.45), (7, 9, 0.52), (8, 5, 0.35), (10, 3, 0.50),
        (11, 1, 0.75), (0, 1, 0.33), (2, 3, 0.28),
    ]
    edges = []
    for src, tgt, base_weight in edges_def:
        w = max(0.05, min(1.0, base_weight + rng.gauss(0, 0.05)))
        edges.append({"from": src, "to": tgt, "weight": round(w, 3)})

    return {"nodes": nodes, "edges": edges}
