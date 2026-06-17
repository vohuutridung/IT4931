"""Network Service - serves graph data from ES ``social_network`` index.

Demo fallback data is returned only when ``ENABLE_DEMO_FALLBACK=true``.
"""

from __future__ import annotations

import logging
import random
from typing import Any

import requests
from config.settings import ENABLE_DEMO_FALLBACK, ES_HOST, REDIS_HOST, REDIS_PORT

logger = logging.getLogger(__name__)

ES_NETWORK_INDEX = "social_network"
PLATFORMS = ["reddit", "facebook", "instagram"]

_COMMUNITY_NAMES = [
    "AI Researchers", "Crypto Traders", "Political Commentators",
    "Sports Fans", "Health Advocates", "Entertainment Fans",
    "Tech Enthusiasts", "Climate Activists",
]

_NODES_PER_COMMUNITY = 25
_NUM_COMMUNITIES = len(_COMMUNITY_NAMES)

# Deterministic colour palette per community (matches dashboard CSS)
_COMMUNITY_COLORS = [
    "#6366f1", "#10b981", "#f59e0b", "#ef4444",
    "#3b82f6", "#ec4899", "#8b5cf6", "#14b8a6",
]


# ---------------------------------------------------------------------------
# Simulated graph helpers (mirrors batch/network_analysis.py)
# ---------------------------------------------------------------------------

def _simulated_graph(platform: str | None) -> dict:
    """Build a simulated user-interaction graph with community + PageRank."""
    rng = random.Random(42)
    plat_suffix = f"_{platform}" if platform else ""

    all_node_ids: list[list[str]] = []
    nodes: list[dict] = []
    for comm_idx in range(_NUM_COMMUNITIES):
        comm_nodes = [
            f"user{comm_idx * _NODES_PER_COMMUNITY + j}{plat_suffix}"
            for j in range(_NODES_PER_COMMUNITY)
        ]
        all_node_ids.append(comm_nodes)
        for node_id in comm_nodes:
            nodes.append({
                "node_id":        node_id,
                "community_id":   comm_idx,
                "community_name": _COMMUNITY_NAMES[comm_idx],
                "color":          _COMMUNITY_COLORS[comm_idx % len(_COMMUNITY_COLORS)],
                "pagerank":       round(rng.gauss(0.005, 0.002), 6),
                "out_degree":     rng.randint(2, 12),
                "in_degree":      round(rng.gauss(5, 2), 1),
                "platform":       platform or "all",
            })

    # Intra-community edges (dense)
    flat_nodes = [n for comm in all_node_ids for n in comm]
    edges: list[dict] = []
    for comm_idx, comm_nodes in enumerate(all_node_ids):
        for i, src in enumerate(comm_nodes):
            num_edges = rng.randint(2, 6)
            targets = rng.sample(
                [n for n in comm_nodes if n != src],
                min(num_edges, len(comm_nodes) - 1),
            )
            for tgt in targets:
                edges.append({"from": src, "to": tgt, "weight": rng.randint(1, 15)})

    # Inter-community edges (sparse)
    num_inter = int(len(flat_nodes) * 0.25)
    for _ in range(num_inter):
        src = rng.choice(flat_nodes)
        tgt = rng.choice(flat_nodes)
        if src != tgt:
            edges.append({"from": src, "to": tgt, "weight": rng.randint(1, 5)})

    return {"nodes": nodes, "edges": edges}


def _simulated_community_sizes(platform: str | None) -> list[dict]:
    rng = random.Random(42)
    result = []
    for comm_idx in range(_NUM_COMMUNITIES):
        result.append({
            "community_id":   comm_idx,
            "community_name": _COMMUNITY_NAMES[comm_idx],
            "size":           _NODES_PER_COMMUNITY + rng.randint(-5, 10),
            "platform":       platform or "all",
            "color":          _COMMUNITY_COLORS[comm_idx % len(_COMMUNITY_COLORS)],
        })
    return sorted(result, key=lambda x: x["size"], reverse=True)


def _simulated_top_influencers(platform: str | None, top_n: int) -> list[dict]:
    rng = random.Random(42)
    plat_suffix = f"_{platform}" if platform else ""
    influencers = []
    for comm_idx in range(_NUM_COMMUNITIES):
        for j in range(_NODES_PER_COMMUNITY):
            node_id = f"user{comm_idx * _NODES_PER_COMMUNITY + j}{plat_suffix}"
            influencers.append({
                "node_id":        node_id,
                "pagerank":       round(rng.gauss(0.005, 0.003), 6),
                "community_name": _COMMUNITY_NAMES[comm_idx],
                "out_degree":     rng.randint(2, 18),
                "platform":       platform or "all",
            })
    return sorted(influencers, key=lambda x: x["pagerank"], reverse=True)[:top_n]


# ---------------------------------------------------------------------------
# NetworkService
# ---------------------------------------------------------------------------

class NetworkService:
    """Query user-interaction graph data from ES with optional demo fallback."""

    def __init__(
        self,
        es_host: str = ES_HOST,
        redis_host: str = REDIS_HOST,
        redis_port: int = REDIS_PORT,
    ) -> None:
        self.es_host = es_host.rstrip("/")
        self._session = requests.Session()
        self._redis = None
        try:
            import redis
            self._redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        except Exception as exc:
            logger.warning("Redis client unavailable for NetworkService: %s", exc)

    def _search(self, query: dict, size: int = 5000) -> list[dict]:
        try:
            resp = self._session.post(
                f"{self.es_host}/{ES_NETWORK_INDEX}/_search",
                json={"query": query, "size": size},
                timeout=5,
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            return [hit.get("_source", {}) for hit in resp.json().get("hits", {}).get("hits", [])]
        except Exception as exc:
            logger.warning("ES network search failed: %s", exc)
            return []

    # ── public API ──────────────────────────────────────────────────────────

    def query_graph(self, platform: str | None = None) -> dict:
        """Return nodes + edges for force-directed graph visualisation."""
        filters: list[dict[str, Any]] = []
        if platform:
            filters.append({"term": {"platform": platform}})

        query: dict = {"match_all": {}} if not filters else {"bool": {"filter": filters}}
        results = self._search(query, size=3000)

        if results:
            nodes = [r for r in results if r.get("record_type") == "node"]
            edges = [r for r in results if r.get("record_type") == "edge"]
            if nodes:
                # Attach community colour
                for node in nodes:
                    cid = node.get("community_id", 0) or 0
                    node["color"] = _COMMUNITY_COLORS[int(cid) % len(_COMMUNITY_COLORS)]
                return {"nodes": nodes, "edges": edges, "simulated": False}

        if ENABLE_DEMO_FALLBACK:
            logger.info("network/graph: using demo fallback data")
            result = _simulated_graph(platform)
            result["simulated"] = True
            return result
        return {"nodes": [], "edges": [], "simulated": False}

    def query_community_sizes(self, platform: str | None = None) -> list[dict]:
        """Return community size distribution."""
        # Try Redis first
        if self._redis:
            try:
                # When platform is specified, query that key directly.
                # When platform is None, aggregate across all platform keys.
                platforms_to_query = [platform] if platform else PLATFORMS
                merged: dict[int, int] = {}
                for p in platforms_to_query:
                    comm_key = f"network:communities:{p}"
                    raw = self._redis.hgetall(comm_key)
                    for k, v in raw.items():
                        try:
                            comm_id = int(k)
                            size = int(v)
                        except ValueError:
                            continue
                        merged[comm_id] = merged.get(comm_id, 0) + size
                if merged:
                    result = []
                    for comm_id, size in merged.items():
                        result.append({
                            "community_id":   comm_id,
                            "community_name": _COMMUNITY_NAMES[comm_id % len(_COMMUNITY_NAMES)],
                            "size":           size,
                            "platform":       platform or "all",
                            "color":          _COMMUNITY_COLORS[comm_id % len(_COMMUNITY_COLORS)],
                        })
                    return sorted(result, key=lambda x: x["size"], reverse=True)
            except Exception as exc:
                logger.warning("Redis community query failed: %s", exc)

        if ENABLE_DEMO_FALLBACK:
            logger.info("network/communities: using demo fallback data")
            return _simulated_community_sizes(platform)
        return []

    def query_top_influencers(self, platform: str | None = None, top_n: int = 20) -> list[dict]:
        """Return top-N nodes by PageRank score."""
        # Try Redis sorted set
        if self._redis:
            try:
                # When platform is specified, query that key directly.
                # When platform is None, aggregate across all platform keys.
                platforms_to_query = [platform] if platform else PLATFORMS
                merged: dict[str, float] = {}
                for p in platforms_to_query:
                    pr_key = f"network:pagerank:{p}"
                    top = self._redis.zrevrange(pr_key, 0, top_n - 1, withscores=True)
                    for node, score in (top or []):
                        merged[node] = max(merged.get(node, 0.0), float(score))
                if merged:
                    sorted_nodes = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:top_n]
                    return [
                        {"node_id": node, "pagerank": round(score, 6), "platform": platform or "all"}
                        for node, score in sorted_nodes
                    ]
            except Exception as exc:
                logger.warning("Redis pagerank query failed: %s", exc)

        # Try ES
        filters: list[dict[str, Any]] = [{"term": {"record_type": "node"}}]
        if platform:
            filters.append({"term": {"platform": platform}})
        results = self._search({"bool": {"filter": filters}}, size=top_n)
        if results:
            return results

        if ENABLE_DEMO_FALLBACK:
            logger.info("network/pagerank: using demo fallback data")
            return _simulated_top_influencers(platform, top_n)
        return []
