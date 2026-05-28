#!/usr/bin/env python3
"""Network & Community Analysis batch job.

Reads enriched post interactions from Elasticsearch (social_batch_views /
social_realtime_views) and builds a directed user-interaction graph where:
  - Node  = unique author_id
  - Edge  = interaction between two users (reply, mention)
  - Weight = number of interactions between the pair

Then computes:
  1. Louvain Community Detection  (community labels per node)
  2. PageRank Influence Scoring   (influence score per node)

Results are written to:
  - Elasticsearch index ``social_network``   (nodes + edges + communities)
  - Redis keys  ``network:pagerank:<platform>``  (sorted set, top-100)
                ``network:communities:<platform>`` (hash, community → count)

When Elasticsearch contains no data the script generates a *deterministic*
simulated graph so that the dashboard always has something to display.

Usage
-----
    python -m batch.network_analysis                 # all platforms
    python -m batch.network_analysis --platform reddit
    python -m batch.network_analysis --dry-run        # skip ES/Redis writes
    python -m batch.network_analysis --simulated      # always use simulated data
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from config.settings import ES_HOST, REDIS_HOST, REDIS_PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
)
logger = logging.getLogger("network_analysis")

ES_NETWORK_INDEX = "social_network"
PLATFORMS = ["reddit", "facebook", "instagram"]

# ---------------------------------------------------------------------------
# Graph primitives
# ---------------------------------------------------------------------------

class DirectedGraph:
    """Lightweight directed weighted graph."""

    def __init__(self) -> None:
        # adjacency: src -> {tgt: weight}
        self._adj: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._nodes: set[str] = set()

    def add_edge(self, src: str, tgt: str, weight: float = 1.0) -> None:
        self._nodes.add(src)
        self._nodes.add(tgt)
        self._adj[src][tgt] += weight

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def out_edges(self, node: str) -> dict[str, float]:
        return dict(self._adj.get(node, {}))

    def in_degree(self, node: str) -> float:
        total = 0.0
        for targets in self._adj.values():
            if node in targets:
                total += targets[node]
        return total

    def edges(self) -> list[tuple[str, str, float]]:
        result = []
        for src, targets in self._adj.items():
            for tgt, w in targets.items():
                result.append((src, tgt, w))
        return result

    def __len__(self) -> int:
        return len(self._nodes)


# ---------------------------------------------------------------------------
# PageRank
# ---------------------------------------------------------------------------

def compute_pagerank(
    graph: DirectedGraph,
    damping: float = 0.85,
    iterations: int = 50,
    tol: float = 1e-6,
) -> dict[str, float]:
    """Compute PageRank for every node in the graph.

    Returns a dict mapping node_id → pagerank_score (values sum to 1).
    """
    nodes = graph.nodes
    n = len(nodes)
    if n == 0:
        return {}

    node_idx = {node: i for i, node in enumerate(nodes)}
    pr = [1.0 / n] * n

    # Build column-normalised adjacency
    out_weights: list[dict[int, float]] = []
    out_totals: list[float] = []
    for node in nodes:
        edges = graph.out_edges(node)
        total = sum(edges.values())
        out_totals.append(total)
        out_weights.append({node_idx[tgt]: w for tgt, w in edges.items() if tgt in node_idx})

    for _ in range(iterations):
        new_pr = [(1.0 - damping) / n] * n
        dangling_sum = 0.0
        for i, node in enumerate(nodes):
            if not out_weights[i]:
                dangling_sum += pr[i]

        for i, node in enumerate(nodes):
            contribution = pr[i]
            total = out_totals[i]
            if total > 0:
                for j, w in out_weights[i].items():
                    new_pr[j] += damping * contribution * (w / total)

        # Distribute dangling mass uniformly
        dangling_add = damping * dangling_sum / n
        new_pr = [v + dangling_add for v in new_pr]

        # Check convergence
        diff = sum(abs(new_pr[i] - pr[i]) for i in range(n))
        pr = new_pr
        if diff < tol:
            break

    return {nodes[i]: round(pr[i], 8) for i in range(n)}


# ---------------------------------------------------------------------------
# Louvain Community Detection (simplified greedy modularity)
# ---------------------------------------------------------------------------

def louvain_communities(graph: DirectedGraph) -> dict[str, int]:
    """Simplified Louvain-style greedy modularity community detection.

    Returns a dict mapping node_id → community_id (integer label).
    """
    nodes = graph.nodes
    if not nodes:
        return {}

    # Build undirected adjacency (symmetrise)
    adj: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    m = 0.0
    for src, tgt, w in graph.edges():
        adj[src][tgt] += w
        adj[tgt][src] += w
        m += w
    if m == 0:
        return {n: i for i, n in enumerate(nodes)}

    # Initial: each node in its own community
    community: dict[str, int] = {n: i for i, n in enumerate(nodes)}
    comm_members: dict[int, set[str]] = {i: {n} for i, n in enumerate(nodes)}

    def _modularity_gain(node: str, comm_id: int, current_comm: int) -> float:
        k_i = sum(adj[node].values())
        k_i_in = sum(adj[node].get(nb, 0.0) for nb in comm_members.get(comm_id, set()) if nb != node)
        sigma_tot = sum(sum(adj[nb].values()) for nb in comm_members.get(comm_id, set()))
        return (k_i_in / m) - (sigma_tot * k_i) / (2 * m * m)

    improved = True
    max_passes = 10
    for _ in range(max_passes):
        if not improved:
            break
        improved = False
        rng = random.Random(42)
        shuffled_nodes = list(nodes)
        rng.shuffle(shuffled_nodes)
        for node in shuffled_nodes:
            current_comm = community[node]
            best_comm = current_comm
            best_gain = 0.0

            neighbour_comms = {community[nb] for nb in adj[node] if nb != node}
            for c in neighbour_comms:
                if c == current_comm:
                    continue
                gain = _modularity_gain(node, c, current_comm)
                if gain > best_gain:
                    best_gain = gain
                    best_comm = c

            if best_comm != current_comm:
                comm_members[current_comm].discard(node)
                community[node] = best_comm
                comm_members.setdefault(best_comm, set()).add(node)
                improved = True

    # Re-label communities as 0..N-1
    old_labels = sorted(set(community.values()))
    relabel = {old: new for new, old in enumerate(old_labels)}
    return {node: relabel[c] for node, c in community.items()}


# ---------------------------------------------------------------------------
# Simulated graph data
# ---------------------------------------------------------------------------

_COMMUNITY_NAMES = [
    "AI Researchers", "Crypto Traders", "Political Commentators",
    "Sports Fans", "Health Advocates", "Entertainment Fans",
    "Tech Enthusiasts", "Climate Activists",
]

_NUM_COMMUNITIES = len(_COMMUNITY_NAMES)
_NODES_PER_COMMUNITY = 25


def build_simulated_graph(platform: str | None, seed: int = 42) -> DirectedGraph:
    """Build a realistic synthetic interaction graph."""
    rng = random.Random(seed)
    graph = DirectedGraph()

    plat_suffix = f"_{platform}" if platform else ""
    total_nodes = _NUM_COMMUNITIES * _NODES_PER_COMMUNITY
    all_nodes: list[list[str]] = []

    for comm_idx in range(_NUM_COMMUNITIES):
        comm_nodes = [f"user{comm_idx * _NODES_PER_COMMUNITY + j}{plat_suffix}" for j in range(_NODES_PER_COMMUNITY)]
        all_nodes.append(comm_nodes)

        # Intra-community edges (dense)
        for i, src in enumerate(comm_nodes):
            num_edges = rng.randint(3, 8)
            targets = rng.sample([n for n in comm_nodes if n != src], min(num_edges, len(comm_nodes) - 1))
            for tgt in targets:
                weight = rng.randint(1, 15)
                graph.add_edge(src, tgt, weight)

    # Inter-community edges (sparse)
    flat_nodes = [n for comm in all_nodes for n in comm]
    num_inter = int(total_nodes * 0.3)
    for _ in range(num_inter):
        src = rng.choice(flat_nodes)
        tgt = rng.choice(flat_nodes)
        if src != tgt:
            graph.add_edge(src, tgt, rng.randint(1, 5))

    return graph


def _assign_simulated_communities(nodes: list[str]) -> dict[str, int]:
    """Assign community IDs based on node naming convention."""
    comm: dict[str, int] = {}
    for node in nodes:
        # Extract community index from synthetic node name
        try:
            idx = int(node.split("user")[1].split("_")[0])
            comm[node] = idx // _NODES_PER_COMMUNITY
        except (IndexError, ValueError):
            comm[node] = 0
    return comm


# ---------------------------------------------------------------------------
# Elasticsearch helpers
# ---------------------------------------------------------------------------

def _es_ensure_index(es_host: str) -> None:
    mapping = {
        "mappings": {
            "properties": {
                "record_type":   {"type": "keyword"},
                "platform":      {"type": "keyword"},
                "node_id":       {"type": "keyword"},
                "community_id":  {"type": "integer"},
                "community_name":{"type": "keyword"},
                "pagerank":      {"type": "float"},
                "out_degree":    {"type": "integer"},
                "in_degree":     {"type": "float"},
                "source_node":   {"type": "keyword"},
                "target_node":   {"type": "keyword"},
                "weight":        {"type": "float"},
                "computed_at":   {"type": "date"},
            }
        }
    }
    resp = requests.head(f"{es_host}/{ES_NETWORK_INDEX}", timeout=5)
    if resp.status_code == 404:
        r = requests.put(f"{es_host}/{ES_NETWORK_INDEX}", json=mapping, timeout=10)
        r.raise_for_status()
        logger.info("Created ES index %s", ES_NETWORK_INDEX)


def _es_bulk(es_host: str, docs: list[dict]) -> None:
    if not docs:
        return
    lines: list[str] = []
    for doc in docs:
        doc_id = doc.get("doc_id")
        action: dict[str, Any] = {"index": {"_index": ES_NETWORK_INDEX}}
        if doc_id:
            action["index"]["_id"] = str(doc_id)
        lines.append(json.dumps(action, ensure_ascii=False))
        lines.append(json.dumps({k: v for k, v in doc.items() if k != "doc_id"}, ensure_ascii=False, default=str))
    payload = "\n".join(lines) + "\n"
    resp = requests.post(
        f"{es_host}/_bulk",
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        logger.error("Bulk index had errors: %s", body)
    else:
        logger.info("Indexed %d docs into %s", len(docs), ES_NETWORK_INDEX)


def _redis_write(
    redis_client: Any,
    platform: str,
    pagerank: dict[str, float],
    community_sizes: dict[int, int],
) -> None:
    """Write PageRank and community sizes to Redis."""
    pr_key = f"network:pagerank:{platform}"
    redis_client.delete(pr_key)
    top_100 = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:100]
    if top_100:
        redis_client.zadd(pr_key, {node: score for node, score in top_100})
        redis_client.expire(pr_key, 86400)  # 24h TTL

    comm_key = f"network:communities:{platform}"
    redis_client.delete(comm_key)
    if community_sizes:
        redis_client.hset(comm_key, mapping={str(k): str(v) for k, v in community_sizes.items()})
        redis_client.expire(comm_key, 86400)

    logger.info("Wrote network results to Redis: pr_key=%s comm_key=%s", pr_key, comm_key)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_platform(
    platform: str,
    es_host: str,
    redis_client: Any,
    dry_run: bool = False,
    simulated: bool = False,
) -> dict:
    """Run full network analysis for one platform. Returns a summary dict."""
    logger.info("── %s ─────────────────────────────────────────", platform)

    # ── 1. Build graph ──────────────────────────────────────────────────────
    graph: DirectedGraph | None = None
    if not simulated:
        graph = _fetch_interaction_graph(es_host, platform)

    if graph is None or len(graph) < 10:
        logger.info("Insufficient real data for %s, using simulated graph", platform)
        graph = build_simulated_graph(platform)

    nodes = graph.nodes
    logger.info("Graph: %d nodes, %d edges", len(nodes), len(graph.edges()))

    # ── 2. PageRank ─────────────────────────────────────────────────────────
    logger.info("Computing PageRank …")
    pagerank = compute_pagerank(graph)

    # ── 3. Louvain ──────────────────────────────────────────────────────────
    logger.info("Computing Louvain communities …")
    community_map: dict[str, int] = louvain_communities(graph)

    # Community sizes
    comm_sizes: dict[int, int] = defaultdict(int)
    for comm_id in community_map.values():
        comm_sizes[comm_id] += 1

    num_communities = len(comm_sizes)
    logger.info("Detected %d communities", num_communities)

    # ── 4. Write results ────────────────────────────────────────────────────
    now_iso = datetime.now(timezone.utc).isoformat()

    if not dry_run:
        # Build ES documents for nodes
        node_docs: list[dict] = []
        for node in nodes:
            comm_id = community_map.get(node, 0)
            comm_name = _COMMUNITY_NAMES[comm_id % len(_COMMUNITY_NAMES)]
            out_edges = graph.out_edges(node)
            node_docs.append({
                "doc_id":        f"node_{platform}_{node}",
                "record_type":   "node",
                "platform":      platform,
                "node_id":       node,
                "community_id":  comm_id,
                "community_name": comm_name,
                "pagerank":      pagerank.get(node, 0.0),
                "out_degree":    len(out_edges),
                "in_degree":     graph.in_degree(node),
                "computed_at":   now_iso,
            })

        # Build ES documents for edges (top 2000 by weight)
        edge_docs: list[dict] = []
        all_edges = sorted(graph.edges(), key=lambda e: e[2], reverse=True)[:2000]
        for src, tgt, w in all_edges:
            edge_docs.append({
                "doc_id":      f"edge_{platform}_{src}_{tgt}",
                "record_type": "edge",
                "platform":    platform,
                "source_node": src,
                "target_node": tgt,
                "weight":      w,
                "computed_at": now_iso,
            })

        # Write to ES in batches of 500
        all_docs = node_docs + edge_docs
        batch_size = 500
        for i in range(0, len(all_docs), batch_size):
            _es_bulk(es_host, all_docs[i:i + batch_size])

        # Write to Redis
        if redis_client:
            _redis_write(redis_client, platform, pagerank, dict(comm_sizes))

    summary = {
        "platform":        platform,
        "nodes":           len(nodes),
        "edges":           len(graph.edges()),
        "communities":     num_communities,
        "top_influencers": sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:5],
    }
    logger.info("Summary: %s", summary)
    return summary


def _fetch_interaction_graph(es_host: str, platform: str) -> DirectedGraph | None:
    """Attempt to build a graph from ES social_batch_views / social_realtime_views.

    Looks for posts that have 'mentions' or 'in_reply_to_user' fields.
    Returns None when there is no usable data.
    """
    query = {
        "bool": {
            "filter": [{"term": {"platform": platform}}],
            "should": [
                {"exists": {"field": "mentions"}},
                {"exists": {"field": "in_reply_to_user"}},
            ],
            "minimum_should_match": 1,
        }
    }
    graph = DirectedGraph()
    for index in ("social_batch_views", "social_realtime_views"):
        try:
            resp = requests.post(
                f"{es_host}/{index}/_search",
                json={"query": query, "size": 5000, "_source": ["author_id", "mentions", "in_reply_to_user"]},
                timeout=10,
            )
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            for hit in resp.json().get("hits", {}).get("hits", []):
                src = hit["_source"].get("author_id")
                if not src:
                    continue
                reply_to = hit["_source"].get("in_reply_to_user")
                if reply_to and reply_to != src:
                    graph.add_edge(str(src), str(reply_to))
                mentions = hit["_source"].get("mentions") or []
                for tgt in mentions:
                    if tgt and str(tgt) != str(src):
                        graph.add_edge(str(src), str(tgt))
        except Exception as exc:
            logger.warning("Could not fetch interaction data from %s: %s", index, exc)

    return graph if len(graph) >= 10 else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Network & Community Analysis batch job")
    parser.add_argument("--platform", choices=PLATFORMS, help="Run for a single platform only")
    parser.add_argument("--dry-run", action="store_true", help="Compute but skip all writes")
    parser.add_argument("--simulated", action="store_true", help="Always use simulated graph data")
    parser.add_argument("--es-host", default=ES_HOST, help="Elasticsearch host URL")
    parser.add_argument("--redis-host", default=REDIS_HOST)
    parser.add_argument("--redis-port", type=int, default=REDIS_PORT)
    args = parser.parse_args()

    es_host = args.es_host.rstrip("/")

    # Elasticsearch index
    if not args.dry_run:
        for attempt in range(1, 6):
            try:
                _es_ensure_index(es_host)
                break
            except Exception as exc:
                if attempt == 5:
                    logger.error("Could not ensure ES index: %s", exc)
                    break
                logger.warning("ES not ready (%d/5): %s – retrying in 5s", attempt, exc)
                time.sleep(5)

    # Redis client
    redis_client = None
    if not args.dry_run:
        try:
            import redis as redis_lib
            redis_client = redis_lib.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
            redis_client.ping()
            logger.info("Redis connected at %s:%d", args.redis_host, args.redis_port)
        except Exception as exc:
            logger.warning("Redis unavailable, skipping Redis writes: %s", exc)
            redis_client = None

    platforms = [args.platform] if args.platform else PLATFORMS
    summaries: list[dict] = []
    for platform in platforms:
        try:
            summary = run_platform(
                platform=platform,
                es_host=es_host,
                redis_client=redis_client,
                dry_run=args.dry_run,
                simulated=args.simulated,
            )
            summaries.append(summary)
        except Exception as exc:
            logger.error("Error processing platform %s: %s", platform, exc)

    logger.info("=== Network Analysis Complete ===")
    for s in summaries:
        logger.info(
            "  %s: %d nodes | %d edges | %d communities | top=%s",
            s["platform"], s["nodes"], s["edges"], s["communities"],
            s["top_influencers"][:2],
        )


if __name__ == "__main__":
    main()
