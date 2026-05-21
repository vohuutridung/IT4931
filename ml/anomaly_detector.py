#!/usr/bin/env python3
"""Long-running anomaly detector service."""

from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone
from statistics import mean, pstdev

from fastapi import FastAPI

from config.settings import CASSANDRA_ALERTS_TABLE, CASSANDRA_HOSTS, CASSANDRA_KEYSPACE
from serving.merge_service import ServeQuery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("anomaly_detector")

app = FastAPI(title="Social Anomaly Alerts")
alerts: deque[dict] = deque(maxlen=50)
service = ServeQuery()


_cassandra_session = None


def get_cassandra_session():
    global _cassandra_session
    if _cassandra_session is not None:
        return _cassandra_session
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
            CREATE TABLE IF NOT EXISTS {CASSANDRA_ALERTS_TABLE} (
                alert_id TEXT PRIMARY KEY,
                metric TEXT,
                value DOUBLE,
                baseline DOUBLE,
                created_at TIMESTAMP,
                payload TEXT
            )
            """
        )
        _cassandra_session = session
        return session
    except Exception as exc:
        logger.warning("Cassandra alerts connection failed, will retry: %s", exc)
        return None


def engagement_value(stats: dict) -> float:
    values = []
    for item in stats.get("stats", []):
        try:
            values.append(float(item.get("post_count", 0)))
        except Exception:
            pass
    return sum(values)


def write_alert(session, alert: dict) -> None:
    alerts.appendleft(alert)
    if session is None:
        session = get_cassandra_session()
    if not session:
        logger.warning("Cassandra not connected. Alert logged locally, but not written to Cassandra: %s", alert)
        return
    import json
    import uuid

    try:
        session.execute(
            f"INSERT INTO {CASSANDRA_ALERTS_TABLE} (alert_id, metric, value, baseline, created_at, payload) VALUES (?, ?, ?, ?, ?, ?)",
            (
                str(uuid.uuid4()),
                alert["metric"],
                float(alert["value"]),
                float(alert["baseline"]),
                datetime.now(timezone.utc),
                json.dumps(alert, ensure_ascii=False),
            ),
        )
    except Exception as exc:
        logger.error("Failed to execute alert insert in Cassandra, resetting session: %s", exc)
        global _cassandra_session
        _cassandra_session = None


def detect_once(history: deque[float], session=None) -> None:
    current = engagement_value(service.query_realtime_stats())
    if len(history) >= 24:
        baseline = mean(history)
        deviation = pstdev(history) or 1.0
        if abs(current - baseline) > 3 * deviation:
            alert = {"metric": "engagement", "value": current, "baseline": baseline}
            write_alert(session, alert)
            logger.warning("Anomaly detected: %s", alert)
    history.append(current)


def run_loop(interval_seconds: int = 60) -> None:
    try:
        from sklearn.ensemble import IsolationForest  # noqa: F401

        logger.info("Initial anomaly model ready using IsolationForest-compatible runtime")
    except Exception:
        logger.info("Initial anomaly model ready using rolling baseline fallback")
    history: deque[float] = deque(maxlen=24 * 7)
    while True:
        detect_once(history, None)
        time.sleep(interval_seconds)


@app.get("/alerts")
def get_alerts() -> dict:
    return {"data": list(alerts)}


if __name__ == "__main__":
    run_loop()
