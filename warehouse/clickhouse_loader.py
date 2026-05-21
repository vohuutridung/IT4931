#!/usr/bin/env python3
"""Load Spark batch views from MinIO into ClickHouse warehouse tables."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from typing import Any

import requests
from pyspark.sql import SparkSession

from config.settings import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    SPARK_MASTER,
    STORAGE_BATCH_VIEWS_BASE,
)
from config.spark import configure_s3a

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("clickhouse_loader")

VIEWS = {
    "platform_daily_stats": "fact_platform_daily_stats",
    "top_hashtags_weekly": "fact_top_hashtags_weekly",
    "author_activity": "fact_author_activity",
    "sentiment_time_series": "fact_sentiment_time_series",
    "top_posts": "fact_top_posts",
}

DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_platform (
        platform LowCardinality(String),
        loaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    ORDER BY platform
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_platform_daily_stats (
        platform LowCardinality(String),
        event_date Date,
        post_count UInt64,
        avg_sentiment Float64,
        total_engagement UInt64,
        loaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (platform, event_date)
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_top_hashtags_weekly (
        platform LowCardinality(String),
        event_week DateTime,
        hashtag String,
        frequency UInt64,
        rank UInt32,
        loaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    PARTITION BY toYYYYMM(event_week)
    ORDER BY (platform, event_week, rank, hashtag)
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_author_activity (
        platform LowCardinality(String),
        author_id String,
        post_count UInt64,
        avg_sentiment Float64,
        total_reach UInt64,
        loaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    ORDER BY (platform, author_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_sentiment_time_series (
        platform LowCardinality(String),
        event_hour DateTime,
        avg_sentiment Float64,
        loaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    PARTITION BY toYYYYMM(event_hour)
    ORDER BY (platform, event_hour)
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_top_posts (
        rank UInt32,
        post_id String,
        platform LowCardinality(String),
        event_ts DateTime,
        author_id String,
        content String,
        engagement_score UInt64,
        loaded_at DateTime
    )
    ENGINE = ReplacingMergeTree(loaded_at)
    PARTITION BY toYYYYMM(event_ts)
    ORDER BY (platform, event_ts, rank, post_id)
    """,
]


def create_spark() -> SparkSession:
    builder = SparkSession.builder.appName("SocialClickHouseWarehouseLoader").master(SPARK_MASTER)
    return configure_s3a(builder).getOrCreate()


def clickhouse(sql: str, data: bytes | None = None, use_database: bool = True) -> str:
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}
    if use_database:
        params["database"] = CLICKHOUSE_DATABASE
    response = requests.post(
        CLICKHOUSE_HOST,
        params=params,
        data=(sql.encode("utf-8") if data is None else sql.encode("utf-8") + data),
        timeout=60,
    )
    response.raise_for_status()
    return response.text


def ensure_schema() -> None:
    clickhouse(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}", use_database=False)
    for ddl in DDL:
        clickhouse(ddl)


def truncate_tables(tables: list[str]) -> None:
    for table in tables:
        clickhouse(f"TRUNCATE TABLE IF EXISTS {table}")


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    return value


def normalize_doc(view: str, row: dict, loaded_at: str) -> dict:
    doc = {key: json_value(value) for key, value in row.items()}
    doc["loaded_at"] = loaded_at
    if view == "top_posts":
        doc["event_ts"] = doc.get("event_ts") or "1970-01-01 00:00:00"
    return doc


def insert_json_each_row(table: str, docs: list[dict]) -> None:
    if not docs:
        return
    payload = "\n".join(json.dumps(doc, ensure_ascii=False, default=str) for doc in docs).encode("utf-8")
    clickhouse(f"INSERT INTO {table} FORMAT JSONEachRow\n", payload)
    logger.info("Inserted %d rows into %s", len(docs), table)


def load_view(spark: SparkSession, view: str, batch_size: int) -> set[str]:
    table = VIEWS[view]
    path = f"{STORAGE_BATCH_VIEWS_BASE.rstrip('/')}/{view}"
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    platforms: set[str] = set()
    docs: list[dict] = []
    for row in spark.read.parquet(path).toLocalIterator():
        doc = normalize_doc(view, row.asDict(recursive=True), loaded_at)
        platform = doc.get("platform")
        if platform:
            platforms.add(str(platform))
        docs.append(doc)
        if len(docs) >= batch_size:
            insert_json_each_row(table, docs)
            docs.clear()
    insert_json_each_row(table, docs)
    return platforms


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--view", action="append", choices=sorted(VIEWS))
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--append", action="store_true", help="Append instead of truncating warehouse fact tables first.")
    args = parser.parse_args()

    views = args.view or list(VIEWS)
    ensure_schema()
    if not args.append:
        truncate_tables(["dim_platform", *(VIEWS[view] for view in views)])

    spark = create_spark()
    try:
        platforms: set[str] = set()
        for view in views:
            platforms.update(load_view(spark, view, args.batch_size))
        loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        insert_json_each_row("dim_platform", [{"platform": platform, "loaded_at": loaded_at} for platform in sorted(platforms)])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
