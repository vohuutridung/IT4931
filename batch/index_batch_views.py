#!/usr/bin/env python3
"""Index Spark batch views into Elasticsearch Serving Layer."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession

from serving.es_indexer import ElasticsearchIndexer
from config.settings import ES_BATCH_INDEX, SPARK_MASTER, STORAGE_BATCH_VIEWS_BASE
from config.spark import configure_s3a


def create_spark() -> SparkSession:
    builder = SparkSession.builder.appName("SocialBatchViewIndexer").master(SPARK_MASTER)
    return configure_s3a(builder).getOrCreate()


def row_to_doc(view: str, row) -> dict:
    doc = {
        key: _json_value(value)
        for key, value in row.asDict(recursive=True).items()
    }
    doc["view"] = view
    doc["indexed_at"] = datetime.now(timezone.utc).isoformat()
    if "post_id" in doc:
        doc["doc_id"] = f"{view}:{doc['post_id']}"
    else:
        key = ":".join(str(v) for v in doc.values() if v is not None)[:256]
        doc["doc_id"] = f"{view}:{key}"
    return doc


def _json_value(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


def index_view(spark: SparkSession, indexer: ElasticsearchIndexer, view: str, batch_size: int) -> None:
    path = f"{STORAGE_BATCH_VIEWS_BASE.rstrip('/')}/{view}"
    docs: list[dict] = []
    for row in spark.read.parquet(path).toLocalIterator():
        docs.append(row_to_doc(view, row))
        if len(docs) >= batch_size:
            indexer.bulk_index(ES_BATCH_INDEX, docs)
            docs.clear()
    indexer.bulk_index(ES_BATCH_INDEX, docs)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--view",
        action="append",
        choices=["platform_daily_stats", "top_hashtags_weekly", "author_activity", "sentiment_time_series", "top_posts"],
    )
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    views = args.view or ["platform_daily_stats", "top_hashtags_weekly", "author_activity", "sentiment_time_series", "top_posts"]
    spark = create_spark()
    try:
        indexer = ElasticsearchIndexer()
        indexer.ensure_indices()
        for view in views:
            index_view(spark, indexer, view, args.batch_size)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
