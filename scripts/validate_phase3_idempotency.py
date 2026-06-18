#!/usr/bin/env python3
"""Validate Spark batch view idempotency."""

from __future__ import annotations

import json
import os
import sys

from pyspark.sql import SparkSession

from batch import spark_batch_job
from config.settings import EVENT_TIME_MIN, SPARK_MASTER, STORAGE_BATCH_VIEWS_BASE
from config.spark import configure_s3a


VIEWS = [
    "platform_daily_stats",
    "top_hashtags_weekly",
    "author_activity",
    "sentiment_time_series",
    "top_posts",
]


def _spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
    )
    return configure_s3a(builder).getOrCreate()


def _signature() -> dict[str, list[str]]:
    spark = _spark("Phase3IdempotencySignature")
    try:
        result: dict[str, list[str]] = {}
        for view in VIEWS:
            path = f"{STORAGE_BATCH_VIEWS_BASE.rstrip('/')}/{view}"
            df = spark.read.parquet(path)
            result[view] = sorted(df.toJSON().collect())
        return result
    finally:
        spark.stop()


def main() -> None:
    before = _signature()
    validation_date = os.getenv("VALIDATION_BATCH_DATE", EVENT_TIME_MIN or "2026-03-04")
    sys.argv = ["spark_batch_job.py", "--date", validation_date]
    spark_batch_job.main()
    after = _signature()
    if before != after:
        for view in VIEWS:
            if before.get(view) != after.get(view):
                b_rows = set(before.get(view, []))
                a_rows = set(after.get(view, []))
                added = a_rows - b_rows
                removed = b_rows - a_rows
                print(f"View {view} changed!")
                print(f"Added ({len(added)}): {list(added)[:5]}")
                print(f"Removed ({len(removed)}): {list(removed)[:5]}")
        raise AssertionError("batch view rows changed after rerun")
    print(json.dumps({view: len(rows) for view, rows in after.items()}, sort_keys=True))


if __name__ == "__main__":
    main()
