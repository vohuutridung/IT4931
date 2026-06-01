#!/usr/bin/env python3
"""
Load Facebook post data from the MinIO batch layer for virality prediction.

Data flow:
  MinIO (s3a://social-lake/data/raw/facebook/) → Parquet → Pandas DataFrame

The raw Parquet schema (written by object_store_writer.py) is:
  post_id, platform, source_id, created_at, ingested_at, author_id,
  content, title, media_urls, hashtags, comments_json, likes, comments,
  shares, views, raw_json

The `url` field is extracted from `raw_json` since it is not a top-level Parquet column.
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ── MinIO / S3 settings ────────────────────────
from config.settings import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    S3_REGION,
)

RAW_PREFIX = "data/raw/facebook"

# ── Columns kept from the Parquet schema ──────────────────────────────────────
KEEP_COLS = [
    "post_id",
    "created_at",
    "author_id",
    "content",
    "hashtags",
    "likes",
    "comments",
    "shares",
    "raw_json",
]


def _s3_client():
    """Return a boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def list_parquet_keys(
    client,
    prefix: str = RAW_PREFIX,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> list[str]:
    """
    List all .parquet keys under *prefix* inside the configured bucket.

    Partition layout: data/raw/facebook/year=YYYY/month=MM/day=DD/part-*.parquet

    Args:
        client: boto3 S3 client.
        prefix: S3 key prefix to list.
        start_date: Only include partitions on or after this date (UTC).
        end_date: Only include partitions on or before this date (UTC).

    Returns:
        Sorted list of S3 object keys.
    """
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            if start_date is not None or end_date is not None:
                date = _partition_date(key)
                if date is None:
                    keys.append(key)
                    continue
                if start_date and date < start_date:
                    continue
                if end_date and date > end_date:
                    continue
            keys.append(key)
    keys.sort()
    logger.info("Found %d parquet files under s3://%s/%s", len(keys), S3_BUCKET, prefix)
    return keys


def _partition_date(key: str) -> Optional[datetime]:
    """Extract datetime from Hive-style partition path (year=YYYY/month=MM/day=DD)."""
    import re
    m = re.search(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})", key)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
    except ValueError:
        return None


def _read_key(client, key: str) -> pd.DataFrame:
    """Download and read a single Parquet key from MinIO into a DataFrame."""
    obj = client.get_object(Bucket=S3_BUCKET, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    table = pq.read_table(buf, columns=[c for c in KEEP_COLS if c != "raw_json"] + ["raw_json"])
    df = table.to_pandas()
    return df


def _extract_url(raw_json_str: str) -> str:
    """Pull the `url` field from the serialised raw_json column."""
    try:
        return json.loads(raw_json_str).get("url") or ""
    except Exception:
        return ""


def load_from_minio(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Load Facebook post data from MinIO and return a clean DataFrame.

    Returns columns:
        post_id, created_at (UTC datetime), author_id, content, hashtags (list),
        url, likes, comments, shares

    Args:
        start_date: Inclusive lower bound on post creation date.
        end_date:   Inclusive upper bound on post creation date.
    """
    client = _s3_client()
    keys = list_parquet_keys(client, start_date=start_date, end_date=end_date)
    if not keys:
        raise FileNotFoundError(
            f"No parquet files found in s3://{S3_BUCKET}/{RAW_PREFIX} "
            f"for date range {start_date} → {end_date}"
        )

    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    total_rows = 0
    for i, key in enumerate(keys):
        try:
            df = _read_key(client, key)
            frames.append(df)
            total_rows += len(df)
        except Exception as exc:
            errors.append(key)
            logger.warning("Skipping %s: %s", key, exc)
        if (i + 1) % 50 == 0 or i == len(keys) - 1:
            logger.info(
                "Reading parquet files: %d/%d done (%d rows loaded, %d errors)",
                i + 1, len(keys), total_rows, len(errors),
            )

    if not frames:
        raise RuntimeError("All parquet files failed to load.")

    df = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d rows from MinIO before cleaning", len(df))

    # ── Extract url from raw_json ──────────────────────────────────────────────
    df["url"] = df["raw_json"].apply(_extract_url)
    df = df.drop(columns=["raw_json"])

    # ── Normalise types ────────────────────────────────────────────────────────
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    for col in ("likes", "comments", "shares"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["content"] = df["content"].fillna("").astype(str)
    df["url"] = df["url"].fillna("").astype(str)
    df["author_id"] = df["author_id"].fillna("unknown").astype(str)

    # ── Drop posts missing timestamp or post_id ────────────────────────────────
    df = df.dropna(subset=["post_id", "created_at"])
    df = df[df["post_id"].str.strip() != ""]

    # ── Sort chronologically ───────────────────────────────────────────────────
    df = df.sort_values("created_at").reset_index(drop=True)

    logger.info(
        "Final dataset: %d posts | date range %s → %s",
        len(df),
        df["created_at"].min(),
        df["created_at"].max(),
    )
    return df


# ── Fallback: load from local raw JSON files (for dev without MinIO) ───────────
def load_from_local(raw_data_dir: str) -> pd.DataFrame:
    """
    Fallback loader: read from local data/facebook_data/raw_data/{page}/{snapshot}/post.json.

    Useful during local development when MinIO is not running.
    Each post.json contains a single post object (as seen from the Facebook scraper).
    """
    import glob

    pattern = os.path.join(raw_data_dir, "**", "post.json")
    paths = glob.glob(pattern, recursive=True)
    logger.info("Found %d local post.json files under %s", len(paths), raw_data_dir)

    records: list[dict] = []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            author = raw.get("author") or {}
            # Handle both list-of-posts and single-post formats
            posts = raw if isinstance(raw, list) else [raw]
            for post in posts:
                author_inner = post.get("author") or author or {}
                records.append({
                    "post_id": str(post.get("post_id") or post.get("id") or ""),
                    "author_id": str(author_inner.get("id") or "unknown"),
                    "content": str(post.get("content") or ""),
                    "url": str(post.get("url") or ""),
                    "created_at": post.get("createdAt"),
                    "likes": int(post.get("reactionsCount") or 0),
                    "comments": int(post.get("commentCount") or 0),
                    "shares": int(post.get("shareCount") or 0),
                    "hashtags": [],
                })
        except Exception as exc:
            logger.warning("Skipping %s: %s", path, exc)

    df = pd.DataFrame(records)
    df = df[df["post_id"] != ""]
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s", utc=True, errors="coerce")
    df = df.dropna(subset=["created_at"])
    df = df.sort_values("created_at").reset_index(drop=True)
    logger.info("Loaded %d posts from local files", len(df))
    return df


def load(
    use_local: bool = False,
    local_data_dir: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Unified entry-point: load from MinIO (production) or local files (dev).

    Args:
        use_local: If True, use local raw JSON files instead of MinIO.
        local_data_dir: Path to data/facebook_data/raw_data (required when use_local=True).
        start_date: Filter posts on/after this UTC datetime.
        end_date:   Filter posts on/before this UTC datetime.

    Returns:
        Cleaned DataFrame sorted by created_at.
    """
    if use_local:
        if not local_data_dir:
            raise ValueError("local_data_dir is required when use_local=True")
        return load_from_local(local_data_dir)
    return load_from_minio(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser(description="Virality data loader smoke-test")
    parser.add_argument("--local", action="store_true", help="Use local raw JSON files")
    parser.add_argument("--data-dir", default="data/facebook_data/raw_data", help="Local data dir")
    args = parser.parse_args()

    df = load(use_local=args.local, local_data_dir=args.data_dir)
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"\nDate range: {df['created_at'].min()} → {df['created_at'].max()}")
    print(f"\nPages (author_id): {df['author_id'].nunique()} unique")
