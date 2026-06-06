#!/usr/bin/env python3
"""
Load Facebook post and comment text from MinIO or local files for sentiment training.
"""

from __future__ import annotations

import io
import json
import logging
import os
import glob
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

from config.settings import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_BUCKET,
    S3_REGION,
)

RAW_PREFIX = "data/raw/facebook"

KEEP_COLS = [
    "post_id",
    "created_at",
    "content",
    "raw_json",
]


def _s3_client():
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
    import re
    m = re.search(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})", key)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
    except ValueError:
        return None


def _extract_texts_from_post(post_data: dict) -> list[dict]:
    """Extract post content and nested comment texts with timestamps."""
    extracted = []
    
    # 1. Post content
    created_at_val = post_data.get("createdAt") or post_data.get("created_at")
    if isinstance(created_at_val, str):
        try:
            dt = pd.to_datetime(created_at_val, utc=True)
            created_at = int(dt.timestamp())
        except Exception:
            created_at = 0
    else:
        created_at = int(created_at_val or 0)

    post_content = post_data.get("content") or ""
    post_id = str(post_data.get("post_id") or post_data.get("id") or "")
    
    if post_content.strip():
        extracted.append({
            "text": post_content,
            "source_type": "post",
            "post_id": post_id,
            "created_at": created_at,
        })
        
    # 2. Comment texts (Ignored for Sentiment training to keep data size reasonable)
    # comments = post_data.get("comments") or []
    # for comment in comments:
    #     c_content = comment.get("content") or comment.get("text") or ""
    #     c_created = int(comment.get("createdAt") or created_at)
    #     if c_content.strip():
    #         extracted.append({
    #             "text": c_content,
    #             "source_type": "comment",
    #             "post_id": post_id,
    #             "created_at": c_created,
    #         })
            
    return extracted


def _read_key_texts(client, key: str) -> list[dict]:
    """Download parquet and extract all text records."""
    obj = client.get_object(Bucket=S3_BUCKET, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    table = pq.read_table(buf, columns=KEEP_COLS)
    df = table.to_pandas()
    
    all_texts = []
    for _, row in df.iterrows():
        raw_json_str = row.get("raw_json")
        if pd.isna(raw_json_str) or not raw_json_str:
            # Fallback to direct content if raw_json is missing
            post_content = row.get("content") or ""
            if post_content.strip():
                all_texts.append({
                    "text": post_content,
                    "source_type": "post",
                    "post_id": str(row.get("post_id") or ""),
                    "created_at": int(pd.to_datetime(row.get("created_at"), errors="coerce").timestamp() or 0),
                })
        else:
            try:
                post_data = json.loads(raw_json_str)
                all_texts.extend(_extract_texts_from_post(post_data))
            except Exception:
                pass
    return all_texts


def load_from_minio(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    client = _s3_client()
    keys = list_parquet_keys(client, start_date=start_date, end_date=end_date)
    if not keys:
        raise FileNotFoundError(
            f"No parquet files found in s3://{S3_BUCKET}/{RAW_PREFIX} "
            f"for date range {start_date} → {end_date}"
        )

    all_records: list[dict] = []
    errors: list[str] = []
    
    for i, key in enumerate(keys):
        try:
            records = _read_key_texts(client, key)
            all_records.extend(records)
        except Exception as exc:
            errors.append(key)
            logger.warning("Skipping %s: %s", key, exc)
            
        if (i + 1) % 50 == 0 or i == len(keys) - 1:
            logger.info(
                "Reading parquet files: %d/%d done (%d text samples loaded, %d errors)",
                i + 1, len(keys), len(all_records), len(errors),
            )

    if not all_records:
        raise RuntimeError("No text data could be loaded from MinIO.")

    df = pd.DataFrame(all_records)
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s", utc=True, errors="coerce")
    df = df.dropna(subset=["text", "created_at"])
    df = df[df["text"].str.strip() != ""]
    df = df.sort_values("created_at").reset_index(drop=True)
    
    logger.info(
        "Final sentiment training dataset: %d text samples | date range %s → %s",
        len(df), df["created_at"].min(), df["created_at"].max()
    )
    return df


def load_from_local(raw_data_dir: str) -> pd.DataFrame:
    pattern = os.path.join(raw_data_dir, "**", "post.json")
    paths = glob.glob(pattern, recursive=True)
    logger.info("Found %d local post.json files under %s", len(paths), raw_data_dir)

    all_records: list[dict] = []
    for path in paths:
        try:
            with open(path, encoding="utf-8-sig") as f:
                raw = json.load(f)
            posts = raw if isinstance(raw, list) else [raw]
            for post in posts:
                all_records.extend(_extract_texts_from_post(post))
        except Exception as exc:
            logger.warning("Skipping %s: %s", path, exc)

    if not all_records:
        raise RuntimeError("No text data could be loaded from local files.")

    df = pd.DataFrame(all_records)
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s", utc=True, errors="coerce")
    df = df.dropna(subset=["text", "created_at"])
    df = df[df["text"].str.strip() != ""]
    df = df.sort_values("created_at").reset_index(drop=True)
    logger.info("Loaded %d text samples from local files", len(df))
    return df


def load(
    use_local: bool = False,
    local_data_dir: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    if use_local:
        if not local_data_dir:
            raise ValueError("local_data_dir is required when use_local=True")
        return load_from_local(local_data_dir)
    return load_from_minio(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = load(use_local=True, local_data_dir="data/facebook_data/raw_data")
    print(df.head())
    print(df["source_type"].value_counts())
