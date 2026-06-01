#!/usr/bin/env python3
"""Debug script to diagnose training pipeline issues."""
import io
import json
import logging
import sys
import traceback

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger("debug_training")

def main():
    logger.info("=== Debug Training Pipeline ===")

    # Step 1: Test imports
    logger.info("--- Testing imports ---")
    try:
        import boto3
        import pandas as pd
        import pyarrow.parquet as pq
        import numpy as np
        logger.info("Core imports OK: boto3, pandas, pyarrow, numpy")
    except ImportError as e:
        logger.error("Import failed: %s", e)
        return

    try:
        import lightgbm
        logger.info("LightGBM OK: %s", lightgbm.__version__)
    except ImportError as e:
        logger.error("LightGBM import failed: %s", e)
        return

    # Step 2: Test MinIO connection
    logger.info("--- Testing MinIO connection ---")
    try:
        from config.settings import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_REGION
        logger.info("S3_ENDPOINT=%s, S3_BUCKET=%s", S3_ENDPOINT, S3_BUCKET)

        client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
            config=boto3.session.Config(signature_version="s3v4"),
        )
        client.head_bucket(Bucket=S3_BUCKET)
        logger.info("MinIO connection OK, bucket '%s' exists", S3_BUCKET)
    except Exception as e:
        logger.error("MinIO connection failed: %s", e)
        traceback.print_exc()
        return

    # Step 3: List parquet files
    logger.info("--- Listing parquet files ---")
    try:
        from ml.virality.data.loader import list_parquet_keys, RAW_PREFIX
        keys = list_parquet_keys(client, prefix=RAW_PREFIX)
        logger.info("Found %d parquet files", len(keys))
        if keys:
            logger.info("First 3 keys: %s", keys[:3])
            logger.info("Last 3 keys: %s", keys[-3:])
        else:
            logger.error("No parquet files found!")
            return
    except Exception as e:
        logger.error("Listing failed: %s", e)
        traceback.print_exc()
        return

    # Step 4: Try reading first file
    logger.info("--- Reading first parquet file ---")
    try:
        from ml.virality.data.loader import _read_key, KEEP_COLS
        df0 = _read_key(client, keys[0])
        logger.info("First file shape: %s", df0.shape)
        logger.info("First file columns: %s", list(df0.columns))
        logger.info("First file dtypes:\n%s", df0.dtypes)
        logger.info("First file head:\n%s", df0.head(2).to_string())
    except Exception as e:
        logger.error("Reading first file failed: %s", e)
        traceback.print_exc()
        return

    # Step 5: Try full load (with progress)
    logger.info("--- Full data load (with progress) ---")
    try:
        frames = []
        errors = []
        for i, key in enumerate(keys):
            try:
                df = _read_key(client, key)
                frames.append(df)
            except Exception as exc:
                errors.append((key, str(exc)))
                logger.warning("Failed to read %s: %s", key, exc)
            if (i + 1) % 100 == 0 or i == len(keys) - 1:
                logger.info("Progress: %d/%d files read (%d errors)", i + 1, len(keys), len(errors))

        if not frames:
            logger.error("All files failed to load!")
            return

        df_all = pd.concat(frames, ignore_index=True)
        logger.info("Total rows loaded: %d", len(df_all))
        logger.info("Columns: %s", list(df_all.columns))

        # Check raw_json content
        logger.info("--- Checking raw_json column ---")
        sample_raw = df_all["raw_json"].iloc[0]
        try:
            parsed = json.loads(sample_raw)
            logger.info("raw_json sample keys: %s", list(parsed.keys()))
            logger.info("Has 'url' key: %s", "url" in parsed)
        except Exception as e:
            logger.error("Failed to parse raw_json: %s", e)
            logger.info("raw_json sample (first 200 chars): %s", sample_raw[:200])

        # Step 6: Try URL extraction
        logger.info("--- URL extraction ---")
        def _extract_url(raw_json_str):
            try:
                return json.loads(raw_json_str).get("url") or ""
            except Exception:
                return ""
        
        df_all["url"] = df_all["raw_json"].apply(_extract_url)
        url_counts = df_all["url"].apply(lambda x: bool(x)).value_counts()
        logger.info("URL extraction results: %s", dict(url_counts))

        # Step 7: Type normalization
        logger.info("--- Type normalization ---")
        df_all["created_at"] = pd.to_datetime(df_all["created_at"], utc=True)
        for col in ("likes", "comments", "shares"):
            df_all[col] = pd.to_numeric(df_all[col], errors="coerce").fillna(0).astype(int)
        df_all = df_all.dropna(subset=["post_id", "created_at"])
        df_all = df_all[df_all["post_id"].str.strip() != ""]
        df_all = df_all.sort_values("created_at").reset_index(drop=True)
        
        logger.info("After cleaning: %d rows", len(df_all))
        logger.info("Date range: %s → %s", df_all["created_at"].min(), df_all["created_at"].max())
        logger.info("Unique authors: %d", df_all["author_id"].nunique())

        # Step 8: Test label building
        logger.info("--- Label building ---")
        from ml.virality.data.labeler import build_labels
        df_labelled, thresholds = build_labels(df_all)
        logger.info("Labels built. Thresholds: %s", thresholds)
        logger.info("Label distribution:\n%s", df_labelled["label"].value_counts().sort_index())

        # Step 9: Test feature extraction (text stats only)
        logger.info("--- Text stats feature extraction (sample) ---")
        from ml.virality.features.text_features import extract_text_stats
        sample_df = df_labelled.head(10)
        text_feats = extract_text_stats(sample_df)
        logger.info("Text stats shape: %s", text_feats.shape)
        logger.info("Text stats columns: %s", list(text_feats.columns))

        logger.info("=== ALL CHECKS PASSED ===")
        if errors:
            logger.warning("%d files had read errors (see above)", len(errors))

    except Exception as e:
        logger.error("Full load failed at this point: %s", e)
        traceback.print_exc()


if __name__ == "__main__":
    main()
