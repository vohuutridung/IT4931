#!/usr/bin/env python3
"""
Main training pipeline for Facebook Post Virality Prediction.

Usage
──────
# Production (from MinIO):
python -m ml.virality.train \
    --output-dir ml/virality/artifacts

# Development (local raw files):
python -m ml.virality.train \
    --local \
    --data-dir data/facebook_data/raw_data \
    --output-dir ml/virality/artifacts

# With Optuna hyperparameter tuning:
python -m ml.virality.train --tune

Pipeline steps
───────────────
  1. Load data  (MinIO or local fallback)
  2. Build labels (engagement score → page-normalised → percentile bins)
  3. Temporal split (90% train / 5% val / 5% test, sorted by created_at)
  4. Extract features:
       a. Text stats (emoji, hashtag, CTA, length, …)
       b. Temporal features (hour sin/cos, weekday, prime-time flags, …)
       c. Page features (rolling historical engagement, viral rate, freq, …)
       d. Media features (reel/video/photo/text from URL)
       e. PhoBERT CLS-token embeddings (768-dim, batched, cached)
  5. Fuse features (direct concatenation)
  6. (Optional) Tune hyperparameters with Optuna
  7. Train LightGBM with early stopping
  8. Evaluate on val and test splits
  9. Save all artifacts
"""

from __future__ import annotations

import argparse
import json
import logging
import os
# Sanitize SSL environment variables if they point to non-existent files/directories
for var in ["SSL_CERT_FILE", "SSL_CERT_DIR"]:
    if var in os.environ and not os.path.exists(os.environ[var]):
        del os.environ[var]

os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

ARTIFACTS_DIR = os.getenv("VIRALITY_ARTIFACTS_DIR", "ml/virality/artifacts")


# ═══════════════════════════════════════════════════════════════════════════════
# Data splitting
# ═══════════════════════════════════════════════════════════════════════════════

def temporal_split(
    df: pd.DataFrame,
    train_frac: float = 0.90,
    val_frac:   float = 0.05,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Time-ordered split: 90% train / 5% val / 5% test.

    The DataFrame must be sorted by `created_at` (ascending) before calling.
    This ensures no future information leaks into training.

    Args:
        df:         Full labelled DataFrame sorted by created_at.
        train_frac: Fraction for training.
        val_frac:   Fraction for validation.

    Returns:
        (train_df, val_df, test_df)
    """
    n = len(df)
    n_train = int(n * train_frac)
    n_val   = int(n * val_frac)

    train = df.iloc[:n_train].copy()
    val   = df.iloc[n_train: n_train + n_val].copy()
    test  = df.iloc[n_train + n_val:].copy()

    logger.info(
        "Temporal split: train=%d (%.0f%%) | val=%d (%.0f%%) | test=%d (%.0f%%)",
        len(train), 100 * len(train) / n,
        len(val),   100 * len(val)   / n,
        len(test),  100 * len(test)  / n,
    )

    # Log label distribution per split
    for name, split in [("train", train), ("val", val), ("test", test)]:
        dist = split["label"].value_counts().sort_index()
        logger.info("%s label dist: %s", name, dict(dist))

    return train, val, test


# ═══════════════════════════════════════════════════════════════════════════════
# Feature extraction helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _build_tabular_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine text stats + temporal + media features into a single DataFrame.
    Page features require the full training set for anti-leakage rolling stats
    and are handled separately in the main pipeline.
    """
    from ml.virality.features.text_features import extract_text_stats
    from ml.virality.features.temporal_features import extract_temporal_features
    from ml.virality.features.media_features import extract_media_features

    text_feats     = extract_text_stats(df)
    temporal_feats = extract_temporal_features(df)
    media_feats    = extract_media_features(df)

    return pd.concat([text_feats, temporal_feats, media_feats], axis=1)


def _get_phobert_embeddings(
    df: pd.DataFrame,
    cache_path: str,
    use_phobert: bool = True,
) -> np.ndarray:
    """
    Return PhoBERT CLS embeddings for all rows in df.
    Falls back to zero-vectors if PhoBERT is disabled or import fails.
    """
    if not use_phobert:
        logger.warning("PhoBERT disabled — using zero embeddings.")
        return np.zeros((len(df), 768), dtype=np.float32)

    from ml.virality.features.text_features import extract_phobert_embeddings

    return extract_phobert_embeddings(
        texts=df["content"].tolist(),
        post_ids=df["post_id"].tolist(),
        cache_path=cache_path,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Artifact saving
# ═══════════════════════════════════════════════════════════════════════════════

def _save_training_metadata(
    output_dir: str,
    metrics: dict,
    args: argparse.Namespace,
    n_train: int,
    n_val: int,
    n_test: int,
    feature_names: list[str],
    duration_sec: float,
) -> None:
    meta = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "n_train": n_train,
        "n_val":   n_val,
        "n_test":  n_test,
        "n_features": len(feature_names),
        "feature_names": feature_names,
        "val_metrics":   metrics.get("val"),
        "test_metrics":  metrics.get("test"),
        "training_duration_sec": round(duration_sec, 1),
        "use_phobert": not args.no_phobert,
        "tuned":       args.tune,
    }
    path = os.path.join(output_dir, "training_metadata.json")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False, default=str)
    logger.info("Saved training metadata → %s", path)


# ═══════════════════════════════════════════════════════════════════════════════
# Main pipeline
# ═══════════════════════════════════════════════════════════════════════════════

def run(args: argparse.Namespace) -> None:
    t0 = time.time()
    output_dir = args.output_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # ── Step 1: Load data ──────────────────────────────────────────────────────
    logger.info("=== Step 1: Loading data ===")
    from ml.virality.data.loader import load
    df = load(
        use_local=args.local,
        local_data_dir=args.data_dir if args.local else None,
    )

    # ── Step 2: Build labels ───────────────────────────────────────────────────
    logger.info("=== Step 2: Building labels ===")
    from ml.virality.data.labeler import build_labels, save_thresholds, save_page_medians

    df, thresholds = build_labels(df)
    save_thresholds(thresholds, os.path.join(output_dir, "label_thresholds.json"))

    # ── Step 3: Temporal split ─────────────────────────────────────────────────
    logger.info("=== Step 3: Temporal split ===")
    train_df, val_df, test_df = temporal_split(df)

    # Save page medians computed from training set only
    save_page_medians(train_df, os.path.join(output_dir, "page_medians.json"))

    # ── Step 4a: Page features (training set → rolling stats, then apply) ─────
    logger.info("=== Step 4a: Page features (anti-leakage rolling) ===")
    from ml.virality.features.page_features import (
        build_page_features,
        save_page_stats,
    )

    # Build rolling page features on training data
    train_page = build_page_features(train_df)
    save_page_stats(train_df, os.path.join(output_dir, "page_stats.pkl"))

    # For val/test: use the training-set page stats for target encoding,
    # but still compute rolling features up to the split boundary.
    # Simple approximation: use mean values from training for unseen windows.
    from ml.virality.features.page_features import get_inference_page_features, load_page_stats
    page_stats = load_page_stats(os.path.join(output_dir, "page_stats.pkl"))

    def _apply_inference_page_feats(split_df: pd.DataFrame) -> pd.DataFrame:
        rows = [
            get_inference_page_features(aid, page_stats)
            for aid in split_df["author_id"]
        ]
        return pd.DataFrame(rows, index=split_df.index)

    val_page  = _apply_inference_page_feats(val_df)
    test_page = _apply_inference_page_feats(test_df)

    # ── Step 4b: Tabular features (text + temporal + media) ────────────────────
    logger.info("=== Step 4b: Text / temporal / media features ===")
    train_tab = _build_tabular_features(train_df)
    val_tab   = _build_tabular_features(val_df)
    test_tab  = _build_tabular_features(test_df)

    # ── Step 4c: PhoBERT embeddings ────────────────────────────────────────────
    phobert_cache = os.path.join(output_dir, "phobert_cache.pkl")
    use_phobert = not args.no_phobert

    logger.info("=== Step 4c: PhoBERT embeddings (use_phobert=%s) ===", use_phobert)
    train_emb = _get_phobert_embeddings(train_df, phobert_cache, use_phobert)
    val_emb   = _get_phobert_embeddings(val_df,   phobert_cache, use_phobert)
    test_emb  = _get_phobert_embeddings(test_df,  phobert_cache, use_phobert)

    # ── Step 5: Fuse features (direct concat) ─────────────────────────────────
    logger.info("=== Step 5: Feature fusion ===")

    def _fuse(tab: pd.DataFrame, page: pd.DataFrame, emb: np.ndarray) -> np.ndarray:
        tab_arr  = tab.values.astype(np.float32)
        page_arr = page.values.astype(np.float32)
        return np.hstack([emb, tab_arr, page_arr])

    X_train = _fuse(train_tab, train_page, train_emb)
    X_val   = _fuse(val_tab,   val_page,   val_emb)
    X_test  = _fuse(test_tab,  test_page,  test_emb)

    y_train = train_df["label"].values
    y_val   = val_df["label"].values
    y_test  = test_df["label"].values

    # Build feature name list for metadata
    phobert_names = [f"phobert_{i}" for i in range(train_emb.shape[1])]
    feature_names = phobert_names + list(train_tab.columns) + list(train_page.columns)
    logger.info("Total features: %d", X_train.shape[1])

    # ── Step 6: Hyperparameter tuning (optional) ───────────────────────────────
    params = None
    if args.tune:
        logger.info("=== Step 6: Optuna hyperparameter tuning ===")
        from ml.virality.model.lgbm_model import tune_hyperparams
        params = tune_hyperparams(X_train, y_train, X_val, y_val)
        params_path = os.path.join(output_dir, "best_params.json")
        with open(params_path, "w") as f:
            json.dump(params, f, indent=2)
        logger.info("Saved best params → %s", params_path)
    else:
        logger.info("=== Step 6: Using default hyperparameters (skip Optuna) ===")

    # ── Step 7: Train ──────────────────────────────────────────────────────────
    logger.info("=== Step 7: Training LightGBM ===")
    from ml.virality.model.lgbm_model import train as lgbm_train, save_model

    model = lgbm_train(X_train, y_train, X_val, y_val, params=params)
    save_model(model, os.path.join(output_dir, "lgbm_model.pkl"))

    # ── Step 8: Evaluate ───────────────────────────────────────────────────────
    logger.info("=== Step 8: Evaluation ===")
    from ml.virality.model.lgbm_model import evaluate

    metrics: dict = {}
    metrics["val"]  = evaluate(model, X_val,  y_val,  split_name="val")
    metrics["test"] = evaluate(model, X_test, y_test, split_name="test")

    # ── Step 9: Save artifacts ─────────────────────────────────────────────────
    logger.info("=== Step 9: Saving artifacts ===")
    _save_training_metadata(
        output_dir,
        metrics,
        args,
        n_train=len(train_df),
        n_val=len(val_df),
        n_test=len(test_df),
        feature_names=feature_names,
        duration_sec=time.time() - t0,
    )

    logger.info(
        "✓ Training complete in %.1f seconds. Artifacts saved to %s",
        time.time() - t0,
        output_dir,
    )
    logger.info(
        "  Val  accuracy=%.4f | weighted_F1=%.4f",
        metrics["val"]["accuracy"],
        metrics["val"]["weighted_f1"],
    )
    logger.info(
        "  Test accuracy=%.4f | weighted_F1=%.4f",
        metrics["test"]["accuracy"],
        metrics["test"]["weighted_f1"],
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Train the Facebook Post Virality Prediction model."
    )
    p.add_argument(
        "--output-dir",
        default=ARTIFACTS_DIR,
        help="Directory for saved artifacts (default: ml/virality/artifacts)",
    )
    p.add_argument(
        "--local",
        action="store_true",
        help="Load data from local raw JSON files instead of MinIO",
    )
    p.add_argument(
        "--data-dir",
        default="data/facebook_data/raw_data",
        help="Local data directory (used with --local)",
    )
    p.add_argument(
        "--tune",
        action="store_true",
        help="Run Optuna hyperparameter search before training",
    )
    p.add_argument(
        "--no-phobert",
        action="store_true",
        help="Disable PhoBERT (use zero embeddings). For quick smoke-tests.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    )
    run(args)
