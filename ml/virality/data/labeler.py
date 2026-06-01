#!/usr/bin/env python3
"""
Label construction for Facebook Post Virality Prediction.

Label formula (fixed):
    E = log(1 + likes) + 2·log(1 + comments) + 3·log(1 + shares)

Page normalisation:
    E_norm = E / median(E_page)      (per-fanpage median; default 1.0 if only 1 post)

Percentile binning (on the full dataset E_norm):
    [0, P50)     → 0  Low
    [P50, P80)   → 1  Medium
    [P80, P95)   → 2  High
    [P95, 100]   → 3  Viral

The computed thresholds are persisted as JSON artifacts so the predictor can
reproduce the same binning at inference time without re-reading training data.
"""

from __future__ import annotations

import json
import logging
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Class definitions ──────────────────────────────────────────────────────────
LABEL_NAMES = {0: "Low", 1: "Medium", 2: "High", 3: "Viral"}
PERCENTILE_CUTS = [50, 80, 95]   # lower bound of each upper class


def compute_engagement_score(df: pd.DataFrame) -> pd.Series:
    """
    E = log(1 + likes) + 2·log(1 + comments) + 3·log(1 + shares)

    Weights reflect viral mechanics:
      shares   → highest weight (content spread across networks)
      comments → medium weight  (discussion depth)
      likes    → base weight    (passive engagement)
    """
    return (
        np.log1p(df["likes"])
        + 2 * np.log1p(df["comments"])
        + 3 * np.log1p(df["shares"])
    )


def compute_page_medians(df: pd.DataFrame, score_col: str = "engagement_score") -> pd.Series:
    """
    Compute per-page median engagement score.

    Returns a Series indexed by author_id with the median E value.
    Pages with < 2 posts get a median of NaN which is later filled with 1.0.
    """
    return df.groupby("author_id")[score_col].median()


def normalise_by_page(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add E_norm = E / median(E_page) column.

    Anti-leakage note: during training we use the FULL dataset medians.
    During inference, stored page medians (from training) are used.
    This is acceptable because:
      1. Page-level median is a stable, slowly-changing quantity.
      2. We do not leak per-post future engagement.
    """
    df = df.copy()
    df["engagement_score"] = compute_engagement_score(df)

    page_medians = compute_page_medians(df)
    df["page_median"] = df["author_id"].map(page_medians).fillna(1.0)
    # Guard against zero-median pages (all posts with 0 engagement)
    df["page_median"] = df["page_median"].replace(0.0, 1.0)
    df["e_norm"] = df["engagement_score"] / df["page_median"]
    return df


def bin_labels(
    e_norm: pd.Series,
    percentile_cuts: list[int] = PERCENTILE_CUTS,
) -> tuple[pd.Series, dict]:
    """
    Bin E_norm values into 4 virality classes using percentile thresholds.

    Args:
        e_norm: Normalised engagement score Series.
        percentile_cuts: List of percentile boundaries [P50, P80, P95].

    Returns:
        (labels Series of int, thresholds dict for persistence)
    """
    thresholds = [float(np.percentile(e_norm, p)) for p in percentile_cuts]
    logger.info(
        "Label thresholds — P%d=%.4f, P%d=%.4f, P%d=%.4f",
        percentile_cuts[0], thresholds[0],
        percentile_cuts[1], thresholds[1],
        percentile_cuts[2], thresholds[2],
    )

    labels = pd.cut(
        e_norm,
        bins=[-math.inf, thresholds[0], thresholds[1], thresholds[2], math.inf],
        labels=[0, 1, 2, 3],
        right=False,
    ).astype(int)

    thresh_dict = {
        "percentile_cuts": percentile_cuts,
        "thresholds": thresholds,
        "labels": LABEL_NAMES,
    }
    return labels, thresh_dict


def build_labels(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Full label construction pipeline.

    Steps:
      1. Compute E = weighted log engagement score.
      2. Normalise by per-page median → E_norm.
      3. Bin E_norm into 4 classes using global percentiles.

    Args:
        df: DataFrame with columns [post_id, author_id, likes, comments, shares].

    Returns:
        (labelled_df, thresholds_dict)
        labelled_df gains columns: engagement_score, page_median, e_norm, label
    """
    df = normalise_by_page(df)
    labels, thresholds = bin_labels(df["e_norm"])
    df["label"] = labels

    dist = df["label"].value_counts().sort_index()
    logger.info("Label distribution:\n%s", dist.to_string())
    for cls, name in LABEL_NAMES.items():
        n = int(dist.get(cls, 0))
        pct = 100 * n / len(df)
        logger.info("  %s (%d): %d posts (%.1f%%)", name, cls, n, pct)

    return df, thresholds


def save_thresholds(thresholds: dict, path: str) -> None:
    """Persist label thresholds to JSON for use at inference time."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(thresholds, f, indent=2, ensure_ascii=False)
    logger.info("Saved label thresholds → %s", path)


def load_thresholds(path: str) -> dict:
    """Load persisted label thresholds."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def apply_saved_thresholds(e_norm_value: float, thresholds: dict) -> int:
    """
    Classify a single E_norm value using pre-computed thresholds.
    Used at inference time to avoid recomputing from training data.

    Args:
        e_norm_value: Normalised engagement score for a single post.
        thresholds:   Dict loaded from label_thresholds.json.

    Returns:
        Integer class label 0–3.
    """
    cuts = thresholds["thresholds"]
    if e_norm_value < cuts[0]:
        return 0
    if e_norm_value < cuts[1]:
        return 1
    if e_norm_value < cuts[2]:
        return 2
    return 3


def save_page_medians(df: pd.DataFrame, path: str) -> None:
    """
    Persist per-page median engagement scores to JSON.
    Used at inference to normalise a new post's score before classifying.
    """
    medians = compute_page_medians(df, score_col="engagement_score")
    data = {
        "page_medians": medians.to_dict(),
        "global_median": float(df["engagement_score"].median()),
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Saved page medians for %d pages → %s", len(medians), path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Quick smoke-test with synthetic data
    rng = np.random.default_rng(42)
    n = 500
    fake = pd.DataFrame({
        "post_id": [str(i) for i in range(n)],
        "author_id": rng.choice(["pageA", "pageB", "pageC"], size=n),
        "likes":    rng.integers(0, 10000, size=n),
        "comments": rng.integers(0, 1000, size=n),
        "shares":   rng.integers(0, 500, size=n),
    })
    labelled, thresholds = build_labels(fake)
    print(labelled[["post_id", "engagement_score", "e_norm", "label"]].head(10))
    print("\nThresholds:", thresholds)
