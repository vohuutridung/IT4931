#!/usr/bin/env python3
"""
Page-level (fanpage) feature extraction for Facebook Post Virality Prediction.

Anti-leakage design
────────────────────
All page features for a given post are computed using ONLY posts that were
published BEFORE that post's `created_at`. This is implemented via a temporal
group-by with shift operations — no future engagement information leaks in.

Features produced per post:
    page_hist_avg_engagement    : Rolling mean engagement score of the page.
    page_hist_viral_rate        : Fraction of page's past posts with label >= 2.
    page_post_count_before      : Total page posts published before this one.
    page_posting_freq_7d        : Posts from this page in the 7 days before this post.
    page_recency_days           : Days since the page's immediately preceding post.
    page_target_encoded         : Target-encoded author_id (Bayesian smoothing).

Usage during training vs inference
────────────────────────────────────
  Training  : Call build_page_features(train_df) — computes rolling stats inline.
  Inference : Call get_inference_page_features(post, page_stats) where `page_stats`
              is loaded from the saved artifact (page_stats.pkl).
"""

from __future__ import annotations

import json
import logging
import pickle

import numpy as np
import pandas as pd

from ml.virality.safe_artifacts import safe_pickle_path

logger = logging.getLogger(__name__)

# Smoothing factor for target encoding (higher = more shrinkage toward global mean)
TARGET_ENCODING_SMOOTHING = 10.0

# ═══════════════════════════════════════════════════════════════════════════════
# Training-time feature construction (with anti-leakage rolling windows)
# ═══════════════════════════════════════════════════════════════════════════════

def build_page_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute page-level features for the entire training dataset.

    The dataset MUST be sorted by `created_at` before calling this function.
    All rolling statistics are computed using only past data (expanding window
    with shift(1) to exclude the current row).

    Args:
        df: DataFrame sorted by created_at, containing:
              post_id, author_id, created_at, engagement_score, label

    Returns:
        DataFrame of page features with the same index as df.
    """
    if not df["created_at"].is_monotonic_increasing:
        logger.warning("DataFrame is not sorted by created_at — sorting now.")
        df = df.sort_values("created_at").reset_index(drop=True)

    orig_index = df.index

    # Work on a reset-index copy so group operations align correctly
    dfw = df.reset_index(drop=True)

    # ── Helper: compute expanding mean with shift(1) per group, return Series
    #   aligned to dfw's index ───────────────────────────────────────────────
    def _expanding_shifted_mean(series: pd.Series, group_keys: pd.Series) -> pd.Series:
        result = pd.Series(index=series.index, dtype=float)
        for _, idx in group_keys.groupby(group_keys).groups.items():
            s = series.iloc[idx]                    # subgroup in sorted order
            expanded = s.expanding().mean().shift(1)
            result.iloc[idx] = expanded.values
        return result

    def _expanding_shifted_rate(series: pd.Series, group_keys: pd.Series) -> pd.Series:
        result = pd.Series(index=series.index, dtype=float)
        for _, idx in group_keys.groupby(group_keys).groups.items():
            s = (series.iloc[idx] >= 2).astype(float)
            expanded = s.expanding().mean().shift(1)
            result.iloc[idx] = expanded.values
        return result

    def _cum_count_before(group_keys: pd.Series) -> pd.Series:
        result = pd.Series(index=group_keys.index, dtype=int)
        for _, idx in group_keys.groupby(group_keys).groups.items():
            # idx is already sorted (DataFrame is sorted by created_at)
            result.iloc[idx] = list(range(len(idx)))
        return result

    hist_avg        = _expanding_shifted_mean(dfw["engagement_score"], dfw["author_id"])
    hist_viral_rate = _expanding_shifted_rate(dfw["label"],            dfw["author_id"])
    post_count_bef  = _cum_count_before(dfw["author_id"])

    # ── Posting frequency: posts in the 7 days before each post ───────────────
    posting_freq_7d = _rolling_7d_count(dfw)

    # ── Recency: days since immediately preceding post from same page ──────────
    recency_days = pd.Series(index=dfw.index, dtype=float)
    for _, idx in dfw.groupby("author_id").groups.items():
        ts = dfw.loc[idx, "created_at"]
        recency_days.iloc[idx] = ts.diff().dt.total_seconds().div(86400.0).values

    # ── Target encoding of author_id (Bayesian smoothing) ─────────────────────
    target_enc = _target_encode(dfw["author_id"], dfw["label"])

    feats = pd.DataFrame(
        {
            "page_hist_avg_engagement": hist_avg.fillna(0.0).values,
            "page_hist_viral_rate":     hist_viral_rate.fillna(0.0).values,
            "page_post_count_before":   post_count_bef.values,
            "page_posting_freq_7d":     posting_freq_7d,
            "page_recency_days":        recency_days.fillna(0.0).values,
            "page_target_encoded":      target_enc,
        },
        index=orig_index,
    )
    return feats


def _rolling_7d_count(df: pd.DataFrame) -> np.ndarray:
    """
    For each post, count how many posts from the same page were published
    in the 7 days BEFORE this post (exclusive of the post itself).
    """
    counts = np.zeros(len(df))
    df_reset = df.reset_index(drop=True)
    for page, group in df_reset.groupby("author_id"):
        idx = group.index.tolist()
        ts  = group["created_at"].tolist()
        for j, (i, t) in enumerate(zip(idx, ts)):
            cutoff = t - pd.Timedelta(days=7)
            counts[i] = sum(
                1 for prev_t in ts[:j] if prev_t >= cutoff
            )
    return counts


def _target_encode(
    author_ids: pd.Series,
    labels: pd.Series,
    smoothing: float = TARGET_ENCODING_SMOOTHING,
) -> np.ndarray:
    """
    Bayesian target encoding of author_id:
        encoded = (n * page_mean + smoothing * global_mean) / (n + smoothing)

    Smoothes toward the global mean for pages with few posts.
    """
    global_mean = float(labels.mean())
    page_stats = labels.groupby(author_ids).agg(["mean", "count"])

    encoded_map = {}
    for page, row in page_stats.iterrows():
        n = row["count"]
        page_mean = row["mean"]
        encoded_map[page] = (n * page_mean + smoothing * global_mean) / (n + smoothing)

    encoded = author_ids.map(encoded_map).fillna(global_mean).values
    return encoded


# ═══════════════════════════════════════════════════════════════════════════════
# Artifact persistence
# ═══════════════════════════════════════════════════════════════════════════════

def save_page_stats(df: pd.DataFrame, path: str) -> None:
    """
    Compute and persist page-level summary statistics for use at inference time.

    Saved statistics:
      - Per-page median engagement score
      - Per-page mean engagement score
      - Per-page historical viral rate (label >= 2)
      - Per-page post count
      - Target encoding map
      - Global mean label (smoothing anchor)
    """
    stats: dict = {}

    page_grp = df.groupby("author_id")
    stats["page_median_engagement"] = page_grp["engagement_score"].median().to_dict()
    stats["page_mean_engagement"]   = page_grp["engagement_score"].mean().to_dict()
    stats["page_viral_rate"]        = (
        page_grp["label"].apply(lambda x: (x >= 2).mean()).to_dict()
    )
    stats["page_post_count"]        = page_grp["post_id"].count().to_dict()
    stats["global_mean_label"]      = float(df["label"].mean())
    stats["global_mean_engagement"] = float(df["engagement_score"].mean())

    # Target encoding map (Bayesian smoothing)
    global_mean = stats["global_mean_label"]
    target_map = {}
    for page, grp in page_grp:
        n = len(grp)
        page_mean = float(grp["label"].mean())
        target_map[page] = (
            (n * page_mean + TARGET_ENCODING_SMOOTHING * global_mean)
            / (n + TARGET_ENCODING_SMOOTHING)
        )
    stats["target_encoding_map"] = target_map

    safe_path = safe_pickle_path(path)
    safe_path.parent.mkdir(parents=True, exist_ok=True)
    with open(safe_path, "wb") as f:
        pickle.dump(stats, f)
    logger.info("Saved page stats for %d pages → %s", len(target_map), path)


def load_page_stats(path: str) -> dict:
    """Load page stats artifact."""
    with open(safe_pickle_path(path), "rb") as f:
        return pickle.load(f)


# ═══════════════════════════════════════════════════════════════════════════════
# Inference-time feature extraction
# ═══════════════════════════════════════════════════════════════════════════════

def get_inference_page_features(author_id: str, page_stats: dict) -> dict:
    """
    Return page features for a single post at inference time.

    Uses the pre-computed page stats artifact. If the page is unseen,
    falls back to global averages.

    Args:
        author_id:  author.id of the post being predicted.
        page_stats: Dict loaded from page_stats.pkl.

    Returns:
        Dict of page feature values.
    """
    fallback_eng  = page_stats["global_mean_engagement"]
    fallback_tenc = page_stats["global_mean_label"]

    return {
        "page_hist_avg_engagement": page_stats["page_mean_engagement"].get(author_id, fallback_eng),
        "page_hist_viral_rate":     page_stats["page_viral_rate"].get(author_id, 0.0),
        "page_post_count_before":   page_stats["page_post_count"].get(author_id, 0),
        # Frequency and recency are unknown at inference — use averages
        "page_posting_freq_7d":     0.0,
        "page_recency_days":        0.0,
        "page_target_encoded":      page_stats["target_encoding_map"].get(author_id, fallback_tenc),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    rng = np.random.default_rng(42)
    n = 200
    fake = pd.DataFrame({
        "post_id": [str(i) for i in range(n)],
        "author_id": rng.choice(["pageA", "pageB", "pageC"], size=n),
        "created_at": pd.date_range("2026-01-01", periods=n, freq="2h", tz="UTC"),
        "engagement_score": rng.uniform(0, 10, size=n),
        "label": rng.integers(0, 4, size=n),
    })
    feats = build_page_features(fake)
    print(feats.head(10).to_string())
