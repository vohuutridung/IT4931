#!/usr/bin/env python3
"""
Text feature extraction for Facebook Post Virality Prediction.

Produces two complementary feature sets:
  1. PhoBERT embeddings (768-dim) — rich semantic representation of Vietnamese text.
  2. Hand-crafted text statistics — length, emoji, hashtag, sentiment signals, etc.

PhoBERT extraction strategy
────────────────────────────
  • Model: vinai/phobert-base (768-dim hidden state)
  • Device: auto-detected (CUDA → MPS → CPU)
  • Tokenisation: max_length=256 tokens, truncation=True, padding="max_length"
  • Pooling: CLS token (index 0) from last_hidden_state
  • Batch size: 32 (configurable via PHOBERT_BATCH_SIZE env var)
  • Caching: embeddings are cached to <artifacts_dir>/phobert_cache.pkl,
             keyed by (post_id, content_hash) to survive incremental re-runs.

Hand-crafted features
──────────────────────
  content_length, word_count, sentence_count, avg_word_length,
  emoji_count, has_emoji,
  hashtag_count, has_hashtag,
  has_question, has_exclamation,
  has_cta (call-to-action phrases),
  has_link_in_content,
  text_is_empty
"""

from __future__ import annotations

import hashlib
import logging
import os
import pickle
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PHOBERT_BATCH_SIZE = int(os.getenv("PHOBERT_BATCH_SIZE", "32"))
PHOBERT_MAX_LEN = 256
PHOBERT_MODEL_NAME = "vinai/phobert-base"

# ── Vietnamese Call-to-Action patterns ────────────────────────────────────────
_CTA_PATTERNS = re.compile(
    r"(để lại|chia sẻ|share|tag bạn|comment|bình luận|đăng k[ý|y]|"
    r"click vào|xem thêm|nhấn vào|theo dõi|follow|like nếu|"
    r"save lại|lưu lại|inbox|liên hệ|đặt hàng|mua ngay)",
    re.IGNORECASE,
)

# ── Emoji detection (Unicode ranges) ─────────────────────────────────────────
_EMOJI_RE = re.compile(
    "[\U00002600-\U000027BF"
    "\U0001F300-\U0001F9FF"
    "\U0001FA00-\U0001FA9F"
    "\U00002702-\U000027B0"
    "\u2764"
    "]+",
    flags=re.UNICODE,
)

# ── Hashtag pattern ───────────────────────────────────────────────────────────
_HASHTAG_RE = re.compile(r"#[\wÀ-ỹ]+", re.UNICODE)

# ── URL in content ────────────────────────────────────────────────────────────
_URL_RE = re.compile(r"https?://\S+")


# ═══════════════════════════════════════════════════════════════════════════════
# Device detection
# ═══════════════════════════════════════════════════════════════════════════════

def _get_device():
    """Auto-detect best available device: CUDA → MPS → CPU."""
    try:
        import torch
        if torch.cuda.is_available():
            logger.info("PhoBERT device: CUDA")
            return torch.device("cuda")
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            logger.info("PhoBERT device: MPS (Apple Silicon)")
            return torch.device("mps")
    except ImportError:
        pass
    logger.info("PhoBERT device: CPU")
    return "cpu"


# ═══════════════════════════════════════════════════════════════════════════════
# PhoBERT extraction
# ═══════════════════════════════════════════════════════════════════════════════

def _content_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _load_cache(cache_path: str) -> dict:
    if cache_path and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            data = pickle.load(f)
        logger.info("Loaded PhoBERT cache with %d entries from %s", len(data), cache_path)
        return data
    return {}


def _save_cache(cache: dict, cache_path: str) -> None:
    Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump(cache, f)
    logger.info("Saved PhoBERT cache with %d entries → %s", len(cache), cache_path)


def extract_phobert_embeddings(
    texts: list[str],
    post_ids: Optional[list[str]] = None,
    cache_path: Optional[str] = None,
    batch_size: int = PHOBERT_BATCH_SIZE,
) -> np.ndarray:
    """
    Extract PhoBERT CLS-token embeddings for a list of Vietnamese texts.

    Args:
        texts:      List of post content strings.
        post_ids:   Optional list of post IDs for cache keying.
        cache_path: Path to pickle cache file. None → no caching.
        batch_size: Number of texts per inference batch.

    Returns:
        np.ndarray of shape (len(texts), 768).
    """
    try:
        import torch
        from transformers import AutoModel, AutoTokenizer
    except ImportError as e:
        raise ImportError(
            "transformers and torch are required for PhoBERT. "
            "Install: pip install transformers torch"
        ) from e

    device = _get_device()
    cache = _load_cache(cache_path) if cache_path else {}

    # Build cache keys
    if post_ids is None:
        post_ids = [None] * len(texts)
    keys = [
        f"{pid}::{_content_hash(t)}" if pid else _content_hash(t)
        for pid, t in zip(post_ids, texts)
    ]

    # Identify which texts need embedding
    missing_indices = [i for i, k in enumerate(keys) if k not in cache]
    logger.info(
        "PhoBERT: %d texts total, %d cached, %d to embed",
        len(texts), len(texts) - len(missing_indices), len(missing_indices),
    )

    if missing_indices:
        tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME, use_fast=False)
        model = AutoModel.from_pretrained(PHOBERT_MODEL_NAME).to(device)
        model.eval()

        missing_texts = [texts[i] for i in missing_indices]

        for start in range(0, len(missing_texts), batch_size):
            batch_texts = missing_texts[start: start + batch_size]
            batch_idxs  = missing_indices[start: start + batch_size]

            inputs = tokenizer(
                batch_texts,
                max_length=PHOBERT_MAX_LEN,
                truncation=True,
                padding="max_length",
                return_tensors="pt",
            )
            inputs = {k: v.to(device) for k, v in inputs.items()}

            with torch.no_grad():
                outputs = model(**inputs)

            # CLS token embedding: shape (batch, 768)
            embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()

            for local_i, global_i in enumerate(batch_idxs):
                cache[keys[global_i]] = embeddings[local_i]

            logger.debug(
                "PhoBERT batch %d/%d done",
                start // batch_size + 1,
                (len(missing_texts) + batch_size - 1) // batch_size,
            )

        if cache_path:
            _save_cache(cache, cache_path)

    # Assemble results preserving original order
    dim = 768
    result = np.zeros((len(texts), dim), dtype=np.float32)
    for i, k in enumerate(keys):
        if k in cache:
            result[i] = cache[k]
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Hand-crafted text statistics
# ═══════════════════════════════════════════════════════════════════════════════

def extract_text_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract hand-crafted text statistics from the `content` column.

    Returns a DataFrame of numeric features, one row per post.

    Features:
        content_length      : Total character count.
        word_count          : Approximate word count (whitespace-split).
        sentence_count      : Count of sentence-ending punctuation (. ! ?).
        avg_word_length     : Mean characters per word.
        emoji_count         : Number of emoji characters/sequences.
        has_emoji           : Binary flag.
        hashtag_count       : Number of #hashtags.
        has_hashtag         : Binary flag.
        has_question        : Post ends with or contains '?'.
        has_exclamation     : Post contains '!'.
        has_cta             : Contains a call-to-action phrase.
        has_link_in_content : Content contains http(s) URL.
        text_is_empty       : Content is blank/whitespace.
    """
    texts = df["content"].fillna("").astype(str)
    records = []

    for text in texts:
        words = text.split()
        word_count = len(words)
        avg_word_length = (
            sum(len(w) for w in words) / word_count if word_count > 0 else 0.0
        )
        emojis = _EMOJI_RE.findall(text)
        hashtags = _HASHTAG_RE.findall(text)
        sentences = re.split(r"[.!?]+", text)
        sentence_count = max(len([s for s in sentences if s.strip()]), 1)

        records.append({
            "content_length":       len(text),
            "word_count":           word_count,
            "sentence_count":       sentence_count,
            "avg_word_length":      avg_word_length,
            "emoji_count":          len(emojis),
            "has_emoji":            int(len(emojis) > 0),
            "hashtag_count":        len(hashtags),
            "has_hashtag":          int(len(hashtags) > 0),
            "has_question":         int("?" in text),
            "has_exclamation":      int("!" in text),
            "has_cta":              int(bool(_CTA_PATTERNS.search(text))),
            "has_link_in_content":  int(bool(_URL_RE.search(text))),
            "text_is_empty":        int(len(text.strip()) == 0),
        })

    return pd.DataFrame(records, index=df.index)


# ═══════════════════════════════════════════════════════════════════════════════
# TF-IDF fallback (used when PhoBERT is unavailable)
# ═══════════════════════════════════════════════════════════════════════════════
def fit_tfidf(texts: list[str], max_features: int = 500):
    """Fit TF-IDF vectorizer and return (vectorizer, matrix)."""
    from sklearn.feature_extraction.text import TfidfVectorizer

    vectorizer = TfidfVectorizer(
        max_features=max_features,
        ngram_range=(1, 2),
        analyzer="word",
        min_df=2,
        sublinear_tf=True,
    )
    matrix = vectorizer.fit_transform(texts)
    return vectorizer, matrix


def transform_tfidf(vectorizer, texts: list[str]) -> np.ndarray:
    return vectorizer.transform(texts).toarray()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sample_texts = [
        "🔥 Bài viết hay quá! #Marketing #ViralContent Để lại comment nhé!",
        "Thông báo quan trọng về chính sách mới.",
        "",
    ]
    fake_df = pd.DataFrame({"content": sample_texts})
    stats = extract_text_stats(fake_df)
    print(stats.to_string())
