#!/usr/bin/env python3
"""
Real-time virality predictor for Facebook Post Virality Prediction.

Loads all artifacts once at startup (model, page stats, thresholds),
then predicts for individual posts with low latency.

Usage (as a class):
    predictor = ViralityPredictor("ml/virality/artifacts")
    result = predictor.predict({
        "content":    "Bài viết hay quá! #Marketing 🔥",
        "url":        "https://www.facebook.com/reel/882538841219501/",
        "author_id":  "100044286136937",
        "created_at": 1770818433,   # unix seconds
    })
    # result = {"prediction": 2, "label": "High", "probabilities": {...}}

Usage (CLI smoke-test):
    python -m ml.virality.predictor --artifacts-dir ml/virality/artifacts
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ml.virality.safe_artifacts import safe_pickle_path

logger = logging.getLogger(__name__)

LABEL_NAMES = ["Low", "Medium", "High", "Viral"]
ARTIFACTS_DIR = os.getenv("VIRALITY_ARTIFACTS_DIR", "ml/virality/artifacts")


class ViralityPredictor:
    """
    Real-time predictor for post virality.

    All heavy artifacts (model, PhoBERT tokenizer/model) are loaded once
    at construction time to minimise per-request latency.
    """

    def __init__(self, artifacts_dir: str = ARTIFACTS_DIR) -> None:
        self._artifacts_dir = artifacts_dir
        self._model = self._load_artifact("lgbm_model.pkl", loader="pickle")
        self._thresholds = self._load_artifact("label_thresholds.json", loader="json")
        self._page_stats = self._load_artifact("page_stats.pkl", loader="pickle")
        self._page_medians = self._load_artifact("page_medians.json", loader="json")

        # PhoBERT: lazy-loaded on first call (may be unavailable in CPU-only envs)
        self._phobert_tokenizer = None
        self._phobert_model = None
        self._phobert_device = None

        # In-memory embedding cache (content hash → vector)
        self._emb_cache: dict[str, np.ndarray] = {}

        logger.info(
            "ViralityPredictor loaded from %s | model=%s",
            artifacts_dir,
            type(self._model).__name__,
        )

    # ── Artifact loading ───────────────────────────────────────────────────────

    def _load_artifact(self, filename: str, loader: str):
        path = os.path.join(self._artifacts_dir, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Artifact not found: {path}. Run ml.virality.train first."
            )
        if loader == "pickle":
            with open(safe_pickle_path(path), "rb") as f:
                return pickle.load(f)
        if loader == "json":
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        raise ValueError(f"Unknown loader: {loader}")

    # ── PhoBERT lazy init ──────────────────────────────────────────────────────

    def _init_phobert(self) -> None:
        if self._phobert_model is not None:
            return
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer

            model_name = "vinai/phobert-base"
            self._phobert_tokenizer = AutoTokenizer.from_pretrained(
                model_name, use_fast=False
            )
            if torch.cuda.is_available():
                device = torch.device("cuda")
            elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                device = torch.device("mps")
            else:
                device = torch.device("cpu")
            self._phobert_device = device
            self._phobert_model = AutoModel.from_pretrained(model_name).to(device)
            self._phobert_model.eval()
            logger.info("PhoBERT loaded on device=%s", device)
        except Exception as exc:
            logger.error("Failed to load PhoBERT: %s. Using zero embeddings.", exc)
            self._phobert_model = "unavailable"

    def _embed_text(self, text: str) -> np.ndarray:
        """Return 768-dim PhoBERT CLS embedding for a single text."""
        key = hashlib.md5(text.encode("utf-8")).hexdigest()
        if key in self._emb_cache:
            return self._emb_cache[key]

        self._init_phobert()

        if self._phobert_model == "unavailable":
            vec = np.zeros(768, dtype=np.float32)
        else:
            import torch

            inputs = self._phobert_tokenizer(
                text,
                max_length=256,
                truncation=True,
                padding="max_length",
                return_tensors="pt",
            )
            inputs = {k: v.to(self._phobert_device) for k, v in inputs.items()}
            with torch.no_grad():
                outputs = self._phobert_model(**inputs)
            vec = outputs.last_hidden_state[0, 0, :].cpu().numpy()

        self._emb_cache[key] = vec
        return vec

    # ── Feature extraction ─────────────────────────────────────────────────────

    def _extract_text_features(self, content: str) -> np.ndarray:
        from ml.virality.features.text_features import extract_text_stats

        row = pd.DataFrame({"content": [content]})
        feats = extract_text_stats(row)
        return feats.values[0].astype(np.float32)

    def _extract_temporal_features(self, created_at_unix: int) -> np.ndarray:
        from ml.virality.features.temporal_features import extract_temporal_features

        ts = datetime.fromtimestamp(created_at_unix, tz=timezone.utc)
        row = pd.DataFrame({"created_at": [ts]})
        feats = extract_temporal_features(row)
        return feats.values[0].astype(np.float32)

    def _extract_page_features(self, author_id: str) -> np.ndarray:
        from ml.virality.features.page_features import get_inference_page_features

        feats = get_inference_page_features(author_id, self._page_stats)
        return np.array(list(feats.values()), dtype=np.float32)

    def _extract_media_features(self, url: str) -> np.ndarray:
        from ml.virality.features.media_features import extract_media_features

        row = pd.DataFrame({"url": [url]})
        feats = extract_media_features(row)
        return feats.values[0].astype(np.float32)

    # ── Public predict API ─────────────────────────────────────────────────────

    def predict(self, post: dict) -> dict:
        """
        Predict virality for a single Facebook post.

        Args:
            post: Dict with keys:
                    content    (str)   — post text
                    url        (str)   — post URL (for media type detection)
                    author_id  (str)   — page/author ID
                    created_at (int)   — unix timestamp in seconds

        Returns:
            Dict:
                prediction      : int  (0=Low, 1=Medium, 2=High, 3=Viral)
                label           : str  ("Low" / "Medium" / "High" / "Viral")
                probabilities   : dict  {label_name: probability}
                confidence      : float (max probability)
        """
        content    = str(post.get("content") or "")
        url        = str(post.get("url") or "")
        author_id  = str(post.get("author_id") or "unknown")
        created_at = int(post.get("created_at") or 0)

        emb          = self._embed_text(content)
        text_feats   = self._extract_text_features(content)
        temp_feats   = self._extract_temporal_features(created_at)
        media_feats  = self._extract_media_features(url)
        page_feats   = self._extract_page_features(author_id)

        # Direct concat: PhoBERT | text_stats | temporal | media | page
        X = np.hstack([emb, text_feats, temp_feats, media_feats, page_feats]).reshape(1, -1)

        proba = self._model.predict_proba(X)[0]
        pred  = int(np.argmax(proba))

        return {
            "prediction":    pred,
            "label":         LABEL_NAMES[pred],
            "probabilities": {
                name: round(float(p), 4)
                for name, p in zip(LABEL_NAMES, proba)
            },
            "confidence":    round(float(proba[pred]), 4),
        }

    def predict_batch(self, posts: list[dict]) -> list[dict]:
        """Predict for a list of posts. Useful for batch scoring."""
        return [self.predict(p) for p in posts]


# ═══════════════════════════════════════════════════════════════════════════════
# CLI smoke-test
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default=ARTIFACTS_DIR)
    args = parser.parse_args()

    predictor = ViralityPredictor(args.artifacts_dir)

    test_posts = [
        {
            "content": (
                "🔥🔥 Quảng cáo mới hot rần rần của Panasonic\n"
                "Chỉ chưa đầy 1p, nhưng cách thương hiệu truyền tải nội dung thực sự lôi cuốn!\n"
                "#Panasonic #ThươngHiệuChuẩnNhật"
            ),
            "url":       "https://www.facebook.com/reel/882538841219501/",
            "author_id": "100044286136937",
            "created_at": 1770818433,
        },
        {
            "content":   "Thông báo họp ban thường vụ.",
            "url":       "https://www.facebook.com/somegroup/posts/12345",
            "author_id": "unknown_page",
            "created_at": 1770818433,
        },
    ]

    for i, post in enumerate(test_posts):
        result = predictor.predict(post)
        print(f"\n[Post {i+1}] {post['content'][:60]}...")
        print(f"  → Prediction: {result['label']} ({result['prediction']})")
        print(f"  → Confidence: {result['confidence']:.1%}")
        print(f"  → Probabilities: {result['probabilities']}")
