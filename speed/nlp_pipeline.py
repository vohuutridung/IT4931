"""NLP enrichment helpers for the Speed Layer.

Heavy models are loaded once when their dependencies are present. In lean local
test environments this module falls back to deterministic lightweight logic.
"""

from __future__ import annotations

import logging
import re
import os
import json
from collections import Counter
from datetime import datetime, timezone

from config.settings import NLP_MODEL_NAME, SENTIMENT_ARTIFACTS_DIR
from shared.sentiment import lexicon_sentiment

logger = logging.getLogger(__name__)
MODEL_VERSION = NLP_MODEL_NAME

try:
    import spacy
except Exception:  # pragma: no cover
    spacy = None

try:
    from langdetect import detect
except Exception:  # pragma: no cover
    detect = None

try:
    from transformers import pipeline
except Exception:  # pragma: no cover
    pipeline = None

_nlp = None
_sentiment = None


def _artifact_is_usable(meta_path: str) -> bool:
    """Return whether a local sentiment artifact is good enough for serving."""
    min_f1 = float(os.getenv("SENTIMENT_MIN_WEIGHTED_F1", "0.80"))
    min_accuracy = float(os.getenv("SENTIMENT_MIN_ACCURACY", "0.60"))
    if not os.path.exists(meta_path):
        return True
    try:
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as exc:
        logger.warning("Could not read sentiment metadata, using fallback: %s", exc)
        return False
    if meta.get("smoke_test", False):
        logger.info("Sentiment artifact is marked smoke_test; using lexicon fallback.")
        return False
    metrics = meta.get("test_metrics") or {}
    weighted_f1 = float(metrics.get("weighted_f1") or 0.0)
    accuracy = float(metrics.get("accuracy") or 0.0)
    if weighted_f1 < min_f1 or accuracy < min_accuracy:
        logger.warning(
            "Sentiment artifact below serving threshold "
            "(weighted_f1=%.4f, accuracy=%.4f); using lexicon fallback.",
            weighted_f1,
            accuracy,
        )
        return False
    return True

if spacy:
    try:
        _nlp = spacy.load("en_core_web_sm")
    except Exception:
        _nlp = spacy.blank("en")

if pipeline:
    try:
        model_path = os.path.join(SENTIMENT_ARTIFACTS_DIR, "fine_tuned_phobert")
        meta_path  = os.path.join(SENTIMENT_ARTIFACTS_DIR, "training_metadata.json")

        force_lightweight = os.getenv("SPEED_LIGHTWEIGHT", "false").lower() in ("1", "true", "yes", "y")

        if (
            os.path.exists(os.path.join(model_path, "config.json"))
            and not force_lightweight
            and _artifact_is_usable(meta_path)
        ):
            logger.info("Loading fine-tuned PhoBERT sentiment model from %s", model_path)
            _sentiment = pipeline("sentiment-analysis", model=model_path, tokenizer=model_path)
            MODEL_VERSION = "local-fine-tuned-phobert"
        elif "phobert" in NLP_MODEL_NAME.lower():
            logger.info("PhoBERT base model cannot be loaded directly for sentiment; using lexicon fallback.")
            _sentiment = None
            MODEL_VERSION = NLP_MODEL_NAME
        else:
            logger.info("Loading default sentiment model: %s", NLP_MODEL_NAME)
            _sentiment = pipeline("sentiment-analysis", model=NLP_MODEL_NAME)
            MODEL_VERSION = NLP_MODEL_NAME
    except Exception as exc:
        logger.warning("Transformer sentiment unavailable, using fallback: %s", exc)




def analyze_sentiment(text: str) -> dict:
    text = text or ""
    if _sentiment:
        result = _sentiment(text[:512])[0]
        raw_score = float(result["score"])
        raw_label = result["label"].upper()
        if "POS" in raw_label or raw_label in ("LABEL_2",):
            score = raw_score
            label = "positive"
        elif "NEG" in raw_label or raw_label in ("LABEL_0",):
            score = -raw_score
            label = "negative"
        else:
            score = 0.0
            label = "neutral"
    else:
        score = _lexicon_sentiment(text)
        label = "positive" if score > 0.03 else "negative" if score < -0.03 else "neutral"
    return {"score": max(-1.0, min(1.0, score)), "label": label}


def _lexicon_sentiment(text: str) -> float:
    """Lexicon-based sentiment — delegates to shared module for consistency."""
    return lexicon_sentiment(text)


def extract_keywords(text: str, top_n: int = 10) -> list[str]:
    if _nlp and "parser" in _nlp.pipe_names:
        doc = _nlp(text or "")
        phrases = [chunk.text.lower().strip() for chunk in doc.noun_chunks if chunk.text.strip()]
    else:
        phrases = [
            word.lower()
            for word in re.findall(r"[A-Za-z][A-Za-z0-9_]{2,}", text or "")
            if word.lower() not in {"the", "and", "for", "with", "this", "that"}
        ]
    return [word for word, _ in Counter(phrases).most_common(top_n)]


def extract_entities(text: str) -> list[dict]:
    if not _nlp or "ner" not in _nlp.pipe_names:
        return []
    labels = {"ORG", "PERSON", "GPE", "PRODUCT"}
    return [{"text": ent.text, "label": ent.label_} for ent in _nlp(text or "").ents if ent.label_ in labels]


def detect_language(text: str) -> str:
    if detect:
        try:
            return detect(text or "")
        except Exception:
            pass
    return "en"


def enrich_post(post: dict) -> dict:
    sentiment = analyze_sentiment(post.get("content") or "")
    return {
        "post_id": post.get("post_id"),
        "sentiment_score": sentiment["score"],
        "sentiment_label": sentiment["label"],
        "keywords": extract_keywords(post.get("content") or ""),
        "entities": extract_entities(post.get("content") or ""),
        "language": detect_language(post.get("content") or ""),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "model_version": MODEL_VERSION,
    }
