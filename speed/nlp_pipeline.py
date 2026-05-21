"""NLP enrichment helpers for the Speed Layer.

Heavy models are loaded once when their dependencies are present. In lean local
test environments this module falls back to deterministic lightweight logic.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from datetime import datetime, timezone

from config.settings import NLP_MODEL_NAME

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

if spacy:
    try:
        _nlp = spacy.load("en_core_web_sm")
    except Exception:
        _nlp = spacy.blank("en")

if pipeline:
    try:
        _sentiment = pipeline("sentiment-analysis", model=NLP_MODEL_NAME)
    except Exception as exc:
        logger.warning("Transformer sentiment unavailable, using fallback: %s", exc)

POSITIVE = {
    "amazing", "awesome", "beautiful", "benefit", "best", "better", "bullish", "calm",
    "clear", "confident", "constructive", "cute", "enjoy", "excellent", "gain", "gains",
    "good", "great", "growth", "happy", "hope", "hopeful", "improve", "improved",
    "like", "love", "positive", "profit", "profits", "recover", "recovery", "safe",
    "strong", "support", "useful", "win", "winner",
    "ổn", "tốt", "hay", "vui", "thích", "yêu", "đẹp", "xinh", "đỉnh", "tuyệt",
    "tuyệtvời", "hạnhphúc", "ủnghộ", "lãi", "tăng", "mạnh", "khỏe", "an toàn",
}
NEGATIVE = {
    "angry", "awful", "bad", "bearish", "beware", "catastrophic", "concern", "crack",
    "crash", "crisis", "cut", "cuts", "decline", "debt", "drop", "fall", "falling",
    "fear", "fraud", "gap", "hate", "inflation", "loss", "losses", "losing", "miss",
    "negative", "poor", "problem", "risk", "sad", "scam", "terrible", "weak", "worse",
    "worst", "worried",
    "buồn", "tệ", "xấu", "ghét", "chán", "khóc", "giận", "lo", "rủi ro", "lỗ",
    "giảm", "sập", "khủng hoảng", "thất vọng", "đau", "kém",
}
POSITIVE_EMOJI = {"😀", "😃", "😄", "😁", "😊", "😍", "🥰", "❤️", "❤", "👍", "🔥", "✨"}
NEGATIVE_EMOJI = {"😢", "😭", "😡", "😠", "💔", "👎", "😞", "😔", "😟", "😨"}


def analyze_sentiment(text: str) -> dict:
    text = text or ""
    if _sentiment:
        result = _sentiment(text[:512])[0]
        raw_score = float(result["score"])
        label = result["label"].lower()
        score = raw_score if "pos" in label else -raw_score
    else:
        score = _lexicon_sentiment(text)
        label = "positive" if score > 0.03 else "negative" if score < -0.03 else "neutral"
    return {"score": max(-1.0, min(1.0, score)), "label": label}


def _lexicon_sentiment(text: str) -> float:
    normalized = _normalize_text(text)
    tokens = re.findall(r"[a-z0-9_]+", normalized)
    token_count = max(len(tokens), 1)
    token_set = set(tokens)
    positive = len(token_set & {_normalize_text(word) for word in POSITIVE if " " not in word})
    negative = len(token_set & {_normalize_text(word) for word in NEGATIVE if " " not in word})

    for phrase in POSITIVE:
        normalized_phrase = _normalize_text(phrase)
        if " " in phrase and normalized_phrase in normalized:
            positive += 1
    for phrase in NEGATIVE:
        normalized_phrase = _normalize_text(phrase)
        if " " in phrase and normalized_phrase in normalized:
            negative += 1

    positive += sum(text.count(item) for item in POSITIVE_EMOJI)
    negative += sum(text.count(item) for item in NEGATIVE_EMOJI)

    exclamation_boost = min(text.count("!"), 3) * 0.03
    raw = (positive - negative) / max(token_count**0.5, 1)
    if raw > 0:
        raw += exclamation_boost
    elif raw < 0:
        raw -= exclamation_boost
    return max(-1.0, min(1.0, raw))


def _normalize_text(text: str) -> str:
    replacements = {
        "áàảãạăắằẳẵặâấầẩẫậ": "a",
        "éèẻẽẹêếềểễệ": "e",
        "íìỉĩị": "i",
        "óòỏõọôốồổỗộơớờởỡợ": "o",
        "úùủũụưứừửữự": "u",
        "ýỳỷỹỵ": "y",
        "đ": "d",
    }
    output = text.lower()
    for chars, replacement in replacements.items():
        for char in chars:
            output = output.replace(char, replacement)
    return re.sub(r"\s+", " ", output)


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
