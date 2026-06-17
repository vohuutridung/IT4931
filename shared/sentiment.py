"""Shared sentiment analysis logic for batch and speed layers.

This module is the **single source of truth** for lexicon-based sentiment
scoring across the Lambda architecture.  Both the Spark batch UDF and the
speed-layer NLP pipeline delegate here so that identical text always
produces the same sentiment score regardless of the layer that processes it.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Vietnamese text normalization
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    """Normalize Vietnamese text: remove diacritics, lowercase, collapse whitespace."""
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


# ---------------------------------------------------------------------------
# Sentiment lexicons
# ---------------------------------------------------------------------------

POSITIVE = {
    "amazing", "awesome", "beautiful", "benefit", "best", "better", "bullish", "calm",
    "clear", "confident", "constructive", "cute", "enjoy", "excellent", "gain", "gains",
    "good", "great", "growth", "happy", "hope", "hopeful", "improve", "improved",
    "like", "love", "positive", "profit", "profits", "recover", "recovery", "safe",
    "strong", "support", "useful", "win", "winner",
    "ổn", "tốt", "hay", "vui", "thích", "yêu", "đẹp", "xinh", "đỉnh", "tuyệt",
    "tuyệt vời", "hạnh phúc", "ủng hộ", "lãi", "tăng", "mạnh", "khỏe", "an toàn",
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

# Pre-normalized word/phrase sets — stripped of non-alphanumeric chars after
# Vietnamese normalization so they match tokenised text exactly.
NORMALIZED_POSITIVE_WORDS = {
    re.sub(r"[^a-z0-9_]+", "", normalize_text(w))
    for w in POSITIVE if " " not in w
}
NORMALIZED_NEGATIVE_WORDS = {
    re.sub(r"[^a-z0-9_]+", "", normalize_text(w))
    for w in NEGATIVE if " " not in w
}
NORMALIZED_POSITIVE_PHRASES = [
    re.sub(r"[^a-z0-9_]+", " ", normalize_text(p)).strip()
    for p in POSITIVE if " " in p
]
NORMALIZED_NEGATIVE_PHRASES = [
    re.sub(r"[^a-z0-9_]+", " ", normalize_text(p)).strip()
    for p in NEGATIVE if " " in p
]


# ---------------------------------------------------------------------------
# Core scoring function — used by both batch and speed layers
# ---------------------------------------------------------------------------

def lexicon_sentiment(text: str) -> float:
    """Lightweight lexicon-based sentiment analysis.

    Returns a score in [-1.0, 1.0].  Positive text → positive score,
    negative text → negative score, neutral → near zero.
    """
    if not text:
        return 0.0

    normalized = normalize_text(text)
    tokens = re.findall(r"[a-z0-9_]+", normalized)
    token_count = max(len(tokens), 1)
    token_set = set(tokens)

    positive = len(token_set & NORMALIZED_POSITIVE_WORDS)
    negative = len(token_set & NORMALIZED_NEGATIVE_WORDS)

    # Space-bounded phrase matching (avoids partial matches)
    normalized_clean = f" {re.sub(r'[^a-z0-9_]+', ' ', normalized)} "
    for norm_phrase in NORMALIZED_POSITIVE_PHRASES:
        if f" {norm_phrase} " in normalized_clean:
            positive += 1
    for norm_phrase in NORMALIZED_NEGATIVE_PHRASES:
        if f" {norm_phrase} " in normalized_clean:
            negative += 1

    # Emoji scoring
    positive += sum(text.count(item) for item in POSITIVE_EMOJI)
    negative += sum(text.count(item) for item in NEGATIVE_EMOJI)

    exclamation_boost = min(text.count("!"), 3) * 0.03
    raw = (positive - negative) / max(token_count ** 0.5, 1)
    if raw > 0:
        raw += exclamation_boost
    elif raw < 0:
        raw -= exclamation_boost
    return max(-1.0, min(1.0, raw))
