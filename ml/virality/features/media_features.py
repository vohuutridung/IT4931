#!/usr/bin/env python3
"""
Media-type feature extraction for Facebook Post Virality Prediction.

Media type is detected by parsing the post `url` field, which uses
Facebook's standard URL patterns (confirmed by reading the actual data):

    /reel/<id>      → Reel (short video)
    /video/<id>     → Regular video
    /photo/<id>     → Photo/image post
    /photo.php      → Photo (alternate URL format)
    facebook.com/<page> (no media path) → Text or link post
    External URL in content  → Link post

Features produced:
    is_reel             : 1 if URL contains /reel/
    is_video            : 1 if URL contains /video/ (not reel)
    is_photo            : 1 if URL contains /photo
    is_text             : 1 if no media detected
    content_type_encoded: Integer encoded content type
                          0=text, 1=photo, 2=video, 3=reel
    has_media           : 1 if any media detected (reel | video | photo)
"""

from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# ── URL pattern matchers ───────────────────────────────────────────────────────
_REEL_RE  = re.compile(r"/reel/", re.IGNORECASE)
_VIDEO_RE = re.compile(r"/video/", re.IGNORECASE)
_PHOTO_RE = re.compile(r"/photo", re.IGNORECASE)   # covers /photo/ and /photo.php

# Content type integer encoding for LightGBM
_CONTENT_TYPE_MAP = {"text": 0, "photo": 1, "video": 2, "reel": 3}


def _classify_url(url: str) -> str:
    """
    Classify a Facebook URL into content type.

    Priority: reel > video > photo > text
    """
    if not url:
        return "text"
    if _REEL_RE.search(url):
        return "reel"
    if _VIDEO_RE.search(url):
        return "video"
    if _PHOTO_RE.search(url):
        return "photo"
    return "text"


def extract_media_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract media-type features from the `url` column.

    Args:
        df: DataFrame containing a `url` column (string).

    Returns:
        DataFrame of media features with the same index as df.
    """
    urls = df["url"].fillna("").astype(str)
    content_types = urls.map(_classify_url)

    is_reel  = (content_types == "reel").astype(int)
    is_video = (content_types == "video").astype(int)
    is_photo = (content_types == "photo").astype(int)
    is_text  = (content_types == "text").astype(int)
    has_media = (is_reel | is_video | is_photo).astype(int)
    content_type_encoded = content_types.map(_CONTENT_TYPE_MAP).fillna(0).astype(int)

    feats = pd.DataFrame(
        {
            "is_reel":               is_reel.values,
            "is_video":              is_video.values,
            "is_photo":              is_photo.values,
            "is_text":               is_text.values,
            "has_media":             has_media.values,
            "content_type_encoded":  content_type_encoded.values,
        },
        index=df.index,
    )

    # Log distribution for visibility
    dist = content_types.value_counts()
    logger.info("Media type distribution:\n%s", dist.to_string())

    return feats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sample = pd.DataFrame({
        "url": [
            "https://www.facebook.com/reel/882538841219501/",
            "https://www.facebook.com/video/123456",
            "https://www.facebook.com/photo.php?fbid=789",
            "https://www.facebook.com/beatvn.network/posts/abc123",
            "",
            None,
        ]
    })
    print(extract_media_features(sample).to_string())
