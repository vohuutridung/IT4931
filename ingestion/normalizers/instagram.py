"""
Instagram normalizer
─────────────────────
Input fields (từ posts.jsonl thực tế):
  id, caption, hashtags (list), url,
  timestamp (ISO str: "2026-04-08T14:57:17.000Z"),
  likesCount, commentsCount,
  ownerUsername, ownerId, ownerFullName,
  inputUrl (hashtag source URL)
"""

import json
import re
import time
from datetime import datetime, timezone


def normalize(raw: dict) -> dict:
    caption = raw.get("caption") or ""
    return {
        "post_id":     f"instagram_{raw['id']}",
        "source":      "instagram",
        "event_time":  _parse_timestamp(raw.get("timestamp")),
        "ingest_time": _now_ms(),
        "author_id":   raw.get("ownerId"),
        "author_name": raw.get("ownerUsername") or raw.get("ownerFullName"),
        "content":     caption,
        "url":         raw.get("url"),
        "hashtags":    raw.get("hashtags") or _extract_hashtags(caption),
        "engagement":  {
            "likes":    int(raw.get("likesCount") or 0),
            "comments": int(raw.get("commentsCount") or 0),
            "shares":   0,
            "score":    0,
        },
        "extra": json.dumps({
            "owner_full_name": raw.get("ownerFullName"),
            "short_code":      raw.get("shortCode"),
            "type":            raw.get("type"),
            "product_type":    raw.get("productType"),
            "source_hashtag":  _extract_source_hashtag(raw.get("inputUrl", "")),
            "first_comment":   (raw.get("firstComment") or "")[:200],
        }, ensure_ascii=False),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_timestamp(ts: str | None) -> int:
    if not ts:
        return _now_ms()
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError):
        return _now_ms()


def _extract_source_hashtag(input_url: str) -> str | None:
    """Lấy hashtag từ inputUrl — vd: .../tags/finance → 'finance'."""
    m = re.search(r"/tags/([^/]+)", input_url or "")
    return m.group(1) if m else None


def _extract_hashtags(text: str) -> list[str]:
    return re.findall(r"#(\w+)", text or "")


def _now_ms() -> int:
    return int(time.time() * 1000)
