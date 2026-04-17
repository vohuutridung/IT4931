"""
Facebook normalizer
────────────────────
Input fields (từ post.json thực tế):
  post_id, content, url,
  reactionsCount, shareCount, commentCount,
  author: { id, name, profile },
  createdAt (unix int seconds),
  comments (list of {content, author, createdAt, ...})
"""

import json
import re
import time


def normalize(raw: dict) -> dict:
    content = raw.get("content") or ""
    author  = raw.get("author") or {}
    return {
        "post_id":     f"facebook_{raw['post_id']}",
        "source":      "facebook",
        "event_time":  _parse_created_at(raw.get("createdAt")),
        "ingest_time": _now_ms(),
        "author_id":   str(author.get("id", "")) or None,
        "author_name": author.get("name"),
        "content":     content,
        "url":         raw.get("url"),
        "hashtags":    _extract_hashtags(content),
        "engagement":  {
            "likes":    int(raw.get("reactionsCount") or 0),
            "comments": int(raw.get("commentCount") or 0),
            "shares":   int(raw.get("shareCount") or 0),
            "score":    0,
        },
        "extra": json.dumps({
            "story_id":    raw.get("storyId"),
            "feedback_id": raw.get("feedbackId"),
            "author_profile": author.get("profile"),
            "comment_sample": _top_comments(raw.get("comments", []), n=3),
        }, ensure_ascii=False),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_created_at(created_at) -> int:
    """createdAt là unix timestamp seconds (int), vd: 1770818433."""
    if created_at is None:
        return _now_ms()
    try:
        ts = float(created_at)
        # Nếu đơn vị là milliseconds (>1e12) thì giữ nguyên, còn seconds thì *1000
        return int(ts * 1000) if ts < 1e12 else int(ts)
    except (ValueError, TypeError):
        return _now_ms()


def _top_comments(comments: list, n: int = 3) -> list[str]:
    result = []
    for c in comments[:n]:
        body = c.get("content", "").strip()
        if body:
            result.append(body[:200])
    return result


def _extract_hashtags(text: str) -> list[str]:
    return re.findall(r"#(\w+)", text or "")


def _now_ms() -> int:
    return int(time.time() * 1000)
