"""
Reddit normalizer
─────────────────
Input fields (từ posts.jsonl thực tế):
  post_id, subreddit, title, selftext, url, author, author_fullname,
  created_utc (ISO str), created_utc_raw (float),
  score, upvotes, comment_count, comments (list)
"""

import json
import re
import time
from datetime import datetime, timezone


def normalize(raw: dict) -> dict:
    content = _build_content(raw)
    return {
        "post_id":     f"reddit_{raw['post_id']}",
        "source":      "reddit",
        "event_time":  _parse_event_time(raw),
        "ingest_time": _now_ms(),
        "author_id":   raw.get("author_fullname"),
        "author_name": raw.get("author"),
        "content":     content,
        "url":         raw.get("url") or raw.get("permalink"),
        "hashtags":    _extract_hashtags(content),
        "engagement":  {
            "likes":    int(raw.get("upvotes") or raw.get("score") or 0),
            "comments": int(raw.get("comment_count") or 0),
            "shares":   int(raw.get("crossposts_count") or 0),
            "score":    int(raw.get("score") or 0),
        },
        "extra": json.dumps({
            "subreddit":    raw.get("subreddit"),
            "post_type":    raw.get("post_type"),
            "domain":       raw.get("domain"),
            "flair":        raw.get("flair_text"),
            "upvote_ratio": raw.get("upvote_ratio"),
            "comment_sample": _top_comments(raw.get("comments", []), n=3),
        }, ensure_ascii=False),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_content(raw: dict) -> str:
    parts = [raw.get("title", ""), raw.get("selftext", "")]
    return " ".join(p for p in parts if p).strip()


def _parse_event_time(raw: dict) -> int:
    # Ưu tiên created_utc_raw (float unix seconds) vì chính xác hơn ISO string
    raw_ts = raw.get("created_utc_raw")
    if raw_ts:
        return int(float(raw_ts) * 1000)

    iso = raw.get("created_utc")
    if iso:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass

    return _now_ms()


def _top_comments(comments: list, n: int = 3) -> list[str]:
    """Lấy n comment đầu để lưu làm context."""
    result = []
    for c in comments[:n]:
        body = c.get("body", "").strip()
        if body:
            result.append(body[:200])
    return result


def _extract_hashtags(text: str) -> list[str]:
    return re.findall(r"#(\w+)", text or "")


def _now_ms() -> int:
    return int(time.time() * 1000)
