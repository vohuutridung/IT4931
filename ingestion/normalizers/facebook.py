"""
Comment normalizer — production-ready version
Supports: Facebook (extensible to other platforms)

Changes vs previous version:
  - _walk_tree()  : flatten nested children → NLP-ready flat list
  - CommentExtra  : typed, consistent extra schema cross-platform
  - normalize_batch now calls _walk_tree before processing
"""

import re
import html
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# =========================================================
# Constants
# =========================================================

MAX_COMMENTS      = 1_000
MAX_CONTENT_LEN   = 2_000
MIN_CONTENT_CHARS = 4       # meaningful Unicode word-chars after stripping


# =========================================================
# Extra schema  (consistent cross-platform)
# =========================================================

@dataclass
class CommentExtra:
    """
    Canonical metadata attached to every normalized comment.

    Fields are platform-agnostic. Platform-specific fields that
    have no cross-platform equivalent go into ``platform_meta``.

    Attributes:
        parent_id     : ID of the direct parent comment; None = top-level.
        depth         : Thread depth. 0 = top-level, 1 = reply, 2 = reply-to-reply.
        reply_count   : Number of direct children (from raw source).
        is_truncated  : True when content was cut at MAX_CONTENT_LEN.
        platform_meta : Free-form dict for source-specific fields (not used in NLP).
    """
    parent_id:     Optional[str]
    depth:         int
    reply_count:   int
    is_truncated:  bool
    platform_meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =========================================================
# Internal helpers
# =========================================================



def _coalesce_int(*values: Any) -> int:
    """
    Return the first non-None value cast to int.
    Handles count = 0 correctly — unlike ``or`` chaining.
    """
    for v in values:
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                continue
    return 0


def _parse_timestamp(value: Any) -> Optional[int]:
    """
    Parse any timestamp into UTC milliseconds, truncated to the minute (giây bị bỏ).
    Accepts: ISO-8601 string | unix-seconds float | unix-ms float.
    Returns None on failure so the caller decides the fallback.
    """
    if value is None:
        return None
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            ts = float(value)
            ts_sec = ts / 1_000 if ts >= 1e12 else ts
            dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)

        return int(dt.timestamp() * 1_000)

    except Exception:
        logger.warning("Cannot parse timestamp: %r", value)
        return None


def _clean_text(text: str) -> str:
    """HTML-unescape, strip URLs, collapse whitespace."""
    text = html.unescape(text or "")
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _safe_truncate(text: str, max_len: int = MAX_CONTENT_LEN) -> tuple:
    """
    Truncate at word boundary.
    Returns (text, is_truncated) so callers can set extra.is_truncated.
    """
    if len(text) <= max_len:
        return text, False
    return text[:max_len].rsplit(" ", 1)[0], True


def _is_low_value(text: str) -> bool:
    """True when text carries fewer than MIN_CONTENT_CHARS word characters."""
    if not text:
        return True
    clean = re.sub(r"\W+", "", text, flags=re.UNICODE)
    return len(clean) < MIN_CONTENT_CHARS


# =========================================================
# Spam detection
# =========================================================

_SPAM_RE = re.compile(
    r"(t\.me/|telegram\.me/|@\w{5,})"  # Telegram handles / links
    r"(?:\+\d|0\d)[\d\s\-\(\)]{7,}"           # Phone numbers
    r"|\b(mdma|lsd|dmt|shrooms)\b",
    re.IGNORECASE,
)


def _is_spam(text: str) -> bool:
    return bool(_SPAM_RE.search(text))


# =========================================================
# Deduplication key
# =========================================================

def _build_key(cmt: Dict[str, Any]) -> str:
    """
    Stable dedup key from id / commentId.
    If both exist and differ they are combined to avoid false merges.
    """
    cid    = cmt.get("id")
    alt_id = cmt.get("commentId")

    if cid and alt_id:
        return cid if cid == alt_id else f"{cid}|{alt_id}"

    key = cid or alt_id or ""
    if not key:
        logger.debug("Comment missing id: %r", cmt)
    return key


# =========================================================
# Tree → flat list
# =========================================================

def _walk_tree(
    comments: List[Dict[str, Any]],
    *,
    parent_id: Optional[str] = None,
    depth: int = 0,
) -> List[Dict[str, Any]]:
    """
    Recursively flatten a nested comment tree into a flat list (DFS order).

    Each node gets two injected fields before normalization:
      ``_parent_id`` — ID of the direct parent (None for root nodes).
      ``_depth``     — Thread depth: 0 = root, 1 = reply, 2 = reply-to-reply.

    Children are read from the ``"children"`` key (Facebook convention).
    Nodes without ``"children"`` are treated as leaves — no error raised.

    Args:
        comments:  Sibling comment dicts at the current level.
        parent_id: Key of the parent node passed down from the caller.
        depth:     Current depth (starts at 0 for top-level calls).

    Returns:
        Flat list of all nodes with ``_parent_id`` / ``_depth`` injected.
    """
    flat: List[Dict[str, Any]] = []

    for cmt in comments:
        if not isinstance(cmt, dict):
            continue

        # Shallow-copy so we never mutate the caller's raw data.
        # Children are NOT deep-copied — they are handled in the recursive
        # call below, each getting their own copy at that level.
        node = {**cmt, "_parent_id": parent_id, "_depth": depth}

        flat.append(node)

        children = cmt.get("children") or []   # read from original, not node
        if children:
            node_key = _build_key(node)
            flat.extend(
                _walk_tree(children, parent_id=node_key or None, depth=depth + 1)
            )

    return flat


# =========================================================
# Single-comment normalizer
# =========================================================

def normalize_comment(
    cmt: Dict[str, Any],
    seen: Set[str],
    *,
    debug: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Normalize one raw comment dict into the canonical schema.

    Expects ``_parent_id`` and ``_depth`` pre-injected by :func:`_walk_tree`.
    Safe to call standalone — falls back to raw fields / sensible defaults.

    Args:
        cmt:   Raw comment dict (may carry _parent_id / _depth from walk_tree).
        seen:  Shared dedup set managed by the caller (e.g. normalize_batch).
        debug: Attach the original payload under ``"_raw"`` when True.

    Returns:
        Normalized dict, or ``None`` if the comment should be dropped.
    """
    if not isinstance(cmt, dict):
        return None

    # ── Deduplication ────────────────────────────────────
    key = _build_key(cmt)
    if not key or key in seen:
        return None
    seen.add(key)

    # ── Content ───────────────────────────────────────────
    raw_text = cmt.get("content") or cmt.get("message") or ""
    content  = _clean_text(raw_text)

    if _is_low_value(content):
        logger.debug("Low-value comment dropped: %s", key)
        return None

    if _is_spam(content):
        logger.debug("Spam comment dropped: %s", key)
        return None

    # ── Timestamp ─────────────────────────────────────────
    created_at = _parse_timestamp(cmt.get("createdAt"))

    # ── Author ────────────────────────────────────────────
    author_raw = cmt.get("author") or cmt.get("user") or {}
    author: Dict[str, str] = {
        "id":   str(author_raw.get("id")   or "unknown"),
        "name": str(author_raw.get("name") or "unknown"),
    }

    # ── Truncation ────────────────────────────────────────
    truncated_content, is_truncated = _safe_truncate(content)

    # ── Extra — tree context ──────────────────────────────
    # Prefer _walk_tree-injected fields; fall back to raw fields for standalone use.
    raw_parent = (
        cmt.get("_parent_id")
        or cmt.get("parent_id")
        or cmt.get("parentFeedbackId")
        or None
    )
    parent_id = raw_parent if raw_parent else None   # normalize "" / "0" → None

    depth_val = cmt.get("_depth")
    depth = int(depth_val) if depth_val is not None else int(cmt.get("depth") or 0)

    # ── Extra — canonical schema ──────────────────────────
    extra = CommentExtra(
        parent_id     = parent_id,
        depth         = depth,
        reply_count   = _coalesce_int(
            cmt.get("reply_count"),
            cmt.get("subCommentsCount"),
        ),
        is_truncated  = is_truncated,
        platform_meta = {
            # Facebook-specific — transparent to NLP pipelines
            "feedback_id":  cmt.get("feedbackId"),
            "attachments":  cmt.get("attachments") or [],
        },
    )

    # ── Assemble ──────────────────────────────────────────
    result: Dict[str, Any] = {
        "id":         key,
        "content":    truncated_content,
        "author":     author,
        "created_at": created_at,
        "like_count": _coalesce_int(
            cmt.get("like_count"),
            cmt.get("likes"),
            cmt.get("reactionsCount"),
        ),
        "extra": extra.to_dict(),
    }

    if debug:
        result["_raw"] = cmt

    return result


# =========================================================
# Batch normalizer  (public API)
# =========================================================

def normalize_batch(
    comments: List[Dict[str, Any]],
    *,
    max_comments: int = MAX_COMMENTS,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Flatten the comment tree, then normalize every node.

    Processing pipeline:
      1. :func:`_walk_tree`        — DFS-flatten tree, inject _parent_id / _depth.
      2. :func:`normalize_comment` — filter noise/spam, clean, build schema.
      3. Size cap at ``max_comments`` applied to *output* (not input).

    Args:
        comments:     Top-level comment list (may contain nested children).
        max_comments: Hard cap on the number of normalized results returned.
        debug:        Include ``"_raw"`` in each result when True.

    Returns:
        Flat list of normalized comment dicts; thread order (DFS) preserved.
    """
    if not isinstance(comments, list):
        logger.warning("normalize_batch expected list, got %s", type(comments))
        return []

    flat_tree = _walk_tree(comments)

    seen:    Set[str]             = set()
    results: List[Dict[str, Any]] = []

    for cmt in flat_tree:
        if len(results) >= max_comments:
            logger.info("MAX_COMMENTS (%d) reached; stopping.", max_comments)
            break

        normalized = normalize_comment(
            cmt,
            seen,
            debug=debug,
        )
        if normalized:
            results.append(normalized)

    return results


# =========================================================
# Post normalizer  (public API)
# =========================================================

def normalize(raw: dict) -> Optional[Dict[str, Any]]:
    """
    Normalize a raw Facebook post dict into the canonical SocialPost schema.

    Args:
        raw: Raw post dict from Facebook scraper.

    Returns:
        Normalized post dict matching SocialPost Avro schema, or None if invalid.
    """
    if not isinstance(raw, dict):
        logger.warning("normalize expected dict, got %s", type(raw))
        return None

    # ── Required fields ───────────────────────────────────
    post_id = raw.get("post_id") or raw.get("id")
    if not post_id:
        logger.warning("Post missing post_id")
        return None

    # ── Timestamps ────────────────────────────────────────
    event_time = _parse_timestamp(raw.get("createdAt"))
    if event_time is None:
        logger.warning("Post %s missing valid timestamp", post_id)
        return None

    from datetime import datetime
    ingest_time = int(datetime.now(timezone.utc).timestamp() * 1000)

    # ── Author ────────────────────────────────────────────
    author_raw = raw.get("author") or {}
    author_id = author_raw.get("id")
    author_name = author_raw.get("name")

    # ── Content ───────────────────────────────────────────
    content = _clean_text(raw.get("content") or "")

    # ── URL ───────────────────────────────────────────────
    url = raw.get("url")

    # ── Hashtags ──────────────────────────────────────────
    hashtags = []  # Facebook may not have explicit hashtags in content

    # ── Engagement ────────────────────────────────────────
    engagement = {
        "likes": _coalesce_int(raw.get("reactionsCount"), raw.get("likes")),
        "comments": _coalesce_int(raw.get("commentCount")),
        "shares": _coalesce_int(raw.get("shareCount")),
        "score": 0,  # Facebook doesn't have score like Reddit
        "video_views": 0,  # Not in sample
        "comments_normalized_count": 0,  # Will set after normalizing comments
    }

    # ── Comments ──────────────────────────────────────────
    raw_comments = raw.get("comments") or []
    if isinstance(raw_comments, list):
        normalized_comments = normalize_batch(raw_comments, max_comments=MAX_COMMENTS)
        engagement["comments_normalized_count"] = len(normalized_comments)
    else:
        normalized_comments = []

    # ── Extra ─────────────────────────────────────────────
    extra = {
        "feedback_id": raw.get("feedbackId"),
        "story_id": raw.get("storyId"),
        # Add other platform-specific fields
    }

    # ── Assemble ──────────────────────────────────────────
    return {
        "schema_version": 1,
        "post_id": str(post_id),
        "source": "facebook",
        "event_time": event_time,
        "ingest_time": ingest_time,
        "author_id": author_id,
        "author_name": author_name,
        "content": content,
        "url": url,
        "hashtags": hashtags,
        "engagement": engagement,
        "comments": normalized_comments,
        "extra": extra,  # Plain dict, will be JSON serialized by producer
    }


# =========================================================
# Smoke-test  (python comment_normalizer.py)
# =========================================================

if __name__ == "__main__":
    from pprint import pprint

    # createdAt dùng unix-seconds (giống Facebook API thực tế)
    # Ghi chú datetime UTC bên cạnh để dễ đọc
    samples = [
        # ── Root với 2 reply lồng nhau ────────────────────
        {
            "id": "root-1",
            "content": "Bài viết rất hay, cảm ơn tác giả đã chia sẻ!",
            "author": {"id": "u1", "name": "An Nguyen"},
            "createdAt": 1605397383,            # 2020-11-15 05:43:03 UTC
            "reactionsCount": 12,
            "subCommentsCount": 2,
            "feedbackId": "fb-root-1",
            "children": [
                {
                    "id": "child-1a",
                    "content": "Mình cũng thấy vậy, học được nhiều thứ từ bài này.",
                    "author": {"id": "u2", "name": "Binh Tran"},
                    "createdAt": 1605400012,    # 2020-11-15 06:26:52 UTC
                    "reactionsCount": 3,
                    "subCommentsCount": 1,
                    "children": [
                        {
                            "id": "grand-1a-i",
                            "content": "Đồng ý! Phần giải thích ARM rất rõ ràng và dễ hiểu.",
                            "author": {"id": "u3", "name": "Cuong Le"},
                            "createdAt": 1605401055,  # 2020-11-15 06:44:15 UTC
                            "reactionsCount": 1,
                            "children": [],
                        }
                    ],
                },
                {
                    "id": "child-1b",
                    "content": "Cho mình hỏi thêm về il2cpp được không bạn?",
                    "author": {"id": "u4", "name": "Dao Pham"},
                    "createdAt": 1605402317,    # 2020-11-15 07:05:17 UTC
                    "reactionsCount": 0,
                    "children": [],
                },
            ],
        },
        # ── Low-value → bị drop ───────────────────────────
        {
            "id": "low-1",
            "content": "👍",
            "createdAt": 1605410000,            # 2020-11-15 09:13:20 UTC
        },
        # ── Spam → bị drop ────────────────────────────────
        {
            "id": "spam-1",
            "content": "Liên hệ t.me/cheap_tools để mua tool giá rẻ nhé!",
            "createdAt": 1605411111,            # 2020-11-15 09:31:51 UTC
        },
        # ── Duplicate id root-1 → bị drop ────────────────
        {
            "id": "root-1",
            "content": "Duplicate should be dropped silently.",
            "createdAt": 1605412222,            # 2020-11-15 09:50:22 UTC
        },
        # ── Bình thường, like_count = 0 không bị bỏ ─────
        {
            "commentId": "standalone-1",
            "message": "Đã lưu lại để đọc sau, cảm ơn bạn nhiều nhé.",
            "like_count": 0,
            "createdAt": 1605434400,            # 2020-11-15 16:00:00 UTC
        },
    ]

    print("=== normalize_batch output ===\n")
    results = normalize_batch(samples, debug=False)
    pprint(results)

    print(f"\nTotal normalized: {len(results)}")
    print("\nThread structure (DFS order):")
    for r in results:
        indent = "  " * r["extra"]["depth"]
        pid    = r["extra"]["parent_id"] or "ROOT"
        ts_s   = r["created_at"] / 1_000
        dt_str = datetime.fromtimestamp(ts_s, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"{indent}[{r['id']}]  parent={pid}  created_at={r['created_at']}  ({dt_str})")