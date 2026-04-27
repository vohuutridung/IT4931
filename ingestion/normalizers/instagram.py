"""
Instagram normalizer — production-ready version
────────────────────────────────────────────────
Input fields thực tế:
  id, caption, hashtags (list), url,
  timestamp (ISO str), timestamp_raw (unix-s, optional — đồng bộ FB/Reddit),
  likesCount, commentsCount,
  ownerUsername, ownerId, ownerFullName,
  latestComments (list → mỗi item có replies[]),
  videoViewCount, videoDuration, productType, inputUrl

Aligned với FB normalizer:
  - _parse_timestamp  : parse ISO-8601 / unix-s / unix-ms → UTC ms (giữ giây)
  - _coalesce_int     : thay _safe_int, xử lý đúng value = 0
  - _is_low_value     : filter comment rác
  - _walk_tree        : flatten replies → NLP-ready flat list
  - extra             : dict thuần (không json.dumps)

Changes vs previous version:
  - [FIX] _parse_timestamp      : bỏ dt.replace(second=0) — giữ nguyên giây.
  - [FIX] timestamp fallback    : xóa _parse_folder_timestamp hoàn toàn;
                                  data luôn có "timestamp" field đầy đủ.
  - [FIX] missing timestamp     : trả về None thay vì fallback về _now_ms().
  - [FIX] _extract_comments     : bỏ [:MAX_COMMENTS] trước _walk_tree;
                                  cap áp dụng trên output, không phải input.
  - [FIX] parent_id             : normalize None / "" / "0" → None nhất quán.
  - [FIX] _SPAM_RE              : thu hẹp pattern platform để tránh false positive
                                  (yêu cầu handle/link theo sau keyword).
  - [FIX] spam check            : thực hiện trên raw text TRƯỚC khi _clean_text
                                  xóa URL, tránh drop vì lý do sai.
  - [FIX] is_verified           : chuyển lên author dict (cross-platform field).
"""

import re
import html
import math
import time
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# =========================================================
# Constants
# =========================================================

MAX_COMMENTS      = 20
MAX_CONTENT_LEN   = 2_000
MIN_CONTENT_CHARS = 4


# =========================================================
# Spam / noise patterns  (IG-specific)
# =========================================================

_SPAM_RE = re.compile(
    # Platform + handle/link ngay sau (tránh false positive khi chỉ nhắc tên)
    r"\b(telegram|whatsapp|snapchat|wickr)\b[.:@\s]*[\w./]+"
    r"|\+\d[\d\s\-\(\)]{7,}"               # phone number
    r"|\b(mdma|lsd|dmt|shrooms|coke)\b"    # drug names
    r"|https?://(?!(?:www\.)?instagram\.com)",  # external links (non-IG)
    re.IGNORECASE,
)

# Comment chỉ toàn mention @user → không có nội dung thực
_MENTION_ONLY_RE = re.compile(r"^(@\w+\s*)+$")


# =========================================================
# Extra schema  (cross-platform, giống FB)
# =========================================================

@dataclass
class CommentExtra:
    """
    Canonical metadata cho mọi normalized comment.
    Platform-specific fields nằm trong platform_meta — NLP pipelines bỏ qua.

    Attributes:
        parent_id     : ID comment cha; None = top-level.
        depth         : 0 = top-level, 1 = reply, 2 = reply-to-reply.
        reply_count   : Số reply trực tiếp (từ raw source).
        is_truncated  : True khi content bị cắt tại MAX_CONTENT_LEN.
        platform_meta : Fields đặc thù của Instagram.
    """
    parent_id:     Optional[str]
    depth:         int
    reply_count:   int
    is_truncated:  bool
    platform_meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =========================================================
# Internal helpers  (aligned với FB normalizer)
# =========================================================

def _now_ms() -> int:
    """Current UTC time in milliseconds."""
    return int(time.time() * 1_000)



def _coalesce_int(*values: Any) -> int:
    """
    Trả về giá trị non-None đầu tiên dưới dạng int.
    Xử lý đúng value = 0 — khác với `or` chaining.
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
    Parse timestamp bất kỳ → UTC ms, giữ nguyên độ chính xác đến giây.
    Chấp nhận: ISO-8601 string (có/không Z) | unix-seconds | unix-ms.
    Numeric path không qua datetime (tránh roundtrip / timezone ngầm).
    Trả về None nếu parse thất bại.
    """
    if not value:
        return None
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            ts = float(value)
            # unix-ms (>= 1e12) giữ nguyên; unix-s nhân 1000 — không qua datetime
            return int(ts if ts >= 1e12 else ts * 1_000)

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
    Cắt tại word boundary.
    Trả về (text, is_truncated).
    """
    if len(text) <= max_len:
        return text, False
    return text[:max_len].rsplit(" ", 1)[0], True


def _is_low_value(text: str) -> bool:
    """True nếu text quá ngắn hoặc chỉ toàn emoji/ký hiệu."""
    if not text:
        return True
    clean = re.sub(r"\W+", "", text, flags=re.UNICODE)
    return len(clean) < MIN_CONTENT_CHARS


def _extract_hashtags(text: str) -> List[str]:
    return re.findall(r"#([\wÀ-ỹ]+)", text or "")


def _build_comment_key(cmt: Dict[str, Any]) -> str:
    """Stable dedup key từ id."""
    key = str(cmt.get("id") or "")
    if not key:
        logger.debug("IG comment missing id: %r", cmt)
    return key


def _normalize_parent_id(raw: Any) -> Optional[str]:
    """
    Chuẩn hóa parent_id: None / "" / "0" / 0 → None.
    Tránh trường hợp "0" truthy string lọt qua `or None`.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    return None if s in ("", "0") else s


# =========================================================
# Tree → flat list  (xử lý replies lồng nhau)
# =========================================================

def _walk_tree(
    comments: List[Dict[str, Any]],
    *,
    parent_id: Optional[str] = None,
    depth: int = 0,
) -> List[Dict[str, Any]]:
    """
    Flatten nested IG comment tree (DFS order).
    Replies nằm ở key ``"replies"`` — bổ sung _parent_id / _depth vào mỗi node.
    Không mutate raw input — dùng shallow copy.

    Args:
        comments:  Sibling comment dicts tại level hiện tại.
        parent_id: Key của node cha (None cho root).
        depth:     Độ sâu hiện tại (0 = top-level).

    Returns:
        Flat list tất cả nodes với _parent_id / _depth đã inject.
    """
    flat: List[Dict[str, Any]] = []

    for cmt in comments:
        if not isinstance(cmt, dict):
            continue

        node = {**cmt, "_parent_id": parent_id, "_depth": depth}
        flat.append(node)

        replies = cmt.get("replies") or []   # đọc từ original, không mutate
        if replies:
            node_key = _build_comment_key(node)
            flat.extend(
                _walk_tree(replies, parent_id=node_key or None, depth=depth + 1)
            )

    return flat


# =========================================================
# Single comment normalizer
# =========================================================

def _normalize_comment(
    cmt: Dict[str, Any],
    post_id: str,
    seen: Set[str],
) -> Optional[Dict[str, Any]]:
    """
    Normalize một IG comment đã được inject _parent_id / _depth bởi _walk_tree.

    Spam check được thực hiện trên raw text TRƯỚC khi _clean_text xóa URL,
    để tránh drop comment vì lý do sai (URL bị strip → nội dung còn lại OK).
    Trả về None nếu comment bị drop (spam / low-value / thiếu timestamp).
    """
    key = _build_comment_key(cmt)
    if not key or key in seen:
        return None
    seen.add(key)

    raw_text = cmt.get("text") or ""

    # ── Spam check trên raw text (trước khi strip URL) ────
    if _SPAM_RE.search(raw_text):
        logger.info("Spam IG comment dropped: id=%s user=%s", key, cmt.get("ownerUsername"))
        return None

    # ── Content ───────────────────────────────────────────
    content = _clean_text(raw_text)

    if _is_low_value(content):
        logger.debug("Low-value IG comment dropped: %s", key)
        return None

    if _MENTION_ONLY_RE.fullmatch(content):
        logger.debug("Mention-only IG comment dropped: %s", key)
        return None

    # ── Timestamp ─────────────────────────────────────────
    # Ưu tiên timestamp_raw (unix-s, đồng bộ FB/Reddit) → fallback về ISO string
    created_at = _parse_timestamp(cmt.get("timestamp_raw") or cmt.get("timestamp"))
    if created_at is None:
        logger.debug("Missing timestamp for comment %s; dropping", key)
        return None

    # ── Author ────────────────────────────────────────────
    owner = cmt.get("owner") or {}
    author: Dict[str, Any] = {
        "id":          str(owner.get("id") or "unknown"),
        "name":        str(cmt.get("ownerUsername") or owner.get("username") or "unknown"),
        "is_verified": bool(owner.get("is_verified", False)),   # cross-platform field
    }

    # ── Truncation ────────────────────────────────────────
    truncated_content, is_truncated = _safe_truncate(content)

    # ── Extra ─────────────────────────────────────────────
    # [FIX] dùng _normalize_parent_id thay vì `or None` để xử lý đúng "0"
    parent_id = _normalize_parent_id(cmt.get("_parent_id"))
    depth     = int(cmt.get("_depth") or 0)

    extra = CommentExtra(
        parent_id     = parent_id,
        depth         = depth,
        reply_count   = _coalesce_int(cmt.get("repliesCount")),
        is_truncated  = is_truncated,
        platform_meta = {},   # không còn is_verified ở đây (đã lên author)
    )

    return {
        "comment_id": key,
        "post_id":    f"instagram_{post_id}",
        "parent_id":  parent_id,
        "author_id":  author["id"],
        "author":     author["name"],
        "text":       truncated_content,
        "likes":      _coalesce_int(cmt.get("likesCount")),
        "created_at": created_at,
        "depth":      depth,
        "extra":      json.dumps(extra.to_dict(), ensure_ascii=False),
    }


# =========================================================
# Comments batch  (flatten tree → normalize)
# =========================================================

def _extract_comments(
    post_id: str,
    comments: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """
    Flatten ``latestComments`` (bao gồm replies lồng nhau),
    sau đó normalize từng node.

    [FIX] _walk_tree nhận toàn bộ list — không cắt input.
    Cap MAX_COMMENTS áp dụng trên OUTPUT để không bỏ sót replies
    của các top-level comment gần cuối.
    """
    if not isinstance(comments, list):
        return []

    # [FIX] flatten hết trước, rồi mới cap
    flat_tree = _walk_tree(comments)

    seen:    Set[str]             = set()
    results: List[Dict[str, Any]] = []

    for cmt in flat_tree:
        if len(results) >= MAX_COMMENTS:
            logger.info("MAX_COMMENTS (%d) reached; stopping.", MAX_COMMENTS)
            break
        normalized = _normalize_comment(cmt, post_id, seen)
        if normalized:
            results.append(normalized)

    return results


# =========================================================
# Main normalize  (public API)
# =========================================================

def normalize(raw: dict) -> dict:
    """
    Normalize một raw Instagram post dict thành canonical schema.

    Args:
        raw: Raw post object từ scraper.

    Returns:
        Normalized post dict. ``event_time`` là None nếu ``timestamp`` bị thiếu
        hoặc không parse được — caller tự quyết định xử lý.

    Raises:
        ValueError: Nếu ``id`` bị thiếu.
    """
    post_id = raw.get("id")
    if not post_id:
        raise ValueError("Missing id")

    # ── Content ───────────────────────────────────────────
    caption = _clean_text(raw.get("caption") or "")
    truncated_caption, caption_truncated = _safe_truncate(caption)

    # ── Hashtags ──────────────────────────────────────────
    hashtags = raw.get("hashtags")
    if not isinstance(hashtags, list):
        hashtags = _extract_hashtags(caption)
    # Dedup giữ thứ tự, lowercase cho NLP — case gốc đã có trong caption nếu cần
    hashtags = list(dict.fromkeys(h.lower() for h in hashtags))

    # ── Timestamp ─────────────────────────────────────────
    # Ưu tiên timestamp_raw (unix-s, đồng bộ FB/Reddit) → fallback về ISO string
    event_time = _parse_timestamp(raw.get("timestamp_raw") or raw.get("timestamp"))

    # ── Comments ──────────────────────────────────────────
    comments = _extract_comments(
        post_id,
        raw.get("latestComments"),
    )

    # ── Engagement score ──────────────────────────────────
    likes = _coalesce_int(raw.get("likesCount"))
    cmts  = _coalesce_int(raw.get("commentsCount"))
    views = _coalesce_int(raw.get("videoViewCount"))
    # log1p để tránh video có views cực cao lấn át hoàn toàn likes/comments
    score = likes + 2 * cmts + int(math.log1p(views) * 10)

    return {
        "schema_version": 1,
        "post_id": str(post_id),
        "source": "instagram",
        "event_time": event_time,
        "ingest_time": _now_ms(),

        "author_id": str(raw.get("ownerId") or "unknown"),
        "author_name": raw.get("ownerUsername") or raw.get("ownerFullName") or "unknown",

        "content": truncated_caption,
        "url": raw.get("url"),
        "hashtags": hashtags,

        "engagement": {
            "likes": likes,
            "comments": cmts,
            "shares": 0,
            "score": score,
            "video_views": views,
            "comments_normalized_count": len(comments),
        },

        "comments": comments,

        "extra": {
            "short_code": raw.get("shortCode"),
            "is_video": raw.get("type") == "Video",
            "video_duration": raw.get("videoDuration"),
            "product_type": raw.get("productType"),
            "caption_truncated": caption_truncated,
        },
    }


# =========================================================
# Smoke-test  (python instagram_normalizer.py)
# =========================================================

if __name__ == "__main__":
    from pprint import pprint

    RAW = {
        "id": "2420190677667987681",
        "type": "Video",
        "shortCode": "CGWPpk_HMDh",
        "caption": ".\n.\n#psychedelic #digitalcreator #trippy #visuals",
        "hashtags": ["psychedelic", "digitalcreator", "trippy", "visuals"],
        "url": "https://www.instagram.com/p/CGWPpk_HMDh/",
        "commentsCount": 28,
        "likesCount": 1745,
        "videoViewCount": 14968,
        "videoDuration": 30.133,
        "productType": "feed",
        "ownerUsername": "austinblakeart",
        "ownerFullName": "Austin Blake",
        "ownerId": "8437765849",
        "timestamp": "2020-10-15T02:34:40.000Z",
        "latestComments": [
            # ── Hợp lệ, có reply lồng ──────────────────────
            {
                "id": "cmt-001",
                "text": "Amazing work! Very inspiring visuals.",
                "ownerUsername": "eyeignite",
                "timestamp": "2020-10-25T07:03:18.000Z",
                "repliesCount": 1,
                "likesCount": 3,
                "owner": {"id": "4175816303", "is_verified": True, "username": "eyeignite"},
                "replies": [
                    {
                        "id": "reply-001a",
                        "text": "Totally agree, this loop is mesmerizing.",
                        "ownerUsername": "raw.visuals4",
                        "timestamp": "2020-10-25T09:00:00.000Z",
                        "repliesCount": 0,
                        "likesCount": 1,
                        "owner": {"id": "55277817093", "is_verified": False, "username": "raw.visuals4"},
                        "replies": [],
                    }
                ],
            },
            # ── Mention-only → dropped ──────────────────────
            {
                "id": "cmt-002",
                "text": "@have_ur_cone_already",
                "ownerUsername": "laughing_luck",
                "timestamp": "2021-03-16T14:28:08.000Z",
                "repliesCount": 0,
                "likesCount": 0,
                "owner": {"id": "1192459509", "is_verified": False},
                "replies": [],
            },
            # ── Emoji-only → dropped ────────────────────────
            {
                "id": "cmt-003",
                "text": "🔥🔥🔥",
                "ownerUsername": "af_311811",
                "timestamp": "2020-10-25T05:45:01.000Z",
                "repliesCount": 0,
                "likesCount": 1,
                "owner": {"id": "8030345864", "is_verified": False},
                "replies": [],
            },
            # ── Spam (drug + contact) → dropped ────────────
            {
                "id": "cmt-004",
                "text": "Telegram...drugstore420q wickr...weedplug420q +1(951)289-5685",
                "ownerUsername": "henry420q",
                "timestamp": "2020-10-26T05:35:49.000Z",
                "repliesCount": 0,
                "likesCount": 0,
                "owner": {"id": "43781899032", "is_verified": False},
                "replies": [],
            },
            # ── Bình thường nhắc tên "telegram" → KHÔNG drop
            {
                "id": "cmt-006",
                "text": "I found this on telegram, sharing here because it's incredible art.",
                "ownerUsername": "normaluser",
                "timestamp": "2021-01-01T10:00:00.000Z",
                "repliesCount": 0,
                "likesCount": 2,
                "owner": {"id": "99999999", "is_verified": False},
                "replies": [],
            },
            # ── like_count = 0 phải giữ ────────────────────
            {
                "id": "cmt-005",
                "text": "I feel like my house is going sideways watching this.",
                "ownerUsername": "forthekidsskeie",
                "timestamp": "2025-12-31T17:57:34.000Z",
                "repliesCount": 0,
                "likesCount": 0,
                "owner": {"id": "79538366648", "is_verified": False},
                "replies": [],
            },
        ],
    }

    print("=== normalize() output ===\n")
    result = normalize(RAW)

    post_preview = {k: v for k, v in result.items() if k != "comments"}
    pprint(post_preview)

    print(f"\nComments normalized: {len(result['comments'])}")
    print("\nThread structure:")
    for c in result["comments"]:
        extra  = json.loads(c["extra"])
        indent = "  " * c["depth"]
        pid    = c["parent_id"] or "ROOT"
        print(f"{indent}[{c['comment_id']}]  parent={pid}  text={c['text'][:45]!r}")
