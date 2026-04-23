"""
Reddit normalizer — production-ready version
─────────────────────────────────────────────
Input fields thực tế:
  post_id, title, selftext, url, permalink,
  author, author_fullname, author_profile,
  score, upvotes, upvote_ratio, comment_count, crossposts_count,
  is_video, subreddit, flair_text,
  created_utc (ISO str), created_utc_raw (unix epoch),
  comments (nested tree, replies key)

Aligned với FB / IG normalizer:
  - _parse_timestamp : truncate về phút (bỏ giây), trả về UTC ms int
  - _coalesce_int    : xử lý đúng value = 0
  - _safe_truncate   : cắt tại word boundary, trả về (text, is_truncated)
  - extra (comment)  : dict với parent_id / reply_count / is_truncated /
                       platform_meta — giống FB/IG
  - extra (post)     : plain dict — không json.dumps

Note: Reddit data luôn có created_utc_raw → không cần folder timestamp fallback.

Changes vs previous version:
  - [FIX] comment extra     : thêm parent_id, reply_count, is_truncated,
                              platform_meta — align cross-platform schema FB/IG.
  - [FIX] parent_id         : _normalize_parent_id() chỉ giữ nguyên nếu đã
                              có prefix hợp lệ (t[1-6]_); bare ID → fallback
                              thay vì giả định t1_ (tránh nhầm t3_/malformed).
  - [ADD] spam/noise filter : _is_spam_comment() lọc bot, spam link, drug.
                              Spam → drop toàn bộ nhánh (replies cũng không duyệt).
  - [FIX] traversal         : dfs() trả về bool → caller cắt nhánh sớm khi MAX đạt,
                              không traverse vô ích.
  - [FIX] content_truncated : flag nhất quán trong post extra lẫn comment extra.
"""

import re
import time
import html
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =========================================================
# Constants
# =========================================================

MAX_COMMENTS    = 5_000
MAX_DEPTH       = 20
MAX_CONTENT_LEN = 2_000
MAX_COMMENT_LEN = 1_000


# =========================================================
# Spam / noise patterns  (Reddit-specific)
# =========================================================

_SPAM_RE = re.compile(
    r"https?://(?!(?:www\.)?reddit\.com|v\.redd\.it|i\.redd\.it)"  # external link
    r"|\b(t\.me|telegram\.me)/\S+"                                  # Telegram
    r"|\+\d[\d\s\-\(\)]{7,}"                                        # phone number
    r"|\b(mdma|lsd|dmt|shrooms|onlyfans\.com)\b",                   # drugs / OF
    re.IGNORECASE,
)

# Known Reddit bots — extend as needed
_BOT_RE = re.compile(
    r"\b(automoderator|remindmebot|sneakpeekbot|imgurlinkcheckerbot"
    r"|repostsleuthbot|anti[_\-]?meme[_\-]?bot)\b",
    re.IGNORECASE,
)


def _is_spam_comment(body: str, author: str) -> bool:
    """True nếu comment là spam, bot, hoặc noise."""
    if _BOT_RE.search(author or ""):
        return True
    if _SPAM_RE.search(body or ""):
        return True
    return False


# =========================================================
# Internal helpers  (aligned với FB / IG normalizer)
# =========================================================

def _now_ms() -> int:
    return int(time.time() * 1_000)


def _coalesce_int(*values: Any) -> int:
    """Trả về giá trị non-None đầu tiên dưới dạng int. Xử lý đúng value = 0."""
    for v in values:
        if v is not None:
            try:
                return int(v)
            except (ValueError, TypeError):
                continue
    return 0


def _parse_timestamp(obj: dict) -> Optional[int]:
    """
    Parse created_utc_raw (unix epoch) hoặc created_utc (ISO string) → UTC ms,
    Trả về None nếu cả hai đều thiếu/lỗi.
    """
    raw_ts = obj.get("created_utc_raw")
    if raw_ts is not None:
        try:
            return int(float(raw_ts) * 1_000)
        except (ValueError, TypeError):
            logger.warning("Invalid created_utc_raw: %r", raw_ts)

    iso = obj.get("created_utc")
    if iso:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return int(dt.astimezone(timezone.utc).timestamp() * 1_000)
        except (ValueError, AttributeError):
            logger.warning("Invalid created_utc ISO string: %r", iso)

    return None


def _clean_text(text: str) -> str:
    """Unescape HTML, xóa markdown media, giữ link label, collapse whitespace."""
    text = html.unescape(text or "")
    text = re.sub(r"!\[.*?\]\(.*?\)", "", text)           # ảnh/gif markdown
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # hyperlink → giữ label
    text = re.sub(r"\*{1,2}(.+?)\*{1,2}", r"\1", text)    # bold / italic
    text = re.sub(r"~~(.+?)~~", r"\1", text)               # strikethrough
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _safe_truncate(text: str, max_len: int) -> Tuple[str, bool]:
    """Cắt tại word boundary. Trả về (text, is_truncated)."""
    if len(text) <= max_len:
        return text, False
    return text[:max_len].rsplit(" ", 1)[0], True


def _normalize_parent_id(pid: Any, fallback: str) -> str:
    """
    Chuẩn hóa parent_id về Reddit prefix format:
      - Đã có prefix hợp lệ (t1_/t2_/t3_/t4_/t5_/t6_) → giữ nguyên
      - Bare alphanumeric / sai format / None / ""       → fallback
        (không tự thêm t1_ vì không đủ context phân biệt t1_ vs t3_)
    """
    if not pid or not isinstance(pid, str):
        return fallback
    pid = pid.strip()
    if re.match(r"^t[1-6]_\w+$", pid):
        return pid                  # already valid
    # bare alphanumeric: không đủ context để biết t1_ hay t3_ → fallback
    return fallback


def _extract_hashtags(text: str) -> List[str]:
    return re.findall(r"#([\wÀ-ỹ]+)", text or "")


def _build_content(raw: dict) -> str:
    parts = [raw.get("title", ""), raw.get("selftext", "")]
    return " ".join(p for p in parts if p).strip()


# =========================================================
# Author profile
# =========================================================

def _extract_author_profile(profile: Optional[dict]) -> Optional[dict]:
    if not profile:
        return None
    return {
        "total_karma":         profile.get("total_karma"),
        "is_mod":              profile.get("is_mod", False),
        "verified":            profile.get("verified", False),
        "is_gold":             profile.get("is_gold", False),
        "account_age_utc_raw": profile.get("created_utc_raw"),
    }


# =========================================================
# Comment tree → flat list
# =========================================================

def _flatten_comments(
    comments: Optional[List[dict]],
    post_id: str,
) -> List[dict]:
    """
    DFS-flatten cây comment Reddit.

    Mỗi comment normalized gồm đủ schema cross-platform:
      comment_id, post_id, author_id, author, text, score, depth,
      created_at, extra { parent_id, reply_count, is_truncated,
                          platform_meta { controversiality,
                                         is_submitter, gilded } }

    Rules:
      - [deleted] / [removed]  : skip body, vẫn duyệt replies.
      - Media-only sau clean    : skip body, vẫn duyệt replies.
      - Spam / bot              : drop toàn bộ nhánh (replies cũng drop).
      - parent_id               : normalize qua _normalize_parent_id().
      - Early exit              : dfs() trả về True khi đạt MAX → caller
                                  dừng ngay, không traverse thêm.
    """
    if not comments:
        return []

    result:  List[dict] = []
    visited: set        = set()

    def dfs(comment: dict, fallback_parent: str, depth: int) -> bool:
        """Trả về True nếu đã đạt MAX → signal dừng toàn bộ."""
        if len(result) >= MAX_COMMENTS or depth > MAX_DEPTH:
            return True

        if not isinstance(comment, dict):
            return False

        cid = comment.get("comment_id")
        if not cid or (post_id, cid) in visited:
            return False
        visited.add((post_id, cid))

        author = str(comment.get("author") or "unknown")
        body   = (comment.get("body") or "").strip()

        is_deleted = body in ("[deleted]", "[removed]")

        # Spam → drop toàn bộ nhánh, không duyệt replies
        if not is_deleted and _is_spam_comment(body, author):
            logger.info("Spam/bot comment dropped: id=%s author=%s", cid, author)
            return False

        if body and not is_deleted:
            clean_body = _clean_text(body)

            if not clean_body:
                # Media-only sau strip → skip body, vẫn duyệt replies bên dưới
                logger.debug("Comment %s skipped: media-only after cleaning", cid)
            else:
                truncated_body, is_truncated = _safe_truncate(
                    clean_body, MAX_COMMENT_LEN
                )
                pid = _normalize_parent_id(
                    comment.get("parent_id"), fallback_parent
                )

                created_at = _parse_timestamp(comment)
                if created_at is None:
                    logger.warning("Missing timestamp for comment %s; dropping", cid)
                    return False

                result.append({
                    "comment_id": cid,
                    "post_id":    f"reddit_{post_id}",
                    "author_id":  str(comment.get("author_fullname") or "unknown"),
                    "author":     author,
                    "text":       truncated_body,
                    "score":      _coalesce_int(comment.get("score")),
                    "depth":      depth,
                    "created_at": created_at,

                    # [FIX] extra đủ schema cross-platform như FB/IG
                    "extra": {
                        "parent_id":    pid,
                        "reply_count":  _coalesce_int(comment.get("reply_count")),
                        "is_truncated": is_truncated,
                        "platform_meta": {
                            "controversiality": comment.get("controversiality", 0),
                            "is_submitter":     comment.get("is_submitter", False),
                            "gilded":           comment.get("gilded", 0),
                        },
                    },
                })

                if len(result) >= MAX_COMMENTS:
                    return True   # đạt MAX ngay sau append

        # Duyệt replies (kể cả khi body deleted / media-only)
        replies = comment.get("replies")
        if isinstance(replies, list):
            for reply in replies:
                if dfs(reply, f"t1_{cid}", depth + 1):
                    return True   # [FIX] cắt nhánh sớm

        return False

    for c in comments:
        if dfs(c, fallback_parent=f"t3_{post_id}", depth=0):
            logger.info("MAX_COMMENTS (%d) reached; stopping.", MAX_COMMENTS)
            break

    return result


# =========================================================
# Main normalize  (public API)
# =========================================================

def normalize(raw: dict) -> dict:
    """
    Normalize một raw Reddit post dict thành canonical schema.

    Timestamp lấy từ created_utc_raw (ưu tiên) hoặc created_utc trong data.
    Fallback về _now_ms() nếu cả hai đều thiếu/lỗi.

    Args:
        raw: Raw post object từ scraper.

    Returns:
        Normalized post dict.

    Raises:
        ValueError: Nếu post_id bị thiếu.
    """
    post_id = raw.get("post_id")
    if not post_id:
        raise ValueError("Missing post_id")

    # ── Content ───────────────────────────────────────────
    content = _clean_text(_build_content(raw))
    truncated_content, content_truncated = _safe_truncate(content, MAX_CONTENT_LEN)

    # ── Timestamp ─────────────────────────────────────────
    event_time = _parse_timestamp(raw)
    if event_time is None:
        raise ValueError(f"Missing or invalid timestamp for post {post_id}")

    # ── Comments ──────────────────────────────────────────
    comments = _flatten_comments(raw.get("comments"), post_id)

    # ── Engagement ────────────────────────────────────────
    likes  = _coalesce_int(raw.get("upvotes"), raw.get("score"))
    cmts   = _coalesce_int(raw.get("comment_count"))
    shares = _coalesce_int(raw.get("crossposts_count"))

    return {
        "schema_version": 1,
        "post_id": str(post_id),
        "source": "reddit",
        "event_time": event_time,
        "ingest_time": _now_ms(),

        "author_id": str(raw.get("author_fullname") or "unknown"),
        "author_name": str(raw.get("author") or "unknown"),

        "content": truncated_content,
        "url": raw.get("url") or raw.get("permalink"),
        "hashtags": [t.lower() for t in _extract_hashtags(truncated_content)],

        "engagement": {
            "likes": likes,
            "comments": cmts,
            "shares": shares,
            "score": likes,  # Reddit score is upvotes
            "video_views": 0,
            "comments_normalized_count": len(comments),
        },

        "comments": comments,

        "extra": {
            "subreddit": raw.get("subreddit"),
            "is_video": raw.get("is_video", False),
            "content_truncated": content_truncated,
            "flair_text": raw.get("flair_text"),
            "upvote_ratio": raw.get("upvote_ratio") or 0.0,
            "author_profile": _extract_author_profile(raw.get("author_profile")),
        },
    }


# =========================================================
# Smoke-test  (python reddit_normalizer.py)
# =========================================================

if __name__ == "__main__":
    from pprint import pprint

    RAW = {
        "post_id": "1snmolx",
        "subreddit": "Amazing",
        "title": "Would you rescue the bear?",
        "selftext": "",
        "url": "https://v.redd.it/jjwlnukqknvg1",
        "permalink": "https://www.reddit.com/r/Amazing/comments/1snmolx/",
        "is_video": True,
        "score": 8057,
        "upvotes": 8057,
        "upvote_ratio": 0.98,
        "comment_count": 356,
        "crossposts_count": 5,
        "author": "sco-go",
        "author_fullname": "t2_bmixs08o",
        "created_utc": "2026-04-17T01:24:39+00:00",
        "created_utc_raw": 1776389079,
        "flair_text": "Awesome !!",
        "author_profile": {
            "total_karma": 5101403,
            "is_mod": True,
            "is_gold": False,
            "verified": True,
            "created_utc_raw": 1618793053,
        },
        "comments": [
            # ── Hợp lệ, nested replies ──────────────────────
            {
                "comment_id": "ogmsk6k",
                "parent_id": "t3_1snmolx",
                "depth": 0,
                "author": "A-lazy-koala",
                "author_fullname": "t2_735m2qs5",
                "body": "Sometimes humans can be alright.",
                "score": 434,
                "reply_count": 1,
                "controversiality": 0,
                "is_submitter": False,
                "gilded": 0,
                "created_utc_raw": 1776389492,
                "replies": [
                    {
                        "comment_id": "ogmtrwa",
                        "parent_id": "t1_ogmsk6k",
                        "depth": 1,
                        "author": "MotherofPirates",
                        "author_fullname": "t2_1v9o0rbx05",
                        "body": "Was our plastic",
                        "score": 150,
                        "reply_count": 1,
                        "controversiality": 0,
                        "is_submitter": False,
                        "gilded": 0,
                        "created_utc_raw": 1776389930,
                        "replies": [
                            {
                                "comment_id": "ogmuryi",
                                "parent_id": "t1_ogmtrwa",
                                "depth": 2,
                                "author": "BeatsbyChrisBrown",
                                "author_fullname": "t2_75997",
                                "body": "Man made.\n\nMan kind.",
                                "score": 63,
                                "reply_count": 0,
                                "controversiality": 0,
                                "is_submitter": False,
                                "gilded": 0,
                                "created_utc_raw": 1776390294,
                                "replies": [],
                            }
                        ],
                    }
                ],
            },
            # ── Bare parent_id → normalize thành t1_ ────────
            {
                "comment_id": "ogmtvxo",
                "parent_id": "ogmsk6k",   # bare ID
                "depth": 1,
                "author": "Longjumping-Ask-1743",
                "author_fullname": "t2_1ibnnqqlfs",
                "body": "Good stuff right there!",
                "score": 11,
                "reply_count": 0,
                "controversiality": 0,
                "is_submitter": False,
                "gilded": 0,
                "created_utc_raw": 1776389972,
                "replies": [],
            },
            # ── [deleted] → skip body, duyệt replies ────────
            {
                "comment_id": "ogmtcw0",
                "parent_id": "t3_1snmolx",
                "depth": 0,
                "author": "[deleted]",
                "author_fullname": None,
                "body": "[deleted]",
                "score": 97,
                "reply_count": 1,
                "controversiality": 0,
                "is_submitter": False,
                "gilded": 0,
                "created_utc_raw": 1776389779,
                "replies": [
                    {
                        "comment_id": "ognb72l",
                        "parent_id": "t1_ogmtcw0",
                        "depth": 1,
                        "author": "srschwenzjr",
                        "author_fullname": "t2_2d8spliy",
                        "body": "Oh bother — reply of deleted parent should survive.",
                        "score": 31,
                        "reply_count": 0,
                        "controversiality": 0,
                        "is_submitter": False,
                        "gilded": 0,
                        "created_utc_raw": 1776396368,
                        "replies": [],
                    }
                ],
            },
            # ── Media-only → skip body, duyệt replies ───────
            {
                "comment_id": "ogmxjdc",
                "parent_id": "t3_1snmolx",
                "depth": 0,
                "author": "goldiekapur",
                "author_fullname": "t2_nsogc",
                "body": "![gif](giphy|cYxLgjZI5ezI2lrItX|downsized)",
                "score": 8,
                "reply_count": 0,
                "controversiality": 0,
                "is_submitter": False,
                "gilded": 0,
                "created_utc_raw": 1776391295,
                "replies": [],
            },
            # ── Bot → drop toàn bộ nhánh ────────────────────
            {
                "comment_id": "bot-001",
                "parent_id": "t3_1snmolx",
                "depth": 0,
                "author": "AutoModerator",
                "author_fullname": "t2_automod",
                "body": "This is a reminder to follow the rules of the subreddit.",
                "score": 1,
                "reply_count": 0,
                "controversiality": 0,
                "is_submitter": False,
                "gilded": 0,
                "created_utc_raw": 1776389500,
                "replies": [],
            },
            # ── Spam link → drop toàn bộ nhánh ─────────────
            {
                "comment_id": "spam-001",
                "parent_id": "t3_1snmolx",
                "depth": 0,
                "author": "spammer99",
                "author_fullname": "t2_spammer",
                "body": "Check out https://some-external-drugstore.com for deals",
                "score": 0,
                "reply_count": 0,
                "controversiality": 0,
                "is_submitter": False,
                "gilded": 0,
                "created_utc_raw": 1776389600,
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
        extra  = c["extra"]
        indent = "  " * c["depth"]
        print(
            f"{indent}[{c['comment_id']}]"
            f"  parent={extra['parent_id']}"
            f"  replies={extra['reply_count']}"
            f"  truncated={extra['is_truncated']}"
            f"  text={c['text'][:45]!r}"
        )