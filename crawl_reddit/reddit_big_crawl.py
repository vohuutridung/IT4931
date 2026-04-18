"""
Reddit Big Data Crawler
=======================
Crawls ~10000 Reddit posts with full metadata:
  - Post details (title, body, url, flair, awards, etc.)
  - Author/user info (name, karma, account age, etc.)
  - Engagement metrics (score, upvote_ratio, comment_count, crossposts)
  - Comments (nested, with author info and scores)
  - Subreddit metadata

Output: reddit_big_data.json  (line-delimited JSONL also available)

Usage:
    python reddit_big_crawl.py --help
    python reddit_big_crawl.py --all-topics --posts-per-topic 1000
    python reddit_big_crawl.py --topic sports --posts 1000
    python reddit_big_crawl.py --subreddit worldnews --posts 10000
    python reddit_big_crawl.py --subreddits worldnews,news,technology --posts 10000
    python reddit_big_crawl.py --keyword "artificial intelligence" --posts 10000
"""

import json
import time
import logging
import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────── Logging ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("crawler.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ──────────────────────────── Constants ──────────────────────────────────────
REDDIT_BASE   = "https://www.reddit.com"
REDDIT_OAUTH  = "https://oauth.reddit.com"
DEFAULT_UA    = "BigDataCrawler/2.0 (by u/your_reddit_username)"

# Rate limits: Reddit allows ~60 req/min for OAuth, ~1 req/2s for anonymous
RATE_LIMIT_DELAY   = 3.0   # seconds between requests (anonymous)
OAUTH_RATE_DELAY   = 0.6   # seconds between requests (OAuth)
MAX_COMMENTS_PER_POST = 50  # cap comments per post to keep output manageable
BATCH_SAVE_EVERY   = 100   # save progress every N posts

# ──────────────────────── Topic → Subreddit Mapping ──────────────────────────
TOPIC_SUBREDDITS: dict[str, list[str]] = {
    "sports": [
        "sports", "nba", "nfl", "soccer", "formula1",
        "tennis", "baseball", "hockey", "mma", "athletics",
    ],
    "trending": [
        "all", "popular", "worldnews", "todayilearned",
        "explainlikeimfive", "askreddit", "mildlyinteresting",
    ],
    "finance": [
        "finance", "investing", "personalfinance", "wallstreetbets",
        "stocks", "economy", "cryptocurrency", "financialindependence",
    ],
    "technology": [
        "technology", "programming", "MachineLearning", "artificial",
        "compsci", "cybersecurity", "datascience", "devops",
    ],
    "science": [
        "science", "askscience", "physics", "chemistry",
        "biology", "space", "EarthPorn", "geology",
    ],
    "health": [
        "health", "medicine", "mentalhealth", "fitness",
        "nutrition", "loseit", "running", "bodyweightfitness",
    ],
    "politics": [
        "politics", "worldnews", "news", "geopolitics",
        "PoliticalDiscussion", "europe", "uknews",
    ],
    "entertainment": [
        "movies", "television", "Music", "gaming",
        "books", "anime", "comicbooks", "hiphopheads",
    ],
    "business": [
        "business", "entrepreneur", "startups", "smallbusiness",
        "marketing", "ecommerce", "sales",
    ],
    "education": [
        "learnprogramming", "languagelearning", "studytips",
        "college", "math", "AskAcademia", "GradSchool",
    ],
}


# ─────────────────────────── HTTP Session ────────────────────────────────────
def build_session(user_agent: str = DEFAULT_UA) -> requests.Session:
    """Build a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update({
        "User-Agent": user_agent,
        "Accept":     "application/json",
    })
    return session


# ─────────────────────────── OAuth (optional) ────────────────────────────────
def authenticate_oauth(
    session: requests.Session,
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
) -> bool:
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {
        "grant_type": "password",
        "username":   username,
        "password":   password,
    }
    r = session.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
    )
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise ValueError("OAuth failed: no access_token in response.")
    session.headers.update({"Authorization": f"bearer {token}"})
    log.info("✅ OAuth authenticated successfully.")
    return True


# ───────────────────────── Data Extractors ───────────────────────────────────
def parse_timestamp(unix_ts) -> Optional[str]:
    if unix_ts is None:
        return None
    return datetime.fromtimestamp(float(unix_ts), tz=timezone.utc).isoformat()


def extract_post(post_data: dict) -> dict:
    d = post_data.get("data", post_data)
    return {
        "post_id":             d.get("id"),
        "fullname":            d.get("name"),
        "subreddit":           d.get("subreddit"),
        "subreddit_id":        d.get("subreddit_id"),
        "subreddit_type":      d.get("subreddit_type"),
        "subreddit_subscribers": d.get("subreddit_subscribers"),
        "title":               d.get("title"),
        "selftext":            d.get("selftext"),
        "url":                 d.get("url"),
        "permalink":           REDDIT_BASE + d.get("permalink", ""),
        "domain":              d.get("domain"),
        "post_type":           "self" if d.get("is_self") else "link",
        "thumbnail":           d.get("thumbnail"),
        "flair_text":          d.get("link_flair_text"),
        "flair_css_class":     d.get("link_flair_css_class"),
        "media":               d.get("media"),
        "is_video":            d.get("is_video", False),
        "is_gallery":          d.get("is_gallery", False),
        "spoiler":             d.get("spoiler", False),
        "nsfw":                d.get("over_18", False),
        "stickied":            d.get("stickied", False),
        "locked":              d.get("locked", False),
        "archived":            d.get("archived", False),
        "pinned":              d.get("pinned", False),
        "score":               d.get("score"),
        "upvotes":             d.get("ups"),
        "downvotes":           d.get("downs"),
        "upvote_ratio":        d.get("upvote_ratio"),
        "comment_count":       d.get("num_comments"),
        "crossposts_count":    d.get("num_crossposts"),
        "gilded":              d.get("gilded"),
        "total_awards_received": d.get("total_awards_received"),
        "all_awardings":       d.get("all_awardings"),
        "author":              d.get("author"),
        "author_fullname":     d.get("author_fullname"),
        "author_flair_text":   d.get("author_flair_text"),
        "is_original_content": d.get("is_original_content", False),
        "created_utc":         parse_timestamp(d.get("created_utc")),
        "created_utc_raw":     d.get("created_utc"),
        "crawled_at":          datetime.now(tz=timezone.utc).isoformat(),
    }


def extract_comment(comment_data: dict, depth: int = 0) -> Optional[dict]:
    kind = comment_data.get("kind")
    if kind == "more":
        return None
    d = comment_data.get("data", {})
    if not d.get("body") or d.get("body") in ("[deleted]", "[removed]"):
        return None

    replies_raw = d.get("replies", "")
    replies = []
    if isinstance(replies_raw, dict):
        children = replies_raw.get("data", {}).get("children", [])
        for child in children:
            parsed = extract_comment(child, depth + 1)
            if parsed:
                replies.append(parsed)

    return {
        "comment_id":      d.get("id"),
        "fullname":        d.get("name"),
        "post_id":         d.get("link_id", "").replace("t3_", ""),
        "parent_id":       d.get("parent_id"),
        "depth":           depth,
        "author":          d.get("author"),
        "author_fullname": d.get("author_fullname"),
        "body":            d.get("body"),
        "score":           d.get("score"),
        "upvotes":         d.get("ups"),
        "gilded":          d.get("gilded"),
        "total_awards":    d.get("total_awards_received"),
        "stickied":        d.get("stickied", False),
        "is_submitter":    d.get("is_submitter", False),
        "controversiality": d.get("controversiality"),
        "created_utc":     parse_timestamp(d.get("created_utc")),
        "created_utc_raw": d.get("created_utc"),
        "replies":         replies,
        "reply_count":     len(replies),
    }


def extract_user(session: requests.Session, username: str,
                 base_url: str, delay: float) -> Optional[dict]:
    if not username or username in ("[deleted]", "AutoModerator"):
        return None
    url = f"{base_url}/user/{username}/about.json"
    try:
        resp = session.get(url, timeout=15)
        time.sleep(delay)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        u = resp.json().get("data", {})
        return {
            "username":           u.get("name"),
            "user_id":            u.get("id"),
            "created_utc":        parse_timestamp(u.get("created_utc")),
            "created_utc_raw":    u.get("created_utc"),
            "comment_karma":      u.get("comment_karma"),
            "link_karma":         u.get("link_karma"),
            "total_karma":        u.get("total_karma"),
            "is_mod":             u.get("is_mod", False),
            "is_gold":            u.get("is_gold", False),
            "is_employee":        u.get("is_employee", False),
            "verified":           u.get("verified", False),
            "has_verified_email": u.get("has_verified_email"),
            "icon_img":           u.get("icon_img"),
        }
    except Exception as exc:
        log.debug("Could not fetch user %s: %s", username, exc)
        return None


# ──────────────────────────── Fetchers ───────────────────────────────────────
def fetch_post_comments(
    session: requests.Session,
    post_id: str,
    subreddit: str,
    base_url: str,
    delay: float,
    max_comments: int = MAX_COMMENTS_PER_POST,
) -> list:
    url = f"{base_url}/r/{subreddit}/comments/{post_id}.json?limit={max_comments}&depth=3"
    for attempt in range(5):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 429:
                wait = (attempt + 1) * 30
                log.warning("Rate limited on comments — waiting %ds (attempt %d/5)", wait, attempt + 1)
                time.sleep(wait)
                continue
            time.sleep(delay)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or len(payload) < 2:
                return []
            comment_listing = payload[1].get("data", {}).get("children", [])
            comments = []
            for child in comment_listing:
                parsed = extract_comment(child, depth=0)
                if parsed:
                    comments.append(parsed)
            return comments
        except Exception as exc:
            log.warning("Attempt %d - Failed comments post %s: %s", attempt + 1, post_id, exc)
            time.sleep(30 * (attempt + 1))

    log.error("Skipping comments for post %s after 5 attempts", post_id)
    return []


def fetch_posts_from_listing(
    session: requests.Session,
    url: str,
    params: dict,
    delay: float,
) -> tuple[list, Optional[str]]:
    for attempt in range(4):
        try:
            resp = session.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                wait = 30 * (2 ** attempt)
                log.warning(
                    "Rate limited on listing (attempt %d/4) — sleeping %ds…",
                    attempt + 1, wait,
                )
                time.sleep(wait)
                continue
            time.sleep(delay)
            resp.raise_for_status()
            data     = resp.json().get("data", {})
            children = data.get("children", [])
            after    = data.get("after")
            return children, after
        except Exception as exc:
            log.error("Failed to fetch listing %s (attempt %d/4): %s", url, attempt + 1, exc)
            if attempt < 3:
                time.sleep(15 * (attempt + 1))
    return [], None


# ──────────────────────────── Core Crawler ───────────────────────────────────
class RedditCrawler:
    def __init__(
        self,
        subreddits: list[str],
        target_posts: int = 10000,
        sort: str = "new",
        time_filter: str = "all",
        fetch_comments: bool = True,
        fetch_user_profiles: bool = True,
        max_comments_per_post: int = MAX_COMMENTS_PER_POST,
        min_comments: int = 15,
        output_file: str = "reddit_big_data.json",
        jsonl_file: str = "reddit_big_data.jsonl",
        client_id: str = "",
        client_secret: str = "",
        username: str = "",
        password: str = "",
        keyword: str = "",
        topic: str = "",
    ):
        self.subreddits      = subreddits
        self.target_posts    = target_posts
        self.sort            = sort
        self.time_filter     = time_filter
        self.fetch_comments  = fetch_comments
        self.fetch_users     = fetch_user_profiles
        self.max_comments    = max_comments_per_post
        self.min_comments    = min_comments
        self.output_file     = output_file
        self.jsonl_file      = jsonl_file
        self.keyword         = keyword
        self.topic           = topic

        self.session = build_session()
        self.use_oauth = False
        self.delay = RATE_LIMIT_DELAY

        if client_id and client_secret and username and password:
            try:
                authenticate_oauth(self.session, client_id, client_secret,
                                   username, password)
                self.use_oauth = True
                self.delay = OAUTH_RATE_DELAY
                self.base_url = REDDIT_OAUTH
            except Exception as e:
                log.warning("OAuth failed (%s) — using anonymous mode.", e)
                self.base_url = REDDIT_BASE
        else:
            self.base_url = REDDIT_BASE

        self.posts: list[dict]           = []
        self.user_cache: dict[str, dict] = {}
        self.seen_ids: set[str]          = set()

    # ── Sort-strategy rotation ─────────────────────────────────────────────────
    SORT_STRATEGIES: list[tuple[str, str]] = [
        ("new",           "all"),
        ("top",           "day"),
        ("top",           "week"),
        ("top",           "month"),
        ("top",           "year"),
        ("top",           "all"),
        ("hot",           "all"),
        ("controversial", "month"),
        ("controversial", "year"),
    ]

    # ── Progress helpers ──────────────────────────────────────────────────────
    def _save_json(self):
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(self.posts, f, ensure_ascii=False, indent=2, default=str)
        log.info("💾 Saved %d posts → %s", len(self.posts), self.output_file)

    def _save_jsonl(self, record: dict):
        with open(self.jsonl_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    # ── User profile helper ───────────────────────────────────────────────────
    def _get_user(self, username: str) -> Optional[dict]:
        if not self.fetch_users or not username:
            return None
        if username in self.user_cache:
            return self.user_cache[username]
        profile = extract_user(self.session, username, self.base_url, self.delay)
        self.user_cache[username] = profile
        return profile

    # ── Build listing URL / params ────────────────────────────────────────────
    def _listing_url_ex(self, subreddit: str, sort: str) -> str:
        if self.keyword:
            return f"{self.base_url}/search.json"
        return f"{self.base_url}/r/{subreddit}/{sort}.json"

    def _listing_params_ex(
        self,
        subreddit: str,
        sort: str,
        time_filter: str,
        after: Optional[str] = None,
    ) -> dict:
        params: dict = {"limit": 100, "raw_json": 1}
        if self.keyword:
            params["q"]           = self.keyword
            params["restrict_sr"] = "on" if subreddit != "all" else "off"
            params["sr_detail"]   = "on"
            params["sort"]        = sort
        else:
            if sort in ("top", "controversial"):
                params["t"] = time_filter
        if after:
            params["after"] = after
        return params

    # ── Main crawl loop ───────────────────────────────────────────────────────
    def crawl(self):
        log.info(
            "🚀 Starting crawl | target=%d posts | subreddits=%s | sort=%s | min_comments=%d",
            self.target_posts, self.subreddits, self.sort, self.min_comments,
        )

        # Clear JSONL file at start
        open(self.jsonl_file, "w").close()

        num_strategies = len(self.SORT_STRATEGIES)
        num_subreddits = len(self.subreddits)

        sub_index      = 0   # current subreddit index
        strategy_index = 0   # current strategy index within current subreddit
        after: Optional[str] = None

        pages_since_new  = 0
        MAX_STALE_PAGES  = 3

        def _advance_strategy():
            """Move to next sort strategy for the current subreddit. Returns True if subreddit is exhausted."""
            nonlocal strategy_index, after, pages_since_new
            strategy_index += 1
            after = None
            pages_since_new = 0
            return strategy_index >= num_strategies  # True → subreddit exhausted

        def _advance_subreddit():
            """Move to next subreddit, reset strategy. Returns True if all subreddits exhausted."""
            nonlocal sub_index, strategy_index, after, pages_since_new
            sub_index      += 1
            strategy_index  = 0
            after           = None
            pages_since_new = 0
            return sub_index >= num_subreddits  # True → all done

        while len(self.posts) < self.target_posts:
            if sub_index >= num_subreddits:
                log.error("All subreddits exhausted — stopping early at %d posts.", len(self.posts))
                break

            subreddit = self.subreddits[sub_index]
            sort, time_filter = self.SORT_STRATEGIES[strategy_index]

            url    = self._listing_url_ex(subreddit, sort)
            params = self._listing_params_ex(subreddit, sort, time_filter, after)

            log.info(
                "📄 Fetching listing | sub=r/%s | sort=%s/%s | collected=%d/%d | after=%s",
                subreddit, sort, time_filter,
                len(self.posts), self.target_posts,
                (after[:8] + "…") if after else "start",
            )

            children, after = fetch_posts_from_listing(
                self.session, url, params, self.delay
            )

            # ── Empty listing → rotate strategy ──────────────────────────────
            if not children:
                log.warning("Empty listing for r/%s [%s/%s] — rotating strategy.", subreddit, sort, time_filter)
                exhausted = _advance_strategy()
                if exhausted:
                    log.warning("All strategies exhausted for r/%s — rotating subreddit.", subreddit)
                    all_done = _advance_subreddit()
                    if all_done:
                        log.error("All subreddits exhausted — stopping early at %d posts.", len(self.posts))
                        break
                continue

            # ── Count truly new posts ─────────────────────────────────────────
            new_this_page = sum(
                1 for c in children
                if c.get("data", {}).get("id") not in self.seen_ids
            )

            if new_this_page == 0:
                pages_since_new += 1
                log.info("⚠️  No new posts on this page (stale=%d/%d)", pages_since_new, MAX_STALE_PAGES)
                if pages_since_new >= MAX_STALE_PAGES:
                    log.warning("Rotating sort strategy for r/%s after %d stale pages.", subreddit, pages_since_new)
                    exhausted = _advance_strategy()
                    if exhausted:
                        log.warning("All strategies exhausted for r/%s — moving to next subreddit.", subreddit)
                        all_done = _advance_subreddit()
                        if all_done:
                            log.error("All subreddits exhausted — stopping early at %d posts.", len(self.posts))
                            break
                # Don't reset after here — keep paginating with current after if stale count not reached
                continue
            else:
                pages_since_new = 0

            # ── Process posts ─────────────────────────────────────────────────
            for child in children:
                if len(self.posts) >= self.target_posts:
                    break

                post_raw = child.get("data", {})
                post_id  = post_raw.get("id")

                if not post_id or post_id in self.seen_ids:
                    continue
                self.seen_ids.add(post_id)

                # Minimum comment filter
                declared_comments = post_raw.get("num_comments", 0) or 0
                if declared_comments < self.min_comments:
                    log.debug(
                        "⏭  Skipping post %s (%d comments < min %d): %s",
                        post_id, declared_comments, self.min_comments,
                        post_raw.get("title", "")[:50],
                    )
                    continue

                post_record = extract_post(child)

                # Attach topic label if provided
                if self.topic:
                    post_record["topic"] = self.topic

                if self.fetch_comments:
                    post_record["comments"] = fetch_post_comments(
                        self.session,
                        post_id,
                        post_record["subreddit"],
                        self.base_url,
                        self.delay,
                        self.max_comments,
                    )
                    post_record["comments_fetched"] = len(post_record["comments"])
                else:
                    post_record["comments"] = []
                    post_record["comments_fetched"] = 0

                author = post_record.get("author")
                post_record["author_profile"] = self._get_user(author)

                self._save_jsonl(post_record)
                self.posts.append(post_record)

                if len(self.posts) % 10 == 0:
                    log.info(
                        "✅ %d/%d posts collected | latest: %s",
                        len(self.posts), self.target_posts,
                        post_record.get("title", "")[:60],
                    )

            # Batch save JSON every N posts
            if len(self.posts) % BATCH_SAVE_EVERY == 0 and self.posts:
                self._save_json()

            # No more pages for this (subreddit, strategy) → advance strategy
            if not after:
                log.info("No more pages for r/%s [%s/%s] — rotating strategy.", subreddit, sort, time_filter)
                exhausted = _advance_strategy()
                if exhausted:
                    log.warning("All strategies exhausted for r/%s — rotating subreddit.", subreddit)
                    all_done = _advance_subreddit()
                    if all_done:
                        log.error("All subreddits exhausted — stopping early at %d posts.", len(self.posts))
                        break

        # Final save
        self._save_json()
        self._print_summary()

    # ── Summary ───────────────────────────────────────────────────────────────
    def _print_summary(self):
        total_comments = sum(p.get("comments_fetched", 0) for p in self.posts)
        unique_authors = len({p.get("author") for p in self.posts if p.get("author")})
        avg_score = (
            sum(p.get("score", 0) or 0 for p in self.posts) / len(self.posts)
            if self.posts else 0
        )
        print("\n" + "=" * 60)
        print("  🎉  CRAWL COMPLETE")
        print("=" * 60)
        print(f"  Posts collected     : {len(self.posts):,}")
        print(f"  Comments fetched    : {total_comments:,}")
        print(f"  Unique authors      : {unique_authors:,}")
        print(f"  Avg post score      : {avg_score:.1f}")
        print(f"  User profiles cached: {len(self.user_cache):,}")
        print(f"  Output (JSON)       : {self.output_file}")
        print(f"  Output (JSONL)      : {self.jsonl_file}")
        print("=" * 60)


# ─────────────────────────────── CLI ─────────────────────────────────────────
def parse_args():
    available_topics = ", ".join(sorted(TOPIC_SUBREDDITS.keys()))

    parser = argparse.ArgumentParser(
        description="Reddit Big Data Crawler — collect posts with full metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Crawl ALL topics, 1000 posts each (~10000 tổng)
  python reddit_big_crawl.py --all-topics --posts-per-topic 1000

  # Crawl một topic cụ thể
  python reddit_big_crawl.py --topic sports --posts 1000

  # Crawl một subreddit
  python reddit_big_crawl.py --subreddit worldnews --posts 10000

  # Crawl nhiều subreddit
  python reddit_big_crawl.py --subreddits worldnews,news,technology --posts 10000

  # Tìm kiếm theo keyword
  python reddit_big_crawl.py --keyword "artificial intelligence" --posts 10000

  # Không lấy comments và user profiles (nhanh hơn)
  python reddit_big_crawl.py --all-topics --posts-per-topic 1000 --no-comments --no-users

  Available topics: {available_topics}
        """,
    )

    # ── Chế độ crawl ─────────────────────────────────────────────────────────
    parser.add_argument(
        "--all-topics", action="store_true",
        help="Crawl TẤT CẢ topics, mỗi topic --posts-per-topic bài. Output gộp vào reddit_all_topics.json",
    )
    parser.add_argument(
        "--posts-per-topic", type=int, default=1000,
        help="Số bài mỗi topic khi dùng --all-topics (default: 1000)",
    )
    parser.add_argument(
        "--topic", default="",
        choices=list(TOPIC_SUBREDDITS.keys()),
        metavar="TOPIC",
        help=f"Crawl theo topic. Available: {available_topics}",
    )
    parser.add_argument("--subreddit",  default="worldnews",
                        help="Subreddit đơn lẻ (default: worldnews)")
    parser.add_argument("--subreddits", default="",
                        help="Danh sách subreddits cách nhau bởi dấu phẩy")
    parser.add_argument("--keyword",    default="",
                        help="Tìm kiếm theo từ khóa")
    parser.add_argument("--posts",      type=int, default=10000,
                        help="Số bài mục tiêu (default: 10000)")
    parser.add_argument("--sort",       default="new",
                        choices=["new", "hot", "top", "controversial", "rising"],
                        help="Thứ tự sắp xếp (default: new)")
    parser.add_argument("--time",       default="all",
                        choices=["hour", "day", "week", "month", "year", "all"],
                        help="Bộ lọc thời gian cho top/controversial (default: all)")
    parser.add_argument("--max-comments", type=int, default=50,
                        help="Số comment tối đa mỗi bài (default: 50)")
    parser.add_argument("--min-comments", type=int, default=15,
                        help="Chỉ lấy bài có ít nhất N comments (default: 15)")
    parser.add_argument("--no-comments", action="store_true",
                        help="Bỏ qua fetch comments (nhanh hơn)")
    parser.add_argument("--no-users",    action="store_true",
                        help="Bỏ qua fetch thông tin tác giả")
    parser.add_argument("--output",     default="reddit_big_data.json",
                        help="Đường dẫn file JSON output")
    parser.add_argument("--jsonl",      default="reddit_big_data.jsonl",
                        help="Đường dẫn file JSONL output")

    # OAuth
    parser.add_argument("--client-id",     default=os.getenv("REDDIT_CLIENT_ID", ""),
                        help="Reddit OAuth client_id")
    parser.add_argument("--client-secret", default=os.getenv("REDDIT_CLIENT_SECRET", ""),
                        help="Reddit OAuth client_secret")
    parser.add_argument("--reddit-user",   default=os.getenv("REDDIT_USERNAME", ""),
                        help="Reddit username cho OAuth")
    parser.add_argument("--reddit-pass",   default=os.getenv("REDDIT_PASSWORD", ""),
                        help="Reddit password cho OAuth")

    return parser.parse_args()


# ────────────────────────────── Main ─────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()

    # ── Chế độ --all-topics ───────────────────────────────────────────────────
    if args.all_topics:
        all_posts    = []
        total_topics = len(TOPIC_SUBREDDITS)

        print("\n" + "=" * 60)
        print("  🌐  ALL-TOPICS MODE")
        print(f"  Topics      : {total_topics}")
        print(f"  Per topic   : {args.posts_per_topic} posts")
        print(f"  Total target: ~{total_topics * args.posts_per_topic:,} posts")
        print("=" * 60 + "\n")

        for i, (topic, subreddits) in enumerate(TOPIC_SUBREDDITS.items(), 1):
            log.info("=" * 55)
            log.info(
                "📌 [%d/%d] Topic: %-15s | Subreddits: %s",
                i, total_topics, topic,
                ", ".join(f"r/{s}" for s in subreddits),
            )
            log.info("=" * 55)

            topic_output = f"reddit_{topic}.json"
            topic_jsonl  = f"reddit_{topic}.jsonl"

            crawler = RedditCrawler(
                subreddits            = subreddits,
                target_posts          = args.posts_per_topic,
                sort                  = args.sort,
                time_filter           = args.time,
                fetch_comments        = not args.no_comments,
                fetch_user_profiles   = not args.no_users,
                max_comments_per_post = args.max_comments,
                min_comments          = args.min_comments,
                output_file           = topic_output,
                jsonl_file            = topic_jsonl,
                client_id             = args.client_id,
                client_secret         = args.client_secret,
                username              = args.reddit_user,
                password              = args.reddit_pass,
                topic                 = topic,
            )

            crawler.crawl()
            all_posts.extend(crawler.posts)

            log.info(
                "✅ Topic '%s' done: %d posts collected (total so far: %d)",
                topic, len(crawler.posts), len(all_posts),
            )

        # ── Gộp tất cả vào một file ───────────────────────────────────────────
        merged_json  = "reddit_all_topics.json"
        merged_jsonl = "reddit_all_topics.jsonl"

        with open(merged_json, "w", encoding="utf-8") as f:
            json.dump(all_posts, f, ensure_ascii=False, indent=2, default=str)

        with open(merged_jsonl, "w", encoding="utf-8") as f:
            for post in all_posts:
                f.write(json.dumps(post, ensure_ascii=False, default=str) + "\n")

        print("\n" + "=" * 60)
        print("  🎉  ALL-TOPICS CRAWL COMPLETE")
        print("=" * 60)
        print(f"  Topics crawled  : {total_topics}")
        print(f"  Total posts     : {len(all_posts):,}")
        print(f"  Merged JSON     : {merged_json}")
        print(f"  Merged JSONL    : {merged_jsonl}")
        print(f"  Per-topic files : reddit_<topic>.json / .jsonl")
        print("=" * 60)

    # ── Chế độ thông thường ───────────────────────────────────────────────────
    else:
        if args.topic:
            subreddits = TOPIC_SUBREDDITS[args.topic]
            log.info(
                "🏷  Topic '%s' → subreddits: %s",
                args.topic, ", ".join(f"r/{s}" for s in subreddits),
            )
            if args.output == "reddit_big_data.json":
                args.output = f"reddit_{args.topic}.json"
            if args.jsonl == "reddit_big_data.jsonl":
                args.jsonl = f"reddit_{args.topic}.jsonl"
        elif args.subreddits:
            subreddits = [s.strip() for s in args.subreddits.split(",") if s.strip()]
        else:
            subreddits = [args.subreddit]

        crawler = RedditCrawler(
            subreddits            = subreddits,
            target_posts          = args.posts,
            sort                  = args.sort,
            time_filter           = args.time,
            fetch_comments        = not args.no_comments,
            fetch_user_profiles   = not args.no_users,
            max_comments_per_post = args.max_comments,
            min_comments          = args.min_comments,
            output_file           = args.output,
            jsonl_file            = args.jsonl,
            client_id             = args.client_id,
            client_secret         = args.client_secret,
            username              = args.reddit_user,
            password              = args.reddit_pass,
            keyword               = args.keyword,
            topic                 = args.topic,
        )
        crawler.crawl()