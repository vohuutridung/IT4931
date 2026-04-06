"""
Reddit Big Data Crawler
=======================
Crawls ~2000 Reddit posts with full metadata:
  - Post details (title, body, url, flair, awards, etc.)
  - Author/user info (name, karma, account age, etc.)
  - Engagement metrics (score, upvote_ratio, comment_count, crossposts)
  - Comments (nested, with author info and scores)
  - Subreddit metadata

Output: reddit_big_data.json  (line-delimited JSONL also available)

Usage:
    python reddit_big_crawl.py --help
    python reddit_big_crawl.py --subreddit worldnews --posts 2000
    python reddit_big_crawl.py --subreddits worldnews,news,technology --posts 2000
    python reddit_big_crawl.py --keyword "artificial intelligence" --posts 2000
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
RATE_LIMIT_DELAY   = 2.5   # seconds between requests (anonymous)
OAUTH_RATE_DELAY   = 0.6   # seconds between requests (OAuth)
MAX_COMMENTS_PER_POST = 50  # cap comments per post to keep output manageable
BATCH_SAVE_EVERY   = 100   # save progress every N posts

# ──────────────────────── Topic → Subreddit Mapping ──────────────────────────
# Add or extend topics freely. Each key is the value passed to --topic.
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
    """
    Authenticate with Reddit OAuth2 (password grant).
    Raises on failure, returns True on success.
    """
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
    """Convert Unix timestamp to ISO-8601 string."""
    if unix_ts is None:
        return None
    return datetime.fromtimestamp(float(unix_ts), tz=timezone.utc).isoformat()


def extract_post(post_data: dict) -> dict:
    """Extract all relevant fields from a Reddit post's 'data' dict."""
    d = post_data.get("data", post_data)
    return {
        # ── Identifiers ──────────────────────────────────────────────────────
        "post_id":             d.get("id"),
        "fullname":            d.get("name"),           # e.g. t3_abc123
        "subreddit":           d.get("subreddit"),
        "subreddit_id":        d.get("subreddit_id"),
        "subreddit_type":      d.get("subreddit_type"),
        "subreddit_subscribers": d.get("subreddit_subscribers"),

        # ── Content ───────────────────────────────────────────────────────────
        "title":               d.get("title"),
        "selftext":            d.get("selftext"),       # post body (text posts)
        "url":                 d.get("url"),            # link posts
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

        # ── Engagement / Reactions ────────────────────────────────────────────
        "score":               d.get("score"),          # upvotes - downvotes
        "upvotes":             d.get("ups"),
        "downvotes":           d.get("downs"),
        "upvote_ratio":        d.get("upvote_ratio"),   # e.g. 0.95 = 95%
        "comment_count":       d.get("num_comments"),
        "crossposts_count":    d.get("num_crossposts"),
        "gilded":              d.get("gilded"),         # gold awards
        "total_awards_received": d.get("total_awards_received"),
        "all_awardings":       d.get("all_awardings"),  # full award list

        # ── Author / User ─────────────────────────────────────────────────────
        "author":              d.get("author"),
        "author_fullname":     d.get("author_fullname"),
        "author_flair_text":   d.get("author_flair_text"),
        "is_original_content": d.get("is_original_content", False),

        # ── Timestamps ────────────────────────────────────────────────────────
        "created_utc":         parse_timestamp(d.get("created_utc")),
        "created_utc_raw":     d.get("created_utc"),

        # ── Meta ──────────────────────────────────────────────────────────────
        "crawled_at":          datetime.now(tz=timezone.utc).isoformat(),
    }


def extract_comment(comment_data: dict, depth: int = 0) -> Optional[dict]:
    """Recursively extract a comment and its replies."""
    kind = comment_data.get("kind")
    if kind == "more":
        return None   # skip "load more" placeholders
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
    """
    Fetch public profile data for a Reddit user.
    Returns None if the user is deleted / suspended / not found.
    """
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
    """Fetch top-level + nested comments for a single post."""
    url = f"{base_url}/r/{subreddit}/comments/{post_id}.json?limit={max_comments}&depth=3"
    for attempt in range(5):  # thử tối đa 5 lần
        try:
            resp = session.get(url, timeout=20)
            
            if resp.status_code == 429:
                wait = (attempt + 1) * 30  # chờ 30, 60, 90, 120, 150 giây
                log.warning("Rate limited on comments — chờ %ds (lần %d/5)", wait, attempt+1)
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
            log.warning("Lần %d - Failed comments post %s: %s", attempt+1, post_id, exc)
            time.sleep(30 * (attempt + 1))
    
    log.error("Bỏ qua comments post %s sau 5 lần thử", post_id)
    return []


def fetch_posts_from_listing(
    session: requests.Session,
    url: str,
    params: dict,
    delay: float,
) -> tuple[list, Optional[str]]:
    """
    Fetch one page of posts from a Reddit listing endpoint.
    Returns (list_of_raw_post_dicts, after_token).
    Retries up to 4 times with exponential back-off on 429.
    """
    for attempt in range(4):
        try:
            resp = session.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                wait = 30 * (2 ** attempt)   # 30 → 60 → 120 → 240 s
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
        target_posts: int = 2000,
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

        self.posts: list[dict]       = []
        self.user_cache: dict[str, dict] = {}
        self.seen_ids: set[str]      = set()

    # ── Progress helpers ──────────────────────────────────────────────────────
    def _save_json(self):
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(self.posts, f, ensure_ascii=False, indent=2, default=str)
        log.info("💾 Saved %d posts → %s", len(self.posts), self.output_file)

    def _save_jsonl(self, record: dict):
        """Append a single record to the JSONL file (streaming / big-data friendly)."""
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

    # ── Build listing URL ─────────────────────────────────────────────────────
    def _listing_url(self, subreddit: str) -> str:
        """Legacy helper kept for compatibility."""
        if self.keyword:
            return f"{self.base_url}/search.json"
        return f"{self.base_url}/r/{subreddit}/{self.sort}.json"

    def _listing_url_ex(self, subreddit: str, sort: str) -> str:
        """Extended URL builder that accepts an explicit sort."""
        if self.keyword:
            return f"{self.base_url}/search.json"
        return f"{self.base_url}/r/{subreddit}/{sort}.json"

    def _listing_params(self, subreddit: str, after: Optional[str] = None) -> dict:
        """Legacy helper kept for compatibility."""
        return self._listing_params_ex(subreddit, self.sort, self.time_filter, after)

    def _listing_params_ex(
        self,
        subreddit: str,
        sort: str,
        time_filter: str,
        after: Optional[str] = None,
    ) -> dict:
        """Build request params with explicit sort & time_filter."""
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

    # ── Sort-strategy rotation (overcomes Reddit's ~1000-post hard cap) ────────
    # Reddit limits each (subreddit, sort, time_filter) combination to ~1000
    # posts. By rotating through these strategies we can collect far more unique
    # posts from a single subreddit before we have to rotate to the next one.
    SORT_STRATEGIES: list[tuple[str, str]] = [
        ("new",          "all"),
        ("top",          "day"),
        ("top",          "week"),
        ("top",          "month"),
        ("top",          "year"),
        ("top",          "all"),
        ("hot",          "all"),
        ("controversial", "month"),
        ("controversial", "year"),
    ]

    # ── Main crawl loop ───────────────────────────────────────────────────────
    def crawl(self):
        log.info(
            "🚀 Starting crawl | target=%d posts | subreddits=%s | sort=%s | min_comments=%d",
            self.target_posts, self.subreddits, self.sort, self.min_comments,
        )

        # Clear JSONL file at start
        open(self.jsonl_file, "w").close()

        sub_index      = 0      # which subreddit we're crawling
        strategy_index = 0      # which sort-strategy we're on for this subreddit
        after: Optional[str] = None

        # Track stagnation: pages fetched without a single NEW (unseen) post
        pages_since_new = 0
        MAX_STALE_PAGES = 3     # rotate strategy after this many fruitless pages

        while len(self.posts) < self.target_posts:
            subreddit = self.subreddits[sub_index % len(self.subreddits)]

            # Pick the current sort strategy
            sort, time_filter = self.SORT_STRATEGIES[
                strategy_index % len(self.SORT_STRATEGIES)
            ]

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

            if not children:
                log.warning(
                    "Empty listing for r/%s [%s/%s] — rotating strategy.",
                    subreddit, sort, time_filter,
                )
                # Try next sort strategy for this subreddit
                strategy_index += 1
                after = None
                pages_since_new = 0
                # If we've exhausted all strategies for this subreddit, move on
                if strategy_index % len(self.SORT_STRATEGIES) == 0:
                    log.warning(
                        "All strategies exhausted for r/%s — rotating subreddit.",
                        subreddit,
                    )
                    sub_index   += 1
                    strategy_index = 0
                    # Hard stop if we've gone through every subreddit with no luck
                    if sub_index >= len(self.subreddits):
                        log.error("All subreddits exhausted — stopping early at %d posts.",
                                  len(self.posts))
                        break
                continue

            # ── Count how many truly new posts were in this page ──────────────
            new_this_page = sum(
                1 for c in children
                if c.get("data", {}).get("id") not in self.seen_ids
            )

            if new_this_page == 0:
                pages_since_new += 1
                log.info(
                    "⚠️  No new posts on this page (stale=%d/%d)",
                    pages_since_new, MAX_STALE_PAGES,
                )
                if pages_since_new >= MAX_STALE_PAGES:
                    log.warning(
                        "Rotating sort strategy for r/%s after %d stale pages.",
                        subreddit, pages_since_new,
                    )
                    strategy_index += 1
                    after = None
                    pages_since_new = 0
                    if strategy_index % len(self.SORT_STRATEGIES) == 0:
                        log.warning(
                            "All strategies exhausted for r/%s — moving to next subreddit.",
                            subreddit,
                        )
                        sub_index      += 1
                        strategy_index  = 0
                        if sub_index >= len(self.subreddits):
                            log.error("All subreddits exhausted — stopping early at %d posts.",
                                      len(self.posts))
                            break
                continue
            else:
                pages_since_new = 0

            for child in children:
                if len(self.posts) >= self.target_posts:
                    break

                post_raw  = child.get("data", {})
                post_id   = post_raw.get("id")

                if not post_id or post_id in self.seen_ids:
                    continue
                self.seen_ids.add(post_id)

                # ── Minimum comment filter ────────────────────────────────────
                declared_comments = post_raw.get("num_comments", 0) or 0
                if declared_comments < self.min_comments:
                    log.debug(
                        "⏭  Skipping post %s (%d comments < min %d): %s",
                        post_id, declared_comments, self.min_comments,
                        post_raw.get("title", "")[:50],
                    )
                    continue

                # ── Extract post ──────────────────────────────────────────────
                post_record = extract_post(child)

                # ── Fetch comments ────────────────────────────────────────────
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

                # ── Fetch author profile ──────────────────────────────────────
                author = post_record.get("author")
                post_record["author_profile"] = self._get_user(author)

                # ── Save to JSONL immediately (streaming) ─────────────────────
                self._save_jsonl(post_record)

                self.posts.append(post_record)

                # ── Progress log ──────────────────────────────────────────────
                if len(self.posts) % 10 == 0:
                    log.info(
                        "✅ %d/%d posts collected | latest: %s",
                        len(self.posts), self.target_posts,
                        post_record.get("title", "")[:60],
                    )

            # Batch save JSON every N posts
            if len(self.posts) % BATCH_SAVE_EVERY == 0 and self.posts:
                self._save_json()

            # No more pages for this subreddit → rotate
            if not after:
                sub_index += 1
                after = None

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
        print(f"  Posts collected    : {len(self.posts):,}")
        print(f"  Comments fetched   : {total_comments:,}")
        print(f"  Unique authors     : {unique_authors:,}")
        print(f"  Avg post score     : {avg_score:.1f}")
        print(f"  User profiles cached: {len(self.user_cache):,}")
        print(f"  Output (JSON)      : {self.output_file}")
        print(f"  Output (JSONL)     : {self.jsonl_file}")
        print("=" * 60)


# ─────────────────────────────── CLI ─────────────────────────────────────────
def parse_args():
    available_topics = ", ".join(sorted(TOPIC_SUBREDDITS.keys()))

    parser = argparse.ArgumentParser(
        description="Reddit Big Data Crawler — collect ~2000 posts with full metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Anonymous crawl — worldnews, 2000 posts, newest first
  python reddit_big_crawl.py --subreddit worldnews --posts 2000

  # Multiple subreddits
  python reddit_big_crawl.py --subreddits worldnews,news,technology --posts 2000

  # Crawl by TOPIC (auto-selects relevant subreddits)
  python reddit_big_crawl.py --topic sports --posts 2000
  python reddit_big_crawl.py --topic finance --posts 2000 --sort top --time week
  python reddit_big_crawl.py --topic trending --posts 500 --no-comments

  # Available topics: {available_topics}

  # Keyword search across all subreddits
  python reddit_big_crawl.py --keyword "climate change" --posts 2000

  # OAuth (higher rate limit) + top posts of the week
  python reddit_big_crawl.py --subreddit science --posts 2000 \\
      --sort top --time week \\
      --client-id YOUR_ID --client-secret YOUR_SECRET \\
      --reddit-user YOUR_USER --reddit-pass YOUR_PASS

  # Skip fetching user profiles and comments (fast, lightweight)
  python reddit_big_crawl.py --subreddit worldnews --posts 2000 \\
      --no-comments --no-users
        """,
    )

    # ── Source selection (mutually exclusive priority: --topic > --subreddits > --subreddit) ──
    parser.add_argument("--topic",      default="",
                        choices=list(TOPIC_SUBREDDITS.keys()),
                        metavar="TOPIC",
                        help=(
                            f"Crawl by topic — auto-selects subreddits. "
                            f"Available: {available_topics}"
                        ))
    parser.add_argument("--subreddit",  default="worldnews",
                        help="Single subreddit to crawl (default: worldnews)")
    parser.add_argument("--subreddits", default="",
                        help="Comma-separated list of subreddits (overrides --subreddit)")
    parser.add_argument("--keyword",    default="",
                        help="Search keyword (searches across Reddit, ignores --subreddit)")
    parser.add_argument("--posts",      type=int, default=2000,
                        help="Target number of posts (default: 2000)")
    parser.add_argument("--sort",       default="new",
                        choices=["new", "hot", "top", "controversial", "rising"],
                        help="Listing sort order (default: new)")
    parser.add_argument("--time",       default="all",
                        choices=["hour", "day", "week", "month", "year", "all"],
                        help="Time filter for top/controversial (default: all)")
    parser.add_argument("--max-comments", type=int, default=50,
                        help="Max comments to fetch per post (default: 50)")
    parser.add_argument("--min-comments", type=int, default=15,
                        help="Only accept posts with at least this many comments (default: 15)")
    parser.add_argument("--no-comments", action="store_true",
                        help="Skip fetching comments (much faster)")
    parser.add_argument("--no-users",    action="store_true",
                        help="Skip fetching author profiles")
    parser.add_argument("--output",     default="reddit_big_data.json",
                        help="Output JSON file path")
    parser.add_argument("--jsonl",      default="reddit_big_data.jsonl",
                        help="Output JSONL file path (streaming, big-data friendly)")

    # OAuth (optional — higher rate limits)
    parser.add_argument("--client-id",     default=os.getenv("REDDIT_CLIENT_ID", ""),
                        help="Reddit OAuth client_id (or set REDDIT_CLIENT_ID env var)")
    parser.add_argument("--client-secret", default=os.getenv("REDDIT_CLIENT_SECRET", ""),
                        help="Reddit OAuth client_secret")
    parser.add_argument("--reddit-user",   default=os.getenv("REDDIT_USERNAME", ""),
                        help="Reddit username for OAuth")
    parser.add_argument("--reddit-pass",   default=os.getenv("REDDIT_PASSWORD", ""),
                        help="Reddit password for OAuth")

    return parser.parse_args()


# ────────────────────────────── Main ─────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()

    # ── Resolve subreddit list (priority: --topic > --subreddits > --subreddit) ──
    if args.topic:
        subreddits = TOPIC_SUBREDDITS[args.topic]
        log.info(
            "🏷  Topic '%s' → subreddits: %s",
            args.topic, ", ".join(f"r/{s}" for s in subreddits),
        )
        # Auto-name output files after the topic if the user didn't override them
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
    )

    crawler.crawl()
