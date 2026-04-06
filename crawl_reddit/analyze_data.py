"""
Reddit Data Analyzer
====================
Quick statistics & schema inspection on the crawled JSON / JSONL data.

Usage:
    python analyze_data.py reddit_big_data.json
    python analyze_data.py reddit_big_data.jsonl --jsonl
"""

import json
import sys
import argparse
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime


def load_json(path: str) -> list:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: str) -> list:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def analyze(records: list):
    n = len(records)
    print(f"\n{'='*60}")
    print(f"  📊  DATASET OVERVIEW  —  {n:,} posts")
    print(f"{'='*60}")

    # ── Basic counts ─────────────────────────────────────────────────────────
    total_comments = sum(r.get("comment_count", 0) or 0 for r in records)
    total_comments_fetched = sum(r.get("comments_fetched", 0) or 0 for r in records)
    total_score = sum(r.get("score", 0) or 0 for r in records)
    total_awards = sum(r.get("total_awards_received", 0) or 0 for r in records)
    total_crossposts = sum(r.get("crossposts_count", 0) or 0 for r in records)

    print(f"\n  Posts              : {n:,}")
    print(f"  Total comments     : {total_comments:,}  (declared by Reddit)")
    print(f"  Comments fetched   : {total_comments_fetched:,}  (actually crawled)")
    print(f"  Total score        : {total_score:,}")
    print(f"  Total awards       : {total_awards:,}")
    print(f"  Total crossposts   : {total_crossposts:,}")

    # ── Top subreddits ────────────────────────────────────────────────────────
    sub_counter = Counter(r.get("subreddit") for r in records)
    print(f"\n  📌 Top 10 Subreddits:")
    for sub, cnt in sub_counter.most_common(10):
        print(f"     r/{sub:<25}  {cnt:>5} posts")

    # ── Top authors ───────────────────────────────────────────────────────────
    author_counter = Counter(r.get("author") for r in records if r.get("author"))
    print(f"\n  👤 Top 10 Authors (by post count):")
    for author, cnt in author_counter.most_common(10):
        print(f"     u/{author:<25}  {cnt:>5} posts")

    # ── Score stats ───────────────────────────────────────────────────────────
    scores = [r.get("score", 0) or 0 for r in records]
    scores.sort()
    avg = sum(scores) / n if n else 0
    print(f"\n  📈 Score Statistics:")
    print(f"     Min   : {scores[0]:,}")
    print(f"     Max   : {scores[-1]:,}")
    print(f"     Avg   : {avg:,.1f}")
    print(f"     Median: {scores[n // 2]:,}")

    # ── Post type breakdown ───────────────────────────────────────────────────
    type_counter = Counter(r.get("post_type") for r in records)
    print(f"\n  🔖 Post Types:")
    for ptype, cnt in type_counter.items():
        print(f"     {ptype:<10}  {cnt:>5}  ({cnt/n*100:.1f}%)")

    # ── NSFW / Stickied / Locked ──────────────────────────────────────────────
    nsfw    = sum(1 for r in records if r.get("nsfw"))
    locked  = sum(1 for r in records if r.get("locked"))
    sticked = sum(1 for r in records if r.get("stickied"))
    print(f"\n  🔞 NSFW: {nsfw} | 🔒 Locked: {locked} | 📌 Stickied: {sticked}")

    # ── Timeline ──────────────────────────────────────────────────────────────
    dates = []
    for r in records:
        ts = r.get("created_utc")
        if ts:
            try:
                dates.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
            except Exception:
                pass
    if dates:
        oldest = min(dates)
        newest = max(dates)
        print(f"\n  📅 Date Range:")
        print(f"     Oldest: {oldest.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"     Newest: {newest.strftime('%Y-%m-%d %H:%M UTC')}")

    # ── Schema keys ──────────────────────────────────────────────────────────
    print(f"\n  🗂  Top-Level Schema Keys:")
    if records:
        keys = sorted(records[0].keys())
        for k in keys:
            sample = records[0].get(k)
            val_type = type(sample).__name__
            print(f"     {k:<30}  ({val_type})")

    print(f"\n{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Analyze crawled Reddit data")
    parser.add_argument("file", help="Path to JSON or JSONL file")
    parser.add_argument("--jsonl", action="store_true", help="Parse as JSONL")
    args = parser.parse_args()

    path = args.file
    if not Path(path).exists():
        print(f"❌ File not found: {path}")
        sys.exit(1)

    print(f"📂 Loading: {path}")
    records = load_jsonl(path) if args.jsonl else load_json(path)
    analyze(records)


if __name__ == "__main__":
    main()
