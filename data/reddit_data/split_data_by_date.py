#!/usr/bin/env python3
"""
Split Reddit JSONL data into:
- Before April 10, 2026
- From April 10, 2026 onwards
"""

import json
from datetime import datetime
from pathlib import Path

INPUT_FILE = "/home/khang/Code/data-pipeline/IT4931/data/reddit_data/raw_data/posts.jsonl"
OUTPUT_DIR = "/home/khang/Code/data-pipeline/IT4931/data/reddit_data"

BEFORE_OUTPUT = f"{OUTPUT_DIR}/batch_data/posts.jsonl"
AFTER_OUTPUT = f"{OUTPUT_DIR}/stream_data/posts.jsonl"

SPLIT_DATE = datetime(2026, 4, 10).date()


# =========================
# PARSE DATE
# =========================
def parse_date(created_value):
    try:
        if isinstance(created_value, (int, float)):
            return datetime.utcfromtimestamp(created_value).date()

        if isinstance(created_value, str):
            return datetime.fromisoformat(
                created_value.replace("Z", "+00:00")
            ).date()

        return None

    except Exception:
        return None


# =========================
# MAIN
# =========================
def main():
    # tạo folder output
    Path(BEFORE_OUTPUT).parent.mkdir(parents=True, exist_ok=True)
    Path(AFTER_OUTPUT).parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "before": 0,
        "after": 0,
        "failed": 0,
        "total": 0
    }

    print("=" * 60)
    print("SPLIT REDDIT DATA (NO MODIFY)")
    print(f"Split date: {SPLIT_DATE}")
    print("=" * 60)

    with open(BEFORE_OUTPUT, 'w', encoding='utf-8') as f_before, \
         open(AFTER_OUTPUT, 'w', encoding='utf-8') as f_after, \
         open(INPUT_FILE, 'r', encoding='utf-8-sig') as f_input:

        for line_num, line in enumerate(f_input, 1):
            try:
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)
                stats["total"] += 1

                created = record.get("created_utc")
                post_date = parse_date(created)

                if not post_date:
                    stats["failed"] += 1
                    continue

                if post_date < SPLIT_DATE:
                    f_before.write(line + "\n")
                    stats["before"] += 1
                else:
                    f_after.write(line + "\n")
                    stats["after"] += 1

                # log progress
                if line_num % 5000 == 0:
                    print(f"Processed {line_num} lines...")

                if line_num % 10000 == 0:
                    f_before.flush()
                    f_after.flush()

            except json.JSONDecodeError:
                stats["failed"] += 1

            except Exception:
                stats["failed"] += 1

    # =========================
    # SUMMARY
    # =========================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print(f"Before 2026-04-10: {stats['before']}")
    print(f"After 2026-04-10: {stats['after']}")
    print(f"Total processed: {stats['total']}")
    print(f"Failed: {stats['failed']}")
    print("=" * 60)


if __name__ == "__main__":
    main()