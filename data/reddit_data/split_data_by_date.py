#!/usr/bin/env python3
"""
Split Reddit JSONL data into:
- Before April 10, 2026 (strictly before)
- From April 10, 2026 onwards
"""

import json
from datetime import datetime
from pathlib import Path

INPUT_FILE = "/home/khang/Code/data-pipeline/IT4931/data/reddit_data/raw_data/posts.jsonl"
OUTPUT_DIR = "/home/khang/Code/data-pipeline/IT4931/data/reddit_data"

BEFORE_OUTPUT = f"{OUTPUT_DIR}/posts_before_2026_04_10.jsonl"
AFTER_OUTPUT = f"{OUTPUT_DIR}/posts_after_2026_04_10.jsonl"

SPLIT_DATE = datetime(2026, 4, 10).date()


# =========================
# PARSE DATE (ROBUST)
# =========================
def parse_date(created_value):
    """
    Handle both:
    - UNIX timestamp (int/float)
    - ISO string
    """
    try:
        if isinstance(created_value, (int, float)):
            return datetime.utcfromtimestamp(created_value).date()

        if isinstance(created_value, str):
            # handle Z timezone
            dt = datetime.fromisoformat(created_value.replace("Z", "+00:00"))
            return dt.date()

        return None

    except Exception as e:
        print(f"  Failed to parse date: {created_value} - {e}")
        return None


# =========================
# MAIN
# =========================
def main():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    stats = {
        "before": 0,
        "after": 0,
        "failed": 0,
        "total": 0
    }

    print("=" * 70)
    print("SPLITTING REDDIT DATA BY DATE")
    print(f"Split date: {SPLIT_DATE}")
    print(f"Input file: {INPUT_FILE}")
    print("=" * 70)

    with open(BEFORE_OUTPUT, 'w') as f_before, \
         open(AFTER_OUTPUT, 'w') as f_after, \
         open(INPUT_FILE, 'r') as f_input:

        for line_num, line in enumerate(f_input, 1):
            try:
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)
                stats["total"] += 1

                if 'created_utc' not in record:
                    stats["failed"] += 1
                    continue

                post_date = parse_date(record['created_utc'])

                if not post_date:
                    stats["failed"] += 1
                    continue

                # FIX: split logic chuẩn
                if post_date < SPLIT_DATE:
                    f_before.write(line + "\n")
                    stats["before"] += 1
                else:
                    f_after.write(line + "\n")
                    stats["after"] += 1

                # giảm IO log
                if line_num % 5000 == 0:
                    print(f"Processed {line_num} lines...")

                # flush định kỳ để tránh mất data
                if line_num % 10000 == 0:
                    f_before.flush()
                    f_after.flush()

            except json.JSONDecodeError as e:
                print(f"   Line {line_num}: Invalid JSON - {e}")
                stats["failed"] += 1

            except Exception as e:
                print(f"   Line {line_num}: Unexpected error - {e}")
                stats["failed"] += 1

    # =========================
    # SUMMARY
    # =========================
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print(f" Before 2026-04-10:")
    print(f"  - Records: {stats['before']}")
    print(f"  - Output: {BEFORE_OUTPUT}")

    print(f"\n From 2026-04-10 onwards:")
    print(f"  - Records: {stats['after']}")
    print(f"  - Output: {AFTER_OUTPUT}")

    print(f"\n Total processed: {stats['total']}")
    print(f" Failed: {stats['failed']}")
    print("=" * 70)


if __name__ == "__main__":
    main()