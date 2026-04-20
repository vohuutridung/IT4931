#!/usr/bin/env python3
"""
Split Instagram JSONL data into:
- Before April 10, 2026 (strictly before)
- From April 10, 2026 onwards
"""

import json
from datetime import datetime
from pathlib import Path

INPUT_FILE = "/home/khang/Code/data-pipeline/IT4931/data/instagram_data/raw_data/posts.jsonl"
OUTPUT_DIR = "/home/khang/Code/data-pipeline/IT4931/data/instagram_data"

BEFORE_OUTPUT = f"{OUTPUT_DIR}/posts_before_2026_04_10.jsonl"
AFTER_OUTPUT = f"{OUTPUT_DIR}/posts_after_2026_04_10.jsonl"

SPLIT_DATE = datetime(2026, 4, 10).date()


# =========================
# PARSE DATE (ROBUST)
# =========================
def parse_date(date_str):
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.date()
    except Exception as e:
        print(f"  ⚠️ Failed to parse: {date_str} - {e}")
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
    print("SPLITTING INSTAGRAM DATA BY DATE")
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

                timestamp_str = record.get("timestamp")
                if not timestamp_str:
                    stats["failed"] += 1
                    continue

                post_date = parse_date(timestamp_str)
                if not post_date:
                    stats["failed"] += 1
                    continue

                # FIX split logic
                if post_date < SPLIT_DATE:
                    f_before.write(line + "\n")
                    stats["before"] += 1
                else:
                    f_after.write(line + "\n")
                    stats["after"] += 1

                # giảm log
                if line_num % 5000 == 0:
                    print(f"Processed {line_num} lines...")

                # tránh mất data khi crash
                if line_num % 10000 == 0:
                    f_before.flush()
                    f_after.flush()

            except json.JSONDecodeError as e:
                print(f"  ✗ Line {line_num}: Invalid JSON - {e}")
                stats["failed"] += 1

            except Exception as e:
                print(f"  ✗ Line {line_num}: Unexpected error - {e}")
                stats["failed"] += 1

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print(f"✓ Before 2026-04-10: {stats['before']}")
    print(f"✓ After 2026-04-10: {stats['after']}")
    print(f"📊 Total: {stats['total']}")
    print(f"✗ Failed: {stats['failed']}")
    print("=" * 70)


if __name__ == "__main__":
    main()