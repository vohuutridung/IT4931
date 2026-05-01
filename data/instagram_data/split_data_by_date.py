#!/usr/bin/env python3
"""
Split Instagram JSONL data into:
- Before 2026-04-15 00:00:00 UTC+7
- From 2026-04-15 00:00:00 UTC+7 onwards

Không thay đổi dữ liệu gốc, chỉ split
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

INPUT_FILE = "/home/khang/Code/data-pipeline/IT4931/data/instagram_data/raw_data/posts.jsonl"
OUTPUT_DIR = "/home/khang/Code/data-pipeline/IT4931/data/instagram_data"

BEFORE_OUTPUT = f"{OUTPUT_DIR}/batch_data/posts.jsonl"
AFTER_OUTPUT = f"{OUTPUT_DIR}/stream_data/posts.jsonl"

UTC_PLUS_7 = timezone(timedelta(hours=7))
SPLIT_DT = datetime(2026, 4, 15, 0, 0, 0, tzinfo=UTC_PLUS_7)
SPLIT_LABEL = "2026-04-15 00:00:00 UTC+7"


# =========================
# PARSE DATETIME
# =========================
def parse_datetime(value):
    try:
        if isinstance(value, (int, float)):
            ts_sec = value / 1000 if value >= 1_000_000_000_000 else value
            return datetime.fromtimestamp(ts_sec, tz=timezone.utc)

        if isinstance(value, str):
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        return None
    except Exception:
        return None


# =========================
# MAIN
# =========================
def main():
    # tạo folder output đầy đủ
    Path(BEFORE_OUTPUT).parent.mkdir(parents=True, exist_ok=True)
    Path(AFTER_OUTPUT).parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "before": 0,
        "after": 0,
        "failed": 0,
        "total": 0
    }

    print("=" * 60)
    print("SPLIT INSTAGRAM DATA")
    print(f"Split time: {SPLIT_LABEL}")
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

                # hỗ trợ nhiều field timestamp
                timestamp_str = record.get("timestamp") or record.get("taken_at")
                if not timestamp_str:
                    stats["failed"] += 1
                    continue

                post_dt = parse_datetime(timestamp_str)
                if not post_dt:
                    stats["failed"] += 1
                    continue

                # giữ nguyên dữ liệu
                if post_dt < SPLIT_DT:
                    f_before.write(line + "\n")
                    stats["before"] += 1
                else:
                    f_after.write(line + "\n")
                    stats["after"] += 1

                # log nhẹ
                if line_num % 5000 == 0:
                    print(f"Processed {line_num} lines...")

                # flush tránh mất data
                if line_num % 10000 == 0:
                    f_before.flush()
                    f_after.flush()

            except json.JSONDecodeError:
                stats["failed"] += 1

            except Exception:
                stats["failed"] += 1

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Before {SPLIT_LABEL}: {stats['before']}")
    print(f"From {SPLIT_LABEL}: {stats['after']}")
    print(f"Total: {stats['total']}")
    print(f"Failed: {stats['failed']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
