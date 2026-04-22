#!/usr/bin/env python3
"""
Split Facebook data into:
- batch_data (< 1775754000)
- stream_data (>= 1775754000)

Dựa trên createdAt (UNIX timestamp - seconds, UTC+7)
Không modify dữ liệu
"""

import os
import json

BASE_DATA_PATH = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data/raw_data"
OUTPUT_PATH = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data"

STREAM_PATH = os.path.join(OUTPUT_PATH, "stream_data")
BATCH_PATH = os.path.join(OUTPUT_PATH, "batch_data")

# Mốc: 2026-04-10 00:00:00 UTC+7
SPLIT_TS = 1775754000


# =========================
# PROCESS FILE
# =========================
def process_json_file(src_file, dest_file):
    try:
        with open(src_file, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        created_at = data.get("createdAt")

        if not isinstance(created_at, (int, float)):
            return None  # failed

        target_base = BATCH_PATH if created_at < SPLIT_TS else STREAM_PATH
        category = "batch" if created_at < SPLIT_TS else "stream"

        final_dest = os.path.join(
            target_base,
            os.path.relpath(dest_file, start=OUTPUT_PATH)
        )

        os.makedirs(os.path.dirname(final_dest), exist_ok=True)

        # tránh overwrite
        if os.path.exists(final_dest):
            return None

        with open(final_dest, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        return category

    except Exception:
        return None


# =========================
# MAIN
# =========================
def main():
    stats = {
        "stream": 0,
        "batch": 0,
        "failed": 0
    }

    print("=" * 60)
    print("SPLIT BY createdAt (UTC+7)")
    print(f"Split timestamp: {SPLIT_TS}")
    print("=" * 60)

    for root, dirs, files in os.walk(BASE_DATA_PATH):
        for file in files:
            if not file.endswith(".json"):
                continue

            src_file = os.path.join(root, file)

            # giữ nguyên cấu trúc folder
            relative_path = os.path.relpath(src_file, BASE_DATA_PATH)
            dest_file = os.path.join(OUTPUT_PATH, relative_path)

            result = process_json_file(src_file, dest_file)

            if result == "stream":
                stats["stream"] += 1
            elif result == "batch":
                stats["batch"] += 1
            else:
                stats["failed"] += 1

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Stream files: {stats['stream']}")
    print(f"Batch files: {stats['batch']}")
    print(f"Failed: {stats['failed']}")
    print("=" * 60)


if __name__ == "__main__":
    main()