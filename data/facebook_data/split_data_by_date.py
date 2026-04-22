#!/usr/bin/env python3
"""
Split Facebook data into:
- stream_data (before SPLIT_DATE)
- batch_data (>= SPLIT_DATE)

AND add full timestamp (YYYY-MM-DDTHH:MM:SS) into each JSON file
"""

import os
import json
import re
from datetime import datetime

BASE_DATA_PATH = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data/raw_data"
OUTPUT_PATH = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data"

STREAM_PATH = os.path.join(OUTPUT_PATH, "stream_data")
BATCH_PATH = os.path.join(OUTPUT_PATH, "batch_data")

SPLIT_DATE = datetime(2026, 4, 10)


# =========================
# PARSE FULL DATETIME
# =========================
def parse_datetime(folder_name):
    try:
        parts = folder_name.split("_")

        parts = [p for p in parts if p]

        # lấy 6 phần đầu: YYYY MM DD HH MM SS
        parts = parts[:6]

        return datetime.strptime("_".join(parts), "%Y_%m_%d_%H_%M_%S")

    except Exception as e:
        print(f"Parse error: {folder_name} -> {e}")
        return None


# =========================
# PROCESS JSON
# =========================
def process_json_file(src_file, dest_file, dt):
    try:
        with open(src_file, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        # add timestamp
        data["timestamp"] = dt.strftime("%Y-%m-%dT%H:%M:%S")

        os.makedirs(os.path.dirname(dest_file), exist_ok=True)

        # tránh overwrite
        if os.path.exists(dest_file):
            print(f"Skip (exists): {dest_file}")
            return False

        with open(dest_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        return True

    except Exception as e:
        print(f"Error: {src_file} -> {e}")
        return False


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
    print("SPLIT + ADD TIMESTAMP")
    print(f"Split date: {SPLIT_DATE.strftime('%Y-%m-%d')}")
    print("=" * 60)

    for page in os.scandir(BASE_DATA_PATH):
        if not page.is_dir():
            continue

        for folder in os.scandir(page.path):
            if not folder.is_dir():
                continue

            dt = parse_datetime(folder.name)
            if dt is None:
                stats["failed"] += 1
                continue

            target_base = BATCH_PATH if dt < SPLIT_DATE else STREAM_PATH
            category = "batch" if dt < SPLIT_DATE else "stream"

            for file in os.listdir(folder.path):
                if not file.endswith(".json"):
                    continue

                src_file = os.path.join(folder.path, file)

                dest_file = os.path.join(
                    target_base,
                    page.name,
                    folder.name,
                    file
                )

                success = process_json_file(src_file, dest_file, dt)

                if success:
                    stats[category] += 1

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Stream files: {stats['stream']}")
    print(f"Batch files: {stats['batch']}")
    print(f"Failed: {stats['failed']}")
    print("=" * 60)


if __name__ == "__main__":
    main()