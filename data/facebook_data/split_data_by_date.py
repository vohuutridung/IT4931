#!/usr/bin/env python3
"""
Script to split Facebook data into two parts:
- Before April 10, 2026
- After April 10, 2026
"""

import os
import shutil
import glob
import re
from datetime import datetime

BASE_DATA_PATH = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data/raw_data"
OUTPUT_PATH = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data"

BEFORE_DATE_OUTPUT = os.path.join(OUTPUT_PATH, "data_before_2026_04_10")
AFTER_DATE_OUTPUT = os.path.join(OUTPUT_PATH, "data_after_2026_04_10")

SPLIT_DATE = datetime(2026, 4, 10)


# =========================
# PARSE TIMESTAMP (ROBUST)
# =========================
def parse_timestamp(timestamp_str):
    """
    Extract YYYY_MM_DD safely from folder name
    """
    try:
        match = re.search(r"(\d{4}_\d{2}_\d{2})", timestamp_str)
        if not match:
            return None

        date_part = match.group(1)
        return datetime.strptime(date_part, "%Y_%m_%d")

    except Exception as e:
        print(f"  ⚠️ Failed to parse: {timestamp_str} - {e}")
        return None


def get_timestamp_from_folder(folder_path):
    return parse_timestamp(os.path.basename(folder_path))


# =========================
# COPY LOGIC (SAFE)
# =========================
def copy_folder_with_posts(src_folder, dest_base_folder, page_name):
    """
    Copy largest JSON file as post.json
    """

    json_files = glob.glob(os.path.join(src_folder, "*.json"))

    if not json_files:
        print(f"    ⚠️ No JSON file in {src_folder}")
        return False

    # chọn file lớn nhất (không phụ thuộc cleanup trước)
    largest_file = max(json_files, key=os.path.getsize)

    timestamp_str = os.path.basename(src_folder)
    dest_page_folder = os.path.join(dest_base_folder, page_name)
    dest_timestamp_folder = os.path.join(dest_page_folder, timestamp_str)

    os.makedirs(dest_timestamp_folder, exist_ok=True)

    dest_post_file = os.path.join(dest_timestamp_folder, "post.json")

    # tránh overwrite nếu đã tồn tại
    if os.path.exists(dest_post_file):
        print(f"    ⚠️ Skipped (already exists)")
        return False

    try:
        shutil.copy2(largest_file, dest_post_file)
        return True
    except Exception as e:
        print(f"    ❌ Copy failed: {e}")
        return False


# =========================
# MAIN
# =========================
def main():
    os.makedirs(BEFORE_DATE_OUTPUT, exist_ok=True)
    os.makedirs(AFTER_DATE_OUTPUT, exist_ok=True)

    stats = {
        "before": {"folders": 0, "files": 0},
        "after": {"folders": 0, "files": 0},
        "failed": 0
    }

    print("=" * 70)
    print("SPLITTING DATA BY DATE")
    print(f"Split date: {SPLIT_DATE.strftime('%Y-%m-%d')}")
    print("=" * 70)

    # scan pages
    for page_entry in os.scandir(BASE_DATA_PATH):
        if not page_entry.is_dir():
            continue

        page_name = page_entry.name
        page_path = page_entry.path

        print(f"\n📄 Processing page: {page_name}")

        for ts_entry in os.scandir(page_path):
            if not ts_entry.is_dir():
                continue

            timestamp_folder = ts_entry.name
            timestamp_folder_path = ts_entry.path

            dt = get_timestamp_from_folder(timestamp_folder_path)

            if dt is None:
                stats["failed"] += 1
                continue

            # FIX LOGIC: strictly before vs after
            if dt < SPLIT_DATE:
                output_folder = BEFORE_DATE_OUTPUT
                category = "BEFORE"
            else:
                output_folder = AFTER_DATE_OUTPUT
                category = "AFTER"

            success = copy_folder_with_posts(
                timestamp_folder_path,
                output_folder,
                page_name
            )

            if success:
                stats[category.lower()]["folders"] += 1
                stats[category.lower()]["files"] += 1
                status = "✓"
            else:
                status = "✗"

            print(f"  [{status}] {timestamp_folder} ({dt.strftime('%Y-%m-%d')}) → {category}")

    # summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print(f"✓ Before 2026-04-10:")
    print(f"  - Folders: {stats['before']['folders']}")
    print(f"  - Files: {stats['before']['files']}")
    print(f"  - Output: {BEFORE_DATE_OUTPUT}")

    print(f"\n✓ After 2026-04-10:")
    print(f"  - Folders: {stats['after']['folders']}")
    print(f"  - Files: {stats['after']['files']}")
    print(f"  - Output: {AFTER_DATE_OUTPUT}")

    print(f"\n✗ Failed to parse: {stats['failed']}")
    print("=" * 70)


if __name__ == "__main__":
    main()