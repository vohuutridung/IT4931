#!/usr/bin/env python3
"""
Script to split Facebook data into two parts:
- Before April 10, 2026
- After April 10, 2026
"""
import os
import shutil
from datetime import datetime
from pathlib import Path

BASE_DATA_PATH = "/home/khang/Code/data-pipeline/IT4931/crawl_facebook/data"
OUTPUT_PATH = "/home/khang/Code/data-pipeline/IT4931/crawl_facebook"

# Create output directories
BEFORE_DATE_OUTPUT = os.path.join(OUTPUT_PATH, "data_before_2026_04_10")
AFTER_DATE_OUTPUT = os.path.join(OUTPUT_PATH, "data_after_2026_04_10")

# Split date: April 10, 2026 (only date matters, time is ignored)
SPLIT_DATE = datetime(2026, 4, 10)

def parse_timestamp(timestamp_str):
    """
    Parse date from folder name format: YYYY_MM_DD__HH_MM_SS or YYYY_MM_DD__HH_MM_SS_[ID]
    Returns datetime object (date only) or None if parsing fails
    """
    try:
        # Remove trailing characters if any
        timestamp_str = timestamp_str.strip()
        
        # Extract the part before first underscore (or use first 10 chars for YYYY_MM_DD)
        # Format can be: 2026_04_10__23_22_41 or 2026_04_10__23_22_41_2234373447419759
        # We only care about the date part: YYYY_MM_DD
        parts = timestamp_str.split('__')
        if len(parts) >= 1:
            date_part = parts[0]  # This is YYYY_MM_DD
        else:
            return None
        
        # Parse only the date part (YYYY_MM_DD)
        dt = datetime.strptime(date_part, "%Y_%m_%d")
        return dt
    except (ValueError, IndexError) as e:
        print(f"  ⚠️  Failed to parse: {timestamp_str} - {e}")
        return None

def get_timestamp_from_folder(folder_path):
    """Extract timestamp from folder name"""
    folder_name = os.path.basename(folder_path)
    return parse_timestamp(folder_name)

def copy_folder_with_posts(src_folder, dest_base_folder, page_name):
    """
    Copy folder structure and post.json to destination
    """
    timestamp_str = os.path.basename(src_folder)
    dest_page_folder = os.path.join(dest_base_folder, page_name)
    dest_timestamp_folder = os.path.join(dest_page_folder, timestamp_str)
    
    # Create destination directory
    os.makedirs(dest_timestamp_folder, exist_ok=True)
    
    # Copy post.json if it exists
    src_post_file = os.path.join(src_folder, "post.json")
    if os.path.exists(src_post_file):
        dest_post_file = os.path.join(dest_timestamp_folder, "post.json")
        shutil.copy2(src_post_file, dest_post_file)
        return True
    return False

def main():
    # Create output base directories
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
    
    # Scan all pages (folders in BASE_DATA_PATH)
    page_folders = [d for d in os.listdir(BASE_DATA_PATH) 
                    if os.path.isdir(os.path.join(BASE_DATA_PATH, d))]
    
    for page_name in sorted(page_folders):
        page_path = os.path.join(BASE_DATA_PATH, page_name)
        print(f"\n📄 Processing page: {page_name}")
        
        # Get all timestamp folders for this page
        timestamp_folders = [d for d in os.listdir(page_path)
                            if os.path.isdir(os.path.join(page_path, d))]
        
        for timestamp_folder in sorted(timestamp_folders):
            timestamp_folder_path = os.path.join(page_path, timestamp_folder)
            
            # Parse timestamp
            dt = get_timestamp_from_folder(timestamp_folder_path)
            if dt is None:
                stats["failed"] += 1
                continue
            
            # Determine which output folder to use
            if dt <= SPLIT_DATE:
                output_folder = BEFORE_DATE_OUTPUT
                category = "BEFORE"
                stats["before"]["folders"] += 1
            else:
                output_folder = AFTER_DATE_OUTPUT
                category = "AFTER"
                stats["after"]["folders"] += 1
            
            # Copy the data
            if copy_folder_with_posts(timestamp_folder_path, output_folder, page_name):
                if category == "BEFORE":
                    stats["before"]["files"] += 1
                else:
                    stats["after"]["files"] += 1
                status = "✓"
            else:
                status = "✗"
            
            print(f"  [{status}] {timestamp_folder} ({dt.strftime('%Y-%m-%d')}) → {category}")
    
    # Print summary
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
