#!/usr/bin/env python3
"""
Script to split Instagram data into two parts:
- Before April 10, 2026
- After April 10, 2026
"""
import json
from datetime import datetime
from pathlib import Path

INPUT_FILE = "/home/khang/Code/data-pipeline/IT4931/crawl_instagram/data/posts.jsonl"
OUTPUT_DIR = "/home/khang/Code/data-pipeline/IT4931/crawl_instagram"

BEFORE_OUTPUT = f"{OUTPUT_DIR}/posts_before_2026_04_10.jsonl"
AFTER_OUTPUT = f"{OUTPUT_DIR}/posts_after_2026_04_10.jsonl"

# Split date: April 10, 2026
SPLIT_DATE = datetime(2026, 4, 10)

def parse_date_from_iso(date_str):
    """
    Parse ISO 8601 datetime string: 2026-04-08T14:57:17.000Z
    Returns datetime object with date only or None if parsing fails
    """
    try:
        # Remove 'Z' suffix and parse
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00').split('+')[0])
        return dt.date()
    except (ValueError, AttributeError, IndexError) as e:
        print(f"  ⚠️  Failed to parse: {date_str} - {e}")
        return None

def main():
    print("=" * 70)
    print("SPLITTING INSTAGRAM DATA BY DATE")
    print(f"Split date: {SPLIT_DATE.strftime('%Y-%m-%d')}")
    print(f"Input file: {INPUT_FILE}")
    print("=" * 70)
    
    stats = {
        "before": 0,
        "after": 0,
        "failed": 0,
        "total": 0
    }
    
    try:
        # Load Instagram data
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
        
        # Handle both single record and array of records
        records = data if isinstance(data, list) else [data]
        
        print(f"\n📊 Found {len(records)} record(s)")
        
        # Open output files
        with open(BEFORE_OUTPUT, 'w') as f_before, \
             open(AFTER_OUTPUT, 'w') as f_after:
            
            for idx, record in enumerate(records, 1):
                stats["total"] += 1
                
                # Extract timestamp
                timestamp_str = record.get('timestamp')
                if not timestamp_str:
                    print(f"  ⚠️  Record {idx}: No 'timestamp' field")
                    stats["failed"] += 1
                    continue
                
                # Parse date
                post_date = parse_date_from_iso(timestamp_str)
                if not post_date:
                    stats["failed"] += 1
                    continue
                
                # Determine which output file to use
                if post_date <= SPLIT_DATE.date():
                    f_before.write(json.dumps(record) + '\n')
                    stats["before"] += 1
                    category = "BEFORE"
                else:
                    f_after.write(json.dumps(record) + '\n')
                    stats["after"] += 1
                    category = "AFTER"
                
                post_title = record.get('caption', 'N/A')[:50].replace('\n', ' ')
                print(f"  [{idx}] {post_title}... ({post_date}) → {category}")
        
    except json.JSONDecodeError as e:
        print(f"  ✗ Invalid JSON: {e}")
        stats["failed"] += 1
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        stats["failed"] += 1
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✓ Before 2026-04-10:")
    print(f"  - Records: {stats['before']}")
    print(f"  - Output: {BEFORE_OUTPUT}")
    print(f"\n✓ After 2026-04-10:")
    print(f"  - Records: {stats['after']}")
    print(f"  - Output: {AFTER_OUTPUT}")
    print(f"\n📊 Total processed: {stats['total']}")
    print(f"✗ Failed: {stats['failed']}")
    print("=" * 70)

if __name__ == "__main__":
    main()
