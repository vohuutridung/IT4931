#!/usr/bin/env python3
"""
Script to split Reddit data into two parts:
- Before April 10, 2026
- After April 10, 2026
"""
import json
from datetime import datetime
from pathlib import Path

INPUT_FILE = "/home/khang/Code/data-pipeline/IT4931/crawl_reddit/data/posts.jsonl"
OUTPUT_DIR = "/home/khang/Code/data-pipeline/IT4931/crawl_reddit"

BEFORE_OUTPUT = f"{OUTPUT_DIR}/posts_before_2026_04_10.jsonl"
AFTER_OUTPUT = f"{OUTPUT_DIR}/posts_after_2026_04_10.jsonl"

# Split date: April 10, 2026
SPLIT_DATE = datetime(2026, 4, 10)

def parse_date_from_iso(date_str):
    """
    Parse ISO 8601 datetime string: 2026-04-11T03:04:35+00:00
    Returns datetime object with date only or None if parsing fails
    """
    try:
        # Remove timezone info and parse
        # Format: 2026-04-11T03:04:35+00:00
        dt = datetime.fromisoformat(date_str.replace('+00:00', ''))
        return dt.date()
    except (ValueError, AttributeError) as e:
        print(f"  ⚠️  Failed to parse: {date_str} - {e}")
        return None

def main():
    print("=" * 70)
    print("SPLITTING REDDIT DATA BY DATE")
    print(f"Split date: {SPLIT_DATE.strftime('%Y-%m-%d')}")
    print(f"Input file: {INPUT_FILE}")
    print("=" * 70)
    
    stats = {
        "before": 0,
        "after": 0,
        "failed": 0,
        "total": 0
    }
    
    # Open output files
    with open(BEFORE_OUTPUT, 'w') as f_before, \
         open(AFTER_OUTPUT, 'w') as f_after:
        
        # Process each line in the JSONL file
        with open(INPUT_FILE, 'r') as f_input:
            for line_num, line in enumerate(f_input, 1):
                try:
                    # Parse JSON line
                    record = json.loads(line)
                    stats["total"] += 1
                    
                    # Extract created_utc
                    created_str = record.get('created_utc')
                    if not created_str:
                        print(f"  ⚠️  Line {line_num}: No 'created_utc' field")
                        stats["failed"] += 1
                        continue
                    
                    # Parse date
                    post_date = parse_date_from_iso(created_str)
                    if not post_date:
                        stats["failed"] += 1
                        continue
                    
                    # Determine which output file to use
                    if post_date <= SPLIT_DATE.date():
                        f_before.write(line)
                        stats["before"] += 1
                        category = "BEFORE"
                    else:
                        f_after.write(line)
                        stats["after"] += 1
                        category = "AFTER"
                    
                    # Print progress every 1000 lines
                    if line_num % 1000 == 0:
                        post_title = record.get('title', 'N/A')[:50]
                        print(f"  [{line_num}] {post_title}... ({post_date}) → {category}")
                
                except json.JSONDecodeError as e:
                    print(f"  ✗ Line {line_num}: Invalid JSON - {e}")
                    stats["failed"] += 1
                except Exception as e:
                    print(f"  ✗ Line {line_num}: Unexpected error - {e}")
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
