#!/usr/bin/env python3
import os
import glob
from pathlib import Path

def cleanup_folder(folder_path):
    """
    Clean up duplicate JSON files in a folder.
    Keep the largest file and rename it to post.json, delete others.
    """
    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    
    if len(json_files) <= 1:
        return False  # No cleanup needed
    
    # Get file sizes
    files_with_sizes = [(f, os.path.getsize(f)) for f in json_files]
    
    # Sort by size descending
    files_with_sizes.sort(key=lambda x: x[1], reverse=True)
    
    largest_file = files_with_sizes[0][0]
    largest_size = files_with_sizes[0][1]
    
    print(f"\nFolder: {folder_path}")
    print(f"Found {len(json_files)} JSON files:")
    
    for file, size in files_with_sizes:
        print(f"  {os.path.basename(file)}: {size:,} bytes")
    
    # Rename largest to post.json
    target_name = os.path.join(folder_path, "post.json")
    if largest_file != target_name:
        if os.path.exists(target_name):
            os.remove(target_name)
            print(f"  Deleted: {os.path.basename(target_name)}")
        os.rename(largest_file, target_name)
        print(f"  Renamed largest to: post.json")
    
    # Delete other files
    for file, size in files_with_sizes[1:]:
        os.remove(file)
        print(f"  Deleted: {os.path.basename(file)}")
    
    return True

def main():
    data_base_path = "/home/khang/Code/data-pipeline/IT4931/crawl_facebook/data"
    
    # Find all folders under data (including subfolders like Beatvn, NEU Confessions, etc.)
    cleaned_count = 0
    skipped_count = 0
    
    # Walk through all subdirectories in data
    for root, dirs, files in os.walk(data_base_path):
        # Check if this is a timestamp folder (contains .json files)
        json_files = glob.glob(os.path.join(root, "*.json"))
        if json_files:
            if cleanup_folder(root):
                cleaned_count += 1
            else:
                skipped_count += 1
    
    print(f"\n{'='*60}")
    print(f"Cleanup completed!")
    print(f"Folders cleaned: {cleaned_count}")
    print(f"Folders with no duplicates: {skipped_count}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
