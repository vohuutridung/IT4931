#!/usr/bin/env python3
import os
import glob
import shutil

def cleanup_folder(folder_path):
    json_files = glob.glob(os.path.join(folder_path, "*.json"))

    if len(json_files) <= 1:
        return False

    # Lấy size
    files_with_sizes = [(f, os.path.getsize(f)) for f in json_files]

    # Sort giảm dần
    files_with_sizes.sort(key=lambda x: x[1], reverse=True)

    largest_file = files_with_sizes[0][0]
    target_file = os.path.join(folder_path, "post.json")

    print(f"\nFolder: {folder_path}")
    for f, size in files_with_sizes:
        print(f"  {os.path.basename(f)}: {size:,} bytes")

    try:
        # Nếu file lớn nhất KHÔNG phải post.json → move về post.json
        if os.path.abspath(largest_file) != os.path.abspath(target_file):
            # Xóa post.json cũ nếu tồn tại (tránh conflict)
            if os.path.exists(target_file):
                os.remove(target_file)
                print("  Deleted old post.json")

            shutil.move(largest_file, target_file)
            print("  Renamed largest -> post.json")

        # Xóa tất cả file còn lại (trừ post.json)
        for f, _ in files_with_sizes:
            if os.path.abspath(f) != os.path.abspath(target_file):
                if os.path.exists(f):
                    os.remove(f)
                    print(f"  Deleted: {os.path.basename(f)}")

        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    data_base_path = "/home/khang/Code/data-pipeline/IT4931/data/facebook_data/raw_data"

    cleaned_count = 0
    skipped_count = 0

    for root, dirs, files in os.walk(data_base_path):
        json_files = glob.glob(os.path.join(root, "*.json"))

        if json_files:
            if cleanup_folder(root):
                cleaned_count += 1
            else:
                skipped_count += 1

    print("\n" + "="*60)
    print("Cleanup completed!")
    print(f"Folders cleaned: {cleaned_count}")
    print(f"Folders skipped: {skipped_count}")
    print("="*60)


if __name__ == "__main__":
    main()