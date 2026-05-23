#!/usr/bin/env python3
"""Script to download raw data zip from Google Drive and extract it to the project root."""

import os
import sys
import zipfile
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger("download_data")

try:
    import requests
except ImportError:
    logger.error("The 'requests' library is required. Please install it using: pip install requests")
    sys.exit(1)

DRIVE_FILE_ID = "1kuD8vNUkuW55lnwK6INegDlO3xm5hjj3"
TEMP_ZIP_PATH = Path("data_temp.zip")
EXTRACT_DIR = Path(".")

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value
    return None

def download_file_from_google_drive(file_id, destination):
    url = "https://docs.google.com/uc?export=download"
    session = requests.Session()

    logger.info("Connecting to Google Drive...")
    response = session.get(url, params={"id": file_id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {"id": file_id, "confirm": token}
        response = session.get(url, params=params, stream=True)

    logger.info("Downloading data package (this may take a few minutes)...")
    chunk_size = 1024 * 1024  # 1MB chunks
    total_downloaded = 0
    
    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size):
            if chunk:
                f.write(chunk)
                total_downloaded += len(chunk)
                # Print progress simple status every 10MB
                if total_downloaded % (10 * 1024 * 1024) < chunk_size:
                    logger.info("Downloaded %d MB...", total_downloaded // (1024 * 1024))
                    
    logger.info("Download completed. Total size: %d MB", total_downloaded // (1024 * 1024))

def main():
    try:
        # 1. Download ZIP file
        download_file_from_google_drive(DRIVE_FILE_ID, TEMP_ZIP_PATH)
        
        # 2. Extract ZIP file
        logger.info("Extracting data package to project root...")
        with zipfile.ZipFile(TEMP_ZIP_PATH, "r") as zip_ref:
            file_list = zip_ref.namelist()
            logger.info("Extracting %d files...", len(file_list))
            zip_ref.extractall(EXTRACT_DIR)
            
        logger.info("Extraction completed successfully!")
        
    except Exception as exc:
        logger.error("An error occurred during download/extraction: %s", exc)
        sys.exit(1)
        
    finally:
        # 3. Clean up temp zip file
        if TEMP_ZIP_PATH.exists():
            logger.info("Cleaning up temporary zip file...")
            TEMP_ZIP_PATH.unlink()
            
    logger.info("Data setup complete. Ready to use!")

if __name__ == "__main__":
    main()
