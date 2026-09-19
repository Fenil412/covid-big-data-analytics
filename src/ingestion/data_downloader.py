"""
data_downloader.py — Google COVID-19 Open Data Downloader
Project: COVID-19 Big Data Analytics Platform
Author:  Sarth Narola (Member 2 — Cluster & Data Ingestion Engineer)

Downloads the Google COVID-19 Open Data CSV files from the official
GitHub repository and saves them to the local dataset/ directory.
"""

import os
import logging
import requests
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Base URL for Google COVID-19 Open Data
BASE_URL = "https://storage.googleapis.com/covid19-open-data/v3"

# Files to download
COVID_DATA_FILES = [
    "epidemiology.csv",
    "vaccinations.csv",
    "hospitalizations.csv",
    "demographics.csv",
    "index.csv",
]

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "dataset"


def download_file(url: str, dest_path: Path, chunk_size: int = 8192) -> bool:
    """
    Download a single file from a URL to dest_path with progress logging.

    Args:
        url:        Source URL.
        dest_path:  Destination file path.
        chunk_size: Download chunk size in bytes.

    Returns:
        True if successful, False otherwise.
    """
    try:
        logger.info(f"Downloading: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

        size_mb = downloaded / (1024 * 1024)
        logger.info(f"  Saved: {dest_path.name} ({size_mb:.1f} MB)")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"  Failed to download {url}: {e}")
        return False


def download_all(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict:
    """
    Download all configured COVID-19 data files.

    Args:
        output_dir: Directory to save downloaded files.

    Returns:
        Dict mapping filename to success/failure status.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    start = datetime.now()

    logger.info("=" * 55)
    logger.info("  Google COVID-19 Open Data — Downloader")
    logger.info(f"  Output directory: {output_dir}")
    logger.info("=" * 55)

    for filename in COVID_DATA_FILES:
        url = f"{BASE_URL}/{filename}"
        dest = output_dir / filename
        success = download_file(url, dest)
        results[filename] = success

    elapsed = (datetime.now() - start).total_seconds()
    success_count = sum(results.values())

    logger.info("=" * 55)
    logger.info(f"  Downloaded {success_count}/{len(COVID_DATA_FILES)} files in {elapsed:.1f}s")
    logger.info("=" * 55)

    return results


if __name__ == "__main__":
    results = download_all()
    if not all(results.values()):
        failed = [f for f, ok in results.items() if not ok]
        logger.warning(f"Failed files: {failed}")
        exit(1)
