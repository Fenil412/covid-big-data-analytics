"""
hdfs_uploader.py — Upload Raw CSV Data to HDFS
Project: COVID-19 Big Data Analytics Platform
Author:  Sarth Narola (Member 2 — Cluster & Data Ingestion Engineer)

Uploads locally downloaded COVID-19 CSV files from dataset/ directory
to the Hadoop HDFS /covid/raw path using the hdfs CLI via subprocess.
"""

import subprocess
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

HDFS_RAW_PATH = "/covid/raw"
HADOOP_CONTAINER = "hadoop-master"
DEFAULT_DATASET_DIR = Path(__file__).resolve().parents[2] / "dataset"


def run_docker_cmd(cmd: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command inside the Hadoop master Docker container."""
    full_cmd = ["docker", "exec", HADOOP_CONTAINER] + cmd
    logger.debug(f"Running: {' '.join(full_cmd)}")
    return subprocess.run(full_cmd, capture_output=True, text=True, check=check)


def hdfs_mkdir(path: str) -> None:
    """Create directory on HDFS if it doesn't exist."""
    run_docker_cmd(["hdfs", "dfs", "-mkdir", "-p", path])
    logger.info(f"HDFS directory ready: {path}")


def upload_file(local_path: Path, hdfs_path: str) -> bool:
    """
    Copy a local file into the Hadoop master container, then put it to HDFS.

    Args:
        local_path: Absolute local path to the CSV file.
        hdfs_path:  Target HDFS directory path.

    Returns:
        True if upload succeeded, False otherwise.
    """
    try:
        filename = local_path.name
        container_tmp = f"/tmp/{filename}"

        # Copy file into container
        logger.info(f"Copying {filename} into container...")
        subprocess.run(
            ["docker", "cp", str(local_path), f"{HADOOP_CONTAINER}:{container_tmp}"],
            check=True, capture_output=True,
        )

        # Upload from container to HDFS
        logger.info(f"Uploading {filename} to HDFS {hdfs_path}/...")
        run_docker_cmd(["hdfs", "dfs", "-put", "-f", container_tmp, hdfs_path])

        # Cleanup temp file
        run_docker_cmd(["rm", "-f", container_tmp])

        size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info(f"  ✓ Uploaded {filename} ({size_mb:.1f} MB)")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"  ✗ Failed to upload {local_path.name}: {e.stderr}")
        return False


def upload_all(dataset_dir: Path = DEFAULT_DATASET_DIR, hdfs_path: str = HDFS_RAW_PATH) -> dict:
    """
    Upload all CSV files from dataset_dir to HDFS.

    Args:
        dataset_dir: Local directory containing CSV files.
        hdfs_path:   HDFS target directory.

    Returns:
        Dict mapping filename → success status.
    """
    csv_files = list(dataset_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {dataset_dir}. Run data_downloader.py first.")
        return {}

    results = {}
    start = datetime.now()

    logger.info("=" * 55)
    logger.info("  COVID-19 Data — HDFS Uploader")
    logger.info(f"  Source: {dataset_dir}")
    logger.info(f"  Target: hdfs://master:9000{hdfs_path}")
    logger.info(f"  Files : {len(csv_files)}")
    logger.info("=" * 55)

    hdfs_mkdir(hdfs_path)

    for csv_file in csv_files:
        results[csv_file.name] = upload_file(csv_file, hdfs_path)

    elapsed = (datetime.now() - start).total_seconds()
    success_count = sum(results.values())
    logger.info(f"Uploaded {success_count}/{len(csv_files)} files in {elapsed:.1f}s")

    # Verify on HDFS
    logger.info("HDFS contents:")
    result = run_docker_cmd(["hdfs", "dfs", "-ls", hdfs_path], check=False)
    logger.info(result.stdout)

    return results


if __name__ == "__main__":
    results = upload_all()
    if not all(results.values()):
        exit(1)
