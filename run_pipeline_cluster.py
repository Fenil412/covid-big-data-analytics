"""
run_pipeline_cluster.py — Full distributed pipeline (Docker + HDFS + YARN Spark).

Steps:
  1. Ensure cluster is up (optional --skip-start)
  2. Download data locally
  3. Upload to HDFS /covid/input
  4. Run PySpark analytics on YARN
  5. Sync results to output/ and MongoDB Atlas

Usage:
    python run_pipeline_cluster.py
    python run_pipeline_cluster.py --skip-start --skip-download
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def run(cmd: list[str], check: bool = True) -> int:
    print(f"\n>> {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=PROJECT_ROOT, check=check).returncode


def main():
    parser = argparse.ArgumentParser(description="COVID distributed cluster pipeline")
    parser.add_argument("--skip-start", action="store_true", help="Skip docker compose up")
    parser.add_argument("--skip-download", action="store_true", help="Skip CSV download")
    parser.add_argument("--skip-upload", action="store_true", help="Skip HDFS upload")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip Spark job")
    parser.add_argument("--mode", default="yarn", choices=["yarn", "cluster", "local"])
    args = parser.parse_args()

    if not args.skip_start:
        if sys.platform == "win32":
            run([
                "powershell", "-ExecutionPolicy", "Bypass", "-File",
                str(PROJECT_ROOT / "scripts" / "cluster" / "start-cluster.ps1"),
            ])
        else:
            run(["bash", "scripts/cluster/start-cluster.sh"])

    if not args.skip_download:
        run([sys.executable, str(PROJECT_ROOT / "src" / "ingestion" / "data_downloader.py")])

    if not args.skip_upload:
        if sys.platform == "win32":
            run([
                "powershell", "-ExecutionPolicy", "Bypass", "-File",
                str(PROJECT_ROOT / "scripts" / "cluster" / "upload-data.ps1"),
            ])
        else:
            run(["bash", "scripts/cluster/upload-data.sh"])

    if not args.skip_analysis:
        env = {"SPARK_RUN_MODE": args.mode}
        if sys.platform == "win32":
            run(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File",
                 str(PROJECT_ROOT / "scripts" / "cluster" / "run-analysis.ps1"),
                 "-Mode", args.mode],
            )
        else:
            import os
            os.environ["SPARK_RUN_MODE"] = args.mode
            run(["bash", "scripts/cluster/run-analysis.sh"])

    print("\nPipeline finished. Verify: bash scripts/cluster/verify-cluster.sh")
    print("Dashboard: streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
