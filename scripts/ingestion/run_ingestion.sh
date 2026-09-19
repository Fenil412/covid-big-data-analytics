#!/bin/bash
# =============================================================================
# run_ingestion.sh — Full Data Ingestion Pipeline
# Project: COVID-19 Big Data Analytics Platform
# Author:  Sarth Narola (Member 2 — Cluster & Data Ingestion Engineer)
#
# Usage: bash scripts/ingestion/run_ingestion.sh
# Runs: data_downloader.py → hdfs_uploader.py
# =============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

log()     { echo -e "${CYAN}[INGEST] $1${NC}"; }
success() { echo -e "${GREEN}[✓] $1${NC}"; }
error()   { echo -e "${RED}[✗] $1${NC}"; exit 1; }

echo ""
echo "========================================================"
echo "  COVID-19 Data Ingestion Pipeline"
echo "========================================================"
echo ""

# ── Step 1: Download CSV data ─────────────────────────────────────────────────
log "Step 1: Downloading Google COVID-19 Open Data..."
python3 "$PROJECT_ROOT/src/ingestion/data_downloader.py" \
    || error "Download failed. Check internet connection."
success "Download complete."

# ── Step 2: Upload to HDFS ────────────────────────────────────────────────────
log "Step 2: Uploading data to HDFS /covid/raw ..."
python3 "$PROJECT_ROOT/src/ingestion/hdfs_uploader.py" \
    || error "HDFS upload failed. Ensure cluster is running: bash scripts/cluster/start_cluster.sh"
success "Upload to HDFS complete."

echo ""
echo "========================================================"
echo "  Ingestion complete! Data is now in HDFS /covid/raw"
echo "  Next: Submit Spark job → analytics_job.py"
echo "========================================================"
echo ""
