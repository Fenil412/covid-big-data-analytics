#!/bin/bash
# =============================================================================
# setup_hdfs_dirs.sh — HDFS Directory Structure Setup
# Project: COVID-19 Big Data Analytics Platform
# Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)
#
# Usage: bash scripts/hdfs/setup_hdfs_dirs.sh
#        (Run after cluster is up)
# =============================================================================

set -e

GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[HDFS] $1${NC}"; }
success() { echo -e "${GREEN}[✓] $1${NC}"; }

CONTAINER="hadoop-master"

log "Creating HDFS directory structure for COVID analytics..."

# ── Raw data directory ────────────────────────────────────────────────────────
docker exec $CONTAINER hdfs dfs -mkdir -p /covid/raw
success "Created /covid/raw"

# ── Processed data directory ──────────────────────────────────────────────────
docker exec $CONTAINER hdfs dfs -mkdir -p /covid/processed
success "Created /covid/processed"

# ── Output / results directory ────────────────────────────────────────────────
docker exec $CONTAINER hdfs dfs -mkdir -p /covid/output
success "Created /covid/output"

# ── Spark logs directory ──────────────────────────────────────────────────────
docker exec $CONTAINER hdfs dfs -mkdir -p /spark-logs
success "Created /spark-logs"

# ── Set permissions ───────────────────────────────────────────────────────────
docker exec $CONTAINER hdfs dfs -chmod -R 777 /covid
docker exec $CONTAINER hdfs dfs -chmod -R 777 /spark-logs
success "Permissions set (777) on /covid and /spark-logs"

# ── List created structure ────────────────────────────────────────────────────
log "HDFS directory structure:"
docker exec $CONTAINER hdfs dfs -ls -R /covid
echo ""
success "HDFS setup complete."
