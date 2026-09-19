#!/bin/bash
# =============================================================================
# stop_cluster.sh — Stop Hadoop + MongoDB Docker Cluster
# Project: COVID-19 Big Data Analytics Platform
# Author:  Sarth Narola (Member 2 — Cluster & Data Ingestion Engineer)
#
# Usage: bash scripts/cluster/stop_cluster.sh
# =============================================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

log()     { echo -e "${CYAN}[CLUSTER] $1${NC}"; }
success() { echo -e "${GREEN}[✓] $1${NC}"; }

echo ""
echo "========================================================"
echo "  Stopping COVID-19 Analytics Cluster"
echo "========================================================"
echo ""

cd "$PROJECT_ROOT"

log "Stopping all containers..."
docker-compose down

success "All containers stopped."
echo ""
