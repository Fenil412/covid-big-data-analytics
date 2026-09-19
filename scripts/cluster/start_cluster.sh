#!/bin/bash
# =============================================================================
# start_cluster.sh — Start Hadoop + MongoDB Docker Cluster
# Project: COVID-19 Big Data Analytics Platform
# Author:  Sarth Narola (Member 2 — Cluster & Data Ingestion Engineer)
#
# Usage: bash scripts/cluster/start_cluster.sh
# =============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()     { echo -e "${CYAN}[CLUSTER] $1${NC}"; }
success() { echo -e "${GREEN}[✓] $1${NC}"; }
warn()    { echo -e "${YELLOW}[!] $1${NC}"; }

echo ""
echo "========================================================"
echo "  Starting COVID-19 Analytics Cluster"
echo "========================================================"
echo ""

cd "$PROJECT_ROOT"

# Check if already running
RUNNING=$(docker-compose ps --services --filter "status=running" 2>/dev/null | wc -l)
if [ "$RUNNING" -gt 0 ]; then
    warn "Some containers are already running. Restarting..."
    docker-compose restart
else
    log "Starting all containers..."
    docker-compose up -d
fi

echo ""
log "Container status:"
docker-compose ps

echo ""
success "Cluster started!"
echo "  HDFS Web UI  → http://localhost:9870"
echo "  YARN UI      → http://localhost:8088"
echo "  Spark UI     → http://localhost:8080"
echo ""
