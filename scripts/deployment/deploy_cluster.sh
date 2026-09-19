#!/bin/bash
# =============================================================================
# deploy_cluster.sh — Full Automated Cluster Deployment
# Project: COVID-19 Big Data Analytics Platform
# Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)
#
# Usage: bash scripts/deployment/deploy_cluster.sh
# =============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_FILE="$PROJECT_ROOT/output/deploy.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[$(date '+%H:%M:%S')] $1${NC}" | tee -a "$LOG_FILE"; }
success() { echo -e "${GREEN}[✓] $1${NC}" | tee -a "$LOG_FILE"; }
warn() { echo -e "${YELLOW}[!] $1${NC}" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[✗] $1${NC}" | tee -a "$LOG_FILE"; exit 1; }

mkdir -p "$PROJECT_ROOT/output"

echo "============================================================" | tee "$LOG_FILE"
echo "   COVID-19 Big Data Analytics Platform — Cluster Deploy    " | tee -a "$LOG_FILE"
echo "============================================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# ── Step 1: Pre-flight checks ────────────────────────────────────────────────
log "Step 1: Pre-flight checks..."
command -v docker &>/dev/null || error "Docker is not installed. Please install Docker first."
command -v docker-compose &>/dev/null || error "docker-compose not found. Please install it."
success "Docker and docker-compose are available."

# ── Step 2: Stop any running containers ──────────────────────────────────────
log "Step 2: Stopping existing containers (if any)..."
cd "$PROJECT_ROOT"
docker-compose down --remove-orphans 2>/dev/null && success "Existing containers stopped." || warn "No containers were running."

# ── Step 3: Pull latest images ───────────────────────────────────────────────
log "Step 3: Pulling Docker images..."
docker-compose pull
success "Images pulled successfully."

# ── Step 4: Start cluster ────────────────────────────────────────────────────
log "Step 4: Starting 3-node Hadoop cluster + MongoDB..."
docker-compose up -d
success "All containers started."

# ── Step 5: Wait for NameNode to be ready ────────────────────────────────────
log "Step 5: Waiting for Hadoop NameNode to be ready..."
MAX_WAIT=60
WAITED=0
until docker exec hadoop-master hdfs dfs -ls / &>/dev/null; do
    sleep 3
    WAITED=$((WAITED + 3))
    if [ $WAITED -ge $MAX_WAIT ]; then
        error "NameNode did not become ready in ${MAX_WAIT}s. Check logs: docker logs hadoop-master"
    fi
    log "  Still waiting... (${WAITED}s)"
done
success "NameNode is ready."

# ── Step 6: Setup HDFS directories ───────────────────────────────────────────
log "Step 6: Setting up HDFS directories..."
bash "$PROJECT_ROOT/scripts/hdfs/setup_hdfs_dirs.sh"
success "HDFS directories created."

# ── Step 7: Verify cluster ────────────────────────────────────────────────────
log "Step 7: Verifying cluster status..."
echo ""
echo "─── Docker Containers ─────────────────────────────────────"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "─── HDFS Report ────────────────────────────────────────────"
docker exec hadoop-master hdfs dfsadmin -report 2>/dev/null | head -20
echo ""
success "Cluster is up and running!"

echo ""
echo "============================================================"
echo " Access Points:"
echo "   HDFS Web UI     → http://localhost:9870"
echo "   YARN UI         → http://localhost:8088"
echo "   Spark Web UI    → http://localhost:8080"
echo "   MongoDB         → localhost:27017"
echo "============================================================"
echo ""
