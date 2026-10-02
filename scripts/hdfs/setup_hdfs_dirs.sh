#!/bin/bash
# Create HDFS paths for COVID analytics (run after NameNode is healthy).

set -e
CONTAINER="hadoop-master"

log() { echo "[HDFS] $1"; }

log "Creating HDFS directory structure..."
docker exec "$CONTAINER" hdfs dfs -mkdir -p /covid/input /covid/processed /covid/output /covid/raw /spark-logs
docker exec "$CONTAINER" hdfs dfs -chmod -R 777 /covid /spark-logs
log "Directory listing:"
docker exec "$CONTAINER" hdfs dfs -ls -R /covid
