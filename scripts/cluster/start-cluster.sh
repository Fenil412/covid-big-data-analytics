#!/bin/bash
set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "Starting 3-node Hadoop (HDFS+YARN) + Spark..."
docker compose up -d

echo "Waiting for NameNode..."
for i in $(seq 1 40); do
  if docker exec hadoop-master hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  sleep 3
done

bash "$PROJECT_ROOT/scripts/hdfs/setup_hdfs_dirs.sh"
echo ""
echo "Cluster started."
echo "  HDFS NameNode  → http://localhost:9870"
echo "  YARN RM        → http://localhost:8088"
echo "  Spark Master   → http://localhost:8080"
echo "Next: bash scripts/cluster/upload-data.sh"
