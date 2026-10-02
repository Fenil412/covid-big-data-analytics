#!/bin/bash
# Verify Docker Hadoop cluster: containers, DataNodes, YARN, HDFS paths.

set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "=== Docker containers ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "=== HDFS dfsadmin -report (Live datanodes) ==="
docker exec hadoop-master hdfs dfsadmin -report | head -40

echo ""
echo "=== YARN nodes ==="
docker exec hadoop-resourcemanager yarn node -list 2>/dev/null || echo "(YARN CLI pending — check http://localhost:8088)"

echo ""
echo "=== HDFS /covid/input ==="
docker exec hadoop-master hdfs dfs -ls /covid/input 2>/dev/null || echo "/covid/input not created yet — run upload-data"

if docker exec hadoop-master hdfs dfs -test -e /covid/input/epidemiology.csv 2>/dev/null; then
  echo ""
  echo "=== Block locations (epidemiology.csv) ==="
  docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations | head -30
fi

echo ""
echo "Verification complete."
