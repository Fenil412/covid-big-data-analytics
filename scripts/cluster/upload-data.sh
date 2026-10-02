#!/bin/bash
set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "=== Download CSVs (if missing) ==="
python "$PROJECT_ROOT/src/ingestion/data_downloader.py"

echo "=== Upload to HDFS /covid/input ==="
export HDFS_INPUT_PATH="${HDFS_INPUT_PATH:-/covid/input}"
python "$PROJECT_ROOT/src/ingestion/hdfs_uploader.py"

echo ""
echo "=== Verify blocks on HDFS ==="
docker exec hadoop-master hdfs dfs -ls /covid/input
if docker exec hadoop-master hdfs dfs -test -e /covid/input/epidemiology.csv; then
  docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations | head -25
fi
