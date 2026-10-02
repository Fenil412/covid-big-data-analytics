#!/bin/bash
# Optional Hadoop Streaming demo on HDFS (validates YARN MapReduce shuffle).
set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

INPUT="${HDFS_INPUT_PATH:-/covid/input/epidemiology.csv}"
OUT="/covid/output/streaming_country_rows"

docker exec hadoop-master hdfs dfs -rm -r -f "$OUT" 2>/dev/null || true

docker cp "$PROJECT_ROOT/hadoop/streaming/mapper.py" hadoop-master:/tmp/mapper.py
docker cp "$PROJECT_ROOT/hadoop/streaming/reducer.py" hadoop-master:/tmp/reducer.py
docker exec hadoop-master chmod +x /tmp/mapper.py /tmp/reducer.py

docker exec hadoop-master bash -c "
  yarn jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -input $INPUT \
    -output $OUT \
    -mapper /tmp/mapper.py \
    -reducer /tmp/reducer.py \
    -file /tmp/mapper.py \
    -file /tmp/reducer.py
"

echo "Streaming job output:"
docker exec hadoop-master hdfs dfs -cat "${OUT}/part-*" | head -20
