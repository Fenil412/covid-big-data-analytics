#!/bin/bash
# Distributed COVID analytics: PySpark on YARN (default) or Spark standalone.
set -e
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

MODE="${SPARK_RUN_MODE:-yarn}"
INPUT="${HDFS_INPUT_PATH:-hdfs://master:9000/covid/input}"
OUTPUT="${HDFS_OUTPUT_PATH:-hdfs://master:9000/covid/output}"
# hdfs dfs commands use paths without the URI scheme
OUTPUT_DIR="${OUTPUT#hdfs://master:9000}"

case "$MODE" in
  cluster)
    SUBMIT_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
    ;;
  local)
    SUBMIT_MASTER="local[*]"
    ;;
  *)
    SUBMIT_MASTER="yarn"
    ;;
esac
DEPLOY_MODE="client"

echo "=== Preparing Spark driver container ==="
docker exec spark-master python3 -m pip install -q python-dotenv==0.21.1
docker exec spark-master python3 -c 'import os,zipfile; z=zipfile.ZipFile("/tmp/covid_app.zip","w",zipfile.ZIP_DEFLATED); [z.write(os.path.join(r,n),os.path.relpath(os.path.join(r,n),"/opt/spark-apps")) for b in ("/opt/spark-apps/src","/opt/spark-apps/spark") for r,d,fs in os.walk(b) for n in fs if n.endswith(".py")]; z.close()'

echo "=== Submitting PySpark job (mode=$MODE, master=$SUBMIT_MASTER) ==="
docker exec -e SPARK_EXECUTOR_MEMORY=512m -e SPARK_EXECUTOR_CORES=2 spark-master /spark/bin/spark-submit \
  --master "$SUBMIT_MASTER" \
  --deploy-mode "$DEPLOY_MODE" \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 2 \
  --num-executors 2 \
  --conf spark.executor.memoryOverhead=128m \
  --conf spark.hadoop.fs.defaultFS=hdfs://master:9000 \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.rpc.askTimeout=600s \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark-apps \
  --conf spark.yarn.appMasterEnv.PYTHONPATH=/opt/spark-apps \
  --py-files /tmp/covid_app.zip \
  /opt/spark-apps/spark/jobs/analytics_job.py \
  --mode "$MODE" \
  --input-path "$INPUT" \
  --output-path "$OUTPUT" \
  --skip-mongo

echo "=== HDFS output ==="
docker exec hadoop-master hdfs dfs -ls "$OUTPUT_DIR" || true

echo "=== Sync Parquet to local output/ (Streamlit fallback) ==="
mkdir -p "$PROJECT_ROOT/output"
for coll in global_metrics country_summary daily_summary country_daily_summary vaccination_summary hospitalization_summary regional_summary; do
  docker exec hadoop-master hdfs dfs -test -e "$OUTPUT_DIR/$coll" && \
    docker exec hadoop-master hdfs dfs -get -f "$OUTPUT_DIR/$coll" "/tmp/$coll" && \
    docker cp "hadoop-master:/tmp/$coll" "$PROJECT_ROOT/output/" || true
done

echo "Analysis complete. Start dashboard: streamlit run dashboard/app.py"
