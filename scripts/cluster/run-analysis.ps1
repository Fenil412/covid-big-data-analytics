param(
    [string]$Mode = "yarn"
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\..")

$input = "hdfs://master:9000/covid/input"
$output = "hdfs://master:9000/covid/output"
$outputDir = "/covid/output"

$submitMaster = switch ($Mode) {
    "cluster" { if ($env:SPARK_MASTER) { $env:SPARK_MASTER } else { "spark://spark-master:7077" } }
    "local"   { "local[*]" }
    default   { "yarn" }
}
$deployMode = "client"

docker exec spark-master python3 -m pip install -q python-dotenv==0.21.1
$zipScript = "import os,zipfile; z=zipfile.ZipFile('/tmp/covid_app.zip','w',zipfile.ZIP_DEFLATED); [z.write(os.path.join(r,n),os.path.relpath(os.path.join(r,n),'/opt/spark-apps')) for b in ('/opt/spark-apps/src','/opt/spark-apps/spark') for r,d,fs in os.walk(b) for n in fs if n.endswith('.py')]; z.close()"
docker exec spark-master python3 -c $zipScript
if ($LASTEXITCODE -ne 0) { throw "Could not package the Spark application sources." }

Write-Host "Submitting PySpark job (mode=$Mode, master=$submitMaster)"
docker exec -e SPARK_EXECUTOR_MEMORY=512m -e SPARK_EXECUTOR_CORES=2 spark-master /spark/bin/spark-submit `
  --master $submitMaster `
  --deploy-mode $deployMode `
  --driver-memory 512m `
  --executor-memory 512m `
  --executor-cores 2 `
  --num-executors 2 `
  --conf spark.executor.memoryOverhead=128m `
  --conf spark.hadoop.fs.defaultFS=hdfs://master:9000 `
  --conf spark.network.timeout=600s `
  --conf spark.executor.heartbeatInterval=60s `
  --conf spark.rpc.askTimeout=600s `
  --conf spark.yarn.am.memory=512m `
  --conf spark.executorEnv.PYTHONPATH=/opt/spark-apps `
  --conf spark.yarn.appMasterEnv.PYTHONPATH=/opt/spark-apps `
  --py-files /tmp/covid_app.zip `
  /opt/spark-apps/spark/jobs/analytics_job.py `
  --mode $Mode `
  --input-path $input `
  --output-path $output `
  --skip-mongo
if ($LASTEXITCODE -ne 0) { throw "Distributed Spark analytics failed with exit code $LASTEXITCODE." }

docker exec hadoop-master hdfs dfs -ls $outputDir

New-Item -ItemType Directory -Force -Path output | Out-Null
foreach ($coll in @("global_metrics","country_summary","daily_summary","country_daily_summary","vaccination_summary","hospitalization_summary","regional_summary")) {
    docker exec hadoop-master hdfs dfs -test -e "$outputDir/$coll" 2>$null
    if ($LASTEXITCODE -eq 0) {
        docker exec hadoop-master hdfs dfs -get -f "$outputDir/$coll" "/tmp/$coll"
        docker cp "hadoop-master:/tmp/$coll" "output/"
    }
}

Write-Host "Done. Run: streamlit run dashboard/app.py"
