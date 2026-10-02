# Start Hadoop + YARN + Spark (Windows)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\..")

Write-Host "Starting cluster..."
docker compose up -d

Write-Host "Waiting for NameNode..."
for ($i = 0; $i -lt 40; $i++) {
    docker exec hadoop-master hdfs dfs -ls / 2>$null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 3
}

docker exec hadoop-master hdfs dfs -mkdir -p /covid/input /covid/processed /covid/output /covid/raw /spark-logs 2>$null
docker exec hadoop-master hdfs dfs -chmod -R 777 /covid /spark-logs 2>$null

Write-Host "Cluster started."
Write-Host "  HDFS: http://localhost:9870"
Write-Host "  YARN: http://localhost:8088"
