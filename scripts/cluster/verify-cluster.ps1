$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..\..")

Write-Host "=== docker ps ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`n=== hdfs dfsadmin -report ==="
docker exec hadoop-master hdfs dfsadmin -report

Write-Host "`n=== hdfs dfs -ls /covid/input ==="
docker exec hadoop-master hdfs dfs -ls /covid/input

$test = docker exec hadoop-master hdfs dfs -test -e /covid/input/epidemiology.csv 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "`n=== hdfs fsck (blocks) ==="
    docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations
}
