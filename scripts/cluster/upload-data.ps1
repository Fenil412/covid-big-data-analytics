$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\..")

python src/ingestion/data_downloader.py
$env:HDFS_INPUT_PATH = "/covid/input"
python src/ingestion/hdfs_uploader.py

docker exec hadoop-master hdfs dfs -ls /covid/input
docker exec hadoop-master hdfs dfs -test -e /covid/input/epidemiology.csv
if ($LASTEXITCODE -eq 0) {
    docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations
}
