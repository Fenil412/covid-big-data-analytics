# Data pipeline

The downloader fetches epidemiology.csv, vaccinations.csv, hospitalizations.csv, demographics.csv, and index.csv from Google COVID-19 Open Data into dataset/. The uploader copies each CSV into the NameNode container and runs hdfs dfs -put to /covid/input. Replication is two.

Windows: upload-data.bat. Shell: bash scripts/cluster/upload-data.sh.

Verify with docker exec hadoop-master hdfs dfs -ls /covid/input. Workers read HDFS blocks; repeated bind mounts are not used as distributed storage.
