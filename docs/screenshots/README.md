# Demonstration screenshots

This directory is reserved for screenshots captured from a real run of the project. No sample, generated, or edited screenshots should be presented as live cluster evidence. The directory may remain empty until a demonstration is performed.

## Capture checklist

1. Start the stack with `docker compose up -d` and capture `docker ps` with the NameNode, ResourceManager, both DataNodes, both NodeManagers, Spark master, and both Spark workers visible.
2. Capture the Hadoop master and worker services in the container list.
3. Capture `docker exec hadoop-master hdfs dfsadmin -report`, showing the actual live DataNode count.
4. Capture `docker exec hadoop-master hdfs dfs -ls /covid/input` after uploading the CSVs.
5. Capture `docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations` to show the real HDFS block locations.
6. Run the Spark job and capture the Spark UI/job status and the actual terminal exit status.
7. Capture `docker exec hadoop-master hdfs dfs -ls -R /covid/output` only after a successful job.
8. Capture the Streamlit dashboard, map and country selection, chart controls, and both theme modes from the running app.

## Saving evidence

Save original image files here with descriptive names, for example `hdfs-report-YYYY-MM-DD.png` or `dashboard-dark-theme-YYYY-MM-DD.png`. Add a short caption to this README or a companion note with the capture date, exact command/page, and whether the job completed successfully. Do not crop away errors or alter numbers in a way that changes their meaning. Avoid including credentials, `.env` contents, or personally identifying information.

For the full demonstration sequence, see [cluster setup](../cluster-setup.md#evaluation-demonstration-flow).
