# Cluster setup and demonstration

## Prerequisites

- Docker Desktop with Compose v2 on Windows/macOS, or Docker Engine with the Compose plugin on Linux.
- Git and internet access for images and dataset download.
- Python 3.10 or 3.11 for the host-side upload script. Java and Hadoop are supplied by the containers.
- Copy `.env.example` to `.env`; set `MONGODB_ATLAS_URI=` empty if MongoDB is not being used.

## Start and verify

From the repository root:

```bash
docker compose build
docker compose up -d
docker compose ps
```

On Windows the helper `start-cluster.bat` starts services and initializes HDFS directories. On Linux/macOS use `bash scripts/cluster/start-cluster.sh`.

Check the Hadoop services and distributed storage:

```bash
docker exec hadoop-master hdfs dfsadmin -report
docker exec hadoop-master hdfs dfs -ls /covid/input
docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations
```

The cluster design is one master (NameNode and ResourceManager) and two workers (each has a DataNode and NodeManager). The two Spark worker containers are additional processing workers. All containers run on the same Docker host. `dfsadmin -report` should show two live DataNodes; `fsck` reports actual HDFS blocks and replicas.

Upload the source files if `/covid/input` is empty:

```text
Windows:      upload-data.bat
Linux/macOS:  bash scripts/cluster/upload-data.sh
```

The input set currently consists of `epidemiology.csv`, `vaccinations.csv`, `hospitalizations.csv`, `demographics.csv`, and `index.csv`; it does not use a file named `covid.csv`.

## Evaluation demonstration flow

Run these steps in order and capture only the output from the live system:

1. Start Docker cluster: `docker compose up -d` (or `start-cluster.bat`).
2. Show Docker containers: `docker ps`.
3. Show Master: `hadoop-master` (NameNode) and `hadoop-resourcemanager` (ResourceManager).
4. Show Worker 1: `hadoop-worker1` (DataNode) and `hadoop-nodemanager1` (NodeManager).
5. Show Worker 2: `hadoop-worker2` (DataNode) and `hadoop-nodemanager2` (NodeManager).
6. Show HDFS status: `docker exec hadoop-master hdfs dfsadmin -report`.
7. Show HDFS files: `docker exec hadoop-master hdfs dfs -ls /covid/input`.
8. Show distributed blocks: `docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations`.
9. Run distributed Spark analytics: Windows `run-analysis.bat -Mode cluster`; Linux/macOS `SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh`.
10. Check the job exit code, then show analytics output: `docker exec hadoop-master hdfs dfs -ls -R /covid/output`. Only show it as fresh output if the current run succeeded.
11. Open Streamlit at <http://localhost:8501> (or run `streamlit run dashboard/app.py` with a local Python environment).
12. Demonstrate the dashboard and dynamic KPI cards.
13. Demonstrate the world map when processed country data is available.
14. Select a country and inspect its details.
15. Change chart type/metric and date/grouping filters.
16. Switch the sidebar theme between Light and Dark.

YARN client mode is the runner default, but the current Hadoop NodeManager images lack Python 3. Use Spark standalone mode above unless the images have been changed. The standalone job has previously lost an executor during shuffle on a memory-constrained host; do not present stale output as a successful current run.

Save real screenshots under `docs/screenshots/`. Capture the command/page and date, and include success or failure context. See [screenshot instructions](screenshots/README.md). Do not fabricate cluster output or dashboard screenshots.

## Stop the cluster

```bash
docker compose down
```

This keeps named HDFS volumes. `docker compose down --volumes` deletes cluster data and NameNode metadata; use only when deliberately resetting the data.
