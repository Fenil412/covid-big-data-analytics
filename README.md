# COVID-19 Big Data Analytics

A Docker Compose project for storing COVID-19 data in HDFS, processing it with Apache Spark across worker containers, and exploring available analytics in the existing Streamlit dashboard.

> **Current verification note:** The three-node Hadoop cluster and HDFS input flow have been verified. The latest full Spark job did not finish on the development host (exit code 137 during regional aggregation); the existing analytics output may therefore be stale. YARN cluster deployment also requires Python 3 in the Hadoop NodeManager containers. Treat the processing commands below as the intended workflow and check job completion and output timestamps before presenting results.

## Architecture

```text
Google COVID-19 Open Data CSVs
              │ download
              ▼
       Local dataset/ files
              │ hdfs dfs -put
              ▼
      HDFS /covid/input
       replication = 2
              │
              ▼
       Apache Spark job
  Spark master + 2 workers
              │
              ▼
      HDFS /covid/output
              │ sync after success
              ├── output/ Parquet ──┐
              └── MongoDB (optional)│
                                    ▼
                         Streamlit dashboard
```

The Compose cluster is a multi-container cluster on one Docker host (not three physical machines). Hadoop services are arranged as:

| Logical node | Services |
|---|---|
| Master | NameNode and ResourceManager |
| Worker 1 | DataNode and NodeManager |
| Worker 2 | DataNode and NodeManager |

Spark has a master and two Spark workers. See [architecture](docs/architecture.md), [cluster setup](docs/cluster-setup.md), [data pipeline](docs/data-pipeline.md), [processing](docs/processing.md), and [dashboard integration](docs/streamlit-integration.md) for details.

## Requirements for a new computer

- Git
- Docker Desktop with Docker Compose v2 (Windows/macOS), or Docker Engine and the Compose plugin (Linux)
- Internet access for downloading container images and the source CSVs
- Recommended: 8 GB or more RAM available to Docker; the full Spark job may need more, depending on source data and host resources
- Python 3.10 or 3.11 only if using local scripts, the optional virtual environment, or running Streamlit outside Docker

Java and Hadoop do not need to be installed on the host for the Docker workflow; their services run in containers. Start Docker Desktop/Engine before using the commands. Check installation with:

```bash
git --version
docker --version
docker compose version
```

## Get the project and configure it

Clone the repository using its current remote URL, then enter the project directory:

```bash
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

Create the local environment file. Compose expects `.env` to exist, even when MongoDB is not configured.

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
```

**Windows Command Prompt:**

```bat
copy .env.example .env
```

**Linux/macOS:**

```bash
cp .env.example .env
```

Edit `.env` on your computer. Set `MONGODB_ATLAS_URI=` to an empty value to use local Parquet results without Atlas, or enter your own MongoDB connection string to enable the optional MongoDB destination. Do not commit `.env` or share credentials. No team member's database credentials are included in this repository.

## Run the Docker cluster and dashboard

Build the dashboard image and start all services:

```bash
docker compose build
docker compose up -d
docker compose ps
```

The first build pulls several large Hadoop/Spark images and may take time. The Compose services include the NameNode, ResourceManager, two DataNodes, two NodeManagers, Spark master, two Spark workers, and Streamlit dashboard.

Useful local pages:

| Service | Address |
|---|---|
| Streamlit dashboard | <http://localhost:8501> |
| HDFS NameNode | <http://localhost:9870> |
| YARN ResourceManager | <http://localhost:8088> |
| Spark master | <http://localhost:8080> |

## Download data and upload it to HDFS

The upload command downloads the source CSVs into the ignored `dataset/` directory and uploads them into HDFS. It requires Python and the project dependencies on the host.

**Windows:**

```bat
upload-data.bat
```

**Linux/macOS:**

```bash
bash scripts/cluster/upload-data.sh
```

Confirm that data is stored in HDFS and has replicated blocks:

```bash
docker exec hadoop-master hdfs dfs -ls /covid/input
docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations
docker exec hadoop-master hdfs dfsadmin -report
```

The report should list two live DataNodes. `fsck` block locations show whether HDFS distributed replicas across the workers. The local CSVs are upload sources; mounting the same file into each worker is not the storage mechanism.

## Run distributed analytics

The Spark job reads the HDFS CSV inputs and writes analytics to `/covid/output`. Use Spark standalone mode to send the job to the two Spark worker containers. On Windows, run:

```bat
run-analysis.bat -Mode cluster
```

On Linux/macOS, run:

```bash
SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh
```

The runners default to YARN client mode if no mode is specified. The current Hadoop NodeManager images lack Python 3 for YARN worker execution, so use standalone mode unless those images have been updated. Standalone mode can be selected on Linux/macOS with:

```bash
SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh
```

If you need the YARN client-mode path on Windows PowerShell, invoke the script directly:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\cluster\run-analysis.ps1 -Mode yarn
```

After a successful job, the runner copies Parquet outputs to `output/` for the dashboard. Check the return code and output timestamps; a failed or interrupted Spark run does not refresh the dashboard data.

```bash
docker exec hadoop-master hdfs dfs -ls -R /covid/output
```

## Use Streamlit

The dashboard starts as part of Compose at <http://localhost:8501>. It reads processed MongoDB collections when configured and available, then uses local `output/` Parquet data as a fallback. It does not run the distributed analytics itself.

To run Streamlit directly on the host, create the optional Python environment below and run:

```bash
python -m streamlit run dashboard/app.py
```

## Optional local Python environment

Use this for host-side ingestion scripts, tests, or direct Streamlit. The Docker dashboard installs its own runtime from `requirements-dashboard.txt` during image build.

**Windows PowerShell:**

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

**Linux/macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell blocks environment activation, use `.\.venv\Scripts\python.exe -m pip install -r requirements.txt` and run tools through `.\.venv\Scripts\python.exe` without changing the machine execution policy.

## Verify, stop, and restart

Windows helper scripts are provided at the repository root:

```text
start-cluster.bat    Start Compose services and initialize HDFS directories
verify-cluster.bat   Show containers, DataNode report, and HDFS input
upload-data.bat      Download source CSVs and upload them to HDFS
run-analysis.bat     Submit the Spark analytics job
stop-cluster.bat     Stop Compose services (retains named HDFS volumes)
```

The equivalent Linux/macOS commands are `bash scripts/cluster/start-cluster.sh`, `verify-cluster.sh`, `upload-data.sh`, `run-analysis.sh`, and `stop-cluster.sh` in `scripts/cluster/`.

Stop services without deleting persistent HDFS data:

```bash
docker compose down
```

To remove the cluster's named HDFS volumes and all stored HDFS data (destructive):

```bash
docker compose down --volumes
```

Do not use `--volumes` if you need to retain uploaded data or NameNode metadata.

## Data and metric notes

- Source files are downloaded from [Google COVID-19 Open Data](https://github.com/GoogleCloudPlatform/covid-19-open-data) into `dataset/`; these large generated files are not tracked by Git.
- Processed Spark results are written to HDFS and synchronized into ignored `output/` after successful completion.
- Country and region coverage is derived from the processed source data; aliases or records without a matching processed row can cause counts to differ between source tables and summaries.
- Vaccination values are labeled according to available source/output fields. A legacy field named `total_vaccinated` does not by itself prove whether it counts people or administered doses.
- Regional aggregates are available only where the source includes state/province observations. The source does not provide boundary geometry for every region.

## Troubleshooting

| Problem | What to check |
|---|---|
| Compose reports `.env` missing | Copy `.env.example` to `.env`; an empty Mongo URI is valid for local Parquet mode. |
| Docker daemon connection error | Start Docker Desktop or the Linux Docker service, then retry. |
| Containers are still starting | Run `docker compose ps` and `docker compose logs --tail=100 namenode resourcemanager datanode1 datanode2`. |
| Fewer than two live DataNodes | Check `docker compose ps`, `docker compose logs datanode1 datanode2`, and the HDFS report. |
| HDFS input directory is empty | Run the upload script and inspect downloader output and `dataset/`. |
| Spark job exits nonzero or code 137 | Inspect `docker compose logs spark-master spark-worker1 spark-worker2`; check Docker memory. Do not treat old `output/` files as fresh job output. |
| Dashboard shows no analytics | Confirm processed Parquet files exist under `output/` or configured MongoDB collections contain current results. |
| MongoDB connection fails | Check your URI, database access list, and network access; MongoDB is optional when using Parquet. |

## Project documentation

- [Architecture](docs/architecture.md)
- [Cluster setup and demo flow](docs/cluster-setup.md)
- [Data pipeline](docs/data-pipeline.md)
- [Distributed processing and current verification notes](docs/processing.md)
- [Streamlit integration](docs/streamlit-integration.md)
- [Real screenshot instructions](docs/screenshots/README.md)

## License

This project is for educational purposes as part of a Big Data Analytics course.
