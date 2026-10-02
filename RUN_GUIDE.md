# COVID-19 Big Data Analytics Platform
## Complete Step-by-Step Software Execution Guide

**Course:** 4CS101ME25 - Big Data Systems  
**Institution:** Nirma University, Institute of Technology  

---

## 📋 Table of Contents
1. [System Prerequisites & Environment Setup](#1-system-prerequisites--environment-setup)
2. [Cloning the Repository & Configuration](#2-cloning-the-repository--configuration)
3. [Virtual Environment Setup (Host Machine)](#3-virtual-environment-setup-host-machine)
4. [Execution Workflow A: Fast Local Pipeline (PySpark Local Mode)](#4-execution-workflow-a-fast-local-pipeline-pyspark-local-mode)
5. [Execution Workflow B: Full Docker Distributed Cluster (HDFS + Spark)](#5-execution-workflow-b-full-docker-distributed-cluster-hdfs--spark)
6. [Interactive Streamlit Analytics Dashboard](#6-interactive-streamlit-analytics-dashboard)
7. [Web User Interfaces & Port Mapping](#7-web-user-interfaces--port-mapping)
8. [Important Command Cheat Sheet (CMD / PowerShell / Bash)](#8-important-command-cheat-sheet-cmd--powershell--bash)
9. [Troubleshooting & FAQs](#9-troubleshooting--faqs)

---

## 1. System Prerequisites & Environment Setup

Before executing any commands, verify that the required software is installed on your computer.

| Software | Minimum Version | Purpose | Check Command |
|---|---|---|---|
| **Git** | 2.30+ | Source code version control | `git --version` |
| **Docker Desktop** | 4.0+ (Compose v2) | Multi-node Hadoop & Spark container cluster | `docker --version`<br>`docker compose version` |
| **Python** | 3.10 or 3.11 | Ingestion, PySpark execution, and Streamlit | `python --version` |
| **Java (JDK)** | OpenJDK 8 or 11 | Required for host-side PySpark execution | `java -version` |

> **System Resource Tip:** Allocate at least 6 GB to 8 GB of RAM to Docker Desktop in **Settings → Resources → Memory** to allow Hadoop NameNode, DataNodes, and Spark executors to run without Out-Of-Memory (OOM) errors.

---

## 2. Cloning the Repository & Configuration

### Step 2.1: Open Command Prompt (CMD) or PowerShell
Open your terminal and navigate to your desired workspace directory.

### Step 2.2: Clone the Project
```cmd
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

### Step 2.3: Configure Environment Variables (`.env`)
The project utilizes a `.env` file to manage configurations and optional database credentials.

**On Windows Command Prompt (CMD):**
```cmd
copy .env.example .env
```

**On Windows PowerShell:**
```powershell
Copy-Item .env.example .env
```

**On Linux / macOS:**
```bash
cp .env.example .env
```

Open `.env` in any text editor (e.g. `notepad .env`):
- If you have a MongoDB Atlas cluster, set your connection URI:  
  `MONGODB_ATLAS_URI=mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority`
- If you **do not** wish to use MongoDB Atlas, leave it empty or leave the defaults; the pipeline will automatically persist all output to local **Parquet** files and the Streamlit dashboard will read directly from the `output/` directory!

---

## 3. Virtual Environment Setup (Host Machine)

Create and activate an isolated Python virtual environment to install the project dependencies.

### Windows (Command Prompt):
```cmd
python -m venv venv
venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Windows (PowerShell):
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```
*(If PowerShell shows execution policy restrictions, run: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`)*

### Linux / macOS:
```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 4. Execution Workflow A: Fast Local Pipeline (PySpark Local Mode)

If you want to run the complete end-to-end Big Data pipeline on your local CPU cores without starting Docker containers, use `run_pipeline_local.py`. This is ideal for quick testing, rapid evaluation, and verifying all transformations.

### Step 4.1: Ensure Virtual Environment is Active
```cmd
venv\Scripts\activate
```

### Step 4.2: (Optional) Test MongoDB Atlas Connectivity
```cmd
python scripts/test_mongo_connection.py
```

### Step 4.3: Execute the End-to-End Pipeline
```cmd
python run_pipeline_local.py
```

### What Happens Behind the Scenes:
1. **Data Download:** Automatically downloads 5 source datasets from Google Cloud Open Data (12.5M raw rows) into `dataset/`.
2. **Data Cleaning:** Casts types, filters nulls, eliminates anomalies and negative values, and cleans `aggregation_level=0` country data using PySpark.
3. **Distributed Analytics Engine:**
   - **Country Summary:** Aggregates confirmed, deceased, case fatality rates across 232 countries.
   - **Daily Global Summary:** Aggregates global daily cases with a 7-day rolling window average.
   - **Vaccination Summary:** Computes vaccination milestones and cumulative doses.
   - **Hospitalization Summary:** Computes daily admissions and peak burden for reporting nations.
4. **Data Persistence:** Exports structured Parquet tables into `output/` and loads collections into MongoDB Atlas (if URI configured).
5. **Execution Time:** ~45 to 60 seconds.

---

## 5. Execution Workflow B: Full Docker Distributed Cluster (HDFS + Spark)

This mode demonstrates true distributed big data architecture with a 6-container cluster simulating a multi-node Hadoop HDFS and Apache Spark standalone environment.

```
┌─────────────────────────────────────────────────────────────┐
│                 DOCKER DISTRIBUTED CLUSTER                  │
│                                                             │
│   [ hadoop-master ]   NameNode (9870) + ResourceManager     │
│   [ hadoop-worker1]   DataNode (9864) + NodeManager         │
│   [ hadoop-worker2]   DataNode (9864) + NodeManager         │
│                                                             │
│   [ spark-master  ]   Spark Master (8080, port 7077)        │
│   [ spark-worker1 ]   Spark Worker (8081)                   │
│   [ spark-worker2 ]   Spark Worker (8081)                   │
│                                                             │
│   [ streamlit-app ]   Dashboard UI (8501)                   │
└─────────────────────────────────────────────────────────────┘
```

### Step 5.1: Build and Launch the Cluster
Run the helper batch script or use `docker compose`:

**Using Root Batch Script:**
```cmd
start-cluster.bat
```

**Or using Docker Compose directly:**
```cmd
docker compose build
docker compose up -d
```

### Step 5.2: Verify Cluster Services & Node Health
Ensure all containers are running healthy:
```cmd
verify-cluster.bat
```

**Manual Verification Commands:**
```cmd
docker ps
docker exec hadoop-master hdfs dfsadmin -report
```
*Expected Output:* Two live DataNodes reported with healthy disk capacity.

### Step 5.3: Ingest Data and Upload to HDFS
Download the Google COVID-19 dataset and upload files directly into the distributed HDFS filesystem:

**Using Root Batch Script:**
```cmd
upload-data.bat
```

**Or using Python directly:**
```cmd
python src/ingestion/data_downloader.py
python src/ingestion/hdfs_uploader.py
```

**Verify Files in HDFS:**
```cmd
docker exec hadoop-master hdfs dfs -ls /covid/input
```

**Check Block Replication Across Both DataNodes:**
```cmd
docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations
```
*Notice that every block has replication factor 2 and resides on both `hadoop-worker1` and `hadoop-worker2`.*

### Step 5.4: Submit Distributed PySpark Job to Spark Cluster
Submit the big data analytics job to the Spark Master container:

**Using Root Batch Script:**
```cmd
run-analysis.bat -Mode cluster
```

**Or using PowerShell Script directly:**
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\cluster\run-analysis.ps1 -Mode cluster
```

### Step 5.5: Verify Output Generated in HDFS
```cmd
docker exec hadoop-master hdfs dfs -ls -R /covid/output
```

### Step 5.6: Stopping the Cluster
When finished, shut down containers while keeping HDFS data intact in Docker persistent volumes:
```cmd
stop-cluster.bat
```
*Or:*
```cmd
docker compose down
```
*(Caution: Do **not** pass `--volumes` unless you wish to permanently delete HDFS storage).*

---

## 6. Interactive Streamlit Analytics Dashboard

The project includes an interactive web dashboard with interactive Plotly graphs, choropleth maps, date-range sliders, and KPI cards.

### Option 1: Access via Docker (Already running with Compose)
Simply open your web browser and navigate to:  
👉 **http://localhost:8501**

### Option 2: Run Streamlit Locally on Host Machine
If running outside Docker:
```cmd
venv\Scripts\activate
streamlit run dashboard/app.py
```
The dashboard will open automatically at **http://localhost:8501**.

---

## 7. Web User Interfaces & Port Mapping

When the Docker stack is running, you can inspect the big data infrastructure using the following web interfaces:

| Service | Web UI URL | Purpose |
|---|---|---|
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | COVID-19 Interactive Visualizations & Analytics |
| **HDFS NameNode Web UI** | [http://localhost:9870](http://localhost:9870) | Browse HDFS Files, Inspect DataNodes & Block Health |
| **Spark Master Web UI** | [http://localhost:8080](http://localhost:8080) | Spark Cluster Status, Active Workers, Executed Jobs |
| **YARN ResourceManager** | [http://localhost:8088](http://localhost:8088) | Hadoop Cluster Resource Allocation & Applications |
| **MongoDB Atlas** | [https://cloud.mongodb.com](https://cloud.mongodb.com) | Cloud NoSQL Collections & Document Inspector |

---

## 8. Important Command Cheat Sheet (CMD / PowerShell / Bash)

### Core Pipeline Execution
| Task | Windows CMD | Windows PowerShell | Linux / macOS |
|---|---|---|---|
| Start Cluster | `start-cluster.bat` | `.\start-cluster.bat` | `bash scripts/cluster/start-cluster.sh` |
| Check Status | `verify-cluster.bat` | `.\verify-cluster.bat` | `bash scripts/cluster/verify-cluster.sh` |
| Upload to HDFS | `upload-data.bat` | `.\upload-data.bat` | `bash scripts/cluster/upload-data.sh` |
| Submit Spark Job | `run-analysis.bat -Mode cluster` | `.\run-analysis.bat -Mode cluster` | `SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh` |
| Stop Cluster | `stop-cluster.bat` | `.\stop-cluster.bat` | `bash scripts/cluster/stop-cluster.sh` |
| Run Fast Local | `python run_pipeline_local.py` | `python run_pipeline_local.py` | `python3 run_pipeline_local.py` |

### Direct HDFS File System Commands (via Docker)
| Action | Command |
|---|---|
| List HDFS root directory | `docker exec hadoop-master hdfs dfs -ls /` |
| List input dataset | `docker exec hadoop-master hdfs dfs -ls /covid/input` |
| Check disk space used | `docker exec hadoop-master hdfs dfs -du -h /covid/input` |
| View first 20 rows of CSV | `docker exec hadoop-master hdfs dfs -cat /covid/input/index.csv \| head -n 20` |
| Check block locations & health | `docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations` |
| View DataNode cluster report | `docker exec hadoop-master hdfs dfsadmin -report` |

### Docker Maintenance Commands
| Action | Command |
|---|---|
| View running containers | `docker ps` |
| View logs of Spark Master | `docker compose logs -f spark-master` |
| View logs of HDFS NameNode | `docker compose logs -f namenode` |
| View logs of Streamlit UI | `docker compose logs -f streamlit` |
| Clean stop without losing data | `docker compose down` |
| Full reset (Deletes HDFS storage) | `docker compose down --volumes` |

---

## 9. Troubleshooting & FAQs

### Q1: `docker compose up` fails with "port already allocated"
**Solution:** Ensure local ports 8501, 9870, 8080, or 8088 are not occupied by existing applications.
```cmd
netstat -ano | findstr :8501
netstat -ano | findstr :9870
```

### Q2: PySpark fails with exit code 137 (OOM - Out of Memory)
**Solution:** Docker ran out of memory while shuffling 12.5M records.
1. Open Docker Desktop → **Settings** → **Resources** → increase memory to 8 GB.
2. Alternatively, run in local mode via `python run_pipeline_local.py` which executes directly using host RAM.

### Q3: `upload-data.bat` fails with connection error to HDFS
**Solution:** NameNode is still in safemode initializing blocks. Wait 15 seconds and re-check:
```cmd
docker exec hadoop-master hdfs dfsadmin -safemode get
```
If safemode is `ON`, manually leave safemode:
```cmd
docker exec hadoop-master hdfs dfsadmin -safemode leave
```

### Q4: Streamlit shows empty graphs
**Solution:** Ensure the pipeline was run at least once so that `output/` directory contains generated Parquet files:
```cmd
dir output\country_summary
```
If missing, run `python run_pipeline_local.py` to populate data.
