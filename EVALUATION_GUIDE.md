# COVID-19 Big Data Analytics Platform — Evaluation Guide

> **Use this file during your project evaluation/presentation.**
> It explains what each technology does, where it's used in the code, and what to show the evaluator.

---

## 📌 Project Summary (30-second pitch)

> "We built a **Distributed Big Data Analytics Platform** that downloads real-world COVID-19 data
> (12.5 million records), processes it using **Apache Spark** on a **3-node Hadoop HDFS cluster**
> running in **Docker**, performs analytics like country-wise case summaries, daily trends,
> vaccination progress, and hospitalization burden, and stores all results in **MongoDB Atlas**
> (cloud NoSQL database). The entire pipeline runs with a single command."

---

## 👥 Team and Work Distribution

| Member | Role | % | What They Did |
|--------|------|---|---------------|
| **Fenil Chodvadiya** (Lead) | Lead Data Engineer | 50% | Project architecture, Spark pipeline, Docker cluster setup, HDFS config, local pipeline runner, README |
| **Sarth Narola** | Cluster and Ingestion | 25% | Data downloader, HDFS uploader, docker-compose, requirements.txt, ingestion scripts |
| **Aayush Savaliya** | Analytics and NoSQL | 25% | PySpark analytics engine (4 analytics), MongoDB Atlas loader, data cleaner, bug fixes |

**Show evaluator**: Run `git log --oneline --format="%h %an | %s" -15` to show all commits with different authors.

---

## 🏗️ Architecture Diagram (Draw or Show This)

```
┌──────────────────────────────────────────────────────────────────┐
│                    DATA SOURCE (Internet)                        │
│         Google COVID-19 Open Data (12.5M rows, 720MB)           │
└──────────────────────┬───────────────────────────────────────────┘
                       │ Python requests (data_downloader.py)
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                    LOCAL STORAGE (dataset/)                       │
│   epidemiology.csv | vaccinations.csv | hospitalizations.csv     │
│   demographics.csv | index.csv                                   │
└──────────┬───────────────────────────────┬───────────────────────┘
           │                               │
     LOCAL MODE                     DOCKER CLUSTER
           │                               │
           ▼                               ▼
  PySpark Local[*]          ┌─────────────────────────────┐
  (run_pipeline_            │  3-Node Hadoop HDFS         │
   local.py)                │  NameNode + 2 DataNodes     │
                            │                             │
                            │  Spark Master + 2 Workers   │
                            └──────────────┬──────────────┘
                                           │
          ┌────────────────────────────────┘
          ▼
┌──────────────────────────────────────────────────────────────────┐
│              ANALYTICS ENGINE (PySpark)                          │
│  data_cleaner.py  →  covid_analyzer.py                          │
│  • Drop nulls        • country_summary (232 countries)          │
│  • Cast types         • daily_summary (990 days)                │
│  • Fix negatives      • vaccination_summary (232 countries)     │
│  • Dedup rows         • hospitalization_summary (15 countries)  │
└──────────────────────────┬───────────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                 MongoDB Atlas (Cloud)                             │
│  Database: covid_analytics                                       │
│  4 collections | 1,469 total documents                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Technologies and Tools — Where Each Is Used

### 1. Python 3.10+

| What | Where in Code | What to Explain |
|------|--------------|-----------------|
| Main programming language | All `.py` files | "Entire application is written in Python" |
| Virtual environment | `venv/` | "Isolated dependencies using python -m venv" |
| Package management | `requirements.txt` | "All dependencies listed with pinned versions" |

---

### 2. Apache Spark (PySpark 3.5.3)

| What | Where in Code | What to Explain |
|------|--------------|-----------------|
| Distributed data processing | `src/analytics/covid_analyzer.py` | "Spark processes 12.5M rows using distributed computing" |
| SparkSession creation | `spark/utils/spark_session.py` | "Factory pattern to create Spark sessions in local or cluster mode" |
| DataFrame API | `covid_analyzer.py` lines 40-135 | "We use Spark DataFrame API for groupBy, aggregations, window functions" |
| Window functions | `covid_analyzer.py` lines 83-92 | "7-day rolling average using Window.orderBy().rowsBetween(-6, 0)" |
| Local mode | `run_pipeline_local.py` | "Runs Spark on single machine using local[*] — uses all CPU cores" |
| Cluster mode | `spark/jobs/analytics_job.py` | "Submits job to Spark cluster via spark-submit" |

**How to explain to evaluator:**
- Open `covid_analyzer.py` and point to:
  - `groupBy("country_name")` — This is like MapReduce aggregation
  - `F.sum()`, `F.max()`, `F.avg()` — These are distributed aggregate functions
  - `Window.orderBy("date").rowsBetween(-6, 0)` — Sliding window for 7-day rolling average
- Say: "Spark splits the data into partitions, each partition is processed on a separate core/worker in parallel, and results are collected back"

---

### 3. Apache Hadoop HDFS

| What | Where in Code | What to Explain |
|------|--------------|-----------------|
| Distributed file system | `docker-compose.yml` (namenode + 2 datanodes) | "Data is replicated across 2 DataNodes for fault tolerance" |
| HDFS data upload | `src/ingestion/hdfs_uploader.py` | "Uploads CSV files from local to HDFS cluster" |
| HDFS paths | `analytics_job.py` lines 45-49 | "Spark reads from hdfs://master:9000/covid/raw" |
| Replication factor | `docker-compose.yml` line 35 | "dfs.replication: 2 — each file stored on 2 nodes" |
| Web UI | http://localhost:9870 | "Browse file system, see block distribution, node health" |

**How to show evaluator:**
1. Open http://localhost:9870
2. Click Utilities → Browse File System → `/covid/raw/`
3. Show the 5 CSV files with their sizes
4. Click on `epidemiology.csv` → show "Block Information" → data split across DataNodes
5. Say: "If one DataNode crashes, the data is still available from the other node"

---

### 4. Docker and Docker Compose

| What | Where in Code | What to Explain |
|------|--------------|-----------------|
| Container orchestration | `docker-compose.yml` | "6 containers defined: 3 Hadoop + 3 Spark" |
| NameNode container | `hadoop-master` | "HDFS metadata server, manages file system namespace" |
| DataNode containers | `hadoop-worker1`, `hadoop-worker2` | "Store actual data blocks" |
| Spark Master | `spark-master` | "Coordinates Spark jobs, assigns tasks to workers" |
| Spark Workers | `spark-worker1`, `spark-worker2` | "Execute tasks assigned by master" |
| Network isolation | `hadoop_net` bridge network | "All containers communicate on internal Docker network" |
| Volume persistence | `hadoop_namenode`, `hadoop_datanode1/2` | "Data persists even when containers restart" |

**How to show evaluator:**
1. Run `docker ps` → show all 6 containers running
2. Open Docker Desktop → Containers tab → show the cluster group
3. Open Docker Desktop → Images tab → show all bde2020 images
4. Open `docker-compose.yml` → explain each service

---

### 5. MongoDB Atlas (Cloud NoSQL)

| What | Where in Code | What to Explain |
|------|--------------|-----------------|
| Cloud database | `.env` with `MONGODB_ATLAS_URI` | "MongoDB hosted on cloud — no local installation needed" |
| Connection | `src/mongodb/mongo_loader.py` | "Uses pymongo driver with SRV connection string" |
| Data loading | `mongo_loader.py` load() method | "Converts Spark DataFrame to Python dicts, inserts via insert_many()" |
| Connection test | `scripts/test_mongo_connection.py` | "Verifies Atlas connectivity before running pipeline" |
| 4 collections | `covid_analytics` database | "Each analytics result stored as separate collection" |

**How to show evaluator:**
1. Open https://cloud.mongodb.com
2. Click Cluster0 → Browse Collections → `covid_analytics`
3. Click `country_summary` → show a document (JSON) → explain each field
4. Click `daily_summary` → show date-wise trend data
5. Use Filter: `{"country_name": "India"}` → show India-specific data
6. Say: "Any team member can access this data from anywhere — it's on the cloud"

---

### 6. Git and GitHub

| What | Where | What to Explain |
|------|-------|-----------------|
| Version control | `.git/` | "All code versioned with meaningful commit messages" |
| Branch strategy | `feature/*` branches merged to `main` | "Each member worked on feature branches" |
| Author attribution | `--author` flag on commits | "Each commit attributed to correct team member" |
| Remote repo | github.com/Fenil412/covid-big-data-analytics | "Code hosted on GitHub for collaboration" |

**Show evaluator**: Run `git log --oneline --format="%h %an | %s" -15`

---

### 7. Jupyter Notebook

| What | Where in Code | What to Explain |
|------|--------------|-----------------|
| Data visualization | `notebooks/covid_analysis.ipynb` | "Interactive charts and graphs" |
| Libraries used | matplotlib, plotly, pandas | "Rich visualizations of analytics results" |

---

### 8. Java 8 (OpenJDK Temurin)

| What | Why | What to Explain |
|------|-----|-----------------|
| JVM runtime | Required by Spark and Hadoop | "Spark runs on JVM — PySpark 3.5.x needs Java 8 or 11" |
| Check version | `java -version` shows 1.8.0 | "We pinned PySpark 3.5.3 because 4.x needs Java 17" |

---

## 📊 The 4 Analytics — What Each Computes

### 1. Country Summary (232 documents)
```
Grouped by: country_name, location_key
Computes:
  - total_confirmed     → SUM of daily new cases
  - total_deceased      → SUM of daily deaths
  - total_vaccinated    → SUM of vaccinations
  - peak_cumulative     → MAX cumulative cases (peak)
  - days_reported       → COUNT of reporting days
  - case_fatality_rate  → (deaths / cases) x 100
```
Say: "This gives a bird's-eye view of each country's COVID impact"

### 2. Daily Global Summary (990 documents)
```
Grouped by: date
Computes:
  - global_new_confirmed  → SUM all countries daily cases
  - global_new_deceased   → SUM all countries daily deaths
  - rolling_avg_7d        → 7-day moving average (Window function)
```
Say: "Shows the global pandemic trend over 990 days with smoothed 7-day average"

### 3. Vaccination Summary (232 documents)
```
Grouped by: country_name, location_key
Computes:
  - total_vaccinated         → SUM of daily vaccinations
  - cumulative_vaccinated    → MAX cumulative vaccines given
  - peak_daily_vaccinated    → Highest single-day vaccination count
```
Say: "Tracks vaccination progress per country"

### 4. Hospitalization Summary (15 documents)
```
Grouped by: country_name, location_key
Computes:
  - total_hospitalized      → SUM hospital admissions
  - peak_hospitalized       → Worst single-day admissions
  - avg_daily_hospitalized  → Average daily burden
```
Say: "Only 15 countries report hospitalization data — shows healthcare system burden"

---

## 🎯 Live Demo Script (Follow This Order)

### Demo 1: Show the Code Structure (2 min)
Open VS Code and show the project tree:
```
src/ingestion/     → "Data download from Google"
src/processing/    → "Spark data cleaning"
src/analytics/     → "Spark analytics engine — the core"
src/mongodb/       → "MongoDB Atlas loader"
spark/jobs/        → "Spark job entry point"
docker-compose.yml → "6-container cluster definition"
```

### Demo 2: Show Docker Cluster (2 min)
Run in terminal:
```powershell
docker ps
```
Say: "6 containers: 3 Hadoop (1 NameNode + 2 DataNodes) + 3 Spark (1 Master + 2 Workers)"

Open browser tabs:
- http://localhost:9870 → "HDFS NameNode UI — cluster health, file browser"
- http://localhost:8080 → "Spark Master UI — shows 2 workers connected"

### Demo 3: Run the Pipeline (3 min)
```powershell
venv\Scripts\activate
python run_pipeline_local.py
```
Say: "Watch — it downloads data, starts Spark, cleans 12.5M rows, runs 4 analytics, loads to Atlas"

Expected output to point at:
```
Raw rows (all regions): 12,525,825
Country-level rows: 227,879 (233 countries)
country_summary: 232 countries
daily_summary: 990 days
vaccination_summary: 232 countries
hospitalization_summary: 15 countries
Total documents in Atlas: 1,469
PIPELINE COMPLETE — Total time: ~60s
```

### Demo 4: Show MongoDB Atlas Results (3 min)
Open https://cloud.mongodb.com → Browse Collections → `covid_analytics`

1. Click `country_summary` → show a document:
```json
{
  "country_name": "United States of America",
  "total_confirmed": 103436829,
  "total_deceased": 1127152,
  "case_fatality_rate": 1.0897,
  "days_reported": 990
}
```
2. Click `daily_summary` → show latest dates
3. Filter: `{"country_name": "India"}` → show India data
4. Say: "1,469 documents stored in cloud — accessible from anywhere"

### Demo 5: Show Git History (1 min)
```powershell
git log --oneline --format="%h %an | %s" -15
```
Say: "All 3 team members contributed — Fenil (infrastructure), Sarth (ingestion), Aayush (analytics)"

---

## ❓ Expected Evaluator Questions and Answers

### Q: "Why Spark instead of Pandas?"
> "Our dataset has 12.5 million rows (720MB). Pandas loads everything into RAM and is
> single-threaded. Spark distributes processing across multiple cores/nodes and can handle
> datasets much larger than available memory. We actually hit OutOfMemory errors until we
> used Spark's distributed aggregation."

### Q: "Why MongoDB Atlas instead of local MongoDB?"
> "Atlas is a managed cloud service — no installation needed, automatic backups, accessible
> from anywhere. For a big data project, the results need to be queryable by any team member
> without setting up a local database."

### Q: "Why Docker?"
> "We need a 3-node Hadoop cluster (NameNode + 2 DataNodes) plus Spark Master + Workers.
> Docker lets us simulate a multi-node distributed cluster on a single laptop. Without Docker,
> we would need 6 physical machines."

### Q: "What is HDFS replication?"
> "Our replication factor is 2. Every file block is stored on 2 different DataNodes. If one
> node goes down, the data is still available from the other node. This is how Hadoop provides
> fault tolerance."

### Q: "What is the 7-day rolling average?"
> "Daily COVID numbers are noisy — weekends report less. The 7-day rolling average smooths
> the trend by averaging the last 7 days. We implemented this using Spark Window function:
> Window.orderBy('date').rowsBetween(-6, 0) — this is a sliding window calculation."

### Q: "How does spark-submit work?"
> "spark-submit sends our Python job to the Spark cluster. The master distributes tasks to
> workers. Each worker processes a partition of the data in parallel. Results are collected
> back to the driver and written to HDFS and MongoDB."

### Q: "What happens if a worker crashes?"
> "Spark automatically re-runs failed tasks on other workers. The data is in HDFS which has
> replication, so no data is lost. This is the key benefit of distributed computing."

### Q: "Explain the data flow end-to-end"
> "Google Cloud Storage → Python downloads CSVs → Local disk → Uploaded to HDFS (replicated
> across 2 DataNodes) → Spark reads from HDFS → Cleans data (drop nulls, fix negatives) →
> Runs 4 analytics (groupBy + aggregations) → Writes Parquet to HDFS → Loads JSON documents
> to MongoDB Atlas cloud database"

### Q: "What is the difference between local mode and cluster mode?"
> "Local mode runs Spark on a single machine using all CPU cores — good for development and
> testing. Cluster mode distributes work across multiple machines (our 2 Spark workers) —
> this is how production big data systems work. Our code supports both modes."

---

## 📁 Key Files to Open During Evaluation

| File | What to Show | Tech Demonstrated |
|------|-------------|-------------------|
| `docker-compose.yml` | 6 service definitions | Docker, Hadoop, Spark |
| `src/analytics/covid_analyzer.py` | groupBy, agg, Window functions | PySpark |
| `src/mongodb/mongo_loader.py` | Atlas connection, insert_many | MongoDB, pymongo |
| `src/ingestion/data_downloader.py` | HTTP download with progress | Python, requests |
| `spark/jobs/analytics_job.py` | Pipeline orchestration | Spark submit, HDFS |
| `run_pipeline_local.py` | One-command full pipeline | End-to-end integration |
| `requirements.txt` | All dependencies pinned | Python packaging |
| `.env.example` | Configuration template | Security best practices |

---

## 🖥️ Quick Commands Cheat Sheet

```powershell
# Activate environment
venv\Scripts\activate

# Test MongoDB Atlas
python scripts/test_mongo_connection.py

# Run full pipeline (local mode — 60 seconds)
python run_pipeline_local.py

# Docker cluster
docker-compose up -d          # Start cluster
docker ps                     # Show 6 containers
docker-compose down           # Stop cluster

# HDFS commands
docker exec hadoop-master hdfs dfs -ls /covid/raw
docker exec hadoop-master hdfs dfs -du -h /covid/raw

# Git history
git log --oneline --format="%h %an | %s" -15

# Web UIs
# HDFS:  http://localhost:9870
# Spark: http://localhost:8080
# Atlas: https://cloud.mongodb.com
```

---

## ✅ Pre-Evaluation Checklist

- [ ] Docker Desktop is open and running (green icon)
- [ ] `docker-compose up -d` done — all 6 containers green
- [ ] `venv\Scripts\activate` done
- [ ] MongoDB Atlas tab open in browser (show collections)
- [ ] HDFS Web UI tab open (http://localhost:9870)
- [ ] Spark Web UI tab open (http://localhost:8080)
- [ ] VS Code open with project files visible
- [ ] Terminal ready for live demo commands
- [ ] This file open for reference
