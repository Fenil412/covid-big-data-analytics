# Distributed COVID-19 Big Data Analytics Platform

> **Distributed Big Data Analytics** using 3-node Hadoop HDFS, Apache Spark (PySpark), and MongoDB Atlas.

---

## Team

| # | Name | GitHub | Role | Work |
|---|------|--------|------|------|
| 1 | **Fenil Chodvadiya** (Leader) | [Fenil412](https://github.com/Fenil412) | Lead Data Engineer / Infrastructure | 50% |
| 2 | Sarth Narola | [Sarth00718](https://github.com/Sarth00718) | Cluster & Data Ingestion Engineer | 25% |
| 3 | Aayush Savaliya | [Aayush-235](https://github.com/Aayush-235) | Analytics & NoSQL Engineer | 25% |

---

## Architecture

```
Google COVID-19 Open Data (CSV)
            │
     Step 1: Data Download (Python)
            │
     dataset/ (local) ──or── Hadoop HDFS /covid/raw (Docker)
            │
   ┌────────────────────────────────────┐
   │  Apache Spark — PySpark Local[*]  │  ◄── or Spark Cluster (Docker)
   │  · data_cleaner.py                │
   │  · covid_analyzer.py              │
   └────────────────────────────────────┘
            │
   MongoDB Atlas (Cloud)
   ├── country_summary        (232 docs)
   ├── daily_summary          (990 docs)
   ├── vaccination_summary    (232 docs)
   └── hospitalization_summary (15 docs)
            │
   Jupyter Notebook (visualizations)
```

---

## Tech Stack

| Technology | Version | Purpose |
|------------|---------|---------|
| Python | 3.10+ | Application |
| PySpark | 3.5.3 | Distributed analytics |
| Java | 8 / 11 | PySpark runtime |
| MongoDB Atlas | M0 Free | Cloud results storage |
| Hadoop HDFS | 3.2.1 | Distributed file system (Docker) |
| Docker / docker-compose | Latest | Cluster deployment |
| Jupyter Notebook | Latest | Visualization |

---

## Dataset

**Google COVID-19 Open Data** — https://github.com/GoogleCloudPlatform/covid-19-open-data

| File | Size | Contents |
|------|------|----------|
| `epidemiology.csv` | ~497 MB | Daily cases, deaths, recoveries |
| `vaccinations.csv` | ~157 MB | Vaccination data per country |
| `hospitalizations.csv` | ~63 MB | Hospital burden per country |
| `demographics.csv` | ~1.5 MB | Population statistics |
| `index.csv` | ~2.3 MB | Country name lookup |

---

## Prerequisites (Install before starting)

- [ ] **Python 3.10+** → https://www.python.org/downloads/
- [ ] **Java 8 or Java 11** (NOT 17) → https://adoptium.net/temurin/releases/?version=11
  - Check: `java -version` → must show `1.8.x` or `11.x`
- [ ] **Docker Desktop** → https://www.docker.com/products/docker-desktop
- [ ] **Git** → https://git-scm.com
- [ ] **MongoDB Atlas free account** → https://www.mongodb.com/cloud/atlas/register

---

## Complete Setup Guide

### ✅ STEP 1 — Clone the repository

```powershell
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

---

### ✅ STEP 2 — Create Python virtual environment

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

> You will see `(venv)` in your prompt after activation.
> **Note**: PySpark 3.5.3 is pinned in requirements.txt for Java 8/11 compatibility.

---

### ✅ STEP 3 — Configure MongoDB Atlas

#### 3a. Atlas is already set up with these collections:
| Collection | Documents |
|---|---|
| `country_summary` | 232 |
| `daily_summary` | 990 |
| `vaccination_summary` | 232 |
| `hospitalization_summary` | 15 |

#### 3b. Copy `.env.example` to `.env` and fill in your Atlas URI:
```powershell
copy .env.example .env
```
Edit `.env`:
```env
MONGODB_ATLAS_URI=mongodb+srv://username:password@cluster0.xxxxx.mongodb.net/covid_analytics?retryWrites=true&w=majority
MONGODB_DATABASE=covid_analytics
```

#### 3c. Test the connection:
```powershell
python scripts/test_mongo_connection.py
```
✅ Expected: `[ALL CHECKS PASSED] MongoDB Atlas is ready!`

---

### ✅ STEP 4 — Run the full pipeline (Local Mode — No Docker needed)

```powershell
python run_pipeline_local.py
```

**What this does automatically:**
1. Downloads 5 COVID-19 CSV files (~720 MB) to `dataset/`
2. Starts PySpark in `local[*]` mode
3. Filters 12.5M rows → 227K country-level rows
4. Runs 4 analytics jobs (country, daily, vaccination, hospitalization)
5. Saves Parquet output to `output/`
6. Loads **1,469 documents** into MongoDB Atlas

**Expected output:**
```
[OK] SparkSession ready | Spark 3.5.3
[OK] Country-level rows: 227,879 (233 countries)
[OK] country_summary: 232 countries
[OK] daily_summary: 990 days
[OK] vaccination_summary: 232 countries
[OK] hospitalization_summary: 15 countries
[OK] Total documents in Atlas: 1,469
PIPELINE COMPLETE — Total time: ~60s
```

---

### ✅ STEP 5 — View results in MongoDB Atlas

Go to → **https://cloud.mongodb.com**
→ Cluster0 → Browse Collections → **covid_analytics**

| Collection | Documents | Contents |
|---|---|---|
| `country_summary` | 232 | Total cases, deaths, CFR per country |
| `daily_summary` | 990 | Global daily trend + 7-day rolling avg |
| `vaccination_summary` | 232 | Vaccination progress per country |
| `hospitalization_summary` | 15 | Hospital burden per country |

---

### ✅ STEP 6 — Open Jupyter Notebook (visualizations)

```powershell
venv\Scripts\activate
jupyter notebook notebooks/covid_analysis.ipynb
```
Opens in browser at http://localhost:8888

---

## STEP 7 — Docker Cluster (Hadoop + Spark) — Optional

> **This step is only needed if you want to run on the full Hadoop HDFS cluster.**
> The pipeline runs perfectly in local mode (Step 4) without Docker.

### 7a. Pull Docker images first (one by one — large files)
```powershell
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/spark-master:3.3.0-hadoop3.3
docker pull bde2020/spark-worker:3.3.0-hadoop3.3
```
> If Docker Hub is slow or gives TLS error, try again — it resumes from where it stopped.
> You can also pull via Docker Desktop: Images → Search → paste image name → Pull

### 7b. Start the cluster (after all images are downloaded)
```powershell
docker-compose up -d
```

**Verify all 5 containers are running:**
```powershell
docker ps
```
Expected:
```
NAMES            STATUS
hadoop-master    Up
hadoop-worker1   Up
hadoop-worker2   Up
spark-master     Up
spark-worker1    Up
spark-worker2    Up
```

### 7c. Setup HDFS directories (wait 30s after cluster starts)
```powershell
docker exec hadoop-master hdfs dfs -mkdir -p /covid/raw /covid/processed /covid/output /spark-logs
docker exec hadoop-master hdfs dfs -chmod -R 777 /covid
docker exec hadoop-master hdfs dfs -ls /
```

### 7d. Upload data to HDFS
```powershell
# First download data locally (if not done yet)
python src/ingestion/data_downloader.py

# Then upload to HDFS
python src/ingestion/hdfs_uploader.py
```

### 7e. Run Spark job on cluster
```powershell
docker cp src/ spark-master:/opt/spark-apps/src
docker cp spark/ spark-master:/opt/spark-apps/spark
docker cp .env spark-master:/opt/.env

docker exec spark-master spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 1g `
  /opt/spark-apps/spark/jobs/analytics_job.py
```

### 7f. Cluster Web UIs
| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |

### 7g. Stop the cluster
```powershell
docker-compose down
```

---

## Quick Reference — All Commands

```powershell
# Activate venv (always do this first)
venv\Scripts\activate

# Test MongoDB Atlas
python scripts/test_mongo_connection.py

# Run full pipeline (local mode)
python run_pipeline_local.py

# Open Jupyter
jupyter notebook notebooks/covid_analysis.ipynb

# Docker cluster
docker-compose up -d       # start
docker ps                  # check status
docker-compose down        # stop
```

---

## Project Structure

```
covid-big-data-analytics/
│
├── .env.example              ← Copy to .env, fill in Atlas URI
├── .env                      ← Your secrets (NEVER commit)
├── docker-compose.yml        ← 3-node Hadoop + Spark cluster
├── requirements.txt          ← Python deps (pyspark==3.5.3)
├── run_pipeline_local.py     ← ONE-COMMAND local pipeline runner
│
├── config/
│   ├── hadoop/               ← Hadoop core/hdfs config
│   └── spark/                ← Spark defaults
│
├── scripts/
│   ├── test_mongo_connection.py  ← Test Atlas connection
│   ├── cluster/                  ← start/stop scripts
│   ├── hdfs/                     ← HDFS setup scripts
│   └── ingestion/                ← run_ingestion.sh
│
├── src/
│   ├── ingestion/
│   │   ├── data_downloader.py    ← Download Google COVID CSVs
│   │   └── hdfs_uploader.py      ← Upload to HDFS
│   ├── processing/
│   │   └── data_cleaner.py       ← PySpark cleaning
│   ├── analytics/
│   │   └── covid_analyzer.py     ← PySpark analytics
│   └── mongodb/
│       └── mongo_loader.py       ← Load results to Atlas
│
├── spark/
│   ├── jobs/
│   │   └── analytics_job.py      ← Spark job entry point
│   └── utils/
│       └── spark_session.py      ← SparkSession factory
│
├── notebooks/
│   └── covid_analysis.ipynb      ← Jupyter visualizations
│
├── tests/                        ← Unit tests
├── dataset/                      ← Downloaded CSVs (gitignored)
└── output/                       ← Parquet output (gitignored)
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `UnsupportedClassVersionError` (Java) | You have wrong Java version. Install Java 11: https://adoptium.net |
| `SparkOutOfMemoryError` | Close other apps, run `python run_pipeline_local.py` again |
| `MONGODB_ATLAS_URI not set` | Check your `.env` file has the URI filled in |
| Atlas connection timeout | Go to Atlas → Network Access → Add IP → Allow Anywhere (`0.0.0.0/0`) |
| Docker TLS timeout | Pull images one by one: `docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` |
| `docker: not running` | Open Docker Desktop app first, wait for green status icon |