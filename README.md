# Distributed COVID-19 Big Data Analytics Platform

A distributed big data analytics platform that processes **12.5 million** real-world COVID-19 records using a **3-node Hadoop HDFS cluster**, **Apache Spark (PySpark)**, and stores results in **MongoDB Atlas** (cloud NoSQL). Includes an interactive **Streamlit dashboard** for data visualization.

---

## Table of Contents

- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Database Design (ER Diagram)](#database-design-er-diagram)
- [Class Diagram](#class-diagram)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup and Installation](#setup-and-installation)
- [Running the Application](#running-the-application)
- [Dashboard](#dashboard)
- [Docker Cluster Mode](#docker-cluster-mode)
- [Testing](#testing)
- [Performance Optimizations](#performance-optimizations)
- [Dataset Information](#dataset-information)
- [API and Module Reference](#api-and-module-reference)
- [References](#references)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL DATA SOURCE                                │
│                                                                             │
│              Google COVID-19 Open Data (12.5M+ records, 720MB)              │
│              https://storage.googleapis.com/covid19-open-data/v3            │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                     ┌──────────────▼──────────────┐
                     │     DATA INGESTION LAYER     │
                     │                              │
                     │  data_downloader.py          │
                     │  • HTTP download with retry  │
                     │  • Progress tracking         │
                     │  • 5 CSV files               │
                     └──────────────┬───────────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  │                 │                  │
        ┌─────────▼────────┐  ┌────▼─────┐  ┌────────▼────────┐
        │  LOCAL STORAGE    │  │  HDFS    │  │  HDFS UPLOAD    │
        │  dataset/         │  │  CLUSTER │  │  hdfs_uploader  │
        │  (720 MB CSVs)    │  │          │  │  .py            │
        └─────────┬────────┘  │ ┌──────┐ │  └─────────────────┘
                  │           │ │Master│ │
                  │           │ │(Name │ │
                  │           │ │Node) │ │
                  │           │ └──┬───┘ │
                  │           │ ┌──┼──┐  │
                  │           │ │W1│W2│  │
                  │           │ │DN│DN│  │
                  │           │ └──┴──┘  │
                  │           └────┬─────┘
                  │                │
                  └────────┬───────┘
                           │
              ┌────────────▼────────────┐
              │   SPARK PROCESSING      │
              │   LAYER                 │
              │                         │
              │  ┌───────────────────┐  │
              │  │  data_cleaner.py  │  │
              │  │  • Drop nulls     │  │
              │  │  • Cast types     │  │
              │  │  • Fix negatives  │  │
              │  │  • Dedup rows     │  │
              │  │  • Parse dates    │  │
              │  └────────┬──────────┘  │
              │           │             │
              │  ┌────────▼──────────┐  │
              │  │ covid_analyzer.py │  │
              │  │ • country_summary │  │
              │  │ • daily_summary   │  │
              │  │ • vaccination     │  │
              │  │ • hospitalization │  │
              │  │ • Window funcs    │  │
              │  │ • GroupBy + Agg   │  │
              │  └────────┬──────────┘  │
              │           │             │
              │  Spark Master ──────┐   │
              │    ├── Worker 1     │   │
              │    └── Worker 2     │   │
              └────────────┬────────┘   │
                           │
              ┌────────────▼────────────┐
              │   STORAGE LAYER         │
              │                         │
              │  ┌───────────────────┐  │
              │  │  MongoDB Atlas    │  │
              │  │  (Cloud NoSQL)    │  │
              │  │                   │  │
              │  │  mongo_loader.py  │  │
              │  │  • insert_many()  │  │
              │  │  • 1,469 docs     │  │
              │  └────────┬──────────┘  │
              │           │             │
              │  ┌────────▼──────────┐  │
              │  │ Local Parquet     │  │
              │  │ output/           │  │
              │  └───────────────────┘  │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │   PRESENTATION LAYER    │
              │                         │
              │  Streamlit Dashboard    │
              │  • Top 10 countries     │
              │  • Daily trends + 7d MA │
              │  • Vaccination progress │
              │  • Hospitalization      │
              │  • Country explorer     │
              │  • Raw data browser     │
              └─────────────────────────┘
```

---

## Technology Stack

| Technology | Version | Purpose | Layer |
|------------|---------|---------|-------|
| Python | 3.10+ | Application language | All |
| Apache Spark / PySpark | 3.5.3 | Distributed data processing | Processing |
| Apache Hadoop HDFS | 3.2.1 | Distributed file storage | Storage |
| MongoDB Atlas | M0 (Free) | Cloud NoSQL results database | Storage |
| Docker / Docker Compose | Latest | Container orchestration | Infrastructure |
| Streamlit | 1.30+ | Interactive web dashboard | Presentation |
| Plotly | 5.18+ | Interactive charts | Presentation |
| Pandas | 2.0+ | Data manipulation | Processing |
| pymongo | 4.6+ | MongoDB Python driver | Storage |
| pytest | 7.4+ | Unit testing framework | Testing |
| Java (OpenJDK) | 8 or 11 | JVM runtime for Spark/Hadoop | Runtime |
| Git / GitHub | Latest | Version control | DevOps |

---

## Database Design (ER Diagram)

```
MongoDB Atlas — Database: covid_analytics
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────┐
│                  country_summary                     │
│                  (232 documents)                     │
├─────────────────────────────────────────────────────┤
│  _id                    : ObjectId (PK)             │
│  location_key           : String   (e.g. "US")      │
│  country_name           : String   (e.g. "United…") │
│  total_confirmed        : Long     (sum of cases)   │
│  total_deceased         : Long     (sum of deaths)  │
│  total_recovered        : Long     (sum recovered)  │
│  total_vaccinated       : Long     (sum vaccinated) │
│  peak_cumulative_conf   : Long     (max cumulative) │
│  peak_cumulative_dec    : Long     (max deaths)     │
│  peak_daily_confirmed   : Long     (worst single day│
│  days_reported          : Long     (count of days)  │
│  case_fatality_rate     : Double   (deaths/cases %) │
└─────────────────────────────────────────────────────┘
         │ location_key
         │
┌────────▼────────────────────────────────────────────┐
│                  daily_summary                       │
│                  (990 documents)                     │
├─────────────────────────────────────────────────────┤
│  _id                    : ObjectId (PK)             │
│  date                   : Date     (yyyy-MM-dd)     │
│  global_new_confirmed   : Long     (world daily)    │
│  global_new_deceased    : Long     (world deaths)   │
│  global_new_recovered   : Long     (world recovered)│
│  rolling_avg_7d         : Double   (7-day MA)       │
│  rolling_avg_deaths_7d  : Double   (7-day MA death) │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│              vaccination_summary                     │
│              (232 documents)                         │
├─────────────────────────────────────────────────────┤
│  _id                    : ObjectId (PK)             │
│  location_key           : String                    │
│  country_name           : String                    │
│  total_vaccinated       : Long                      │
│  cumulative_vaccinated  : Long     (max cumulative) │
│  peak_daily_vaccinated  : Long     (best single day)│
│  vaccination_days       : Long     (reporting days) │
└─────────────────────────────────────────────────────┘
         │ location_key
         │
┌────────▼────────────────────────────────────────────┐
│           hospitalization_summary                    │
│           (15 documents)                             │
├─────────────────────────────────────────────────────┤
│  _id                    : ObjectId (PK)             │
│  location_key           : String                    │
│  country_name           : String                    │
│  total_hospitalized     : Long                      │
│  peak_hospitalized      : Long     (worst day)      │
│  avg_daily_hospitalized : Double   (avg burden)     │
│  hospitalization_days   : Long     (reporting days) │
└─────────────────────────────────────────────────────┘

Relationships:
  country_summary.location_key ←→ vaccination_summary.location_key
  country_summary.location_key ←→ hospitalization_summary.location_key
  daily_summary.date = aggregation across all countries per day

Total Documents: 1,469
```

---

## Class Diagram

```
┌────────────────────────────────┐
│         DataDownloader         │
├────────────────────────────────┤
│ - BASE_URL: str                │
│ - DATASET_DIR: Path            │
│ - FILES: list[str]             │
├────────────────────────────────┤
│ + download_all() → list[Path]  │
│ + download_file(name) → Path   │
│ - _progress_bar(current, total)│
└────────────────────────────────┘

┌────────────────────────────────┐
│         HdfsUploader           │
├────────────────────────────────┤
│ - hdfs_uri: str                │
│ - hdfs_path: str               │
├────────────────────────────────┤
│ + upload_all(local_dir) → None │
│ + upload_file(path) → None     │
└────────────────────────────────┘

┌────────────────────────────────┐       ┌────────────────────────────┐
│         DataCleaner            │       │       SparkSession         │
├────────────────────────────────┤       │       (PySpark)            │
│ - spark: SparkSession          │◄──────┤                            │
│ - NUMERIC_COLUMNS: list[str]   │       │ + builder.getOrCreate()    │
├────────────────────────────────┤       └────────────────────────────┘
│ + clean(df) → DataFrame       │                    │
│ - _drop_nulls(df) → DataFrame │                    │
│ - _cast_types(df) → DataFrame │                    │
│ - _fix_negatives(df)→DataFrame│                    │
│ - _parse_dates(df) → DataFrame│                    │
│ - _dedup(df) → DataFrame      │                    ▼
└────────────────────────────────┘       ┌────────────────────────────┐
                                         │      CovidAnalyzer         │
                                         ├────────────────────────────┤
                                         │ - spark: SparkSession      │
                                         ├────────────────────────────┤
┌────────────────────────────────┐       │ + run_all(df) → dict       │
│         MongoLoader            │       │ + country_summary(df)→DF   │
├────────────────────────────────┤       │ + daily_global_summary()→DF│
│ - uri: str                     │       │ + vaccination_summary()→DF │
│ - db_name: str                 │       │ + hospitalization_summary()│
│ - client: MongoClient          │       │   → DataFrame             │
├────────────────────────────────┤       └────────────────────────────┘
│ + load(df, collection) → None  │
│ + connect() → MongoClient      │
│ + close() → None               │
└────────────────────────────────┘

┌────────────────────────────────┐
│      analytics_job.py          │
│      (Entry Point)             │
├────────────────────────────────┤
│ + main()                       │
│ + parse_args() → Namespace     │
│ + run_pipeline(spark, args)    │
│   → orchestrates all modules   │
└────────────────────────────────┘

┌────────────────────────────────┐
│   run_pipeline_local.py        │
│   (One-Command Runner)         │
├────────────────────────────────┤
│ + main()                       │
│   → download + spark + clean   │
│   → analyze + mongo load       │
└────────────────────────────────┘
```

---

## Project Structure

```
covid-big-data-analytics/
│
├── .env.example                  # Environment template (copy to .env)
├── .env                          # Your secrets (NEVER commit)
├── .gitignore                    # Git ignore rules
├── docker-compose.yml            # 6-container cluster definition
├── requirements.txt              # Python dependencies
├── run_pipeline_local.py         # ★ One-command local pipeline runner
├── README.md                     # This file
├── EVALUATION_GUIDE.md           # Evaluation/presentation guide
│
├── config/
│   ├── hadoop/
│   │   ├── core-site.xml         # Hadoop core configuration
│   │   └── hdfs-site.xml         # HDFS replication settings
│   └── spark/
│       └── spark-defaults.conf   # Spark default configuration
│
├── dashboard/
│   └── app.py                    # ★ Streamlit analytics dashboard
│
├── scripts/
│   ├── test_mongo_connection.py  # MongoDB Atlas connection tester
│   ├── cluster/
│   │   ├── start_cluster.sh      # Start Docker cluster
│   │   └── stop_cluster.sh       # Stop Docker cluster
│   ├── hdfs/
│   │   └── setup_hdfs.sh         # HDFS directory setup
│   └── ingestion/
│       └── run_ingestion.sh      # Run data ingestion
│
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── data_downloader.py    # Download Google COVID CSVs
│   │   └── hdfs_uploader.py      # Upload files to HDFS
│   ├── processing/
│   │   ├── __init__.py
│   │   └── data_cleaner.py       # PySpark data cleaning
│   ├── analytics/
│   │   ├── __init__.py
│   │   └── covid_analyzer.py     # ★ PySpark analytics engine
│   └── mongodb/
│       ├── __init__.py
│       └── mongo_loader.py       # Load results to Atlas
│
├── spark/
│   ├── jobs/
│   │   └── analytics_job.py      # Spark job entry point
│   └── utils/
│       └── spark_session.py      # SparkSession factory
│
├── tests/
│   ├── test_analytics.py         # Analytics unit tests (5 tests)
│   ├── test_ingestion.py         # Ingestion tests
│   └── test_mongodb.py           # MongoDB tests
│
├── notebooks/
│   └── covid_analysis.ipynb      # Jupyter notebook
│
├── dataset/                      # Downloaded CSVs (gitignored)
│   ├── epidemiology.csv          # 497 MB, 12.5M rows
│   ├── vaccinations.csv          # 157 MB
│   ├── hospitalizations.csv      # 63 MB
│   ├── demographics.csv          # 1.5 MB
│   └── index.csv                 # 2.3 MB (country names)
│
└── output/                       # Parquet output (gitignored)
    ├── country_summary/
    ├── daily_summary/
    ├── vaccination_summary/
    └── hospitalization_summary/
```

---

## Prerequisites

Install the following before running:

| Software | Version | Download |
|----------|---------|----------|
| Python | 3.10 or 3.11 | https://www.python.org/downloads/ |
| Java (OpenJDK) | 8 or 11 | https://adoptium.net/temurin/releases/?version=11 |
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Git | Latest | https://git-scm.com |

> **Important**: PySpark 3.5.x requires Java 8 or 11. Do NOT use Java 17+.
> Verify: `java -version` should show `1.8.x` or `11.x`

You also need a **MongoDB Atlas** free account: https://www.mongodb.com/cloud/atlas/register

---

## Setup and Installation

### Step 1 — Clone the repository
```bash
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

### Step 2 — Create virtual environment and install dependencies
```bash
# Windows
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Linux / Mac
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 3 — Configure environment variables
```bash
# Windows
copy .env.example .env

# Linux / Mac
cp .env.example .env
```

Edit `.env` and add your MongoDB Atlas connection string:
```env
MONGODB_ATLAS_URI=mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/covid_analytics?retryWrites=true&w=majority
MONGODB_DATABASE=covid_analytics
```

> **How to get your Atlas URI:**
> 1. Go to https://cloud.mongodb.com
> 2. Click your Cluster → Connect → Drivers → Python 3.6+
> 3. Copy the connection string and replace `<password>`
> 4. Under Network Access → Add IP → Allow Access from Anywhere

### Step 4 — Test MongoDB Atlas connection
```bash
python scripts/test_mongo_connection.py
```
Expected: `[ALL CHECKS PASSED] MongoDB Atlas is ready!`

---

## Running the Application

### Option A: Local Mode (Recommended — No Docker needed)

```bash
python run_pipeline_local.py
```

This single command will:
1. Download 5 COVID-19 CSV files (~720 MB) from Google
2. Start PySpark in `local[*]` mode (uses all CPU cores)
3. Filter 12.5M rows → 227K country-level records
4. Run 4 analytics (country, daily, vaccination, hospitalization)
5. Save Parquet output to `output/`
6. Load 1,469 documents into MongoDB Atlas

Expected output:
```
[OK] SparkSession ready | Spark 3.5.3
Raw rows (all regions): 12,525,825
Country-level rows: 227,879 (233 countries)
country_summary: 232 countries
daily_summary: 990 days
vaccination_summary: 232 countries
hospitalization_summary: 15 countries
Total documents in Atlas: 1,469
PIPELINE COMPLETE — Total time: ~60s
```

### Option B: Docker Cluster Mode (Full distributed setup)

See [Docker Cluster Mode](#docker-cluster-mode) section below.

---

## Dashboard

An interactive Streamlit web dashboard for exploring the analytics results.

### Start the dashboard
```bash
streamlit run dashboard/app.py
```
Opens at → **http://localhost:8501**

### Dashboard Features

| Tab | Visualizations |
|-----|---------------|
| 🏆 Top Countries | Top 10 cases/deaths bar charts, Case Fatality Rate |
| 📈 Daily Trends | Global daily cases + 7-day rolling average, deaths trend |
| 💉 Vaccinations | Top 15 vaccinated countries, peak daily vaccination rates |
| 🏥 Hospitalizations | Hospital burden by country, peak vs average charts |
| 🔍 Country Explorer | Compare any countries, radar chart, detailed data table |
| 📋 Raw Data | Browse all collections, search, download as CSV |

The dashboard reads live data from **MongoDB Atlas** — no local data files needed.

---

## Docker Cluster Mode

### Architecture
```
Docker Compose → 6 Containers:
  ├── hadoop-master    (NameNode — HDFS metadata)
  ├── hadoop-worker1   (DataNode 1 — stores data blocks)
  ├── hadoop-worker2   (DataNode 2 — stores data blocks)
  ├── spark-master     (Spark Master — job coordinator)
  ├── spark-worker1    (Spark Worker 1 — task executor)
  └── spark-worker2    (Spark Worker 2 — task executor)
```

### Step 1 — Pull Docker images
```bash
docker pull bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
docker pull bde2020/spark-master:3.3.0-hadoop3.3
docker pull bde2020/spark-worker:3.3.0-hadoop3.3
```

### Step 2 — Start the cluster
```bash
docker-compose up -d
docker ps    # Verify 6 containers are "Up"
```

### Step 3 — Setup HDFS (wait 30 seconds after start)
```bash
docker exec hadoop-master hdfs dfs -mkdir -p /covid/raw /covid/processed /covid/output
docker exec hadoop-master hdfs dfs -chmod -R 777 /covid
```

### Step 4 — Upload data to HDFS
```bash
docker exec hadoop-master hdfs dfs -put /data/raw/epidemiology.csv /covid/raw/
docker exec hadoop-master hdfs dfs -put /data/raw/vaccinations.csv /covid/raw/
docker exec hadoop-master hdfs dfs -put /data/raw/hospitalizations.csv /covid/raw/
docker exec hadoop-master hdfs dfs -put /data/raw/index.csv /covid/raw/
docker exec hadoop-master hdfs dfs -put /data/raw/demographics.csv /covid/raw/

# Verify
docker exec hadoop-master hdfs dfs -ls /covid/raw
```

### Step 5 — Deploy code and install dependencies in container
```bash
docker cp spark/ spark-master:/opt/spark-apps/spark
docker cp src/ spark-master:/opt/spark-apps/src
docker cp .env spark-master:/opt/.env
docker exec spark-master python3 -m pip install python-dotenv pymongo dnspython -q
```

### Step 6 — Run Spark job on cluster
```bash
docker exec spark-master /spark/bin/spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --conf "spark.sql.shuffle.partitions=4" \
  /opt/spark-apps/spark/jobs/analytics_job.py \
  --mode local \
  --input-path hdfs://master:9000/covid/raw \
  --output-path hdfs://master:9000/covid/output
```

### Step 7 — Web UIs
| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |
| Dashboard | http://localhost:8501 |
| MongoDB Atlas | https://cloud.mongodb.com |

### Stop the cluster
```bash
docker-compose down
```

---

## Testing

Run all unit tests:
```bash
# Set PySpark Python path (Windows)
$env:PYSPARK_PYTHON="venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="venv\Scripts\python.exe"

# Run tests
python -m pytest tests/test_analytics.py -v
```

### Test Coverage

| Test | What it Verifies |
|------|-----------------|
| `test_country_summary_has_correct_columns` | Output schema contains expected columns |
| `test_country_summary_row_count` | One row per country (3 test countries → 3 rows) |
| `test_daily_summary_has_correct_columns` | Date-level aggregation columns exist |
| `test_run_all_returns_dict` | `run_all()` returns dict of DataFrames |
| `test_no_negative_confirmed_after_cleaning` | Data cleaner removes negative values |

---

## Performance Optimizations

| Optimization | Implementation | Impact |
|-------------|----------------|--------|
| Country-level filtering | `F.length(location_key) == 2` | 12.5M → 227K rows (98% reduction) |
| Broadcast join | `F.broadcast(idx_df)` for index.csv | Avoids shuffle for small lookup table |
| Disk spillover | `spark.memory.offHeap.enabled=true` | Prevents OOM on large aggregations |
| Driver memory tuning | `spark.driver.memory=3g` | Accommodates large DataFrame operations |
| Shuffle partitions | `spark.sql.shuffle.partitions=4` | Optimized for local mode (default 200 is too high) |
| DataFrame caching | `clean_df.cache()` | Avoids recomputation across 4 analytics |
| Batch MongoDB insert | `insert_many()` per collection | Faster than document-by-document insertion |

---

## Dataset Information

**Source**: [Google COVID-19 Open Data](https://github.com/GoogleCloudPlatform/covid-19-open-data)

| File | Size | Records | Description |
|------|------|---------|-------------|
| `epidemiology.csv` | 497 MB | 12,525,825 | Daily cases, deaths, recoveries per region |
| `vaccinations.csv` | 157 MB | - | Vaccination counts per region |
| `hospitalizations.csv` | 63 MB | - | Hospital admissions per region |
| `demographics.csv` | 1.5 MB | - | Population statistics |
| `index.csv` | 2.3 MB | - | Location key to country name mapping |

### Analytics Output

| Collection | Documents | Description |
|-----------|-----------|-------------|
| `country_summary` | 232 | Total cases, deaths, CFR per country |
| `daily_summary` | 990 | Global daily trend with 7-day rolling average |
| `vaccination_summary` | 232 | Vaccination progress per country |
| `hospitalization_summary` | 15 | Hospital burden (only 15 countries report) |
| **Total** | **1,469** | All stored in MongoDB Atlas |

---

## API and Module Reference

### `src/ingestion/data_downloader.py`
Downloads COVID-19 CSV files from Google Cloud Storage.
```python
from src.ingestion.data_downloader import DataDownloader
downloader = DataDownloader()
files = downloader.download_all()  # Returns list of file paths
```

### `src/processing/data_cleaner.py`
Cleans raw PySpark DataFrames.
```python
from src.processing.data_cleaner import DataCleaner
cleaner = DataCleaner(spark)
clean_df = cleaner.clean(raw_df)
```

### `src/analytics/covid_analyzer.py`
Runs all 4 COVID-19 analytics using PySpark.
```python
from src.analytics.covid_analyzer import CovidAnalyzer
analyzer = CovidAnalyzer(spark)
results = analyzer.run_all(clean_df)  # Returns dict of DataFrames
# results["country_summary"], results["daily_summary"], etc.
```

### `src/mongodb/mongo_loader.py`
Loads PySpark DataFrames into MongoDB Atlas.
```python
from src.mongodb.mongo_loader import MongoLoader
loader = MongoLoader()
loader.load(df, collection="country_summary")
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `UnsupportedClassVersionError` | Wrong Java version. Install Java 11 from Adoptium |
| `SparkOutOfMemoryError` | Close other apps, use `run_pipeline_local.py` |
| `MONGODB_ATLAS_URI not set` | Check `.env` file has the URI filled in |
| Atlas connection timeout | Atlas → Network Access → Add `0.0.0.0/0` |
| Docker TLS timeout | Pull images one by one (they resume) |
| `docker: not running` | Open Docker Desktop, wait for green icon |
| Tests fail with `SocketException` | Set `PYSPARK_PYTHON` to venv Python path |

---

## References

1. **Google COVID-19 Open Data**
   - Repository: https://github.com/GoogleCloudPlatform/covid-19-open-data
   - Data: https://storage.googleapis.com/covid19-open-data/v3/

2. **Apache Spark Documentation**
   - Official: https://spark.apache.org/docs/3.5.3/
   - PySpark API: https://spark.apache.org/docs/3.5.3/api/python/
   - DataFrame Guide: https://spark.apache.org/docs/3.5.3/sql-programming-guide.html

3. **Apache Hadoop HDFS**
   - Architecture: https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
   - Commands: https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-common/FileSystemShell.html

4. **MongoDB Atlas**
   - Documentation: https://www.mongodb.com/docs/atlas/
   - PyMongo Driver: https://pymongo.readthedocs.io/en/stable/
   - Aggregation: https://www.mongodb.com/docs/manual/aggregation/

5. **Docker**
   - Docker Compose: https://docs.docker.com/compose/
   - BDE2020 Hadoop Images: https://hub.docker.com/u/bde2020
   - Networking: https://docs.docker.com/network/

6. **Streamlit**
   - Documentation: https://docs.streamlit.io/
   - API Reference: https://docs.streamlit.io/library/api-reference

7. **Plotly**
   - Python Graphing: https://plotly.com/python/
   - Express API: https://plotly.com/python/plotly-express/

8. **Python Libraries**
   - pandas: https://pandas.pydata.org/docs/
   - python-dotenv: https://pypi.org/project/python-dotenv/
   - dnspython: https://www.dnspython.org/

---

## License

This project is for educational purposes as part of the Big Data Analytics course.