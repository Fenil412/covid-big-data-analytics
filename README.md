# Distributed COVID-19 Big Data Analytics Platform

A distributed Big Data analytics platform for processing historical
COVID-19 data using a 3-node Hadoop HDFS cluster, Apache Spark, and **MongoDB Atlas**.

---

## Project Overview

This project processes **Google COVID-19 Open Data** using a distributed
3-node Hadoop cluster (1 Master + 2 Workers) deployed via Docker.
Analytics results are stored in **MongoDB Atlas** (cloud-hosted NoSQL).

The pipeline covers:
1. **Data Ingestion** — Download CSV data and upload to HDFS
2. **Distributed Processing** — Clean & transform data using PySpark
3. **Analytics** — Compute country-level and daily COVID statistics
4. **Storage** — Store results in MongoDB Atlas collections
5. **Visualization** — Explore results via Jupyter Notebook

---

## Architecture

```
Google COVID-19 Open Data (CSV)
            ↓
    Data Ingestion (Python)
            ↓
   Hadoop HDFS — /covid/raw
            ↓
  3-Node Docker Cluster
  ┌──────────────────────────────────────┐
  │  master   → NameNode + Spark Master  │
  │  worker1  → DataNode + Spark Worker  │
  │  worker2  → DataNode + Spark Worker  │
  └──────────────────────────────────────┘
            ↓
   Apache Spark (PySpark)
   ├── data_cleaner.py
   └── covid_analyzer.py
            ↓
     MongoDB Atlas (Cloud)
   ├── country_summary
   ├── daily_summary
   ├── vaccination_summary
   └── hospitalization_summary
            ↓
  Jupyter Notebook / Reports
```

---

## Team

| Member | Name | GitHub | Role |
|---|---|---|---|
| Member 1 | Fenil Chodvadiya | [Fenil412](https://github.com/Fenil412) | Project Leader / Lead Data Engineer |
| Member 2 | Sarth Narola | [Sarth00718](https://github.com/Sarth00718) | Cluster & Data Ingestion Engineer |
| Member 3 | Aayush Savaliya | [Aayush-235](https://github.com/Aayush-235) | Analytics & NoSQL Engineer |

---

## Dataset

**Google COVID-19 Open Data**
https://github.com/GoogleCloudPlatform/covid-19-open-data

---

## Technologies

| Technology | Purpose |
|---|---|
| Hadoop HDFS 3.3.6 | Distributed storage |
| Apache Spark / PySpark | Distributed data processing |
| **MongoDB Atlas** | Cloud-hosted NoSQL results storage |
| Python 3.10 | Application development |
| Docker / docker-compose | Cluster deployment |
| Jupyter Notebook | Data exploration & visualization |
| GitHub | Version control & collaboration |

---

## Quick Start

### 1. Prerequisites

- [ ] **Docker Desktop** installed and running → https://www.docker.com/products/docker-desktop
- [ ] **Java 8 or Java 11** installed (NOT Java 17+ — PySpark 3.5.x needs Java 8/11) → https://adoptium.net/temurin/releases/?version=11
- [ ] **Python 3.10+** installed → https://www.python.org/downloads/
- [ ] **Git** installed → https://git-scm.com
- [ ] **MongoDB Atlas account** (free) → https://www.mongodb.com/cloud/atlas/register
- [ ] At least **8 GB RAM** free for Docker (Hadoop cluster)

> **Java Note**: PySpark 3.5.x (in requirements.txt) works with **Java 8 or 11**.
> PySpark 4.x requires Java 17. Check your version: `java -version`

### 2. Clone the repository
```bash
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up MongoDB Atlas

> **This is required before running anything.**

#### Step 1 — Create a free Atlas cluster
1. Go to [MongoDB Atlas](https://cloud.mongodb.com)
2. Sign up / log in
3. Click **"Build a Database"** → choose **Free (M0)**
4. Choose a cloud provider & region → click **"Create"**

#### Step 2 — Create a database user
1. In Atlas → **Database Access** → **Add New Database User**
2. Choose **"Password"** authentication
3. Enter a username and strong password
4. Under **Built-in Role** select **"Read and write to any database"**
5. Click **"Add User"**

#### Step 3 — Whitelist your IP
1. In Atlas → **Network Access** → **Add IP Address**
2. Click **"Allow Access from Anywhere"** (for development) or add your specific IP
3. Click **"Confirm"**

#### Step 4 — Get your connection string
1. In Atlas → your cluster → **"Connect"**
2. Choose **"Drivers"**
3. Select **Python** / version **3.6 or later**
4. Copy the connection string — it looks like:
   ```
   mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
   ```

#### Step 5 — Configure your .env
```bash
cp .env.example .env
```
Open `.env` and set:
```env
MONGODB_ATLAS_URI=mongodb+srv://youruser:yourpassword@yourcluster.mongodb.net/covid_analytics?retryWrites=true&w=majority
MONGODB_DATABASE=covid_analytics
```

> ⚠️ **Never commit your `.env` file** — it contains your credentials. It is already listed in `.gitignore`.

### 5. Deploy the Hadoop cluster
```bash
bash scripts/deployment/deploy_cluster.sh
```

### 6. Run data ingestion
```bash
bash scripts/ingestion/run_ingestion.sh
```

### 7. Submit the Spark analytics job
```bash
docker exec hadoop-master spark-submit \
    --master spark://master:7077 \
    /opt/spark/jobs/analytics_job.py
```

### 8. View results in MongoDB Atlas
- Go to your Atlas cluster → **Browse Collections**
- Database: `covid_analytics`
- You will see: `country_summary`, `daily_summary`, `vaccination_summary`, `hospitalization_summary`

### 9. Explore results in Jupyter
```bash
jupyter notebook notebooks/covid_analysis.ipynb
```

---

## Environment Variables

| Variable | Description | Example |
|---|---|---|
| `MONGODB_ATLAS_URI` | MongoDB Atlas connection string | `mongodb+srv://user:pass@cluster.mongodb.net/...` |
| `MONGODB_DATABASE` | Database name | `covid_analytics` |
| `HDFS_NAMENODE_URI` | HDFS NameNode URI | `hdfs://master:9000` |
| `SPARK_MASTER` | Spark master URL | `spark://master:7077` |
| `SPARK_EXECUTOR_MEMORY` | Memory per executor | `2g` |
| `SPARK_EXECUTOR_CORES` | CPU cores per executor | `2` |

See [`.env.example`](.env.example) for all variables.

---

## Project Structure

```
covid-big-data-analytics/
├── .env.example             ← Copy to .env and fill your Atlas URI
├── .env                     ← Your secrets (gitignored — never commit!)
├── docker-compose.yml       ← 3-node Hadoop cluster (no local MongoDB)
├── requirements.txt
├── config/
│   ├── hadoop/              ← core-site.xml, hdfs-site.xml
│   └── spark/               ← spark-defaults.conf
├── scripts/
│   ├── deployment/          ← deploy_cluster.sh
│   ├── hdfs/                ← setup_hdfs_dirs.sh
│   ├── cluster/             ← start_cluster.sh, stop_cluster.sh
│   └── ingestion/           ← run_ingestion.sh
├── src/
│   ├── ingestion/           ← data_downloader.py, hdfs_uploader.py
│   ├── processing/          ← data_cleaner.py
│   ├── analytics/           ← covid_analyzer.py
│   └── mongodb/             ← mongo_loader.py (Atlas)
├── spark/
│   ├── jobs/                ← analytics_job.py
│   └── utils/               ← spark_session.py
├── notebooks/               ← covid_analysis.ipynb
├── tests/                   ← pytest test suite
├── dataset/                 ← Raw CSV data (gitignored)
└── output/                  ← Pipeline output (gitignored)
```

---

## Running Tests

```bash
pytest tests/ -v
```

> Note: MongoDB tests use mocking — no Atlas connection needed for tests.