# Distributed COVID-19 Big Data Analytics Platform

A distributed Big Data analytics platform for processing historical
COVID-19 data using a 3-node Hadoop HDFS cluster, Apache Spark, and MongoDB.

---

## Project Overview

This project processes **Google COVID-19 Open Data** using a distributed
3-node Hadoop cluster (1 Master + 2 Workers) deployed via Docker.

The pipeline covers:
1. **Data Ingestion** — Download CSV data and upload to HDFS
2. **Distributed Processing** — Clean & transform data using PySpark
3. **Analytics** — Compute country-level and daily COVID statistics
4. **Storage** — Store results in MongoDB collections
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
      MongoDB (NoSQL)
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
| MongoDB 6.0 | NoSQL results storage |
| Python 3.10 | Application development |
| Docker / docker-compose | Cluster deployment |
| Jupyter Notebook | Data exploration & visualization |
| GitHub | Version control & collaboration |

---

## Quick Start

### 1. Prerequisites
- Docker Desktop installed and running
- Python 3.10+
- pip

### 2. Clone the repository
```bash
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Deploy the cluster
```bash
bash scripts/deployment/deploy_cluster.sh
```

### 5. Run data ingestion
```bash
bash scripts/ingestion/run_ingestion.sh
```

### 6. Submit the Spark analytics job
```bash
docker exec hadoop-master spark-submit \
    --master spark://master:7077 \
    /opt/spark/jobs/analytics_job.py
```

### 7. Explore results
Open `notebooks/covid_analysis.ipynb` in Jupyter.

---

## Project Structure

```
covid-big-data-analytics/
├── config/
│   ├── hadoop/          # core-site.xml, hdfs-site.xml
│   ├── spark/           # spark-defaults.conf
│   └── mongodb/         # mongod.conf
├── scripts/
│   ├── deployment/      # deploy_cluster.sh
│   ├── hdfs/            # setup_hdfs_dirs.sh
│   ├── cluster/         # start_cluster.sh, stop_cluster.sh
│   └── ingestion/       # run_ingestion.sh
├── src/
│   ├── ingestion/       # data_downloader.py, hdfs_uploader.py
│   ├── processing/      # data_cleaner.py
│   ├── analytics/       # covid_analyzer.py
│   └── mongodb/         # mongo_loader.py
├── spark/
│   ├── jobs/            # analytics_job.py
│   └── utils/           # spark_session.py
├── notebooks/           # covid_analysis.ipynb
├── tests/               # pytest test suite
├── dataset/             # Raw CSV data (gitignored)
├── output/              # Pipeline output (gitignored)
├── docker-compose.yml
└── requirements.txt
```