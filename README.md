# COVID-19 Big Data Analytics Platform

> **Distributed COVID-19 data engineering and analytics platform using Hadoop HDFS, Apache Spark/PySpark, MongoDB Atlas, Docker, and Streamlit.**

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.3.0-orange?logo=apachespark)](https://spark.apache.org/)
[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.2.1-yellow?logo=apachehadoop)](https://hadoop.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-Distributed%20Processing-red?logo=apachespark)](https://spark.apache.org/docs/latest/api/python/)
[![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-green?logo=mongodb)](https://www.mongodb.com/atlas)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Problem Statement](#problem-statement)
- [Objectives](#objectives)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [System Architecture](#system-architecture)
- [Distributed Cluster Design](#distributed-cluster-design)
- [End-to-End Data Flow](#end-to-end-data-flow)
- [Data Sources](#data-sources)
- [Data Storage Design](#data-storage-design)
- [HDFS Design](#hdfs-design)
- [Data Cleaning and Transformation](#data-cleaning-and-transformation)
- [Analytics Engine](#analytics-engine)
- [Analytics Outputs](#analytics-outputs)
- [Database Schema](#database-schema)
- [MongoDB Schema](#mongodb-schema)
- [Parquet Output Schema](#parquet-output-schema)
- [Dashboard](#dashboard)
- [Project Structure](#project-structure)
- [Functional Requirements](#functional-requirements)
- [Non-Functional Requirements](#non-functional-requirements)
- [System Requirements](#system-requirements)
- [Environment Configuration](#environment-configuration)
- [Installation](#installation)
- [Running the Complete Pipeline](#running-the-complete-pipeline)
- [Windows Commands](#windows-commands)
- [Linux/macOS Commands](#linuxmacos-commands)
- [HDFS Commands](#hdfs-commands)
- [Spark Commands](#spark-commands)
- [MongoDB Configuration](#mongodb-configuration)
- [Dashboard Commands](#dashboard-commands)
- [Monitoring and Web UIs](#monitoring-and-web-uis)
- [Testing](#testing)
- [Data Validation](#data-validation)
- [Troubleshooting](#troubleshooting)
- [Performance and Scalability](#performance-and-scalability)
- [Design Decisions](#design-decisions)
- [Known Limitations](#known-limitations)
- [Verification Status](#verification-status)
- [Future Enhancements](#future-enhancements)
- [Team Responsibilities](#team-responsibilities)
- [Academic Relevance](#academic-relevance)
- [License](#license)

---

# Overview

The **COVID-19 Big Data Analytics Platform** is an end-to-end distributed data engineering and analytics system designed to process large-scale COVID-19 datasets.

The platform demonstrates how a Big Data pipeline can ingest raw public-health datasets, distribute them across an HDFS cluster, process them using Apache Spark/PySpark, persist analytical results in Parquet and optionally MongoDB Atlas, and expose the results through an interactive Streamlit dashboard.

### High-Level Pipeline

```text
Google COVID-19 Open Data
          │
          ▼
   Data Downloader
          │
          ▼
      dataset/
          │
          ▼
       HDFS Input
   /covid/input
          │
          ▼
   Hadoop HDFS Cluster
   ┌───────────────┐
   │   NameNode    │
   ├───────────────┤
   │ DataNode 1    │
   │ DataNode 2    │
   └───────────────┘
          │
          ▼
   Apache Spark / PySpark
          │
    ┌─────┴─────┐
    │           │
 Cleaning    Analytics
    │           │
    └─────┬─────┘
          ▼
     /covid/output
          │
     ┌────┴─────┐
     │          │
  Parquet    MongoDB Atlas
     │          │
     └────┬─────┘
          ▼
   Streamlit Dashboard
```

---

# Problem Statement

COVID-19 datasets contain large volumes of time-series, geographic, vaccination, hospitalization, and demographic information.

Processing these datasets using a single-machine workflow can become inefficient as data volume grows.

This project addresses the problem by implementing a distributed architecture that provides:

- Distributed storage using HDFS
- Data replication and fault tolerance
- Distributed processing using Apache Spark
- Parallel analytics across Spark workers
- Structured analytical output using Parquet
- Optional NoSQL storage using MongoDB Atlas
- Interactive visualization using Streamlit and Plotly

---

# Objectives

The primary objectives are:

1. Build a multi-node Hadoop cluster using Docker.
2. Store COVID-19 datasets in HDFS.
3. Demonstrate HDFS replication across DataNodes.
4. Process large datasets using distributed PySpark jobs.
5. Clean and standardize raw COVID-19 records.
6. Join epidemiological, vaccination, hospitalization, demographic, and index data.
7. Generate country-level and global analytics.
8. Generate daily time-series analytics.
9. Generate vaccination and hospitalization analytics.
10. Generate regional/state/province analytics where available.
11. Store processed results as Parquet.
12. Optionally publish analytical results to MongoDB Atlas.
13. Provide an interactive Streamlit dashboard.
14. Provide monitoring and validation commands for the distributed system.

---

# Key Features

## Data Engineering

- Automated COVID-19 dataset downloading
- HDFS-based distributed storage
- HDFS replication factor of 2
- Multi-container Hadoop cluster
- Dockerized infrastructure
- Reproducible cluster setup

## Distributed Processing

- Apache Spark
- PySpark
- Spark Standalone execution
- YARN execution path
- Distributed transformations
- Distributed aggregations
- Window functions
- Broadcast joins
- Parquet output

## Analytics

The platform generates:

- Global COVID-19 metrics
- Country-level summaries
- Daily global trends
- Country-by-date summaries
- Vaccination summaries
- Hospitalization summaries
- Regional/state/province summaries
- Active cases
- Case-fatality rates
- Cases per million
- Deaths per million
- 7-day rolling averages

## Database

- MongoDB Atlas integration
- Separate analytical collections
- Batch insertion
- Full-refresh loading strategy
- MongoDB indexes
- Environment-variable based credentials

## Visualization

- Streamlit
- Plotly
- Interactive filters
- Country comparison
- Historical trends
- Vaccination analysis
- Hospitalization analysis
- Data Explorer
- CSV/Excel downloads
- Light/dark dashboard themes

---

# Technology Stack

| Layer | Technology |
|---|---|
| Programming | Python |
| Distributed Storage | Apache Hadoop HDFS |
| Resource Management | Hadoop YARN |
| Distributed Processing | Apache Spark / PySpark |
| Database | MongoDB Atlas |
| File Format | CSV, Parquet |
| Dashboard | Streamlit |
| Visualization | Plotly |
| Containerization | Docker |
| Orchestration | Docker Compose |
| Data Processing | PySpark DataFrames |
| Testing | Python testing framework / repository tests |
| Configuration | `.env`, Hadoop configuration |
| Source Data | Google COVID-19 Open Data |

---

# System Architecture

```text
                         ┌──────────────────────────┐
                         │ Google COVID-19 Open Data│
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │ Python Data Downloader   │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │       Local dataset/     │
                         │        CSV Files         │
                         └────────────┬─────────────┘
                                      │
                              hdfs dfs -put
                                      │
                                      ▼
                    ┌────────────────────────────────┐
                    │          HDFS Cluster           │
                    │                                │
                    │ ┌────────────┐ ┌────────────┐ │
                    │ │ DataNode 1 │ │ DataNode 2 │ │
                    │ └────────────┘ └────────────┘ │
                    │            ▲                   │
                    │            │                   │
                    │      ┌────────────┐             │
                    │      │  NameNode  │             │
                    │      └────────────┘             │
                    └──────────────┬─────────────────┘
                                   │
                                   ▼
                     ┌──────────────────────────┐
                     │ Apache Spark / PySpark   │
                     │                          │
                     │ Spark Master             │
                     │      │                   │
                     │ ┌────┴────┐              │
                     │ ▼         ▼              │
                     │ Worker 1  Worker 2       │
                     └───────────┬──────────────┘
                                 │
                       Distributed Analytics
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
        ┌─────────────────┐             ┌─────────────────┐
        │ HDFS / Parquet  │             │ MongoDB Atlas   │
        └────────┬────────┘             └────────┬────────┘
                 │                               │
                 └──────────────┬────────────────┘
                                ▼
                     ┌──────────────────────┐
                     │ Streamlit Dashboard  │
                     │      + Plotly        │
                     └──────────────────────┘
```

---

# Distributed Cluster Design

The project uses Docker Compose to simulate a distributed Big Data environment on a single physical host.

> The Hadoop cluster consists of multiple containers, not three separate physical machines.

## Hadoop Components

### Master

| Container | Role |
|---|---|
| `hadoop-master` | HDFS NameNode |

### Hadoop Worker 1

| Container | Role |
|---|---|
| `hadoop-worker1` | HDFS DataNode |
| `hadoop-nodemanager1` | YARN NodeManager |

### Hadoop Worker 2

| Container | Role |
|---|---|
| `hadoop-worker2` | HDFS DataNode |
| `hadoop-nodemanager2` | YARN NodeManager |

## Spark Components

| Container | Role |
|---|---|
| `spark-master` | Spark Master / submission client |
| `spark-worker1` | Spark executor |
| `spark-worker2` | Spark executor |

## Dashboard

| Container | Role |
|---|---|
| `covid-dashboard` | Streamlit application |

---

# End-to-End Data Flow

```text
1. Download
   ↓
2. Store raw CSV files locally
   ↓
3. Upload CSV files to HDFS
   ↓
4. HDFS distributes and replicates blocks
   ↓
5. Spark reads CSV data from HDFS
   ↓
6. Data cleaning
   ↓
7. Dataset joins
   ↓
8. Distributed aggregations
   ↓
9. Analytical DataFrames
   ↓
10. Write Parquet to HDFS
   ↓
11. Synchronize processed output
   ↓
12. Optional MongoDB Atlas loading
   ↓
13. Streamlit reads processed results
   ↓
14. Interactive visualization
```

---

# Data Sources

The ingestion pipeline downloads five datasets from Google COVID-19 Open Data:

```text
epidemiology.csv
vaccinations.csv
hospitalizations.csv
demographics.csv
index.csv
```

The downloader stores them under:

```text
dataset/
```

The source URL base is:

```text
https://storage.googleapis.com/covid19-open-data/v3
```

The project does not commit these large generated datasets to Git.

---

# Data Storage Design

The project uses multiple storage layers.

```text
┌──────────────────────┐
│ Raw CSV              │
│ Local dataset/       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ HDFS                 │
│ /covid/input         │
│ Replication = 2      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Spark Processing     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ HDFS                 │
│ /covid/output        │
│ Parquet              │
└──────────┬───────────┘
           │
       ┌───┴────┐
       ▼        ▼
   Parquet   MongoDB
       │        │
       └───┬────┘
           ▼
       Dashboard
```

---

# HDFS Design

## Input Path

```text
/covid/input
```

Expected files:

```text
/covid/input/epidemiology.csv
/covid/input/vaccinations.csv
/covid/input/hospitalizations.csv
/covid/input/demographics.csv
/covid/input/index.csv
```

## Output Path

```text
/covid/output
```

Expected analytical datasets include:

```text
global_metrics
country_summary
daily_summary
country_daily_summary
vaccination_summary
hospitalization_summary
regional_summary
```

## Replication

The configured HDFS replication factor is:

```text
2
```

This means each HDFS block is intended to have two replicas distributed across the DataNodes.

---

# Data Cleaning and Transformation

The `DataCleaner` performs the following operations:

### 1. Remove invalid rows

Rows without:

```text
date
location_key
```

are removed.

### 2. Numeric conversion

COVID-19 numeric columns are converted to numeric types.

Important fields include:

```text
new_confirmed
new_deceased
new_recovered
cumulative_confirmed
cumulative_deceased
new_persons_vaccinated
cumulative_persons_vaccinated
new_vaccine_doses_administered
cumulative_vaccine_doses_administered
new_hospitalized_patients
cumulative_hospitalized_patients
```

### 3. Negative value handling

Negative numeric values are replaced with:

```text
0
```

### 4. Date parsing

Dates are converted to:

```text
yyyy-MM-dd
```

### 5. Duplicate removal

Duplicate records are removed using:

```text
date + location_key
```

### 6. Null handling

Remaining numeric null values are filled with:

```text
0
```

---

# Dataset Join Strategy

The Spark pipeline combines multiple datasets.

```text
                  index.csv
                      │
                      ▼
             Country Metadata
                      │
                      │
epidemiology.csv ─────┼──── vaccinations.csv
                      │
                      ├──── hospitalizations.csv
                      │
                      └──── demographics.csv
                              │
                              ▼
                       Clean DataFrame
                              │
                              ▼
                      Analytics Engine
```

## Primary Join Keys

Country-level records use:

```text
location_key
date
```

Vaccination and hospitalization records are joined using:

```text
location_key + date
```

Demographic information is associated using:

```text
location_key
```

---

# Analytics Engine

The analytics engine is implemented using PySpark DataFrames.

The main analytics class is:

```text
src/analytics/covid_analyzer.py
```

The engine generates seven analytical result sets.

---

## 1. Global Metrics

Collection:

```text
global_metrics
```

Contains worldwide KPIs such as:

- Total cases
- Total deaths
- Total recoveries
- Countries reporting
- Total people vaccinated
- Total vaccine doses administered
- Latest available data date
- Active cases worldwide

---

## 2. Country Summary

Collection:

```text
country_summary
```

Provides country-level statistics.

Major metrics include:

- Total confirmed cases
- Total deaths
- Total recoveries
- Total people vaccinated
- Total vaccine doses
- Active cases
- Peak cumulative cases
- Peak cumulative deaths
- Peak daily confirmed cases
- Days reported
- Case fatality rate
- Population
- Cases per million
- Deaths per million

---

## 3. Daily Summary

Collection:

```text
daily_summary
```

Provides global time-series analytics.

Includes:

- Daily confirmed cases
- Daily deaths
- Daily recoveries
- Daily vaccination counts
- Cumulative cases
- Cumulative deaths
- Cumulative recoveries
- Cumulative vaccination
- Cumulative vaccine doses
- 7-day rolling average of confirmed cases
- 7-day rolling average of deaths

---

## 4. Country Daily Summary

Collection:

```text
country_daily_summary
```

Granularity:

```text
Country + Date
```

Used by the dashboard for:

- Date filtering
- Country trends
- Country comparison
- Historical analysis
- Map filtering

---

## 5. Vaccination Summary

Collection:

```text
vaccination_summary
```

Includes:

- Total people vaccinated
- Cumulative people vaccinated
- Peak daily people vaccinated
- Vaccination days
- Total vaccine doses administered
- Cumulative vaccine doses administered
- Peak daily vaccine doses administered

### Important Metric Distinction

The project distinguishes:

```text
total_people_vaccinated
```

from:

```text
total_vaccine_doses_administered
```

These metrics must not be interpreted as the same measurement.

---

## 6. Hospitalization Summary

Collection:

```text
hospitalization_summary
```

Includes:

- Total hospitalized patients
- Peak hospitalized patients
- Average daily hospitalized patients

Hospitalization coverage depends on whether the source dataset reports the relevant metric.

---

## 7. Regional Summary

Collection:

```text
regional_summary
```

Used for state/province/sub-national analytics where available.

Fields include:

- Location key
- Region name
- Country code
- Parent country
- Total confirmed
- Total deceased
- Total recovered
- Total vaccinated where available
- Total vaccine doses where available
- Days reported

The system does not assume that every regional record has geographic boundary geometry.

---

# Database Schema

The platform follows a hybrid storage model:

```text
                 COVID Analytics
                       │
        ┌──────────────┴──────────────┐
        │                             │
       HDFS                         MongoDB
        │                             │
   Raw + Parquet                Analytics Results
        │                             │
        └──────────────┬──────────────┘
                       │
                  Streamlit
```

---

# MongoDB Schema

Database name:

```text
covid_analytics
```

The MongoDB integration uses the following analytical collections:

```text
global_metrics
country_summary
daily_summary
country_daily_summary
vaccination_summary
hospitalization_summary
regional_summary
```

The MongoDB loader performs a full refresh:

```text
Spark DataFrame
      ↓
JSON records
      ↓
Drop existing collection
      ↓
Batch insert
      ↓
Create indexes
```

## MongoDB Environment Variables

```env
MONGODB_ATLAS_URI=
MONGODB_DATABASE=covid_analytics
```

Credentials must never be committed to Git.

---

# MongoDB Collection Schemas

## `global_metrics`

| Field | Description |
|---|---|
| `total_cases` | Total reported cases |
| `total_deaths` | Total reported deaths |
| `total_recoveries` | Total reported recoveries |
| `countries_reporting` | Number of reporting locations |
| `total_people_vaccinated` | Total vaccinated people where available |
| `total_vaccine_doses_administered` | Total administered doses |
| `latest_data_date` | Latest source date |
| `active_cases_worldwide` | Estimated active cases |

---

## `country_summary`

| Field | Description |
|---|---|
| `country_name` | Country name |
| `location_key` | Source location identifier |
| `total_confirmed` | Total confirmed cases |
| `total_deceased` | Total deaths |
| `total_recovered` | Total recoveries |
| `total_people_vaccinated` | People receiving vaccination |
| `total_vaccine_doses_administered` | Administered vaccine doses |
| `active_cases` | Estimated active cases |
| `peak_cumulative_confirmed` | Maximum cumulative cases |
| `peak_cumulative_deceased` | Maximum cumulative deaths |
| `peak_daily_confirmed` | Maximum daily confirmed cases |
| `days_reported` | Number of reporting days |
| `case_fatality_rate` | Deaths / confirmed cases × 100 |
| `population` | Population where available |
| `cases_per_million` | Cases per million population |
| `deaths_per_million` | Deaths per million population |

---

## `daily_summary`

| Field | Description |
|---|---|
| `date` | Reporting date |
| `global_new_confirmed` | New global confirmed cases |
| `global_new_deceased` | New global deaths |
| `global_new_recovered` | New global recoveries |
| `global_new_vaccinated` | New vaccinated people |
| `global_new_vaccine_doses_administered` | New vaccine doses |
| `cumulative_global_confirmed` | Cumulative cases |
| `cumulative_global_deceased` | Cumulative deaths |
| `cumulative_global_recovered` | Cumulative recoveries |
| `cumulative_global_vaccinated` | Cumulative vaccinated people |
| `cumulative_global_vaccine_doses_administered` | Cumulative doses |
| `rolling_avg_confirmed_7d` | 7-day rolling case average |
| `rolling_avg_deceased_7d` | 7-day rolling death average |

---

## `country_daily_summary`

Logical key:

```text
country_name + location_key + date
```

Contains country/date level:

```text
new_confirmed
new_deceased
new_recovered
new_persons_vaccinated
new_vaccine_doses_administered
cumulative_confirmed
cumulative_deceased
cumulative_recovered
cumulative_persons_vaccinated
cumulative_vaccine_doses_administered
```

---

## `vaccination_summary`

| Field | Description |
|---|---|
| `country_name` | Country |
| `location_key` | Country identifier |
| `total_people_vaccinated` | Total people vaccinated |
| `cumulative_people_vaccinated` | Latest cumulative people vaccinated |
| `peak_daily_people_vaccinated` | Maximum daily vaccinated people |
| `vaccination_days` | Number of days with vaccination activity |
| `total_vaccine_doses_administered` | Total doses |
| `cumulative_vaccine_doses_administered` | Latest cumulative doses |
| `peak_daily_vaccine_doses_administered` | Maximum daily doses |

---

## `hospitalization_summary`

| Field | Description |
|---|---|
| `country_name` | Country |
| `location_key` | Location identifier |
| `total_hospitalized` | Total hospitalized records |
| `peak_hospitalized` | Maximum hospitalization value |
| `avg_daily_hospitalized` | Average daily hospitalization |

---

## `regional_summary`

| Field | Description |
|---|---|
| `location_key` | Regional location identifier |
| `region_name` | State/province/region |
| `country_code` | Parent country code |
| `parent_country` | Parent country |
| `total_confirmed` | Total confirmed cases |
| `total_deceased` | Total deaths |
| `total_recovered` | Total recoveries |
| `total_people_vaccinated` | Vaccinated people where available |
| `total_vaccine_doses_administered` | Doses where available |
| `days_reported` | Number of reporting days |

---

# MongoDB Indexes

The MongoDB loader creates indexes for frequently queried fields.

| Collection | Indexed Fields |
|---|---|
| `global_metrics` | `total_cases` |
| `country_summary` | `country_name`, `total_confirmed` |
| `daily_summary` | `date` |
| `vaccination_summary` | `country_name`, `total_vaccinated` |
| `hospitalization_summary` | `country_name` |

---

# Parquet Output Schema

Spark writes every analytical DataFrame as Parquet:

```text
/covid/output/<collection_name>
```

Example:

```text
/covid/output/global_metrics
/covid/output/country_summary
/covid/output/daily_summary
/covid/output/country_daily_summary
/covid/output/vaccination_summary
/covid/output/hospitalization_summary
/covid/output/regional_summary
```

Parquet is used because it provides a columnar storage format suitable for analytical workloads and reduces repeated parsing compared with raw CSV.

---

# Dashboard

The dashboard is implemented using:

```text
Streamlit
Plotly
Pandas
NumPy
SciPy where available
```

Main dashboard capabilities include:

### Overview

- Global KPIs
- Total cases
- Deaths
- Recoveries
- Vaccination
- Active cases

### Country Analysis

- Top affected countries
- Country comparison
- Country-level metrics
- Per-capita statistics

### Trend Analysis

- Daily trends
- Cumulative trends
- 7-day rolling averages
- Weekly grouping
- Monthly grouping

### Vaccination

- Vaccination progress
- Vaccinated people
- Vaccine doses
- Country comparisons

### Hospitalization

- Hospital burden
- Peak hospitalization
- Average hospitalization

### Regional Analysis

- State/province level data where available
- Parent-country mapping
- Regional ranking

### Data Explorer

- Processed dataset preview
- Raw source preview
- Search
- Sorting
- Data-quality information
- CSV download
- Excel download

---

# Project Structure

```text
covid-big-data-analytics/
│
├── config/
│   └── hadoop/
│       └── hadoop.env
│
├── dashboard/
│   ├── app.py
│   ├── data_loader.py
│   └── theme.py
│
├── dataset/
│   └── *.csv
│
├── docs/
│   ├── architecture.md
│   ├── cluster-setup.md
│   ├── data-pipeline.md
│   ├── processing.md
│   ├── streamlit-integration.md
│   └── screenshots/
│
├── hadoop/
│   └── streaming/
│
├── notebooks/
│
├── scripts/
│   └── cluster/
│       ├── start-cluster.*
│       ├── stop-cluster.*
│       ├── verify-cluster.*
│       ├── upload-data.*
│       └── run-analysis.*
│
├── spark/
│   └── jobs/
│       └── analytics_job.py
│
├── src/
│   ├── analytics/
│   │   └── covid_analyzer.py
│   │
│   ├── ingestion/
│   │   └── data_downloader.py
│   │
│   ├── mongodb/
│   │   └── mongo_loader.py
│   │
│   └── processing/
│       └── data_cleaner.py
│
├── tests/
│
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── EVALUATION_GUIDE.md
├── LICENSE
├── README.md
├── RUN_GUIDE.md
├── requirements.txt
├── requirements-dashboard.txt
├── run_pipeline_cluster.py
└── run_pipeline_local.py
```

---

# Functional Requirements

## FR-01 — Data Ingestion

The system shall download the configured COVID-19 source datasets.

## FR-02 — Distributed Storage

The system shall store raw datasets in HDFS.

## FR-03 — Replication

HDFS shall maintain a replication factor of 2 for uploaded source data.

## FR-04 — Distributed Processing

The system shall process the datasets using Apache Spark/PySpark.

## FR-05 — Data Cleaning

The system shall remove invalid records, handle nulls, cast numeric columns, remove duplicates, and normalize negative values.

## FR-06 — Data Integration

The system shall combine epidemiology, vaccination, hospitalization, demographic, and index data where corresponding records are available.

## FR-07 — Analytics

The system shall generate global, country, daily, vaccination, hospitalization, and regional analytics.

## FR-08 — Analytical Storage

The system shall store processed results as Parquet.

## FR-09 — NoSQL Storage

The system shall optionally load processed results into MongoDB Atlas.

## FR-10 — Visualization

The system shall expose processed analytics through an interactive dashboard.

## FR-11 — Data Exploration

The dashboard shall provide processed and raw-data exploration capabilities.

## FR-12 — Export

The dashboard shall support data downloads.

---

# Non-Functional Requirements

## Scalability

The architecture should allow additional Spark workers and HDFS DataNodes to be added.

## Fault Tolerance

HDFS replication provides redundant storage of HDFS blocks.

## Maintainability

The system separates:

```text
Ingestion
Processing
Analytics
Database
Dashboard
Infrastructure
```

## Reproducibility

Docker Compose provides a reproducible development environment.

## Security

Database credentials are provided through environment variables rather than source code.

## Performance

Spark performs distributed transformations and aggregations rather than loading the complete analytical workload into a single Python process.

## Observability

The system provides:

- NameNode UI
- YARN ResourceManager UI
- Spark Master UI
- Spark application UI
- Docker logs
- HDFS filesystem commands

---

# System Requirements

## Minimum

- Git
- Docker Desktop / Docker Engine
- Docker Compose v2
- Internet connection
- Python 3.10 or 3.11 for host-side scripts

## Recommended

```text
CPU:       4+ cores
RAM:       8 GB+ available to Docker
Storage:   15 GB+ free
Internet:  Stable broadband connection
```

The complete Spark workload may require more memory depending on source data size and Docker configuration.

---

# Environment Configuration

Create the environment file from the template.

### Windows PowerShell

```powershell
Copy-Item .env.example .env
```

### Windows CMD

```bat
copy .env.example .env
```

### Linux/macOS

```bash
cp .env.example .env
```

Example:

```env
MONGODB_ATLAS_URI=
MONGODB_DATABASE=covid_analytics
```

MongoDB is optional.

For local Parquet-only dashboard usage:

```env
MONGODB_ATLAS_URI=
```

For MongoDB Atlas:

```env
MONGODB_ATLAS_URI=mongodb+srv://<username>:<password>@<cluster>/...
MONGODB_DATABASE=covid_analytics
```

Never commit `.env`.

---

# Installation

## 1. Clone Repository

```bash
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

## 2. Verify Tools

```bash
git --version
docker --version
docker compose version
python --version
```

## 3. Create Environment

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

---

# Optional Python Environment

## Windows

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

# Running the Complete Pipeline

The complete pipeline can be executed through:

```bash
python run_pipeline_cluster.py
```

The pipeline performs:

```text
Start Docker cluster
        ↓
Download datasets
        ↓
Upload datasets to HDFS
        ↓
Run Spark analytics
        ↓
Write Parquet
        ↓
Load MongoDB
        ↓
Verify cluster
```

---

# Pipeline Options

The cluster runner supports:

```bash
python run_pipeline_cluster.py --skip-start
```

Skip Docker startup.

```bash
python run_pipeline_cluster.py --skip-download
```

Skip data download.

```bash
python run_pipeline_cluster.py --skip-upload
```

Skip HDFS upload.

```bash
python run_pipeline_cluster.py --skip-analysis
```

Skip Spark processing.

## Execution Mode

YARN:

```bash
python run_pipeline_cluster.py --mode yarn
```

Spark Standalone:

```bash
python run_pipeline_cluster.py --mode cluster
```

Local:

```bash
python run_pipeline_cluster.py --mode local
```

---

# Windows Commands

## Start Cluster

```bat
start-cluster.bat
```

## Verify Cluster

```bat
verify-cluster.bat
```

## Download and Upload Data

```bat
upload-data.bat
```

## Run Spark Analytics

```bat
run-analysis.bat -Mode cluster
```

## Stop Cluster

```bat
stop-cluster.bat
```

---

# Linux/macOS Commands

## Start

```bash
bash scripts/cluster/start-cluster.sh
```

## Verify

```bash
bash scripts/cluster/verify-cluster.sh
```

## Upload

```bash
bash scripts/cluster/upload-data.sh
```

## Run Spark

```bash
SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh
```

## Stop

```bash
bash scripts/cluster/stop-cluster.sh
```

---

# HDFS Commands

## List HDFS Input

```bash
docker exec hadoop-master hdfs dfs -ls /covid/input
```

## List HDFS Output

```bash
docker exec hadoop-master hdfs dfs -ls -R /covid/output
```

## Check HDFS Capacity

```bash
docker exec hadoop-master hdfs dfsadmin -report
```

## Check File Blocks

```bash
docker exec hadoop-master \
hdfs fsck /covid/input/epidemiology.csv \
-files -blocks -locations
```

## Display HDFS Root

```bash
docker exec hadoop-master hdfs dfs -ls /
```

## Create Input Directory

```bash
docker exec hadoop-master \
hdfs dfs -mkdir -p /covid/input
```

## Create Output Directory

```bash
docker exec hadoop-master \
hdfs dfs -mkdir -p /covid/output
```

---

# Spark Commands

## Submit Through Standalone Cluster

```bash
SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh
```

Windows:

```bat
run-analysis.bat -Mode cluster
```

## Run Analytics Directly

Inside the Spark environment:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  spark/jobs/analytics_job.py \
  --mode cluster
```

## Skip MongoDB

```bash
spark-submit \
  --master spark://spark-master:7077 \
  spark/jobs/analytics_job.py \
  --mode cluster \
  --skip-mongo
```

---

# Dashboard Commands

## Docker Dashboard

Start:

```bash
docker compose up -d dashboard
```

Open:

```text
http://localhost:8501
```

## Host Dashboard

```bash
python -m streamlit run dashboard/app.py
```

Or:

```bash
streamlit run dashboard/app.py
```

---

# Monitoring and Web UIs

Once the cluster is running:

| Component | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Spark Master | http://localhost:8080 |
| Spark Application UI | http://localhost:4040 |
| NodeManager 1 | http://localhost:8042 |
| NodeManager 2 | http://localhost:8043 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |

---

# Docker Commands

## Build Images

```bash
docker compose build
```

## Start

```bash
docker compose up -d
```

## Check Services

```bash
docker compose ps
```

## View Logs

```bash
docker compose logs --tail=100
```

## View Spark Logs

```bash
docker compose logs --tail=100 spark-master spark-worker1 spark-worker2
```

## View Hadoop Logs

```bash
docker compose logs --tail=100 namenode resourcemanager datanode1 datanode2
```

## Stop

```bash
docker compose down
```

## Stop and Remove Volumes

> Destructive: this removes persistent HDFS data.

```bash
docker compose down --volumes
```

---

# Testing

Run the repository tests using the project's Python environment.

Example:

```bash
pytest
```

For verbose output:

```bash
pytest -v
```

Before testing the full distributed pipeline, verify:

```bash
docker compose config --quiet
docker compose ps
```

Then validate HDFS:

```bash
docker exec hadoop-master hdfs dfsadmin -report
```

---

# Data Validation

## Verify Input Files

```bash
docker exec hadoop-master \
hdfs dfs -ls /covid/input
```

Expected:

```text
epidemiology.csv
vaccinations.csv
hospitalizations.csv
demographics.csv
index.csv
```

## Verify DataNode Count

```bash
docker exec hadoop-master \
hdfs dfsadmin -report
```

Expected:

```text
Live datanodes (2)
```

## Verify Replication

```bash
docker exec hadoop-master \
hdfs fsck /covid/input/epidemiology.csv \
-files -blocks -locations
```

The output should show the blocks and their DataNode locations.

## Verify Spark Output

```bash
docker exec hadoop-master \
hdfs dfs -ls -R /covid/output
```

Do not treat old Parquet files as new results unless their timestamps correspond to a successful Spark execution.

---

# Troubleshooting

## `.env` Missing

Error:

```text
.env file not found
```

Fix:

```powershell
Copy-Item .env.example .env
```

---

## Docker Daemon Not Running

Check:

```bash
docker info
```

Start Docker Desktop or the Docker service.

---

## DataNode Missing

Check:

```bash
docker compose ps
```

Then:

```bash
docker compose logs datanode1 datanode2
```

And:

```bash
docker exec hadoop-master hdfs dfsadmin -report
```

---

## HDFS Input Empty

Run:

```bash
docker exec hadoop-master hdfs dfs -ls /covid/input
```

If empty:

```bash
upload-data.bat
```

or:

```bash
bash scripts/cluster/upload-data.sh
```

---

## Spark Job Exits With Code 137

Check:

```bash
docker compose logs spark-master spark-worker1 spark-worker2
```

Code 137 commonly indicates that a process was killed by the operating system/container environment, often due to memory pressure.

Recommended actions:

1. Increase Docker memory.
2. Reduce concurrent workload.
3. Increase available host resources.
4. Run smaller test subsets first.
5. Check Spark worker memory.
6. Inspect executor logs.

---

## Spark `FetchFailed` Errors

Inspect:

```bash
docker compose logs spark-master spark-worker1 spark-worker2
```

Check:

```text
Spark worker connectivity
Executor memory
Shuffle operations
Docker resource allocation
```

---

## YARN Python Error

The Hadoop NodeManager container configuration may not provide the Python runtime expected for YARN executor execution.

If YARN execution fails because `python3` is unavailable, use Spark Standalone:

```bash
SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh
```

Windows:

```bat
run-analysis.bat -Mode cluster
```

---

## MongoDB Connection Failure

Check:

```env
MONGODB_ATLAS_URI=
```

Verify:

- Atlas connection string
- Username
- Password
- Network Access
- IP allowlist
- Database name
- DNS/network connectivity

MongoDB is optional if the dashboard is using local Parquet output.

---

## Dashboard Shows Old Results

Check:

```bash
ls output/
```

And:

```bash
docker exec hadoop-master \
hdfs dfs -ls -R /covid/output
```

Verify timestamps before interpreting the displayed data as current.

---

# Performance and Scalability

The architecture is designed around distributed processing.

## Current Spark Cluster

```text
Spark Master
    │
    ├── Spark Worker 1
    │     ├── 2 cores
    │     └── 1 GB configured worker memory
    │
    └── Spark Worker 2
          ├── 2 cores
          └── 1 GB configured worker memory
```

## HDFS

```text
NameNode
   │
   ├── DataNode 1
   └── DataNode 2
```

## Scaling Strategy

The architecture can be extended by adding:

```text
DataNode 3
DataNode 4
...
```

and:

```text
Spark Worker 3
Spark Worker 4
...
```

This allows storage and compute capacity to scale independently.

---

# Design Decisions

## Why HDFS?

HDFS provides:

- Distributed storage
- Block-based storage
- Data replication
- Fault tolerance
- Horizontal scalability

## Why Spark?

Spark provides:

- Distributed processing
- DataFrame APIs
- Parallel transformations
- Aggregations
- Window functions
- Efficient analytical processing

## Why Parquet?

Parquet provides:

- Columnar storage
- Efficient analytical reads
- Reduced storage overhead compared with raw CSV
- Better compatibility with analytical processing

## Why MongoDB?

MongoDB provides a flexible NoSQL representation of processed analytical results and allows the dashboard to retrieve result collections without directly executing distributed Spark jobs.

## Why Streamlit?

Streamlit provides a lightweight interactive analytics interface directly on top of Python data-processing libraries.

## Why Docker?

Docker provides:

- Reproducible environments
- Isolated services
- Easier Hadoop/Spark setup
- Portable cluster configuration
- Simple service orchestration

---

# Separation of Responsibilities

The architecture deliberately separates processing from visualization.

```text
Spark
  │
  └── Performs distributed analytics

HDFS
  │
  └── Stores raw and processed data

MongoDB
  │
  └── Stores analytical documents

Streamlit
  │
  └── Visualizes already-processed data
```

The dashboard does **not** execute the distributed Spark analytics itself.

---

# Data Quality Strategy

The platform performs multiple data-quality operations:

```text
Raw Data
   │
   ├── Remove missing date
   ├── Remove missing location
   ├── Cast numeric columns
   ├── Replace negative values
   ├── Parse dates
   ├── Remove duplicate date/location records
   ├── Fill numeric nulls
   │
   ▼
Clean Data
```

The dashboard additionally exposes data-quality information for processed datasets.

---

# Geographic Data Model

The project distinguishes between:

## Country-Level Records

Location keys generally have a two-character country-level format.

Example conceptually:

```text
US
IN
GB
```

## Regional Records

Regional records use longer location keys.

Example:

```text
US_CA
US_TX
```

The regional pipeline extracts:

```text
country_code
region_name
parent_country
```

This allows state/province-level aggregation while retaining the parent country relationship.

---

# Current Verification Status

## Verified

The following infrastructure components have been verified:

- Docker Compose configuration
- Hadoop services
- NameNode
- ResourceManager
- Two DataNodes
- Two NodeManagers
- HDFS input directory
- Five source CSV uploads
- HDFS replication for the epidemiology input file
- Dashboard health
- Spark workers registering with Spark Master

## Not Fully Verified

The latest full distributed Spark analytics execution has not completed successfully.

The latest documented Spark attempt encountered:

```text
FetchFailed
```

during distributed output/shuffle processing.

Therefore:

> **Existing Parquet/MongoDB analytics output must be treated as potentially stale until a complete Spark run exits successfully and the resulting schemas, timestamps, and values are validated.**

This distinction is important for reproducibility and prevents old output from being presented as newly computed analytics.

---

# Known Limitations

1. The complete Spark workload can require substantial memory.
2. The Docker cluster runs on one physical host.
3. HDFS replication does not provide physical-machine fault tolerance in this setup because the DataNodes are Docker containers on the same host.
4. YARN executor execution depends on Python availability in the NodeManager environment.
5. Regional analytics depend on the geographic coverage of the source dataset.
6. Regional data does not provide boundary geometry for every region.
7. MongoDB is optional.
8. Existing output files may be stale after an unsuccessful Spark execution.
9. Vaccination metrics must distinguish people vaccinated from vaccine doses administered.
10. Source-data aliases and geographic naming differences can affect comparisons.

---

# Future Enhancements

Potential future improvements include:

## Infrastructure

- Add more HDFS DataNodes
- Add more Spark workers
- Deploy across multiple physical machines
- Add Kubernetes deployment
- Add cloud deployment

## Processing

- Optimize Spark shuffle operations
- Tune partition sizes
- Introduce adaptive Spark configuration
- Add incremental processing
- Add checkpointing
- Add structured streaming

## Data Engineering

- Add schema validation
- Add data-quality scoring
- Add pipeline metadata
- Add data lineage
- Add automated freshness checks

## Database

- Improve MongoDB indexing
- Add TTL/version metadata where appropriate
- Add incremental MongoDB updates instead of full refresh
- Add analytical API layer

## Dashboard

- Add geographic choropleth maps where boundary data is available
- Add forecasting
- Add anomaly detection
- Add alerting
- Add richer comparative analytics

## DevOps

- Add CI/CD
- Add automated integration tests
- Add Docker image publishing
- Add pipeline monitoring
- Add automated Spark job validation

---

# Academic Relevance

This project demonstrates important concepts from a **Big Data Analytics** curriculum.

| Big Data Concept | Implementation |
|---|---|
| Distributed Storage | HDFS |
| Data Replication | HDFS replication factor 2 |
| Cluster Architecture | Hadoop multi-container cluster |
| Resource Management | YARN |
| Distributed Processing | Apache Spark |
| Data Transformation | PySpark |
| Data Cleaning | PySpark DataFrames |
| Data Integration | Multi-source joins |
| Aggregation | Spark groupBy/aggregation |
| Window Processing | Spark Window functions |
| Columnar Storage | Parquet |
| NoSQL | MongoDB |
| Visualization | Streamlit + Plotly |
| Containerization | Docker |
| Orchestration | Docker Compose |

---

# Quick Start

For a quick demonstration:

```bash
git clone https://github.com/Fenil412/covid-big-data-analytics.git
cd covid-big-data-analytics
```

Create environment:

```bash
cp .env.example .env
```

Build and start:

```bash
docker compose build
docker compose up -d
```

Download/upload data:

```bash
bash scripts/cluster/upload-data.sh
```

Run distributed analytics:

```bash
SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh
```

Verify:

```bash
docker exec hadoop-master hdfs dfsadmin -report
docker exec hadoop-master hdfs dfs -ls /covid/input
docker exec hadoop-master hdfs dfs -ls -R /covid/output
```

Open dashboard:

```text
http://localhost:8501
```

---

# Useful Command Cheat Sheet

| Task | Command |
|---|---|
| Clone | `git clone https://github.com/Fenil412/covid-big-data-analytics.git` |
| Build | `docker compose build` |
| Start | `docker compose up -d` |
| Status | `docker compose ps` |
| Stop | `docker compose down` |
| Logs | `docker compose logs --tail=100` |
| HDFS report | `docker exec hadoop-master hdfs dfsadmin -report` |
| HDFS input | `docker exec hadoop-master hdfs dfs -ls /covid/input` |
| HDFS output | `docker exec hadoop-master hdfs dfs -ls -R /covid/output` |
| HDFS blocks | `docker exec hadoop-master hdfs fsck /covid/input/epidemiology.csv -files -blocks -locations` |
| Upload | `bash scripts/cluster/upload-data.sh` |
| Spark | `SPARK_RUN_MODE=cluster bash scripts/cluster/run-analysis.sh` |
| Dashboard | `streamlit run dashboard/app.py` |
| Tests | `pytest -v` |

---

# Team Responsibilities

The project is organized around three major responsibilities:

### Member 1 — Distributed Processing / Analytics

Responsibilities:

- Spark architecture
- PySpark processing
- Distributed aggregations
- Analytics engine
- Performance tuning

### Member 2 — Cluster & Data Ingestion

Responsibilities:

- Docker cluster
- Hadoop configuration
- HDFS
- Data ingestion
- Data download
- Cluster verification

### Member 3 — Analytics & NoSQL

Responsibilities:

- MongoDB Atlas
- Analytical result storage
- Dashboard
- Streamlit
- Plotly visualization
- Data exploration

---

# Project Documentation

Additional documentation is available in:

```text
docs/
```

### Architecture

```text
docs/architecture.md
```

### Cluster Setup

```text
docs/cluster-setup.md
```

### Data Pipeline

```text
docs/data-pipeline.md
```

### Distributed Processing

```text
docs/processing.md
```

### Streamlit Integration

```text
docs/streamlit-integration.md
```

### Screenshots

```text
docs/screenshots/README.md
```

---

# Repository

**GitHub:**  
https://github.com/Fenil412/covid-big-data-analytics

---

# License

This project is licensed under the **Apache License 2.0**.

See:

```text
LICENSE
```

for the complete license text.

---

# Final Architecture Summary

```text
                    ┌──────────────────────┐
                    │ Google COVID Data    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Python Downloader    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Local CSV Dataset    │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌─────────────────────────────────┐
              │          HDFS CLUSTER            │
              │                                 │
              │  NameNode                       │
              │      │                          │
              │  ┌───┴────┐                     │
              │  ▼        ▼                     │
              │ DN1      DN2                    │
              │                                 │
              │ Replication Factor = 2          │
              └────────────────┬────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Apache Spark         │
                    │                      │
                    │ Master               │
                    │   │                  │
                    │   ├── Worker 1       │
                    │   └── Worker 2       │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Data Cleaning        │
                    │ + Joins              │
                    │ + Transformations    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Analytics Engine     │
                    │                      │
                    │ Global               │
                    │ Country              │
                    │ Daily                │
                    │ Vaccination          │
                    │ Hospitalization       │
                    │ Regional              │
                    └──────────┬───────────┘
                               │
                     ┌─────────┴─────────┐
                     │                   │
                     ▼                   ▼
             ┌──────────────┐    ┌──────────────┐
             │ HDFS Parquet │    │ MongoDB Atlas│
             └──────┬───────┘    └──────┬───────┘
                    │                   │
                    └─────────┬─────────┘
                              ▼
                    ┌──────────────────────┐
                    │ Streamlit + Plotly   │
                    │ Interactive Dashboard │
                    └──────────────────────┘
```

---

## Project Status

**Infrastructure:** Operational / verified  
**HDFS ingestion:** Verified  
**Data replication:** Verified for tested input  
**Dashboard:** Operational  
**MongoDB integration:** Implemented  
**Spark analytics pipeline:** Implemented  
**Latest full distributed Spark execution:** **Not yet verified successful**

This README intentionally distinguishes implemented functionality from successfully verified execution results.
