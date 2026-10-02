# COVID-19 Big Data Analytics Platform
## Viva, Presentation & Professor Explanation Guide

**Course:** 4CS101ME25 - Big Data Systems  
**Institution:** Nirma University, Institute of Technology  

---

## 🎯 1. 30-Second Elevator Pitch
> *"We built an end-to-end **Distributed Big Data Analytics Platform** that processes over **12.5 million real-world COVID-19 records** (~720 MB) using **Apache Spark** on a multi-node **Hadoop HDFS cluster** containerized with **Docker**. Our pipeline performs distributed data cleaning, computes 4 distinct analytical aggregations (including 7-day rolling window averages and mortality rates across 232 countries), persists fault-tolerant analytical views in **Parquet and MongoDB Atlas**, and serves an interactive **Streamlit** dashboard for public health decision-makers."*

---

## ⏱️ 2. Recommended 4-Minute Presentation Script

Use this structured script when demonstrating the project to the professor:

### Minute 1: Introduction & Problem Motivation
- *"Good morning/afternoon, Professor. Public health surveillance during pandemics requires processing massive, noisy, and fast-updating data streams from global agencies. Traditional single-node tools like Python Pandas fail due to memory saturation (`OutOfMemory` errors) and lack of fault tolerance."*
- *"In this project for **Big Data Systems (4CS101ME25)**, we designed a distributed big data architecture separating **distributed storage (Hadoop HDFS)**, **distributed compute (Apache Spark)**, **cloud persistence (MongoDB Atlas)**, and **interactive analytics (Streamlit)**."*

### Minute 2: Architectural Walkthrough
- Point to the Architecture diagram or terminal:
  - *"Our storage layer runs **HDFS** with a NameNode and two DataNodes. Files like `epidemiology.csv` (521 MB) are split into 128 MB blocks and replicated across the two DataNodes with a replication factor of 2. If any worker container crashes, data remains accessible without loss."*
  - *"Our processing layer uses **PySpark (Apache Spark 3.5.3)**. Spark decomposes work into distributed partitions across worker cores. We use Spark's Catalyst optimizer and DataFrame API for distributed group-by, join, and sliding window operations."*

### Minute 3: Live Demonstration
- Open terminal and run the pipeline or show running cluster:
  - **Show HDFS Health:** Open `http://localhost:9870` → Utilities → Browse Filesystem → Show `/covid/input` with 5 CSVs and 2 live DataNodes.
  - **Show Block Distribution:** Point out `hdfs fsck` showing block replicas on both `hadoop-worker1` and `hadoop-worker2`.
  - **Show Spark Cluster:** Open `http://localhost:8080` → Show Spark Master with active worker nodes and executor memory.
  - **Show Live Pipeline Execution:** Run `python run_pipeline_local.py` or `run-analysis.bat -Mode cluster` to show distributed processing of 12.5M rows in under 60 seconds.

### Minute 4: Insights, Results & Conclusion
- Open the Streamlit dashboard (`http://localhost:8501`) and MongoDB Atlas (`cloud.mongodb.com`):
  - Highlight the 4 analytical summaries: Country KPIs, 990-day Global Trend with 7-day moving average, Vaccination Milestones, and Hospitalization Burden.
  - Conclude: *"Our system demonstrates high throughput, horizontal scalability, fault tolerance, and production-grade separation of concerns."*

---

## 🏗️ 3. Architecture Deep Dive

```
                             [ Google COVID-19 Open Data ]
                                           │
                                           │ HTTP Chunked Ingestion
                                           ▼
                                [ Local Dataset Cache ]
                                           │
                                           │ WebHDFS Upload
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              HADOOP DISTRIBUTED FILE SYSTEM                            │
│                                                                                        │
│   [ NameNode (Metadata) ]                                                              │
│            │                                                                           │
│            ├─────────────────────────────────────────┬─────────────────────────────┐   │
│            ▼                                         ▼                             │   │
│   [ DataNode 1 (Worker 1) ]                 [ DataNode 2 (Worker 2) ]              │   │
│   - Block 1 (Replica A)                     - Block 1 (Replica B)                  │   │
│   - Block 2 (Replica B)                     - Block 2 (Replica A)                  │   │
└──────────────────────────────────────────────┬─────────────────────────────────────────┘
                                               │
                                               │ Distributed Partition Read
                                               ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                           APACHE SPARK DISTRIBUTED ENGINE                              │
│                                                                                        │
│   [ Spark Driver / Master ] ── Coordinates DAG, Catalyst Optimizer, Task Scheduling   │
│            │                                                                           │
│            ├── Worker 1 Executor (2 Cores) ── Data Cleaning & Map Phase                │
│            └── Worker 2 Executor (2 Cores) ── Aggregation & Reduce/Shuffle Phase       │
└──────────────────────────────────────────────┬─────────────────────────────────────────┘
                                               │
                                               │ Output Sync
                                ┌──────────────┴──────────────┐
                                ▼                             ▼
                    [ Partitioned Parquet ]          [ MongoDB Atlas NoSQL ]
                    - Snappy Compression             - Cloud Document DB
                    - Columnar Storage               - 4 Collections
                                │                             │
                                └──────────────┬──────────────┘
                                               ▼
                                  [ Streamlit Dashboard ]
                                  - 232 Countries
                                  - Dynamic Filter Engine
                                  - Choropleth World Maps
```

---

## 🔍 4. Key Files & Code Walkthrough (Where to Point the Professor)

When the professor asks *"Show me your code"*, open these specific files:

### 1. `src/analytics/covid_analyzer.py` (Core PySpark Analytics Engine)
- **Line 40–75 (`compute_country_summary`):**
  - **What to show:** `df.groupBy("country_name", "location_key").agg(F.sum("new_confirmed"), F.max("cumulative_confirmed"), ...)`
  - **What to say:** *"Here, Spark executes distributed MapReduce-style aggregation across partitions without pulling raw rows into the driver memory."*
- **Line 80–110 (`compute_daily_summary` with Window Function):**
  - **What to show:**
    ```python
    window_spec = Window.orderBy("date").rowsBetween(-6, 0)
    df.withColumn("rolling_avg_7d", F.avg("global_new_confirmed").over(window_spec))
    ```
  - **What to say:** *"We implemented a 7-day rolling window average using Spark Window functions. This smooths out weekend reporting dips in pandemic surveillance."*

### 2. `src/processing/data_cleaner.py` (Data Cleansing & Validation)
- **What to show:** Null filtering, schema casting (`IntegerType`, `DoubleType`), handling negative delta corrections in COVID reporting, and filtering `aggregation_level == 0` for national summaries.
- **What to say:** *"Real-world data has reporting corrections (negative daily values) and missing fields. PySpark handles data quality transformations in parallel."*

### 3. `docker-compose.yml` (Cluster Definition)
- **What to show:** The multi-container network defining `namenode`, `datanode1`, `datanode2`, `spark-master`, `spark-worker1`, and `spark-worker2`.
- **What to say:** *"This Docker Compose file creates an isolated virtual bridge network where 6 distributed nodes interact exactly like a physical multi-server cluster."*

### 4. `src/mongodb/mongo_loader.py` (NoSQL Persistence)
- **What to show:** PyMongo driver connection, batch `insert_many` operations, and SRV connection to MongoDB Atlas.
- **What to say:** *"We convert aggregate Spark DataFrames to JSON-like BSON documents and insert them into cloud-hosted MongoDB Atlas collections for microservice consumption."*

---

## 📊 5. Explanation of the 4 Analytics Computed

| Analytic Module | Input Rows | Output Output Documents | Key Metrics Computed | Big Data Concept Used |
|---|---|---|---|---|
| **1. Country Summary** | ~12.5M rows | 232 Countries | Total Confirmed, Deaths, Case Fatality Rate (CFR %), Peak Daily Cases | Distributed `groupBy()`, `agg()`, `withColumn()` |
| **2. Daily Global Summary** | ~12.5M rows | 990 Days (2020–2022) | Global Daily Cases, Global Deaths, 7-Day Moving Average | Sliding Window Functions (`rowsBetween(-6, 0)`) |
| **3. Vaccination Summary** | ~1.8M rows | 232 Countries | Total Doses Administered, Cumulative Vaccinated, Peak Day Inoculations | Multi-column aggregations & metric alignment |
| **4. Hospitalization Burden** | ~1.5M rows | 15 Reporting Countries | Total Hospital Admissions, Peak ICU Occupancy, Daily Average Burden | Outer join with country metadata & selective aggregation |

---

## ❓ 6. Expected Professor Questions & High-Scoring Answers (Viva Q&A)

### Q1: "Why did you choose Apache Spark over traditional MapReduce or Python Pandas?"
> **Answer:** *"Pandas is single-threaded and requires all data to reside in RAM on a single machine. Loading 12.5 million rows (~720 MB raw, expanding to several gigabytes in memory) causes Out-Of-Memory (OOM) crashes. Traditional Hadoop MapReduce writes all intermediate state to disk, causing high I/O latency. Apache Spark uses in-memory Resilient Distributed Datasets (RDDs) and the Catalyst query optimizer, making it up to 100x faster than MapReduce for iterative computations and analytical queries."*

### Q2: "What is HDFS replication and why is it important in your system?"
> **Answer:** *"In our `docker-compose.yml`, we configured `dfs.replication = 2`. When a file is uploaded to HDFS, it is split into 128 MB blocks, and each block is stored on 2 independent DataNodes. If `hadoop-worker1` fails or its container crashes, `hadoop-worker2` immediately serves the missing blocks. This provides zero data loss and high availability."*

### Q3: "How does Spark's Window function work in your daily summary?"
> **Answer:** *"Pandemic numbers fluctuate due to uneven weekend reporting. We used Spark's `Window.orderBy('date').rowsBetween(-6, 0)`. For every date, Spark defines a logical sliding frame spanning the current date and the preceding 6 dates, computing the average daily cases over that 7-day interval across all partitions."*

### Q4: "What is the difference between a Narrow transformation and a Wide transformation in your Spark job?"
> **Answer:** 
> - *"A **Narrow transformation** (such as `.filter()` or `.withColumn()`) operates on data within a single partition without requiring data movement across nodes."*
> - *"A **Wide transformation** (such as `.groupBy('country_name')` or `.join()`) requires a **Shuffle**, where data from multiple partitions across different workers is re-partitioned and transferred over the network by key. This is where memory overhead and network I/O are critical."*

### Q5: "Is this running on physical machines or virtual containers?"
> **Answer:** *"The cluster runs on Docker containers managed via Docker Compose on a single host. Each container has its own assigned IP, hostname, JVM process, and resource limits, simulating a true distributed multi-node topology with dedicated Master, DataNode, and Spark Worker roles."*

### Q6: "Why use MongoDB Atlas and Parquet together?"
> **Answer:** 
> - *"**Parquet** is an open-source, columnar storage format with Snappy compression, ideal for downstream big data analytics and local file fallback."*
> - *"**MongoDB Atlas** is a managed cloud NoSQL database that stores data as JSON/BSON documents, enabling web applications, REST APIs, or external dashboards to query individual country or date records with low latency."*

### Q7: "What happens if a Spark Worker node crashes during job execution?"
> **Answer:** *"Spark builds a Directed Acyclic Graph (DAG) of lineage for every DataFrame. If Worker 1 crashes mid-job, the Spark Master detects the lost heartbeat, reallocates the pending partition tasks to Worker 2, reads the source blocks from the surviving HDFS DataNode, and completes the computation without restarting the entire pipeline."*

### Q8: "What was the biggest technical challenge you solved?"
> **Answer:** *"Handling JVM memory limits and PySpark shuffle overhead during the 12.5M row aggregation. Initially, Spark jobs threw memory overhead errors during cross-table joins. We optimized the pipeline by filtering at the partition level (`aggregation_level == 0`), tuning Spark executor memory (`--executor-memory 512m`, `--conf spark.executor.memoryOverhead=128m`), and selecting the Spark Standalone cluster manager."*

---

## 📋 7. Pre-Evaluation Demonstration Checklist

Before entering the evaluation room, ensure your system is prepared:

- [ ] **Docker Desktop** is running (all status indicators green).
- [ ] Run `docker ps` to verify all 6 cluster containers are UP.
- [ ] Open Browser Tab 1: **Streamlit Dashboard** (`http://localhost:8501`).
- [ ] Open Browser Tab 2: **HDFS NameNode Web UI** (`http://localhost:9870`).
- [ ] Open Browser Tab 3: **Spark Master Web UI** (`http://localhost:8080`).
- [ ] Open Browser Tab 4: **MongoDB Atlas Collections** (`cloud.mongodb.com`).
- [ ] Open **VS Code** with `src/analytics/covid_analyzer.py` and `docker-compose.yml` open.
- [ ] Terminal window open with virtual environment activated (`venv\Scripts\activate`).
