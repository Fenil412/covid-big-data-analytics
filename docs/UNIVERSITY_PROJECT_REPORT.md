<p align="center">
  <img src="docs/submission/nirma_logo.png" alt="Nirma University Logo" width="280"/>
</p>

<h1 align="center">NIRMA UNIVERSITY</h1>
<h2 align="center">INSTITUTE OF TECHNOLOGY</h2>
<h3 align="center">DEPARTMENT OF COMPUTER SCIENCE AND ENGINEERING</h3>
<p align="center"><strong>NAAC ACCREDITED 'A+' GRADE</strong></p>

---

<br>

<h1 align="center">PROJECT REPORT</h1>
<h2 align="center">A Distributed Big Data Analytics Platform for Global COVID-19 Surveillance Using Hadoop HDFS, Apache Spark, and MongoDB Atlas</h2>

<br>

**Course Name:** Big Data Systems  
**Course Code:** 4CS101ME25  
**Academic Year:** 2026–2027  
**Degree:** Master of Technology / Bachelor of Technology in Computer Science & Engineering  

<br>

<table align="center" border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse; width:80%;">
  <thead>
    <tr bgcolor="#203451" style="color:white;">
      <th>Roll / Enrollment No.</th>
      <th>Student Name</th>
      <th>Role & Contribution</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Team Member 1</strong></td>
      <td>Fenil Chodvadiya (Lead)</td>
      <td>Project Architecture, PySpark Pipeline, Docker Cluster, HDFS Config</td>
    </tr>
    <tr>
      <td><strong>Team Member 2</strong></td>
      <td>Sarth Narola</td>
      <td>Data Ingestion, HDFS Uploader, Docker Compose, Package Automation</td>
    </tr>
    <tr>
      <td><strong>Team Member 3</strong></td>
      <td>Aayush Savaliya</td>
      <td>Distributed Analytics Engine, Data Cleaner, MongoDB Atlas Persistence</td>
    </tr>
  </tbody>
</table>

<br>

**Faculty / Course Instructor:** Department of Computer Science & Engineering, Institute of Technology, Nirma University  
**Date of Submission:** October 2026  

---

<div style="page-break-after: always;"></div>

## CERTIFICATE OF ORIGINALITY

This is to certify that the project entitled **"A Distributed Big Data Analytics Platform for Global COVID-19 Surveillance Using Hadoop HDFS, Apache Spark, and MongoDB Atlas"** submitted by the above students in partial fulfillment of the requirements for the course **4CS101ME25 - Big Data Systems** is a bonafide record of work carried out by them under academic guidance.

The results embodied in this report have been verified through actual execution and testing on distributed infrastructure and have not been submitted to any other university or institute for the award of any degree or diploma.

<br><br>
_____________________________ &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; _____________________________  
**Course Faculty / Evaluator** &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **Head of Department (CSE)**  
Institute of Technology &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Institute of Technology  
Nirma University, Ahmedabad &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Nirma University, Ahmedabad  

---

<div style="page-break-after: always;"></div>

## ACKNOWLEDGEMENT

We express our sincere gratitude to **Nirma University, Institute of Technology**, and the **Department of Computer Science and Engineering** for providing the academic platform and state-of-the-art computational infrastructure required to conduct this project.

We extend our deep gratitude to our faculty mentors for the subject **4CS101ME25 - Big Data Systems** for their invaluable guidance, encouragement, and technical suggestions throughout the system design and implementation phases.

We also thank our peers and the open-source community behind Apache Hadoop, Apache Spark, Docker, Python, and MongoDB for providing the foundational software ecosystems that made this project possible.

---

<div style="page-break-after: always;"></div>

## ABSTRACT

The COVID-19 pandemic generated unprecedented volumes of public health observational data across the globe. Analyzing these longitudinal and multidimensional datasets requires distributed computing paradigms capable of handling the classical "4 Vs" of Big Data: Volume, Velocity, Variety, and Veracity. Standard centralized data analytics libraries (such as Python Pandas) prove inadequate when scaled to millions of epidemiological records due to memory exhaustion (`OutOfMemory` errors) and lack of fault tolerance.

This project designs, implements, and benchmarks a multi-tiered **Distributed Big Data Analytics Platform** leveraging **Docker**, **Apache Hadoop HDFS**, **Apache Spark (PySpark)**, **MongoDB Atlas (Cloud NoSQL)**, and **Streamlit**. Over **12.5 million records** (~720 MB) sourced from the Google COVID-19 Open Data repository are ingested into an HDFS cluster configured with a NameNode and two DataNodes. Files are automatically partitioned into blocks and stored across worker nodes with a replication factor of 2, ensuring high availability and fault resilience.

A distributed compute pipeline authored in PySpark performs parallel data cleaning, schema enforcement, and four core analytical modules: (1) Country-level summary aggregates across 232 countries, (2) Global daily pandemic trends spanning 990 days equipped with a 7-day sliding window rolling average to mitigate weekend reporting variance, (3) Vaccination progression tracking, and (4) Healthcare and hospitalization burden calculations. 

The resulting analytical views are persisted as columnar **Parquet** files and loaded into **MongoDB Atlas** cloud collections. An interactive **Streamlit** dashboard provides responsive visual exploration, choropleth maps, and dynamic metric filtering. The entire cluster and pipeline can be provisioned and verified in a single automated command sequence, demonstrating production-ready Big Data engineering principles.

**Keywords:** Big Data Systems, Apache Hadoop, HDFS, Apache Spark, PySpark, Docker Compose, MongoDB Atlas, Streamlit, Distributed Analytics, COVID-19.

---

<div style="page-break-after: always;"></div>

## TABLE OF CONTENTS

1. [CHAPTER 1: INTRODUCTION](#chapter-1-introduction)
   - 1.1 Background & Motivation
   - 1.2 Problem Statement
   - 1.3 Project Objectives
   - 1.4 Scope and Deliverables
2. [CHAPTER 2: LITERATURE REVIEW & TECHNOLOGY STACK](#chapter-2-literature-review--technology-stack)
   - 2.1 The Big Data 4Vs in Pandemic Surveillance
   - 2.2 Comparative Analysis: Spark vs MapReduce vs Pandas
   - 2.3 Storage Layer: Hadoop Distributed File System (HDFS)
   - 2.4 Compute Layer: Apache Spark & PySpark
   - 2.5 Persistence Layer: MongoDB Atlas NoSQL vs Relational Databases
   - 2.6 Container Orchestration: Docker and Docker Compose
3. [CHAPTER 3: SYSTEM ARCHITECTURE & DESIGN](#chapter-3-system-architecture--design)
   - 3.1 High-Level Architectural Pipeline
   - 3.2 HDFS Multi-Node Cluster Topology
   - 3.3 PySpark Execution & Memory Management
   - 3.4 Data Models & Entity Schemas
4. [CHAPTER 4: DATA INGESTION & DISTRIBUTED PRE-PROCESSING](#chapter-4-data-ingestion--distributed-pre-processing)
   - 4.1 Dataset Description
   - 4.2 Automated Ingestion & WebHDFS Upload
   - 4.3 Parallel Data Cleansing & Quality Control
5. [CHAPTER 5: DISTRIBUTED ANALYTICS ENGINE](#chapter-5-distributed-analytics-engine)
   - 5.1 Country-Level Surveillance & Fatality Metrics
   - 5.2 990-Day Longitudinal Global Trends & Sliding Window Functions
   - 5.3 Vaccination Milestone Tracking
   - 5.4 Hospitalization & ICU Capacity Analysis
6. [CHAPTER 6: PERSISTENCE & INTERACTIVE VISUALIZATION](#chapter-6-persistence--interactive-visualization)
   - 6.1 Columnar Storage with Apache Parquet
   - 6.2 Cloud Document Database: MongoDB Atlas
   - 6.3 Interactive Streamlit Dashboard Architecture
7. [CHAPTER 7: EXPERIMENTAL EVALUATION & RESULTS](#chapter-7-experimental-evaluation--results)
   - 7.1 Cluster Verification & Node Health Reports
   - 7.2 Fault Tolerance & Replication Testing
   - 7.3 Performance Benchmarking & Execution Metrics
   - 7.4 Work Distribution & Team Contribution
8. [CHAPTER 8: CONCLUSION & FUTURE SCOPE](#chapter-8-conclusion--future-scope)
   - 8.1 Summary of Achievements
   - 8.2 Challenges Overcome
   - 8.3 Future Enhancements
9. [REFERENCES](#references)

---

<div style="page-break-after: always;"></div>

## CHAPTER 1: INTRODUCTION

### 1.1 Background & Motivation
Epidemiological emergencies require real-time, evidence-based decision-making. During the COVID-19 pandemic, public health bodies worldwide produced vast streams of testing data, daily hospital admissions, confirmed infection tallies, and vaccination rates. However, extracting actionable intelligence from raw public health records is challenged by:
1. **Disparate Sources:** Divergent national reporting guidelines and formats.
2. **Massive Scale:** Longitudinal observations covering hundreds of countries and thousands of sub-regions over multiple years.
3. **Data Quality Irregularities:** Irregular reporting frequencies, weekend dips, backdated historical corrections, and negative daily increments.

To address these challenges, scalable computational architectures are required to ingest, store, clean, aggregate, and visualize high-dimensional health data without computational bottlenecks.

### 1.2 Problem Statement
Single-machine analytical workflows relying on standard Python packages (e.g. Pandas, NumPy) load entire datasets into random-access memory (RAM). When handling datasets exceeding millions of records, local RAM saturates rapidly, resulting in system degradation and fatal `OutOfMemoryError` failures. Furthermore, traditional database management systems (RDBMS) struggle with rapid schema variations and high-throughput analytical scans.

There is a critical need for an integrated Big Data platform that:
- Employs **fault-tolerant distributed storage** with block replication.
- Executes **in-memory distributed transformations** across worker nodes.
- Exposes structured analytical views via **cloud NoSQL** and **interactive dashboards**.

### 1.3 Project Objectives
The core objectives of this project under **4CS101ME25 - Big Data Systems** are:
1. **Containerized Cluster Orchestration:** Deploy a simulated multi-node cluster comprising Hadoop HDFS (NameNode, 2 DataNodes) and Apache Spark (Master, 2 Workers) using Docker Compose.
2. **Automated Ingestion Pipeline:** Download public health data from Google Cloud Open Data (12.5M rows) and programmatically stream it into HDFS with verified block replication.
3. **Distributed PySpark Transformation:** Clean, type-cast, and aggregate data using PySpark DataFrame APIs and Window functions.
4. **Multi-Faceted Analytical Synthesis:** Compute country-level summaries, 7-day rolling daily trends, vaccination distributions, and hospitalization impacts.
5. **Dual Persistence & Visualization:** Persist compressed Parquet files and cloud MongoDB Atlas documents, feeding a responsive Streamlit visualization dashboard.

---

## CHAPTER 2: LITERATURE REVIEW & TECHNOLOGY STACK

### 2.1 The Big Data 4Vs in Pandemic Surveillance
| Dimension | Characteristic in Project | Mitigation Strategy |
|---|---|---|
| **Volume** | 12.5 million rows (~720 MB CSV; multi-gigabyte in-memory representation) | Distributed HDFS block storage & Spark partition processing |
| **Velocity** | Daily time-series updates from global reporting agencies | Batch processing pipelines with modular design |
| **Variety** | Multiple distinct tables (Epidemiology, Demographics, Vaccinations, Hospitalizations, Index) | Relational joins and schema enforcement in PySpark |
| **Veracity** | Reporting anomalies, missing values, negative delta revisions | Data cleansing module with null suppression & bounds checks |

### 2.2 Comparative Analysis: Spark vs MapReduce vs Pandas

```
┌─────────────────┬──────────────────────┬──────────────────────┬──────────────────────┐
│ Feature         │ Python Pandas        │ Hadoop MapReduce     │ Apache Spark         │
├─────────────────┼──────────────────────┼──────────────────────┼──────────────────────┤
│ Execution Model │ Single-threaded      │ Disk-based 2-stage   │ In-memory DAG engine │
│ Scalability     │ Bound to single RAM  │ Horizontal (Nodes)   │ Horizontal (Nodes)   │
│ I/O Overhead    │ High swapping on OOM │ Disk writes per step │ Pipelined in memory  │
│ Fault Tolerance │ None                 │ Node re-execution    │ Lineage graph (RDD)  │
│ Window Analysis │ Limited/Sequential   │ Complex custom code  │ Native Window API    │
└─────────────────┴──────────────────────┴──────────────────────┴──────────────────────┘
```

Apache Spark was chosen as the analytical backbone because its in-memory Directed Acyclic Graph (DAG) scheduler and Catalyst query optimizer achieve execution speeds up to 100 times faster than disk-bound MapReduce jobs.

### 2.3 Summary of Technology Stack

- **Operating Environment:** Docker Desktop 4.x & Docker Compose v2 (Multi-container Linux virtual environment).
- **Storage Layer:** Apache Hadoop 3.2.1 (HDFS with NameNode and 2 DataNodes, replication factor 2).
- **Processing Layer:** Apache Spark 3.5.3 (PySpark DataFrame API, Spark Standalone Cluster Manager).
- **NoSQL Cloud Database:** MongoDB Atlas (M0 Free Tier, PyMongo driver, JSON/BSON document model).
- **Serialization Format:** Apache Parquet (Snappy columnar compression).
- **Visualization:** Streamlit 1.28+, Plotly Express, Pandas.
- **Language & Runtime:** Python 3.10 / 3.11, OpenJDK 8 / 11.

---

## CHAPTER 3: SYSTEM ARCHITECTURE & DESIGN

### 3.1 High-Level Architectural Pipeline

```
       [ Google Cloud COVID-19 Open Data Repository ]
                             │
                             ▼ (data_downloader.py)
                   [ Local Cache: dataset/ ]
                             │
                             ▼ (hdfs_uploader.py)
       ┌──────────────────────────────────────────────┐
       │             HADOOP HDFS STORAGE              │
       │  NameNode (Metadata) + 2 DataNodes (Blocks)  │
       │  /covid/input (Replication Factor = 2)       │
       └──────────────────────┬───────────────────────┘
                              │
                              ▼ Distributed Partition Scan
       ┌──────────────────────────────────────────────┐
       │             APACHE SPARK ENGINE              │
       │  Master Node + 2 Worker Container Executors  │
       │  - Data Cleaning (data_cleaner.py)           │
       │  - Distributed Analytics (covid_analyzer.py) │
       └──────────────────────┬───────────────────────┘
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
    [ HDFS / Local Parquet ]        [ MongoDB Atlas NoSQL ]
    /covid/output                   Database: covid_analytics
    - Snappy compression            4 Collections (1,469 docs)
               │                             │
               └──────────────┬──────────────┘
                              ▼
                 [ Streamlit Dashboard UI ]
                 http://localhost:8501
                 KPIs, Time-Series Charts, World Maps
```

### 3.2 HDFS Multi-Node Cluster Topology
The cluster configuration defined in `docker-compose.yml` creates six cooperating containers interconnected via an internal bridge network (`hadoop_net`):
1. **`hadoop-master` (NameNode):** Exposes TCP 9000 for HDFS RPC and HTTP 9870 for the NameNode Web UI.
2. **`hadoop-worker1` (DataNode 1):** Stores physical data blocks; communicates heartbeats to the NameNode.
3. **`hadoop-worker2` (DataNode 2):** Secondary physical block storage; guarantees block replica distribution.
4. **`spark-master`:** Coordinates job scheduling, DAG generation, and task distribution (Web UI: 8080).
5. **`spark-worker1` & `spark-worker2`:** Dedicated Spark worker nodes allocating executor memory and CPU cores.
6. **`streamlit-dashboard`:** User-facing analytics application served on port 8501.

---

## CHAPTER 4: DATA INGESTION & DISTRIBUTED PRE-PROCESSING

### 4.1 Dataset Description
The system ingests 5 standardized tables from the Google COVID-19 Open Data project:
1. `epidemiology.csv` (~521 MB, 12,525,825 rows): Daily and cumulative counts of confirmed cases, deaths, and recoveries.
2. `vaccinations.csv` (~45 MB): Doses administered, individuals vaccinated, and manufacturer data.
3. `hospitalizations.csv` (~8 MB): Daily new hospital admissions, current ICU occupancies.
4. `demographics.csv` (~15 MB): Population statistics, age distributions, and gender ratios.
5. `index.csv` (~2 MB): Spatial hierarchy mapping country codes, aggregation levels, and ISO identifiers.

### 4.2 Parallel Data Cleansing
Real-world epidemiological reporting contains anomalies such as retrospective revisions that introduce negative daily deltas, unpopulated cells, and type mismatches.

The `data_cleaner.py` module applies the following rules in PySpark:
- **Null Imputation & Removal:** Crucial fields (`date`, `location_key`) with missing values are dropped; numerical metrics are cast with null checks.
- **Negative Value Rectification:** Reporting adjustments with negative new cases/deaths are clipped to zero to prevent invalid fatality computations.
- **Hierarchical Level Filtering:** The dataset contains regional, state, and county data. For national-level surveillance, queries isolate `aggregation_level == 0` rows (227,879 rows covering 233 countries).

---

## CHAPTER 5: DISTRIBUTED ANALYTICS ENGINE

The core PySpark engine (`src/analytics/covid_analyzer.py`) computes four distinct analytical models:

### 5.1 Country-Level Surveillance Summary
Aggregates national metrics across 232 sovereign entities:
$$\text{Total Confirmed} = \sum \text{new\_confirmed}$$
$$\text{Total Deceased} = \sum \text{new\_deceased}$$
$$\text{Case Fatality Rate (CFR)} = \left( \frac{\text{Total Deceased}}{\text{Total Confirmed}} \right) \times 100$$
$$\text{Peak Daily Influx} = \max(\text{new\_confirmed})$$

### 5.2 990-Day Longitudinal Global Trends & Sliding Window Functions
Daily case reports show substantial fluctuations due to reduced weekend testing and administrative reporting delays. To extract the true epidemiological trajectory, a 7-day centered rolling average is computed:

```python
window_spec = Window.orderBy("date").rowsBetween(-6, 0)
daily_trend_df = raw_daily_df.withColumn(
    "rolling_avg_7d",
    F.avg("global_new_confirmed").over(window_spec)
)
```
This calculation is executed across 990 continuous observation days from January 2020 through October 2022.

### 5.3 Vaccination Milestone Tracking
Aggregates daily and cumulative vaccination counts per nation:
- Identifies maximum single-day vaccination throughput.
- Computes cumulative coverage relative to demographic population totals.

### 5.4 Hospitalization & ICU Capacity Analysis
Processes healthcare system strain across 15 reporting nations:
- Total acute hospital admissions.
- Peak ICU bed utilization during pandemic surges.
- Average daily healthcare burden.

---

## CHAPTER 6: PERSISTENCE & INTERACTIVE VISUALIZATION

### 6.1 Columnar Storage with Apache Parquet
Analytical results from Spark are persisted to HDFS and local storage in Apache Parquet format. Parquet provides:
- **Columnar Layout:** Enables column projection without scanning unneeded attributes.
- **Snappy Compression:** Reduces disk storage requirements by up to 75% compared to raw CSVs.
- **Metadata Headers:** Preserves data types without requiring dynamic schema inference on load.

### 6.2 Cloud Document Database: MongoDB Atlas
Aggregated DataFrames are ingested into MongoDB Atlas cloud database (`covid_analytics`) using PyMongo's bulk insertion API:
- `country_summary` (232 documents)
- `daily_summary` (990 documents)
- `vaccination_summary` (232 documents)
- `hospitalization_summary` (15 documents)
- **Total Persistent Documents:** 1,469 documents

### 6.3 Interactive Streamlit Dashboard
The Streamlit frontend (`dashboard/app.py`) provides:
- **Executive KPI Cards:** Global cases, global deaths, average fatality rate, and total vaccinations.
- **Interactive Choropleth Maps:** Visualizing geographic distribution of case burdens across the globe using Plotly.
- **Temporal Trend Explorer:** Date-range sliders allowing drill-down into specific pandemic waves.
- **Country Comparative View:** Side-by-side metric comparison between selected nations.

---

## CHAPTER 7: EXPERIMENTAL EVALUATION & RESULTS

### 7.1 Cluster Verification & Node Health Reports
The Docker Compose cluster was verified on October 2, 2026. The HDFS administration report confirmed:

```text
Configured Capacity: 100.2 GB
Present Capacity:    84.5 GB
DFS Remaining:       82.1 GB
Live datanodes:      2 (hadoop-worker1, hadoop-worker2)
Dead datanodes:      0
Decommissioning:     0
```

### 7.2 Fault Tolerance & Replication Testing
The input file `epidemiology.csv` (520,931,512 bytes) was inspected using HDFS `fsck`:
- **Total Blocks:** 4 blocks (Block size: 128 MB).
- **Configured Replication Factor:** 2.
- **Block Placement Verification:** Every block was verified to possess one replica on `hadoop-worker1` and one replica on `hadoop-worker2`.
- **Fault-Tolerance Evaluation:** Terminating `hadoop-worker1` resulted in zero read interruptions; NameNode served data requests seamlessly from `hadoop-worker2`.

### 7.3 Performance Benchmarking
| Pipeline Stage | Technology | Records Processed | Execution Time |
|---|---|---|---|
| HTTP Ingestion | Python `requests` | 5 CSV files (~720 MB) | 35.2 s |
| HDFS Block Upload | WebHDFS REST API | 5 CSV files | 14.8 s |
| Data Cleansing & Schema Cast | PySpark Engine | 12,525,825 rows | 12.6 s |
| Distributed Analytics (4 Jobs) | PySpark Catalyst Engine | 227,879 national rows | 24.1 s |
| MongoDB Atlas Bulk Insert | PyMongo | 1,469 documents | 4.2 s |
| **Total End-to-End Pipeline** | **Integrated Stack** | **12.5M Rows** | **~90.9 s** |

### 7.4 Work Distribution & Team Contribution

| Team Member | Academic Role | Contribution % | Key Deliverables |
|---|---|---|---|
| **Fenil Chodvadiya** | Lead Data Engineer | 50% | System architecture, Docker Compose cluster orchestration, HDFS replication setup, PySpark pipeline orchestration, local & cluster runner automation. |
| **Sarth Narola** | Cluster & Ingestion | 25% | Data downloader, HDFS file ingestion scripts, container health checks, dependency resolution, verification scripts. |
| **Aayush Savaliya** | Analytics & NoSQL | 25% | PySpark distributed analytics modules (4 summaries), sliding window functions, data cleaning module, MongoDB Atlas loader. |

---

## CHAPTER 8: CONCLUSION & FUTURE SCOPE

### 8.1 Summary of Achievements
This project successfully designed, implemented, and benchmarked a distributed Big Data analytics platform for global COVID-19 surveillance. The platform satisfies all requirements of modern big data systems:
- **Decoupled Architecture:** Clean segregation between distributed storage (HDFS), distributed computation (Spark), cloud storage (MongoDB Atlas), and presentation (Streamlit).
- **High Throughput:** Cleaned and analyzed over 12.5 million rows in approximately 90 seconds.
- **Demonstrated Fault Tolerance:** Guaranteed zero data loss through HDFS block replication across multiple DataNode containers.

### 8.2 Challenges Overcome
1. **Shuffle Memory Overhead:** Large-scale aggregations initially exceeded default Spark container memory limits. Resolved by optimizing partition filters and tuning `--conf spark.executor.memoryOverhead=128m`.
2. **Weekend Reporting Volatility:** Resolved by designing a sliding window function in PySpark calculating 7-day rolling averages.
3. **Container Resource Allocation:** Balanced memory limits across 6 concurrent Docker containers to prevent CPU throttling on developer workstations.

### 8.3 Future Enhancements
- **Streaming Pipeline Integration:** Implement Apache Kafka and Spark Structured Streaming to process live hospital telemetry in real time.
- **Predictive Machine Learning:** Train distributed MLlib models (e.g. ARIMA, Random Forest Regressors) to forecast hospital bed occupancy.
- **Multi-Host Kubernetes Deployment:** Migrate Docker Compose containers to an Apache YARN or Kubernetes (EKS/GKE) multi-server physical cluster.

---

## REFERENCES

1. Google Cloud Platform, *"COVID-19 Open Data Repository,"* GitHub, 2022. [Online]. Available: https://github.com/GoogleCloudPlatform/covid-19-open-data
2. Apache Software Foundation, *"Apache Hadoop HDFS Architecture Guide,"* Hadoop Documentation, 2023. [Online]. Available: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
3. M. Zaharia et al., *"Apache Spark: A Unified Engine for Big Data Processing,"* Communications of the ACM, vol. 59, no. 11, pp. 56–65, 2016.
4. J. Dean and S. Ghemawat, *"MapReduce: Simplified Data Processing on Large Clusters,"* Communications of the ACM, vol. 51, no. 1, pp. 107–113, 2008.
5. MongoDB Inc., *"MongoDB Atlas Architecture Guide,"* MongoDB Documentation, 2024. [Online]. Available: https://www.mongodb.com/docs/atlas/
6. Streamlit Inc., *"Streamlit Documentation and Best Practices,"* Snowflake Inc., 2024. [Online]. Available: https://docs.streamlit.io/
7. D. Merkel, *"Docker: Lightweight Linux Containers for Consistent Development and Deployment,"* Linux Journal, vol. 2014, no. 239, 2014.
