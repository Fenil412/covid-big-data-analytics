# Architecture

The project retains Streamlit and adds Hadoop/Spark data processing. CSVs are downloaded to dataset/, copied into the NameNode container, then uploaded using hdfs dfs -put. Services provide a NameNode, ResourceManager, two DataNodes, two NodeManagers, Spark master/driver and two Spark executors. Docker Compose runs on one host; HDFS blocks and Spark tasks are distributed across worker containers, not three physical hosts.

Flow: COVID CSVs → HDFS /covid/input (replication 2) → distributed Spark → HDFS /covid/output Parquet → output/ or MongoDB Atlas → Streamlit.

Ports: NameNode UI 9870, ResourceManager 8088, Spark UI 8080, Streamlit 8501; HDFS RPC master:9000.
