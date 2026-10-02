"""
spark_session.py — SparkSession factory (local, Spark standalone, or YARN).
"""

import os
import logging
from typing import Optional
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_ATLAS_URI = os.getenv("MONGODB_ATLAS_URI", "")
_MONGO_DB = os.getenv("MONGODB_DATABASE", "covid_analytics")
_HDFS_URI = os.getenv("HDFS_NAMENODE_URI", "hdfs://master:9000")


def get_spark_session(
    app_name: str = "COVID-Analytics",
    mode: str = "yarn",
    executor_memory: Optional[str] = None,
    executor_cores: Optional[int] = None,
) -> SparkSession:
    """
    Build a SparkSession.

    mode:
      - 'local'    : single-machine (development)
      - 'cluster'  : Spark standalone (spark://spark-master:7077)
      - 'yarn'     : YARN on the 3-node Hadoop cluster (recommended for Docker)
    """
    mem = executor_memory or os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    cores = str(executor_cores or os.getenv("SPARK_EXECUTOR_CORES", "2"))

    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active SparkSession (%s).", active.sparkContext.master)
        return active

    if mode == "local":
        master = "local[*]"
        logger.info("Starting SparkSession in LOCAL mode.")
    elif mode == "cluster":
        master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
        logger.info("Starting SparkSession in Spark standalone mode (%s).", master)
    else:
        master = "yarn"
        logger.info("Starting SparkSession on YARN (ResourceManager).")

    atlas_output_uri = _ATLAS_URI.rstrip("/")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.executor.memory", mem)
        .config("spark.executor.cores", cores)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.defaultFS", _HDFS_URI)
    )

    if atlas_output_uri:
        builder = (builder
            .config("spark.mongodb.output.uri", atlas_output_uri)
            .config("spark.mongodb.input.uri", atlas_output_uri)
            .config("spark.mongodb.output.database", _MONGO_DB)
            .config("spark.mongodb.output.connection.ssl.enabled", "true")
            .config("spark.mongodb.output.connection.ssl.invalidHostNameAllowed", "false"))

    if mode == "yarn":
        builder = (
            builder
            .config("spark.yarn.am.waitTime", "600s")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.executor.instances", os.getenv("SPARK_EXECUTOR_INSTANCES", "2"))
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created | Version: %s | Master: %s", spark.version, master)
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    if spark:
        app_name = spark.sparkContext.appName
        spark.stop()
        logger.info("SparkSession stopped: %s", app_name)
