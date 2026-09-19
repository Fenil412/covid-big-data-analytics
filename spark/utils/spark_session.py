"""
spark_session.py — SparkSession Factory Utility
Project: COVID-19 Big Data Analytics Platform
Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)

Provides a centralized SparkSession builder used by all Spark jobs.
Supports local mode (testing) and cluster mode (production).

MongoDB Atlas URI is loaded from the MONGODB_ATLAS_URI environment variable.
Never hardcode credentials — always use .env (see .env.example).
"""

import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load .env file
load_dotenv()

logger = logging.getLogger(__name__)

# ── Read Atlas URI from environment ────────────────────────────────────────────
_ATLAS_URI = os.getenv("MONGODB_ATLAS_URI", "")
_MONGO_DB  = os.getenv("MONGODB_DATABASE", "covid_analytics")
_HDFS_URI  = os.getenv("HDFS_NAMENODE_URI", "hdfs://master:9000")


def get_spark_session(
    app_name: str = "COVID-Analytics",
    mode: str = "cluster",
    executor_memory: str = None,
    executor_cores: int = None,
) -> SparkSession:
    """
    Build and return a configured SparkSession.

    Args:
        app_name:        Spark application name (shown in Spark UI).
        mode:            'cluster' for production, 'local' for testing.
        executor_memory: Memory per executor. Defaults to SPARK_EXECUTOR_MEMORY env var or '2g'.
        executor_cores:  CPU cores per executor. Defaults to SPARK_EXECUTOR_CORES env var or 2.

    Returns:
        SparkSession: Configured and active SparkSession.

    Raises:
        EnvironmentError: If MONGODB_ATLAS_URI is not set.
    """
    if not _ATLAS_URI:
        raise EnvironmentError(
            "MONGODB_ATLAS_URI is not set in .env!\n"
            "  1. cp .env.example .env\n"
            "  2. Set your Atlas connection string in .env"
        )

    mem   = executor_memory or os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
    cores = str(executor_cores or os.getenv("SPARK_EXECUTOR_CORES", "2"))

    if mode == "local":
        master = "local[*]"
        logger.info("Starting SparkSession in LOCAL mode.")
    else:
        master = os.getenv("SPARK_MASTER", "spark://master:7077")
        logger.info(f"Starting SparkSession in CLUSTER mode ({master}).")

    # MongoDB Atlas output URI (for Spark MongoDB connector)
    atlas_output_uri = f"{_ATLAS_URI.rstrip('/')}"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.executor.memory", mem)
        .config("spark.executor.cores", cores)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # HDFS integration
        .config("spark.hadoop.fs.defaultFS", _HDFS_URI)
        # MongoDB Atlas connector (mongo-spark-connector)
        .config("spark.mongodb.output.uri", atlas_output_uri)
        .config("spark.mongodb.input.uri",  atlas_output_uri)
        .config("spark.mongodb.output.database", _MONGO_DB)
        # TLS required for Atlas
        .config("spark.mongodb.output.connection.ssl.enabled", "true")
        .config("spark.mongodb.output.connection.ssl.invalidHostNameAllowed", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created | Version: {spark.version} | App: {app_name}")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop a SparkSession."""
    if spark:
        app_name = spark.sparkContext.appName
        spark.stop()
        logger.info(f"SparkSession stopped: {app_name}")
