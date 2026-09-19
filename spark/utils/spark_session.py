"""
spark_session.py — SparkSession Factory Utility
Project: COVID-19 Big Data Analytics Platform
Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)

Provides a centralized SparkSession builder used by all Spark jobs.
Supports local mode (testing) and cluster mode (production).
"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = "COVID-Analytics",
    mode: str = "cluster",
    executor_memory: str = "2g",
    executor_cores: int = 2,
) -> SparkSession:
    """
    Build and return a SparkSession.

    Args:
        app_name:        Spark application name (shown in Spark UI).
        mode:            'cluster' for production, 'local' for testing.
        executor_memory: Memory per executor (e.g. '2g').
        executor_cores:  CPU cores per executor.

    Returns:
        SparkSession: Configured and active SparkSession.
    """
    if mode == "local":
        master = "local[*]"
        logger.info("Starting SparkSession in LOCAL mode.")
    else:
        master = "spark://master:7077"
        logger.info("Starting SparkSession in CLUSTER mode (spark://master:7077).")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", str(executor_cores))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.defaultFS", "hdfs://master:9000")
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/covid_analytics")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {spark.version} | App: {app_name}")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop a SparkSession."""
    if spark:
        app_name = spark.sparkContext.appName
        spark.stop()
        logger.info(f"SparkSession stopped: {app_name}")
