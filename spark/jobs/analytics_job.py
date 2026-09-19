"""
analytics_job.py — Main Spark Job Entry Point
Project: COVID-19 Big Data Analytics Platform
Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)

Entry point for submitting the full COVID-19 analytics pipeline to
the Spark cluster. Orchestrates all stages: ingestion → cleaning →
analytics → MongoDB storage.

Submit command (from inside master container):
    spark-submit \\
        --master spark://master:7077 \\
        --executor-memory 2g \\
        --executor-cores 2 \\
        /opt/spark/jobs/analytics_job.py
"""

import sys
import logging
import argparse
from datetime import datetime

# Add project root to path
sys.path.insert(0, "/opt/spark")

from utils.spark_session import get_spark_session, stop_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("analytics_job")


def parse_args():
    parser = argparse.ArgumentParser(description="COVID-19 Big Data Analytics Job")
    parser.add_argument("--mode", default="cluster", choices=["cluster", "local"],
                        help="Spark run mode: 'cluster' or 'local' (default: cluster)")
    parser.add_argument("--input-path", default="hdfs://master:9000/covid/raw",
                        help="HDFS path to raw CSV data")
    parser.add_argument("--output-path", default="hdfs://master:9000/covid/output",
                        help="HDFS path to write processed output")
    parser.add_argument("--skip-mongo", action="store_true",
                        help="Skip MongoDB loading step (useful for testing)")
    return parser.parse_args()


def run_pipeline(spark, args):
    """
    Orchestrate the full analytics pipeline.
    Each module (cleaning, analytics, mongo) is imported here to
    keep them independently testable.
    """
    from src.processing.data_cleaner import DataCleaner
    from src.analytics.covid_analyzer import CovidAnalyzer
    if not args.skip_mongo:
        from src.mongodb.mongo_loader import MongoLoader

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("  COVID-19 Analytics Pipeline Started")
    logger.info(f"  Input  : {args.input_path}")
    logger.info(f"  Output : {args.output_path}")
    logger.info("=" * 60)

    # ── Stage 1: Load raw data ────────────────────────────────────────────────
    logger.info("[Stage 1] Loading raw data from HDFS...")
    raw_df = spark.read.option("header", True).option("inferSchema", True).csv(args.input_path)
    raw_count = raw_df.count()
    logger.info(f"  Loaded {raw_count:,} records.")

    # ── Stage 2: Data Cleaning ────────────────────────────────────────────────
    logger.info("[Stage 2] Cleaning and transforming data...")
    cleaner = DataCleaner(spark)
    clean_df = cleaner.clean(raw_df)
    logger.info(f"  Records after cleaning: {clean_df.count():,}")

    # ── Stage 3: Analytics ────────────────────────────────────────────────────
    logger.info("[Stage 3] Running distributed analytics...")
    analyzer = CovidAnalyzer(spark)
    results = analyzer.run_all(clean_df)
    for name, df in results.items():
        logger.info(f"  {name}: {df.count()} aggregated rows")
        df.write.mode("overwrite").parquet(f"{args.output_path}/{name}")

    # ── Stage 4: MongoDB Load ─────────────────────────────────────────────────
    if not args.skip_mongo:
        logger.info("[Stage 4] Loading results into MongoDB...")
        loader = MongoLoader()
        for name, df in results.items():
            loader.load(df, collection=name)
            logger.info(f"  Loaded '{name}' into MongoDB.")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info(f"  Pipeline completed in {elapsed:.1f}s")
    logger.info("=" * 60)


def main():
    args = parse_args()
    spark = get_spark_session(app_name="COVID-Analytics-Job", mode=args.mode)
    try:
        run_pipeline(spark, args)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
