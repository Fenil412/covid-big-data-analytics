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
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Support both local dev and Docker container execution
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # spark/jobs -> project root
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "/opt/spark")       # Docker container path
sys.path.insert(0, "/opt/spark-apps")  # bde2020 container path

from spark.utils.spark_session import get_spark_session, stop_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("analytics_job")


def parse_args():
    parser = argparse.ArgumentParser(description="COVID-19 Big Data Analytics Job")
    parser.add_argument("--mode", default="local", choices=["cluster", "local"],
                        help="Spark run mode: 'local' (default) or 'cluster'")
    parser.add_argument("--input-path",
                        default=str(PROJECT_ROOT / "dataset"),
                        help="Path to raw CSV data (local path or hdfs://...)")
    parser.add_argument("--output-path",
                        default=str(PROJECT_ROOT / "output"),
                        help="Path to write processed output")
    parser.add_argument("--skip-mongo", action="store_true",
                        help="Skip MongoDB loading step (useful for testing)")
    return parser.parse_args()


def run_pipeline(spark, args):
    """
    Orchestrate the full analytics pipeline.
    Reads individual CSVs from input_path, joins country names,
    filters to country-level, runs analytics, and stores results.
    """
    from src.analytics.covid_analyzer import CovidAnalyzer
    if not args.skip_mongo:
        from src.mongodb.mongo_loader import MongoLoader
    from pyspark.sql import functions as F

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("  COVID-19 Analytics Pipeline Started")
    logger.info(f"  Input  : {args.input_path}")
    logger.info(f"  Output : {args.output_path}")
    logger.info("=" * 60)

    input_path = args.input_path.rstrip("/")

    # ── Stage 1: Load raw data from individual CSVs ───────────────────────────
    logger.info("[Stage 1] Loading raw data...")

    # Read index.csv for country names
    logger.info("  Reading index.csv...")
    idx_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{input_path}/index.csv")
        .select("location_key", "country_name")
        .dropDuplicates(["location_key"])
    )

    # Read epidemiology.csv (primary dataset)
    logger.info("  Reading epidemiology.csv...")
    epi_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{input_path}/epidemiology.csv")
    )
    logger.info(f"  Raw rows (all regions): {epi_df.count():,}")

    # Filter to country-level only (location_key = 2 chars like "US", "IN")
    epi_df = epi_df.filter(F.length(F.col("location_key")) == 2)
    logger.info(f"  Country-level rows: {epi_df.count():,}")

    # Join country names
    epi_df = epi_df.join(F.broadcast(idx_df), on="location_key", how="left")

    # Cast numeric columns
    for col in ["new_confirmed", "new_deceased", "new_recovered",
                "cumulative_confirmed", "cumulative_deceased"]:
        if col in epi_df.columns:
            epi_df = epi_df.withColumn(col, F.col(col).cast("double"))

    # Drop rows without case data
    epi_df = epi_df.filter(
        F.col("new_confirmed").isNotNull() |
        F.col("cumulative_confirmed").isNotNull()
    )

    # ── Join vaccinations ──────────────────────────────────────────────────────
    logger.info("  Reading vaccinations.csv...")
    vacc_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{input_path}/vaccinations.csv")
        .filter(F.length(F.col("location_key")) == 2)
    )
    vacc_cols = ["new_persons_vaccinated", "cumulative_persons_vaccinated"]
    for col in vacc_cols:
        if col in vacc_df.columns:
            vacc_df = vacc_df.withColumn(col, F.col(col).cast("double"))
    avail_vacc = [c for c in vacc_cols if c in vacc_df.columns]
    if avail_vacc:
        clean_df = epi_df.join(
            vacc_df.select("date", "location_key", *avail_vacc),
            on=["date", "location_key"], how="left"
        )
    else:
        clean_df = epi_df

    # ── Join hospitalizations ──────────────────────────────────────────────────
    logger.info("  Reading hospitalizations.csv...")
    hosp_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{input_path}/hospitalizations.csv")
        .filter(F.length(F.col("location_key")) == 2)
    )
    hosp_cols = ["new_hospitalized_patients", "current_hospitalized_patients"]
    for col in hosp_cols:
        if col in hosp_df.columns:
            hosp_df = hosp_df.withColumn(col, F.col(col).cast("double"))
    avail_hosp = [c for c in hosp_cols if c in hosp_df.columns]
    if avail_hosp:
        clean_df = clean_df.join(
            hosp_df.select("date", "location_key", *avail_hosp),
            on=["date", "location_key"], how="left"
        )

    clean_df.cache()
    clean_count = clean_df.count()
    logger.info(f"  [OK] Final clean rows: {clean_count:,}")

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
