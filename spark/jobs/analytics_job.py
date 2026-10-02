"""
analytics_job.py — Spark job: HDFS CSV → distributed analytics → HDFS + MongoDB.
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Repo root locally; /opt/spark-apps when running inside spark-master container
_here = Path(__file__).resolve()
APP_ROOT = Path("/opt/spark-apps") if Path("/opt/spark-apps/src").exists() else _here.parents[2]
sys.path.insert(0, str(APP_ROOT))

from spark.utils.spark_session import get_spark_session, stop_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("analytics_job")


def parse_args():
    parser = argparse.ArgumentParser(description="COVID-19 Big Data Analytics Job")
    parser.add_argument(
        "--mode",
        default="yarn",
        choices=["yarn", "cluster", "local"],
        help="Execution mode: yarn (default), cluster (Spark standalone), or local",
    )
    parser.add_argument(
        "--input-path",
        default="hdfs://master:9000/covid/input",
        help="HDFS or local directory containing CSV files",
    )
    parser.add_argument(
        "--output-path",
        default="hdfs://master:9000/covid/output",
        help="HDFS or local directory for Parquet output",
    )
    parser.add_argument("--skip-mongo", action="store_true", help="Skip MongoDB load")
    return parser.parse_args()


def _load_csv(spark, base_path: str, name: str):
    path = f"{base_path.rstrip('/')}/{name}"
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


def build_clean_dataframe(spark, input_path: str):
    """Load multi-table CSVs from HDFS, join, filter to country level, clean."""
    from pyspark.sql import functions as F
    from src.processing.data_cleaner import DataCleaner

    logger.info("Loading datasets from %s ...", input_path)
    idx_df = (
        _load_csv(spark, input_path, "index.csv")
        .select("location_key", "country_name")
        .dropDuplicates(["location_key"])
    )

    epi_df = _load_csv(spark, input_path, "epidemiology.csv")

    epi_df = epi_df.filter(F.length(F.col("location_key")) == 2)
    epi_df = epi_df.join(F.broadcast(idx_df), on="location_key", how="left")
    for col in (
        "cumulative_confirmed",
        "cumulative_deceased",
        "cumulative_recovered",
        "new_confirmed",
        "new_deceased",
        "new_recovered",
    ):
        if col in epi_df.columns:
            epi_df = epi_df.withColumn(col, F.col(col).cast("double"))

    vacc_df = (
        _load_csv(spark, input_path, "vaccinations.csv")
        .filter(F.length(F.col("location_key")) == 2)
    )
    vacc_cols = [
        "new_persons_vaccinated", "cumulative_persons_vaccinated",
        "new_vaccine_doses_administered", "cumulative_vaccine_doses_administered",
    ]
    avail_vacc = [c for c in vacc_cols if c in vacc_df.columns]
    if avail_vacc:
        clean_df = epi_df.join(
            vacc_df.select("date", "location_key", *avail_vacc),
            on=["date", "location_key"],
            how="left",
        )
    else:
        clean_df = epi_df

    hosp_df = (
        _load_csv(spark, input_path, "hospitalizations.csv")
        .filter(F.length(F.col("location_key")) == 2)
    )
    hosp_cols = ["new_hospitalized_patients", "current_hospitalized_patients"]
    avail_hosp = [c for c in hosp_cols if c in hosp_df.columns]
    if avail_hosp:
        clean_df = clean_df.join(
            hosp_df.select("date", "location_key", *avail_hosp),
            on=["date", "location_key"],
            how="left",
        )

    clean_df = clean_df.filter(
        F.col("new_confirmed").isNotNull() | F.col("cumulative_confirmed").isNotNull()
    )

    cleaner = DataCleaner(spark)
    clean_df = cleaner.clean(clean_df)
    clean_df.cache()
    logger.info("  Cleaned country-level data cached.")
    return clean_df


def build_regional_dataframe(spark, input_path: str):
    """Load sub-national epidemiology rows (states/provinces) from HDFS."""
    from pyspark.sql import functions as F
    from src.processing.data_cleaner import DataCleaner

    logger.info("Loading regional (sub-national) data from %s ...", input_path)
    idx_df = (
        _load_csv(spark, input_path, "index.csv")
        .select("location_key", "country_name")
        .dropDuplicates(["location_key"])
    )
    country_lookup = (
        idx_df.filter(F.length(F.col("location_key")) == 2)
        .select(
            F.col("location_key").alias("country_code"),
            F.col("country_name").alias("parent_country"),
        )
    )

    epi_df = _load_csv(spark, input_path, "epidemiology.csv")
    epi_df = epi_df.filter(F.length(F.col("location_key")) > 2)
    epi_df = epi_df.join(F.broadcast(idx_df), on="location_key", how="left")
    epi_df = epi_df.withColumn("country_code", F.split(F.col("location_key"), "_").getItem(0))
    epi_df = epi_df.join(F.broadcast(country_lookup), on="country_code", how="left")

    for col in (
        "new_confirmed", "new_deceased", "new_recovered",
        "cumulative_confirmed", "cumulative_deceased", "cumulative_recovered",
        "new_persons_vaccinated", "cumulative_persons_vaccinated",
        "new_vaccine_doses_administered", "cumulative_vaccine_doses_administered",
    ):
        if col in epi_df.columns:
            epi_df = epi_df.withColumn(col, F.col(col).cast("double"))

    vacc_df = (
        _load_csv(spark, input_path, "vaccinations.csv")
        .filter(F.length(F.col("location_key")) > 2)
    )
    regional_vacc_cols = [
        "new_persons_vaccinated", "cumulative_persons_vaccinated",
        "new_vaccine_doses_administered", "cumulative_vaccine_doses_administered",
    ]
    available_regional_vacc_cols = [c for c in regional_vacc_cols if c in vacc_df.columns]
    if available_regional_vacc_cols:
        for col in available_regional_vacc_cols:
            vacc_df = vacc_df.withColumn(col, F.col(col).cast("double"))
        epi_df = epi_df.join(
            vacc_df.select("date", "location_key", *available_regional_vacc_cols),
            on=["date", "location_key"],
            how="left",
        )

    epi_df = epi_df.filter(
        F.col("new_confirmed").isNotNull() | F.col("cumulative_confirmed").isNotNull()
    )
    cleaner = DataCleaner(spark)
    regional_df = cleaner.clean(epi_df)
    logger.info("  Regional data prepared for aggregation.")
    return regional_df


def load_demographics(spark, input_path: str):
    from pyspark.sql import functions as F

    try:
        demo = _load_csv(spark, input_path, "demographics.csv")
        if "population" not in demo.columns:
            return None
        return demo.filter(F.length(F.col("location_key")) == 2).select(
            "location_key", "population"
        )
    except Exception as exc:
        logger.warning("Demographics not loaded (%s)", exc)
        return None


def run_pipeline(spark, args):
    from src.analytics.covid_analyzer import CovidAnalyzer
    if not args.skip_mongo:
        from src.mongodb.mongo_loader import MongoLoader

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("  COVID-19 Analytics Pipeline Started")
    logger.info("  Input  : %s", args.input_path)
    logger.info("  Output : %s", args.output_path)
    logger.info("  Mode   : %s", args.mode)
    logger.info("=" * 60)

    clean_df = build_clean_dataframe(spark, args.input_path)
    regional_df = build_regional_dataframe(spark, args.input_path)
    demographics_df = load_demographics(spark, args.input_path)

    analyzer = CovidAnalyzer(spark)
    results = analyzer.run_all(clean_df, demographics_df, regional_df=regional_df)

    out_base = args.output_path.rstrip("/")
    for name, df in results.items():
        df.write.mode("overwrite").parquet(f"{out_base}/{name}")

    if not args.skip_mongo:
        logger.info("[Stage 4] Loading results into MongoDB Atlas...")
        loader = MongoLoader()
        for name, df in results.items():
            loader.load(df, collection=name)
            logger.info("  Loaded '%s' into MongoDB.", name)
        loader.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("  Pipeline completed in %.1fs", elapsed)
    logger.info("=" * 60)


def main():
    args = parse_args()
    spark = get_spark_session(app_name="COVID-Analytics-Job", mode=args.mode)
    try:
        run_pipeline(spark, args)
    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    main()
