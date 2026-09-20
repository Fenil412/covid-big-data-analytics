"""
run_pipeline_local.py - Complete COVID-19 Analytics Pipeline (Local Mode)
Project: COVID-19 Big Data Analytics Platform
Author:  Fenil Chodvadiya (Member 1 - Lead Data Engineer)

Runs the FULL pipeline without Docker/Hadoop/HDFS:
  Step 1: Download COVID-19 CSV data (Sarth)
  Step 2: Clean data with PySpark local mode (Aayush)
  Step 3: Run analytics with PySpark (Aayush)
  Step 4: Store results in MongoDB Atlas (Aayush)

Usage:
    venv\\Scripts\\activate
    python run_pipeline_local.py

Requirements:
    - .env file with MONGODB_ATLAS_URI set
    - pip install -r requirements.txt
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# Load .env
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_DIR  = PROJECT_ROOT / "dataset"
OUTPUT_DIR   = PROJECT_ROOT / "output"

sys.path.insert(0, str(PROJECT_ROOT))


def print_banner(title: str):
    line = "=" * 60
    print(f"\n{line}")
    print(f"  {title}")
    print(f"{line}")


def step1_download():
    """Step 1 (Sarth): Download COVID-19 data."""
    print_banner("STEP 1 | Downloading COVID-19 Dataset")

    # Check if already downloaded
    existing = list(DATASET_DIR.glob("*.csv"))
    if len(existing) >= 3:
        logger.info(f"[SKIP] Dataset already exists ({len(existing)} files in dataset/)")
        logger.info("  Delete dataset/ folder to re-download.")
        return True

    from src.ingestion.data_downloader import download_all
    results = download_all(DATASET_DIR)
    success = sum(results.values())
    if success == 0:
        logger.error("[FAIL] No files downloaded. Check internet connection.")
        return False
    logger.info(f"[OK] Downloaded {success}/{len(results)} files.")
    return True


def step2_spark_pipeline():
    """Steps 2-4 (Aayush): Clean, Analyze, Store in Atlas."""
    print_banner("STEP 2 | Starting PySpark (Local Mode)")

    # Check Atlas URI
    atlas_uri = os.getenv("MONGODB_ATLAS_URI", "")
    if not atlas_uri or "<password>" in atlas_uri:
        logger.error("[FAIL] MONGODB_ATLAS_URI not set in .env!")
        logger.error("  Run: python scripts/test_mongo_connection.py  to verify.")
        return False

    # Find CSV files
    csv_files = list(DATASET_DIR.glob("*.csv"))
    if not csv_files:
        logger.error("[FAIL] No CSV files in dataset/ — run Step 1 first.")
        return False

    logger.info(f"  Dataset files: {[f.name for f in csv_files]}")

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # ── Build local SparkSession ───────────────────────────────────────────
        logger.info("  Building SparkSession in local[*] mode...")
        spark = (
            SparkSession.builder
            .appName("COVID-Analytics-Local")
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info(f"  [OK] SparkSession ready | Spark {spark.version}")

        # ── Stage 2: Load & Clean ──────────────────────────────────────────────
        print_banner("STEP 2 | Cleaning Data")

        # Load epidemiology as primary
        epi_path = str(DATASET_DIR / "epidemiology.csv")
        vacc_path = str(DATASET_DIR / "vaccinations.csv")
        hosp_path = str(DATASET_DIR / "hospitalizations.csv")
        idx_path  = str(DATASET_DIR / "index.csv")

        logger.info(f"  Reading epidemiology.csv...")
        epi_df = spark.read.option("header", True).option("inferSchema", True).csv(epi_path)
        logger.info(f"  Raw rows: {epi_df.count():,}")

        logger.info(f"  Reading index.csv (country names)...")
        idx_df = (
            spark.read.option("header", True).option("inferSchema", True).csv(idx_path)
            .select("location_key", "country_name")
            .dropDuplicates(["location_key"])
        )

        # Join country names
        epi_df = epi_df.join(idx_df, on="location_key", how="left")

        # Cast numeric columns
        numeric_cols = [
            "new_confirmed", "new_deceased", "new_recovered",
            "cumulative_confirmed", "cumulative_deceased",
        ]
        for col in numeric_cols:
            if col in epi_df.columns:
                epi_df = epi_df.withColumn(col, F.col(col).cast("double"))

        # Drop rows with no case data
        epi_df = epi_df.filter(
            F.col("new_confirmed").isNotNull() |
            F.col("cumulative_confirmed").isNotNull()
        )

        # Load vaccinations
        logger.info(f"  Reading vaccinations.csv...")
        vacc_df = spark.read.option("header", True).option("inferSchema", True).csv(vacc_path)
        vacc_num_cols = ["new_persons_vaccinated", "cumulative_persons_vaccinated"]
        for col in vacc_num_cols:
            if col in vacc_df.columns:
                vacc_df = vacc_df.withColumn(col, F.col(col).cast("double"))

        # Join vaccinations
        clean_df = epi_df.join(
            vacc_df.select("date", "location_key", *[c for c in vacc_num_cols if c in vacc_df.columns]),
            on=["date", "location_key"], how="left"
        )

        # Load hospitalizations
        logger.info(f"  Reading hospitalizations.csv...")
        hosp_df = spark.read.option("header", True).option("inferSchema", True).csv(hosp_path)
        hosp_cols = ["new_hospitalized_patients", "current_hospitalized_patients",
                     "new_intensive_care_patients", "current_intensive_care_patients"]
        for col in hosp_cols:
            if col in hosp_df.columns:
                hosp_df = hosp_df.withColumn(col, F.col(col).cast("double"))

        avail_hosp_cols = [c for c in hosp_cols if c in hosp_df.columns]
        if avail_hosp_cols:
            clean_df = clean_df.join(
                hosp_df.select("date", "location_key", *avail_hosp_cols),
                on=["date", "location_key"], how="left"
            )

        clean_df.cache()
        clean_count = clean_df.count()
        logger.info(f"  [OK] Clean rows: {clean_count:,}")

        # ── Stage 3: Analytics ─────────────────────────────────────────────────
        print_banner("STEP 3 | Running PySpark Analytics")

        results = {}

        # Country Summary
        logger.info("  Computing country_summary...")
        agg_cols = {
            "new_confirmed": "total_confirmed",
            "new_deceased":  "total_deceased",
        }
        if "new_persons_vaccinated" in clean_df.columns:
            agg_cols["new_persons_vaccinated"] = "total_vaccinated"

        agg_exprs = [F.sum(k).alias(v) for k, v in agg_cols.items()]
        agg_exprs += [
            F.max("cumulative_confirmed").alias("peak_cumulative"),
            F.count("date").alias("days_reported"),
        ]
        country_df = (
            clean_df
            .filter(F.col("country_name").isNotNull())
            .groupBy("country_name", "location_key")
            .agg(*agg_exprs)
            .withColumn(
                "case_fatality_rate",
                F.round(
                    (F.col("total_deceased") / F.when(F.col("total_confirmed") > 0, F.col("total_confirmed")).otherwise(1)) * 100, 4
                )
            )
            .orderBy(F.col("total_confirmed").desc())
        )
        results["country_summary"] = country_df
        logger.info(f"  [OK] country_summary: {country_df.count()} countries")

        # Daily Global Summary
        logger.info("  Computing daily_summary...")
        daily_agg = [F.sum("new_confirmed").alias("global_new_confirmed"),
                     F.sum("new_deceased").alias("global_new_deceased")]
        if "new_persons_vaccinated" in clean_df.columns:
            daily_agg.append(F.sum("new_persons_vaccinated").alias("global_new_vaccinated"))

        daily_df = (
            clean_df
            .groupBy("date")
            .agg(*daily_agg)
            .orderBy("date")
        )
        window_7d = Window.orderBy("date").rowsBetween(-6, 0)
        daily_df = (
            daily_df
            .withColumn("rolling_avg_7d",
                        F.round(F.avg("global_new_confirmed").over(window_7d), 0))
        )
        results["daily_summary"] = daily_df
        logger.info(f"  [OK] daily_summary: {daily_df.count()} days")

        # Vaccination Summary
        if "new_persons_vaccinated" in clean_df.columns:
            logger.info("  Computing vaccination_summary...")
            vacc_result = (
                clean_df.filter(F.col("country_name").isNotNull())
                .groupBy("country_name", "location_key")
                .agg(
                    F.sum("new_persons_vaccinated").alias("total_vaccinated"),
                    F.max("cumulative_persons_vaccinated").alias("cumulative_vaccinated"),
                    F.max("new_persons_vaccinated").alias("peak_daily_vaccinated"),
                )
                .orderBy(F.col("total_vaccinated").desc())
            )
            results["vaccination_summary"] = vacc_result
            logger.info(f"  [OK] vaccination_summary: {vacc_result.count()} countries")

        # Hospitalization Summary
        hosp_col = "new_hospitalized_patients"
        if hosp_col in clean_df.columns:
            logger.info("  Computing hospitalization_summary...")
            hosp_result = (
                clean_df
                .filter(F.col(hosp_col) > 0)
                .filter(F.col("country_name").isNotNull())
                .groupBy("country_name", "location_key")
                .agg(
                    F.sum(hosp_col).alias("total_hospitalized"),
                    F.max(hosp_col).alias("peak_hospitalized"),
                    F.round(F.avg(hosp_col), 2).alias("avg_daily_hospitalized"),
                )
                .orderBy(F.col("total_hospitalized").desc())
            )
            results["hospitalization_summary"] = hosp_result
            logger.info(f"  [OK] hospitalization_summary: {hosp_result.count()} countries")

        # Save as Parquet locally
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        for name, df in results.items():
            out = str(OUTPUT_DIR / name)
            df.write.mode("overwrite").parquet(out)
            logger.info(f"  Saved: output/{name}/")

        # ── Stage 4: Load into MongoDB Atlas ──────────────────────────────────
        print_banner("STEP 4 | Loading Results into MongoDB Atlas")

        from src.mongodb.mongo_loader import MongoLoader
        loader = MongoLoader()
        summary = loader.load_all(results)
        loader.close()

        total = sum(summary.values())
        logger.info(f"[OK] Total documents in Atlas: {total:,}")
        for coll, count in summary.items():
            logger.info(f"    {coll}: {count:,} docs")

        spark.stop()
        return True

    except Exception as e:
        logger.error(f"[FAIL] Pipeline error: {e}", exc_info=True)
        return False


def main():
    start = datetime.now()
    print_banner("COVID-19 Big Data Analytics - Local Pipeline")
    print(f"  Project Root: {PROJECT_ROOT}")
    print(f"  Mode: PySpark Local[*] + MongoDB Atlas")

    ok1 = step1_download()
    if not ok1:
        sys.exit(1)

    ok2 = step2_spark_pipeline()
    if not ok2:
        sys.exit(1)

    elapsed = (datetime.now() - start).total_seconds()
    print_banner("PIPELINE COMPLETE")
    print(f"  Total time : {elapsed:.1f}s")
    print(f"  Output     : {OUTPUT_DIR}")
    print(f"  Atlas DB   : covid_analytics")
    print(f"\n  View results at: https://cloud.mongodb.com")
    print(f"  Browse Collections -> covid_analytics\n")


if __name__ == "__main__":
    main()
