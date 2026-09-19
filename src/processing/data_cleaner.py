"""
data_cleaner.py — PySpark Data Cleaning & Transformation
Project: COVID-19 Big Data Analytics Platform
Author:  Aayush Savaliya (Member 3 — Analytics & NoSQL Engineer)

Cleans and standardises the raw Google COVID-19 Open Data CSV
loaded from HDFS before it is passed to the analytics module.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

logger = logging.getLogger(__name__)

# Columns expected in the raw dataset
NUMERIC_COLUMNS = [
    "new_confirmed",
    "new_deceased",
    "new_recovered",
    "cumulative_confirmed",
    "cumulative_deceased",
    "new_persons_vaccinated",
    "cumulative_persons_vaccinated",
    "new_hospitalized_patients",
    "cumulative_hospitalized_patients",
]

REQUIRED_COLUMNS = ["date", "location_key", "country_name"] + NUMERIC_COLUMNS


class DataCleaner:
    """
    Cleans raw COVID-19 CSV data loaded from HDFS.

    Steps performed:
    1. Drop rows with null date or location_key
    2. Cast numeric columns to IntegerType
    3. Replace negative values with 0 (data entry errors)
    4. Parse date column to DateType
    5. Drop full duplicates
    6. Fill remaining nulls with 0 for numeric columns
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Run the full cleaning pipeline.

        Args:
            df: Raw DataFrame loaded from HDFS CSV.

        Returns:
            Cleaned DataFrame ready for analytics.
        """
        logger.info("Starting data cleaning pipeline...")
        original_count = df.count()

        # ── 1. Drop rows without a date or location ───────────────────────────
        df = df.dropna(subset=["date", "location_key"])
        logger.info(f"After dropping null date/location: {df.count():,} rows")

        # ── 2. Cast numeric columns ───────────────────────────────────────────
        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(IntegerType()))

        # ── 3. Replace negatives with 0 ───────────────────────────────────────
        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.when(F.col(col) < 0, 0).otherwise(F.col(col))
                )

        # ── 4. Parse date column ──────────────────────────────────────────────
        df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

        # ── 5. Drop duplicates ────────────────────────────────────────────────
        df = df.dropDuplicates(["date", "location_key"])

        # ── 6. Fill nulls in numeric columns with 0 ───────────────────────────
        fill_map = {col: 0 for col in NUMERIC_COLUMNS if col in df.columns}
        df = df.fillna(fill_map)

        clean_count = df.count()
        dropped = original_count - clean_count
        logger.info(f"Cleaning complete. Rows: {original_count:,} → {clean_count:,} (dropped {dropped:,})")
        return df
