"""
covid_analyzer.py — PySpark COVID-19 Analytics Engine
Project: COVID-19 Big Data Analytics Platform
Author:  Aayush Savaliya (Member 3 — Analytics & NoSQL Engineer)

Performs distributed analytics on cleaned COVID-19 data using PySpark.
Produces country-level summaries, daily global trends, vaccination and
hospitalization statistics — all stored as Parquet on HDFS and MongoDB.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class CovidAnalyzer:
    """
    Distributed COVID-19 analytics using PySpark.

    Produces the following result DataFrames:
    - country_summary        : Total cases/deaths/vaccinations per country
    - daily_summary          : Global new cases/deaths per day
    - vaccination_summary    : Vaccination progress per country
    - hospitalization_summary: Hospital burden per country
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ── 1. Country-Level Summary ──────────────────────────────────────────────
    def country_summary(self, df: DataFrame) -> DataFrame:
        """
        Aggregate total confirmed cases, deaths, recoveries and
        vaccinations grouped by country.
        """
        logger.info("Computing country_summary...")
        result = (
            df.groupBy("country_name", "location_key")
            .agg(
                F.sum("new_confirmed").alias("total_confirmed"),
                F.sum("new_deceased").alias("total_deceased"),
                F.sum("new_recovered").alias("total_recovered"),
                F.sum("new_persons_vaccinated").alias("total_vaccinated"),
                F.max("cumulative_confirmed").alias("peak_cumulative_confirmed"),
                F.max("cumulative_deceased").alias("peak_cumulative_deceased"),
                F.max("new_confirmed").alias("peak_daily_confirmed"),
                F.count("date").alias("days_reported"),
            )
            .withColumn(
                "case_fatality_rate",
                F.round(
                    (F.col("total_deceased") / F.col("total_confirmed")) * 100, 4
                )
            )
            .orderBy(F.col("total_confirmed").desc())
        )
        logger.info(f"  country_summary: {result.count()} countries")
        return result

    # ── 2. Daily Global Summary ───────────────────────────────────────────────
    def daily_global_summary(self, df: DataFrame) -> DataFrame:
        """
        Aggregate global new cases and deaths per day (all countries combined).
        Includes a 7-day rolling average for trend analysis.
        """
        logger.info("Computing daily_global_summary...")

        daily = (
            df.groupBy("date")
            .agg(
                F.sum("new_confirmed").alias("global_new_confirmed"),
                F.sum("new_deceased").alias("global_new_deceased"),
                F.sum("new_recovered").alias("global_new_recovered"),
                F.sum("new_persons_vaccinated").alias("global_new_vaccinated"),
            )
            .orderBy("date")
        )

        # 7-day rolling average
        window_7d = (
            Window.orderBy("date")
            .rowsBetween(-6, 0)
        )
        result = (
            daily
            .withColumn("rolling_avg_confirmed_7d",
                        F.round(F.avg("global_new_confirmed").over(window_7d), 0))
            .withColumn("rolling_avg_deceased_7d",
                        F.round(F.avg("global_new_deceased").over(window_7d), 0))
        )
        logger.info(f"  daily_global_summary: {result.count()} days")
        return result

    # ── 3. Vaccination Summary ────────────────────────────────────────────────
    def vaccination_summary(self, df: DataFrame) -> DataFrame:
        """
        Vaccination progress per country: total vaccinated and peak daily rate.
        """
        logger.info("Computing vaccination_summary...")
        result = (
            df.groupBy("country_name", "location_key")
            .agg(
                F.sum("new_persons_vaccinated").alias("total_vaccinated"),
                F.max("cumulative_persons_vaccinated").alias("cumulative_vaccinated"),
                F.max("new_persons_vaccinated").alias("peak_daily_vaccinated"),
                F.count(
                    F.when(F.col("new_persons_vaccinated") > 0, 1)
                ).alias("vaccination_days"),
            )
            .orderBy(F.col("total_vaccinated").desc())
        )
        logger.info(f"  vaccination_summary: {result.count()} countries")
        return result

    # ── 4. Hospitalization Summary ────────────────────────────────────────────
    def hospitalization_summary(self, df: DataFrame) -> DataFrame:
        """
        Hospital burden per country: average and peak hospitalized patients.
        Returns empty DataFrame if hospitalization columns not present.
        """
        logger.info("Computing hospitalization_summary...")
        hosp_col = "new_hospitalized_patients"

        # Guard: return empty DF if column doesn't exist
        if hosp_col not in df.columns:
            logger.warning(f"  Column '{hosp_col}' not found - skipping hospitalization_summary.")
            return self.spark.createDataFrame([], schema="country_name STRING, location_key STRING")

        result = (
            df.filter(F.col(hosp_col) > 0)
            .groupBy("country_name", "location_key")
            .agg(
                F.sum(hosp_col).alias("total_hospitalized"),
                F.max(hosp_col).alias("peak_hospitalized"),
                F.round(F.avg(hosp_col), 2).alias("avg_daily_hospitalized"),
            )
            .orderBy(F.col("total_hospitalized").desc())
        )
        logger.info(f"  hospitalization_summary: {result.count()} countries")
        return result

    # ── Run All ───────────────────────────────────────────────────────────────
    def run_all(self, df: DataFrame) -> dict:
        """
        Run all analytics and return results as a dict of DataFrames.

        Returns:
            {
                'country_summary': DataFrame,
                'daily_summary': DataFrame,
                'vaccination_summary': DataFrame,
                'hospitalization_summary': DataFrame,
            }
        """
        logger.info("Running all COVID-19 analytics...")
        results = {
            "country_summary":         self.country_summary(df),
            "daily_summary":           self.daily_global_summary(df),
            "vaccination_summary":     self.vaccination_summary(df),
            "hospitalization_summary": self.hospitalization_summary(df),
        }
        logger.info("All analytics complete.")
        return results
