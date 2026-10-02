"""
covid_analyzer.py — PySpark COVID-19 Analytics Engine
Project: COVID-19 Big Data Analytics Platform

Distributed aggregations: country totals, daily global trends, vaccinations,
per-capita metrics, cumulative series, and world-level KPIs.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class CovidAnalyzer:
    """
    Distributed COVID-19 analytics using PySpark.

    Output collections (written to HDFS / MongoDB / local Parquet):
    - global_metrics         : World totals (cases, deaths, recoveries, vaccinations)
    - country_summary        : Per-country totals and rates
    - daily_summary          : Global daily + cumulative series
    - vaccination_summary    : Vaccination progress by country
    - hospitalization_summary: Hospital burden (where reported)
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _join_demographics(self, country_df: DataFrame, demographics_df: Optional[DataFrame]) -> DataFrame:
        if demographics_df is None or demographics_df.count() == 0:
            return (
                country_df
                .withColumn("population", F.lit(None).cast("long"))
                .withColumn("cases_per_million", F.lit(None).cast("double"))
                .withColumn("deaths_per_million", F.lit(None).cast("double"))
            )
        demo = demographics_df.select(
            F.col("location_key"),
            F.col("population").cast("long").alias("population"),
        ).dropDuplicates(["location_key"])
        joined = country_df.join(demo, on="location_key", how="left")
        pop_m = F.when(F.col("population") > 0, F.col("population") / F.lit(1_000_000))
        return joined.withColumn(
            "cases_per_million",
            F.round(F.col("total_confirmed") / pop_m, 2),
        ).withColumn(
            "deaths_per_million",
            F.round(F.col("total_deceased") / pop_m, 2),
        )

    def country_summary(self, df: DataFrame, demographics_df: Optional[DataFrame] = None) -> DataFrame:
        """Country → total cases, deaths, recoveries, vaccinations, active cases, per-million rates."""
        logger.info("Computing country_summary...")

        latest = Window.partitionBy("location_key").orderBy(F.col("date").desc())
        if "cumulative_recovered" in df.columns:
            recovered_term = F.coalesce(F.col("cumulative_recovered"), F.lit(0))
        else:
            recovered_term = F.lit(0)
        active_df = (
            df.withColumn("_rn", F.row_number().over(latest))
            .filter(F.col("_rn") == 1)
            .select(
                "location_key",
                (
                    F.coalesce(F.col("cumulative_confirmed"), F.lit(0))
                    - F.coalesce(F.col("cumulative_deceased"), F.lit(0))
                    - recovered_term
                ).alias("active_cases"),
            )
        )

        result = (
            df.filter(F.col("country_name").isNotNull())
            .groupBy("country_name", "location_key")
            .agg(
                F.sum("new_confirmed").alias("total_confirmed"),
                F.sum("new_deceased").alias("total_deceased"),
                F.sum("new_recovered").alias("total_recovered"),
                F.sum("new_persons_vaccinated").alias("total_people_vaccinated"),
                F.sum("new_vaccine_doses_administered").alias("total_vaccine_doses_administered"),
                F.max("cumulative_vaccine_doses_administered").alias("cumulative_vaccine_doses_administered"),
                F.max("cumulative_confirmed").alias("peak_cumulative_confirmed"),
                F.max("cumulative_deceased").alias("peak_cumulative_deceased"),
                F.max("new_confirmed").alias("peak_daily_confirmed"),
                F.count("date").alias("days_reported"),
            )
            .join(active_df, on="location_key", how="left")
            .withColumn(
                "active_cases",
                F.greatest(F.coalesce(F.col("active_cases"), F.lit(0)), F.lit(0)),
            )
            .withColumn(
                "case_fatality_rate",
                F.round(
                    (
                        F.col("total_deceased")
                        / F.when(F.col("total_confirmed") > 0, F.col("total_confirmed")).otherwise(F.lit(1))
                    )
                    * 100,
                    4,
                ),
            )
            .orderBy(F.col("total_confirmed").desc())
        )
        result = self._join_demographics(result, demographics_df)
        result = result.withColumn("total_vaccinated", F.col("total_people_vaccinated"))
        logger.info("  country_summary aggregation prepared")
        return result

    def daily_global_summary(self, df: DataFrame) -> DataFrame:
        """Date → global new/cumulative cases, deaths, recoveries, vaccinations."""
        logger.info("Computing daily_global_summary...")

        daily = (
            df.groupBy("date")
            .agg(
                F.sum("new_confirmed").alias("global_new_confirmed"),
                F.sum("new_deceased").alias("global_new_deceased"),
                F.sum("new_recovered").alias("global_new_recovered"),
                F.sum("new_persons_vaccinated").alias("global_new_vaccinated"),
                F.sum("new_vaccine_doses_administered").alias("global_new_vaccine_doses_administered"),
            )
            .orderBy("date")
        )

        w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        w7 = Window.orderBy("date").rowsBetween(-6, 0)

        result = (
            daily
            .withColumn("cumulative_global_confirmed", F.sum("global_new_confirmed").over(w))
            .withColumn("cumulative_global_deceased", F.sum("global_new_deceased").over(w))
            .withColumn("cumulative_global_recovered", F.sum("global_new_recovered").over(w))
            .withColumn("cumulative_global_vaccinated", F.sum("global_new_vaccinated").over(w))
            .withColumn(
                "cumulative_global_vaccine_doses_administered",
                F.sum("global_new_vaccine_doses_administered").over(w),
            )
            .withColumn(
                "rolling_avg_confirmed_7d",
                F.round(F.avg("global_new_confirmed").over(w7), 0),
            )
            .withColumn(
                "rolling_avg_deceased_7d",
                F.round(F.avg("global_new_deceased").over(w7), 0),
            )
            # Backward-compatible aliases used by the dashboard
            .withColumn("rolling_avg_7d", F.col("rolling_avg_confirmed_7d"))
            .withColumn("rolling_avg_deaths_7d", F.col("rolling_avg_deceased_7d"))
        )
        logger.info("  daily_global_summary aggregation prepared")
        return result

    def country_daily_summary(self, df: DataFrame) -> DataFrame:
        """Country-by-date aggregates used for date-filtered maps and trends."""
        logger.info("Computing country_daily_summary...")
        aggregations = [
            F.sum("new_confirmed").alias("new_confirmed"),
            F.sum("new_deceased").alias("new_deceased"),
            F.sum("new_recovered").alias("new_recovered"),
        ]
        for source, output in (
            ("new_persons_vaccinated", "new_persons_vaccinated"),
            ("new_vaccine_doses_administered", "new_vaccine_doses_administered"),
            ("cumulative_confirmed", "cumulative_confirmed"),
            ("cumulative_deceased", "cumulative_deceased"),
            ("cumulative_recovered", "cumulative_recovered"),
            ("cumulative_persons_vaccinated", "cumulative_persons_vaccinated"),
            ("cumulative_vaccine_doses_administered", "cumulative_vaccine_doses_administered"),
        ):
            if source in df.columns:
                aggregations.append(F.max(source).alias(output) if source.startswith("cumulative_") else F.sum(source).alias(output))
        return (
            df.filter(F.col("country_name").isNotNull())
            .groupBy("country_name", "location_key", "date")
            .agg(*aggregations)
            .orderBy("date", "country_name")
        )

    def vaccination_summary(self, df: DataFrame) -> DataFrame:
        """Country → vaccination doses administered (Google Open Data: new_persons_vaccinated)."""
        logger.info("Computing vaccination_summary...")
        if "new_persons_vaccinated" not in df.columns and "new_vaccine_doses_administered" not in df.columns:
            return self.spark.createDataFrame(
                [],
                "country_name STRING, location_key STRING, total_vaccination_doses LONG",
            )
        result = (
            df.filter(F.col("country_name").isNotNull())
            .groupBy("country_name", "location_key")
            .agg(
                F.sum("new_persons_vaccinated").alias("total_people_vaccinated"),
                F.max("cumulative_persons_vaccinated").alias("cumulative_people_vaccinated"),
                F.max("new_persons_vaccinated").alias("peak_daily_people_vaccinated"),
                F.count(F.when(F.col("new_persons_vaccinated") > 0, 1)).alias("vaccination_days"),
                F.sum("new_vaccine_doses_administered").alias("total_vaccine_doses_administered"),
                F.max("cumulative_vaccine_doses_administered").alias("cumulative_vaccine_doses_administered"),
                F.max("new_vaccine_doses_administered").alias("peak_daily_vaccine_doses_administered"),
            )
            .orderBy(F.col("total_vaccine_doses_administered").desc())
        )
        result = result.withColumn("total_vaccinated", F.col("total_people_vaccinated"))
        logger.info("  vaccination_summary aggregation prepared")
        return result

    def hospitalization_summary(self, df: DataFrame) -> DataFrame:
        """Hospital metrics by country (sparse coverage)."""
        logger.info("Computing hospitalization_summary...")
        hosp_col = "new_hospitalized_patients"
        if hosp_col not in df.columns:
            logger.warning("  Column '%s' not found — skipping hospitalization_summary.", hosp_col)
            return self.spark.createDataFrame(
                [],
                "country_name STRING, location_key STRING, total_hospitalized LONG",
            )
        result = (
            df.filter(F.col(hosp_col) > 0)
            .filter(F.col("country_name").isNotNull())
            .groupBy("country_name", "location_key")
            .agg(
                F.sum(hosp_col).alias("total_hospitalized"),
                F.max(hosp_col).alias("peak_hospitalized"),
                F.round(F.avg(hosp_col), 2).alias("avg_daily_hospitalized"),
            )
            .orderBy(F.col("total_hospitalized").desc())
        )
        logger.info("  hospitalization_summary aggregation prepared")
        return result

    def global_metrics(self, df: DataFrame) -> DataFrame:
        """Single-row world totals for dashboard KPIs."""
        logger.info("Computing global_metrics...")
        agg_exprs = [
            F.sum("new_confirmed").alias("total_cases"),
            F.sum("new_deceased").alias("total_deaths"),
            F.sum("new_recovered").alias("total_recoveries"),
            F.countDistinct("location_key").alias("countries_reporting"),
        ]
        if "new_persons_vaccinated" in df.columns:
            agg_exprs.append(F.sum("new_persons_vaccinated").alias("total_people_vaccinated"))
        if "new_vaccine_doses_administered" in df.columns:
            agg_exprs.append(F.sum("new_vaccine_doses_administered").alias("total_vaccine_doses_administered"))
        if "date" in df.columns:
            agg_exprs.append(F.max("date").alias("latest_data_date"))

        result = df.agg(*agg_exprs)
        latest_window = Window.partitionBy("location_key").orderBy(F.col("date").desc())
        latest = (
            df.withColumn("_latest_row", F.row_number().over(latest_window))
            .filter(F.col("_latest_row") == 1)
            .agg(F.sum(F.greatest(
                F.coalesce(F.col("cumulative_confirmed"), F.lit(0))
                - F.coalesce(F.col("cumulative_deceased"), F.lit(0))
                - F.coalesce(F.col("cumulative_recovered"), F.lit(0)),
                F.lit(0),
            )).alias("active_cases_worldwide"))
        )
        result = result.crossJoin(latest)
        if "total_people_vaccinated" not in result.columns:
            result = result.withColumn("total_people_vaccinated", F.lit(None).cast("long"))
        if "total_vaccine_doses_administered" not in result.columns:
            result = result.withColumn("total_vaccine_doses_administered", F.lit(None).cast("long"))
        logger.info("  global_metrics: 1 row")
        return result

    def regional_summary(self, df: DataFrame) -> DataFrame:
        """State/province level aggregates (location_key length > 2, e.g. US_CA)."""
        logger.info("Computing regional_summary...")
        regional = df.filter(F.length(F.col("location_key")) > 2)
        if regional.limit(1).count() == 0:
            return self.spark.createDataFrame(
                [],
                "location_key STRING, region_name STRING, country_code STRING, "
                "parent_country STRING, total_confirmed LONG, total_deceased LONG, "
                "total_recovered LONG, total_vaccination_doses LONG, days_reported LONG",
            )

        if "country_name" not in regional.columns:
            regional = regional.withColumn("region_name", F.col("location_key"))
        else:
            regional = regional.withColumn("region_name", F.coalesce(F.col("country_name"), F.col("location_key")))
        if "parent_country" not in regional.columns:
            regional = regional.withColumn("parent_country", F.lit(None).cast("string"))

        regional = regional.withColumn("country_code", F.split(F.col("location_key"), "_").getItem(0))

        regional_agg = [
            F.sum("new_confirmed").alias("total_confirmed"),
            F.sum("new_deceased").alias("total_deceased"),
            F.sum("new_recovered").alias("total_recovered"),
            F.count("date").alias("days_reported"),
        ]
        if "new_persons_vaccinated" in regional.columns:
            regional_agg.append(F.sum("new_persons_vaccinated").alias("total_people_vaccinated"))
        if "new_vaccine_doses_administered" in regional.columns:
            regional_agg.append(F.sum("new_vaccine_doses_administered").alias("total_vaccine_doses_administered"))

        result = (
            regional.filter(F.col("region_name").isNotNull())
            .groupBy("location_key", "region_name", "country_code", "parent_country")
            .agg(*regional_agg)
            .orderBy(F.col("total_confirmed").desc())
        )
        logger.info("  regional_summary aggregation prepared")
        return result

    def run_all(
        self,
        df: DataFrame,
        demographics_df: Optional[DataFrame] = None,
        regional_df: Optional[DataFrame] = None,
    ) -> dict:
        """Run all analytics and return a dict of result DataFrames."""
        logger.info("Running all COVID-19 analytics...")
        results = {
            "global_metrics": self.global_metrics(df),
            "country_summary": self.country_summary(df, demographics_df),
            "daily_summary": self.daily_global_summary(df),
            "country_daily_summary": self.country_daily_summary(df),
            "vaccination_summary": self.vaccination_summary(df),
            "hospitalization_summary": self.hospitalization_summary(df),
        }
        if regional_df is not None:
            results["regional_summary"] = self.regional_summary(regional_df)
        logger.info("All analytics complete.")
        return results
