"""
test_analytics.py — Unit Tests for CovidAnalyzer
Project: COVID-19 Big Data Analytics Platform
Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)

Run:  pytest tests/test_analytics.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from datetime import date


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    spark = (
        SparkSession.builder
        .appName("test-covid-analytics")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_schema():
    return StructType([
        StructField("date", StringType(), True),
        StructField("location_key", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("new_confirmed", IntegerType(), True),
        StructField("new_deceased", IntegerType(), True),
        StructField("new_recovered", IntegerType(), True),
        StructField("cumulative_confirmed", IntegerType(), True),
        StructField("cumulative_deceased", IntegerType(), True),
        StructField("new_persons_vaccinated", IntegerType(), True),
        StructField("cumulative_persons_vaccinated", IntegerType(), True),
        StructField("new_hospitalized_patients", IntegerType(), True),
    ])


@pytest.fixture
def sample_data(spark, sample_schema):
    data = [
        ("2021-01-01", "IN", "India", 15000, 200, 12000, 1000000, 15000, 5000, 100000, 800),
        ("2021-01-02", "IN", "India", 16000, 210, 13000, 1016000, 15210, 5500, 105500, 820),
        ("2021-01-01", "US", "United States", 250000, 3000, 200000, 20000000, 350000, 50000, 1000000, 10000),
        ("2021-01-02", "US", "United States", 260000, 3100, 210000, 20260000, 353100, 55000, 1055000, 10200),
        ("2021-01-01", "BR", "Brazil", 50000, 1000, 45000, 5000000, 100000, 2000, 50000, 2000),
    ]
    return spark.createDataFrame(data, schema=sample_schema)


class TestCovidAnalyzer:
    """Tests for the CovidAnalyzer module."""

    def test_country_summary_has_correct_columns(self, spark, sample_data):
        """Country summary should contain expected aggregation columns."""
        from src.analytics.covid_analyzer import CovidAnalyzer
        analyzer = CovidAnalyzer(spark)
        result = analyzer.country_summary(sample_data)
        cols = result.columns
        assert "country_name" in cols
        assert "total_confirmed" in cols
        assert "total_deceased" in cols
        assert "total_vaccinated" in cols

    def test_country_summary_row_count(self, spark, sample_data):
        """Country summary should have one row per country."""
        from src.analytics.covid_analyzer import CovidAnalyzer
        analyzer = CovidAnalyzer(spark)
        result = analyzer.country_summary(sample_data)
        assert result.count() == 3  # India, US, Brazil

    def test_daily_summary_has_correct_columns(self, spark, sample_data):
        """Daily summary should have date-level aggregation."""
        from src.analytics.covid_analyzer import CovidAnalyzer
        analyzer = CovidAnalyzer(spark)
        result = analyzer.daily_global_summary(sample_data)
        cols = result.columns
        assert "date" in cols
        assert "global_new_confirmed" in cols

    def test_run_all_returns_dict(self, spark, sample_data):
        """run_all() should return a dict of DataFrames."""
        from src.analytics.covid_analyzer import CovidAnalyzer
        analyzer = CovidAnalyzer(spark)
        results = analyzer.run_all(sample_data)
        assert isinstance(results, dict)
        assert len(results) > 0
        for key, df in results.items():
            assert df is not None
            assert df.count() >= 0

    def test_no_negative_confirmed_after_cleaning(self, spark, sample_schema):
        """After cleaning, no negative case counts should remain."""
        from src.processing.data_cleaner import DataCleaner
        data = [
            ("2021-01-01", "IN", "India", -100, 200, 12000, 1000000, 15000, 5000, 100000, 800),
            ("2021-01-02", "US", "United States", 260000, 3100, 210000, 20260000, 353100, 55000, 1055000, 10200),
        ]
        df = spark.createDataFrame(data, schema=sample_schema)
        cleaner = DataCleaner(spark)
        clean_df = cleaner.clean(df)
        neg = clean_df.filter(clean_df.new_confirmed < 0).count()
        assert neg == 0
