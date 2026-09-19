"""
test_mongodb.py — Unit Tests for MongoLoader
Project: COVID-19 Big Data Analytics Platform
Author:  Aayush Savaliya (Member 3 — Analytics & NoSQL Engineer)

Run:  pytest tests/test_mongodb.py -v
"""

import pytest
from unittest.mock import patch, MagicMock, call
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("test-mongo-loader")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("country_name", StringType(), True),
        StructField("location_key", StringType(), True),
        StructField("total_confirmed", IntegerType(), True),
        StructField("total_deceased", IntegerType(), True),
    ])
    data = [
        ("India", "IN", 43000000, 520000),
        ("United States", "US", 100000000, 1100000),
    ]
    return spark.createDataFrame(data, schema=schema)


class TestMongoLoader:
    """Tests for the MongoLoader module."""

    def test_default_uri_and_database(self):
        """MongoLoader should have correct default URI and database."""
        from src.mongodb.mongo_loader import MongoLoader, MONGO_URI, DATABASE_NAME
        loader = MongoLoader()
        assert loader.uri == MONGO_URI
        assert loader.db_name == DATABASE_NAME
        assert "27017" in MONGO_URI
        assert DATABASE_NAME == "covid_analytics"

    def test_collection_indexes_defined(self):
        """All four expected collections should have index definitions."""
        from src.mongodb.mongo_loader import COLLECTION_INDEXES
        expected = {"country_summary", "daily_summary", "vaccination_summary", "hospitalization_summary"}
        assert expected.issubset(set(COLLECTION_INDEXES.keys()))

    @patch("src.mongodb.mongo_loader.MongoClient")
    def test_load_inserts_documents(self, mock_mongo_cls, sample_df):
        """load() should call insert_many with the correct number of docs."""
        from src.mongodb.mongo_loader import MongoLoader

        # Setup mock collection
        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_mongo_cls.return_value = mock_client

        loader = MongoLoader()
        count = loader.load(sample_df, "country_summary")

        assert count == 2
        mock_col.insert_many.assert_called_once()
        inserted_docs = mock_col.insert_many.call_args[0][0]
        assert len(inserted_docs) == 2

    @patch("src.mongodb.mongo_loader.MongoClient")
    def test_load_drops_collection_before_insert(self, mock_mongo_cls, sample_df):
        """load() should drop the existing collection before inserting (full refresh)."""
        from src.mongodb.mongo_loader import MongoLoader

        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_mongo_cls.return_value = mock_client

        loader = MongoLoader()
        loader.load(sample_df, "country_summary")

        mock_col.drop.assert_called_once()

    @patch("src.mongodb.mongo_loader.MongoClient")
    def test_load_all_loads_all_collections(self, mock_mongo_cls, sample_df):
        """load_all() should call load() for every key in the results dict."""
        from src.mongodb.mongo_loader import MongoLoader

        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        mock_client = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        mock_mongo_cls.return_value = mock_client

        loader = MongoLoader()
        results = {
            "country_summary": sample_df,
            "vaccination_summary": sample_df,
        }
        summary = loader.load_all(results)

        assert len(summary) == 2
        assert "country_summary" in summary
        assert "vaccination_summary" in summary

    @patch("src.mongodb.mongo_loader.MongoClient")
    def test_close_disconnects_client(self, mock_mongo_cls):
        """close() should call client.close() and set _client to None."""
        from src.mongodb.mongo_loader import MongoLoader

        mock_client = MagicMock()
        mock_mongo_cls.return_value = mock_client

        loader = MongoLoader()
        loader._client = mock_client
        loader.close()

        mock_client.close.assert_called_once()
        assert loader._client is None
