"""
mongo_loader.py — Load Spark Results into MongoDB
Project: COVID-19 Big Data Analytics Platform
Author:  Aayush Savaliya (Member 3 — Analytics & NoSQL Engineer)

Stores PySpark analytics results (DataFrames) into MongoDB collections
using pymongo after converting Spark DataFrames to Python dicts.

Collections used (from config/project_config.yaml):
  - country_summary
  - daily_summary
  - vaccination_summary
  - hospitalization_summary
"""

import logging
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

MONGO_URI = "mongodb://mongodb:27017"
DATABASE_NAME = "covid_analytics"

# Index definitions per collection for fast querying
COLLECTION_INDEXES = {
    "country_summary": [("country_name", ASCENDING), ("total_confirmed", ASCENDING)],
    "daily_summary": [("date", ASCENDING)],
    "vaccination_summary": [("country_name", ASCENDING), ("total_vaccinated", ASCENDING)],
    "hospitalization_summary": [("country_name", ASCENDING)],
}


class MongoLoader:
    """
    Converts Spark DataFrames to Python dicts and inserts them
    into the configured MongoDB collections.
    """

    def __init__(self, uri: str = MONGO_URI, db_name: str = DATABASE_NAME):
        self.uri = uri
        self.db_name = db_name
        self._client = None

    def _get_client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            logger.info(f"Connected to MongoDB: {self.uri}")
        return self._client

    def _get_collection(self, collection_name: str):
        client = self._get_client()
        return client[self.db_name][collection_name]

    def _ensure_indexes(self, collection_name: str) -> None:
        """Create indexes on the collection for faster reads."""
        if collection_name not in COLLECTION_INDEXES:
            return
        col = self._get_collection(collection_name)
        for index_field, direction in COLLECTION_INDEXES[collection_name]:
            col.create_index([(index_field, direction)])
        logger.debug(f"Indexes ensured on {collection_name}")

    def load(self, df: DataFrame, collection: str, batch_size: int = 1000) -> int:
        """
        Load a Spark DataFrame into a MongoDB collection.

        Steps:
        1. Convert Spark DataFrame → list of Python dicts
        2. Drop existing collection (full refresh)
        3. Insert in batches
        4. Create indexes

        Args:
            df:          Spark DataFrame with analytics results.
            collection:  MongoDB collection name.
            batch_size:  Insert batch size.

        Returns:
            Number of documents inserted.
        """
        logger.info(f"Loading '{collection}' into MongoDB...")
        start = datetime.now()

        try:
            # Convert Spark rows to dicts (handle date objects → string)
            rows = df.toJSON().collect()
            import json
            docs = []
            for row_json in rows:
                doc = json.loads(row_json)
                # Convert date objects to ISO strings for MongoDB
                for k, v in doc.items():
                    if hasattr(v, "isoformat"):
                        doc[k] = v.isoformat()
                doc["_loaded_at"] = datetime.utcnow().isoformat()
                docs.append(doc)

            if not docs:
                logger.warning(f"  No documents to insert into '{collection}'.")
                return 0

            col = self._get_collection(collection)

            # Full refresh: drop and re-insert
            col.drop()
            logger.debug(f"  Dropped existing '{collection}' collection.")

            # Batch insert
            total_inserted = 0
            for i in range(0, len(docs), batch_size):
                batch = docs[i: i + batch_size]
                col.insert_many(batch)
                total_inserted += len(batch)

            # Create indexes
            self._ensure_indexes(collection)

            elapsed = (datetime.now() - start).total_seconds()
            logger.info(f"  ✓ Inserted {total_inserted:,} docs into '{collection}' in {elapsed:.1f}s")
            return total_inserted

        except PyMongoError as e:
            logger.error(f"  ✗ MongoDB error while loading '{collection}': {e}")
            raise

    def load_all(self, results: dict) -> dict:
        """
        Load all analytics result DataFrames into their respective collections.

        Args:
            results: Dict of {collection_name: DataFrame}

        Returns:
            Dict of {collection_name: docs_inserted}
        """
        summary = {}
        for collection, df in results.items():
            count = self.load(df, collection)
            summary[collection] = count

        total = sum(summary.values())
        logger.info(f"MongoDB load complete. Total docs inserted: {total:,}")
        return summary

    def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB connection closed.")
