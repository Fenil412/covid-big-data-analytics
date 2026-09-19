"""
mongo_loader.py — Load Spark Results into MongoDB Atlas
Project: COVID-19 Big Data Analytics Platform
Author:  Aayush Savaliya (Member 3 — Analytics & NoSQL Engineer)

Stores PySpark analytics results (DataFrames) into MongoDB Atlas collections
using pymongo. Connection string is loaded from the MONGODB_ATLAS_URI
environment variable (set in .env — never hardcoded).

Collections used:
  - country_summary
  - daily_summary
  - vaccination_summary
  - hospitalization_summary

Setup:
  1. Copy .env.example → .env
  2. Set MONGODB_ATLAS_URI to your Atlas connection string
  3. Ensure your IP is whitelisted in Atlas Network Access
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from pyspark.sql import DataFrame

# Load .env file
load_dotenv()

logger = logging.getLogger(__name__)

# ── Connection settings (loaded from .env) ─────────────────────────────────────
MONGO_ATLAS_URI = os.getenv("MONGODB_ATLAS_URI")
DATABASE_NAME   = os.getenv("MONGODB_DATABASE", "covid_analytics")

if not MONGO_ATLAS_URI:
    raise EnvironmentError(
        "MONGODB_ATLAS_URI is not set!\n"
        "  1. Copy .env.example to .env\n"
        "  2. Set your MongoDB Atlas connection string in .env\n"
        "  Get it from: Atlas → Cluster → Connect → Drivers → Python"
    )

# ── Index definitions per collection ──────────────────────────────────────────
COLLECTION_INDEXES = {
    "country_summary":         [("country_name", ASCENDING), ("total_confirmed", ASCENDING)],
    "daily_summary":           [("date", ASCENDING)],
    "vaccination_summary":     [("country_name", ASCENDING), ("total_vaccinated", ASCENDING)],
    "hospitalization_summary": [("country_name", ASCENDING)],
}


class MongoLoader:
    """
    Converts Spark DataFrames to dicts and inserts them into
    MongoDB Atlas collections via the URI in MONGODB_ATLAS_URI env var.
    """

    def __init__(self, uri: str = None, db_name: str = None):
        self.uri     = uri or MONGO_ATLAS_URI
        self.db_name = db_name or DATABASE_NAME
        self._client = None

    def _get_client(self) -> MongoClient:
        if self._client is None:
            try:
                self._client = MongoClient(
                    self.uri,
                    serverSelectionTimeoutMS=10000,
                    tls=True,                    # Atlas always uses TLS
                    tlsAllowInvalidCertificates=False,
                )
                # Test connection
                self._client.admin.command("ping")
                logger.info("✓ Connected to MongoDB Atlas successfully.")
            except ServerSelectionTimeoutError as e:
                raise ConnectionError(
                    f"Cannot connect to MongoDB Atlas.\n"
                    f"  Check: (1) MONGODB_ATLAS_URI is correct, "
                    f"(2) your IP is whitelisted in Atlas Network Access.\n"
                    f"  Error: {e}"
                )
        return self._client

    def _get_collection(self, collection_name: str):
        client = self._get_client()
        return client[self.db_name][collection_name]

    def _ensure_indexes(self, collection_name: str) -> None:
        """Create indexes on the Atlas collection for fast queries."""
        if collection_name not in COLLECTION_INDEXES:
            return
        col = self._get_collection(collection_name)
        for field, direction in COLLECTION_INDEXES[collection_name]:
            col.create_index([(field, direction)])
        logger.debug(f"Indexes ensured on '{collection_name}'")

    def load(self, df: DataFrame, collection: str, batch_size: int = 500) -> int:
        """
        Load a Spark DataFrame into a MongoDB Atlas collection.

        Steps:
        1. Convert Spark DataFrame → list of Python dicts (via JSON)
        2. Drop existing collection (full refresh strategy)
        3. Insert documents in batches (smaller batches for Atlas free tier)
        4. Create indexes for fast querying

        Args:
            df:          Spark DataFrame with analytics results.
            collection:  Target MongoDB Atlas collection name.
            batch_size:  Insert batch size (500 recommended for Atlas M0 free tier).

        Returns:
            Number of documents inserted.
        """
        logger.info(f"Loading '{collection}' → MongoDB Atlas ({self.db_name})...")
        start = datetime.now()

        try:
            # Convert Spark rows → Python dicts via JSON
            rows_json = df.toJSON().collect()
            docs = []
            for row_str in rows_json:
                doc = json.loads(row_str)
                # Stringify any date objects
                for k, v in list(doc.items()):
                    if hasattr(v, "isoformat"):
                        doc[k] = v.isoformat()
                doc["_loaded_at"] = datetime.utcnow().isoformat() + "Z"
                docs.append(doc)

            if not docs:
                logger.warning(f"  No documents to insert into '{collection}'.")
                return 0

            col = self._get_collection(collection)

            # Full refresh — drop and re-insert
            col.drop()
            logger.debug(f"  Dropped existing '{collection}'.")

            # Batch insert
            total_inserted = 0
            for i in range(0, len(docs), batch_size):
                batch = docs[i: i + batch_size]
                col.insert_many(batch, ordered=False)
                total_inserted += len(batch)
                logger.debug(f"  Inserted batch {i // batch_size + 1} ({len(batch)} docs)")

            # Create indexes
            self._ensure_indexes(collection)

            elapsed = (datetime.now() - start).total_seconds()
            logger.info(
                f"  ✓ '{collection}': {total_inserted:,} docs inserted in {elapsed:.1f}s "
                f"→ Atlas/{self.db_name}"
            )
            return total_inserted

        except PyMongoError as e:
            logger.error(f"  ✗ MongoDB Atlas error on '{collection}': {e}")
            raise

    def load_all(self, results: dict) -> dict:
        """
        Load all analytics result DataFrames into their Atlas collections.

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
        logger.info(f"✓ All data loaded to MongoDB Atlas. Total docs: {total:,}")
        return summary

    def close(self) -> None:
        """Close the Atlas connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("MongoDB Atlas connection closed.")
