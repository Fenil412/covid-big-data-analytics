"""
test_mongo_connection.py -- MongoDB Atlas Connection Tester
Project: COVID-19 Big Data Analytics Platform
Author:  Fenil Chodvadiya (Member 1 — Lead Data Engineer)

Run this script to verify your MongoDB Atlas connection is working.

Usage:
    python scripts/test_mongo_connection.py

Make sure your .env file has MONGODB_ATLAS_URI set correctly before running.
"""

import os
import sys
import io

# Force UTF-8 output on Windows to avoid cp1252 encoding errors
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from datetime import datetime

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠  python-dotenv not installed. Run: pip install python-dotenv")
    sys.exit(1)

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, ConfigurationError
except ImportError:
    print("⚠  pymongo not installed. Run: pip install 'pymongo[srv]'")
    sys.exit(1)


# ── ANSI colors ────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}[OK]  {msg}{RESET}")
def fail(msg):  print(f"  {RED}[FAIL] {msg}{RESET}")
def info(msg):  print(f"  {CYAN}[INFO] {msg}{RESET}")
def warn(msg):  print(f"  {YELLOW}[WARN] {msg}{RESET}")


def main():
    print()
    print(f"{BOLD}{'='*55}{RESET}")
    print(f"{BOLD}   MongoDB Atlas - Connection Test{RESET}")
    print(f"{BOLD}{'='*55}{RESET}")
    print()

    # ── Step 1: Check .env ────────────────────────────────────────────────────
    print(f"{BOLD}[1] Checking environment variables...{RESET}")
    uri     = os.getenv("MONGODB_ATLAS_URI", "")
    db_name = os.getenv("MONGODB_DATABASE", "covid_analytics")

    if not uri:
        fail("MONGODB_ATLAS_URI is not set in .env!")
        info("Steps to fix:")
        print("       1. Open your .env file")
        print("       2. Set: MONGODB_ATLAS_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/...")
        sys.exit(1)

    if "<password>" in uri or "<username>" in uri:
        fail("MONGODB_ATLAS_URI still has placeholder values!")
        info("Replace <username> and <password> with your real Atlas credentials.")
        sys.exit(1)

    # Mask password for display
    masked_uri = uri
    try:
        from urllib.parse import urlparse
        parsed = urlparse(uri)
        masked_uri = uri.replace(parsed.password or "", "***") if parsed.password else uri
    except Exception:
        pass

    ok(f"MONGODB_ATLAS_URI found: {masked_uri[:60]}...")
    ok(f"Database: {db_name}")
    print()

    # ── Step 2: Connect ───────────────────────────────────────────────────────
    print(f"{BOLD}[2] Connecting to MongoDB Atlas...{RESET}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000, tls=True)
        # Force connection
        client.admin.command("ping")
        ok("Ping successful — Connected to MongoDB Atlas!")
    except ServerSelectionTimeoutError:
        fail("Connection TIMED OUT!")
        warn("Possible causes:")
        print("       • Your IP is not whitelisted in Atlas → Network Access")
        print("         Go to: Atlas → Network Access → Add IP Address → Allow from Anywhere")
        print("       • Wrong cluster hostname in connection string")
        sys.exit(1)
    except ConfigurationError as e:
        fail(f"Configuration error: {e}")
        warn("Check your MONGODB_ATLAS_URI format.")
        sys.exit(1)
    except ConnectionFailure as e:
        fail(f"Connection failed: {e}")
        sys.exit(1)
    print()

    # ── Step 3: Server info ───────────────────────────────────────────────────
    print(f"{BOLD}[3] Server information...{RESET}")
    try:
        server_info = client.server_info()
        ok(f"MongoDB version : {server_info.get('version', 'unknown')}")
        ok(f"Connection type : MongoDB Atlas (cloud)")
    except Exception as e:
        warn(f"Could not fetch server info: {e}")
    print()

    # ── Step 4: Database access ───────────────────────────────────────────────
    print(f"{BOLD}[4] Checking database access...{RESET}")
    try:
        db = client[db_name]
        collections = db.list_collection_names()
        ok(f"Database '{db_name}' is accessible.")
        if collections:
            ok(f"Existing collections: {', '.join(collections)}")
        else:
            info(f"Database '{db_name}' is empty (no collections yet — that's normal before the pipeline runs).")
    except Exception as e:
        fail(f"Cannot access database '{db_name}': {e}")
        sys.exit(1)
    print()

    # ── Step 5: Write test ────────────────────────────────────────────────────
    print(f"{BOLD}[5] Write test (insert + delete)...{RESET}")
    try:
        test_col = db["_connection_test"]
        doc = {
            "project": "covid-big-data-analytics",
            "test": "connection_check",
            "tested_at": datetime.utcnow().isoformat() + "Z",
            "tested_by": "Fenil Chodvadiya"
        }
        result = test_col.insert_one(doc)
        ok(f"Write successful (doc inserted to Atlas)")
        test_col.delete_one({"_id": result.inserted_id})
        ok("Cleanup successful (test document deleted).")
    except Exception as e:
        fail(f"Write test failed: {e}")
        warn("Check your Atlas user has 'Read and write to any database' role.")
        sys.exit(1)

    client.close()
    print()
    print(f"{BOLD}{'='*55}{RESET}")
    print(f"{GREEN}{BOLD}  [ALL CHECKS PASSED] MongoDB Atlas is ready!{RESET}")
    print(f"{BOLD}{'='*55}{RESET}")
    print()


if __name__ == "__main__":
    main()
