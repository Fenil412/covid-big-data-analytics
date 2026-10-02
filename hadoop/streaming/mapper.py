#!/usr/bin/env python3
"""Hadoop Streaming mapper: count country-level rows (ISO location_key length 2)."""
import csv
import sys

reader = csv.reader(sys.stdin)
next(reader, None)  # header
for row in reader:
    if len(row) < 2:
        continue
    location_key = row[1].strip()
    if len(location_key) == 2:
        print(f"{location_key}\t1")
