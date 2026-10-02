#!/usr/bin/env python3
"""Hadoop Streaming reducer: sum counts per country key."""
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, _, value = line.partition("\t")
    try:
        count = int(value)
    except ValueError:
        continue
    if current_key == key:
        current_count += count
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = count

if current_key is not None:
    print(f"{current_key}\t{current_count}")
