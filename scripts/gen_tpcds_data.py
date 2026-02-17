#!/usr/bin/env python3
"""Generate TPC-DS data as CSV files using DuckDB's built-in dsdgen."""

import duckdb
import os
import time

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "tpcds")
SCALE_FACTOR = 1  # SF=1 -> ~1GB raw data

# TPC-DS tables we want to export
TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    con = duckdb.connect()

    print(f"Generating TPC-DS data at SF={SCALE_FACTOR}...")
    start = time.time()
    con.execute(f"CALL dsdgen(sf={SCALE_FACTOR})")
    elapsed = time.time() - start
    print(f"  Generated in {elapsed:.1f}s")

    print(f"\nExporting to {DATA_DIR}/")
    for table in TABLES:
        csv_path = os.path.join(DATA_DIR, f"{table}.csv")
        print(f"  {table}...", end="", flush=True)
        start = time.time()
        con.execute(f"COPY {table} TO '{csv_path}' (HEADER, DELIMITER ',')")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        elapsed = time.time() - start
        size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        print(f" {count:>10,} rows, {size_mb:>7.1f} MB ({elapsed:.1f}s)")

    con.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
