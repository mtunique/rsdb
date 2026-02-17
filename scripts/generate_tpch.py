#!/usr/bin/env python3
"""Generate TPC-H data and export to CSV for RSDB."""

import duckdb
import os

# Scale factor
SCALE = 0.01

print(f"Generating TPC-H data at scale {SCALE}...")

# Use in-memory DuckDB
con = duckdb.connect(':memory:')

# Load tpch extension
con.execute("INSTALL tpch; LOAD tpch;")

# Generate data
print(f"Generating TPC-H data (SF={SCALE})...")
con.execute(f"CALL dbgen(sf={SCALE});")

# Export to CSV
output_dir = os.path.join(os.getcwd(), "data", "tpch")
os.makedirs(output_dir, exist_ok=True)

print("Exporting to CSV...")
tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
for table in tables:
    print(f"  Exporting {table}...")
    con.execute(f"COPY {table} TO '{output_dir}/{table}.csv' (HEADER, DELIMITER ',');")

# Get row counts
print("Data generated:")
total_rows = 0
for table in tables:
    result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    rows = result[0]
    total_rows += rows
    print(f"  {table}: {rows} rows")

print(f"Total rows: {total_rows}")
print(f"Data exported to {output_dir}")
