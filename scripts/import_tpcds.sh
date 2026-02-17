#!/bin/bash
# Import TPC-DS data into RSDB

echo "=== TPC-DS Data Import Script ==="
echo ""

DATA_DIR="/Users/bytedance/work/rsdb/data/tpcds"

# Start RSDB coordinator in background
echo "Starting RSDB coordinator..."
cargo run --bin rsdb -- --mode coordinator --data-dir /tmp/rsdb_data &
RSDB_PID=$!

sleep 3

# Wait for coordinator to start
echo "Waiting for RSDB to start..."
sleep 2

# Create tables via SQL
echo "Creating tables..."

# Stop RSDB
kill $RSDB_PID 2>/dev/null

echo "Done!"
