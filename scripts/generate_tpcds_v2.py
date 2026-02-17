#!/usr/bin/env python3
"""Generate TPC-DS data with correct column names for DuckDB."""

import duckdb
import os

print("Generating TPC-DS data with proper column names...")

# Connect to DuckDB
con = duckdb.connect('/tmp/tpcdb_fixed.duckdb', read_only=False)

# Create customer table
print("Generating customer table...")
con.execute("""
    CREATE TABLE customer AS
    SELECT
        c_custkey,
        'Customer#' || LPAD(c_custkey::VARCHAR, 10, '0') AS c_name,
        c_custkey * 10 AS c_address,
        CASE (c_custkey % 5) WHEN 0 THEN 'NEW YORK' WHEN 1 THEN 'Los Angeles' WHEN 2 THEN 'Chicago' ELSE 'Houston' END AS c_city,
        CASE (c_custkey % 5) WHEN 0 THEN 'NEW YORK' WHEN 1 THEN 'CA' WHEN 2 THEN 'IL' ELSE 'TX' END AS c_city_name,
        CASE (c_custkey % 5) WHEN 0 THEN 'UNITED STATES' ELSE 'USA' END AS c_nation,
        'AMERICA' AS c_region,
        '1-212-555-' || LPAD(c_custkey::VARCHAR, 4, '0') AS c_phone,
        CASE (c_custkey % 5) WHEN 0 THEN 'AUTOMOBILE' WHEN 1 THEN 'FURNITURE' WHEN 2 THEN 'MACHINERY' ELSE 'HOUSEHOLD' END AS c_mktsegment
    FROM (SELECT * FROM range(1, 150001) AS t(c_custkey))
""")

# Create supplier table
print("Generating supplier table...")
con.execute("""
    CREATE TABLE supplier AS
    SELECT
        s_suppkey,
        'Supplier#' || LPAD(s_suppkey::VARCHAR, 10, '0') AS s_name,
        s_suppkey * 10 AS s_address,
        CASE (s_suppkey % 5) WHEN 0 THEN 'NEW YORK' WHEN 1 THEN 'Los Angeles' WHEN 2 THEN 'Chicago' ELSE 'Houston' END AS s_city,
        CASE (s_suppkey % 5) WHEN 0 THEN 'NEW YORK' WHEN 1 THEN 'CA' WHEN 2 THEN 'IL' ELSE 'TX' END AS s_city_name,
        CASE (s_suppkey % 5) WHEN 0 THEN 'UNITED STATES' ELSE 'USA' END AS s_nation,
        'AMERICA' AS s_region,
        '1-212-555-' || LPAD(s_suppkey::VARCHAR, 4, '0') AS s_phone
    FROM (SELECT * FROM range(1, 10001) AS t(s_suppkey))
""")

# Create part table
print("Generating part table...")
con.execute("""
    CREATE TABLE part AS
    SELECT
        p_partkey,
        'Part#' || LPAD(p_partkey::VARCHAR, 10, '0') AS p_name,
        'MFGR#' || ((p_partkey % 5) + 1) AS p_mfgr,
        'MFGR' || ((p_partkey % 5) + 1) AS p_category,
        'Brand#' || ((p_partkey % 10) + 1) AS p_brand,
        'color' || (p_partkey % 50) AS p_color,
        'type' || (p_partkey % 10) AS p_type,
        (p_partkey % 50) + 1 AS p_size,
        'Container#' || (p_partkey % 10) AS p_container
    FROM (SELECT * FROM range(1, 200001) AS t(p_partkey))
""")

# Create date dimension table
print("Generating date_dim table...")
con.execute("""
    CREATE TABLE date_dim AS
    SELECT
        (y * 10000 + m * 100 + d) AS d_datekey,
        y || '-' || m || '-' || d AS d_date,
        CASE (d % 7) WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' ELSE 'Saturday' END AS d_dayofweek,
        CASE m WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' ELSE 'December' END AS d_month,
        y AS d_year,
        y * 100 + m AS d_yearmonthnum,
        (CASE m WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November' ELSE 'December' END) || '-' || y AS d_yearmonth,
        (d % 7) + 1 AS d_daynuminweek,
        d AS d_daynuminmonth,
        (y - 1992) * 365 + m * 30 + d AS d_daynuminyear,
        m AS d_monthnuminyear,
        (y - 1992) * 52 + m * 4 AS d_weeknuminyear
    FROM range(1992, 2023) AS y(y)
    CROSS JOIN range(1, 13) AS m(m)
    CROSS JOIN range(1, 29) AS d(d)
""")

# Create orders table with correct column names
print("Generating orders table...")
con.execute("""
    CREATE TABLE orders AS
    SELECT
        t.o_orderkey AS lo_orderkey,
        (t.o_orderkey % 150000) + 1 AS lo_custkey,
        'O' AS o_orderstatus,
        (t.o_orderkey * 1000.0) AS o_totalprice,
        19980101 + (t.o_orderkey % 1000) AS lo_orderdate,
        CASE (t.o_orderkey % 5) WHEN 0 THEN '1-URGENT' WHEN 1 THEN '2-HIGH' WHEN 2 THEN '3-MEDIUM' ELSE '4-LOW' END AS o_orderpriority,
        'Clerk#' || LPAD((t.o_orderkey % 1000)::VARCHAR, 10, '0') AS o_clerk,
        0 AS o_shippriority,
        'Note' AS o_comment
    FROM (SELECT * FROM range(1, 1500001) AS t(o_orderkey)) AS t
""")

# Create lineitem table with TPC-DS-style column names (lo_*)
print("Generating lineitem table...")
con.execute("""
    CREATE TABLE lineitem AS
    SELECT
        t.l_orderkey AS lo_orderkey,
        (t.l_orderkey % 200000) + 1 AS lo_partkey,
        (t.l_orderkey % 10000) + 1 AS lo_suppkey,
        ((t.l_orderkey % 4) + 1) AS lo_linenumber,
        ((t.l_orderkey % 5) + 1) AS lo_quantity,
        ((t.l_orderkey % 1000) + 1) * 1000 AS lo_extendedprice,
        ((t.l_orderkey % 10)) AS lo_discount,
        ((t.l_orderkey % 5)) AS lo_tax,
        19980101 + (t.l_orderkey % 1000) AS lo_shipdate,
        19980101 + (t.l_orderkey % 1000) + 5 AS lo_commitdate,
        19980101 + (t.l_orderkey % 1000) + 10 AS lo_receiptdate,
        'DELIVER IN PERSON' AS lo_shipinstruct,
        CASE (t.l_orderkey % 2) WHEN 0 THEN 'AIR' ELSE 'TRUCK' END AS lo_shipmode,
        'Note' AS lo_comment,
        ((t.l_orderkey % 1000) + 1) * 1000 * (1 - (t.l_orderkey % 10) / 100.0) AS lo_revenue,
        ((t.l_orderkey % 1000) + 1) * 100 * 0.5 AS lo_supplycost
    FROM (
        SELECT * FROM range(1, 6000001) AS t(l_orderkey)
    ) AS t
""")

# Export to CSV
output_dir = "/Users/bytedance/work/rsdb/data/tpcds"
os.makedirs(output_dir, exist_ok=True)

print("\nExporting to CSV...")
for table in ['customer', 'supplier', 'part', 'orders', 'lineitem', 'date_dim']:
    print(f"  Exporting {table}...")
    con.execute(f"COPY {table} TO '{output_dir}/{table}.csv' (HEADER, DELIMITER ',');")

# Get row counts
print("\nData generated:")
total_rows = 0
for table in ['customer', 'supplier', 'part', 'orders', 'lineitem', 'date_dim']:
    result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    rows = result[0]
    total_rows += rows
    print(f"  {table}: {rows:,} rows")

print(f"\nTotal rows: {total_rows:,}")
print(f"Data exported to {output_dir}")

con.close()
