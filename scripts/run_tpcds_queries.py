#!/usr/bin/env python3
"""Run TPC-DS queries using DuckDB (simulating RSDB behavior)."""

import duckdb
import time
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "tpcds")

print("=== TPC-DS Query Benchmark ===\n")

con = duckdb.connect()

# Import CSV data
TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]

print("Importing CSV data...")
for table in TABLES:
    csv_path = f"{DATA_DIR}/{table}.csv"
    if os.path.exists(csv_path):
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{csv_path}')")
    else:
        print(f"  Warning: {csv_path} not found!")

# Get row counts
print("\nTable sizes:")
for table in TABLES:
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")
    except Exception:
        pass

# TPC-DS queries (simplified from official spec)
queries = [
    ("Q3: Sales by brand/year", """
        SELECT d_year, i_brand_id, i_brand,
               SUM(ss_ext_sales_price) AS sum_agg
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manufact_id = 128
          AND d_moy = 11
        GROUP BY d_year, i_brand_id, i_brand
        ORDER BY d_year, sum_agg DESC, i_brand_id
        LIMIT 100
    """),
    ("Q7: Promo impact", """
        SELECT i_item_id,
               AVG(ss_quantity) AS agg1,
               AVG(ss_list_price) AS agg2,
               AVG(ss_coupon_amt) AS agg3,
               AVG(ss_sales_price) AS agg4
        FROM store_sales
        JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN promotion ON ss_promo_sk = p_promo_sk
        WHERE cd_gender = 'M'
          AND cd_marital_status = 'S'
          AND cd_education_status = 'College'
          AND d_year = 2000
          AND (p_channel_email = 'N' OR p_channel_event = 'N')
        GROUP BY i_item_id
        ORDER BY i_item_id
        LIMIT 100
    """),
    ("Q19: Sales by brand/manager", """
        SELECT i_brand_id, i_brand, i_manufact_id, i_manufact,
               SUM(ss_ext_sales_price) AS ext_price
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN customer ON ss_customer_sk = c_customer_sk
        JOIN customer_address ON c_current_addr_sk = ca_address_sk
        JOIN store ON ss_store_sk = s_store_sk
        WHERE i_manager_id = 8
          AND d_moy = 11
          AND d_year = 1998
          AND ca_gmt_offset = s_gmt_offset
        GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
        ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        LIMIT 100
    """),
    ("Q25: Returns by store", """
        SELECT i_item_id, i_item_desc, s_store_id, s_store_name,
               SUM(ss_net_profit) AS store_sales_profit,
               SUM(sr_net_loss) AS store_returns_loss,
               SUM(cs_net_profit) AS catalog_sales_profit
        FROM store_sales
        JOIN store_returns ON ss_customer_sk = sr_customer_sk
                           AND ss_item_sk = sr_item_sk
                           AND ss_ticket_number = sr_ticket_number
        JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
                           AND sr_item_sk = cs_item_sk
        JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
        JOIN date_dim d2 ON d2.d_date_sk = sr_returned_date_sk
        JOIN date_dim d3 ON d3.d_date_sk = cs_sold_date_sk
        JOIN store ON s_store_sk = ss_store_sk
        JOIN item ON i_item_sk = ss_item_sk
        WHERE d1.d_moy = 4
          AND d1.d_year = 2001
          AND d2.d_moy BETWEEN 4 AND 10
          AND d2.d_year = 2001
          AND d3.d_moy BETWEEN 4 AND 10
          AND d3.d_year = 2001
        GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
        ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
        LIMIT 100
    """),
    ("Q42: Sales by category", """
        SELECT d_year, i_category_id, i_category,
               SUM(ss_ext_sales_price) AS total_sales
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manager_id = 1
          AND d_moy = 11
          AND d_year = 2000
        GROUP BY d_year, i_category_id, i_category
        ORDER BY total_sales DESC, d_year, i_category_id, i_category
        LIMIT 100
    """),
    ("Q52: Sales by brand", """
        SELECT d_year, i_brand_id, i_brand,
               SUM(ss_ext_sales_price) AS ext_price
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manager_id = 1
          AND d_moy = 12
          AND d_year = 1998
        GROUP BY d_year, i_brand_id, i_brand
        ORDER BY d_year, ext_price DESC, i_brand_id
        LIMIT 100
    """),
    ("Q55: Revenue by brand/month", """
        SELECT i_brand_id, i_brand,
               SUM(ss_ext_sales_price) AS ext_price
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manager_id = 36
          AND d_moy = 12
          AND d_year = 2001
        GROUP BY i_brand_id, i_brand
        ORDER BY ext_price DESC, i_brand_id
        LIMIT 100
    """),
    ("Q68: Customer purchase analysis", """
        SELECT c_last_name, c_first_name, ca_city,
               ss_ticket_number, amt, profit
        FROM (
            SELECT ss_ticket_number, ss_customer_sk,
                   ca_city AS bought_city,
                   SUM(ss_coupon_amt) AS amt,
                   SUM(ss_net_profit) AS profit
            FROM store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN store ON ss_store_sk = s_store_sk
            JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
            JOIN customer_address ON ss_addr_sk = ca_address_sk
            WHERE d_dow IN (6, 0)
              AND d_year IN (1999, 2000, 2001)
              AND s_city IN ('Fairview', 'Midway')
              AND (hd_dep_count = 4 OR hd_vehicle_count = 3)
            GROUP BY ss_ticket_number, ss_customer_sk, ca_city
        ) dn
        JOIN customer ON c_customer_sk = ss_customer_sk
        JOIN customer_address ON c_current_addr_sk = ca_address_sk
        WHERE ca_city <> bought_city
        ORDER BY c_last_name, c_first_name, ca_city, ss_ticket_number
        LIMIT 100
    """),
    ("Q73: Ticket count analysis", """
        SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag,
               ss_ticket_number, cnt
        FROM (
            SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt
            FROM store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN store ON ss_store_sk = s_store_sk
            JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
            WHERE d_dom BETWEEN 1 AND 2
              AND (hd_buy_potential = '>10000' OR hd_buy_potential = 'Unknown')
              AND hd_vehicle_count > 0
              AND d_year IN (1999, 2000, 2001)
              AND s_county IN ('Williamson County', 'Franklin Parish',
                               'Bronx County', 'Orange County')
            GROUP BY ss_ticket_number, ss_customer_sk
            HAVING COUNT(*) BETWEEN 1 AND 5
        ) dn
        JOIN customer ON ss_customer_sk = c_customer_sk
        ORDER BY cnt DESC, c_last_name
        LIMIT 100
    """),
    ("Q96: Sales by time/household", """
        SELECT COUNT(*) AS cnt
        FROM store_sales
        JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
        JOIN time_dim ON ss_sold_time_sk = t_time_sk
        JOIN store ON ss_store_sk = s_store_sk
        WHERE t_hour = 20
          AND t_minute >= 30
          AND hd_dep_count = 7
          AND s_store_name = 'ese'
    """),
]

# Run queries
print("\n=== Running Queries ===\n")

total_time = 0
results = []

for name, query in queries:
    print(f"{name}...", end="", flush=True)
    start = time.time()
    try:
        result = con.execute(query).fetchall()
        elapsed = time.time() - start
        total_time += elapsed
        print(f" {len(result)} rows in {elapsed:.3f}s")
        results.append((name, len(result), elapsed))
    except Exception as e:
        elapsed = time.time() - start
        print(f" Error: {e} ({elapsed:.3f}s)")
        results.append((name, 0, elapsed))

# Summary
print(f"\n=== Summary ===")
print(f"Total queries: {len(queries)}")
print(f"Total time: {total_time:.3f}s")
print(f"Avg time/query: {total_time/len(queries):.3f}s")

con.close()
print("\n=== Benchmark Complete ===")
