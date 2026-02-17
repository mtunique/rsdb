#!/usr/bin/env python3
import duckdb
import os

con = duckdb.connect(':memory:')
con.execute("INSTALL tpch; LOAD tpch;")

output_dir = "data/tpch_queries"
os.makedirs(output_dir, exist_ok=True)

for i in range(1, 23):
    query = con.execute(f"SELECT query FROM tpch_queries() WHERE query_nr = {i}").fetchone()[0]
    with open(os.path.join(output_dir, f"{i:02d}.sql"), "w") as f:
        f.write(query)
    print(f"Generated Q{i:02d}.sql")
