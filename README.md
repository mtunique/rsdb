# RSDB: High-Performance Distributed SQL Engine

RSDB is a modular SQL database engine written in Rust, leveraging [Apache DataFusion](https://datafusion.apache.org/ ) for execution and featuring a custom Cascades-based Cost-Based Optimizer (CBO). It is designed for high-performance analytical workloads and includes full support for TPC-H and TPC-DS benchmarks.

## Architecture

The project is structured as a set of specialized crates:

- **`rsdb-sql`**: SQL parsing (via `sqlparser-rs`) and translation to RSDB LogicalPlans.
- **`rsdb-planner`**: Features a custom Cascades-based optimizer that performs join reordering and optimization based on table statistics.
- **`rsdb-executor`**: Direct execution of optimized plans using the DataFusion engine.
- **`rsdb-catalog`**: Metadata and schema management.
- **`rsdb-storage`**: Data persistence and storage abstractions.
- **`rsdb-common`**: Shared error types, results, and utilities.
- **`rsdb-server/worker/coordinator`**: Distributed architecture components for scaling workloads.

## Features

- **Cost-Based Optimization (CBO)**: Advanced join reordering using table statistics collected via `ANALYZE TABLE`.
- **Benchmark Driven**: Built-in support for generating and validating TPC-H and TPC-DS datasets.
- **DataFusion Integration**: Seamless conversion between RSDB's optimized logical plans and DataFusion's physical execution.
- **Complex Query Support**: Handles CTEs (WITH clause), Subqueries (IN, EXISTS), and implicit/explicit aggregations.

## Getting Started

### Prerequisites

- Rust (latest stable)
- Python 3 (for data generation scripts)

### 1. Generate Benchmark Data

To run TPC-H tests, you first need to generate the CSV data (Scale Factor 0.01 by default for testing):

```bash
python3 scripts/generate_tpch.py
python3 scripts/gen_tpch_queries.py
```

### 2. Running Tests

RSDB includes an end-to-end TPC-H CBO test suite that covers data loading, statistics collection, and query execution:

```bash
# Run all TPC-H queries through the CBO pipeline
cargo test -p rsdb-executor --test tpch_test -- --nocapture
```

### 3. Statistics Collection

Before running optimized queries, collect statistics to inform the CBO:

```sql
ANALYZE TABLE lineitem;
ANALYZE TABLE orders;
```

## Benchmark Status (TPC-H)

Currently, **13 out of 22** TPC-H queries pass through the full RSDB CBO pipeline (SQL -> RSDB LogicalPlan -> Cascades Optimizer -> DataFusion Plan -> Execution). 

Key queries successfully handled:
- **Q01**: Complex aggregation with arithmetic.
- **Q06**: Complex sum expressions.
- **Q08**: Multi-table joins with aliases and subqueries.
- **Q19**: High-volume data processing with multiple filter conditions.

## Implementation Details

### Stack Management
Deep query trees in TPC-H/DS can cause stack overflows during optimization. RSDB automatically handles this by spawning 16MB large-stack threads for the planner and optimizer.

### Aggregate Argument Transformation
To support DataFusion's physical limitations with complex aggregate arguments (e.g., `SUM(a * b)`), RSDB's `SqlPlanner` automatically performs a pre-projection transformation to pull expressions into an intermediate `Project` node.

## License

This project is licensed under the MIT License.
