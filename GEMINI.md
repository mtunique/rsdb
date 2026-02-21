# GEMINI.md - RSDB Project Context

## Project Overview
RSDB is a high-performance, distributed MPP (Massively Parallel Processing) OLAP database engine written in Rust. It leverages the Apache Arrow ecosystem (DataFusion, Flight, IPC) and Substrait for query plan serialization. The engine features a custom Cascades-based Cost-Based Optimizer (CBO) for advanced query optimization, including join reordering and predicate pushdown.

### Main Technologies
- **Language**: Rust (latest stable)
- **Execution Engine**: [Apache DataFusion](https://datafusion.apache.org/)
- **Data Format**: Apache Arrow
- **Serialization**: Substrait (for distributed query plans)
- **Networking**: Tonic (gRPC) and Arrow Flight
- **SQL Parsing**: `sqlparser-rs`

## Architecture
RSDB follows a distributed architecture with two primary node modes:

1.  **Coordinator**:
    - Acts as the entry point for clients (via FlightSQL or MySQL protocol).
    - Parses SQL into Logical Plans.
    - Optimizes plans using a Cascades-based CBO.
    - Fragments the optimized plan into fragments for distributed execution.
    - Dispatches fragments to Workers via Arrow Flight.
    - Aggregates final results and returns them to the client.
2.  **Worker**:
    - Registers itself with the Coordinator.
    - Executes query fragments using DataFusion's physical execution engine.
    - Manages intermediate "shuffle" results.
    - Communicates with other workers for data exchange (W2W shuffle) via Arrow Flight.

### Project Structure (Crates)
- `rsdb-sql`: SQL parsing and logical plan construction.
- `rsdb-planner`: Cascades optimizer, Substrait producer/consumer, and fragment planning.
- `rsdb-executor`: Integration with DataFusion for physical execution.
- `rsdb-catalog`: Metadata, schema, and statistics management.
- `rsdb-storage`: Data persistence (Delta Lake based) and storage abstractions.
- `rsdb-network`: Network protocols (FlightSQL, RPC client/server).
- `rsdb-coordinator`: Coordinator logic, cluster state, and scheduler.
- `rsdb-worker`: Worker logic and task execution.
- `rsdb-server`: Main CLI entry point.
- `rsdb-common`: Shared types, errors, and configurations.

## Building and Running

### Building
```bash
cargo build
```

### Running Tests
```bash
# General tests
cargo test

# TPC-H CBO benchmark tests (requires data generation)
cargo test -p rsdb-executor --test tpch_test -- --nocapture
```

### Starting the Cluster
1.  **Start Coordinator**:
    ```bash
    cargo run --bin rsdb -- --mode coordinator --mysql-listen-addr 127.0.0.1:3306
    ```
2.  **Start Worker(s)**:
    ```bash
    cargo run --bin rsdb -- --mode worker --coordinator-addr 127.0.0.1:8819
    ```

### Benchmarks (TPC-H / TPC-DS)
Scripts for data generation and query validation are located in `scripts/`:
- `python3 scripts/generate_tpch.py`: Generate TPC-H CSV data.
- `python3 scripts/gen_tpch_queries.py`: Generate TPC-H SQL queries.

## Development Conventions

- **Modular Design**: Each component is isolated in its own crate under `crates/`. Use specific crate dependencies in `Cargo.toml`.
- **Async First**: The project uses `tokio` for the async runtime. Most networking and execution tasks are async.
- **Error Handling**: Use `anyhow` for top-level error handling and `thiserror` for library-level errors in `rsdb-common`.
- **Logging**: Use the `tracing` crate for instrumentation and logging.
- **Stack Management**: The planner and optimizer spawn dedicated threads with 16MB stacks to prevent stack overflows on deep query trees.
- **Statistics**: Always run `ANALYZE TABLE <name>;` before executing queries that require CBO optimization.
- **Arrow/DataFusion Versions**: Strictly adhere to the workspace versions (currently Arrow 57, DataFusion 52) to ensure compatibility.
