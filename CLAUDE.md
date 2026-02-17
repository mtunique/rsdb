# RSDB - Distributed MPP OLAP Database

RSDB is a distributed MPP OLAP database built in Rust.

## Architecture

```
Client (FlightSQL) → Coordinator
  → SQL Parser (sqlparser-rs)
  → LogicalPlan
  → Optimizer (predicate pushdown, projection pruning, constant folding)
  → Substrait Plan (序列化为 protobuf)
  → FragmentPlanner (切分为 per-worker 片段)
  → Arrow Flight 分发到 Worker
  → Workers 执行片段 (DataFusion 物理执行)
  → Arrow Flight shuffle 中间结果
  → Coordinator 聚合最终结果
  → 返回客户端
```

## Crates

| Crate | Description |
|-------|-------------|
| rsdb-common | Error types, config, shared types |
| rsdb-catalog | CatalogProvider/SchemaProvider traits + InMemoryCatalog |
| rsdb-sql | SqlParser, LogicalPlan, SqlPlanner |
| rsdb-planner | Optimizer rules, Substrait producer/consumer, FragmentPlanner |
| rsdb-executor | ExecutionEngine trait |
| rsdb-storage | StorageEngine trait + DeltaStorageEngine |
| rsdb-network | FlightSQL server, Flight client pool |
| rsdb-coordinator | Coordinator, ClusterState, Scheduler |
| rsdb-worker | Worker, TaskRunner |
| rsdb-server | CLI binary (coordinator/worker modes) |

## Building

```bash
cargo build
cargo test
```

## Running

```bash
# Start coordinator
cargo run --bin rsdb -- --mode coordinator

# Start worker
cargo run --bin rsdb -- --mode worker --coordinator-addr 127.0.0.1:8819
```

## Dependencies

- Arrow 57
- DataFusion 45
- Substrait 0.57
- Delta Lake 0.30
- sqlparser 0.53
- tonic 0.12
