//! TPC-H Benchmark Tests
//!
//! Loads TPC-H SF=0.01 CSV data from `data/tpch/` into DataFusion,
//! collects statistics via SQL ANALYZE, and runs TPC-H queries
//! through the RSDB CBO pipeline.

use rsdb_executor::tpch::register_tpch_tables;
use rsdb_executor::DataFusionEngine;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// Path to the TPC-H CSV data directory.
fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/tpch")
}

/// Path to the TPC-H SQL queries directory.
fn queries_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/tpch_queries")
}

/// Helper: create engine and register all tables.
async fn setup_engine() -> Option<DataFusionEngine> {
    let dir = data_dir();
    if !dir.exists() {
        eprintln!("Skipping TPC-H test: data directory {:?} not found", dir);
        return None;
    }
    let engine = DataFusionEngine::new();

    let registered = register_tpch_tables(&engine, &dir).await.unwrap();
    if registered.is_empty() {
        eprintln!("Skipping TPC-H test: no CSV files found in {:?}", dir);
        return None;
    }
    eprintln!("Registered {} TPC-H tables", registered.len());
    Some(engine)
}

#[tokio::test]
async fn test_tpch_all_queries_with_cbo() {
    let Some(engine) = setup_engine().await else {
        return;
    };

    // 1. Collect Statistics via SQL ANALYZE
    eprintln!("Collecting statistics via ANALYZE TABLE...");
    let tables = [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ];
    for table in tables {
        let sql = format!("ANALYZE TABLE {}", table);
        engine.execute_via_logical_plan(&sql).await.unwrap();
    }

    // 2. Run all 22 queries through the CBO pipeline
    let q_dir = queries_dir();
    if !q_dir.exists() {
        panic!(
            "Queries directory {:?} not found. Run scripts/gen_tpch_queries.py first.",
            q_dir
        );
    }

    let mut success_count = 0;
    let mut fail_count = 0;
    let mut timeout_count = 0;
    let mut failures = Vec::new();

    for i in 1..=22 {
        let name = format!("Q{:02}", i);
        let query_path = q_dir.join(format!("{:02}.sql", i));
        let sql = fs::read_to_string(&query_path).expect("Failed to read query file");

        eprintln!("Running {} with CBO...", name);
        let start = Instant::now();

        // Use a 10-second timeout for each query to avoid hanging on complex joins
        let result = timeout(
            Duration::from_secs(10),
            engine.execute_with_cbo(&sql, &tables, true),
        )
        .await;

        match result {
            Ok(Ok(batches)) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                eprintln!("  {}: {} rows in {:?}", name, row_count, start.elapsed());
                success_count += 1;
            }
            Ok(Err(e)) => {
                eprintln!("  {} failed: {}", name, e);
                fail_count += 1;
                failures.push((name, format!("{}", e)));
            }
            Err(_) => {
                eprintln!("  {} timed out", name);
                timeout_count += 1;
                failures.push((name, "Timeout".to_string()));
            }
        }
    }

    eprintln!("\nTPC-H CBO Execution Summary:");
    eprintln!("  Total: 22");
    eprintln!("  Success: {}", success_count);
    eprintln!("  Failure: {}", fail_count);
    eprintln!("  Timeout: {}", timeout_count);

    assert!(
        success_count > 0,
        "At least some queries should succeed through RSDB CBO pipeline"
    );
}
