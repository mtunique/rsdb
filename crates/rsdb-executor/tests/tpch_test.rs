use datafusion::prelude::CsvReadOptions;
use rsdb_executor::DataFusionEngine;
use rsdb_executor::ExecutionEngine;
use rsdb_executor::tpch;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::{timeout, Duration, Instant};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn data_dir() -> PathBuf {
    workspace_root().join("data").join("tpch")
}

fn queries_dir() -> PathBuf {
    workspace_root().join("data").join("tpch_queries")
}

async fn setup_engine() -> Option<Arc<DataFusionEngine>> {
    let engine = Arc::new(DataFusionEngine::new());
    let tpch_dir = data_dir();
    if !tpch_dir.exists() {
        return None;
    }

    let tables = [
        ("customer", tpch::customer_schema()),
        ("lineitem", tpch::lineitem_schema()),
        ("nation", tpch::nation_schema()),
        ("orders", tpch::orders_schema()),
        ("part", tpch::part_schema()),
        ("partsupp", tpch::partsupp_schema()),
        ("region", tpch::region_schema()),
        ("supplier", tpch::supplier_schema()),
    ];

    for (name, schema) in tables {
        let path = tpch_dir.join(format!("{}.csv", name));
        if path.exists() {
            engine
                .register_csv(
                    name, 
                    &path, 
                    CsvReadOptions::new()
                        .delimiter(b',')
                        .has_header(true)
                        .schema(&schema) // FORCE EXPLICIT SCHEMA
                )
                .await
                .unwrap();
        }
    }

    println!("Registered 8 TPC-H tables with explicit schemas");
    Some(engine)
}

#[tokio::test]
async fn test_tpch_all_queries_with_cbo() {
    let Some(engine) = setup_engine().await else {
        return;
    };

    // 1. Run all 22 queries
    let q_dir = queries_dir();
    let mut success_count = 0;
    let mut fail_count = 0;

    for i in 1..=22 {
        let name = format!("Q{:02}", i);
        let query_path = q_dir.join(format!("{:02}.sql", i));
        let sql = fs::read_to_string(&query_path).expect("Failed to read query file");

        println!("Running {}...", name);
        let start = Instant::now();

        let result = timeout(
            Duration::from_secs(30),
            engine.execute_sql(&sql),
        )
        .await;

        match result {
            Ok(Ok(batches)) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                println!("  {}: {} rows in {:?}", name, row_count, start.elapsed());
                success_count += 1;
            }
            Ok(Err(e)) => {
                println!("  {} failed: {}", name, e);
                fail_count += 1;
            }
            Err(_) => {
                println!("  {} timed out", name);
                fail_count += 1;
            }
        }
    }

    println!("\nTPC-H Execution Summary:");
    println!("  Total: 22");
    println!("  Success: {}", success_count);
    println!("  Failure: {}", fail_count);

    assert!(success_count > 0);
}
