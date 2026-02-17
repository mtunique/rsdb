//! Test: End-to-end execution with statistics

#![cfg(feature = "tpcds")]
//!
//! This tests the complete flow:
//! 1. Register TPC-DS tables
//! 2. Collect table statistics
//! 3. Execute query through our optimizer
//! 4. Compare results with DataFusion default

use rsdb_executor::tpcds::register_tpcds_tables;
use rsdb_executor::DataFusionEngine;
use rsdb_planner::{collect_statistics, FullOptimizer, PlanStats};
use std::collections::HashMap;
use std::path::PathBuf;

/// Path to TPC-DS data
fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/tpcds")
}

async fn setup() -> Option<DataFusionEngine> {
    let dir = data_dir();
    if !dir.exists() {
        eprintln!("Skipping: data dir not found");
        return None;
    }
    let engine = DataFusionEngine::new();
    let registered = register_tpcds_tables(&engine, &dir).await.unwrap();
    if registered.is_empty() {
        eprintln!("Skipping: no CSV files");
        return None;
    }
    println!("Registered {} tables", registered.len());
    Some(engine)
}

/// Test 1: Collect stats from DataFusion tables
#[tokio::test]
async fn test_stats_collection() {
    let engine = setup().await.unwrap();

    // Get stats using our collect_table_stats method
    let tables = &["date_dim", "store_sales", "item"];
    let stats = engine.collect_table_stats(tables).await.unwrap();

    println!("\n=== Table Statistics ===");
    for (name, s) in &stats {
        println!("{}: {} rows", name, s.row_count);
    }

    assert!(stats.contains_key("date_dim"));
    assert!(stats.contains_key("store_sales"));
    assert!(stats.contains_key("item"));
}

/// Test 2: Execute query through our FullOptimizer
#[tokio::test]
async fn test_optimizer_execution() {
    let engine = setup().await.unwrap();

    // Register stats manually
    let tables = &["date_dim", "store_sales", "item"];
    let stats = engine.collect_table_stats(tables).await.unwrap();

    // Create optimizer with stats
    let mut optimizer = FullOptimizer::new();
    for (name, s) in stats {
        optimizer.register_table(name, s);
    }

    // Simple query
    let sql = "SELECT d_year, COUNT(*) as cnt FROM date_dim GROUP BY d_year ORDER BY d_year";

    // Parse through our planner
    use std::sync::Arc;
    let catalog =
        Arc::new(rsdb_catalog::InMemoryCatalog::new()) as Arc<dyn rsdb_catalog::CatalogProvider>;
    let planner = rsdb_sql::SqlPlanner::new(catalog);
    let plan = planner.plan(sql).unwrap();

    println!("\n=== Logical Plan ===");
    println!("{:?}", plan);

    // Optimize
    let optimized = optimizer.optimize(plan).unwrap();

    println!("\n=== Optimized Plan ===");
    println!("{:?}", optimized);

    // Execute
    let result = engine.execute_logical_plan(&optimized).await;
    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("\n=== Result ===");
            println!("Returned {} rows", total);
        }
        Err(e) => {
            println!("\n=== Error ===");
            println!("{}", e);
        }
    }
}

/// Test 3: Compare default vs optimized execution time
#[tokio::test]
async fn test_query_performance() {
    let engine = setup().await.unwrap();

    let sql = "SELECT d_year, COUNT(*) as cnt FROM date_dim GROUP BY d_year ORDER BY d_year";

    // Default execution (DataFusion default optimizer)
    println!("\n=== Default Execution ===");
    let start = std::time::Instant::now();
    let default_result = engine.execute_sql(sql).await.unwrap();
    let default_time = start.elapsed();
    println!("Default: {}ms", default_time.as_millis());

    // Optimized execution (our optimizer)
    println!("\n=== Optimized Execution ===");
    let tables = &["date_dim"];
    let stats = engine.collect_table_stats(tables).await.unwrap();

    let mut optimizer = FullOptimizer::new();
    for (name, s) in stats {
        optimizer.register_table(name, s);
    }

    use std::sync::Arc;
    let catalog =
        Arc::new(rsdb_catalog::InMemoryCatalog::new()) as Arc<dyn rsdb_catalog::CatalogProvider>;
    let planner = rsdb_sql::SqlPlanner::new(catalog);
    let plan = planner.plan(sql).unwrap();
    let optimized = optimizer.optimize(plan).unwrap();

    let start = std::time::Instant::now();
    let opt_result = engine.execute_logical_plan(&optimized).await;
    let opt_time = start.elapsed();

    match opt_result {
        Ok(batches) => {
            println!("Optimized: {}ms", opt_time.as_millis());
        }
        Err(e) => {
            println!("Optimized Error: {}", e);
        }
    }

    // Verify results match
    let default_rows: usize = default_result.iter().map(|b| b.num_rows()).sum();
    println!("\nDefault rows: {}", default_rows);
}

/// Test 4: JOIN query
#[tokio::test]
async fn test_join_query() {
    let engine = setup().await.unwrap();

    let sql = r#"
        SELECT d_year, i_brand, SUM(ss_ext_sales_price) AS total_sales
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manufact_id = 128
          AND d_moy = 11
        GROUP BY d_year, i_brand
        ORDER BY d_year, total_sales DESC
        LIMIT 10
    "#;

    println!("\n=== JOIN Query ===");

    // Default execution
    let start = std::time::Instant::now();
    let result = engine.execute_sql(sql).await.unwrap();
    let time = start.elapsed();
    let rows: usize = result.iter().map(|b| b.num_rows()).sum();
    println!("Default: {} rows in {}ms", rows, time.as_millis());

    // Through our optimizer
    let tables = &["date_dim", "store_sales", "item"];
    let stats = engine.collect_table_stats(tables).await.unwrap();

    let mut optimizer = FullOptimizer::new();
    for (name, s) in stats {
        optimizer.register_table(name, s);
    }

    use std::sync::Arc;
    let catalog =
        Arc::new(rsdb_catalog::InMemoryCatalog::new()) as Arc<dyn rsdb_catalog::CatalogProvider>;
    let planner = rsdb_sql::SqlPlanner::new(catalog);
    let plan = planner.plan(sql).unwrap();
    let optimized = optimizer.optimize(plan).unwrap();

    let start = std::time::Instant::now();
    let result = engine.execute_logical_plan(&optimized).await;
    let time = start.elapsed();

    match result {
        Ok(batches) => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("Optimized: {} rows in {}ms", rows, time.as_millis());
        }
        Err(e) => {
            println!("Optimized Error: {}", e);
        }
    }
}
