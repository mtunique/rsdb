//! TPC-DS Statistics Collection Test

#![cfg(feature = "tpcds")]
//!
//! Demonstrates that table statistics can be collected and are available for CBO.

use rsdb_executor::tpcds::register_tpcds_tables;
use rsdb_executor::DataFusionEngine;
use std::path::PathBuf;

/// Path to the TPC-DS CSV data directory
fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/tpcds")
}

/// Helper: create engine and register all tables
async fn setup_engine() -> Option<DataFusionEngine> {
    let dir = data_dir();
    if !dir.exists() {
        eprintln!("Skipping: data directory {:?} not found", dir);
        return None;
    }
    let engine = DataFusionEngine::new();
    let registered = register_tpcds_tables(&engine, &dir).await.unwrap();
    if registered.is_empty() {
        eprintln!("Skipping: no CSV files found in {:?}", dir);
        return None;
    }
    println!("Registered {} TPC-DS tables", registered.len());
    Some(engine)
}

/// Test: Show collected statistics for TPC-DS tables
#[tokio::test]
async fn test_tpcds_statistics() {
    let Some(engine) = setup_engine().await else {
        return;
    };

    // Tables used in Q3
    let tables = &["date_dim", "store_sales", "item"];

    println!("\n=== Collecting Statistics for Q3 ===");
    let stats = engine.collect_table_stats(tables).await.unwrap();

    println!("\nTable Statistics (Row Counts):");
    println!("===============================");
    for (table_name, table_stats) in &stats {
        println!("  {}: {:>10} rows", table_name, table_stats.row_count);
    }

    println!("\nCBO can use these row counts to:");
    println!("  1. Estimate join output sizes");
    println!("  2. Choose optimal join order (smallest first)");
    println!("  3. Apply filter selectivity estimates");
    println!();

    // Verify we got stats
    assert_eq!(stats.len(), 3);
}

/// Test: Show statistics for more tables used in complex joins
#[tokio::test]
async fn test_tpcds_statistics_large_tables() {
    let Some(engine) = setup_engine().await else {
        return;
    };

    // Large fact tables
    let tables = &["store_sales", "catalog_sales", "web_sales", "inventory"];

    println!("\n=== Large Table Statistics ===");
    let stats = engine.collect_table_stats(tables).await.unwrap();

    println!("\nTable Statistics:");
    println!("=================");
    let mut table_names: Vec<_> = stats.keys().collect();
    table_names.sort();
    for table_name in table_names {
        let table_stats = stats.get(table_name).unwrap();
        println!(
            "  {}: {:>10} rows ({:.1}M)",
            table_name,
            table_stats.row_count,
            table_stats.row_count as f64 / 1_000_000.0
        );
    }

    println!("\nThese are the key tables for join order optimization.");
    println!("With these stats, CBO can order joins as:");
    println!("  1. Start with smallest table (inventory)");
    println!("  2. Join with medium tables (store_sales ~2.9M)");
    println!("  3. Finally join with largest tables");
}
