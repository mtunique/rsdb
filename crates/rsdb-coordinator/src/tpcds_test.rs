//! TPC-DS Benchmark Tests
//!
//! This module runs a subset of TPC-DS queries against RSDB.

use crate::Coordinator;
use rsdb_catalog::{CatalogProvider, InMemoryCatalog};
use rsdb_executor::{DataFusionEngine, ExecutionEngine};
use rsdb_sql::SqlPlanner;
use rsdb_storage::DeltaStorageEngine;
use std::sync::Arc;
use tempfile::TempDir;

/// Create test catalog
fn create_test_catalog() -> Arc<dyn CatalogProvider> {
    Arc::new(InMemoryCatalog::new())
}

/// Create a table via DDL
async fn create_table(coordinator: &Coordinator, sql: &str) -> Result<(), rsdb_common::RsdbError> {
    let planner = SqlPlanner::new(coordinator.catalog().clone());
    let plan = planner.plan(sql)?;
    coordinator.execute_ddl(plan).await
}

/// Create minimal TPC-DS schema for testing
pub async fn setup_tpcds_schema(coordinator: &Coordinator) -> Result<(), rsdb_common::RsdbError> {
    // Create customer table
    let sql = r#"
        CREATE TABLE customer (
            c_custkey INTEGER NOT NULL,
            c_name VARCHAR(100),
            c_city VARCHAR(50),
            c_city_name VARCHAR(50),
            c_nation VARCHAR(50),
            c_region VARCHAR(50)
        )
    "#;
    create_table(coordinator, sql).await?;

    // Create date_dim table
    let sql = r#"
        CREATE TABLE date_dim (
            d_datekey INTEGER NOT NULL,
            d_date VARCHAR(50),
            d_year INTEGER,
            d_yearmonthnum INTEGER
        )
    "#;
    create_table(coordinator, sql).await?;

    // Create lineorder table
    let sql = r#"
        CREATE TABLE lineorder_deep (
            lo_orderkey INTEGER NOT NULL,
            lo_custkey INTEGER NOT NULL,
            lo_partkey INTEGER NOT NULL,
            lo_suppkey INTEGER NOT NULL,
            lo_orderdate INTEGER NOT NULL,
            lo_quantity INTEGER,
            lo_extendedprice BIGINT,
            lo_revenue BIGINT,
            lo_supplycost BIGINT
        )
    "#;
    create_table(coordinator, sql).await?;

    Ok(())
}

#[tokio::test]
async fn test_tpcds_schema_creation() {
    let catalog = create_test_catalog();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(DeltaStorageEngine::new(temp_dir.path()));
    let executor: Arc<dyn ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let coordinator = Coordinator::new(catalog.clone(), storage.clone(), executor);

    // Create TPC-DS schema
    let result = setup_tpcds_schema(&coordinator).await;
    assert!(
        result.is_ok(),
        "Failed to create TPC-DS schema: {:?}",
        result
    );

    // Verify tables exist
    let schema = catalog.schema("default").unwrap();
    assert!(schema.table_exists("customer"));
    assert!(schema.table_exists("date_dim"));
    assert!(schema.table_exists("lineorder_deep"));
}

#[tokio::test]
async fn test_tpcds_q1_simple() {
    let catalog = create_test_catalog();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(DeltaStorageEngine::new(temp_dir.path()));
    let executor: Arc<dyn ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let coordinator = Coordinator::new(catalog.clone(), storage.clone(), executor);

    // Create minimal schema
    let _ = setup_tpcds_schema(&coordinator).await;

    // Test simple aggregation query (TPC-DS like)
    let planner = SqlPlanner::new(catalog.clone());
    let result = planner.plan("SELECT d_year, SUM(lo_revenue) FROM lineorder_deep JOIN date_dim ON lo_orderdate = d_datekey GROUP BY d_year");

    // Should parse successfully (execution will fail as no data)
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_tpcds_catalog_operations() {
    let catalog = create_test_catalog();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(DeltaStorageEngine::new(temp_dir.path()));
    let executor: Arc<dyn ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let coordinator = Coordinator::new(catalog.clone(), storage.clone(), executor);

    // Create TPC-DS schema
    setup_tpcds_schema(&coordinator).await.unwrap();

    // Verify all expected tables exist
    let schema = catalog.schema("default").unwrap();
    let tables = schema.tables();

    assert!(tables.contains(&"customer".to_string()));
    assert!(tables.contains(&"date_dim".to_string()));
    assert!(tables.contains(&"lineorder_deep".to_string()));

    // Check customer table schema
    let customer = schema.table("customer");
    assert!(customer.is_some());
    let customer_schema = customer.unwrap().schema;
    assert!(customer_schema.field_with_name("c_custkey").is_ok());
    assert!(customer_schema.field_with_name("c_name").is_ok());
}
