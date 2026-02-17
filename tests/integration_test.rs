//! Basic query tests

use rsdb_catalog::{CatalogProvider, SchemaProvider, InMemoryCatalog, TableEntry};
use rsdb_storage::DeltaStorageEngine;
use rsdb_executor::{ExecutionEngine, DataFusionEngine};
use rsdb_coordinator::Coordinator;
use rsdb_sql::LogicalPlan;
use arrow_array::{RecordBatch, Int64Array, StringArray};
use arrow_schema::{Schema, Field, DataType};
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_catalog() -> Arc<dyn CatalogProvider> {
    Arc::new(InMemoryCatalog::new())
}

fn create_test_table(catalog: &Arc<dyn CatalogProvider>, table_name: &str) -> TableEntry {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let entry = TableEntry {
        name: table_name.to_string(),
        schema,
        partition_keys: vec![],
        storage_location: format!("default/{}", table_name),
    };

    // Register in catalog
    if let Some(schema) = catalog.schema("default") {
        let _ = schema.create_table(entry.clone());
    }

    entry
}

#[tokio::test]
async fn test_create_table() {
    let catalog = create_test_catalog();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(DeltaStorageEngine::new(temp_dir.path()));
    let executor: Arc<dyn ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let coordinator = Coordinator::new(
        catalog.clone(),
        storage.clone(),
        executor,
    );

    // Create a logical plan for CREATE TABLE
    let plan = LogicalPlan::CreateTable {
        table_name: "test_table".to_string(),
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        partition_keys: vec![],
        if_not_exists: false,
    };

    let result = coordinator.execute_ddl(plan).await;
    assert!(result.is_ok(), "Failed to create table: {:?}", result);

    // Verify table exists in catalog
    let schema = catalog.schema("default").unwrap();
    assert!(schema.table_exists("test_table"));
}

#[tokio::test]
async fn test_drop_table() {
    let catalog = create_test_catalog();
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(DeltaStorageEngine::new(temp_dir.path()));
    let executor: Arc<dyn ExecutionEngine> = Arc::new(DataFusionEngine::new());

    // First create a table
    let entry = create_test_table(&catalog, "test_drop");
    storage.create_table(entry, false).await.unwrap();

    let coordinator = Coordinator::new(
        catalog.clone(),
        storage.clone(),
        executor,
    );

    // Drop the table
    let plan = LogicalPlan::DropTable {
        table_name: "test_drop".to_string(),
        if_exists: false,
    };

    let result = coordinator.execute_ddl(plan).await;
    assert!(result.is_ok(), "Failed to drop table: {:?}", result);
}

#[tokio::test]
async fn test_catalog_operations() {
    let catalog = create_test_catalog();

    // Verify default database exists
    assert!(catalog.database_exists("default"));

    // List databases
    let dbs = catalog.databases();
    assert!(dbs.contains(&"default".to_string()));

    // Get default schema
    let schema = catalog.schema("default");
    assert!(schema.is_some());

    // Schema should have no tables initially
    let tables = schema.unwrap().tables();
    assert!(tables.is_empty());
}
