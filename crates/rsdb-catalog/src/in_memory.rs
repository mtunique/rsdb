//! In-memory catalog implementation

use super::{CatalogProvider, SchemaProvider, TableEntry};
use crate::stats::TableStatistics;
use async_trait::async_trait;
use dashmap::DashMap;
use rsdb_common::{Result, RsdbError};
use std::sync::Arc;

/// In-memory catalog implementation
pub struct InMemoryCatalog {
    databases: DashMap<String, Arc<InMemorySchema>>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        let catalog = Self {
            databases: DashMap::new(),
        };
        // Create default database
        catalog.create_database_sync("default");
        catalog
    }

    fn create_database_sync(&self, name: &str) {
        if !self.databases.contains_key(name) {
            self.databases.insert(
                name.to_string(),
                Arc::new(InMemorySchema::new(name.to_string())),
            );
        }
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CatalogProvider for InMemoryCatalog {
    fn databases(&self) -> Vec<String> {
        self.databases.iter().map(|r| r.key().clone()).collect()
    }

    fn database_exists(&self, name: &str) -> bool {
        self.databases.contains_key(name)
    }

    fn schema(&self, name: &str) -> Option<Box<dyn SchemaProvider>> {
        self.databases.get(name).map(|s| {
            let schema: Arc<InMemorySchema> = s.value().clone();
            let boxed: Box<dyn SchemaProvider> = Box::new(InMemorySchemaWrapper(schema));
            boxed
        })
    }

    async fn create_database(&self, name: &str) -> Result<()> {
        if self.database_exists(name) {
            return Err(RsdbError::AlreadyExists(format!("database '{}'", name)));
        }
        self.create_database_sync(name);
        Ok(())
    }

    async fn drop_database(&self, name: &str) -> Result<()> {
        if !self.database_exists(name) {
            return Err(RsdbError::NotFound(format!("database '{}'", name)));
        }
        self.databases.remove(name);
        Ok(())
    }
}

/// Wrapper to convert Arc<InMemorySchema> to dyn SchemaProvider
struct InMemorySchemaWrapper(Arc<InMemorySchema>);

#[async_trait]
impl SchemaProvider for InMemorySchemaWrapper {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn tables(&self) -> Vec<String> {
        self.0.tables()
    }

    fn table_exists(&self, name: &str) -> bool {
        self.0.table_exists(name)
    }

    fn table(&self, name: &str) -> Option<TableEntry> {
        self.0.table(name)
    }

    async fn create_table(&self, entry: TableEntry) -> Result<()> {
        self.0.create_table(entry).await
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        self.0.drop_table(name).await
    }

    fn table_statistics(&self, name: &str) -> Option<TableStatistics> {
        self.0.table_statistics(name)
    }

    fn set_table_statistics(&self, name: &str, stats: TableStatistics) {
        self.0.set_table_statistics(name, stats);
    }
}

/// In-memory schema
pub struct InMemorySchema {
    name: String,
    tables: DashMap<String, TableEntry>,
    statistics: DashMap<String, TableStatistics>,
}

impl InMemorySchema {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: DashMap::new(),
            statistics: DashMap::new(),
        }
    }
}

#[async_trait]
impl SchemaProvider for InMemorySchema {
    fn name(&self) -> &str {
        &self.name
    }

    fn tables(&self) -> Vec<String> {
        self.tables.iter().map(|r| r.key().clone()).collect()
    }

    fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn table(&self, name: &str) -> Option<TableEntry> {
        self.tables.get(name).map(|e| e.value().clone())
    }

    async fn create_table(&self, entry: TableEntry) -> Result<()> {
        if self.table_exists(&entry.name) {
            return Err(RsdbError::AlreadyExists(format!("table '{}'", entry.name)));
        }
        self.tables.insert(entry.name.clone(), entry);
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        if !self.table_exists(name) {
            return Err(RsdbError::NotFound(format!("table '{}'", name)));
        }
        self.tables.remove(name);
        self.statistics.remove(name);
        Ok(())
    }

    fn table_statistics(&self, name: &str) -> Option<TableStatistics> {
        self.statistics.get(name).map(|s| s.value().clone())
    }

    fn set_table_statistics(&self, name: &str, stats: TableStatistics) {
        self.statistics.insert(name.to_string(), stats);
    }
}

impl Clone for InMemorySchema {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            tables: DashMap::new(),
            statistics: DashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CatalogProvider, InMemoryCatalog, TableEntry};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> arrow_schema::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_table_entry() -> TableEntry {
        TableEntry {
            name: "test_table".to_string(),
            schema: test_schema(),
            partition_keys: vec![],
            storage_location: "default/test_table".to_string(),
        }
    }

    #[tokio::test]
    async fn test_catalog_databases() {
        let catalog = InMemoryCatalog::new();

        // Default database should exist
        assert!(catalog.database_exists("default"));

        // List databases
        let dbs = catalog.databases();
        assert_eq!(dbs.len(), 1);
        assert!(dbs.contains(&"default".to_string()));
    }

    #[tokio::test]
    async fn test_create_drop_database() {
        let catalog = InMemoryCatalog::new();

        // Create a new database
        catalog.create_database("testdb").await.unwrap();
        assert!(catalog.database_exists("testdb"));

        // Can't create duplicate
        let result = catalog.create_database("testdb").await;
        assert!(result.is_err());

        // Drop database
        catalog.drop_database("testdb").await.unwrap();
        assert!(!catalog.database_exists("testdb"));

        // Can't drop non-existent
        let result = catalog.drop_database("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_tables() {
        let catalog = InMemoryCatalog::new();

        // Get default schema
        let schema = catalog.schema("default");
        assert!(schema.is_some());

        // List tables (should be empty)
        let tables = schema.unwrap().tables();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn test_create_drop_table() {
        let catalog = InMemoryCatalog::new();
        let schema = catalog.schema("default").unwrap();

        let entry = test_table_entry();

        // Create table
        schema.create_table(entry.clone()).await.unwrap();
        assert!(schema.table_exists("test_table"));

        // Can't create duplicate
        let result = schema.create_table(entry).await;
        assert!(result.is_err());

        // Drop table
        schema.drop_table("test_table").await.unwrap();
        assert!(!schema.table_exists("test_table"));

        // Can't drop non-existent
        let result = schema.drop_table("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_table_entry_values() {
        let entry = test_table_entry();

        assert_eq!(entry.name, "test_table");
        assert_eq!(entry.schema.fields().len(), 2);
        assert_eq!(entry.storage_location, "default/test_table");
    }

    #[tokio::test]
    async fn test_table_statistics() {
        use crate::stats::{ColumnStatistics, TableStatistics};
        use std::collections::HashMap;

        let catalog = InMemoryCatalog::new();
        let schema = catalog.schema("default").unwrap();

        // Create table
        let entry = test_table_entry();
        schema.create_table(entry).await.unwrap();

        // No stats initially
        assert!(schema.table_statistics("test_table").is_none());

        // Store statistics
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "id".to_string(),
            ColumnStatistics {
                distinct_count: 1000,
                null_count: 0,
                min_value: Some("1".to_string()),
                max_value: Some("1000".to_string()),
                avg_size_bytes: 8,
            },
        );
        col_stats.insert(
            "name".to_string(),
            ColumnStatistics {
                distinct_count: 500,
                null_count: 10,
                min_value: Some("alice".to_string()),
                max_value: Some("zoe".to_string()),
                avg_size_bytes: 12,
            },
        );

        let stats = TableStatistics {
            row_count: 1000,
            total_size_bytes: 20000,
            column_statistics: col_stats,
            last_analyzed: 1700000000,
        };

        schema.set_table_statistics("test_table", stats);

        // Retrieve statistics
        let retrieved = schema.table_statistics("test_table").unwrap();
        assert_eq!(retrieved.row_count, 1000);
        assert_eq!(retrieved.total_size_bytes, 20000);
        assert_eq!(retrieved.column_statistics.len(), 2);

        let id_stats = retrieved.column_statistics.get("id").unwrap();
        assert_eq!(id_stats.distinct_count, 1000);
        assert_eq!(id_stats.null_count, 0);

        let name_stats = retrieved.column_statistics.get("name").unwrap();
        assert_eq!(name_stats.distinct_count, 500);
        assert_eq!(name_stats.null_count, 10);
    }

    #[tokio::test]
    async fn test_drop_table_removes_statistics() {
        use crate::stats::TableStatistics;

        let catalog = InMemoryCatalog::new();
        let schema = catalog.schema("default").unwrap();

        // Create table and add stats
        let entry = test_table_entry();
        schema.create_table(entry).await.unwrap();
        schema.set_table_statistics(
            "test_table",
            TableStatistics {
                row_count: 100,
                ..Default::default()
            },
        );
        assert!(schema.table_statistics("test_table").is_some());

        // Drop table should also remove stats
        schema.drop_table("test_table").await.unwrap();
        assert!(schema.table_statistics("test_table").is_none());
    }
}
