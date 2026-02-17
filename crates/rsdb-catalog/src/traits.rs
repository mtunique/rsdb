//! Catalog traits

use crate::stats::TableStatistics;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rsdb_common::Result;

/// Table entry metadata
#[derive(Debug, Clone)]
pub struct TableEntry {
    pub name: String,
    pub schema: SchemaRef,
    pub partition_keys: Vec<String>,
    pub storage_location: String,
}

/// Catalog provider trait
#[async_trait]
pub trait CatalogProvider: Send + Sync {
    /// List databases
    fn databases(&self) -> Vec<String>;

    /// Check if database exists
    fn database_exists(&self, name: &str) -> bool;

    /// Get schema provider for a database
    fn schema(&self, name: &str) -> Option<Box<dyn SchemaProvider>>;

    /// Create a database
    async fn create_database(&self, name: &str) -> Result<()>;

    /// Drop a database
    async fn drop_database(&self, name: &str) -> Result<()>;
}

/// Schema provider trait
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Schema name
    fn name(&self) -> &str;

    /// List tables
    fn tables(&self) -> Vec<String>;

    /// Check if table exists
    fn table_exists(&self, name: &str) -> bool;

    /// Get table metadata
    fn table(&self, name: &str) -> Option<TableEntry>;

    /// Create a table
    async fn create_table(&self, entry: TableEntry) -> Result<()>;

    /// Drop a table
    async fn drop_table(&self, name: &str) -> Result<()>;

    /// Get table statistics (computed by ANALYZE)
    fn table_statistics(&self, name: &str) -> Option<TableStatistics>;

    /// Store table statistics (called by ANALYZE)
    fn set_table_statistics(&self, name: &str, stats: TableStatistics);
}
