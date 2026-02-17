//! Storage engine trait and implementations

use arrow_array::RecordBatch;
use async_trait::async_trait;
use rsdb_catalog::TableEntry;
use rsdb_common::{Result, RsdbError};
use std::path::PathBuf;

/// Storage engine trait for table operations
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Create a table
    async fn create_table(&self, entry: TableEntry, overwrite: bool) -> Result<()>;

    /// Scan a table, returning record batches
    async fn scan_table(
        &self,
        table: &TableEntry,
        projection: Option<Vec<String>>,
        filters: Option<String>,
    ) -> Result<Vec<RecordBatch>>;

    /// Write record batches to a table
    async fn write_table(
        &self,
        table: &TableEntry,
        batches: Vec<RecordBatch>,
        overwrite: bool,
    ) -> Result<()>;

    /// Drop a table
    async fn drop_table(&self, table: &TableEntry) -> Result<()>;
}

/// Delta Lake storage engine implementation
/// Note: Full Delta Lake 0.30 integration pending API stabilization.
/// This provides the trait implementation structure.
pub struct DeltaStorageEngine {
    data_dir: PathBuf,
}

impl DeltaStorageEngine {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Get the table path for a given table entry
    fn get_table_path(&self, entry: &TableEntry) -> PathBuf {
        self.data_dir.join(&entry.storage_location)
    }
}

#[async_trait]
impl StorageEngine for DeltaStorageEngine {
    async fn create_table(&self, entry: TableEntry, _overwrite: bool) -> Result<()> {
        let table_path = self.get_table_path(&entry);

        // Create table directory
        std::fs::create_dir_all(&table_path)
            .map_err(|e| RsdbError::Storage(format!("Failed to create table directory: {}", e)))?;

        // Write a _delta_log directory to mark as Delta table
        let delta_log = table_path.join("_delta_log");
        std::fs::create_dir_all(&delta_log)
            .map_err(|e| RsdbError::Storage(format!("Failed to create delta log: {}", e)))?;

        // Write empty commit (0.json) - minimal Delta Lake commit
        let commit_file = delta_log.join("00000000000000000000.json");
        if !commit_file.exists() {
            let commit_content = r#"{"commitInfo":{"timestamp":0,"operation":"CREATE TABLE","operationParameters":{}}}"#;
            std::fs::write(&commit_file, commit_content)
                .map_err(|e| RsdbError::Storage(format!("Failed to write commit: {}", e)))?;
        }

        tracing::info!("Created Delta table at {:?}", table_path);
        Ok(())
    }

    async fn scan_table(
        &self,
        table: &TableEntry,
        _projection: Option<Vec<String>>,
        _filters: Option<String>,
    ) -> Result<Vec<RecordBatch>> {
        let table_path = self.get_table_path(table);

        tracing::info!("Scanning table {} at {:?}", table.name, table_path);

        // Check if table exists
        if !table_path.exists() {
            return Err(RsdbError::NotFound(format!(
                "Table {} not found",
                table.name
            )));
        }

        // TODO: Full implementation would use deltalake with DataFusion
        // For now, return empty - actual data reading would use DataFusion
        tracing::info!("Table exists, returning empty results (full scan not implemented)");

        Ok(vec![])
    }

    async fn write_table(
        &self,
        table: &TableEntry,
        batches: Vec<RecordBatch>,
        _overwrite: bool,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let table_path = self.get_table_path(table);

        // Check if table exists
        if !table_path.exists() {
            return Err(RsdbError::Storage(
                "Table does not exist. Create it first.".to_string(),
            ));
        }

        // Log write operation
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        tracing::info!("Writing {} rows to table {}", total_rows, table.name);

        // TODO: Full implementation would write using Delta Lake format
        // Would convert Arrow RecordBatch to Parquet and write to _delta_log
        Ok(())
    }

    async fn drop_table(&self, table: &TableEntry) -> Result<()> {
        let table_path = self.get_table_path(table);

        // Remove the directory
        if table_path.exists() {
            std::fs::remove_dir_all(&table_path)
                .map_err(|e| RsdbError::Storage(format!("Failed to drop table: {}", e)))?;
            tracing::info!("Dropped table {}", table.name);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{DeltaStorageEngine, StorageEngine};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use rsdb_catalog::TableEntry;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_table_entry(name: &str) -> TableEntry {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        TableEntry {
            name: name.to_string(),
            schema,
            partition_keys: vec![],
            storage_location: format!("default/{}", name),
        }
    }

    fn create_test_batches() -> Vec<RecordBatch> {
        use arrow_array::{Int64Array, StringArray};

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        vec![batch]
    }

    #[tokio::test]
    async fn test_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DeltaStorageEngine::new(temp_dir.path());

        let entry = test_table_entry("test_create");

        // Create table
        storage.create_table(entry.clone(), false).await.unwrap();

        // Verify table directory exists
        let table_path = temp_dir.path().join("default/test_create");
        assert!(table_path.exists());

        // Verify _delta_log exists
        let delta_log = table_path.join("_delta_log");
        assert!(delta_log.exists());
    }

    #[tokio::test]
    async fn test_drop_table() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DeltaStorageEngine::new(temp_dir.path());

        let entry = test_table_entry("test_drop");

        // Create table first
        storage.create_table(entry.clone(), false).await.unwrap();

        let table_path = temp_dir.path().join("default/test_drop");
        assert!(table_path.exists());

        // Drop table
        storage.drop_table(&entry).await.unwrap();

        // Verify table directory is removed
        assert!(!table_path.exists());
    }

    #[tokio::test]
    async fn test_scan_nonexistent_table() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DeltaStorageEngine::new(temp_dir.path());

        let entry = test_table_entry("nonexistent");

        // Try to scan non-existent table
        let result = storage.scan_table(&entry, None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_without_create() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DeltaStorageEngine::new(temp_dir.path());

        let entry = test_table_entry("test_write");
        let batches = create_test_batches();

        // Try to write without creating table first
        let result = storage.write_table(&entry, batches, false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_after_create() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DeltaStorageEngine::new(temp_dir.path());

        let entry = test_table_entry("test_write");

        // Create table first
        storage.create_table(entry.clone(), false).await.unwrap();

        // Write data
        let batches = create_test_batches();
        storage.write_table(&entry, batches, false).await.unwrap();
    }
}
