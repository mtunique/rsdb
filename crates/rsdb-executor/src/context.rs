//! Execution context - runtime state for query execution

use rsdb_catalog::CatalogProvider;
use rsdb_storage::StorageEngine;
use std::sync::Arc;

/// Execution context
pub struct ExecutionContext {
    catalog: Arc<dyn CatalogProvider>,
    storage: Arc<dyn StorageEngine>,
}

impl ExecutionContext {
    pub fn new(catalog: Arc<dyn CatalogProvider>, storage: Arc<dyn StorageEngine>) -> Self {
        Self { catalog, storage }
    }

    pub fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
    }

    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }
}
