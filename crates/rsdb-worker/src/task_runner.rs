//! Task runner for executing fragments

use arrow_array::RecordBatch;
use rsdb_common::Result;
use rsdb_executor::ExecutionEngine;
use std::sync::Arc;

/// Task runner - executes plan fragments
pub struct TaskRunner {
    executor: Option<Arc<dyn ExecutionEngine>>,
}

impl TaskRunner {
    pub fn new() -> Self {
        Self { executor: None }
    }

    pub fn with_executor(executor: Arc<dyn ExecutionEngine>) -> Self {
        Self {
            executor: Some(executor),
        }
    }

    /// Execute a fragment
    pub async fn execute(&self, fragment_bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
        if let Some(executor) = &self.executor {
            executor.execute(&fragment_bytes).await
        } else {
            Err(rsdb_common::RsdbError::Worker(
                "No executor configured".to_string(),
            ))
        }
    }
}

impl Default for TaskRunner {
    fn default() -> Self {
        Self::new()
    }
}
