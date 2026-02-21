//! Task runner for executing fragments

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

    /// Execute a fragment streaming
    pub async fn execute_stream(
        &self,
        fragment_bytes: Vec<u8>,
        sources: Vec<rsdb_common::rpc::RemoteSource>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        if let Some(executor) = &self.executor {
            executor.execute_stream(&fragment_bytes, &sources).await
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
