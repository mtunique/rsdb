use crate::task_runner::TaskRunner;
use crate::result_manager::ResultManager;
use rsdb_catalog::CatalogProvider;
use rsdb_common::rpc::coordinator_control_client::CoordinatorControlClient;
use rsdb_common::rpc::{HeartbeatRequest, RegisterWorkerRequest};
use rsdb_common::{NodeAddr, NodeId, Result, RsdbError};
use rsdb_executor::ExecutionEngine;
use rsdb_storage::StorageEngine;
use std::sync::Arc;
use tonic::transport::Channel;
use datafusion::physical_plan::SendableRecordBatchStream;

/// Worker node
pub struct Worker {
    node_id: NodeId,
    _catalog: Arc<dyn CatalogProvider>,
    _storage: Arc<dyn StorageEngine>,
    executor: Arc<dyn ExecutionEngine>,
    task_runner: TaskRunner,
    result_manager: Arc<ResultManager>,
}

impl Worker {
    pub fn new(
        node_id: NodeId,
        catalog: Arc<dyn CatalogProvider>,
        storage: Arc<dyn StorageEngine>,
        executor: Arc<dyn ExecutionEngine>,
    ) -> Self {
        Self {
            node_id,
            _catalog: catalog,
            _storage: storage,
            executor: executor.clone(),
            task_runner: TaskRunner::with_executor(executor),
            result_manager: Arc::new(ResultManager::new()),
        }
    }

    pub fn take_result(&self, task_id: &str, partition_id: usize) -> Result<SendableRecordBatchStream> {
        self.result_manager.take_result(task_id, partition_id)
    }

    pub fn register_result(&self, task_id: &str, streams: Vec<SendableRecordBatchStream>) {
        self.result_manager.register_result(task_id, streams);
    }

    fn tonic_endpoint_from_addr(addr: &str) -> String {
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        }
    }

    async fn control_client(coordinator_addr: &str) -> Result<CoordinatorControlClient<Channel>> {
        let endpoint = Self::tonic_endpoint_from_addr(coordinator_addr);
        CoordinatorControlClient::connect(endpoint)
            .await
            .map_err(|e| RsdbError::Network(format!("Failed to connect coordinator: {e}")))
    }

    /// Register with coordinator.
    pub async fn register(&self, coordinator_addr: &str, self_addr: &NodeAddr) -> Result<()> {
        let mut client = Self::control_client(coordinator_addr).await?;
        client
            .register_worker(RegisterWorkerRequest {
                node_id: self.node_id.0,
                addr: self_addr.to_string(),
            })
            .await
            .map_err(|e| RsdbError::Network(format!("RegisterWorker RPC failed: {e}")))?;
        Ok(())
    }

    /// Start periodic heartbeat to coordinator.
    pub async fn start_heartbeat(&self, coordinator_addr: String, interval_secs: u64) {
        let node_id = self.node_id.0;
        loop {
            match Self::control_client(&coordinator_addr).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .heartbeat(HeartbeatRequest { node_id })
                        .await
                        .map_err(|e| RsdbError::Network(format!("Heartbeat RPC failed: {e}")))
                    {
                        tracing::warn!("Heartbeat failed: {}", e);
                    }
                }
                Err(e) => {
                    tracing::warn!("Heartbeat connect failed: {}", e);
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        }
    }

    /// Execute a task (fragment) streaming
    pub async fn execute_task_stream(
        &self,
        fragment_bytes: Vec<u8>,
        sources: Vec<rsdb_common::rpc::RemoteSource>,
    ) -> Result<SendableRecordBatchStream> {
        self.task_runner.execute_stream(fragment_bytes, sources).await
    }

    pub fn datafusion_engine(&self) -> Result<&rsdb_executor::DataFusionEngine> {
        self.executor
            .as_any()
            .downcast_ref::<rsdb_executor::DataFusionEngine>()
            .ok_or_else(|| RsdbError::Worker("Engine is not DataFusion".to_string()))
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<arrow_array::RecordBatch>> {
        self.executor.execute_sql(sql).await
    }

    pub async fn register_csv(
        &self,
        table_name: &str,
        path: &str,
        has_header: bool,
        delimiter: u8,
    ) -> Result<()> {
        let engine = self.datafusion_engine()?;
        engine
            .register_csv(
                table_name,
                std::path::Path::new(path),
                datafusion::prelude::CsvReadOptions::new()
                    .has_header(has_header)
                    .delimiter(delimiter),
            )
            .await?;
        Ok(())
    }

    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        let engine = self.datafusion_engine()?;
        let ctx = engine.session_context();
        ctx.register_parquet(
            table_name,
            path,
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| RsdbError::Execution(format!("Failed to register parquet {table_name}: {e}")))?;
        Ok(())
    }

    pub fn list_tables(&self) -> Vec<String> {
        let engine = self
            .executor
            .as_any()
            .downcast_ref::<rsdb_executor::DataFusionEngine>();
        if let Some(engine) = engine {
            let ctx = engine.session_context();
            let state = ctx.state();
            let catalog_list = state.catalog_list();
            let mut tables = Vec::new();
            for catalog_name in catalog_list.catalog_names() {
                if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                    for schema_name in catalog.schema_names() {
                        if let Some(schema) = catalog.schema(&schema_name) {
                            tables.extend(schema.table_names());
                        }
                    }
                }
            }
            tables
        } else {
            vec![]
        }
    }
}
