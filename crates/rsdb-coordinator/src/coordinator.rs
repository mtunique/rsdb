//! Coordinator - main query coordinator

use crate::cluster_state::ClusterState;
use crate::scheduler::Scheduler;
use arrow_array::RecordBatch;
use rsdb_catalog::CatalogProvider;
use rsdb_common::{FlightClientPool, NodeId, Result, RsdbError};
use rsdb_executor::{DataFusionEngine, ExecutionEngine};
use rsdb_planner::{FragmentPlanner, FullOptimizer};
use rsdb_sql::LogicalPlan;
use rsdb_sql::SqlPlanner;
use rsdb_storage::StorageEngine;
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use rsdb_common::rpc::worker_task_client::WorkerTaskClient;
use rsdb_common::rpc::{
    ExecutePlanFragmentRequest, RegisterCsvRequest, RegisterParquetRequest,
};
use tonic::transport::Channel;
use tonic::Code;


/// Coordinator for distributed query execution
pub struct Coordinator {
    catalog: Arc<dyn CatalogProvider>,
    storage: Arc<dyn StorageEngine>,
    optimizer: FullOptimizer,
    _fragment_planner: FragmentPlanner,
    sql_planner: SqlPlanner,
    executor: Arc<dyn ExecutionEngine>,
    cluster_state: ClusterState,
    _scheduler: Scheduler,
    _client_pool: FlightClientPool,

    registered_tables: HashMap<String, RegisteredTableSource>,

    next_worker_rr: usize,
}

#[derive(Debug, Clone)]
pub enum RegisteredTableSource {
    Csv {
        path: PathBuf,
        has_header: bool,
        delimiter: u8,
    },
    Parquet {
        path: PathBuf,
    },
}

impl Coordinator {
    pub fn new(
        catalog: Arc<dyn CatalogProvider>,
        storage: Arc<dyn StorageEngine>,
        executor: Arc<dyn ExecutionEngine>,
    ) -> Self {
        Self {
            catalog: catalog.clone(),
            storage,
            optimizer: FullOptimizer::new(),
            _fragment_planner: FragmentPlanner::new(),
            sql_planner: SqlPlanner::new(catalog),
            executor,
            cluster_state: ClusterState::new(),
            _scheduler: Scheduler::new(),
            _client_pool: FlightClientPool::new(),
            registered_tables: HashMap::new(),
            next_worker_rr: 0,
        }
    }

    /// Execute a SQL query.
    ///
    /// Uses DataFusion's SQL execution directly, which handles:
    /// - DDL (CREATE EXTERNAL TABLE, DROP TABLE)
    /// - DML (INSERT, COPY)
    /// - DQL (SELECT, WITH, etc.)
    /// - SHOW TABLES, SHOW COLUMNS, etc.
    pub async fn execute_query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        tracing::info!("Executing SQL: {}", sql);

        if Self::should_offload_sql(sql) {
            let workers = self.list_workers();
            if !workers.is_empty() {
                match self.execute_query_distributed(sql, &workers).await {
                    Ok(b) => {
                        return Ok(b);
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Distributed execution failed: {}, fallback to local",
                            e
                        );
                    }
                }
            }
        }

        self.executor.execute_sql(sql).await
    }

    async fn execute_query_distributed(
        &mut self,
        sql: &str,
        workers: &[rsdb_common::types::WorkerInfo],
    ) -> Result<Vec<RecordBatch>> {
        // Build an optimized DataFusion logical plan on the coordinator.
        let engine = self
            .executor
            .as_any()
            .downcast_ref::<DataFusionEngine>()
            .ok_or_else(|| RsdbError::Coordinator("Engine is not DataFusion".to_string()))?;

        let df = engine
            .session_context()
            .sql(sql)
            .await
            .map_err(|e| RsdbError::Execution(format!("SQL planning error: {e}")))?;
        let plan = df
            .into_optimized_plan()
            .map_err(|e| RsdbError::Execution(format!("Failed to build optimized plan: {e}")))?;

        let fragments = self.fragmentize_datafusion_plan(&plan, workers)?;

        let mut tasks = Vec::with_capacity(fragments.len());
        for (addr, sub_plan) in fragments {
            let state = engine.session_context().state();
            let substrait_plan = datafusion_substrait::logical_plan::producer::to_substrait_plan(
                &sub_plan,
                &state,
            )
            .map_err(|e| RsdbError::Execution(format!("Failed to produce Substrait plan: {e}")))?;

            let plan_bytes = substrait_plan
                .encode_to_vec();
            tasks.push(self.execute_plan_fragment_on_worker(&addr, plan_bytes));
        }

        let results: Vec<Vec<RecordBatch>> = futures::future::try_join_all(tasks).await?;
        Ok(results.into_iter().flatten().collect())
    }

    fn fragmentize_datafusion_plan(
        &mut self,
        plan: &datafusion::logical_expr::LogicalPlan,
        workers: &[rsdb_common::types::WorkerInfo],
    ) -> Result<Vec<(String, datafusion::logical_expr::LogicalPlan)>> {
        use datafusion::logical_expr::LogicalPlan;

        if workers.is_empty() {
            return Ok(vec![]);
        }

        match plan {
            LogicalPlan::Union(u) => {
                let inputs = u.inputs().to_vec();
                let mut out = Vec::with_capacity(inputs.len());
                for (i, p) in inputs.into_iter().enumerate() {
                    let w = &workers[i % workers.len()];
                    out.push((w.addr.clone(), p));
                }
                Ok(out)
            }
            _ => {
                let w = self
                    .pick_worker_round_robin()
                    .ok_or_else(|| RsdbError::Coordinator("No workers available".to_string()))?;
                Ok(vec![(w.addr, plan.clone())])
            }
        }
    }

    fn should_offload_sql(sql: &str) -> bool {
        let s = sql.trim_start();
        let upper = s
            .chars()
            .take(16)
            .collect::<String>()
            .to_ascii_uppercase();
        upper.starts_with("SELECT") || upper.starts_with("WITH")
    }

    fn pick_worker_round_robin(&mut self) -> Option<rsdb_common::types::WorkerInfo> {
        let workers = self.list_workers();
        if workers.is_empty() {
            return None;
        }
        let idx = self.next_worker_rr % workers.len();
        self.next_worker_rr = self.next_worker_rr.wrapping_add(1);
        Some(workers[idx].clone())
    }

    async fn execute_plan_fragment_on_worker(
        &self,
        worker_addr: &str,
        plan_bytes: Vec<u8>,
    ) -> Result<Vec<RecordBatch>> {
        use std::io::Cursor;

        let mut client = Self::worker_task_client(worker_addr).await?;
        let resp = client
            .execute_plan_fragment(ExecutePlanFragmentRequest { plan_bytes })
            .await
            .map_err(|e| {
                // Distinguish unavailable vs internal.
                if e.code() == Code::Unavailable {
                    RsdbError::Network(format!("Worker unavailable: {e}"))
                } else {
                    RsdbError::Network(format!("ExecutePlanFragment RPC failed: {e}"))
                }
            })?
            .into_inner();

        let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(resp.arrow_ipc), None)
            .map_err(|e| RsdbError::Execution(format!("Failed to decode Arrow IPC: {e}")))?;
        let mut batches = Vec::new();
        for b in reader {
            let b = b.map_err(|e| RsdbError::Execution(format!("Arrow IPC read error: {e}")))?;
            batches.push(b);
        }
        Ok(batches)
    }

    /// Execute a SQL query using RSDB's full optimization pipeline.
    /// SQL -> SqlPlanner -> LogicalPlan -> Optimizer -> DataFusion Execution
    ///
    /// Use this when tables are registered in both RSDB catalog and DataFusion.
    pub async fn execute_query_with_optimizer(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let logical_plan = self.sql_planner.plan(sql)?;

        if let LogicalPlan::Analyze { ref table_name } = logical_plan {
            self.execute_analyze(table_name).await?;
            return Ok(vec![]);
        }

        self.load_catalog_stats(&logical_plan);
        let optimized_plan = self.optimizer.optimize(logical_plan)?;
        self.executor.execute_logical_plan(&optimized_plan).await
    }

    /// Load table statistics from the catalog into the optimizer's CBO context.
    fn load_catalog_stats(&mut self, plan: &LogicalPlan) {
        self.optimizer
            .load_stats_from_catalog(plan, self.catalog.as_ref());
    }

    /// Register a CSV file as a table in DataFusion.
    /// Auto-detects delimiter by checking the first line of the file.
    pub async fn register_csv(&mut self, table_name: &str, path: &Path) -> Result<()> {
        let engine = self
            .executor
            .as_any()
            .downcast_ref::<DataFusionEngine>()
            .ok_or_else(|| RsdbError::Coordinator("Engine is not DataFusion".to_string()))?;

        // Auto-detect delimiter from first line
        let delimiter = detect_csv_delimiter(path);

        engine
            .register_csv(
                table_name,
                path,
                datafusion::prelude::CsvReadOptions::new()
                    .has_header(true)
                    .delimiter(delimiter),
            )
            .await?;

        tracing::info!(
            "Registered CSV table '{}' from {:?} (delimiter={:?})",
            table_name,
            path,
            delimiter as char
        );

        self.registered_tables.insert(
            table_name.to_string(),
            RegisteredTableSource::Csv {
                path: path.to_path_buf(),
                has_header: true,
                delimiter,
            },
        );

        self.sync_one_table_to_all_workers(table_name).await;
        Ok(())
    }

    /// Register a Parquet file as a table in DataFusion
    pub async fn register_parquet(&mut self, table_name: &str, path: &Path) -> Result<()> {
        let engine = self
            .executor
            .as_any()
            .downcast_ref::<DataFusionEngine>()
            .ok_or_else(|| RsdbError::Coordinator("Engine is not DataFusion".to_string()))?;

        let ctx = engine.session_context();
        ctx.register_parquet(
            table_name,
            path.to_str().unwrap_or_default(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| {
            RsdbError::Execution(format!("Failed to register parquet {table_name}: {e}"))
        })?;

        tracing::info!("Registered Parquet table '{}' from {:?}", table_name, path);

        self.registered_tables.insert(
            table_name.to_string(),
            RegisteredTableSource::Parquet {
                path: path.to_path_buf(),
            },
        );

        self.sync_one_table_to_all_workers(table_name).await;
        Ok(())
    }

    pub fn update_worker_heartbeat(&self, node_id: NodeId) {
        self.cluster_state.update_heartbeat(node_id);
    }

    pub fn list_workers(&self) -> Vec<rsdb_common::types::WorkerInfo> {
        let mut workers = self.cluster_state.list_workers();
        workers.sort_by_key(|w| w.node_id.0);
        workers
    }

    pub fn registered_table_sources(&self) -> Vec<(String, RegisteredTableSource)> {
        self.registered_tables
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    async fn sync_one_table_to_all_workers(&self, table_name: &str) {
        let src = match self.registered_tables.get(table_name) {
            Some(s) => s.clone(),
            None => return,
        };
        let workers = self.list_workers();
        for w in workers {
            if let Err(e) = Self::sync_one_table_to_worker_addr(&w.addr, table_name, &src).await {
                tracing::warn!(
                    "Failed to sync table {} to worker {} ({}): {}",
                    table_name,
                    w.node_id,
                    w.addr,
                    e
                );
            }
        }
    }

    fn tonic_endpoint_from_addr(addr: &str) -> String {
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        }
    }

    async fn worker_task_client(addr: &str) -> Result<WorkerTaskClient<Channel>> {
        let endpoint = Self::tonic_endpoint_from_addr(addr);
        WorkerTaskClient::connect(endpoint)
            .await
            .map_err(|e| RsdbError::Network(format!("Failed to connect worker task service: {e}")))
    }

    pub async fn sync_tables_to_worker_addr(
        worker_addr: &str,
        tables: &[(String, RegisteredTableSource)],
    ) -> Result<()> {
        let mut client = Self::worker_task_client(worker_addr).await?;
        for (name, src) in tables {
            Self::sync_one_table_to_worker(&mut client, name, src).await?;
        }
        Ok(())
    }

    async fn sync_one_table_to_worker_addr(
        worker_addr: &str,
        name: &str,
        src: &RegisteredTableSource,
    ) -> Result<()> {
        let mut client = Self::worker_task_client(worker_addr).await?;
        Self::sync_one_table_to_worker(&mut client, name, src).await
    }

    async fn sync_one_table_to_worker(
        client: &mut WorkerTaskClient<Channel>,
        name: &str,
        src: &RegisteredTableSource,
    ) -> Result<()> {
        match src {
            RegisteredTableSource::Csv {
                path,
                has_header,
                delimiter,
            } => {
                client
                    .register_csv(RegisterCsvRequest {
                        table_name: name.to_string(),
                        path: path.to_string_lossy().to_string(),
                        has_header: *has_header,
                        delimiter: *delimiter as u32,
                    })
                    .await
                    .map_err(|e| {
                        RsdbError::Network(format!("RegisterCsv RPC failed: {e}"))
                    })?;
            }
            RegisteredTableSource::Parquet { path } => {
                client
                    .register_parquet(RegisterParquetRequest {
                        table_name: name.to_string(),
                        path: path.to_string_lossy().to_string(),
                    })
                    .await
                    .map_err(|e| {
                        RsdbError::Network(format!("RegisterParquet RPC failed: {e}"))
                    })?;
            }
        }
        Ok(())
    }

    /// List all tables registered in DataFusion
    pub fn list_tables(&self) -> Vec<String> {
        let engine = self.executor.as_any().downcast_ref::<DataFusionEngine>();

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

    /// Execute ANALYZE TABLE: scan data, compute stats, store in catalog
    pub async fn execute_analyze(&self, table_name: &str) -> Result<()> {
        let schema_provider = self
            .catalog
            .schema("default")
            .ok_or_else(|| RsdbError::Coordinator("default schema not found".to_string()))?;

        let table_entry = schema_provider
            .table(table_name)
            .ok_or_else(|| RsdbError::NotFound(format!("table '{}'", table_name)))?;

        let batches = self.storage.scan_table(&table_entry, None, None).await?;
        let stats = rsdb_planner::collect_statistics(&batches);

        tracing::info!(
            "ANALYZE TABLE {}: {} rows, {} columns, {} bytes",
            table_name,
            stats.row_count,
            stats.column_statistics.len(),
            stats.total_size_bytes,
        );

        schema_provider.set_table_statistics(table_name, stats);
        Ok(())
    }

    /// Execute DDL statement
    pub async fn execute_ddl(&self, plan: LogicalPlan) -> Result<()> {
        match plan {
            LogicalPlan::CreateTable {
                table_name,
                schema,
                partition_keys,
                if_not_exists: _,
            } => {
                let entry = rsdb_catalog::TableEntry {
                    name: table_name.clone(),
                    schema,
                    partition_keys,
                    storage_location: format!("default/{}", table_name),
                };

                self.storage.create_table(entry.clone(), false).await?;

                if let Some(schema) = self.catalog.schema("default") {
                    schema.create_table(entry).await?;
                }

                Ok(())
            }
            LogicalPlan::DropTable {
                table_name,
                if_exists,
            } => {
                if let Some(entry) = self
                    .catalog
                    .schema("default")
                    .and_then(|s| s.table(&table_name))
                {
                    self.storage.drop_table(&entry).await?;
                }

                if let Some(schema) = self.catalog.schema("default") {
                    if if_exists || schema.table_exists(&table_name) {
                        schema.drop_table(&table_name).await?;
                    }
                }

                Ok(())
            }
            _ => Err(RsdbError::Coordinator("Unsupported DDL".to_string())),
        }
    }

    /// Register a worker
    pub fn register_worker(&mut self, node_id: NodeId, addr: String) {
        self.cluster_state.add_worker(node_id, addr);
    }

    /// Get cluster state
    pub fn cluster_state(&self) -> &ClusterState {
        &self.cluster_state
    }

    /// Get catalog
    pub fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
    }

    /// Get storage
    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    /// Get executor
    pub fn executor(&self) -> &Arc<dyn ExecutionEngine> {
        &self.executor
    }
}

/// Auto-detect CSV delimiter by checking the first line of the file.
/// Returns '|' if pipe characters are found, otherwise ','.
fn detect_csv_delimiter(path: &Path) -> u8 {
    if let Ok(content) = std::fs::read_to_string(path) {
        if let Some(first_line) = content.lines().next() {
            let pipes = first_line.matches('|').count();
            let commas = first_line.matches(',').count();
            if pipes > commas {
                return b'|';
            }
        }
    }
    b','
}
