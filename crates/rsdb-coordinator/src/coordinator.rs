//! Coordinator - main query coordinator

use crate::cluster_state::ClusterState;
use crate::scheduler::Scheduler;
use arrow_array::RecordBatch;
use rsdb_catalog::CatalogProvider;
use rsdb_common::{NodeId, Result, RsdbError};
use rsdb_executor::{DataFusionEngine, ExecutionEngine};
use rsdb_planner::{FragmentPlanner, FullOptimizer};
use rsdb_sql::LogicalPlan as RsdbLogicalPlan;
use rsdb_sql::SqlPlanner;
use rsdb_storage::StorageEngine;
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use uuid::Uuid;

use rsdb_common::rpc::worker_task_client::WorkerTaskClient;
use rsdb_common::rpc::{
    ExecutePlanFragmentRequest, ExecuteSqlRequest, RegisterCsvRequest, RegisterParquetRequest,
};
use rsdb_common::types::WorkerInfo;
use prost::Message;
use tonic::transport::Channel;
use futures::StreamExt;
use datafusion::prelude::CsvReadOptions;

/// Coordinator for distributed query execution
pub struct Coordinator {
    catalog: Arc<dyn CatalogProvider>,
    storage: Arc<dyn StorageEngine>,
    optimizer: FullOptimizer,
    sql_planner: SqlPlanner,
    executor: Arc<dyn ExecutionEngine>,
    cluster_state: ClusterState,
    registered_tables: HashMap<String, RegisteredTableSource>,
    next_worker_rr: usize,
}

#[derive(Debug, Clone)]
pub enum RegisteredTableSource {
    Csv { path: PathBuf, has_header: bool, delimiter: u8 },
    Parquet { path: PathBuf },
    View { sql: String },
}

impl Coordinator {
    pub fn new(catalog: Arc<dyn CatalogProvider>, storage: Arc<dyn StorageEngine>, executor: Arc<dyn ExecutionEngine>) -> Self {
        Self {
            catalog: catalog.clone(),
            storage,
            optimizer: FullOptimizer::new(),
            sql_planner: SqlPlanner::new(catalog),
            executor,
            cluster_state: ClusterState::new(),
            registered_tables: HashMap::new(),
            next_worker_rr: 0,
        }
    }

    pub async fn execute_query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        tracing::info!("Executing SQL: {}", sql);
        if Self::should_offload_sql(sql) {
            let workers = self.list_workers();
            if !workers.is_empty() {
                return self.execute_query_distributed(sql, &workers).await;
            }
        }
        self.executor.execute_sql(sql).await
    }

    async fn execute_query_distributed(&mut self, sql: &str, workers: &[WorkerInfo]) -> Result<Vec<RecordBatch>> {
        let logical_plan = self.sql_planner.plan(sql)?;
        let optimized_plan = self.optimizer.optimize(logical_plan)?;
        let fragments = FragmentPlanner::new().plan_fragments(&optimized_plan)?;
        
        let job_id = Uuid::new_v4().to_string();
        let schedule = Scheduler::new().create_job_schedule(&job_id, &fragments, workers)?;
        tracing::info!("MPP Execution: job_id={}, fragments={}", job_id, fragments.len());
        
        let mut tasks = Vec::new();
        let root_frag_id = fragments.last().map(|f| f.id).unwrap_or(0);
        
        for frag in fragments.iter() {
            let is_root = frag.id == root_frag_id;
            let engine = self.executor.as_any().downcast_ref::<DataFusionEngine>().unwrap();
            let ctx = engine.session_context();
            
            let rewritten_plan = Scheduler::new().rewrite_plan_for_execution(&frag.plan, &schedule)?;
            let mut sources_with_schema = Vec::new();
            Self::collect_remote_sources(&rewritten_plan, &mut sources_with_schema);
            
            let df_plan = rsdb_executor::convert::to_datafusion_plan(&rewritten_plan, ctx)?;
            let sub_plan = datafusion_substrait::logical_plan::producer::to_substrait_plan(&df_plan, &ctx.state()).unwrap();
            let plan_bytes = sub_plan.encode_to_vec();
            
            let proto_sources: Vec<rsdb_common::rpc::RemoteSource> = sources_with_schema.into_iter().map(|s| {
                rsdb_common::rpc::RemoteSource {
                    worker_addr: s.worker_addr,
                    task_id: s.task_id,
                    schema_ipc: self.encode_schema_to_ipc(&s.schema),
                }
            }).collect();
            
            let assignment = schedule.assignments.get(&frag.id).unwrap();
            for (i, worker_addr) in assignment.worker_addrs.iter().enumerate() {
                tasks.push(self.execute_plan_fragment_on_worker_stream(
                    worker_addr.clone(), plan_bytes.clone(), format!("{}-{}-{}", job_id, frag.id, i), is_root, proto_sources.clone(),
                ));
            }
        }
        
        let results: Vec<Vec<RecordBatch>> = futures::future::try_join_all(tasks).await?;
        Ok(results.into_iter().flatten().collect())
    }

    fn collect_remote_sources(plan: &RsdbLogicalPlan, out: &mut Vec<RemoteSourceWithSchema>) {
        match plan {
            RsdbLogicalPlan::RemoteExchange { sources, input, .. } => {
                let schema = input.schema();
                for s in sources {
                    out.push(RemoteSourceWithSchema { worker_addr: s.worker_addr.clone(), task_id: s.task_id.clone(), schema: schema.clone() });
                }
                Self::collect_remote_sources(input, out);
            }
            RsdbLogicalPlan::Filter { input, .. } | RsdbLogicalPlan::Project { input, .. } | RsdbLogicalPlan::Aggregate { input, .. }
            | RsdbLogicalPlan::Sort { input, .. } | RsdbLogicalPlan::Limit { input, .. } | RsdbLogicalPlan::Exchange { input, .. } => Self::collect_remote_sources(input, out),
            RsdbLogicalPlan::Join { left, right, .. } => { Self::collect_remote_sources(left, out); Self::collect_remote_sources(right, out); }
            _ => {}
        }
    }

    fn encode_schema_to_ipc(&self, schema: &arrow_schema::Schema) -> Vec<u8> {
        use arrow_ipc::writer::StreamWriter;
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, schema).unwrap();
        writer.finish().unwrap();
        buf
    }

    async fn execute_plan_fragment_on_worker_stream(
        &self,
        worker_addr: String,
        plan_bytes: Vec<u8>,
        task_id: String,
        return_data: bool,
        sources: Vec<rsdb_common::rpc::RemoteSource>,
    ) -> Result<Vec<RecordBatch>> {
        let mut client = Self::worker_task_client(&worker_addr).await?;
        let mut stream = client.execute_plan_fragment_stream(ExecutePlanFragmentRequest { 
            plan_bytes, task_id, return_data, sources 
        }).await.map_err(|e| RsdbError::Network(e.to_string()))?.into_inner();

        let mut batches = Vec::new();
        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| RsdbError::Network(e.to_string()))?;
            batches.push(self.decode_ipc_batch(&msg.arrow_ipc)?);
        }
        Ok(batches)
    }

    fn decode_ipc_batch(&self, bytes: &[u8]) -> Result<RecordBatch> {
        use std::io::Cursor;
        let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes.to_vec()), None).unwrap();
        for b in reader { return Ok(b.unwrap()); }
        Err(RsdbError::Execution("Empty batch".to_string()))
    }

    fn should_offload_sql(sql: &str) -> bool {
        let s = sql.trim_start().to_uppercase();
        s.starts_with("SELECT") || s.starts_with("WITH")
    }

    pub async fn register_csv(&mut self, name: &str, path: &Path) -> Result<()> {
        let engine = self.executor.as_any().downcast_ref::<DataFusionEngine>().unwrap();
        engine.register_csv(name, path, CsvReadOptions::new().has_header(true)).await?;

        // Sync to internal catalog for SqlPlanner
        let df = engine.session_context().table(name).await.map_err(|e| RsdbError::Execution(e.to_string()))?;
        let schema: arrow_schema::SchemaRef = df.schema().inner().clone();

        if let Some(schema_provider) = self.catalog.schema("default") {
            let _ = schema_provider.create_table(rsdb_catalog::TableEntry {
                name: name.to_string(),
                schema,
                partition_keys: vec![],
                storage_location: path.to_string_lossy().to_string(),
            }).await;
        }

        self.registered_tables.insert(name.to_string(), RegisteredTableSource::Csv { path: path.to_path_buf(), has_header: true, delimiter: b',' });
        self.sync_one_table_to_all_workers(name).await;
        Ok(())
    }

    pub async fn register_parquet(&mut self, table_name: &str, path: &Path) -> Result<()> {
        self.registered_tables.insert(table_name.to_string(), RegisteredTableSource::Parquet { path: path.to_path_buf() });
        self.sync_one_table_to_all_workers(table_name).await;
        Ok(())
    }

    async fn sync_one_table_to_all_workers(&self, name: &str) {
        if let Some(src) = self.registered_tables.get(name) {
            for w in self.list_workers() {
                let _ = Self::sync_one_table_to_worker_addr(&w.addr, name, src).await;
            }
        }
    }

    async fn sync_one_table_to_worker_addr(addr: &str, name: &str, src: &RegisteredTableSource) -> Result<()> {
        let mut client = Self::worker_task_client(addr).await?;
        match src {
            RegisteredTableSource::Csv { path, has_header, delimiter } => {
                client.register_csv(RegisterCsvRequest { table_name: name.to_string(), path: path.to_string_lossy().to_string(), has_header: *has_header, delimiter: *delimiter as u32 }).await.map_err(|e| RsdbError::Network(e.to_string()))?;
            }
            RegisteredTableSource::Parquet { path } => {
                client.register_parquet(RegisterParquetRequest { table_name: name.to_string(), path: path.to_string_lossy().to_string() }).await.map_err(|e| RsdbError::Network(e.to_string()))?;
            }
            RegisteredTableSource::View { sql } => {
                client.execute_sql(ExecuteSqlRequest { sql: sql.clone() }).await.map_err(|e| RsdbError::Network(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub async fn sync_tables_to_worker_addr(addr: &str, tables: &[(String, RegisteredTableSource)]) -> Result<()> {
        for (name, src) in tables { Self::sync_one_table_to_worker_addr(addr, name, src).await?; }
        Ok(())
    }

    async fn worker_task_client(addr: &str) -> Result<WorkerTaskClient<Channel>> {
        WorkerTaskClient::connect(format!("http://{}", addr)).await.map_err(|e| RsdbError::Network(e.to_string()))
    }

    pub fn list_tables(&self) -> Vec<String> {
        let engine = self.executor.as_any().downcast_ref::<DataFusionEngine>().unwrap();
        let state = engine.session_context().state();
        let mut tables = Vec::new();
        for cat in state.catalog_list().catalog_names() {
            if let Some(c) = state.catalog_list().catalog(&cat) {
                for sch in c.schema_names() {
                    if let Some(s) = c.schema(&sch) {
                        tables.extend(s.table_names());
                    }
                }
            }
        }
        tables
    }

    pub fn registered_table_sources(&self) -> Vec<(String, RegisteredTableSource)> {
        self.registered_tables.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn update_worker_heartbeat(&self, node_id: NodeId) { self.cluster_state.update_heartbeat(node_id); }
    pub fn register_worker(&mut self, node_id: NodeId, addr: String) { self.cluster_state.add_worker(node_id, addr); }
    pub fn list_workers(&self) -> Vec<WorkerInfo> { self.cluster_state.list_workers() }
    pub fn catalog(&self) -> &Arc<dyn CatalogProvider> { &self.catalog }
    pub fn executor(&self) -> &Arc<dyn ExecutionEngine> { &self.executor }
}

struct RemoteSourceWithSchema { worker_addr: String, task_id: String, schema: arrow_schema::SchemaRef }
