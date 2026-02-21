//! Execution engine

use crate::convert::to_datafusion_plan;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::*;
use rsdb_catalog::CatalogProvider;
use rsdb_catalog::InMemoryCatalog;
use rsdb_common::{Result, RsdbError};
use rsdb_sql::logical_plan::LogicalPlan as RsdbLogicalPlan;
use rsdb_sql::SqlPlanner;
use std::any::Any;
use std::path::Path;
use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::datasource::provider_as_source;
use futures::FutureExt;
use prost::Message;

/// Execution engine trait
#[async_trait]
pub trait ExecutionEngine: Send + Sync {
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>>;
    async fn execute_logical_plan(&self, plan: &RsdbLogicalPlan) -> Result<Vec<RecordBatch>>;
    async fn execute_stream(&self, plan_bytes: &[u8], remote_sources: &[rsdb_common::rpc::RemoteSource]) -> Result<SendableRecordBatchStream>;
    fn as_any(&self) -> &dyn Any;
}

pub struct DataFusionEngine {
    ctx: SessionContext,
}

impl DataFusionEngine {
    pub fn new() -> Self {
        let config = datafusion::prelude::SessionConfig::new()
            .with_information_schema(true)
            .set_bool("datafusion.sql_parser.enable_ident_normalization", true);
        Self { ctx: SessionContext::new_with_config(config) }
    }

    pub async fn register_csv(&self, table_name: &str, path: &Path, options: CsvReadOptions<'_>) -> Result<()> {
        self.ctx.register_csv(table_name, path.to_str().unwrap(), options).await
            .map_err(|e| RsdbError::Execution(format!("Failed to register CSV {table_name}: {e}")))
    }

    pub fn session_context(&self) -> &SessionContext { &self.ctx }

    async fn build_catalog_from_datafusion(&self) -> Result<Arc<InMemoryCatalog>> {
        let catalog = Arc::new(InMemoryCatalog::new());
        let schema = catalog.schema("default").unwrap();
        let state = self.ctx.state();
        let catalog_list = state.catalog_list();
        for cat_name in catalog_list.catalog_names() {
            if let Some(cat) = catalog_list.catalog(&cat_name) {
                for schema_name in cat.schema_names() {
                    if let Some(s) = cat.schema(&schema_name) {
                        for table_name in s.table_names() {
                            if let Some(Ok(Some(tp))) = s.table(&table_name).now_or_never() {
                                let entry = rsdb_catalog::TableEntry {
                                    name: table_name.clone(),
                                    schema: tp.schema(),
                                    partition_keys: vec![],
                                    storage_location: String::new(),
                                };
                                let _ = schema.create_table(entry).await;
                            }
                        }
                    }
                }
            }
        }
        Ok(catalog)
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let catalog = self.build_catalog_from_datafusion().await?;
        let planner = SqlPlanner::new(catalog);
        let plan = planner.plan(sql)?;
        self.execute_logical_plan(&plan).await
    }

    pub async fn execute_logical_plan(&self, plan: &RsdbLogicalPlan) -> Result<Vec<RecordBatch>> {
        match plan {
            RsdbLogicalPlan::Analyze { table_name } => {
                let df = self.ctx.table(table_name.as_str()).await
                    .map_err(|e| RsdbError::Execution(format!("table not found: {e}")))?;
                let batches = df.aggregate(vec![], vec![datafusion::functions_aggregate::expr_fn::count(datafusion::prelude::lit(1))])
                    .map_err(|e| RsdbError::Execution(format!("aggregate error: {e}")))?
                    .collect().await
                    .map_err(|e| RsdbError::Execution(format!("collect error: {e}")))?;
                
                if !batches.is_empty() && batches[0].num_rows() > 0 {
                    let count = batches[0].column(0)
                        .as_any().downcast_ref::<arrow_array::Int64Array>()
                        .map(|a| a.value(0) as u64).unwrap_or(0);
                    
                    // Update catalog
                    let catalog = self.build_catalog_from_datafusion().await?;
                    if let Some(schema) = catalog.schema("default") {
                        let mut stats = rsdb_catalog::TableStatistics {
                            row_count: count,
                            total_size_bytes: count * 100, // Heuristic
                            ..Default::default()
                        };
                        // Add some dummy column stats for CBO to have something to work with
                        if let Some(table) = schema.table(table_name) {
                            for field in table.schema.fields() {
                                stats.column_statistics.insert(field.name().clone(), rsdb_catalog::ColumnStatistics {
                                    distinct_count: count / 10 + 1, // Heuristic
                                    null_count: 0,
                                    ..Default::default()
                                });
                            }
                        }
                        schema.set_table_statistics(table_name, stats);
                        println!("Analyzed table {}: {} rows", table_name, count);
                    }
                }
                Ok(vec![])
            }
            _ => {
                let df_plan = to_datafusion_plan(plan, &self.ctx)?;
                
                // METADATA BINDING: Replace TableScans with actual Providers
                fn bind_providers(p: datafusion::logical_expr::LogicalPlan, ctx: &SessionContext) -> datafusion::logical_expr::LogicalPlan {
                    use datafusion::logical_expr::{LogicalPlan, TableScan};
                    match p {
                        LogicalPlan::TableScan(ts) if ts.table_name.to_string() != "remote_exchange" => {
                            let name = ts.table_name.table();
                            if let Some(tp) = futures::executor::block_on(ctx.table_provider(name)).ok() {
                                LogicalPlan::TableScan(TableScan { source: provider_as_source(tp), ..ts })
                            } else {
                                LogicalPlan::TableScan(ts)
                            }
                        }
                        _ => {
                            let inputs = p.inputs().into_iter().cloned().map(|i| bind_providers(i, ctx)).collect();
                            let exprs = p.expressions();
                            p.with_new_exprs(exprs, inputs).unwrap()
                        }
                    }
                }
                
                let bound_plan = bind_providers(df_plan, &self.ctx);
                let df = self.ctx.execute_logical_plan(bound_plan).await.map_err(|e| RsdbError::Execution(e.to_string()))?;
                df.collect().await.map_err(|e| RsdbError::Execution(e.to_string()))
            }
        }
    }

    pub async fn execute_stream(
        &self,
        plan_bytes: &[u8],
        remote_sources: &[rsdb_common::rpc::RemoteSource],
    ) -> Result<SendableRecordBatchStream> {
        let plan = datafusion_substrait::substrait::proto::Plan::decode(plan_bytes)
            .map_err(|e| RsdbError::Execution(format!("Substrait decode error: {e}")))?;
        let logical = datafusion_substrait::logical_plan::consumer::from_substrait_plan(&self.ctx.state(), &plan).await
            .map_err(|e| RsdbError::Execution(format!("Substrait consume error: {e}")))?;
        
        // RECURSIVE METADATA INJECTION: Essential for Distributed execution
        fn inject_remote(p: datafusion::logical_expr::LogicalPlan, sources: &[rsdb_common::rpc::RemoteSource]) -> datafusion::logical_expr::LogicalPlan {
            use datafusion::logical_expr::{LogicalPlan, TableScan};
            use crate::physical_plan::shuffle_reader::{PartitionLocation, RemoteExchangeProvider};
            
            match p {
                LogicalPlan::TableScan(ts) if ts.table_name.to_string().contains("remote_exchange") => {
                    let locations: Vec<PartitionLocation> = sources.iter().map(|s| PartitionLocation {
                        worker_addr: s.worker_addr.clone(),
                        ticket: s.task_id.as_bytes().to_vec(),
                    }).collect();
                    
                    // SIDE-CAR SCHEMA RECOVERY
                    let schema = if !sources.is_empty() && !sources[0].schema_ipc.is_empty() {
                        use arrow_ipc::reader::StreamReader;
                        use std::io::Cursor;
                        StreamReader::try_new(Cursor::new(&sources[0].schema_ipc), None).unwrap().schema()
                    } else {
                        Arc::new(ts.projected_schema.as_arrow().clone())
                    };

                    let provider = Arc::new(RemoteExchangeProvider { partitions: vec![locations], schema });
                    LogicalPlan::TableScan(TableScan { source: provider_as_source(provider), ..ts })
                }
                _ => {
                    let inputs = p.inputs().into_iter().cloned().map(|i| inject_remote(i, sources)).collect();
                    let exprs = p.expressions();
                    p.with_new_exprs(exprs, inputs).unwrap()
                }
            }
        }
        
        let df = self.ctx.execute_logical_plan(inject_remote(logical, remote_sources)).await
            .map_err(|e| RsdbError::Execution(format!("DataFusion error: {e}")))?;
        df.execute_stream().await.map_err(|e| RsdbError::Execution(format!("Stream error: {e}")))
    }
}

#[async_trait]
impl ExecutionEngine for DataFusionEngine {
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> { DataFusionEngine::execute_sql(self, sql).await }
    async fn execute_logical_plan(&self, plan: &RsdbLogicalPlan) -> Result<Vec<RecordBatch>> { DataFusionEngine::execute_logical_plan(self, plan).await }
    async fn execute_stream(&self, pb: &[u8], rs: &[rsdb_common::rpc::RemoteSource]) -> Result<SendableRecordBatchStream> { DataFusionEngine::execute_stream(self, pb, rs).await }
    fn as_any(&self) -> &dyn Any { self }
}

impl Default for DataFusionEngine { fn default() -> Self { Self::new() } }
