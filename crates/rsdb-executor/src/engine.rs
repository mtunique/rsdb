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
    catalog: Arc<InMemoryCatalog>,
}

impl DataFusionEngine {
    pub fn new() -> Self {
        let config = datafusion::prelude::SessionConfig::new()
            .with_information_schema(true)
            .set_bool("datafusion.sql_parser.enable_ident_normalization", true);
        Self { 
            ctx: SessionContext::new_with_config(config),
            catalog: Arc::new(InMemoryCatalog::new()),
        }
    }

    pub async fn register_csv(&self, table_name: &str, path: &Path, options: CsvReadOptions<'_>) -> Result<()> {
        self.ctx.register_csv(table_name, path.to_str().unwrap(), options.clone()).await
            .map_err(|e| RsdbError::Execution(format!("Failed to register CSV {table_name}: {e}")))?;
        
        // Register in RsDB catalog as well to keep them in sync
        // Note: We need to extract schema from options or infer it. 
        // For simplicity, we rely on build_catalog_from_datafusion to sync schemas lazily,
        // but we keep the catalog persistent to store stats.
        Ok(())
    }

    pub fn session_context(&self) -> &SessionContext { &self.ctx }

    async fn get_catalog(&self) -> Result<Arc<InMemoryCatalog>> {
        // Sync schema from DataFusion to RsDB catalog
        let schema = self.catalog.schema("default").unwrap();
        let state = self.ctx.state();
        let catalog_list = state.catalog_list();
        for cat_name in catalog_list.catalog_names() {
            if let Some(cat) = catalog_list.catalog(&cat_name) {
                for schema_name in cat.schema_names() {
                    if let Some(s) = cat.schema(&schema_name) {
                        for table_name in s.table_names() {
                            if !schema.table_exists(&table_name) {
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
        }
        Ok(self.catalog.clone())
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let catalog = self.get_catalog().await?;
        let planner = SqlPlanner::new(catalog.clone());
        let plan = planner.plan(sql)?;
        
        // Run Optimizer
        let mut optimizer = rsdb_planner::FullOptimizer::new();
        // Load stats for CBO
        optimizer.load_stats_from_catalog(&plan, catalog.as_ref());
        let optimized_plan = optimizer.optimize(plan)?;
        
        self.execute_logical_plan(&optimized_plan).await
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
                    let catalog = self.get_catalog().await?;
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
            RsdbLogicalPlan::Explain { input } => {
                let catalog = self.get_catalog().await?;
                let mut cbo_ctx = rsdb_planner::CBOContext::new();
                
                // Load stats
                if let Some(schema) = catalog.schema("default") {
                    for table_ref in input.table_refs() {
                        if let Some(stats) = schema.table_statistics(&table_ref) {
                            cbo_ctx.register_table(table_ref, rsdb_planner::catalog_stats_to_plan_stats(&stats));
                        }
                    }
                }

                let plan_str = explain_plan_with_stats(input, &cbo_ctx, 0);
                let schema = plan.schema();
                let array = arrow_array::StringArray::from(vec![plan_str]);
                let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])
                    .map_err(|e| RsdbError::Execution(e.to_string()))?;
                Ok(vec![batch])
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

        let bound_plan = inject_remote(logical, remote_sources);
        
        // Create execution plan
        let physical = self.ctx.state().create_physical_plan(&bound_plan).await
            .map_err(|e| RsdbError::Execution(format!("create physical plan error: {e}")))?;
            
        let task_ctx = self.ctx.task_ctx();
        datafusion::physical_plan::execute_stream(physical, task_ctx)
            .map_err(|e| RsdbError::Execution(format!("execute stream error: {e}")))
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

fn explain_plan_with_stats(plan: &RsdbLogicalPlan, cbo: &rsdb_planner::CBOContext, indent: usize) -> String {
    let stats = rsdb_planner::stats_derivation::derive_stats_recursive(plan, cbo);
    let indent_str = if indent == 0 { "".to_string() } else { format!("{}|_ ", "  ".repeat(indent - 1)) };
    
    let mut output = match plan {
        RsdbLogicalPlan::Scan { table_name, filters, .. } => {
            format!("Scan: {} [Filters: {:?}]", table_name, filters.len())
        }
        RsdbLogicalPlan::Filter { predicate, .. } => {
            format!("Filter: {:?}", predicate)
        }
        RsdbLogicalPlan::Project { expr, .. } => {
            format!("Project: {:?}", expr)
        }
        RsdbLogicalPlan::Aggregate { group_expr, .. } => {
            format!("Aggregate: [Group: {:?}]", group_expr.len())
        }
        RsdbLogicalPlan::Sort { .. } => "Sort".to_string(),
        RsdbLogicalPlan::Limit { limit, .. } => format!("Limit: {}", limit),
        RsdbLogicalPlan::Join { join_type, join_condition, .. } => {
            format!("Join: {:?} ON {:?}", join_type, join_condition)
        }
        RsdbLogicalPlan::CrossJoin { .. } => "CrossJoin".to_string(),
        RsdbLogicalPlan::Exchange { partitioning, .. } => {
            let p_str = match partitioning {
                rsdb_sql::logical_plan::Partitioning::Hash(keys, n) => format!("Hash({:?}, {})", keys, n),
                rsdb_sql::logical_plan::Partitioning::Single => "Single".to_string(),
                rsdb_sql::logical_plan::Partitioning::Broadcast => "Broadcast".to_string(),
                _ => format!("{:?}", partitioning),
            };
            format!("Exchange: {}", p_str)
        },
        RsdbLogicalPlan::Explain { .. } => "Explain".to_string(),
        _ => format!("{:?}", plan),
    };

    output = format!("{}{:<50} [Rows: {}]", indent_str, output, stats.row_count);

    match plan {
        RsdbLogicalPlan::Filter { input, .. } 
        | RsdbLogicalPlan::Project { input, .. }
        | RsdbLogicalPlan::Aggregate { input, .. }
        | RsdbLogicalPlan::Sort { input, .. }
        | RsdbLogicalPlan::Limit { input, .. }
        | RsdbLogicalPlan::Explain { input }
        | RsdbLogicalPlan::Exchange { input, .. } => {
            output.push('\n');
            output.push_str(&explain_plan_with_stats(input, cbo, indent + 1));
        }
        RsdbLogicalPlan::Join { left, right, .. } | RsdbLogicalPlan::CrossJoin { left, right, .. } => {
            output.push('\n');
            output.push_str(&explain_plan_with_stats(left, cbo, indent + 1));
            output.push('\n');
            output.push_str(&explain_plan_with_stats(right, cbo, indent + 1));
        }
        RsdbLogicalPlan::Union { inputs, .. } => {
            for input in inputs {
                output.push('\n');
                output.push_str(&explain_plan_with_stats(input, cbo, indent + 1));
            }
        }
        _ => {}
    }
    
    output
}
