//! Execution engine

use crate::convert::to_datafusion_plan;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::*;
use prost::Message;
use rsdb_catalog::CatalogProvider;
use rsdb_catalog::InMemoryCatalog;
use rsdb_common::{Result, RsdbError};
use rsdb_sql::logical_plan::LogicalPlan as RsdbLogicalPlan;
use rsdb_sql::SqlPlanner;
use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

/// Execution engine trait
#[async_trait]
pub trait ExecutionEngine: Send + Sync {
    /// Execute a Substrait plan and return record batches
    async fn execute(&self, plan_bytes: &[u8]) -> Result<Vec<RecordBatch>>;

    /// Execute a SQL query directly
    async fn execute_sql(&self, _sql: &str) -> Result<Vec<RecordBatch>> {
        Err(RsdbError::Execution(
            "Direct SQL execution not supported".to_string(),
        ))
    }

    /// Execute a LogicalPlan (RSDB's LogicalPlan)
    async fn execute_logical_plan(&self, _plan: &RsdbLogicalPlan) -> Result<Vec<RecordBatch>> {
        Err(RsdbError::Execution(
            "LogicalPlan execution not supported".to_string(),
        ))
    }

    /// Get a reference to self as Any (for downcasting)
    fn as_any(&self) -> &dyn Any;
}

/// DataFusion-based execution engine
pub struct DataFusionEngine {
    ctx: SessionContext,
    /// Cache for table statistics collected via ANALYZE
    table_stats: Arc<Mutex<HashMap<String, rsdb_planner::PlanStats>>>,
}

impl DataFusionEngine {
    pub fn new() -> Self {
        let config = datafusion::prelude::SessionConfig::new().with_information_schema(true);
        Self {
            ctx: SessionContext::new_with_config(config),
            table_stats: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a CSV file as a table
    pub async fn register_csv(
        &self,
        table_name: &str,
        path: &Path,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        self.ctx
            .register_csv(table_name, path.to_str().unwrap(), options)
            .await
            .map_err(|e| RsdbError::Execution(format!("Failed to register CSV {table_name}: {e}")))
    }

    /// Execute a SQL query directly
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| RsdbError::Execution(format!("SQL execution error: {e}")))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| RsdbError::Execution(format!("Failed to collect results: {e}")))?;
        Ok(batches)
    }

    /// Execute a LogicalPlan - converts to DataFusion plan and executes
    pub async fn execute_logical_plan(&self, plan: &RsdbLogicalPlan) -> Result<Vec<RecordBatch>> {
        match plan {
            RsdbLogicalPlan::Analyze { table_name } => {
                let stats = self.collect_table_stats(&[table_name]).await?;
                if let Some(s) = stats.get(table_name) {
                    eprintln!("Analyzed table {}: {} rows", table_name, s.row_count);
                    // Persist stats for CBO
                    let mut cache = self.table_stats.lock().unwrap();
                    cache.insert(table_name.clone(), s.clone());
                }
                Ok(vec![])
            }
            RsdbLogicalPlan::DropTable {
                table_name,
                if_exists,
            } => {
                if self.ctx.table_exist(table_name).unwrap_or(false) {
                    self.ctx.deregister_table(table_name).map_err(|e| {
                        RsdbError::Execution(format!("Failed to drop table {table_name}: {e}"))
                    })?;
                    // Clear stats
                    let mut cache = self.table_stats.lock().unwrap();
                    cache.remove(table_name);
                    eprintln!("Dropped table {}", table_name);
                } else if !if_exists {
                    return Err(RsdbError::Execution(format!(
                        "Table {table_name} not found"
                    )));
                }
                Ok(vec![])
            }
            RsdbLogicalPlan::CreateTable {
                table_name,
                schema,
                if_not_exists,
                ..
            } => {
                if self.ctx.table_exist(table_name).unwrap_or(false) {
                    if !if_not_exists {
                        return Err(RsdbError::Execution(format!(
                            "Table {table_name} already exists"
                        )));
                    }
                } else {
                    use datafusion::datasource::MemTable;
                    // MemTable needs at least one partition even if empty
                    let table = MemTable::try_new(schema.clone(), vec![vec![]]).map_err(|e| {
                        RsdbError::Execution(format!("Failed to create MemTable: {e}"))
                    })?;
                    self.ctx
                        .register_table(table_name, Arc::new(table))
                        .map_err(|e| {
                            RsdbError::Execution(format!(
                                "Failed to register table {table_name}: {e}"
                            ))
                        })?;
                    eprintln!("Created table {}", table_name);
                }
                Ok(vec![])
            }
            _ => {
                // Convert RSDB LogicalPlan to DataFusion LogicalPlan.
                // The conversion is synchronous and recursive. Deep queries might cause stack overflow,
                // so we execute it in a dedicated thread with a larger stack.
                let plan = plan.clone();
                let df_plan = tokio::task::spawn_blocking({
                    let ctx = self.ctx.clone();
                    move || {
                        std::thread::Builder::new()
                            .name("rsdb-to-df-plan".to_string())
                            .stack_size(16 * 1024 * 1024)
                            .spawn(move || to_datafusion_plan(&plan, &ctx))
                            .map_err(|e| {
                                RsdbError::Execution(format!(
                                    "Failed to spawn plan convert thread: {e}"
                                ))
                            })?
                            .join()
                            .map_err(|_| {
                                RsdbError::Execution("Plan convert thread panicked".to_string())
                            })?
                    }
                })
                .await
                .map_err(|e| {
                    RsdbError::Execution(format!("Plan convert task join error: {e}"))
                })??;

                // Execute via DataFusion
                let df = self.ctx.execute_logical_plan(df_plan).await.map_err(|e| {
                    RsdbError::Execution(format!("DataFusion execution error: {e}"))
                })?;

                let batches = df
                    .collect()
                    .await
                    .map_err(|e| RsdbError::Execution(format!("Failed to collect results: {e}")))?;

                Ok(batches)
            }
        }
    }

    /// Execute SQL using RSDB's full LogicalPlan pipeline with CBO
    pub async fn execute_via_logical_plan(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let catalog = self.build_catalog_from_datafusion().await?;
        let stats = self.table_stats.lock().unwrap().clone();

        // Planning can also be deep, so use a large stack thread.
        let sql = sql.to_string();
        let catalog_for_planner = catalog.clone();
        let logical_plan = tokio::task::spawn_blocking(move || {
            std::thread::Builder::new()
                .name("rsdb-sql-plan".to_string())
                .stack_size(16 * 1024 * 1024)
                .spawn(move || {
                    let planner = SqlPlanner::new(catalog_for_planner);
                    let plan = planner.plan(&sql)?;

                    // Apply CBO if it's a query and we have statistics
                    if matches!(
                        plan,
                        RsdbLogicalPlan::Scan { .. }
                            | RsdbLogicalPlan::Join { .. }
                            | RsdbLogicalPlan::Filter { .. }
                    ) {
                        let mut optimizer = rsdb_planner::FullOptimizer::new().with_cascades(true);
                        for (table_name, table_stats) in stats {
                            optimizer.register_table(table_name, table_stats);
                        }
                        optimizer.optimize(plan)
                    } else {
                        Ok(plan)
                    }
                })
                .map_err(|e| RsdbError::Execution(format!("Failed to spawn planner thread: {e}")))?
                .join()
                .map_err(|_| RsdbError::Execution("Planner thread panicked".to_string()))?
        })
        .await
        .map_err(|e| RsdbError::Execution(format!("Planner task join error: {e}")))??;
        self.execute_logical_plan(&logical_plan).await
    }

    /// Build an RSDB InMemoryCatalog from registered DataFusion tables for type inference.
    async fn build_catalog_from_datafusion(&self) -> Result<Arc<InMemoryCatalog>> {
        let catalog = Arc::new(InMemoryCatalog::new());
        let schema = catalog
            .schema("default")
            .ok_or_else(|| RsdbError::Execution("default schema not found".to_string()))?;

        // Enumerate all tables from DataFusion's CatalogList
        let state = self.ctx.state();
        let catalog_list = state.catalog_list();
        let mut tables = Vec::new();
        for catalog_name in catalog_list.catalog_names() {
            if let Some(df_catalog) = catalog_list.catalog(&catalog_name) {
                for schema_name in df_catalog.schema_names() {
                    if let Some(df_schema) = df_catalog.schema(&schema_name) {
                        tables.extend(df_schema.table_names());
                    }
                }
            }
        }

        for table_name in tables {
            // Skip DataFusion internal tables
            if table_name.starts_with("information_schema") {
                continue;
            }

            // Get real Arrow Schema via DataFusion
            let df = match self.ctx.table(&table_name).await {
                Ok(df) => df,
                Err(_) => continue,
            };

            let arrow_schema = Arc::new(df.schema().as_arrow().clone());
            let entry = rsdb_catalog::TableEntry {
                name: table_name.clone(),
                schema: arrow_schema,
                partition_keys: vec![],
                storage_location: format!("datafusion/{table_name}"),
            };

            // Register in catalog if it doesn't exist
            if !schema.table_exists(&table_name) {
                let _ = schema.create_table(entry).await;
            }
        }

        Ok(catalog)
    }

    /// Collect statistics from all registered tables
    pub async fn collect_table_stats(
        &self,
        table_names: &[&str],
    ) -> Result<HashMap<String, rsdb_planner::PlanStats>> {
        let mut table_stats = HashMap::new();

        for table_name in table_names {
            // Get row count using COUNT(*)
            let count_sql = format!("SELECT COUNT(*) as cnt FROM {}", table_name);
            if let Ok(batches) = self.execute_sql(&count_sql).await {
                if let Some(batch) = batches.first() {
                    let row_count = if let Some(array) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::UInt64Array>()
                    {
                        array.value(0)
                    } else if let Some(array) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                    {
                        array.value(0) as u64
                    } else {
                        continue;
                    };

                    table_stats.insert(
                        table_name.to_string(),
                        rsdb_planner::PlanStats {
                            row_count,
                            output_size: row_count * 100,
                            column_stats: HashMap::new(),
                        },
                    );
                }
            }
        }

        Ok(table_stats)
    }

    /// Execute SQL using RSDB's FullOptimizer with collected statistics
    pub async fn execute_with_cbo(
        &self,
        sql: &str,
        table_names: &[&str],
        enable_cascades: bool,
    ) -> Result<Vec<RecordBatch>> {
        let stats = self.collect_table_stats(table_names).await?;

        let mut optimizer = rsdb_planner::FullOptimizer::new().with_cascades(enable_cascades);
        for (table_name, table_stats) in stats {
            optimizer.register_table(table_name, table_stats);
        }

        let catalog = self.build_catalog_from_datafusion().await?;
        let sql = sql.to_string();
        let catalog_for_planner = catalog.clone();
        let optimized_plan = tokio::task::spawn_blocking(move || {
            std::thread::Builder::new()
                .name("rsdb-cbo-opt".to_string())
                .stack_size(16 * 1024 * 1024)
                .spawn(move || {
                    let planner = SqlPlanner::new(catalog_for_planner);
                    let logical_plan = planner.plan(&sql)?;
                    optimizer.optimize(logical_plan)
                })
                .map_err(|e| {
                    RsdbError::Execution(format!("Failed to spawn optimizer thread: {e}"))
                })?
                .join()
                .map_err(|_| RsdbError::Execution("Optimizer thread panicked".to_string()))?
        })
        .await
        .map_err(|e| RsdbError::Execution(format!("Optimizer task join error: {e}")))??;

        self.execute_logical_plan(&optimized_plan).await
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionEngine for DataFusionEngine {
    async fn execute(&self, plan_bytes: &[u8]) -> Result<Vec<RecordBatch>> {
        if plan_bytes.is_empty() {
            return Ok(vec![]);
        }

        let plan = datafusion_substrait::substrait::proto::Plan::decode(plan_bytes).map_err(|e| {
            RsdbError::Execution(format!("Failed to decode Substrait plan bytes: {e}"))
        })?;

        let logical = datafusion_substrait::logical_plan::consumer::from_substrait_plan(
            &self.ctx.state(),
            &plan,
        )
        .await
        .map_err(|e| RsdbError::Execution(format!("Failed to consume Substrait plan: {e}")))?;

        let df = self
            .ctx
            .execute_logical_plan(logical)
            .await
            .map_err(|e| RsdbError::Execution(format!("Failed to execute logical plan: {e}")))?;

        df.collect()
            .await
            .map_err(|e| RsdbError::Execution(format!("Failed to collect results: {e}")))
    }

    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        DataFusionEngine::execute_sql(self, sql).await
    }

    async fn execute_logical_plan(&self, plan: &RsdbLogicalPlan) -> Result<Vec<RecordBatch>> {
        DataFusionEngine::execute_logical_plan(self, plan).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
