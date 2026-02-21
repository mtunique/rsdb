//! Cost-Based Optimizer (CBO)
//!
//! Reference: ClickHouse cost-based optimizer implementation
//! Key features:
//! - Join graph construction
//! - Selectivity-based join ordering (greedy algorithm)
//! - Cardinality estimation for operators
//! - Cost model for join, scan, filter, aggregation
//! - StatsProvider trait for pluggable statistics sources

use rsdb_common::Result;
use rsdb_sql::expr::Expr as RsdbExpr;
use rsdb_sql::logical_plan::LogicalPlan;
use std::collections::{HashMap, HashSet};

// ============================================================================
// Cost - Multi-dimensional
// ============================================================================

/// Multi-dimensional cost estimate
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost {
    pub cpu: f64,
    pub memory: f64,
    pub network: f64,
}

impl Default for Cost {
    fn default() -> Self {
        Self::infinite()
    }
}

impl Cost {
    pub fn zero() -> Self {
        Self {
            cpu: 0.0,
            memory: 0.0,
            network: 0.0,
        }
    }

    pub fn infinite() -> Self {
        Self {
            cpu: f64::INFINITY,
            memory: f64::INFINITY,
            network: f64::INFINITY,
        }
    }

    pub fn new(cpu: f64, memory: f64, network: f64) -> Self {
        Self {
            cpu,
            memory,
            network,
        }
    }

    pub fn total(&self) -> f64 {
        self.cpu + 0.5 * self.memory + 2.0 * self.network
    }

    pub fn add(&self, other: &Cost) -> Self {
        Self {
            cpu: self.cpu + other.cpu,
            memory: self.memory + other.memory,
            network: self.network + other.network,
        }
    }

    pub fn is_less_than(&self, other: &Cost) -> bool {
        self.total() < other.total()
    }

    pub fn is_infinite(&self) -> bool {
        self.cpu.is_infinite() || self.memory.is_infinite() || self.network.is_infinite()
    }
}

// ============================================================================
// Statistics Types
// ============================================================================

/// Statistics for a plan node
#[derive(Debug, Clone, Default)]
pub struct PlanStats {
    pub row_count: u64,
    pub output_size: u64,
    pub column_stats: HashMap<String, ColumnStats>,
}

/// Column statistics
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub ndv: u64,
    pub null_count: u64,
    pub min: Option<String>,
    pub max: Option<String>,
    pub size: u64,
}

// ============================================================================
// StatsProvider trait - Pluggable table statistics source
// ============================================================================

/// Trait for providing table statistics to the optimizer.
/// Implement this to integrate with a catalog or metadata store.
pub trait StatsProvider: Send + Sync {
    fn get_table_stats(&self, table_name: &str) -> Option<PlanStats>;
    fn get_column_stats(&self, table_name: &str, column_name: &str) -> Option<ColumnStats>;
}

/// In-memory stats provider backed by a HashMap
#[derive(Debug, Clone, Default)]
pub struct InMemoryStatsProvider {
    pub table_stats: HashMap<String, PlanStats>,
}

impl StatsProvider for InMemoryStatsProvider {
    fn get_table_stats(&self, table_name: &str) -> Option<PlanStats> {
        self.table_stats.get(table_name).cloned()
    }

    fn get_column_stats(&self, table_name: &str, column_name: &str) -> Option<ColumnStats> {
        self.table_stats
            .get(table_name)
            .and_then(|s| s.column_stats.get(column_name).cloned())
    }
}

// ============================================================================
// CostModel - Per-operator cost parameters
// ============================================================================

/// Cost model with per-operator cost parameters
#[derive(Debug, Clone)]
pub struct CostModel {
    pub scan_cost_per_row: f64,
    pub filter_cost_per_row: f64,
    pub hash_build_cost_per_row: f64,
    pub hash_probe_cost_per_row: f64,
    pub sort_cost_per_row: f64,
    pub aggregate_cost_per_row: f64,
    pub project_cost_per_row: f64,
    pub network_cost_per_row: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            scan_cost_per_row: 1.0,
            filter_cost_per_row: 0.5,
            hash_build_cost_per_row: 2.0,
            hash_probe_cost_per_row: 1.5,
            sort_cost_per_row: 3.0,
            aggregate_cost_per_row: 2.0,
            project_cost_per_row: 0.1,
            network_cost_per_row: 5.0,
        }
    }
}

impl CostModel {
    /// Compute cost for a single operator (not including children)
    pub fn compute_operator_cost(&self, operator: &LogicalPlan, row_count: f64) -> Cost {
        let rows = row_count.max(1.0);
        match operator {
            LogicalPlan::Scan { .. } => Cost::new(rows * self.scan_cost_per_row, 0.0, 0.0),
            LogicalPlan::Filter { .. } => Cost::new(rows * self.filter_cost_per_row, 0.0, 0.0),
            LogicalPlan::Project { .. } => Cost::new(rows * self.project_cost_per_row, 0.0, 0.0),
            LogicalPlan::Join { .. } => {
                // HashJoin cost: build side + probe side
                // Use row_count as an approximation for both sides
                let build_cost = rows * self.hash_build_cost_per_row;
                let probe_cost = rows * self.hash_probe_cost_per_row;
                Cost::new(build_cost + probe_cost, rows * 8.0, 0.0)
            }
            LogicalPlan::CrossJoin { .. } => {
                // Cross join is expensive: O(n^2)
                Cost::new(rows * rows * 0.1, rows * 8.0, 0.0)
            }
            LogicalPlan::Sort { .. } => {
                let n = rows;
                let log_n = if n > 1.0 { n.log2() } else { 1.0 };
                Cost::new(n * log_n * self.sort_cost_per_row, n * 8.0, 0.0)
            }
            LogicalPlan::Aggregate { .. } => {
                Cost::new(rows * self.aggregate_cost_per_row, rows * 8.0, 0.0)
            }
            LogicalPlan::Exchange { .. } => {
                Cost::new(rows * 0.1, 0.0, rows * self.network_cost_per_row)
            }
            LogicalPlan::Limit { .. } => Cost::new(1.0, 0.0, 0.0),
            LogicalPlan::Union { .. } => Cost::new(rows * 0.1, 0.0, 0.0),
            _ => Cost::new(1.0, 0.0, 0.0),
        }
    }

    /// Compute cost for a physical join given build/probe row counts
    pub fn compute_hash_join_cost(&self, build_rows: f64, probe_rows: f64) -> Cost {
        let cpu =
            build_rows * self.hash_build_cost_per_row + probe_rows * self.hash_probe_cost_per_row;
        let memory = build_rows * 8.0; // hash table memory
        Cost::new(cpu, memory, 0.0)
    }

    /// Compute cost for a sort-merge join
    pub fn compute_sort_merge_join_cost(&self, left_rows: f64, right_rows: f64) -> Cost {
        let left_sort = if left_rows > 1.0 {
            left_rows * left_rows.log2() * self.sort_cost_per_row
        } else {
            0.0
        };
        let right_sort = if right_rows > 1.0 {
            right_rows * right_rows.log2() * self.sort_cost_per_row
        } else {
            0.0
        };
        let merge = left_rows + right_rows;
        Cost::new(left_sort + right_sort + merge, 0.0, 0.0)
    }
}

// ============================================================================
// Join Graph Types
// ============================================================================

/// Join edge in the join graph
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub left_table: String,
    pub right_table: String,
    pub left_key: String,
    pub right_key: String,
    pub selectivity: f64,
    pub output_rows: u64,
}

/// Join graph representation
#[derive(Debug, Clone, Default)]
pub struct JoinGraph {
    pub tables: Vec<String>,
    pub edges: Vec<JoinEdge>,
    pub table_stats: HashMap<String, PlanStats>,
}

/// Join order node - represents a partial join order
#[derive(Debug, Clone)]
pub struct JoinOrderNode {
    pub tables: Vec<String>,
    pub output_rows: u64,
    pub edges: Vec<JoinEdge>,
}

impl JoinOrderNode {
    pub fn new(table: String, stats: &PlanStats) -> Self {
        Self {
            tables: vec![table],
            output_rows: stats.row_count,
            edges: vec![],
        }
    }

    pub fn merge(&self, other: &JoinOrderNode, edge: &JoinEdge, new_stats: PlanStats) -> Self {
        let mut tables = self.tables.clone();
        tables.extend(other.tables.clone());
        let mut edges = self.edges.clone();
        edges.extend(other.edges.clone());
        edges.push(edge.clone());

        Self {
            tables,
            output_rows: new_stats.row_count,
            edges,
        }
    }
}

// ============================================================================
// CBO Configuration and Context
// ============================================================================

/// CBO configuration
#[derive(Debug, Clone)]
pub struct CBOConfig {
    pub default_filter_selectivity: f64,
    pub max_join_reorder_size: usize,
    pub enable_join_reordering: bool,
    pub heuristic_join_reorder_times: usize,
}

impl Default for CBOConfig {
    fn default() -> Self {
        Self {
            default_filter_selectivity: 0.1,
            max_join_reorder_size: 10,
            enable_join_reordering: true,
            heuristic_join_reorder_times: 10,
        }
    }
}

/// CBO context - holds statistics and configuration
pub struct CBOContext {
    pub table_stats: HashMap<String, PlanStats>,
    config: CBOConfig,
    pub default_row_count: u64,
}

impl CBOContext {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
            config: CBOConfig::default(),
            default_row_count: 1000000,
        }
    }

    pub fn with_config(config: CBOConfig) -> Self {
        Self {
            table_stats: HashMap::new(),
            config,
            default_row_count: 1000000,
        }
    }

    /// Create a new CBOContext that shares statistics from an existing one
    pub fn new_from(other: &CBOContext) -> Self {
        Self {
            table_stats: other.table_stats.clone(),
            config: other.config.clone(),
            default_row_count: other.default_row_count,
        }
    }

    pub fn register_table(&mut self, table_name: String, stats: PlanStats) {
        self.table_stats.insert(table_name, stats);
    }

    pub fn get_table_stats(&self, table_name: &str) -> Option<&PlanStats> {
        self.table_stats.get(table_name)
    }

    pub fn get_or_default_stats(&self, table_name: &str) -> PlanStats {
        self.table_stats
            .get(table_name)
            .cloned()
            .unwrap_or_else(|| PlanStats {
                row_count: self.default_row_count,
                output_size: self.default_row_count * 100,
                column_stats: HashMap::new(),
            })
    }

    pub fn config(&self) -> &CBOConfig {
        &self.config
    }
}

impl Default for CBOContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Cardinality Estimation - Improved
// ============================================================================

/// Estimate selectivity of a predicate
pub fn estimate_selectivity(predicate: &RsdbExpr, cbo: &CBOContext) -> f64 {
    match predicate {
        RsdbExpr::BinaryOp { left, op, right } => match op {
            rsdb_sql::expr::BinaryOperator::And => {
                estimate_selectivity(left, cbo) * estimate_selectivity(right, cbo)
            }
            rsdb_sql::expr::BinaryOperator::Or => {
                let l = estimate_selectivity(left, cbo);
                let r = estimate_selectivity(right, cbo);
                (l + r - l * r).min(1.0)
            }
            rsdb_sql::expr::BinaryOperator::Eq => {
                // Try to use NDV for equality predicates
                if let RsdbExpr::Column(col) = left.as_ref() {
                    if let Some(ndv) = find_column_ndv(col, cbo) {
                        if ndv > 0 {
                            return 1.0 / ndv as f64;
                        }
                    }
                }
                cbo.config().default_filter_selectivity
            }
            rsdb_sql::expr::BinaryOperator::Lt
            | rsdb_sql::expr::BinaryOperator::Lte
            | rsdb_sql::expr::BinaryOperator::Gt
            | rsdb_sql::expr::BinaryOperator::Gte => {
                // Range predicates: assume 1/3 selectivity
                0.33
            }
            rsdb_sql::expr::BinaryOperator::Neq => {
                if let RsdbExpr::Column(col) = left.as_ref() {
                    if let Some(ndv) = find_column_ndv(col, cbo) {
                        if ndv > 0 {
                            return 1.0 - 1.0 / ndv as f64;
                        }
                    }
                }
                0.9
            }
            _ => cbo.config().default_filter_selectivity,
        },
        RsdbExpr::UnaryOp { op, expr } => {
            if matches!(op, rsdb_sql::expr::UnaryOperator::Not) {
                1.0 - estimate_selectivity(expr, cbo)
            } else {
                cbo.config().default_filter_selectivity
            }
        }
        _ => cbo.config().default_filter_selectivity,
    }
}

/// Find NDV for a column across all registered tables
fn find_column_ndv(col_name: &str, cbo: &CBOContext) -> Option<u64> {
    for stats in cbo.table_stats.values() {
        if let Some(col_stats) = stats.column_stats.get(col_name) {
            if col_stats.ndv > 0 {
                return Some(col_stats.ndv);
            }
        }
    }
    None
}

/// Improved join selectivity: 1 / max(NDV_left, NDV_right)
pub fn estimate_join_selectivity(left_key: &str, right_key: &str, cbo: &CBOContext) -> f64 {
    let left_ndv = find_column_ndv(left_key, cbo);
    let right_ndv = find_column_ndv(right_key, cbo);

    match (left_ndv, right_ndv) {
        (Some(l), Some(r)) => {
            let max_ndv = l.max(r).max(1) as f64;
            1.0 / max_ndv
        }
        (Some(ndv), None) | (None, Some(ndv)) => 1.0 / ndv.max(1) as f64,
        (None, None) => cbo.config().default_filter_selectivity,
    }
}

/// Estimate cardinality for a filter
pub fn estimate_filter_cardinality(
    input_stats: &PlanStats,
    predicate: &RsdbExpr,
    cbo: &CBOContext,
) -> PlanStats {
    let selectivity = estimate_selectivity(predicate, cbo);
    let filtered_rows = (input_stats.row_count as f64 * selectivity) as u64;
    let output_size = (input_stats.output_size as f64 * selectivity) as u64;

    PlanStats {
        row_count: filtered_rows.max(1),
        output_size,
        column_stats: input_stats.column_stats.clone(),
    }
}

/// Estimate cardinality for a join using NDV-based selectivity
pub fn estimate_join_cardinality(
    left_stats: &PlanStats,
    right_stats: &PlanStats,
    left_key: &str,
    right_key: &str,
    cbo: &CBOContext,
) -> PlanStats {
    let selectivity = estimate_join_selectivity(left_key, right_key, cbo);
    let output_rows =
        ((left_stats.row_count as f64 * right_stats.row_count as f64) * selectivity) as u64;
    let output_size = output_rows * 100;

    PlanStats {
        row_count: output_rows.max(1),
        output_size,
        column_stats: HashMap::new(),
    }
}

// ============================================================================
// Join Graph Construction
// ============================================================================

fn build_join_graph(plan: &LogicalPlan, cbo: &CBOContext) -> JoinGraph {
    let mut tables = Vec::new();
    let mut edges = Vec::new();
    collect_join_elements(plan, &mut tables, &mut edges, cbo);

    let mut unique_tables: Vec<String> = Vec::new();
    for t in tables {
        if !unique_tables.contains(&t) {
            unique_tables.push(t);
        }
    }

    JoinGraph {
        tables: unique_tables,
        edges,
        table_stats: cbo.table_stats.clone(),
    }
}

fn collect_join_elements(
    plan: &LogicalPlan,
    tables: &mut Vec<String>,
    edges: &mut Vec<JoinEdge>,
    cbo: &CBOContext,
) {
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            collect_join_elements(left, tables, edges, cbo);
            collect_join_elements(right, tables, edges, cbo);

            if let rsdb_sql::logical_plan::JoinCondition::On(expr) = join_condition {
                let join_keys = extract_join_keys(expr);
                let left_tables = get_tables_from_plan(left);
                let right_tables = get_tables_from_plan(right);

                for (left_key, right_key) in join_keys {
                    let (left_table, right_table) =
                        if left_tables.len() == 1 && right_tables.len() == 1 {
                            (left_tables[0].clone(), right_tables[0].clone())
                        } else {
                            (
                                left_tables.first().cloned().unwrap_or_default(),
                                right_tables.first().cloned().unwrap_or_default(),
                            )
                        };

                    if !left_table.is_empty() && !right_table.is_empty() {
                        let left_stats = cbo.get_or_default_stats(&left_table);
                        let right_stats = cbo.get_or_default_stats(&right_table);

                        let selectivity = estimate_join_selectivity(&left_key, &right_key, cbo);
                        let output_rows = ((left_stats.row_count as f64
                            * right_stats.row_count as f64)
                            * selectivity) as u64;

                        edges.push(JoinEdge {
                            left_table,
                            right_table,
                            left_key,
                            right_key,
                            selectivity,
                            output_rows,
                        });
                    }
                }
            }
        }
        LogicalPlan::CrossJoin { left, right, .. } => {
            collect_join_elements(left, tables, edges, cbo);
            collect_join_elements(right, tables, edges, cbo);
        }
        LogicalPlan::Scan { table_name, .. } => {
            if !tables.contains(table_name) {
                tables.push(table_name.clone());
            }
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. } => {
            collect_join_elements(input, tables, edges, cbo);
        }
        LogicalPlan::Subquery { query, .. } => {
            collect_join_elements(query, tables, edges, cbo);
        }
        _ => {}
    }
}

fn get_tables_from_plan(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Scan { table_name, .. } => vec![table_name.clone()],
        LogicalPlan::CrossJoin { left, right, .. } | LogicalPlan::Join { left, right, .. } => {
            let mut tables = get_tables_from_plan(left);
            tables.extend(get_tables_from_plan(right));
            tables
        }
        LogicalPlan::Filter { input, .. } | LogicalPlan::Project { input, .. } => {
            get_tables_from_plan(input)
        }
        _ => vec![],
    }
}

fn extract_join_keys(expr: &RsdbExpr) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    extract_equality_conditions(expr, &mut keys);
    keys
}

fn extract_equality_conditions(expr: &RsdbExpr, keys: &mut Vec<(String, String)>) {
    match expr {
        RsdbExpr::BinaryOp { left, op, right } => {
            if matches!(op, rsdb_sql::expr::BinaryOperator::Eq) {
                if let RsdbExpr::Column(col_left) = left.as_ref() {
                    if let RsdbExpr::Column(col_right) = right.as_ref() {
                        keys.push((col_left.clone(), col_right.clone()));
                        return;
                    }
                }
            }
            extract_equality_conditions(left, keys);
            extract_equality_conditions(right, keys);
        }
        _ => {}
    }
}

// ============================================================================
// Join Reordering (Greedy)
// ============================================================================

fn find_optimal_join_order(graph: &JoinGraph, cbo: &CBOContext) -> Vec<String> {
    if graph.tables.len() <= 1 {
        return graph.tables.clone();
    }

    let stats_map: HashMap<String, PlanStats> = graph.table_stats.clone();

    let mut edge_map: HashMap<(String, String), &JoinEdge> = HashMap::new();
    for edge in &graph.edges {
        edge_map.insert((edge.left_table.clone(), edge.right_table.clone()), edge);
        edge_map.insert((edge.right_table.clone(), edge.left_table.clone()), edge);
    }

    let mut remaining: HashSet<String> = graph.tables.iter().cloned().collect();
    let mut result: Vec<String> = Vec::new();

    let mut current_table = remaining
        .iter()
        .min_by_key(|t| stats_map.get(*t).map(|s| s.row_count).unwrap_or(u64::MAX))
        .cloned()
        .unwrap_or_else(|| graph.tables[0].clone());

    result.push(current_table.clone());
    remaining.remove(&current_table);

    while !remaining.is_empty() {
        let mut best_table: Option<String> = None;
        let mut best_output: u64 = u64::MAX;

        for table in &remaining {
            let edge_key = if edge_map.contains_key(&(current_table.clone(), table.clone())) {
                (current_table.clone(), table.clone())
            } else if edge_map.contains_key(&(table.clone(), current_table.clone())) {
                (table.clone(), current_table.clone())
            } else {
                continue;
            };

            if let Some(edge) = edge_map.get(&edge_key) {
                let default_stats = PlanStats {
                    row_count: cbo.default_row_count,
                    output_size: cbo.default_row_count * 100,
                    column_stats: HashMap::new(),
                };
                let left_stats = stats_map
                    .get(&edge_key.0)
                    .cloned()
                    .unwrap_or(default_stats.clone());
                let right_stats = stats_map.get(&edge_key.1).cloned().unwrap_or(default_stats);

                let output_rows = ((left_stats.row_count as f64 * right_stats.row_count as f64)
                    * edge.selectivity) as u64;

                if output_rows < best_output {
                    best_output = output_rows;
                    best_table = Some(table.clone());
                }
            }
        }

        if let Some(next_table) = best_table {
            result.push(next_table.clone());
            remaining.remove(&next_table);
            current_table = next_table;
        } else {
            for t in remaining {
                result.push(t);
            }
            break;
        }
    }

    result
}

// ============================================================================
// Reorder joins in a plan
// ============================================================================

fn reorder_joins(plan: LogicalPlan, cbo: &CBOContext) -> Result<LogicalPlan> {
    if !cbo.config().enable_join_reordering {
        return Ok(plan);
    }

    let join_graph = build_join_graph(&plan, cbo);
    if join_graph.tables.len() > cbo.config().max_join_reorder_size {
        return Ok(plan);
    }

    let _optimal_order = find_optimal_join_order(&join_graph, cbo);

    // Recursively reorder sub-plans
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let new_input = Box::new(reorder_joins(*input, cbo)?);
            Ok(LogicalPlan::Filter {
                input: new_input,
                predicate,
            })
        }
        LogicalPlan::Project {
            input,
            expr,
            schema,
        } => {
            let new_input = Box::new(reorder_joins(*input, cbo)?);
            Ok(LogicalPlan::Project {
                input: new_input,
                expr,
                schema,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggregate_expr,
            schema,
        } => {
            let new_input = Box::new(reorder_joins(*input, cbo)?);
            Ok(LogicalPlan::Aggregate {
                input: new_input,
                group_expr,
                aggregate_expr,
                schema,
            })
        }
        LogicalPlan::Sort { input, expr } => {
            let new_input = Box::new(reorder_joins(*input, cbo)?);
            Ok(LogicalPlan::Sort {
                input: new_input,
                expr,
            })
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let new_input = Box::new(reorder_joins(*input, cbo)?);
            Ok(LogicalPlan::Limit {
                input: new_input,
                limit,
                offset,
            })
        }
        LogicalPlan::CrossJoin {
            left,
            right,
            schema,
        } => {
            let new_left = Box::new(reorder_joins(*left, cbo)?);
            let new_right = Box::new(reorder_joins(*right, cbo)?);
            Ok(LogicalPlan::CrossJoin {
                left: new_left,
                right: new_right,
                schema,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            join_condition,
            schema,
        } => {
            let new_left = Box::new(reorder_joins(*left, cbo)?);
            let new_right = Box::new(reorder_joins(*right, cbo)?);
            Ok(LogicalPlan::Join {
                left: new_left,
                right: new_right,
                join_type,
                join_condition,
                schema,
            })
        }
        LogicalPlan::Union { inputs, schema } => {
            let new_inputs: Result<Vec<LogicalPlan>> =
                inputs.into_iter().map(|p| reorder_joins(p, cbo)).collect();
            Ok(LogicalPlan::Union {
                inputs: new_inputs?,
                schema,
            })
        }
        LogicalPlan::Subquery { query, schema } => {
            let new_query = Box::new(reorder_joins(*query, cbo)?);
            Ok(LogicalPlan::Subquery {
                query: new_query,
                schema,
            })
        }
        LogicalPlan::Exchange {
            input,
            partitioning,
        } => {
            let new_input = Box::new(reorder_joins(*input, cbo)?);
            Ok(LogicalPlan::Exchange {
                input: new_input,
                partitioning,
            })
        }
        _ => Ok(plan),
    }
}

// ============================================================================
// Main CBO Optimizer
// ============================================================================

/// Cost-based optimizer
pub struct CBOOptimizer {
    context: CBOContext,
}

impl CBOOptimizer {
    pub fn new() -> Self {
        Self {
            context: CBOContext::new(),
        }
    }

    pub fn with_context(context: CBOContext) -> Self {
        Self { context }
    }

    pub fn context(&mut self) -> &mut CBOContext {
        &mut self.context
    }

    pub fn get_context(&self) -> &CBOContext {
        &self.context
    }

    pub fn optimize(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = reorder_joins(plan, &self.context)?;
        let optimizer = super::Optimizer::new();
        let optimized = optimizer.optimize(plan)?;
        Ok(optimized)
    }

    pub fn optimize_with_cascades(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        use super::CascadesOptimizer;

        let mut cascades = CascadesOptimizer::new_with_stats(&self.context);
        let plan = cascades.optimize(plan)?;

        let optimizer = super::Optimizer::new();
        let optimized = optimizer.optimize(plan)?;
        Ok(optimized)
    }
}

impl Default for CBOOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_graph_construction() {
        let mut cbo = CBOContext::new();
        cbo.register_table(
            "orders".to_string(),
            PlanStats {
                row_count: 1000000,
                output_size: 100_000_000,
                column_stats: HashMap::new(),
            },
        );
        cbo.register_table(
            "customers".to_string(),
            PlanStats {
                row_count: 100000,
                output_size: 10_000_000,
                column_stats: HashMap::new(),
            },
        );

        let join_graph = JoinGraph {
            tables: vec!["orders".to_string(), "customers".to_string()],
            edges: vec![JoinEdge {
                left_table: "orders".to_string(),
                right_table: "customers".to_string(),
                left_key: "customer_id".to_string(),
                right_key: "id".to_string(),
                selectivity: 0.1,
                output_rows: 10000,
            }],
            table_stats: cbo.table_stats.clone(),
        };

        assert_eq!(join_graph.tables.len(), 2);
        assert_eq!(join_graph.edges.len(), 1);
    }

    #[test]
    fn test_join_ordering() {
        let mut cbo = CBOContext::new();
        cbo.register_table(
            "small".to_string(),
            PlanStats {
                row_count: 100,
                output_size: 1000,
                column_stats: HashMap::new(),
            },
        );
        cbo.register_table(
            "medium".to_string(),
            PlanStats {
                row_count: 10000,
                output_size: 100_000,
                column_stats: HashMap::new(),
            },
        );
        cbo.register_table(
            "large".to_string(),
            PlanStats {
                row_count: 1000000,
                output_size: 10_000_000,
                column_stats: HashMap::new(),
            },
        );

        let graph = JoinGraph {
            tables: vec![
                "small".to_string(),
                "medium".to_string(),
                "large".to_string(),
            ],
            edges: vec![],
            table_stats: cbo.table_stats.clone(),
        };

        let order = find_optimal_join_order(&graph, &cbo);
        assert_eq!(order[0], "small");
    }

    #[test]
    fn test_cost_model_scan() {
        let model = CostModel::default();
        let scan = LogicalPlan::Scan {
            table_name: "t1".to_string(),
            schema: std::sync::Arc::new(arrow_schema::Schema::empty()),
            projection: None,
            filters: vec![],
        };
        let cost = model.compute_operator_cost(&scan, 1000.0);
        assert_eq!(cost.cpu, 1000.0);
        assert_eq!(cost.memory, 0.0);
    }

    #[test]
    fn test_cost_model_hash_join() {
        let model = CostModel::default();
        let cost = model.compute_hash_join_cost(100.0, 1000.0);
        assert!(cost.cpu > 0.0);
        assert!(cost.memory > 0.0);
    }

    #[test]
    fn test_join_selectivity_with_ndv() {
        let mut cbo = CBOContext::new();
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "id".to_string(),
            ColumnStats {
                ndv: 1000,
                ..Default::default()
            },
        );
        cbo.register_table(
            "t1".to_string(),
            PlanStats {
                row_count: 10000,
                output_size: 100000,
                column_stats: col_stats,
            },
        );

        let mut col_stats2 = HashMap::new();
        col_stats2.insert(
            "ref_id".to_string(),
            ColumnStats {
                ndv: 500,
                ..Default::default()
            },
        );
        cbo.register_table(
            "t2".to_string(),
            PlanStats {
                row_count: 5000,
                output_size: 50000,
                column_stats: col_stats2,
            },
        );

        let sel = estimate_join_selectivity("id", "ref_id", &cbo);
        // Should be 1/max(1000, 500) = 1/1000 = 0.001
        assert!((sel - 0.001).abs() < 1e-9);
    }

    #[test]
    fn test_new_from_context() {
        let mut cbo = CBOContext::new();
        cbo.register_table(
            "t1".to_string(),
            PlanStats {
                row_count: 42,
                output_size: 420,
                column_stats: HashMap::new(),
            },
        );

        let cbo2 = CBOContext::new_from(&cbo);
        assert_eq!(cbo2.get_or_default_stats("t1").row_count, 42);
    }
}
