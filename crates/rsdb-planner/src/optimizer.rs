//! Optimizer rules
//!
//! Based on ClickHouse's implementation of subquery decorrelation
//! References:
//! - "Unnesting Arbitrary Queries"
//! - "Orthogonal Optimization of Subqueries and Aggregation"

use rsdb_common::Result;
use rsdb_sql::expr::Expr as RsdbExpr;
use rsdb_sql::logical_plan::LogicalPlan;

/// Optimizer that applies a chain of rules
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Optimizer {
    pub fn new() -> Self {
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(ConstantFolding),
            // Box::new(SubqueryDecorrelation), // Disabled - causes stack overflow
            Box::new(PredicatePushdown),
            Box::new(ProjectionPruning),
        ];
        Self { rules }
    }

    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut current = plan;
        for rule in &self.rules {
            current = rule.optimize(current)?;
        }
        Ok(current)
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimizer rule trait
pub trait OptimizerRule: Send + Sync {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
    fn name(&self) -> &str;
}

/// Predicate pushdown optimization
pub struct PredicatePushdown;

impl OptimizerRule for PredicatePushdown {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "predicate_pushdown"
    }
}

/// Projection pruning optimization
pub struct ProjectionPruning;

impl OptimizerRule for ProjectionPruning {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "projection_pruning"
    }
}

/// Constant folding optimization
pub struct ConstantFolding;

impl OptimizerRule for ConstantFolding {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "constant_folding"
    }
}

/// Subquery decorrelation optimization
/// Converts correlated subqueries to joins for efficient execution
pub struct SubqueryDecorrelation;

impl OptimizerRule for SubqueryDecorrelation {
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.decorrelate(plan)
    }

    fn name(&self) -> &str {
        "subquery_decorrelation"
    }
}

/// Information about a subquery found in an expression
#[derive(Debug)]
struct SubqueryInfo {
    outer_refs: Vec<String>,
}

impl SubqueryDecorrelation {
    /// Main entry point for decorrelation
    fn decorrelate(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Process Filter nodes - check for subquery expressions
            LogicalPlan::Filter { input, predicate } => {
                // Find all subquery expressions in the predicate
                let subqueries = find_subqueries(&predicate);

                if !subqueries.is_empty() {
                    // Check if any are correlated (have outer references)
                    let has_correlation = subqueries.iter().any(|sq| !sq.outer_refs.is_empty());

                    if has_correlation {
                        // This is a correlated subquery - skip processing to avoid infinite recursion
                        // The executor will handle it (return false = no rows)
                        // Just process the input non-recursively
                        let new_input = match *input {
                            LogicalPlan::Scan { .. } => input,
                            _ => Box::new(self.decorrelate_simple(*input)?),
                        };
                        Ok(LogicalPlan::Filter {
                            input: new_input,
                            predicate,
                        })
                    } else {
                        // No correlation - just recursively process
                        let new_input = Box::new(self.decorrelate(*input)?);
                        Ok(LogicalPlan::Filter {
                            input: new_input,
                            predicate,
                        })
                    }
                } else {
                    // No subqueries - just recurse
                    let new_input = Box::new(self.decorrelate(*input)?);
                    Ok(LogicalPlan::Filter {
                        input: new_input,
                        predicate,
                    })
                }
            }

            // Process Subquery nodes
            LogicalPlan::Subquery { query, schema } => {
                let new_query = Box::new(self.decorrelate(*query)?);
                Ok(LogicalPlan::Subquery {
                    query: new_query,
                    schema,
                })
            }

            LogicalPlan::SubqueryAlias { input, alias } => {
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::SubqueryAlias {
                    input: new_input,
                    alias,
                })
            }

            // Recursively process other plan types
            LogicalPlan::Scan { .. } => Ok(plan),

            LogicalPlan::Project {
                input,
                expr,
                schema,
            } => {
                let new_input = Box::new(self.decorrelate(*input)?);
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
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::Aggregate {
                    input: new_input,
                    group_expr,
                    aggregate_expr,
                    schema,
                })
            }

            LogicalPlan::Sort { input, expr } => {
                let new_input = Box::new(self.decorrelate(*input)?);
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
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::Limit {
                    input: new_input,
                    limit,
                    offset,
                })
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                join_condition,
                schema,
            } => {
                let new_left = Box::new(self.decorrelate(*left)?);
                let new_right = Box::new(self.decorrelate(*right)?);
                Ok(LogicalPlan::Join {
                    left: new_left,
                    right: new_right,
                    join_type,
                    join_condition,
                    schema,
                })
            }

            LogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let new_left = Box::new(self.decorrelate(*left)?);
                let new_right = Box::new(self.decorrelate(*right)?);
                Ok(LogicalPlan::CrossJoin {
                    left: new_left,
                    right: new_right,
                    schema,
                })
            }

            LogicalPlan::Union { inputs, schema } => {
                let new_inputs: Result<Vec<LogicalPlan>> =
                    inputs.into_iter().map(|p| self.decorrelate(p)).collect();
                Ok(LogicalPlan::Union {
                    inputs: new_inputs?,
                    schema,
                })
            }

            LogicalPlan::CreateTable { .. } => Ok(plan),
            LogicalPlan::DropTable { .. } => Ok(plan),
            LogicalPlan::Insert { .. } => Ok(plan),
            LogicalPlan::Analyze { .. } => Ok(plan),
            LogicalPlan::Explain { input } => {
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::Explain { input: new_input })
            }
            LogicalPlan::SubqueryAlias { input, alias } => {
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::SubqueryAlias {
                    input: new_input,
                    alias,
                })
            }
            LogicalPlan::Exchange { input, partitioning } => {
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::Exchange {
                    input: new_input,
                    partitioning,
                })
            }
            LogicalPlan::RemoteExchange { input, partitioning, sources } => {
                let new_input = Box::new(self.decorrelate(*input)?);
                Ok(LogicalPlan::RemoteExchange {
                    input: new_input,
                    partitioning,
                    sources,
                })
            }
            LogicalPlan::EmptyRelation => Ok(plan),
        }
    }

    /// Simple decorrelation that doesn't recurse into Filter nodes
    fn decorrelate_simple(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan { .. } => Ok(plan),
            LogicalPlan::EmptyRelation => Ok(plan),
            LogicalPlan::Exchange { .. } => Ok(plan),
            LogicalPlan::RemoteExchange { .. } => Ok(plan),
            LogicalPlan::Subquery { query, schema } => Ok(LogicalPlan::Subquery { query, schema }),
            LogicalPlan::Project {
                input,
                expr,
                schema,
            } => Ok(LogicalPlan::Project {
                input,
                expr,
                schema,
            }),
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggregate_expr,
                schema,
            } => Ok(LogicalPlan::Aggregate {
                input,
                group_expr,
                aggregate_expr,
                schema,
            }),
            LogicalPlan::Sort { input, expr } => Ok(LogicalPlan::Sort { input, expr }),
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => Ok(LogicalPlan::Limit {
                input,
                limit,
                offset,
            }),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                join_condition,
                schema,
            } => Ok(LogicalPlan::Join {
                left,
                right,
                join_type,
                join_condition,
                schema,
            }),
            LogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => Ok(LogicalPlan::CrossJoin {
                left,
                right,
                schema,
            }),
            LogicalPlan::Union { inputs, schema } => Ok(LogicalPlan::Union { inputs, schema }),
            LogicalPlan::Filter { input, predicate } => {
                // Don't recurse into Filter - just keep it as-is
                Ok(LogicalPlan::Filter { input, predicate })
            }
            LogicalPlan::CreateTable { .. } => Ok(plan),
            LogicalPlan::DropTable { .. } => Ok(plan),
            LogicalPlan::Insert { .. } => Ok(plan),
            LogicalPlan::Analyze { .. } => Ok(plan),
            LogicalPlan::Explain { input } => Ok(LogicalPlan::Explain { input }),
            LogicalPlan::SubqueryAlias { input, alias } => Ok(LogicalPlan::SubqueryAlias { input, alias }),
        }
    }
}

/// Find all subquery expressions in an expression
fn find_subqueries(expr: &RsdbExpr) -> Vec<SubqueryInfo> {
    let mut subqueries = vec![];
    collect_subqueries(expr, &mut subqueries);
    subqueries
}

/// Recursively collect subqueries from an expression
fn collect_subqueries(expr: &RsdbExpr, subqueries: &mut Vec<SubqueryInfo>) {
    match expr {
        RsdbExpr::Subquery { outer_refs, .. } => {
            subqueries.push(SubqueryInfo {
                outer_refs: outer_refs.clone(),
            });
        }
        RsdbExpr::BinaryOp { left, right, .. } => {
            collect_subqueries(left, subqueries);
            collect_subqueries(right, subqueries);
        }
        RsdbExpr::UnaryOp { expr, .. } => {
            collect_subqueries(expr, subqueries);
        }
        RsdbExpr::AggFunc { args, .. } => {
            for arg in args {
                collect_subqueries(arg, subqueries);
            }
        }
        RsdbExpr::Alias { expr, .. } => {
            collect_subqueries(expr, subqueries);
        }
        RsdbExpr::Function { args, .. } => {
            for arg in args {
                collect_subqueries(arg, subqueries);
            }
        }
        RsdbExpr::Cast { expr, .. } => {
            collect_subqueries(expr, subqueries);
        }
        RsdbExpr::Case { operand, conditions, results, else_result } => {
            if let Some(op) = operand {
                collect_subqueries(op, subqueries);
            }
            for e in conditions.iter().chain(results.iter()) {
                collect_subqueries(e, subqueries);
            }
            if let Some(else_res) = else_result {
                collect_subqueries(else_res, subqueries);
            }
        }
        RsdbExpr::Column(_) | RsdbExpr::Literal(_) => {}
    }
}

// ============================================================================
// FullOptimizer - Integrates rule-based + CBO Cascades
// ============================================================================

/// Full optimizer that combines rule-based rewrites with cost-based optimization.
///
/// Phase 1: Apply rule-based rewrites (constant folding, predicate pushdown, etc.)
/// Phase 2: If the plan contains joins and Cascades is enabled, run the Cascades
///          optimizer for join reordering and physical plan selection. Falls back
///          to the rule-based result if Cascades fails.
pub struct FullOptimizer {
    rule_optimizer: Optimizer,
    cbo_context: crate::cbo::CBOContext,
    enable_cascades: bool,
}

impl FullOptimizer {
    pub fn new() -> Self {
        Self {
            rule_optimizer: Optimizer::new(),
            cbo_context: crate::cbo::CBOContext::new(),
            enable_cascades: true,
        }
    }

    pub fn with_cbo_context(mut self, ctx: crate::cbo::CBOContext) -> Self {
        self.cbo_context = ctx;
        self
    }

    pub fn with_cascades(mut self, enable: bool) -> Self {
        self.enable_cascades = enable;
        self
    }

    /// Register table statistics for cost-based optimization
    pub fn register_table(&mut self, table_name: String, stats: crate::cbo::PlanStats) {
        self.cbo_context.register_table(table_name, stats);
    }

    /// Load table statistics from catalog for all tables referenced in a plan
    pub fn load_stats_from_catalog(
        &mut self,
        plan: &LogicalPlan,
        catalog: &dyn rsdb_catalog::CatalogProvider,
    ) {
        let table_refs = plan.table_refs();
        if let Some(schema) = catalog.schema("default") {
            for table_name in &table_refs {
                if let Some(catalog_stats) = schema.table_statistics(table_name) {
                    let plan_stats =
                        crate::stats_collector::catalog_stats_to_plan_stats(&catalog_stats);
                    self.cbo_context
                        .register_table(table_name.clone(), plan_stats);
                }
            }
        }
    }

    /// Optimize a logical plan through both phases
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Special case for EXPLAIN: optimize the inner plan
        if let LogicalPlan::Explain { input } = plan {
            let optimized_input = self.optimize(*input)?;
            return Ok(LogicalPlan::Explain { input: Box::new(optimized_input) });
        }

        // Phase 1: Global Logical Rewrites (Predicate Pushdown, Join Reordering base)
        let plan = crate::predicate_pushdown::PredicatePushdown::optimize(plan)?;

        // Phase 2: Heuristic Rule-based Optimization
        let plan = self.rule_optimizer.optimize(plan)?;

        // Phase 3: Cascades Cost-Based Optimizer (Join Ordering & Implementation Selection)
        let plan = if self.enable_cascades {
            let mut cascades =
                crate::cascades::CascadesOptimizer::new_with_stats(&self.cbo_context);
            
            let required = crate::property::Property {
                partitioning: rsdb_sql::logical_plan::Partitioning::Single,
                sorting: vec![],
            };

            match cascades.optimize(plan.clone(), required) {
                Ok(optimized) => optimized,
                Err(_) => plan,
            }
        } else {
            plan
        };

        // Phase 4: Distribution Planning (Physical Property Enforcement)
        DistributionPlanner::optimize(plan)
    }
}

impl Default for FullOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// DistributionPlanner - Physical property enforcement after CBO
// ============================================================================

/// DistributionPlanner
///
/// Ensures all operators have their distribution requirements met by inserting
/// Exchange (Shuffle) nodes.
pub struct DistributionPlanner;

impl DistributionPlanner {
    pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut planner = Self;
        planner.plan_distribution(plan)
    }

    fn plan_distribution(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate { input, group_expr, aggregate_expr, schema } => {
                let input = self.plan_distribution(*input)?;
                
                if group_expr.is_empty() {
                    // Global: Gather to Single partition
                    Ok(LogicalPlan::Aggregate {
                        input: Box::new(LogicalPlan::Exchange {
                            input: Box::new(input),
                            partitioning: rsdb_sql::logical_plan::Partitioning::Single,
                        }),
                        group_expr,
                        aggregate_expr,
                        schema,
                    })
                } else {
                    // Grouped: Hash Shuffle on group keys
                    Ok(LogicalPlan::Aggregate {
                        input: Box::new(LogicalPlan::Exchange {
                            input: Box::new(input),
                            partitioning: rsdb_sql::logical_plan::Partitioning::Hash(group_expr.clone(), 4),
                        }),
                        group_expr,
                        aggregate_expr,
                        schema,
                    })
                }
            }
            LogicalPlan::Join { left, right, join_type, join_condition, schema } => {
                let left_opt = self.plan_distribution(*left)?;
                let right_opt = self.plan_distribution(*right)?;
                
                if let rsdb_sql::logical_plan::JoinCondition::On(expr) = &join_condition {
                    let (keys1, keys2) = crate::cascades::extract_equi_join_keys(expr);
                    if !keys1.is_empty() && !keys2.is_empty() {
                        let left_schema = left_opt.schema();
                        // Determine which key set belongs to the left input
                        let k1_in_left = match &keys1[0] {
                            rsdb_sql::expr::Expr::Column(n) => left_schema.fields.iter().any(|f| f.name().as_str() == n.as_str()),
                            _ => false,
                        };
                        
                        let (l_keys, r_keys) = if k1_in_left { (keys1, keys2) } else { (keys2, keys1) };
                        
                        return Ok(LogicalPlan::Join {
                            left: Box::new(LogicalPlan::Exchange {
                                input: Box::new(left_opt),
                                partitioning: rsdb_sql::logical_plan::Partitioning::Hash(l_keys, 4),
                            }),
                            right: Box::new(LogicalPlan::Exchange {
                                input: Box::new(right_opt),
                                partitioning: rsdb_sql::logical_plan::Partitioning::Hash(r_keys, 4),
                            }),
                            join_type,
                            join_condition,
                            schema,
                        });
                    }
                }
                
                Ok(LogicalPlan::Join { left: Box::new(left_opt), right: Box::new(right_opt), join_type, join_condition, schema })
            }
            LogicalPlan::Filter { input, predicate } => {
                Ok(LogicalPlan::Filter { input: Box::new(self.plan_distribution(*input)?), predicate })
            }
            LogicalPlan::Project { input, expr, schema } => {
                Ok(LogicalPlan::Project { input: Box::new(self.plan_distribution(*input)?), expr, schema })
            }
            LogicalPlan::Sort { input, expr } => {
                // Global sort requires Single partition
                Ok(LogicalPlan::Sort {
                    input: Box::new(LogicalPlan::Exchange {
                        input: Box::new(self.plan_distribution(*input)?),
                        partitioning: rsdb_sql::logical_plan::Partitioning::Single,
                    }),
                    expr,
                })
            }
            LogicalPlan::Limit { input, limit, offset } => {
                // Global limit requires Single partition
                Ok(LogicalPlan::Limit {
                    input: Box::new(LogicalPlan::Exchange {
                        input: Box::new(self.plan_distribution(*input)?),
                        partitioning: rsdb_sql::logical_plan::Partitioning::Single,
                    }),
                    limit,
                    offset,
                })
            }
            LogicalPlan::SubqueryAlias { input, alias } => {
                Ok(LogicalPlan::SubqueryAlias { input: Box::new(self.plan_distribution(*input)?), alias })
            }
            LogicalPlan::Explain { input } => {
                Ok(LogicalPlan::Explain { input: Box::new(self.plan_distribution(*input)?) })
            }
            _ => Ok(plan),
        }
    }
}
