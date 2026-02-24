//! Statistics Derivation Module
//!
//! Provides traits and implementations for deriving statistics (cardinality,
//! distinct values, etc.) for logical operators bottom-up.
//! Reference: ClickHouse CardinalityEstimator architecture.

use crate::cbo::{CBOContext, PlanStats};
use rsdb_sql::expr::{BinaryOperator, Expr as RsdbExpr, UnaryOperator};
use rsdb_sql::logical_plan::{JoinCondition, JoinType, LogicalPlan};

/// Trait for deriving statistics for a logical plan node
pub trait StatsDerivator {
    fn derive(&self, input_stats: &[&PlanStats], cbo: &CBOContext) -> PlanStats;
}

/// Derivator for Filter operator
pub struct FilterStatsDerivator<'a> {
    pub predicate: &'a RsdbExpr,
}

impl<'a> StatsDerivator for FilterStatsDerivator<'a> {
    fn derive(&self, input_stats: &[&PlanStats], cbo: &CBOContext) -> PlanStats {
        if input_stats.is_empty() {
            return PlanStats::default();
        }
        let input = input_stats[0];
        let selectivity = estimate_selectivity(self.predicate, cbo);
        
        let row_count = (input.row_count as f64 * selectivity).ceil() as u64;
        // Output size scales with row count
        let output_size = (input.output_size as f64 * selectivity).ceil() as u64;

        // Propagate column stats, adjusting NDV based on selectivity
        let mut column_stats = input.column_stats.clone();
        for stat in column_stats.values_mut() {
            // Simple heuristic: NDV reduces slower than row count
            if stat.ndv > row_count {
                stat.ndv = row_count;
            } else {
                stat.ndv = (stat.ndv as f64 * selectivity.sqrt()).ceil() as u64; 
            }
        }

        PlanStats {
            row_count: row_count.max(1),
            output_size,
            column_stats,
        }
    }
}

/// Derivator for Join operator
pub struct JoinStatsDerivator<'a> {
    pub join_type: JoinType,
    pub condition: &'a JoinCondition,
}

impl<'a> StatsDerivator for JoinStatsDerivator<'a> {
    fn derive(&self, input_stats: &[&PlanStats], cbo: &CBOContext) -> PlanStats {
        if input_stats.len() < 2 {
            return PlanStats::default();
        }
        let left = input_stats[0];
        let right = input_stats[1];

        let mut selectivity = match self.join_type {
            JoinType::Cross => 1.0,
            _ => cbo.config().default_filter_selectivity, // Default join selectivity
        };

        if let JoinCondition::On(expr) = self.condition {
            // Try to find equi-join keys to improve estimation
            if let Some((left_col, right_col)) = find_join_keys(expr) {
                let left_ndv = get_ndv(left, &left_col);
                let right_ndv = get_ndv(right, &right_col);
                let max_ndv = left_ndv.max(right_ndv).max(1);
                selectivity = 1.0 / max_ndv as f64;
            }
        } else if let JoinCondition::Using(cols) = self.condition {
             if let Some(col) = cols.first() {
                let left_ndv = get_ndv(left, col);
                let right_ndv = get_ndv(right, col);
                let max_ndv = left_ndv.max(right_ndv).max(1);
                selectivity = 1.0 / max_ndv as f64;
             }
        }

        let card_product = left.row_count as f64 * right.row_count as f64;
        let mut row_count = if card_product.is_finite() {
            let mut derived = (card_product * selectivity).ceil() as u64;
            // Cap for Equi-Join: Shouldn't exceed the product, but usually 
            // closer to the larger side in 1:N relationships.
            if matches!(self.condition, JoinCondition::On(_) | JoinCondition::Using(_)) {
                derived = derived.min(std::cmp::max(left.row_count, right.row_count) * 2);
            }
            derived
        } else {
            u64::MAX
        };

        // Adjust for Outer joins
        match self.join_type {
            JoinType::Left => row_count = row_count.max(left.row_count),
            JoinType::Right => row_count = row_count.max(right.row_count),
            JoinType::Full => row_count = row_count.max(left.row_count + right.row_count),
            _ => {}
        }

        // Merge column stats
        let mut column_stats = left.column_stats.clone();
        for (k, v) in &right.column_stats {
            // Handle name collisions if necessary, usually handled by qualified names
            column_stats.insert(k.clone(), v.clone()); 
        }

        PlanStats {
            row_count: row_count.max(1),
            output_size: row_count.saturating_mul(100), // Approximation
            column_stats,
        }
    }
}

/// Derivator for Aggregate operator
pub struct AggregateStatsDerivator<'a> {
    pub group_expr: &'a [RsdbExpr],
}

impl<'a> StatsDerivator for AggregateStatsDerivator<'a> {
    fn derive(&self, input_stats: &[&PlanStats], _cbo: &CBOContext) -> PlanStats {
        if input_stats.is_empty() {
            return PlanStats::default();
        }
        let input = input_stats[0];

        let row_count = if self.group_expr.is_empty() {
            1 // Global aggregate
        } else {
            // Group by cardinality estimation
            // Ideally: product of NDVs of group columns, capped by input rows
            let mut product_ndv = 1.0;
            for expr in self.group_expr {
                if let RsdbExpr::Column(name) = expr {
                    product_ndv *= get_ndv(input, name) as f64;
                } else {
                    // Complex expr, assume high cardinality
                    product_ndv *= 10.0; 
                }
            }
            product_ndv.min(input.row_count as f64) as u64
        };

        PlanStats {
            row_count: row_count.max(1),
            output_size: row_count * 50,
            column_stats: input.column_stats.clone(), // Rough approximation
        }
    }
}

/// Derivator for Project/Sort/Limit/Exchange (Cardinality Preserving or limiting)
pub struct SimpleStatsDerivator {
    pub limit: Option<usize>,
}

impl StatsDerivator for SimpleStatsDerivator {
    fn derive(&self, input_stats: &[&PlanStats], _cbo: &CBOContext) -> PlanStats {
        if input_stats.is_empty() {
            return PlanStats::default();
        }
        let input = input_stats[0];
        let mut stats = input.clone();
        
        if let Some(limit) = self.limit {
            stats.row_count = stats.row_count.min(limit as u64);
            stats.output_size = (stats.output_size as f64 * (stats.row_count as f64 / input.row_count as f64)).ceil() as u64;
        }
        
        stats
    }
}

// Helpers

fn estimate_selectivity(predicate: &RsdbExpr, cbo: &CBOContext) -> f64 {
    match predicate {
        RsdbExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                estimate_selectivity(left, cbo) * estimate_selectivity(right, cbo)
            }
            BinaryOperator::Or => {
                let l = estimate_selectivity(left, cbo);
                let r = estimate_selectivity(right, cbo);
                (l + r - l * r).min(1.0)
            }
            BinaryOperator::Eq => 0.05, // Default equality selectivity
            BinaryOperator::Neq => 0.95,
            BinaryOperator::Lt | BinaryOperator::Lte | BinaryOperator::Gt | BinaryOperator::Gte => 0.33,
            _ => 0.1,
        },
        RsdbExpr::UnaryOp { op: UnaryOperator::Not, expr } => {
            1.0 - estimate_selectivity(expr, cbo)
        }
        _ => 0.1,
    }
}

fn get_ndv(stats: &PlanStats, col: &str) -> u64 {
    stats.column_stats.get(col).map(|s| s.ndv).unwrap_or(100) // Default NDV
}

fn find_join_keys(expr: &RsdbExpr) -> Option<(String, String)> {
    match expr {
        RsdbExpr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            if let (RsdbExpr::Column(l), RsdbExpr::Column(r)) = (left.as_ref(), right.as_ref()) {
                Some((l.clone(), r.clone()))
            } else {
                None
            }
        }
        RsdbExpr::BinaryOp { left, op: BinaryOperator::And, .. } => {
            // Just take the first one for now
            find_join_keys(left)
        }
        _ => None
    }
}

/// Recursively derive statistics for a LogicalPlan
pub fn derive_stats_recursive(plan: &LogicalPlan, cbo: &CBOContext) -> PlanStats {
    match plan {
        LogicalPlan::Scan { table_name, .. } => {
            if let Some(stats) = cbo.get_table_stats(table_name) {
                stats.clone()
            } else {
                PlanStats::default() // Or some default guess
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            let input_stats = derive_stats_recursive(input, cbo);
            let derivator = FilterStatsDerivator { predicate };
            derivator.derive(&[&input_stats], cbo)
        }
        LogicalPlan::Join { left, right, join_type, join_condition, .. } => {
            let left_stats = derive_stats_recursive(left, cbo);
            let right_stats = derive_stats_recursive(right, cbo);
            let derivator = JoinStatsDerivator { join_type: *join_type, condition: join_condition };
            derivator.derive(&[&left_stats, &right_stats], cbo)
        }
        LogicalPlan::CrossJoin { left, right, .. } => {
            let left_stats = derive_stats_recursive(left, cbo);
            let right_stats = derive_stats_recursive(right, cbo);
            let derivator = JoinStatsDerivator { join_type: JoinType::Cross, condition: &JoinCondition::None };
            derivator.derive(&[&left_stats, &right_stats], cbo)
        }
        LogicalPlan::Aggregate { input, group_expr, .. } => {
            let input_stats = derive_stats_recursive(input, cbo);
            let derivator = AggregateStatsDerivator { group_expr };
            derivator.derive(&[&input_stats], cbo)
        }
        LogicalPlan::Project { input, .. } => {
            let input_stats = derive_stats_recursive(input, cbo);
            // Project preserves cardinality
            let derivator = SimpleStatsDerivator { limit: None };
            derivator.derive(&[&input_stats], cbo)
        }
        LogicalPlan::Sort { input, .. } => {
            let input_stats = derive_stats_recursive(input, cbo);
            let derivator = SimpleStatsDerivator { limit: None };
            derivator.derive(&[&input_stats], cbo)
        }
        LogicalPlan::Limit { input, limit, .. } => {
            let input_stats = derive_stats_recursive(input, cbo);
            let derivator = SimpleStatsDerivator { limit: Some(*limit) };
            derivator.derive(&[&input_stats], cbo)
        }
        LogicalPlan::Exchange { input, .. } => {
            derive_stats_recursive(input, cbo)
        }
        LogicalPlan::Explain { input } => derive_stats_recursive(input, cbo),
        LogicalPlan::Subquery { query, .. } => derive_stats_recursive(query, cbo),
        _ => PlanStats::default(),
    }
}
