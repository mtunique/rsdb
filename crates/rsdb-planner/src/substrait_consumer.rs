//! Substrait consumer - Substrait to LogicalPlan

use rsdb_common::{Result, RsdbError};
use rsdb_sql::LogicalPlan;
use substrait::proto::Plan;

/// Convert Substrait Plan to LogicalPlan
pub struct SubstraitConsumer;

impl SubstraitConsumer {
    pub fn new() -> Self {
        Self
    }

    /// Consume a Substrait Plan and produce a LogicalPlan
    /// Currently returns a minimal stub implementation
    pub fn consume(&self, _plan: &Plan) -> Result<LogicalPlan> {
        // TODO: Implement full Substrait consumption
        // This requires mapping each Substrait relation to LogicalPlan:
        // - ReadRel -> Scan
        // - FilterRel -> Filter
        // - ProjectRel -> Project
        // - AggregateRel -> Aggregate
        // - JoinRel -> Join
        // - SortRel -> Sort
        // - LimitRel -> Limit
        Err(RsdbError::Planner(
            "Substrait consumer not fully implemented".to_string(),
        ))
    }
}

impl Default for SubstraitConsumer {
    fn default() -> Self {
        Self::new()
    }
}
