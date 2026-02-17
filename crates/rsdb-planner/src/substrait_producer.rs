//! Substrait producer - LogicalPlan to Substrait

use rsdb_common::{Result, RsdbError};
use rsdb_sql::LogicalPlan;
use substrait::proto::Plan;

/// Convert LogicalPlan to Substrait Plan
pub struct SubstraitProducer;

impl SubstraitProducer {
    pub fn new() -> Self {
        Self
    }

    /// Produce a Substrait Plan from LogicalPlan
    /// Currently returns a minimal stub implementation
    pub fn produce(&self, _plan: &LogicalPlan) -> Result<Plan> {
        // TODO: Implement full Substrait production
        // This requires mapping each LogicalPlan variant to its Substrait equivalent:
        // - Scan -> ReadRel
        // - Filter -> FilterRel
        // - Project -> ProjectRel
        // - Aggregate -> AggregateRel
        // - Join -> JoinRel
        // - Sort -> SortRel
        // - Limit -> LimitRel
        Err(RsdbError::Planner(
            "Substrait producer not fully implemented".to_string(),
        ))
    }
}

impl Default for SubstraitProducer {
    fn default() -> Self {
        Self::new()
    }
}
