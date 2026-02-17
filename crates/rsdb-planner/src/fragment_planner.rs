//! Fragment planner - split plan for distributed execution

use rsdb_common::{PlanFragment, Result};
use rsdb_sql::LogicalPlan;

/// Fragment planner for distributed query execution
pub struct FragmentPlanner;

impl FragmentPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Plan fragments for a query
    /// For single-node mode, returns a single fragment containing the entire plan
    /// Note: Currently stores empty plan_bytes - full implementation would serialize LogicalPlan to Substrait
    pub fn plan_fragments(
        &self,
        _plan: &LogicalPlan,
        _num_workers: usize,
    ) -> Result<Vec<PlanFragment>> {
        // For single-node mode, return one fragment
        // In distributed mode, this would detect exchange points and split accordingly
        // TODO: Serialize LogicalPlan to Substrait bytes using SubstraitProducer
        let fragment = PlanFragment::new(
            0.into(),
            vec![], // Empty plan_bytes - to be implemented with Substrait serialization
        );
        Ok(vec![fragment])
    }
}

impl Default for FragmentPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use rsdb_sql::LogicalPlan;
    use std::sync::Arc;

    #[test]
    fn test_single_fragment() {
        let planner = FragmentPlanner::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let plan = LogicalPlan::Scan {
            table_name: "test".to_string(),
            schema,
            projection: None,
            filters: vec![],
        };

        let fragments = planner.plan_fragments(&plan, 1).unwrap();
        assert_eq!(fragments.len(), 1);
    }
}
