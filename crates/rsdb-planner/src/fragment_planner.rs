//! Fragment planner - splits a LogicalPlan into independent fragments at Exchange boundaries.

use rsdb_common::Result;
use rsdb_sql::logical_plan::LogicalPlan;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_FRAGMENT_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
pub struct PlanFragment {
    pub id: usize,
    pub plan: LogicalPlan,
    pub child_ids: Vec<usize>,
}

pub struct FragmentPlanner;

impl FragmentPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Split a logical plan into a DAG of fragments.
    /// Returns fragments in bottom-up order (post-order traversal).
    pub fn plan_fragments(&self, plan: &LogicalPlan) -> Result<Vec<PlanFragment>> {
        let mut fragments = Vec::new();
        self.split_recursive(plan, &mut fragments)?;
        Ok(fragments)
    }

    fn split_recursive(
        &self,
        plan: &LogicalPlan,
        fragments: &mut Vec<PlanFragment>,
    ) -> Result<usize> {
        let current_id = NEXT_FRAGMENT_ID.fetch_add(1, Ordering::SeqCst);

        match plan {
            LogicalPlan::Exchange { input, .. } | LogicalPlan::RemoteExchange { input, .. } => {
                // An Exchange marks a boundary. Split the child first.
                let child_id = self.split_recursive(input, fragments)?;
                
                // This fragment IS the exchange receiver point.
                fragments.push(PlanFragment {
                    id: current_id,
                    plan: plan.clone(), // Keep the Exchange node for now
                    child_ids: vec![child_id],
                });
            }
            _ => {
                // For non-exchange nodes, recurse into all children.
                let mut child_ids = Vec::new();
                
                // This is slightly tricky as we need to traverse the LogicalPlan variants
                // We'll simulate traversal by matching common patterns
                match plan {
                    LogicalPlan::Join { left, right, .. } | LogicalPlan::CrossJoin { left, right, .. } => {
                        child_ids.push(self.split_recursive(left, fragments)?);
                        child_ids.push(self.split_recursive(right, fragments)?);
                    }
                    LogicalPlan::Filter { input, .. } 
                    | LogicalPlan::Project { input, .. }
                    | LogicalPlan::Aggregate { input, .. }
                    | LogicalPlan::Sort { input, .. }
                    | LogicalPlan::Limit { input, .. }
                    | LogicalPlan::Subquery { query: input, .. }
                    | LogicalPlan::SubqueryAlias { input, .. }
                    | LogicalPlan::Explain { input, .. }
                    | LogicalPlan::Exchange { input, .. }
                    | LogicalPlan::Insert { source: input, .. } => {
                        child_ids.push(self.split_recursive(input, fragments)?);
                    }
                    LogicalPlan::Union { inputs, .. } => {
                        for input in inputs {
                            child_ids.push(self.split_recursive(input, fragments)?);
                        }
                    }
                    LogicalPlan::Scan { .. } | LogicalPlan::EmptyRelation | LogicalPlan::CreateTable { .. } 
                    | LogicalPlan::DropTable { .. } | LogicalPlan::Analyze { .. } => {
                        // Leaf nodes, no children to split
                    }
                    LogicalPlan::RemoteExchange { .. } => unreachable!("handled above"),
                }

                // Create fragment for current node (or sub-tree)
                fragments.push(PlanFragment {
                    id: current_id,
                    plan: plan.clone(),
                    child_ids,
                });
            }
        }

        Ok(current_id)
    }
}

impl Default for FragmentPlanner {
    fn default() -> Self {
        Self::new()
    }
}
