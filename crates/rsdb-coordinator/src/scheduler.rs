//! Query scheduler

use rsdb_common::{NodeId, PlanFragment};

/// Scheduler for assigning fragments to workers
pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Self {
        Self
    }

    /// Schedule fragments to workers
    /// Returns the assigned node for each fragment
    pub fn schedule(
        &self,
        fragments: &[PlanFragment],
        available_workers: &[NodeId],
    ) -> Result<Vec<Option<NodeId>>, rsdb_common::RsdbError> {
        if available_workers.is_empty() {
            return Ok(vec![None; fragments.len()]);
        }

        let mut out = Vec::with_capacity(fragments.len());
        for f in fragments {
            // Deterministic assignment: fragment_id -> worker index.
            let idx = (f.fragment_id.0 as usize) % available_workers.len();
            out.push(Some(available_workers[idx]));
        }
        Ok(out)
    }

    /// Select the best worker for a fragment
    pub fn select_worker(&self, _fragment: &PlanFragment, _workers: &[NodeId]) -> Option<NodeId> {
        // Keep API for future scheduling strategies.
        None
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
