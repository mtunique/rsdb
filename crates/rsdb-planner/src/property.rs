//! Physical Properties and Statistics
//!
//! Represents the physical requirements (distribution, sorting) and
//! statistical properties (row count, distinct values) of data streams.

use rsdb_sql::logical_plan::Partitioning;
use crate::cbo::{PlanStats, ColumnStats};

/// Required physical property for optimization
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Property {
    pub partitioning: Partitioning,
    pub sorting: Vec<String>,
}

impl Default for Property {
    fn default() -> Self {
        Self {
            partitioning: Partitioning::Any,
            sorting: vec![],
        }
    }
}

impl Property {
    /// Check if this property satisfies the required property
    pub fn satisfies(&self, required: &Property) -> bool {
        // 1. Check partitioning
        let partitioning_satisfied = match &required.partitioning {
            Partitioning::Any => true,
            req_p => &self.partitioning == req_p,
        };

        if !partitioning_satisfied {
            return false;
        }

        // 2. Check sorting
        if required.sorting.is_empty() {
            true
        } else {
            // Strict prefix match for now
            self.sorting.starts_with(&required.sorting)
        }
    }
}

pub type Statistics = PlanStats;
pub type ColumnStat = ColumnStats;
