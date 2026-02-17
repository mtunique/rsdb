//! Table and column statistics for cost-based optimization

use std::collections::HashMap;

/// Statistics for a table, computed by ANALYZE
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    /// Total number of rows in the table
    pub row_count: u64,
    /// Total size in bytes (estimated)
    pub total_size_bytes: u64,
    /// Per-column statistics
    pub column_statistics: HashMap<String, ColumnStatistics>,
    /// Unix timestamp of when stats were last computed (seconds since epoch)
    pub last_analyzed: i64,
}

/// Statistics for a single column
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Number of distinct values (NDV)
    pub distinct_count: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value (string representation for universal storage)
    pub min_value: Option<String>,
    /// Maximum value (string representation for universal storage)
    pub max_value: Option<String>,
    /// Average size in bytes per value
    pub avg_size_bytes: u64,
}
