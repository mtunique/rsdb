//! Statistics Collector
//!
//! Computes table and column statistics from Arrow RecordBatches.
//! Used by ANALYZE TABLE to gather statistics for cost-based optimization.

use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use rsdb_catalog::stats::{ColumnStatistics, TableStatistics};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Collect table statistics from record batches.
///
/// Computes row count, total size, and per-column stats (NDV, null_count, min, max).
pub fn collect_statistics(batches: &[RecordBatch]) -> TableStatistics {
    if batches.is_empty() {
        return TableStatistics::default();
    }

    let schema = batches[0].schema();
    let mut row_count: u64 = 0;
    let mut total_size_bytes: u64 = 0;

    // Per-column collectors
    let num_cols = schema.fields().len();
    let mut col_collectors: Vec<ColumnCollector> = (0..num_cols)
        .map(|i| ColumnCollector::new(schema.field(i).data_type().clone()))
        .collect();

    for batch in batches {
        let num_rows = batch.num_rows() as u64;
        row_count += num_rows;

        for (col_idx, collector) in col_collectors.iter_mut().enumerate() {
            if col_idx < batch.num_columns() {
                let array = batch.column(col_idx);
                total_size_bytes += array.get_buffer_memory_size() as u64;
                collector.observe(array);
            }
        }
    }

    // Build column statistics
    let mut column_statistics = HashMap::new();
    for (i, collector) in col_collectors.into_iter().enumerate() {
        let col_name = schema.field(i).name().clone();
        column_statistics.insert(col_name, collector.finalize(row_count));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    TableStatistics {
        row_count,
        total_size_bytes,
        column_statistics,
        last_analyzed: now,
    }
}

/// Per-column statistics collector
struct ColumnCollector {
    data_type: DataType,
    null_count: u64,
    distinct_values: HashSet<String>,
    min_value: Option<String>,
    max_value: Option<String>,
    total_bytes: u64,
    value_count: u64,
}

impl ColumnCollector {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            null_count: 0,
            distinct_values: HashSet::new(),
            min_value: None,
            max_value: None,
            total_bytes: 0,
            value_count: 0,
        }
    }

    fn observe(&mut self, array: &dyn arrow_array::Array) {
        let len = array.len();
        self.null_count += array.null_count() as u64;
        self.total_bytes += array.get_buffer_memory_size() as u64;

        for i in 0..len {
            if array.is_null(i) {
                continue;
            }
            self.value_count += 1;
            if let Some(val) = self.extract_value(array, i) {
                self.update_min_max(&val);
                // Collect distinct values (capped for memory safety)
                if self.distinct_values.len() < 100_000 {
                    self.distinct_values.insert(val);
                }
            }
        }
    }

    fn extract_value(&self, array: &dyn arrow_array::Array, index: usize) -> Option<String> {
        match &self.data_type {
            DataType::Boolean => {
                let arr = array.as_boolean();
                Some(arr.value(index).to_string())
            }
            DataType::Int8 => {
                let arr = array.as_primitive::<arrow_array::types::Int8Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Int16 => {
                let arr = array.as_primitive::<arrow_array::types::Int16Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Int32 => {
                let arr = array.as_primitive::<arrow_array::types::Int32Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Int64 => {
                let arr = array.as_primitive::<arrow_array::types::Int64Type>();
                Some(arr.value(index).to_string())
            }
            DataType::UInt8 => {
                let arr = array.as_primitive::<arrow_array::types::UInt8Type>();
                Some(arr.value(index).to_string())
            }
            DataType::UInt16 => {
                let arr = array.as_primitive::<arrow_array::types::UInt16Type>();
                Some(arr.value(index).to_string())
            }
            DataType::UInt32 => {
                let arr = array.as_primitive::<arrow_array::types::UInt32Type>();
                Some(arr.value(index).to_string())
            }
            DataType::UInt64 => {
                let arr = array.as_primitive::<arrow_array::types::UInt64Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Float32 => {
                let arr = array.as_primitive::<arrow_array::types::Float32Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Float64 => {
                let arr = array.as_primitive::<arrow_array::types::Float64Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Utf8 => {
                let arr = array.as_string::<i32>();
                Some(arr.value(index).to_string())
            }
            DataType::LargeUtf8 => {
                let arr = array.as_string::<i64>();
                Some(arr.value(index).to_string())
            }
            DataType::Date32 => {
                let arr = array.as_primitive::<arrow_array::types::Date32Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Date64 => {
                let arr = array.as_primitive::<arrow_array::types::Date64Type>();
                Some(arr.value(index).to_string())
            }
            DataType::Decimal128(_, _) => {
                let arr = array.as_primitive::<arrow_array::types::Decimal128Type>();
                Some(arr.value(index).to_string())
            }
            _ => None,
        }
    }

    fn update_min_max(&mut self, val: &str) {
        match &self.min_value {
            None => self.min_value = Some(val.to_string()),
            Some(current_min) => {
                if val < current_min.as_str() {
                    self.min_value = Some(val.to_string());
                }
            }
        }
        match &self.max_value {
            None => self.max_value = Some(val.to_string()),
            Some(current_max) => {
                if val > current_max.as_str() {
                    self.max_value = Some(val.to_string());
                }
            }
        }
    }

    fn finalize(self, _total_rows: u64) -> ColumnStatistics {
        let avg_size = if self.value_count > 0 {
            self.total_bytes / self.value_count
        } else {
            0
        };

        ColumnStatistics {
            distinct_count: self.distinct_values.len() as u64,
            null_count: self.null_count,
            min_value: self.min_value,
            max_value: self.max_value,
            avg_size_bytes: avg_size,
        }
    }
}

/// Convert catalog TableStatistics to CBO PlanStats
pub fn catalog_stats_to_plan_stats(table_stats: &TableStatistics) -> crate::cbo::PlanStats {
    let mut column_stats = HashMap::new();
    for (col_name, col_stat) in &table_stats.column_statistics {
        column_stats.insert(
            col_name.clone(),
            crate::cbo::ColumnStats {
                ndv: col_stat.distinct_count,
                null_count: col_stat.null_count,
                min: col_stat.min_value.clone(),
                max: col_stat.max_value.clone(),
                size: col_stat.avg_size_bytes,
            },
        );
    }
    crate::cbo::PlanStats {
        row_count: table_stats.row_count,
        output_size: table_stats.total_size_bytes,
        column_stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn make_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                    None,
                    Some("alice"),
                ])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![6, 7, 8])),
                Arc::new(StringArray::from(vec![Some("dave"), Some("eve"), None])),
            ],
        )
        .unwrap();

        vec![batch1, batch2]
    }

    #[test]
    fn test_collect_statistics_row_count() {
        let batches = make_batches();
        let stats = collect_statistics(&batches);
        assert_eq!(stats.row_count, 8);
    }

    #[test]
    fn test_collect_statistics_column_ndv() {
        let batches = make_batches();
        let stats = collect_statistics(&batches);

        // id column: 8 distinct values (1..8)
        let id_stats = stats.column_statistics.get("id").unwrap();
        assert_eq!(id_stats.distinct_count, 8);
        assert_eq!(id_stats.null_count, 0);

        // name column: alice(x2), bob, charlie, dave, eve = 5 distinct, 2 nulls
        let name_stats = stats.column_statistics.get("name").unwrap();
        assert_eq!(name_stats.distinct_count, 5);
        assert_eq!(name_stats.null_count, 2);
    }

    #[test]
    fn test_collect_statistics_min_max() {
        let batches = make_batches();
        let stats = collect_statistics(&batches);

        let id_stats = stats.column_statistics.get("id").unwrap();
        assert_eq!(id_stats.min_value.as_deref(), Some("1"));
        assert_eq!(id_stats.max_value.as_deref(), Some("8"));

        let name_stats = stats.column_statistics.get("name").unwrap();
        assert_eq!(name_stats.min_value.as_deref(), Some("alice"));
        assert_eq!(name_stats.max_value.as_deref(), Some("eve"));
    }

    #[test]
    fn test_collect_statistics_empty() {
        let stats = collect_statistics(&[]);
        assert_eq!(stats.row_count, 0);
        assert!(stats.column_statistics.is_empty());
    }

    #[test]
    fn test_catalog_stats_to_plan_stats() {
        let batches = make_batches();
        let table_stats = collect_statistics(&batches);
        let plan_stats = catalog_stats_to_plan_stats(&table_stats);

        assert_eq!(plan_stats.row_count, 8);
        let id_cbo = plan_stats.column_stats.get("id").unwrap();
        assert_eq!(id_cbo.ndv, 8);
        assert_eq!(id_cbo.null_count, 0);
    }

    #[test]
    fn test_last_analyzed_timestamp() {
        let batches = make_batches();
        let stats = collect_statistics(&batches);
        assert!(stats.last_analyzed > 0);
    }

    #[test]
    fn test_stats_to_cbo_join_selectivity() {
        use crate::cbo::{estimate_join_selectivity, CBOContext};

        // Simulate two tables: orders (id 1..8) and customers (name: 5 distinct)
        let orders_batches = make_batches();
        let orders_stats = collect_statistics(&orders_batches);

        let customer_schema = Arc::new(Schema::new(vec![Field::new(
            "customer_id",
            DataType::Int64,
            false,
        )]));
        let customer_batch = RecordBatch::try_new(
            customer_schema,
            vec![Arc::new(Int64Array::from(vec![
                1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
            ]))],
        )
        .unwrap();
        let customer_stats = collect_statistics(&[customer_batch]);

        // Register in CBO context
        let mut cbo = CBOContext::new();
        let orders_plan_stats = catalog_stats_to_plan_stats(&orders_stats);
        let customer_plan_stats = catalog_stats_to_plan_stats(&customer_stats);

        cbo.register_table("orders".to_string(), orders_plan_stats);
        cbo.register_table("customers".to_string(), customer_plan_stats);

        // Join orders.id = customers.customer_id
        // NDV(orders.id) = 8, NDV(customers.customer_id) = 5
        // selectivity = 1 / max(8, 5) = 1/8 = 0.125
        let sel = estimate_join_selectivity("id", "customer_id", &cbo);
        assert!((sel - 0.125).abs() < 1e-9);
    }

    #[test]
    fn test_stats_integration_with_full_optimizer() {
        use crate::FullOptimizer;
        use rsdb_sql::expr::{BinaryOperator, Expr};
        use rsdb_sql::logical_plan::{JoinCondition, JoinType, LogicalPlan};

        // Create stats from actual data
        let orders_batches = make_batches();
        let orders_stats = collect_statistics(&orders_batches);
        let orders_plan_stats = catalog_stats_to_plan_stats(&orders_stats);

        // Set up optimizer with stats
        let mut optimizer = FullOptimizer::new();
        optimizer.register_table("orders".to_string(), orders_plan_stats);
        optimizer.register_table(
            "customers".to_string(),
            crate::cbo::PlanStats {
                row_count: 100,
                output_size: 1000,
                column_stats: HashMap::new(),
            },
        );

        // Build a simple join plan
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("cid", DataType::Int64, false),
        ]));
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                projection: None,
                filters: vec![],
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "customers".to_string(),
                schema: Arc::new(Schema::new(vec![Field::new("cid", DataType::Int64, false)])),
                projection: None,
                filters: vec![],
            }),
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Column("cid".to_string())),
            }),
            schema,
        };

        // Optimize should succeed (with stats-informed Cascades)
        let result = optimizer.optimize(plan);
        assert!(result.is_ok());
    }
}
