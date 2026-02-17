//! TPC-H schema helpers for loading CSV data into DataFusion

use crate::DataFusionEngine;
use datafusion::prelude::CsvReadOptions;
use rsdb_common::Result;
use std::path::Path;

/// All 8 TPC-H tables
const TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// Register all TPC-H tables from CSV files in `data_dir`.
///
/// Each table is registered with `has_header = true` and all columns inferred.
/// Returns the list of tables that were successfully registered.
pub async fn register_tpch_tables(
    engine: &DataFusionEngine,
    data_dir: &Path,
) -> Result<Vec<String>> {
    let mut registered = Vec::new();

    for &table in TABLES {
        let csv_path = data_dir.join(format!("{table}.csv"));
        if csv_path.exists() {
            let opts = CsvReadOptions::new().has_header(true);
            engine.register_csv(table, &csv_path, opts).await?;
            registered.push(table.to_string());
        }
    }

    Ok(registered)
}
