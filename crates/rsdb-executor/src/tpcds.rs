//! TPC-DS schema helpers for loading CSV data into DataFusion

use crate::DataFusionEngine;
use datafusion::prelude::CsvReadOptions;
use rsdb_common::Result;
use std::path::Path;

/// All 24 TPC-DS tables
const TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// Register all TPC-DS tables from CSV files in `data_dir`.
///
/// Each table is registered with `has_header = true` and all columns inferred.
/// Returns the list of tables that were successfully registered.
pub async fn register_tpcds_tables(
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
