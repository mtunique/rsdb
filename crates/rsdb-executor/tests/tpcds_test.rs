//! TPC-DS Integration Tests

#![cfg(feature = "tpcds")]
//!
//! Loads TPC-DS SF=1 CSV data from `data/tpcds/` into DataFusion and runs
//! 10 representative TPC-DS queries end-to-end.
//! Tests are skipped automatically if data files are not present.
//!
//! Generate data: python3 scripts/gen_tpcds_data.py
//! Run tests:     cargo test -p rsdb-executor --test tpcds_test -- --nocapture

use rsdb_executor::tpcds::register_tpcds_tables;
use rsdb_executor::DataFusionEngine;
use std::path::PathBuf;
use std::time::Instant;

/// Path to the TPC-DS CSV data directory (workspace root / data/tpcds).
fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/tpcds")
}

/// Helper: create engine and register all tables. Returns None if data dir missing.
async fn setup_engine() -> Option<DataFusionEngine> {
    let dir = data_dir();
    if !dir.exists() {
        eprintln!("Skipping TPC-DS test: data directory {:?} not found", dir);
        return None;
    }
    let engine = DataFusionEngine::new();
    let registered = register_tpcds_tables(&engine, &dir).await.unwrap();
    if registered.is_empty() {
        eprintln!("Skipping TPC-DS test: no CSV files found in {:?}", dir);
        return None;
    }
    eprintln!("Registered {} TPC-DS tables", registered.len());
    Some(engine)
}

/// Run a query, print timing, return row count.
async fn run_query(engine: &DataFusionEngine, name: &str, sql: &str) -> usize {
    let start = Instant::now();
    let batches = engine.execute_sql(sql).await.unwrap_or_else(|e| {
        panic!("{name} failed: {e}");
    });
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let elapsed = start.elapsed();
    eprintln!("  {name}: {row_count} rows in {elapsed:.3?}");
    row_count
}

// ---------------------------------------------------------------------------
// TPC-DS Queries (simplified from official TPC-DS spec)
// ---------------------------------------------------------------------------

/// Q3: Total extended sales price by item brand in a given year and month
#[tokio::test]
async fn test_tpcds_q03_sales_by_brand() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q3: Sales by brand/year",
        "
        SELECT d_year, i_brand_id, i_brand,
               SUM(ss_ext_sales_price) AS sum_agg
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manufact_id = 128
          AND d_moy = 11
        GROUP BY d_year, i_brand_id, i_brand
        ORDER BY d_year, sum_agg DESC, i_brand_id
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q3 should return rows");
}

/// Q7: Promotion impact on avg quantity/price/discount
#[tokio::test]
async fn test_tpcds_q07_promo_impact() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q7: Promo impact",
        "
        SELECT i_item_id,
               AVG(ss_quantity) AS agg1,
               AVG(ss_list_price) AS agg2,
               AVG(ss_coupon_amt) AS agg3,
               AVG(ss_sales_price) AS agg4
        FROM store_sales
        JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN promotion ON ss_promo_sk = p_promo_sk
        WHERE cd_gender = 'M'
          AND cd_marital_status = 'S'
          AND cd_education_status = 'College'
          AND d_year = 2000
          AND (p_channel_email = 'N' OR p_channel_event = 'N')
        GROUP BY i_item_id
        ORDER BY i_item_id
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q7 should return rows");
}

/// Q19: Store sales by brand/manufacturer for given year/month
#[tokio::test]
async fn test_tpcds_q19_sales_by_manufacturer() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q19: Sales by brand/manager",
        "
        SELECT i_brand_id, i_brand, i_manufact_id, i_manufact,
               SUM(ss_ext_sales_price) AS ext_price
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN customer ON ss_customer_sk = c_customer_sk
        JOIN customer_address ON c_current_addr_sk = ca_address_sk
        JOIN store ON ss_store_sk = s_store_sk
        WHERE i_manager_id = 8
          AND d_moy = 11
          AND d_year = 1998
          AND ca_gmt_offset = s_gmt_offset
        GROUP BY i_brand_id, i_brand, i_manufact_id, i_manufact
        ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q19 should return rows");
}

/// Q25: Catalog returns cross-channel analysis by store
#[tokio::test]
async fn test_tpcds_q25_returns_by_store() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q25: Returns by store",
        "
        SELECT i_item_id, i_item_desc, s_store_id, s_store_name,
               SUM(ss_net_profit) AS store_sales_profit,
               SUM(sr_net_loss) AS store_returns_loss,
               SUM(cs_net_profit) AS catalog_sales_profit
        FROM store_sales
        JOIN store_returns ON ss_customer_sk = sr_customer_sk
                           AND ss_item_sk = sr_item_sk
                           AND ss_ticket_number = sr_ticket_number
        JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
                           AND sr_item_sk = cs_item_sk
        JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
        JOIN date_dim d2 ON d2.d_date_sk = sr_returned_date_sk
        JOIN date_dim d3 ON d3.d_date_sk = cs_sold_date_sk
        JOIN store ON s_store_sk = ss_store_sk
        JOIN item ON i_item_sk = ss_item_sk
        WHERE d1.d_moy = 4
          AND d1.d_year = 2001
          AND d2.d_moy BETWEEN 4 AND 10
          AND d2.d_year = 2001
          AND d3.d_moy BETWEEN 4 AND 10
          AND d3.d_year = 2001
        GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
        ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q25 should return rows");
}

/// Q42: Sales by year/category/month
#[tokio::test]
async fn test_tpcds_q42_sales_by_category() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q42: Sales by category",
        "
        SELECT d_year, i_category_id, i_category,
               SUM(ss_ext_sales_price) AS total_sales
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manager_id = 1
          AND d_moy = 11
          AND d_year = 2000
        GROUP BY d_year, i_category_id, i_category
        ORDER BY total_sales DESC, d_year, i_category_id, i_category
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q42 should return rows");
}

/// Q52: Extended price by brand/year
#[tokio::test]
async fn test_tpcds_q52_sales_by_brand() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q52: Sales by brand",
        "
        SELECT d_year, i_brand_id, i_brand,
               SUM(ss_ext_sales_price) AS ext_price
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manager_id = 1
          AND d_moy = 12
          AND d_year = 1998
        GROUP BY d_year, i_brand_id, i_brand
        ORDER BY d_year, ext_price DESC, i_brand_id
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q52 should return rows");
}

/// Q55: Revenue by brand for specific month
#[tokio::test]
async fn test_tpcds_q55_revenue_by_brand() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q55: Revenue by brand/month",
        "
        SELECT i_brand_id, i_brand,
               SUM(ss_ext_sales_price) AS ext_price
        FROM date_dim
        JOIN store_sales ON d_date_sk = ss_sold_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE i_manager_id = 36
          AND d_moy = 12
          AND d_year = 2001
        GROUP BY i_brand_id, i_brand
        ORDER BY ext_price DESC, i_brand_id
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q55 should return rows");
}

/// Q68: Customer purchase analysis with demographic filters
#[tokio::test]
async fn test_tpcds_q68_customer_purchase() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q68: Customer purchase analysis",
        "
        SELECT c_last_name, c_first_name, ca_city,
               ss_ticket_number, amt, profit
        FROM (
            SELECT ss_ticket_number, ss_customer_sk,
                   ca_city AS bought_city,
                   SUM(ss_coupon_amt) AS amt,
                   SUM(ss_net_profit) AS profit
            FROM store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN store ON ss_store_sk = s_store_sk
            JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
            JOIN customer_address ON ss_addr_sk = ca_address_sk
            WHERE d_dow IN (6, 0)
              AND d_year IN (1999, 2000, 2001)
              AND s_city IN ('Fairview', 'Midway')
              AND (hd_dep_count = 4 OR hd_vehicle_count = 3)
            GROUP BY ss_ticket_number, ss_customer_sk, ca_city
        ) dn
        JOIN customer ON c_customer_sk = ss_customer_sk
        JOIN customer_address ON c_current_addr_sk = ca_address_sk
        WHERE ca_city <> bought_city
        ORDER BY c_last_name, c_first_name, ca_city, ss_ticket_number
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q68 should return rows");
}

/// Q73: Count of store sales tickets by household demographics
#[tokio::test]
async fn test_tpcds_q73_ticket_count() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q73: Ticket count analysis",
        "
        SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag,
               ss_ticket_number, cnt
        FROM (
            SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt
            FROM store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN store ON ss_store_sk = s_store_sk
            JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
            WHERE d_dom BETWEEN 1 AND 2
              AND (hd_buy_potential = '>10000' OR hd_buy_potential = 'Unknown')
              AND hd_vehicle_count > 0
              AND d_year IN (1999, 2000, 2001)
              AND s_county IN ('Williamson County', 'Franklin Parish',
                               'Bronx County', 'Orange County')
            GROUP BY ss_ticket_number, ss_customer_sk
            HAVING COUNT(*) BETWEEN 1 AND 5
        ) dn
        JOIN customer ON ss_customer_sk = c_customer_sk
        ORDER BY cnt DESC, c_last_name
        LIMIT 100
    ",
    )
    .await;
    assert!(rows > 0, "Q73 should return rows");
}

/// Q96: Store sales count filtered by time/household/store
#[tokio::test]
async fn test_tpcds_q96_sales_count() {
    let Some(engine) = setup_engine().await else {
        return;
    };
    let rows = run_query(
        &engine,
        "Q96: Sales by time/household",
        "
        SELECT COUNT(*) AS cnt
        FROM store_sales
        JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
        JOIN time_dim ON ss_sold_time_sk = t_time_sk
        JOIN store ON ss_store_sk = s_store_sk
        WHERE t_hour = 20
          AND t_minute >= 30
          AND hd_dep_count = 7
          AND s_store_name = 'ese'
    ",
    )
    .await;
    assert!(rows > 0, "Q96 should return rows");
}
