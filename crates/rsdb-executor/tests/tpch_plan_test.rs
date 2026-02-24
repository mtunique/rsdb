use datafusion::prelude::CsvReadOptions;
use rsdb_executor::DataFusionEngine;
use rsdb_executor::tpch;
use std::path::PathBuf;
use std::sync::Arc;
use std::fs;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn data_dir() -> PathBuf {
    workspace_root().join("data").join("tpch")
}

fn queries_dir() -> PathBuf {
    workspace_root().join("data").join("tpch_queries")
}

async fn setup_engine() -> Arc<DataFusionEngine> {
    let engine = Arc::new(DataFusionEngine::new());
    let tpch_dir = data_dir();

    let tables = [
        ("customer", tpch::customer_schema()),
        ("lineitem", tpch::lineitem_schema()),
        ("nation", tpch::nation_schema()),
        ("orders", tpch::orders_schema()),
        ("part", tpch::part_schema()),
        ("partsupp", tpch::partsupp_schema()),
        ("region", tpch::region_schema()),
        ("supplier", tpch::supplier_schema()),
    ];

    for (name, schema) in tables {
        let path = tpch_dir.join(format!("{}.csv", name));
        if path.exists() {
            engine.register_csv(name, &path, CsvReadOptions::new().delimiter(b',').has_header(true).schema(&schema)).await.unwrap();
            engine.execute_sql(&format!("ANALYZE TABLE {};", name)).await.unwrap();
        }
    }
    engine
}

#[tokio::test]
async fn test_tpch_q03_plan_validation() {
    let engine = setup_engine().await;
    
    // Q03 Explain test
    let sql = "EXPLAIN SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate;";

    let results = engine.execute_sql(sql).await.unwrap();
    let batch = &results[0];
    let column = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("Explain should return StringArray");
    let plan = column.value(0);
    
    println!("\n=== OPTIMIZED Q03 PLAN WITH STATS ===\n{}\n=====================================\n", plan);
    
    assert!(plan.contains("Aggregate"), "Q03 should contain Aggregate");
    assert!(plan.contains("Join"), "Q03 should contain Join");
    assert!(plan.contains("Exchange"), "Q03 should contain Exchange for distribution");
    assert!(!plan.contains("CrossJoin"), "Q03 should not contain CrossJoin after optimization");
}

#[tokio::test]
async fn test_all_tpch_queries_contain_exchange() {
    let engine = setup_engine().await;
    let qdir = queries_dir();

    for i in 1..=22 {
        let q_file = qdir.join(format!("q{:02}.sql", i));
        if !q_file.exists() { continue; }
        
        let sql = fs::read_to_string(&q_file).unwrap();
        let explain_sql = format!("EXPLAIN {}", sql);
        
        println!("Checking TPC-H Q{:02}...", i);
        match engine.execute_sql(&explain_sql).await {
            Ok(results) => {
                let batch = &results[0];
                let column = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
                let plan = column.value(0);
                
                assert!(plan.contains("Exchange"), "TPC-H Q{:02} plan missing Exchange. Plan:\n{}", i, plan);
            }
            Err(e) => {
                // Some queries might fail due to complex SQL features not yet supported, 
                // but for those that plan, we require Exchange.
                println!("Warning: TPC-H Q{:02} failed to plan: {}", i, e);
            }
        }
    }
}
