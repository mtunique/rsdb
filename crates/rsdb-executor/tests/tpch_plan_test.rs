use datafusion::prelude::CsvReadOptions;
use rsdb_executor::DataFusionEngine;
use rsdb_executor::tpch;
use std::path::PathBuf;
use std::sync::Arc;

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
    
    // The result is a single string column containing the plan
    let batch = &results[0];
    let column = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("Explain should return StringArray");
    let plan = column.value(0);
    
    println!("\n=== OPTIMIZED Q03 PLAN WITH STATS ===\n{}\n=====================================\n", plan);
    
    println!("Validated Q03 Plan Structure");
    
    // 1. Core Node Existence
    assert!(plan.contains("Aggregate"), "Plan should contain Aggregate node");
    assert!(plan.contains("Filter"), "Plan should contain Filter node");
    assert!(plan.contains("customer"), "Plan should reference customer table");
    assert!(plan.contains("orders"), "Plan should reference orders table");
    assert!(plan.contains("lineitem"), "Plan should reference lineitem table");
    
    // 2. Schema Resolution Checks (from our planner fixes)
    assert!(plan.contains("group_0"), "Plan should use group_i aliases for robust schema resolution");
    assert!(plan.contains("agg_0"), "Plan should use agg_i aliases for robust schema resolution");
    assert!(plan.contains("revenue"), "Plan should correctly propagate output aliases");
    
    // 3. Join Structure
    // With Predicate Pushdown and CrossJoin conversion, we should see Inner Joins instead of CrossJoins
    assert!(plan.contains("Join"), "Plan should contain Join nodes");
    // CrossJoin should be gone or minimized (only for true cross products)
    assert!(!plan.contains("CrossJoin"), "CrossJoins should be converted to Inner Joins with predicates");

    // 4. Distribution (New)
    // We added InsertExchange rule, so we should see explicit Exchange nodes
    assert!(plan.contains("Exchange"), "Plan should contain Exchange nodes for distributed execution");
}
