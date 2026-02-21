use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use rsdb_catalog::InMemoryCatalog;
use rsdb_common::{NodeAddr, NodeId};
use rsdb_coordinator::{Coordinator, RsdbFlightServer};
use rsdb_executor::DataFusionEngine;
use rsdb_storage::DeltaStorageEngine;
use rsdb_worker::Worker;

fn pick_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

fn tpch_csv(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../data/tpch/{name}.csv"))
        .canonicalize()
        .expect("tpch csv exists")
}

fn split_csv_into_two(src: &Path, out_dir: &Path) {
    let content = std::fs::read_to_string(src).expect("read csv");
    let mut lines = content.lines();
    let header = lines.next().expect("header");

    let mut part0 = String::new();
    let mut part1 = String::new();
    part0.push_str(header);
    part0.push('\n');
    part1.push_str(header);
    part1.push('\n');

    for (i, line) in lines.enumerate() {
        if i % 2 == 0 {
            part0.push_str(line);
            part0.push('\n');
        } else {
            part1.push_str(line);
            part1.push('\n');
        }
    }

    std::fs::write(out_dir.join("part0.csv"), part0).expect("write part0");
    std::fs::write(out_dir.join("part1.csv"), part1).expect("write part1");
}

fn first_i64(batches: &[RecordBatch]) -> i64 {
    let mut total = 0;
    tracing::info!("first_i64 received {} batches", batches.len());
    for (i, b) in batches.iter().enumerate() {
        tracing::info!("Batch {}: {} rows, {} columns", i, b.num_rows(), b.num_columns());
        if b.num_rows() == 0 {
            continue;
        }
        if b.num_columns() > 0 {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64");
            for j in 0..a.len() {
                total += a.value(j);
            }
        } else {
            total += b.num_rows() as i64;
        }
    }
    tracing::info!("first_i64 returning total={}", total);
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_hash_shuffle_join_and_groupby() {
    let _ = tracing_subscriber::fmt::try_init();
    let coord_port = pick_port();
    let coord_addr: SocketAddr = format!("127.0.0.1:{coord_port}").parse().unwrap();
    let coord_addr_str = format!("127.0.0.1:{coord_port}");

    let catalog: Arc<dyn rsdb_catalog::CatalogProvider> = Arc::new(InMemoryCatalog::new());
    let storage = Arc::new(DeltaStorageEngine::new(&std::env::temp_dir().join("rsdb-mpp-test")));
    let executor: Arc<dyn rsdb_executor::ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let coordinator = Coordinator::new(catalog, storage, executor);
    let coordinator = Arc::new(tokio::sync::Mutex::new(coordinator));
    let flight = RsdbFlightServer::new(coordinator.clone());

    let coord_handle = tokio::spawn(async move {
        flight.serve(coord_addr).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Start two workers.
    let mut worker_handles = Vec::new();
    for (i, port) in [pick_port(), pick_port()].into_iter().enumerate() {
        let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let self_addr = NodeAddr::new("127.0.0.1", port);

        let catalog: Arc<dyn rsdb_catalog::CatalogProvider> = Arc::new(InMemoryCatalog::new());
        let storage = Arc::new(DeltaStorageEngine::new(&std::env::temp_dir().join(format!(
            "rsdb-mpp-worker-{i}"
        ))));
        let executor: Arc<dyn rsdb_executor::ExecutionEngine> = Arc::new(DataFusionEngine::new());
        let worker = Arc::new(Worker::new(NodeId((i as u32) + 1), catalog, storage, executor));

        let w_for_rpc = worker.clone();
        let handle = tokio::spawn(async move {
            rsdb_worker::serve_worker_task(w_for_rpc, listen).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        worker
            .register(&coord_addr_str, &self_addr)
            .await
            .expect("register worker");

        worker_handles.push(handle);
    }

    // Prepare partitioned directories for nation/region.
    let tmp = tempfile::tempdir().unwrap();
    let nation_dir = tmp.path().join("nation");
    let region_dir = tmp.path().join("region");
    std::fs::create_dir_all(&nation_dir).unwrap();
    std::fs::create_dir_all(&region_dir).unwrap();
    split_csv_into_two(&tpch_csv("nation"), &nation_dir);
    split_csv_into_two(&tpch_csv("region"), &region_dir);

    // Register sharded tables.
    {
        let mut coord = coordinator.lock().await;
        coord.register_csv("nation", &nation_dir).await.unwrap();
        coord.register_csv("region", &region_dir).await.unwrap();
    }

    // Join should run as hash-shuffle join (plan fragments + repartition + memtable overwrite).
    let join_batches = {
        let mut coord = coordinator.lock().await;
        coord.execute_query(
            "SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey",
        )
        .await
        .unwrap()
    };
    assert_eq!(first_i64(&join_batches), 25);

    // Group by should run as hash-shuffle aggregation (keys hashed -> partitions -> local aggregate).
    let gb_batches = {
        let mut coord = coordinator.lock().await;
        coord.execute_query("SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey")
            .await
            .unwrap()
    };
    // sum(counts) == 25
    let mut total = 0i64;
    for b in &gb_batches {
        let a = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is int64");
        for i in 0..a.len() {
            total += a.value(i);
        }
    }
    assert_eq!(total, 25);

    coord_handle.abort();
    for h in worker_handles {
        h.abort();
    }
}
