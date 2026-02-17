use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::Int64Array;
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

fn nation_csv_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/tpch/nation.csv")
        .canonicalize()
        .expect("nation.csv exists")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multi_worker_register_and_execute_select() {
    let coord_port = pick_port();
    let coord_addr: SocketAddr = format!("127.0.0.1:{coord_port}").parse().unwrap();
    let coord_addr_str = format!("127.0.0.1:{coord_port}");

    let catalog: Arc<dyn rsdb_catalog::CatalogProvider> = Arc::new(InMemoryCatalog::new());
    let storage = Arc::new(DeltaStorageEngine::new(&std::env::temp_dir().join("rsdb-mw-test")));
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
            "rsdb-mw-worker-{i}"
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

    // Register table on coordinator; it should be synced to the workers.
    {
        let mut coord = coordinator.lock().await;
        coord.register_csv("nation", &nation_csv_path())
            .await
            .expect("register nation csv");
    }

    // Execute a SELECT; Coordinator should offload to workers and return batches.
    let batches = {
        let mut coord = coordinator.lock().await;
        coord.execute_query("SELECT COUNT(*) AS c FROM nation")
            .await
            .expect("execute query")
    };
    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is int64");
    assert_eq!(arr.value(0), 25);

    // UNION ALL should be split into multiple fragments and executed across workers.
    let union_batches = {
        let mut coord = coordinator.lock().await;
        coord.execute_query(
            "SELECT COUNT(*) AS c FROM nation UNION ALL SELECT COUNT(*) AS c FROM nation",
        )
        .await
        .expect("execute union query")
    };
    assert!(!union_batches.is_empty());
    let mut values = Vec::new();
    for b in &union_batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is int64");
        for i in 0..a.len() {
            values.push(a.value(i));
        }
    }
    values.sort();
    assert_eq!(values, vec![25, 25]);

    // Cleanup.
    coord_handle.abort();
    for h in worker_handles {
        h.abort();
    }
}
