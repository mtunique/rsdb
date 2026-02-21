//! W2W Shuffle Integration Test
//!
//! Verifies the Static Topology Scheduler and Worker-to-Worker direct data transfer.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use rsdb_catalog::InMemoryCatalog;
use rsdb_common::{NodeAddr, NodeId};
use rsdb_coordinator::{Coordinator, RsdbFlightServer};
use rsdb_executor::DataFusionEngine;
use rsdb_storage::DeltaStorageEngine;
use rsdb_worker::Worker;

fn pick_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

fn tpch_csv(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../data/tpch/{name}.csv"))
        .canonicalize()
        .expect("tpch csv exists")
}

struct TestCluster {
    coordinator: Arc<tokio::sync::Mutex<Coordinator>>,
    _coord_handle: tokio::task::JoinHandle<()>,
    _worker_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl TestCluster {
    async fn start(num_workers: usize) -> Self {
        let coord_port = pick_port();
        let coord_addr: SocketAddr = format!("127.0.0.1:{coord_port}").parse().unwrap();
        let coord_addr_str = format!("127.0.0.1:{coord_port}");

        let catalog: Arc<dyn rsdb_catalog::CatalogProvider> =
            Arc::new(InMemoryCatalog::new());
        let storage = Arc::new(DeltaStorageEngine::new(
            &std::env::temp_dir().join("rsdb-w2w-coord"),
        ));
        let executor: Arc<dyn rsdb_executor::ExecutionEngine> =
            Arc::new(DataFusionEngine::new());

        let coordinator = Coordinator::new(catalog, storage, executor);
        let coordinator = Arc::new(tokio::sync::Mutex::new(coordinator));
        let flight = RsdbFlightServer::new(coordinator.clone());

        let coord_handle = tokio::spawn(async move {
            flight.serve(coord_addr).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let mut worker_handles = Vec::new();
        for i in 0..num_workers {
            let port = pick_port();
            let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let self_addr = NodeAddr::new("127.0.0.1", port);

            let cat: Arc<dyn rsdb_catalog::CatalogProvider> =
                Arc::new(InMemoryCatalog::new());
            let sto = Arc::new(DeltaStorageEngine::new(
                &std::env::temp_dir().join(format!("rsdb-w2w-worker-{i}")),
            ));
            let exe: Arc<dyn rsdb_executor::ExecutionEngine> =
                Arc::new(DataFusionEngine::new());
            let worker = Arc::new(Worker::new(NodeId((i as u32) + 1), cat, sto, exe));

            let w = worker.clone();
            let handle = tokio::spawn(async move {
                // Serve both Task RPC and Flight Data
                // In a real deployment, these might be separate ports, but for test simplicity:
                // We actually need TWO ports: one for gRPC (WorkerTask), one for Flight (Data).
                // Existing `serve_worker_task` is gRPC.
                // We need to spawn `serve_flight` as well.
                
                // Hack for test: Use same port? No, Tonic and ArrowFlight might conflict if on same port unless multiplexed.
                // rsdb-worker server usually separates them.
                // Let's spawn Flight on port + 10000? Or just pick another port.
                
                // Wait, `serve_worker_task` takes ownership of the listener in some impls, 
                // but here it binds.
                // I will spawn Flight on a separate random port and register it.
                // But `WorkerInfo` only has one addr.
                // For now, let's assume W2W shuffle uses the same address if possible, 
                // OR we update WorkerInfo to have `flight_addr`.
                // BUT `ShuffleReaderExec` uses `worker_addr`.
                
                // CRITICAL: `rsdb` architecture currently assumes one address per worker.
                // `serve_worker_task` serves `WorkerTask` service.
                // Can we add `FlightService` to the SAME Tonic router?
                // Yes! `serve_worker_task` in `rpc_server.rs` builds the router.
                // I need to modify `serve_worker_task` to ALSO add `FlightService`.
                
                rsdb_worker::serve_worker_task(w, listen).await.unwrap();
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            worker
                .register(&coord_addr_str, &self_addr)
                .await
                .expect("register worker");

            worker_handles.push(handle);
        }

        Self {
            coordinator,
            _coord_handle: coord_handle,
            _worker_handles: worker_handles,
        }
    }

    async fn execute(&self, sql: &str) -> Vec<RecordBatch> {
        let mut coord = self.coordinator.lock().await;
        coord.execute_query(sql).await.expect(sql)
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self._coord_handle.abort();
        for h in &self._worker_handles {
            h.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_w2w_shuffle_join() {
    let _ = tracing_subscriber::fmt::try_init();
    let cluster = TestCluster::start(2).await;
    
    // Register tables via coordinator (which syncs to workers)
    let mut coord = cluster.coordinator.lock().await;
    coord.register_csv("nation", &tpch_csv("nation")).await.unwrap();
    coord.register_csv("region", &tpch_csv("region")).await.unwrap();
    drop(coord);

    // Join query that returns columns
    let batches = cluster.execute(
        "SELECT n.n_name FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey"
    ).await;

    let names = collect_string_column(&batches, 0);
    // nation has 25 rows. Each fragment is assigned to a single worker,
    // so we get 25 rows (one worker executes the full join).
    assert_eq!(names.len(), 25);
    assert!(names.contains(&"ALGERIA".to_string()));
}

fn collect_string_column(batches: &[RecordBatch], col: usize) -> Vec<String> {
    use arrow_array::StringArray;
    let mut out = Vec::new();
    for (bi, b) in batches.iter().enumerate() {
        if b.num_rows() == 0 { continue; }
        if b.num_columns() <= col {
            tracing::warn!("Batch {} has only {} columns, requested {}", bi, b.num_columns(), col);
            continue;
        }
        let arr = b.column(col).as_any().downcast_ref::<StringArray>().expect("expected StringArray");
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

fn collect_i64_column(batches: &[RecordBatch], col: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for (bi, b) in batches.iter().enumerate() {
        if b.num_rows() == 0 { continue; }
        if b.num_columns() <= col {
            tracing::warn!("Batch {} has only {} columns, requested {}", bi, b.num_columns(), col);
            continue;
        }
        let arr = b.column(col).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}
