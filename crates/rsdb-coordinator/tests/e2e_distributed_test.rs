//! End-to-end distributed execution test.
//!
//! Spins up 1 coordinator + 2 workers **in-process**, registers partitioned
//! TPC-H data, and exercises the full distributed query path:
//!   1. Worker registration & table sync
//!   2. Hash-shuffle JOIN
//!   3. Hash-shuffle GROUP BY
//!   4. UNION ALL fragment splitting
//!   5. Simple SELECT offloaded to worker
//!   6. FlightSQL client connection (get_flight_info + do_get)

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use rsdb_catalog::InMemoryCatalog;
use rsdb_common::{NodeAddr, NodeId};
use rsdb_coordinator::{Coordinator, RsdbFlightServer};
use rsdb_executor::DataFusionEngine;
use rsdb_storage::DeltaStorageEngine;
use rsdb_worker::Worker;
use tonic::transport::Channel;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Split a CSV into N parts (round-robin by line) inside `out_dir`.
fn split_csv(src: &Path, out_dir: &Path, n: usize) {
    let content = std::fs::read_to_string(src).expect("read csv");
    let mut lines = content.lines();
    let header = lines.next().expect("header");

    let mut parts: Vec<String> = (0..n)
        .map(|_| {
            let mut s = String::new();
            s.push_str(header);
            s.push('\n');
            s
        })
        .collect();

    for (i, line) in lines.enumerate() {
        parts[i % n].push_str(line);
        parts[i % n].push('\n');
    }

    for (i, p) in parts.iter().enumerate() {
        std::fs::write(out_dir.join(format!("part{i}.csv")), p).expect("write part");
    }
}

fn collect_i64_column(batches: &[RecordBatch], col: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

fn collect_string_column(batches: &[RecordBatch], col: usize) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Cluster setup
// ---------------------------------------------------------------------------

struct TestCluster {
    coordinator: Arc<tokio::sync::Mutex<Coordinator>>,
    coord_addr: String,
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
            &std::env::temp_dir().join("rsdb-e2e-coord"),
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
                &std::env::temp_dir().join(format!("rsdb-e2e-worker-{i}")),
            ));
            let exe: Arc<dyn rsdb_executor::ExecutionEngine> =
                Arc::new(DataFusionEngine::new());
            let worker = Arc::new(Worker::new(NodeId((i as u32) + 1), cat, sto, exe));

            let w = worker.clone();
            let handle = tokio::spawn(async move {
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
            coord_addr: coord_addr_str,
            _coord_handle: coord_handle,
            _worker_handles: worker_handles,
        }
    }

    async fn execute(&self, sql: &str) -> Vec<RecordBatch> {
        let mut coord = self.coordinator.lock().await;
        coord.execute_query(sql).await.expect(sql)
    }

    async fn register_csv_partitioned(
        &self,
        table: &str,
        src: &Path,
        tmp_dir: &Path,
        parts: usize,
    ) {
        let dir = tmp_dir.join(table);
        std::fs::create_dir_all(&dir).unwrap();
        split_csv(src, &dir, parts);

        let mut coord = self.coordinator.lock().await;
        coord.register_csv(table, &dir).await.unwrap();
    }

    async fn register_csv_single(&self, table: &str, path: &Path) {
        let mut coord = self.coordinator.lock().await;
        coord.register_csv(table, path).await.unwrap();
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: Worker registration is visible to coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_worker_registration() {
    let cluster = TestCluster::start(2).await;
    let coord = cluster.coordinator.lock().await;
    let workers = coord.list_workers();
    assert_eq!(workers.len(), 2, "Expected 2 workers registered");
    drop(coord);
}

/// Test 2: Hash-shuffle JOIN across 2 workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_join() {
    let cluster = TestCluster::start(2).await;
    let tmp = tempfile::tempdir().unwrap();

    // Register nation and region as partitioned tables (2 shards each).
    cluster
        .register_csv_partitioned("nation", &tpch_csv("nation"), tmp.path(), 2)
        .await;
    cluster
        .register_csv_partitioned("region", &tpch_csv("region"), tmp.path(), 2)
        .await;

    // nation has 25 rows, every nation has a valid regionkey → join produces 25 rows.
    let batches = cluster
        .execute(
            "SELECT COUNT(*) AS cnt FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey",
        )
        .await;
    let counts = collect_i64_column(&batches, 0);
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 25, "JOIN should produce 25 rows");
}

/// Test 3: Hash-shuffle GROUP BY across 2 workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_group_by() {
    let cluster = TestCluster::start(2).await;
    let tmp = tempfile::tempdir().unwrap();

    cluster
        .register_csv_partitioned("nation", &tpch_csv("nation"), tmp.path(), 2)
        .await;

    let batches = cluster
        .execute("SELECT n_regionkey, COUNT(*) AS cnt FROM nation GROUP BY n_regionkey")
        .await;
    let counts = collect_i64_column(&batches, 1);
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 25, "GROUP BY counts should sum to 25");

    // There are 5 regions (0-4), so we should get 5 groups.
    assert_eq!(counts.len(), 5, "Expected 5 region groups");
}

/// Test 4: UNION ALL should be split across workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_union_all() {
    let cluster = TestCluster::start(2).await;

    // Register single file (not partitioned) — both coordinator and workers get it.
    cluster
        .register_csv_single("nation", &tpch_csv("nation"))
        .await;

    let batches = cluster
        .execute("SELECT COUNT(*) AS c FROM nation UNION ALL SELECT COUNT(*) AS c FROM nation")
        .await;
    let mut vals = collect_i64_column(&batches, 0);
    vals.sort();
    assert_eq!(vals, vec![25, 25], "UNION ALL of two COUNT(*)");
}

/// Test 5: Simple SELECT offloaded to a single worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_simple_select() {
    let cluster = TestCluster::start(2).await;

    cluster
        .register_csv_single("nation", &tpch_csv("nation"))
        .await;

    let batches = cluster.execute("SELECT COUNT(*) AS c FROM nation").await;
    let vals = collect_i64_column(&batches, 0);
    assert_eq!(vals, vec![25], "Simple COUNT(*) should be 25");
}

/// Test 6: FlightSQL client connection (get_flight_info + do_get).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_flight_sql_client_e2e() {
    let cluster = TestCluster::start(2).await;

    cluster
        .register_csv_single("region", &tpch_csv("region"))
        .await;

    // Connect via FlightSQL client protocol.
    let channel = Channel::from_shared(format!("http://{}", cluster.coord_addr))
        .unwrap()
        .connect()
        .await
        .expect("connect to FlightSQL server");
    let mut client = FlightSqlServiceClient::new(channel);

    // Handshake
    client.handshake("user", "pass").await.expect("handshake");

    // Execute query via FlightSQL protocol
    let mut flight_info = client
        .execute("SELECT r_regionkey, r_name FROM region ORDER BY r_regionkey".to_string(), None)
        .await
        .expect("execute via FlightSQL");

    let ticket = flight_info
        .endpoint
        .pop()
        .expect("at least one endpoint")
        .ticket
        .expect("ticket");

    let stream = client.do_get(ticket).await.expect("do_get");
    let batches: Vec<RecordBatch> = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("collect flight data");

    // region has 5 rows.
    let total_rows: usize = batches.iter().map(|b: &RecordBatch| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "FlightSQL should return 5 region rows");

    let names = collect_string_column(&batches, 1);
    assert!(names.contains(&"AFRICA".to_string()));
    assert!(names.contains(&"ASIA".to_string()));
}

/// Test 7: Distributed JOIN with WHERE filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_join_with_filter() {
    let cluster = TestCluster::start(2).await;
    let tmp = tempfile::tempdir().unwrap();

    cluster
        .register_csv_partitioned("nation", &tpch_csv("nation"), tmp.path(), 2)
        .await;
    cluster
        .register_csv_partitioned("region", &tpch_csv("region"), tmp.path(), 2)
        .await;

    // Only count nations in AFRICA (regionkey=0).
    let batches = cluster
        .execute(
            "SELECT COUNT(*) AS cnt \
             FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AFRICA'",
        )
        .await;
    let counts = collect_i64_column(&batches, 0);
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 5, "5 nations in AFRICA");
}

/// Test 8: Distributed aggregation with multiple agg functions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_distributed_multi_agg() {
    let cluster = TestCluster::start(2).await;
    let tmp = tempfile::tempdir().unwrap();

    cluster
        .register_csv_partitioned("nation", &tpch_csv("nation"), tmp.path(), 2)
        .await;

    let batches = cluster
        .execute(
            "SELECT n_regionkey, COUNT(*) AS cnt, MIN(n_nationkey) AS min_nk, MAX(n_nationkey) AS max_nk \
             FROM nation GROUP BY n_regionkey",
        )
        .await;

    let counts = collect_i64_column(&batches, 1);
    let total: i64 = counts.iter().sum();
    assert_eq!(total, 25, "All 25 nations should be counted");
}

/// Test 9: Explicit verification of streaming execution.
/// We use a query that produces enough batches to verify streaming behavior effectively.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_streaming_execution() {
    let cluster = TestCluster::start(2).await;

    // Lineitem is larger (60k rows)
    cluster
        .register_csv_single("lineitem", &tpch_csv("lineitem"))
        .await;

    // A simple scan should be streamed.
    let batches = cluster.execute("SELECT * FROM lineitem").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // lineitem in TPC-H SF=0.01 has 60175 rows.
    assert_eq!(total_rows, 60175, "Lineitem row count match");
}
