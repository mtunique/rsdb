//! Coordinator control plane RPC.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use rsdb_common::rpc::coordinator_control_server::CoordinatorControl;
use rsdb_common::rpc::{
    GetRegisteredTablesRequest, GetRegisteredTablesResponse, HeartbeatRequest, HeartbeatResponse,
    ListWorkersRequest, ListWorkersResponse, RegisterWorkerRequest, RegisterWorkerResponse,
    RegisteredTable, Worker,
};
use rsdb_common::NodeId;

use crate::coordinator::{Coordinator, RegisteredTableSource};

pub struct CoordinatorControlService {
    coordinator: Arc<tokio::sync::Mutex<Coordinator>>,
}

impl CoordinatorControlService {
    pub fn new(coordinator: Arc<tokio::sync::Mutex<Coordinator>>) -> Self {
        Self { coordinator }
    }
}

#[tonic::async_trait]
impl CoordinatorControl for CoordinatorControlService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId(req.node_id);
        let addr = req.addr;

        // Update in-memory cluster state and snapshot current table registrations.
        let tables: Vec<(String, RegisteredTableSource)> = {
            let mut coord = self.coordinator.lock().await;
            coord.register_worker(node_id, addr.clone());
            coord.registered_table_sources()
        };

        // Best-effort table sync to the newly registered worker.
        if let Err(e) = Coordinator::sync_tables_to_worker_addr(&addr, &tables).await {
            tracing::warn!(
                "Failed to sync tables to worker {} ({}): {}",
                node_id,
                addr,
                e
            );
        }

        Ok(Response::new(RegisterWorkerResponse {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let node_id = NodeId(req.node_id);
        let coord = self.coordinator.lock().await;
        coord.update_worker_heartbeat(node_id);
        Ok(Response::new(HeartbeatResponse {}))
    }

    async fn list_workers(
        &self,
        _request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        let coord = self.coordinator.lock().await;
        let workers = coord
            .list_workers()
            .into_iter()
            .map(|w| Worker {
                node_id: w.node_id.0,
                addr: w.addr,
                registered_at_unix_ms: w.registered_at.timestamp_millis(),
                last_heartbeat_unix_ms: w.last_heartbeat.timestamp_millis(),
                status: format!("{:?}", w.status).to_lowercase(),
            })
            .collect();

        Ok(Response::new(ListWorkersResponse { workers }))
    }

    async fn get_registered_tables(
        &self,
        _request: Request<GetRegisteredTablesRequest>,
    ) -> Result<Response<GetRegisteredTablesResponse>, Status> {
        let coord = self.coordinator.lock().await;
        let tables = coord
            .registered_table_sources()
            .into_iter()
            .map(|(name, src)| match src {
                RegisteredTableSource::Csv {
                    path,
                    has_header,
                    delimiter,
                } => RegisteredTable {
                    table_name: name,
                    kind: "csv".to_string(),
                    path: path.to_string_lossy().to_string(),
                    has_header,
                    delimiter: delimiter as u32,
                },
                RegisteredTableSource::Parquet { path } => RegisteredTable {
                    table_name: name,
                    kind: "parquet".to_string(),
                    path: path.to_string_lossy().to_string(),
                    has_header: true,
                    delimiter: 0,
                },
            })
            .collect();

        Ok(Response::new(GetRegisteredTablesResponse { tables }))
    }
}
