//! Cluster state management

use parking_lot::RwLock;
use rsdb_common::{types::WorkerInfo, NodeId};
use std::collections::HashMap;

/// In-memory cluster state
pub struct ClusterState {
    workers: RwLock<HashMap<u32, WorkerInfo>>,
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a worker
    pub fn register_worker(&self, info: WorkerInfo) {
        self.workers.write().insert(info.node_id.0, info);
    }

    /// Unregister a worker
    pub fn unregister_worker(&self, node_id: NodeId) {
        self.workers.write().remove(&node_id.0);
    }

    /// Get worker info
    pub fn get_worker(&self, node_id: NodeId) -> Option<WorkerInfo> {
        self.workers.read().get(&node_id.0).cloned()
    }

    /// List all workers
    pub fn list_workers(&self) -> Vec<WorkerInfo> {
        self.workers.read().values().cloned().collect()
    }

    /// Update worker heartbeat
    pub fn update_heartbeat(&self, node_id: NodeId) {
        let mut workers = self.workers.write();
        if let Some(info) = workers.get_mut(&node_id.0) {
            info.last_heartbeat = chrono::Utc::now();
        }
    }

    /// Add a worker (convenience method)
    pub fn add_worker(&mut self, node_id: NodeId, addr: String) {
        let info = WorkerInfo {
            node_id,
            addr,
            status: rsdb_common::types::WorkerStatus::Active,
            last_heartbeat: chrono::Utc::now(),
            registered_at: chrono::Utc::now(),
        };
        self.workers.write().insert(info.node_id.0, info);
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}
