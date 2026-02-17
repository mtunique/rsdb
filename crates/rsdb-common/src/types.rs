//! RSDB Core Types

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique node identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u32);

impl NodeId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node_{}", self.0)
    }
}

impl From<u32> for NodeId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

/// Unique query identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryId(pub Uuid);

impl QueryId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for QueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query_{}", self.0)
    }
}

impl From<Uuid> for QueryId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Fragment identifier within a query
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FragmentId(pub u32);

impl FragmentId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for FragmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fragment_{}", self.0)
    }
}

impl From<u32> for FragmentId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

/// A plan fragment to be executed on a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanFragment {
    /// Fragment ID
    pub fragment_id: FragmentId,

    /// Node ID to execute on (None = local/coordinator)
    pub node_id: Option<NodeId>,

    /// Substrait plan serialized as bytes
    pub plan_bytes: Vec<u8>,

    /// Input partition IDs (for shuffle receive)
    pub input_partitions: Vec<u32>,

    /// Output partition count (for shuffle send)
    pub output_partition_count: u32,
}

impl PlanFragment {
    pub fn new(fragment_id: FragmentId, plan_bytes: Vec<u8>) -> Self {
        Self {
            fragment_id,
            node_id: None,
            plan_bytes,
            input_partitions: vec![],
            output_partition_count: 1,
        }
    }

    pub fn with_node(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_shuffle(mut self, input_partitions: Vec<u32>, output_partition_count: u32) -> Self {
        self.input_partitions = input_partitions;
        self.output_partition_count = output_partition_count;
        self
    }
}

/// Query execution metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMeta {
    pub query_id: QueryId,
    pub sql: String,
    pub submitted_at: DateTime<Utc>,
    pub status: QueryStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl QueryMeta {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            query_id: QueryId::new(),
            sql: sql.into(),
            submitted_at: Utc::now(),
            status: QueryStatus::Pending,
        }
    }
}

/// Worker registration info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub node_id: NodeId,
    pub addr: String,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub status: WorkerStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Active,
    Idle,
    Busy,
    Dead,
}

impl WorkerInfo {
    pub fn new(node_id: NodeId, addr: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            node_id,
            addr: addr.into(),
            registered_at: now,
            last_heartbeat: now,
            status: WorkerStatus::Active,
        }
    }
}
