//! RSDB Common - Shared types, errors, and configuration

pub mod client_pool;
pub mod config;
pub mod error;
pub mod rpc;
pub mod types;

pub use client_pool::{FlightClient, FlightClientPool};
pub use config::{NodeAddr, NodeMode, ServerConfig};
pub use error::{Result, RsdbError};
pub use types::{FragmentId, NodeId, PlanFragment, QueryId};

use arrow_array::RecordBatch;

/// Trait for query execution - implemented by Coordinator
/// This allows FlightServer to execute queries without depending on Coordinator directly
#[async_trait::async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute a SQL query and return record batches
    async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>>;
}
