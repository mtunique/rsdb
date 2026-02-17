//! Flight client pool for inter-node communication

use crate::{NodeAddr, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// Connection pool for Flight clients
pub struct FlightClientPool {
    clients: HashMap<NodeAddr, Arc<FlightClient>>,
}

/// A Flight client connection to a remote node
pub struct FlightClient {
    addr: NodeAddr,
}

impl FlightClient {
    pub fn new(addr: NodeAddr) -> Self {
        Self { addr }
    }

    pub fn addr(&self) -> &NodeAddr {
        &self.addr
    }
}

impl FlightClientPool {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    /// Get or create a client for the given address
    pub async fn get_client(&mut self, addr: &NodeAddr) -> Result<Arc<FlightClient>> {
        // TODO: Implement actual Flight connection
        // For now, create a stub client
        if let Some(client) = self.clients.get(addr) {
            return Ok(client.clone());
        }
        let client = Arc::new(FlightClient::new(addr.clone()));
        self.clients.insert(addr.clone(), client.clone());
        Ok(client)
    }

    /// Remove a client from the pool
    pub fn remove_client(&mut self, addr: &NodeAddr) {
        self.clients.remove(addr);
    }

    /// Clear all clients
    pub fn clear(&mut self) {
        self.clients.clear();
    }
}

impl Default for FlightClientPool {
    fn default() -> Self {
        Self::new()
    }
}
