//! RSDB Server Configuration

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Server running mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum, Default)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    #[default]
    Coordinator,
    Worker,
}

/// Network address for a node
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct NodeAddr {
    pub host: String,
    pub port: u16,
}

impl NodeAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn to_socket_addr(&self) -> std::io::Result<SocketAddr> {
        use std::net::ToSocketAddrs;
        let addr = format!("{}:{}", self.host, self.port);
        addr.to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid address"))
    }
}

impl std::fmt::Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server mode (coordinator or worker)
    pub mode: NodeMode,

    /// Listen address for this node
    pub listen_addr: NodeAddr,

    /// For workers: coordinator address to register with
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinator_addr: Option<NodeAddr>,

    /// Data directory for storage
    pub data_dir: PathBuf,

    /// Worker ID (for workers only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,

    /// Max concurrent queries
    pub max_concurrent_queries: usize,

    /// Heartbeat interval in seconds
    pub heartbeat_interval_secs: u64,

    /// Optional MySQL protocol listen address (coordinator only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mysql_listen_addr: Option<NodeAddr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: NodeMode::Coordinator,
            listen_addr: NodeAddr::new("127.0.0.1", 8819),
            coordinator_addr: None,
            data_dir: std::env::temp_dir().join("rsdb"),
            worker_id: None,
            max_concurrent_queries: 10,
            heartbeat_interval_secs: 30,
            mysql_listen_addr: None,
        }
    }
}

impl ServerConfig {
    pub fn load_from_file(path: impl AsRef<std::path::Path>) -> Result<Self, crate::RsdbError> {
        let content = std::fs::read_to_string(path)?;
        toml::from_str(&content).map_err(|e| crate::RsdbError::Config(e.to_string()))
    }

    pub fn save_to_file(&self, path: impl AsRef<std::path::Path>) -> Result<(), crate::RsdbError> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}
