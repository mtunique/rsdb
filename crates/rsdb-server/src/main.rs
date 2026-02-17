//! RSDB Server Binary

use clap::Parser;
use rsdb_catalog::InMemoryCatalog;
use rsdb_common::{NodeId, NodeMode, ServerConfig};
use rsdb_coordinator::Coordinator;
use rsdb_executor::DataFusionEngine;
use rsdb_storage::DeltaStorageEngine;
use rsdb_worker::Worker;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod mysql_server;

/// RSDB Server CLI
#[derive(Parser, Debug)]
#[command(name = "rsdb")]
#[command(version = "0.1.0")]
#[command(about = "Distributed MPP OLAP Database")]
struct Args {
    /// Server mode: coordinator or worker
    #[arg(long, value_enum, default_value = "coordinator")]
    mode: NodeMode,

    /// Listen address (host:port)
    #[arg(long, default_value = "127.0.0.1:8819")]
    listen_addr: String,

    /// Coordinator address (for workers)
    #[arg(long)]
    coordinator_addr: Option<String>,

    /// Data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Worker ID (for workers)
    #[arg(long)]
    worker_id: Option<String>,

    /// Config file
    #[arg(long)]
    config: Option<PathBuf>,

    /// Load TPC-DS CSV data from directory (pipe-delimited, with headers)
    #[arg(long)]
    load_tpcds: Option<PathBuf>,

    /// Load CSV files: table_name=path pairs, e.g. --load-csv users=/path/to/users.csv
    #[arg(long, value_parser = parse_csv_pair)]
    load_csv: Vec<(String, PathBuf)>,

    /// Load Parquet files: table_name=path pairs
    #[arg(long, value_parser = parse_csv_pair)]
    load_parquet: Vec<(String, PathBuf)>,

    /// Optional MySQL protocol listen address (e.g. 127.0.0.1:3307)
    #[arg(long)]
    mysql_listen_addr: Option<String>,
}

fn parse_csv_pair(s: &str) -> Result<(String, PathBuf), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!("Expected format: table_name=path, got: {s}"));
    }
    Ok((parts[0].to_string(), PathBuf::from(parts[1])))
}

fn parse_listen_addr(s: &str) -> anyhow::Result<rsdb_common::NodeAddr> {
    let parts: Vec<&str> = s.split(':').collect();
    let host = parts
        .first()
        .map(|s| (*s).to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(3307);
    Ok(rsdb_common::NodeAddr::new(host, port))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config = if let Some(config_path) = args.config {
        let mut cfg = ServerConfig::load_from_file(config_path)?;
        if let Some(mysql_addr) = &args.mysql_listen_addr {
            cfg.mysql_listen_addr = Some(parse_listen_addr(mysql_addr)?);
        }
        cfg
    } else {
        let mut config = ServerConfig {
            mode: args.mode,
            ..Default::default()
        };

        let parts: Vec<&str> = args.listen_addr.split(':').collect();
        if let Some(host) = parts.first() {
            config.listen_addr.host = host.to_string();
        }
        if let Some(port) = parts.get(1) {
            config.listen_addr.port = port.parse().unwrap_or(8819);
        }

        if let Some(coord_addr) = args.coordinator_addr {
            let coord_parts: Vec<&str> = coord_addr.split(':').collect();
            if let Some(host) = coord_parts.first() {
                config.coordinator_addr = Some(rsdb_common::NodeAddr::new(
                    host.to_string(),
                    coord_parts
                        .get(1)
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(8819),
                ));
            }
        }

        if let Some(data_dir) = args.data_dir {
            config.data_dir = data_dir;
        }

        config.worker_id = args.worker_id;

        if let Some(mysql_addr) = &args.mysql_listen_addr {
            config.mysql_listen_addr = Some(parse_listen_addr(mysql_addr)?);
        }
        config
    };

    tracing::info!("Starting RSDB in {:?} mode", config.mode);

    match config.mode {
        NodeMode::Coordinator => {
            run_coordinator(config, args.load_tpcds, args.load_csv, args.load_parquet).await?
        }
        NodeMode::Worker => run_worker(config).await?,
    }

    Ok(())
}

async fn run_coordinator(
    config: ServerConfig,
    load_tpcds: Option<PathBuf>,
    load_csv: Vec<(String, PathBuf)>,
    load_parquet: Vec<(String, PathBuf)>,
) -> anyhow::Result<()> {
    let catalog: Arc<dyn rsdb_catalog::CatalogProvider> = Arc::new(InMemoryCatalog::new());
    let storage = Arc::new(DeltaStorageEngine::new(&config.data_dir));
    let executor: Arc<dyn rsdb_executor::ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let coordinator = Coordinator::new(catalog, storage, executor);
    let coordinator = Arc::new(tokio::sync::Mutex::new(coordinator));

    // Load data
    {
        let mut coord = coordinator.lock().await;

        // Load TPC-DS data
        if let Some(tpcds_dir) = &load_tpcds {
            load_tpcds_data(&mut coord, tpcds_dir).await?;
        }

        // Load individual CSV files
        for (table_name, path) in &load_csv {
            coord.register_csv(table_name, path).await?;
        }

        // Load individual Parquet files
        for (table_name, path) in &load_parquet {
            coord.register_parquet(table_name, path).await?;
        }

        let tables = coord.list_tables();
        if !tables.is_empty() {
            tracing::info!("Registered tables: {}", tables.join(", "));
        }
    }

    // Start FlightSQL server
    let addr: std::net::SocketAddr = config.listen_addr.to_string().parse()?;
    let coordinator_for_mysql = coordinator.clone();
    let flight_server = rsdb_coordinator::RsdbFlightServer::new(coordinator);

    tracing::info!("RSDB Coordinator ready on {}", addr);
    tracing::info!(
        "Connect via FlightSQL: grpc://{}:{}",
        config.listen_addr.host,
        config.listen_addr.port
    );

    let flight_handle = tokio::spawn(async move {
        if let Err(e) = flight_server.serve(addr).await {
            tracing::error!("Flight server error: {}", e);
        }
    });

    if let Some(mysql_addr) = config.mysql_listen_addr.clone() {
        let host = mysql_addr.host.clone();
        let port = mysql_addr.port;
        mysql_server::spawn_mysql_server(mysql_addr, coordinator_for_mysql)?;
        tracing::info!("Connect via MySQL: mysql -h {} -P {}", host, port);
    }

    tokio::select! {
        _ = flight_handle => {}
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down coordinator");
        }
    }

    Ok(())
}

/// Load TPC-DS data from a directory of pipe-delimited CSV files
async fn load_tpcds_data(coordinator: &mut Coordinator, data_dir: &Path) -> anyhow::Result<()> {
    let tpcds_tables = [
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "dbgen_version",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ];

    let mut loaded = 0;
    for table_name in &tpcds_tables {
        let csv_path = data_dir.join(format!("{}.csv", table_name));
        if csv_path.exists() {
            match coordinator.register_csv(table_name, &csv_path).await {
                Ok(()) => loaded += 1,
                Err(e) => tracing::warn!("Failed to load {}: {}", table_name, e),
            }
        }
        // Also try .dat extension
        let dat_path = data_dir.join(format!("{}.dat", table_name));
        if dat_path.exists() && !csv_path.exists() {
            match coordinator.register_csv(table_name, &dat_path).await {
                Ok(()) => loaded += 1,
                Err(e) => tracing::warn!("Failed to load {}: {}", table_name, e),
            }
        }
    }

    tracing::info!(
        "Loaded {}/{} TPC-DS tables from {:?}",
        loaded,
        tpcds_tables.len(),
        data_dir
    );
    Ok(())
}

async fn run_worker(config: ServerConfig) -> anyhow::Result<()> {
    let worker_id = config
        .worker_id
        .clone()
        .unwrap_or_else(|| "worker-1".to_string());
    let node_id = NodeId(worker_id.parse().unwrap_or(1));

    let catalog: Arc<dyn rsdb_catalog::CatalogProvider> = Arc::new(InMemoryCatalog::new());
    let storage = Arc::new(DeltaStorageEngine::new(&config.data_dir));
    let executor: Arc<dyn rsdb_executor::ExecutionEngine> = Arc::new(DataFusionEngine::new());

    let worker = Worker::new(node_id, catalog, storage, executor);

    // Start worker task RPC server.
    let listen: std::net::SocketAddr = config.listen_addr.to_string().parse()?;
    let worker = Arc::new(worker);
    let worker_for_rpc = worker.clone();
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = rsdb_worker::serve_worker_task(worker_for_rpc, listen).await {
            tracing::error!("WorkerTask server error: {}", e);
        }
    });

    if let Some(coord_addr) = &config.coordinator_addr {
        let addr_str = coord_addr.to_string();
        if let Err(e) = worker.register(&addr_str, &config.listen_addr).await {
            tracing::warn!("Failed to register with coordinator: {}", e);
        } else {
            let heartbeat_addr = addr_str.clone();
            let interval_secs = config.heartbeat_interval_secs;
            let worker_for_hb = worker.clone();
            tokio::spawn(async move {
                worker_for_hb
                    .start_heartbeat(heartbeat_addr, interval_secs)
                    .await;
            });
        }
    }

    tracing::info!(
        "Worker {} started on {}",
        worker.node_id(),
        config.listen_addr
    );

    tokio::select! {
        _ = rpc_handle => {}
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down worker");
        }
    }
    Ok(())
}
