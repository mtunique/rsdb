//! FlightSQL server implementation
//!
//! Implements Arrow Flight SQL protocol for RSDB, allowing standard
//! FlightSQL clients (Python ADBC, DBeaver, JDBC) to connect.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use arrow_schema::{Schema, SchemaRef};
use dashmap::DashMap;
use futures::{Stream, TryStreamExt};
use prost::bytes::Bytes;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use crate::coordinator::Coordinator;
use crate::control_server::CoordinatorControlService;
use rsdb_common::rpc::coordinator_control_server::CoordinatorControlServer;

/// RSDB Flight SQL Server
pub struct RsdbFlightServer {
    coordinator: Arc<tokio::sync::Mutex<Coordinator>>,
}

impl RsdbFlightServer {
    pub fn new(coordinator: Arc<tokio::sync::Mutex<Coordinator>>) -> Self {
        Self { coordinator }
    }

    /// Start the FlightSQL server on the given address
    pub async fn serve(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let service = RsdbFlightSqlService {
            coordinator: self.coordinator.clone(),
            query_results: Arc::new(DashMap::new()),
        };

        let control = CoordinatorControlService::new(self.coordinator.clone());

        tracing::info!("FlightSQL server listening on {}", addr);

        Server::builder()
            .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(service))
            .add_service(CoordinatorControlServer::new(control))
            .serve(addr)
            .await?;

        Ok(())
    }
}

/// Cached query result
struct QueryResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

/// Inner FlightSQL service implementation
struct RsdbFlightSqlService {
    coordinator: Arc<tokio::sync::Mutex<Coordinator>>,
    /// Cache query results between get_flight_info and do_get
    query_results: Arc<DashMap<String, QueryResult>>,
}

impl RsdbFlightSqlService {
    /// Execute a SQL query through the coordinator
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator
            .execute_query(sql)
            .await
            .map_err(|e| Status::internal(format!("Query execution error: {e}")))
    }

    /// Get schema from batches, or empty schema
    fn get_schema(batches: &[RecordBatch]) -> SchemaRef {
        if let Some(first) = batches.first() {
            first.schema()
        } else {
            Arc::new(Schema::empty())
        }
    }

    /// Build a FlightData stream from record batches
    fn batches_to_flight_stream(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>
    {
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(|e| Status::internal(format!("Flight encoding error: {e}")));
        Box::pin(flight_data_stream)
    }
}

#[tonic::async_trait]
impl FlightSqlService for RsdbFlightSqlService {
    type FlightService = RsdbFlightSqlService;

    /// Handshake - simple auth
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::from_static(b"OK"),
        };
        let stream = futures::stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Get flight info for a SQL statement
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = &query.query;
        tracing::info!("get_flight_info_statement: {}", sql);

        // Execute the query
        let batches = self.execute_sql(sql).await?;
        let schema = Self::get_schema(&batches);
        let total_records: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        // Generate a handle and cache the results
        let handle = uuid::Uuid::new_v4().to_string();
        self.query_results.insert(
            handle.clone(),
            QueryResult {
                schema: schema.clone(),
                batches,
            },
        );

        // Create a ticket with TicketStatementQuery
        let ticket = TicketStatementQuery {
            statement_handle: handle.into(),
        };
        let ticket_bytes: Vec<u8> = {
            let any = ticket.as_any();
            prost::Message::encode_to_vec(&any)
        };

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema encoding error: {e}")))?
            .with_endpoint(endpoint)
            .with_total_records(total_records)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    /// Get the query results as a FlightData stream
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid statement handle"))?;

        tracing::info!("do_get_statement: handle={}", handle);

        // Retrieve cached results
        let result = self
            .query_results
            .remove(&handle)
            .map(|(_, v)| v)
            .ok_or_else(|| Status::not_found(format!("No cached result for handle: {handle}")))?;

        let stream = Self::batches_to_flight_stream(result.schema, result.batches);
        Ok(Response::new(stream))
    }

    /// Get catalogs
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Return a single catalog "rsdb"
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "catalog_name",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["rsdb"]))],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let stream = Self::batches_to_flight_stream(schema, vec![batch]);
        Ok(Response::new(stream))
    }

    /// Get schemas
    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("catalog_name", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("db_schema_name", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["rsdb"])),
                Arc::new(arrow_array::StringArray::from(vec!["public"])),
            ],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let stream = Self::batches_to_flight_stream(schema, vec![batch]);
        Ok(Response::new(stream))
    }

    /// Get tables - list all registered tables
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Get table names from DataFusion's catalog
        let coordinator = self.coordinator.lock().await;
        let table_names = coordinator.list_tables();

        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("catalog_name", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("db_schema_name", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("table_name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("table_type", arrow_schema::DataType::Utf8, false),
        ]));

        let n = table_names.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["rsdb"; n])),
                Arc::new(arrow_array::StringArray::from(vec!["public"; n])),
                Arc::new(arrow_array::StringArray::from(table_names.clone())),
                Arc::new(arrow_array::StringArray::from(vec!["TABLE"; n])),
            ],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let stream = Self::batches_to_flight_stream(schema, vec![batch]);
        Ok(Response::new(stream))
    }

    /// Get table types
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "table_type",
            arrow_schema::DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["TABLE"]))],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let stream = Self::batches_to_flight_stream(schema, vec![batch]);
        Ok(Response::new(stream))
    }

    /// Get SQL info
    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Return empty for now
        let schema = Arc::new(Schema::empty());
        let stream = Self::batches_to_flight_stream(schema, vec![]);
        Ok(Response::new(stream))
    }

    /// Register SQL info (required by trait)
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
