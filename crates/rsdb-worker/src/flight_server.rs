//! Worker Flight Server for W2W Shuffle

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::error::FlightError;
use arrow_flight::{Action, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket, PollInfo};
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};

use crate::worker::Worker;

pub async fn serve_flight(worker: Arc<Worker>, listen: SocketAddr) -> anyhow::Result<()> {
    let svc = WorkerFlightService { worker };
    tracing::info!("Worker Flight server listening on {}", listen);
    Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve(listen)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

struct WorkerFlightService {
    worker: Arc<Worker>,
}

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::ActionType, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let s = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid ticket UTF-8"))?;
        
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(Status::invalid_argument("Invalid ticket format. Expected task_id:partition_id"));
        }
        
        let task_id = parts[0];
        let partition_id = parts[1].parse::<usize>()
            .map_err(|_| Status::invalid_argument("Invalid partition_id"))?;

        let stream = self.worker.take_result(task_id, partition_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let schema = stream.schema();
        
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map(|b| b.map_err(|e| FlightError::ExternalError(Box::new(e)))))
            .map_err(|e| Status::internal(format!("Encoding error: {e}")));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
