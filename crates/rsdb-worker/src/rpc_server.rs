//! Worker task RPC server.

use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use tonic::{Request, Response, Status};

use rsdb_common::rpc::worker_task_server::{WorkerTask, WorkerTaskServer};
use rsdb_common::rpc::{
    ExecutePlanFragmentRequest, ExecutePlanFragmentResponse,
    ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse,
    RegisterCsvRequest, RegisterCsvResponse, RegisterParquetRequest, RegisterParquetResponse,
};

use crate::worker::Worker;

pub async fn serve_worker_task(worker: Arc<Worker>, listen: SocketAddr) -> anyhow::Result<()> {
    let svc = WorkerTaskService { worker };
    tracing::info!("WorkerTask server listening on {}", listen);
    tonic::transport::Server::builder()
        .add_service(WorkerTaskServer::new(svc))
        .serve(listen)
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

struct WorkerTaskService {
    worker: Arc<Worker>,
}

#[tonic::async_trait]
impl WorkerTask for WorkerTaskService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let sql = request.into_inner().sql;
        let batches = self
            .worker
            .execute_sql(&sql)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let bytes = batches_to_arrow_ipc(&batches).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExecuteSqlResponse { arrow_ipc: bytes }))
    }

    async fn execute_plan_fragment(
        &self,
        request: Request<ExecutePlanFragmentRequest>,
    ) -> Result<Response<ExecutePlanFragmentResponse>, Status> {
        let plan_bytes = request.into_inner().plan_bytes;

        let batches = self
            .worker
            .execute_task(plan_bytes)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let bytes = batches_to_arrow_ipc(&batches).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExecutePlanFragmentResponse { arrow_ipc: bytes }))
    }

    async fn register_csv(
        &self,
        request: Request<RegisterCsvRequest>,
    ) -> Result<Response<RegisterCsvResponse>, Status> {
        let req = request.into_inner();
        self.worker
            .register_csv(&req.table_name, &req.path, req.has_header, req.delimiter as u8)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RegisterCsvResponse {}))
    }

    async fn register_parquet(
        &self,
        request: Request<RegisterParquetRequest>,
    ) -> Result<Response<RegisterParquetResponse>, Status> {
        let req = request.into_inner();
        self.worker
            .register_parquet(&req.table_name, &req.path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RegisterParquetResponse {}))
    }

    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let tables = self.worker.list_tables();
        Ok(Response::new(ListTablesResponse { table_names: tables }))
    }
}

fn batches_to_arrow_ipc(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let schema = if let Some(b) = batches.first() {
        b.schema()
    } else {
        Arc::new(arrow_schema::Schema::empty())
    };

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for b in batches {
            writer.write(b)?;
        }
        writer.finish()?;
    }

    // Basic sanity check: try reading back the stream header.
    let _ = StreamReader::try_new(Cursor::new(buf.clone()), None)?;
    Ok(buf)
}
