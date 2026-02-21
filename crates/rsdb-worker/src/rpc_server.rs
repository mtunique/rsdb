//! Worker task RPC server.

use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use datafusion::datasource::MemTable;
use futures::StreamExt;
use tonic::{Request, Response, Status};

use rsdb_common::rpc::worker_task_server::{WorkerTask, WorkerTaskServer};
use rsdb_common::rpc::{
    ArrowBatch,
    ExecutePlanFragmentRequest, 
    ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse,
    RegisterMemTableRequest, RegisterMemTableResponse,
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
    type ExecutePlanFragmentStreamStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<ArrowBatch, Status>> + Send + 'static>,
    >;

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

    async fn execute_plan_fragment_stream(
        &self,
        request: Request<ExecutePlanFragmentRequest>,
    ) -> Result<Response<Self::ExecutePlanFragmentStreamStream>, Status> {
        let req = request.into_inner();
        let plan_bytes = req.plan_bytes;
        
        let task_id = if req.task_id.is_empty() {
            format!("task-{}", uuid::Uuid::new_v4())
        } else {
            req.task_id.clone()
        };

        let visible_tables = self.worker.list_tables();
        tracing::info!("Executing task {} (return_data={}). Visible tables: {:?}", task_id, req.return_data, visible_tables);

        let stream = self
            .worker
            .execute_task_stream(plan_bytes, req.sources)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.worker.register_result(&task_id, vec![stream]);
        
        if req.return_data {
            let retrieved_stream = self.worker.take_result(&task_id, 0)
                .map_err(|e| Status::internal(e.to_string()))?;

            let output_stream = retrieved_stream.map(move |batch_res| {
                match batch_res {
                    Ok(batch) => {
                        tracing::info!("Task {} produced batch with {} rows, {} columns", task_id, batch.num_rows(), batch.num_columns());
                        let bytes = batches_to_arrow_ipc(&[batch]).map_err(|e| Status::internal(e.to_string()))?;
                        Ok(ArrowBatch {
                            arrow_ipc: bytes,
                            schema_ipc: vec![],
                        })
                    }
                    Err(e) => {
                        tracing::error!("Task {} produced error: {}", task_id, e);
                        Err(Status::internal(e.to_string()))
                    },
                }
            });
            Ok(Response::new(Box::pin(output_stream)))
        } else {
            tracing::info!("Task {} registered for remote pull", task_id);
            let output_stream = futures::stream::empty();
            Ok(Response::new(Box::pin(output_stream)))
        }
    }

    async fn register_mem_table(
        &self,
        request: Request<RegisterMemTableRequest>,
    ) -> Result<Response<RegisterMemTableResponse>, Status> {
        let req = request.into_inner();
        let (schema, batches) = arrow_ipc_to_schema_and_batches(&req.arrow_ipc)
            .map_err(|e| Status::internal(format!("Invalid Arrow IPC: {e}")))?;

        // Overwrite existing table.
        let engine = self
            .worker
            .datafusion_engine()
            .map_err(|e| Status::internal(e.to_string()))?;
        let ctx = engine.session_context();
        let _ = ctx.deregister_table(&req.table_name);
        let mem = MemTable::try_new(schema, vec![batches])
            .map_err(|e| Status::internal(format!("MemTable error: {e}")))?;
        ctx.register_table(req.table_name.as_str(), Arc::new(mem))
            .map_err(|e| Status::internal(format!("Register table error: {e}")))?;

        Ok(Response::new(RegisterMemTableResponse {}))
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

fn arrow_ipc_to_schema_and_batches(
    bytes: &[u8],
) -> anyhow::Result<(arrow_schema::SchemaRef, Vec<RecordBatch>)> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes.to_vec()), None)?;
    let schema = reader.schema();
    let mut out = Vec::new();
    for b in &mut reader {
        out.push(b?);
    }
    Ok((schema, out))
}
