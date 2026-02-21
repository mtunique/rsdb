//! RemoteExchangeExec: Reads data from upstream workers via Arrow Flight

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::Ticket;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    empty::EmptyExec,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::execution::TaskContext;
use datafusion::common::{Result, DataFusionError};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use futures::{Stream, TryStreamExt};
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct PartitionLocation {
    pub worker_addr: String,
    pub ticket: Vec<u8>,
}

/// A physical plan node that reads data from upstream workers via Arrow Flight.
#[derive(Debug)]
pub struct RemoteExchangeExec {
    _partitioning: Partitioning,
    locations: Vec<Vec<PartitionLocation>>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl RemoteExchangeExec {
    pub fn new(
        locations: Vec<Vec<PartitionLocation>>,
        schema: SchemaRef,
    ) -> Self {
        let partitioning = Partitioning::UnknownPartitioning(locations.len());
        
        let empty_exec = EmptyExec::new(schema.clone());
        let base_props = empty_exec.properties();
        
        // Clone properties and update the partitioning.
        // DataFusion 52 fields: eq_properties, partitioning, emission_type, boundedness, evaluation_type, scheduling_type
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            partitioning.clone(),
            base_props.emission_type.clone(),
            base_props.boundedness.clone(),
        );
        
        Self {
            _partitioning: partitioning,
            locations,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }
}

impl DisplayAs for RemoteExchangeExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        match t {
            _ => write!(f, "RemoteExchangeExec: partitions={}", self.locations.len()),
        }
    }
}

impl ExecutionPlan for RemoteExchangeExec {
    fn name(&self) -> &str { "RemoteExchangeExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let locations = self.locations.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!("Partition {partition} not found"))
        })?;

        let mut streams = Vec::with_capacity(locations.len());
        for loc in locations {
            let stream = fetch_partition(loc.worker_addr.clone(), loc.ticket.clone());
            streams.push(stream);
        }

        let merged = futures::stream::select_all(streams);
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), merged)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn create_flight_client(addr: String) -> Result<FlightSqlServiceClient<Channel>> {
    let url = if addr.starts_with("http") { addr } else { format!("http://{}", addr) };
    let endpoint = Channel::from_shared(url).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let channel = endpoint.connect().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(FlightSqlServiceClient::new(channel))
}

fn fetch_partition(addr: String, ticket_bytes: Vec<u8>) -> Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> {
    let stream = async move {
        let mut client = create_flight_client(addr).await?;
        let ticket = Ticket { ticket: ticket_bytes.into() };
        let flight_stream = client.do_get(ticket).await.map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok::<_, DataFusionError>(flight_stream)
    };
    Box::pin(futures::stream::once(stream).map_ok(|s| s.map_err(|e| DataFusionError::External(Box::new(e)))).try_flatten())
}

#[derive(Debug)]
pub struct RemoteExchangeProvider {
    pub partitions: Vec<Vec<PartitionLocation>>,
    pub schema: SchemaRef,
}

#[async_trait::async_trait]
impl TableProvider for RemoteExchangeProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = RemoteExchangeExec::new(self.partitions.clone(), self.schema.clone());
        Ok(Arc::new(exec))
    }
    
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
    }
}
