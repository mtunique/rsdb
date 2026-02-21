use dashmap::DashMap;
use datafusion::physical_plan::SendableRecordBatchStream;
use rsdb_common::Result;
use parking_lot::Mutex;

/// Manages execution results (streams) for remote consumption.
pub struct ResultManager {
    /// Map TaskId -> Partitioned Streams.
    /// Mutex is used to make the value Sync, allowing DashMap to be Sync.
    partitions: DashMap<String, Vec<Option<Mutex<SendableRecordBatchStream>>>>,
}

impl ResultManager {
    pub fn new() -> Self {
        Self {
            partitions: DashMap::new(),
        }
    }

    pub fn register_result(
        &self,
        task_id: &str,
        streams: Vec<SendableRecordBatchStream>,
    ) {
        let streams = streams.into_iter().map(|s| Some(Mutex::new(s))).collect();
        self.partitions.insert(task_id.to_string(), streams);
    }

    pub fn take_result(
        &self,
        task_id: &str,
        partition_id: usize,
    ) -> Result<SendableRecordBatchStream> {
        if let Some(mut parts) = self.partitions.get_mut(task_id) {
            if partition_id < parts.len() {
                if let Some(mutex) = parts[partition_id].take() {
                    return Ok(mutex.into_inner());
                }
            }
        }
        Err(rsdb_common::RsdbError::Worker(format!(
            "Result not found or already consumed: {}/{}",
            task_id, partition_id
        )))
    }
}
