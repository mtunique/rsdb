//! Data partitioner for distribution

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Hash partitioner for distributing data across workers
pub struct HashPartitioner {
    num_partitions: u32,
}

impl HashPartitioner {
    pub fn new(num_partitions: u32) -> Self {
        Self { num_partitions }
    }

    /// Partition a key to a partition ID
    pub fn partition(&self, key: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.num_partitions as u64) as u32
    }

    /// Partition by multiple keys (hash combine)
    pub fn partition_keys(&self, keys: &[String]) -> u32 {
        let mut hasher = DefaultHasher::new();
        for key in keys {
            key.hash(&mut hasher);
        }
        (hasher.finish() % self.num_partitions as u64) as u32
    }

    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_partitioner() {
        let partitioner = HashPartitioner::new(4);
        assert_eq!(partitioner.partition("a") % 4, partitioner.partition("a"));
    }
}
