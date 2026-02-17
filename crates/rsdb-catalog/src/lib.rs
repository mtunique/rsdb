//! RSDB Catalog - Schema and table management

pub mod in_memory;
pub mod stats;
pub mod traits;

pub use in_memory::InMemoryCatalog;
pub use stats::{ColumnStatistics, TableStatistics};
pub use traits::{CatalogProvider, SchemaProvider, TableEntry};
