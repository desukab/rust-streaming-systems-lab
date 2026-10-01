//! Independent systems-engineering exercises in asynchronous Rust.
//!
//! The project deliberately implements the core control flow rather than copying
//! an existing production system. Tokio is used as the runtime and channel
//! primitive; the architecture makes queue bounds, ownership, concurrency,
//! durability, and failure behavior visible and measurable.

pub mod durable;
pub mod index;
pub mod metrics;
pub mod partitioned;
pub mod pipeline;
pub mod state;
pub mod types;

pub use durable::{Checkpoint, CheckpointStore, EventLog};
pub use index::IndexedState;
pub use partitioned::{stable_partition, PartitionedConfig, PartitionedEngine};
pub use pipeline::{Pipeline, PipelineConfig};
pub use state::StateStore;
pub use types::PipelineReport;
pub use types::{Event, EventKind, ProcessedEvent};
