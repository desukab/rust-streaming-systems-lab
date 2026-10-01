//! Independent systems-engineering exercises in asynchronous Rust.
//!
//! The project deliberately implements the core control flow rather than copying
//! an existing production system. Tokio is used as the runtime and channel
//! primitive; the architecture is designed to make queue bounds, ownership,
//! concurrency, and failure behavior visible and measurable.

pub mod pipeline;
pub mod state;
pub mod types;

pub use pipeline::{Pipeline, PipelineConfig};
pub use types::PipelineReport;
pub use types::{Event, EventKind, ProcessedEvent};

pub use state::StateStore;
