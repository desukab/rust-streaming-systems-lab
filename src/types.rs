use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,
    pub partition: u16,
    pub sequence: u64,
    pub kind: EventKind,
    pub key: String,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedEvent {
    pub event: Event,
    pub worker: usize,
    pub attempts: u8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineReport {
    pub submitted: u64,
    pub processed: u64,
    pub failed: u64,
    pub retried: u64,
    pub peak_queue_depth: usize,
    pub final_keys: usize,
}
