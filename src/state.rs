use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::types::{Event, EventKind};

/// Concurrent in-memory state keyed by the logical record key.
///
/// A real indexer would persist or shard this state differently. The lab keeps
/// the storage intentionally small so the concurrency and ownership model is
/// easy to inspect.
#[derive(Debug, Default)]
pub struct StateStore {
    records: RwLock<HashMap<String, String>>,
}

impl StateStore {
    pub async fn apply(&self, event: &Event) {
        let mut records = self.records.write().await;
        match event.kind {
            EventKind::Insert | EventKind::Update => {
                records.insert(event.key.clone(), event.payload.clone());
            }
            EventKind::Delete => {
                records.remove(&event.key);
            }
        }
    }

    pub async fn len(&self) -> usize {
        self.records.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.records.read().await.is_empty()
    }

    pub async fn snapshot(&self) -> HashMap<String, String> {
        self.records.read().await.clone()
    }
}
