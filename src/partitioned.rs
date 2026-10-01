use std::sync::Arc;
use std::time::Duration;

use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::index::IndexedState;
use crate::types::{Event, PipelineReport};

#[derive(Debug, Clone)]
pub struct PartitionedConfig {
    pub partitions: usize,
    pub queue_capacity_per_partition: usize,
    pub max_attempts: u8,
    pub process_delay: Duration,
}

impl Default for PartitionedConfig {
    fn default() -> Self {
        Self {
            partitions: 8,
            queue_capacity_per_partition: 128,
            max_attempts: 3,
            process_delay: Duration::from_micros(50),
        }
    }
}

#[derive(Debug)]
pub struct PartitionedEngine {
    config: PartitionedConfig,
    state: Arc<IndexedState>,
}

#[derive(Debug)]
struct Command {
    event: Event,
    done: oneshot::Sender<bool>,
}

impl PartitionedEngine {
    pub fn new(config: PartitionedConfig) -> Self {
        assert!(config.partitions > 0);
        assert!(config.queue_capacity_per_partition > 0);
        assert!(config.max_attempts > 0);

        Self {
            config,
            state: Arc::new(IndexedState::default()),
        }
    }

    /// Route each event to exactly one partition.
    ///
    /// A partition has a single consumer, so events routed to the same
    /// partition are applied sequentially while unrelated partitions run
    /// concurrently. This is the core ordering/concurrency trade-off.
    pub async fn run<I>(&self, events: I) -> PipelineReport
    where
        I: IntoIterator<Item = Event>,
    {
        let mut senders = Vec::with_capacity(self.config.partitions);
        let mut workers = JoinSet::new();

        for partition_id in 0..self.config.partitions {
            let (tx, mut rx) = mpsc::channel::<Command>(self.config.queue_capacity_per_partition);
            senders.push(tx);

            let state = Arc::clone(&self.state);
            let max_attempts = self.config.max_attempts;
            let delay = self.config.process_delay;

            workers.spawn(async move {
                let mut processed = 0_u64;
                let mut seen = HashSet::new();
                let mut last_sequence: HashMap<String, u64> = HashMap::new();
                let mut retried = 0_u64;

                while let Some(command) = rx.recv().await {
                    let mut attempts = 1;

                    loop {
                        sleep(delay).await;

                        if command.event.sequence % 97 == 0 && attempts < max_attempts {
                            attempts += 1;
                            retried += 1;
                            warn!(
                                partition = partition_id,
                                event_id = command.event.id,
                                attempts,
                                "retrying partitioned event"
                            );
                            continue;
                        }
                        break;
                    }

                    let mut applied = false;
                    if seen.insert(command.event.id) {
                        let previous = last_sequence.get(&command.event.key).copied();
                        if previous.is_none_or(|sequence| command.event.sequence >= sequence) {
                            state.apply(&command.event).await;
                            last_sequence.insert(command.event.key.clone(), command.event.sequence);
                            processed += 1;
                            applied = true;
                        }
                    }
                    let _ = command.done.send(applied);
                }

                info!(
                    partition = partition_id,
                    processed, retried, "partition stopped"
                );
                (processed, retried)
            });
        }

        let mut submitted = 0_u64;
        let mut receivers = Vec::new();

        for event in events {
            submitted += 1;
            let partition = stable_partition(&event.key, self.config.partitions);
            let (done_tx, done_rx) = oneshot::channel();
            senders[partition]
                .send(Command {
                    event,
                    done: done_tx,
                })
                .await
                .expect("partition worker is alive");
            receivers.push(done_rx);
        }

        drop(senders);

        let mut processed = 0_u64;
        let mut retried = 0_u64;

        for receiver in receivers {
            if receiver.await.unwrap_or(false) {
                processed += 1;
            }
        }

        while let Some(result) = workers.join_next().await {
            match result {
                Ok((_worker_processed, worker_retried)) => retried += worker_retried,
                Err(error) => warn!(%error, "partition worker failed"),
            }
        }

        PipelineReport {
            submitted,
            processed,
            failed: submitted.saturating_sub(processed),
            retried,
            peak_queue_depth: 0,
            final_keys: self.state.len().await,
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.state.get(key).await
    }

    pub async fn search(&self, term: &str) -> Vec<String> {
        self.state.search(term).await
    }
}

/// FNV-1a gives us a deterministic, dependency-free partition function.
///
/// The important property here is stability: the same logical key always maps
/// to the same partition for a fixed partition count.
pub fn stable_partition(key: &str, partitions: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;

    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    (hash as usize) % partitions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Event, EventKind};

    fn event(id: u64, key: &str, payload: &str) -> Event {
        Event {
            id,
            partition: stable_partition(key, 4) as u16,
            sequence: id,
            kind: EventKind::Update,
            key: key.into(),
            payload: payload.into(),
        }
    }

    #[test]
    fn partition_mapping_is_stable() {
        assert_eq!(
            stable_partition("wallet-42", 16),
            stable_partition("wallet-42", 16)
        );
    }

    #[tokio::test]
    async fn same_key_is_serialized_and_final_state_is_latest() {
        let engine = PartitionedEngine::new(PartitionedConfig {
            partitions: 4,
            queue_capacity_per_partition: 2,
            max_attempts: 3,
            process_delay: Duration::from_micros(1),
        });

        let events = [
            event(1, "account-1", "first"),
            event(2, "account-1", "second"),
            event(3, "account-1", "third"),
        ];

        let report = engine.run(events).await;
        assert_eq!(report.processed, 3);
        assert_eq!(engine.get("account-1").await.as_deref(), Some("third"));
    }

    #[tokio::test]
    async fn duplicate_event_id_is_idempotent() {
        let engine = PartitionedEngine::new(PartitionedConfig::default());
        let first = event(7, "account-1", "first");
        let duplicate = first.clone();

        let report = engine.run([first, duplicate]).await;

        assert_eq!(report.submitted, 2);
        assert_eq!(report.processed, 1);
        assert_eq!(engine.get("account-1").await.as_deref(), Some("first"));
    }

    #[tokio::test]
    async fn out_of_order_event_does_not_overwrite_newer_state() {
        let engine = PartitionedEngine::new(PartitionedConfig::default());
        let newer = event(2, "account-1", "new");
        let older = event(1, "account-1", "old");

        let report = engine.run([newer, older]).await;

        assert_eq!(report.processed, 1);
        assert_eq!(engine.get("account-1").await.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn secondary_index_supports_queries() {
        let engine = PartitionedEngine::new(PartitionedConfig::default());

        let events = [
            event(1, "a", "Rust systems"),
            event(2, "b", "Rust async"),
            event(3, "c", "database"),
        ];

        engine.run(events).await;
        assert_eq!(engine.search("rust").await, vec!["a", "b"]);
    }
}
