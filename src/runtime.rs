use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::info;

use crate::index::IndexedState;
use crate::metrics::RuntimeMetrics;
use crate::partitioned::stable_partition;
use crate::types::Event;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub partitions: usize,
    pub queue_capacity: usize,
    pub max_attempts: u8,
    pub process_delay: Duration,
    pub broadcast_capacity: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            partitions: 8,
            queue_capacity: 256,
            max_attempts: 3,
            process_delay: Duration::from_micros(50),
            broadcast_capacity: 4096,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum RuntimeStatus {
    Running,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeSnapshot {
    pub status: RuntimeStatus,
    pub partitions: usize,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub peak_queue_depth: usize,
    pub submitted: u64,
    pub processed: u64,
    pub retried: u64,
    pub duplicates: u64,
    pub stale: u64,
    pub average_latency_micros: u64,
    pub keys: usize,
}

struct Command {
    event: Event,
    ack: Option<oneshot::Sender<Result<(), SubmitError>>>,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    Draining,
    Closed,
}

impl std::fmt::Display for SubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draining => write!(f, "runtime is draining"),
            Self::Closed => write!(f, "runtime is closed"),
        }
    }
}

impl std::error::Error for SubmitError {}

#[derive(Debug)]
pub struct StreamingRuntime {
    config: RuntimeConfig,
    state: Arc<IndexedState>,
    metrics: Arc<RuntimeMetrics>,
    status: std::sync::Arc<tokio::sync::RwLock<RuntimeStatus>>,
    senders: tokio::sync::Mutex<Option<Vec<mpsc::Sender<Command>>>>,
    events: broadcast::Sender<Event>,
    workers: tokio::sync::Mutex<Option<JoinSet<()>>>,
}

impl StreamingRuntime {
    pub fn start(config: RuntimeConfig) -> Arc<Self> {
        assert!(config.partitions > 0);
        assert!(config.queue_capacity > 0);
        assert!(config.max_attempts > 0);
        assert!(config.broadcast_capacity > 0);

        let (events, _) = broadcast::channel(config.broadcast_capacity);
        let state = Arc::new(IndexedState::default());
        let metrics = Arc::new(RuntimeMetrics::default());
        let status = std::sync::Arc::new(tokio::sync::RwLock::new(RuntimeStatus::Running));
        let mut senders = Vec::with_capacity(config.partitions);
        let mut workers = JoinSet::new();

        for partition_id in 0..config.partitions {
            let (tx, mut rx) = mpsc::channel::<Command>(config.queue_capacity);
            senders.push(tx);

            let state = Arc::clone(&state);
            let metrics = Arc::clone(&metrics);
            let events = events.clone();
            let max_attempts = config.max_attempts;
            let delay = config.process_delay;

            workers.spawn(async move {
                let mut seen = HashSet::<u64>::new();
                let mut last_sequence = HashMap::<String, u64>::new();

                while let Some(command) = rx.recv().await {
                    metrics.dequeued();
                    let mut attempts = 1_u8;

                    loop {
                        sleep(delay).await;
                        if command.event.sequence % 97 == 0 && attempts < max_attempts {
                            attempts += 1;
                            metrics.retried();
                            continue;
                        }
                        break;
                    }

                    if !seen.insert(command.event.id) {
                        metrics.duplicate();
                        if let Some(ack) = command.ack {
                            let _ = ack.send(Ok(()));
                        }
                        continue;
                    }

                    let previous = last_sequence.get(&command.event.key).copied();
                    if previous.is_some_and(|sequence| command.event.sequence < sequence) {
                        metrics.stale();
                        if let Some(ack) = command.ack {
                            let _ = ack.send(Ok(()));
                        }
                        continue;
                    }

                    state.apply(&command.event).await;
                    last_sequence.insert(command.event.key.clone(), command.event.sequence);
                    metrics.processed();
                    metrics.record_latency(command.enqueued_at);
                    let _ = events.send(command.event.clone());

                    if let Some(ack) = command.ack {
                        let _ = ack.send(Ok(()));
                    }

                    info!(
                        partition = partition_id,
                        event_id = command.event.id,
                        attempts,
                        "event applied"
                    );
                }

                info!(partition = partition_id, "partition worker stopped");
            });
        }

        Arc::new(Self {
            config,
            state,
            metrics,
            status,
            senders: tokio::sync::Mutex::new(Some(senders)),
            events,
            workers: tokio::sync::Mutex::new(Some(workers)),
        })
    }

    pub async fn submit(&self, event: Event) -> Result<(), SubmitError> {
        self.submit_inner(event, false).await.map(|_| ())
    }

    pub async fn submit_and_wait(&self, event: Event) -> Result<(), SubmitError> {
        self.submit_inner(event, true).await.map(|_| ())
    }

    async fn submit_inner(&self, event: Event, wait: bool) -> Result<(), SubmitError> {
        if *self.status.read().await != RuntimeStatus::Running {
            return Err(SubmitError::Draining);
        }

        let partition = stable_partition(&event.key, self.config.partitions);
        let (ack, receiver) = if wait {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        self.metrics.submitted();
        self.metrics.queued();

        let command = Command {
            event,
            ack,
            enqueued_at: Instant::now(),
        };
        let senders = self.senders.lock().await;
        let sender = senders.as_ref().ok_or(SubmitError::Closed)?[partition].clone();
        drop(senders);

        if sender.send(command).await.is_err() {
            self.metrics.dequeued();
            self.metrics.failed();
            return Err(SubmitError::Closed);
        }

        if let Some(receiver) = receiver {
            match receiver.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(SubmitError::Closed),
            }
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub async fn record(&self, key: &str) -> Option<String> {
        self.state.get(key).await
    }

    pub async fn search(&self, term: &str) -> Vec<String> {
        self.state.search(term).await
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeSnapshot {
            status: *self.status.read().await,
            partitions: self.config.partitions,
            queue_capacity: self.config.queue_capacity,
            queue_depth: self.metrics.queue_depth(),
            peak_queue_depth: self.metrics.peak_queue_depth(),
            submitted: self.metrics.submitted_count(),
            processed: self.metrics.processed_count(),
            retried: self.metrics.retried_count(),
            duplicates: self.metrics.duplicate_count(),
            stale: self.metrics.stale_count(),
            average_latency_micros: self.metrics.average_latency_micros(),
            keys: self.state.len().await,
        }
    }

    pub async fn shutdown(&self) {
        {
            let mut status = self.status.write().await;
            if *status != RuntimeStatus::Running {
                return;
            }
            *status = RuntimeStatus::Draining;
        }

        let mut workers_guard = self.workers.lock().await;
        self.senders.lock().await.take();

        if let Some(mut workers) = workers_guard.take() {
            while workers.join_next().await.is_some() {}
        }

        *self.status.write().await = RuntimeStatus::Stopped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EventKind;

    fn event(id: u64, key: &str, sequence: u64) -> Event {
        Event {
            id,
            partition: 0,
            sequence,
            kind: EventKind::Update,
            key: key.into(),
            payload: format!("value-{id}"),
        }
    }

    #[tokio::test]
    async fn submit_and_wait_observes_applied_state() {
        let runtime = StreamingRuntime::start(RuntimeConfig {
            partitions: 2,
            queue_capacity: 2,
            process_delay: Duration::ZERO,
            ..RuntimeConfig::default()
        });

        runtime
            .submit_and_wait(event(1, "account-1", 1))
            .await
            .unwrap();
        assert_eq!(
            runtime.record("account-1").await.as_deref(),
            Some("value-1")
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_and_stale_events_do_not_corrupt_state() {
        let runtime = StreamingRuntime::start(RuntimeConfig {
            process_delay: Duration::ZERO,
            ..RuntimeConfig::default()
        });

        runtime
            .submit_and_wait(event(2, "account-1", 2))
            .await
            .unwrap();
        runtime
            .submit_and_wait(event(1, "account-1", 1))
            .await
            .unwrap();
        runtime
            .submit_and_wait(event(2, "account-1", 2))
            .await
            .unwrap();

        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.processed, 1);
        assert_eq!(snapshot.stale, 1);
        assert_eq!(snapshot.duplicates, 1);
        runtime.shutdown().await;
    }
}
