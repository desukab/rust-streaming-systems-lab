use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::state::StateStore;
use crate::types::{Event, PipelineReport, ProcessedEvent};

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub queue_capacity: usize,
    pub workers: usize,
    pub max_attempts: u8,
    pub process_delay: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 128,
            workers: 4,
            max_attempts: 3,
            process_delay: Duration::from_micros(100),
        }
    }
}

#[derive(Debug, Default)]
struct Metrics {
    submitted: AtomicU64,
    processed: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    queue_depth: AtomicUsize,
    peak_queue_depth: AtomicUsize,
}

impl Metrics {
    fn queued(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        let mut current = self.peak_queue_depth.load(Ordering::Relaxed);
        while depth > current {
            match self.peak_queue_depth.compare_exchange_weak(
                current,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    fn dequeued(&self) {
        // The counter represents producer-outstanding work: buffered items plus
        // items already handed to workers. Saturation protects the metric if a
        // worker races the producer during shutdown.
        let _ = self.queue_depth.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| Some(value.saturating_sub(1)),
        );
    }
}

#[derive(Debug)]
pub struct Pipeline {
    config: PipelineConfig,
    state: Arc<StateStore>,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        assert!(config.queue_capacity > 0);
        assert!(config.workers > 0);
        assert!(config.max_attempts > 0);
        Self {
            config,
            state: Arc::new(StateStore::default()),
        }
    }

    /// Run a bounded producer/worker pipeline.
    ///
    /// The Tokio mpsc channel is the backpressure boundary: once it is full,
    /// send().await waits instead of growing memory without a limit.
    pub async fn run<I>(&self, events: I) -> PipelineReport
    where
        I: IntoIterator<Item = Event>,
    {
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::channel::<Event>(self.config.queue_capacity);
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = JoinSet::new();

        for worker_id in 0..self.config.workers {
            let rx = Arc::clone(&rx);
            let state = Arc::clone(&self.state);
            let metrics = Arc::clone(&metrics);
            let max_attempts = self.config.max_attempts;
            let delay = self.config.process_delay;

            workers.spawn(async move {
                loop {
                    let event = {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    };

                    let Some(event) = event else { break };
                    metrics.dequeued();

                    let mut attempts = 1;
                    loop {
                        sleep(delay).await;

                        // Deterministic failure injection: sequence numbers divisible
                        // by 97 fail twice, then succeed. This keeps retry behavior
                        // reproducible without external services or flaky timing.
                        if event.sequence % 97 == 0 && attempts < max_attempts {
                            attempts += 1;
                            metrics.retried.fetch_add(1, Ordering::Relaxed);
                            warn!(worker_id, event_id = event.id, attempts, "retrying event");
                            continue;
                        }
                        break;
                    }

                    state.apply(&event).await;
                    metrics.processed.fetch_add(1, Ordering::Relaxed);

                    let processed = ProcessedEvent {
                        event,
                        worker: worker_id,
                        attempts,
                    };
                    info!(worker_id, event_id = processed.event.id, "processed event");
                }
            });
        }

        for event in events {
            metrics.submitted.fetch_add(1, Ordering::Relaxed);
            metrics.queued();
            tx.send(event).await.expect("workers are still running");
        }
        drop(tx);

        while let Some(result) = workers.join_next().await {
            if let Err(error) = result {
                metrics.failed.fetch_add(1, Ordering::Relaxed);
                warn!(%error, "worker task terminated unexpectedly");
            }
        }

        PipelineReport {
            submitted: metrics.submitted.load(Ordering::Relaxed),
            processed: metrics.processed.load(Ordering::Relaxed),
            failed: metrics.failed.load(Ordering::Relaxed),
            retried: metrics.retried.load(Ordering::Relaxed),
            peak_queue_depth: metrics.peak_queue_depth.load(Ordering::Relaxed),
            final_keys: self.state.len().await,
        }
    }

    pub async fn snapshot(&self) -> std::collections::HashMap<String, String> {
        self.state.snapshot().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Event, EventKind};

    fn event(id: u64, sequence: u64, key: &str) -> Event {
        Event {
            id,
            partition: 0,
            sequence,
            kind: EventKind::Insert,
            key: key.to_owned(),
            payload: format!("value-{id}"),
        }
    }

    #[tokio::test]
    async fn bounded_pipeline_processes_every_event() {
        let pipeline = Pipeline::new(PipelineConfig {
            queue_capacity: 2,
            workers: 3,
            max_attempts: 3,
            process_delay: Duration::from_micros(1),
        });

        let report = pipeline.run((0..100).map(|id| event(id, id + 1, &format!("k-{id}")))).await;

        assert_eq!(report.submitted, 100);
        assert_eq!(report.processed, 100);
        assert_eq!(report.failed, 0);
        assert!(report.peak_queue_depth <= 5);
        assert_eq!(report.final_keys, 100);
    }

    #[tokio::test]
    async fn deterministic_failures_exercise_retry_path() {
        let pipeline = Pipeline::new(PipelineConfig {
            queue_capacity: 4,
            workers: 2,
            max_attempts: 3,
            process_delay: Duration::from_micros(1),
        });

        let report = pipeline
            .run([event(1, 97, "retry-me"), event(2, 98, "normal")])
            .await;

        assert_eq!(report.processed, 2);
        assert_eq!(report.retried, 2);
    }

    #[tokio::test]
    async fn updates_and_deletes_are_applied_to_shared_state() {
        let pipeline = Pipeline::new(PipelineConfig {
            queue_capacity: 2,
            workers: 2,
            max_attempts: 2,
            process_delay: Duration::from_micros(1),
        });

        let mut update = event(2, 2, "same");
        update.kind = EventKind::Update;
        update.payload = "new".into();

        let mut delete = event(3, 3, "same");
        delete.kind = EventKind::Delete;

        pipeline.run([event(1, 1, "same"), update, delete]).await;

        assert!(pipeline.snapshot().await.is_empty());
    }
}
