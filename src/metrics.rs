use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    submitted: AtomicU64,
    processed: AtomicU64,
    retried: AtomicU64,
    failed: AtomicU64,
    duplicate: AtomicU64,
    stale: AtomicU64,
    queue_depth: AtomicUsize,
    peak_queue_depth: AtomicUsize,
    latency_samples: AtomicU64,
    latency_total_micros: AtomicU64,
}

impl RuntimeMetrics {
    pub fn submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn processed(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }
    pub fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn duplicate(&self) {
        self.duplicate.fetch_add(1, Ordering::Relaxed);
    }
    pub fn stale(&self) {
        self.stale.fetch_add(1, Ordering::Relaxed);
    }

    pub fn queued(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_queue_depth.fetch_max(depth, Ordering::Relaxed);
    }

    pub fn dequeued(&self) {
        let _ = self
            .queue_depth
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            });
    }

    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }
    pub fn peak_queue_depth(&self) -> usize {
        self.peak_queue_depth.load(Ordering::Relaxed)
    }
    pub fn submitted_count(&self) -> u64 {
        self.submitted.load(Ordering::Relaxed)
    }
    pub fn processed_count(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }
    pub fn retried_count(&self) -> u64 {
        self.retried.load(Ordering::Relaxed)
    }
    pub fn failed_count(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
    pub fn duplicate_count(&self) -> u64 {
        self.duplicate.load(Ordering::Relaxed)
    }
    pub fn stale_count(&self) -> u64 {
        self.stale.load(Ordering::Relaxed)
    }

    pub fn record_latency(&self, started: Instant) {
        self.latency_samples.fetch_add(1, Ordering::Relaxed);
        self.latency_total_micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    pub fn average_latency_micros(&self) -> u64 {
        let samples = self.latency_samples.load(Ordering::Relaxed);
        self.latency_total_micros
            .load(Ordering::Relaxed)
            .checked_div(samples)
            .unwrap_or(0)
    }
}
