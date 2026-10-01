use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    submitted: AtomicU64,
    processed: AtomicU64,
    retried: AtomicU64,
    failed: AtomicU64,
    queue_depth: AtomicUsize,
    peak_queue_depth: AtomicUsize,
}

impl RuntimeMetrics {
    pub fn submitted(&self) { self.submitted.fetch_add(1, Ordering::Relaxed); }
    pub fn processed(&self) { self.processed.fetch_add(1, Ordering::Relaxed); }
    pub fn retried(&self) { self.retried.fetch_add(1, Ordering::Relaxed); }
    pub fn failed(&self) { self.failed.fetch_add(1, Ordering::Relaxed); }

    pub fn queued(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_queue_depth.fetch_max(depth, Ordering::Relaxed);
    }

    pub fn dequeued(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn peak_queue_depth(&self) -> usize {
        self.peak_queue_depth.load(Ordering::Relaxed)
    }
}
