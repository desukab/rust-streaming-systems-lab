use std::sync::Arc;
use std::time::{Duration, Instant};

use rust_streaming_systems_lab::{Event, EventKind, RuntimeConfig, StreamingRuntime};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let producers = std::env::var("PRODUCERS").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    let events_per_producer = std::env::var("EVENTS").ok().and_then(|v| v.parse().ok()).unwrap_or(2_000);
    let partitions = std::env::var("PARTITIONS").ok().and_then(|v| v.parse().ok()).unwrap_or(16);

    let runtime = StreamingRuntime::start(RuntimeConfig {
        partitions,
        queue_capacity: 256,
        max_attempts: 3,
        process_delay: Duration::ZERO,
        ..RuntimeConfig::default()
    });

    let latencies = Arc::new(Mutex::new(Vec::with_capacity(producers * events_per_producer)));
    let started = Instant::now();
    let mut tasks = JoinSet::new();

    for producer in 0..producers {
        let runtime = Arc::clone(&runtime);
        let latencies = Arc::clone(&latencies);

        tasks.spawn(async move {
            for offset in 0..events_per_producer {
                let id = (producer * events_per_producer + offset) as u64 + 1;
                let event = Event {
                    id,
                    partition: 0,
                    sequence: id,
                    kind: EventKind::Update,
                    key: format!("account-{}", id % 10_000),
                    payload: format!("producer={producer} event={id}"),
                };

                let request_started = Instant::now();
                runtime.submit_and_wait(event).await?;
                latencies.lock().await.push(request_started.elapsed().as_micros() as u64);
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    runtime.shutdown().await;
    let elapsed = started.elapsed();
    let mut samples = latencies.lock().await.clone();
    samples.sort_unstable();

    let total = samples.len() as f64;
    let snapshot = runtime.snapshot().await;
    println!("events={}", samples.len());
    println!("partitions={partitions}");
    println!("elapsed_ms={}", elapsed.as_millis());
    println!("events_per_sec={:.0}", total / elapsed.as_secs_f64());
    println!("latency_p50_us={}", percentile(&samples, 0.50));
    println!("latency_p95_us={}", percentile(&samples, 0.95));
    println!("latency_p99_us={}", percentile(&samples, 0.99));
    println!("processed={}", snapshot.processed);
    println!("retried={}", snapshot.retried);
    println!("peak_queue_depth={}", snapshot.peak_queue_depth);
    println!("keys={}", snapshot.keys);

    Ok(())
}
