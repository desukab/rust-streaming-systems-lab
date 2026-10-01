use criterion::{criterion_group, criterion_main, Criterion};
use rust_streaming_systems_lab::{Event, EventKind, Pipeline, PipelineConfig};
use std::time::Duration;
use tokio::runtime::Runtime;

fn make_events(count: u64) -> Vec<Event> {
    (0..count)
        .map(|id| Event {
            id,
            partition: (id % 16) as u16,
            sequence: id + 1,
            kind: EventKind::Insert,
            key: format!("key-{}", id % 1000),
            payload: format!("payload-{id}"),
        })
        .collect()
}

fn pipeline_throughput(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let events = make_events(10_000);

    c.bench_function("process_10k_events", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let pipeline = Pipeline::new(PipelineConfig {
                    queue_capacity: 256,
                    workers: 8,
                    max_attempts: 3,
                    process_delay: Duration::ZERO,
                });
                let report = pipeline.run(events.clone()).await;
                assert_eq!(report.processed, 10_000);
            });
        });
    });
}

criterion_group!(benches, pipeline_throughput);
criterion_main!(benches);
