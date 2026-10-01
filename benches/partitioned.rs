use criterion::{criterion_group, criterion_main, Criterion};
use rust_streaming_systems_lab::{Event, EventKind, PartitionedConfig, PartitionedEngine};
use std::time::Duration;
use tokio::runtime::Runtime;

fn events(count: u64) -> Vec<Event> {
    (0..count)
        .map(|id| {
            let key = format!("account-{}", id % 10_000);
            Event {
                id,
                partition: 0,
                sequence: id + 1,
                kind: EventKind::Update,
                key,
                payload: format!("payload-{id}"),
            }
        })
        .collect()
}

fn partitioned_throughput(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let input = events(20_000);

    c.bench_function("partitioned_20k_events", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let engine = PartitionedEngine::new(PartitionedConfig {
                    partitions: 16,
                    queue_capacity_per_partition: 256,
                    max_attempts: 3,
                    process_delay: Duration::ZERO,
                });

                let report = engine.run(input.clone()).await;
                assert_eq!(report.processed, 20_000);
            });
        });
    });
}

criterion_group!(benches, partitioned_throughput);
criterion_main!(benches);
