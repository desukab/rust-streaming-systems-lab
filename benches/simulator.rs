use criterion::{criterion_group, criterion_main, Criterion};
use rust_streaming_systems_lab::{simulator, PartitionedConfig, PartitionedEngine};
use std::time::Duration;
use tokio::runtime::Runtime;

fn simulated_chain(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let input = simulator::generate(100, 100);

    c.bench_function("simulated_chain_10k", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let engine = PartitionedEngine::new(PartitionedConfig {
                    partitions: 16,
                    queue_capacity_per_partition: 256,
                    max_attempts: 3,
                    process_delay: Duration::ZERO,
                });

                let events = input.iter().cloned().map(|event| event.into_event());
                let report = engine.run(events).await;
                assert_eq!(report.processed, 10_000);
            });
        });
    });
}

criterion_group!(benches, simulated_chain);
criterion_main!(benches);
