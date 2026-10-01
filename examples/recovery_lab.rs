use std::time::Duration;

use rust_streaming_systems_lab::{
    simulator, Checkpoint, CheckpointStore, EventLog, PartitionedConfig, PartitionedEngine,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::temp_dir().join("rust-streaming-systems-lab-recovery");
    let _ = tokio::fs::remove_dir_all(&directory).await;
    tokio::fs::create_dir_all(&directory).await?;

    let log = EventLog::open(directory.join("events.log")).await?;
    let checkpoints = CheckpointStore::open(directory.join("checkpoints")).await?;

    let generated = simulator::generate(20, 25);
    let events: Vec<_> = generated
        .into_iter()
        .map(|event| event.into_event())
        .collect();
    for event in &events {
        log.append(event).await?;
    }

    let engine = PartitionedEngine::new(PartitionedConfig {
        partitions: 8,
        queue_capacity_per_partition: 32,
        max_attempts: 3,
        process_delay: Duration::ZERO,
    });
    let report = engine.run(events.iter().take(250).cloned()).await;

    for partition in 0..8 {
        checkpoints
            .save(&Checkpoint {
                partition,
                sequence: 250,
            })
            .await?;
    }
    println!("initial run: {report:?}");

    let replayed = log.replay().await?;
    let recovered = PartitionedEngine::new(PartitionedConfig {
        partitions: 8,
        queue_capacity_per_partition: 32,
        max_attempts: 3,
        process_delay: Duration::ZERO,
    });
    let recovery_report = recovered.run(replayed.into_iter().skip(250)).await;
    println!("recovery run: {recovery_report:?}");
    println!(
        "recovery complete: {} events replayed",
        recovery_report.processed
    );
    Ok(())
}
