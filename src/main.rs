use std::time::Duration;

use rust_streaming_systems_lab::{Event, EventKind, Pipeline, PipelineConfig};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let config = PipelineConfig {
        queue_capacity: 32,
        workers: 4,
        max_attempts: 3,
        process_delay: Duration::from_micros(50),
    };

    let pipeline = Pipeline::new(config);

    let events = (0..1_000).map(|id| Event {
        id,
        partition: (id % 8) as u16,
        sequence: id + 1,
        kind: if id % 17 == 0 {
            EventKind::Update
        } else {
            EventKind::Insert
        },
        key: format!("record-{}", id % 250),
        payload: format!("payload-{id}"),
    });

    let report = pipeline.run(events).await;
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
