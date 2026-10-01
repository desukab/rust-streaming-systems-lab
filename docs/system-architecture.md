# System architecture

The lab has four separable concerns:

1. Ingestion — bounded Tokio channels prevent producer bursts from becoming unbounded memory growth.
2. Partitioning — deterministic FNV-1a routing sends a logical key to one worker for a fixed partition count.
3. State — IndexedState maintains a materialized key/value view and inverted index under coordinated write locks.
4. Serving — Axum exposes health, record lookup, search, and a WebSocket event stream.

Durability is orthogonal: event -> append-only log -> partition workers -> state/index, with checkpoints beside the stream.

## Consistency boundary

For a fixed partition count, one key has one owner partition. This provides per-key ordering without a global lock around every event. Changing the partition count changes the mapping, so production rebalancing would require migration and checkpoint coordination.

## Read path

HTTP/WebSocket -> materialized memory -> index lookup.

The demo keeps reads in memory. PostgreSQL, Redis, Kafka, and ClickHouse are intentionally not included merely for résumé keywords; each would be a separate measured integration exercise.