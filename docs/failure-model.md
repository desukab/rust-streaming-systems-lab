# Failure Model

The lab currently exercises several failure classes.

| Failure | Mechanism | Expected behavior |
| --- | --- | --- |
| Queue saturation | bounded mpsc | producer waits |
| Transient processing failure | deterministic sequence rule | retry |
| Worker task failure | JoinSet result | report failure |
| Client disconnect | TCP line stream ends | connection task exits |
| Service shutdown | Ctrl-C | stop accepting and drain queued work |
| Shared-state contention | concurrent workers | RwLock coordinates access |

## What this does not claim

This is a learning system, not a production distributed indexer. It does not currently provide durable offsets, cross-process coordination, persistent storage, Kafka semantics, Redis semantics, ClickHouse ingestion, or Solana/EVM protocol handling.

Those technologies are separate skills to learn and demonstrate only when actually implemented.
