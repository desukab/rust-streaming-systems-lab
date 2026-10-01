# Rust Streaming Systems Lab

An independent Rust systems-engineering lab built to learn and demonstrate the mechanics behind continuously running, high-throughput services.

This is **not** a copy of an Axiom system and does not claim prior production experience with Axiom, Solana, Kafka, Redis, ClickHouse, or PostgreSQL. It is a portfolio project that demonstrates transferable systems fundamentals honestly.

## What it demonstrates

- async Rust with Tokio
- bounded message channels and explicit backpressure
- fixed-key partitioning with deterministic FNV-1a routing
- one ordered consumer per partition with cross-partition concurrency
- concurrent materialized state using RwLock
- an in-memory inverted index with update/delete consistency
- deterministic retry/failure injection
- append-only JSON event log and atomic checkpoint files
- graceful producer/worker shutdown
- ownership-safe task spawning
- atomic runtime metrics and queue-depth accounting primitives
- unit and integration-style async tests
- Criterion throughput benchmarks
- CI with formatting, linting, tests, and a release build

## Architecture

    event source
        |
        +---- append-only event log
        |
        v
    stable_partition(key)
        |
        +---- bounded queue ---- worker 0 ----+
        +---- bounded queue ---- worker 1 ----+----> materialized state
        +---- bounded queue ---- worker N ----+          |
                                                     inverted index
                                                          |
                                                     checkpoint

The queue is intentionally bounded. A producer that reaches capacity must
await the channel instead of creating an unbounded heap backlog.

The partition boundary is a deliberate consistency/concurrency trade-off:
events for one logical key are routed to one partition, giving that key a
single ordered consumer while unrelated keys can be processed concurrently.

## Durability model

EventLog provides an append-only newline-delimited JSON log.

CheckpointStore writes one checkpoint per partition through a temporary file
followed by rename. Together they provide the primitives for replay-based
recovery:

1. append events to the log;
2. process them through the normal partition path;
3. record the latest applied sequence per partition;
4. after restart, replay records newer than the checkpoint.

See docs/recovery.md.

This is intentionally **not** presented as a production WAL. The lab does not
claim distributed consensus, exactly-once semantics, crash-consistent
multi-file transactions, dynamic partition rebalancing, or state migration.

## Queryable state

IndexedState maintains:

- a key/value materialized view;
- a token to keys inverted index.

Updates remove stale postings before adding new ones. Deletes remove both the
record and its postings, so queries do not return deleted state.

## Run

    cargo run --release

The binary emits a JSON report containing submitted/processed events, retries,
observed outstanding queue depth, and final key count.

## Long-running service example

A small TCP ingestion service shows the bounded-queue idea at a service
boundary:

    cargo run --example line_server

It listens on 127.0.0.1:7000, accepts newline-delimited records, applies
backpressure when its bounded channel is full, and shuts down cleanly on
Ctrl-C.

## Test

    cargo fmt --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets --all-features
    cargo build --release

## Benchmark

    cargo bench

The partitioned benchmark drives 20,000 events through 16 partitions with a
bounded queue per partition. Benchmark numbers should be generated locally
with cargo bench; no unmeasured throughput claim is hard-coded into this
README.

## Why this project exists

The goal is to build hands-on evidence of systems thinking: where work is
queued, how concurrency is bounded, how ownership and locks affect state,
what happens when work fails, how state can be rebuilt after restart, and how
the service shuts down.

The design is deliberately small enough to audit end-to-end rather than hiding
behavior behind a framework.

## Portfolio honesty

A good description of this repository is:

> Independent Rust systems engineering project exploring async streaming,
> bounded backpressure, deterministic partitioning, concurrent materialized
> state, indexing, retries, durability primitives, and recovery design.

It should **not** be described as previous production experience with
technologies or companies that are not actually represented here.

## Live query and streaming service

The Axum server turns the library into a small continuously running service:

    cargo run --bin server

Endpoints:

    GET  /health
    POST /events
    GET  /record/{key}
    GET  /search?q=rust
    WS   /ws

POSTing an event updates the materialized state and publishes the same event to connected WebSocket clients. This makes the data path observable end to end instead of leaving the project as a batch-only benchmark.

## Deterministic chain-like workload

`simulator` generates reproducible slot/transaction/account/program events shaped like a blockchain indexing workload. It is explicitly a simulator, not a claim of Solana or EVM implementation.

Run the benchmark with:

    cargo bench --bench simulator

## Recovery experiment

Run:

    cargo run --example recovery_lab

This writes a durable event stream, processes a prefix, checkpoints it, constructs a fresh engine, and replays the remaining stream. See `docs/failure-lab.md`.

## What I would build next

- PostgreSQL-backed persistence with measured query plans
- Redis cache with hit/miss and tail-latency measurements
- Kafka/Redpanda ingestion with explicit offset semantics
- WebSocket fan-out load test
- CPU/allocation profiling and flamegraph analysis
- property-based invariant testing
- dynamic partition migration and checkpoint coordination

These are deliberately listed as next experiments rather than presented as experience the repository does not contain.

## Why this is a useful systems portfolio

The project is designed to answer engineering questions with code and measurements:

- Where does backpressure occur?
- What state is ordered and what state is concurrent?
- What happens when processing fails?
- What survives a restart?
- Which reads stay off the database?
- What consistency guarantees does partitioning actually provide?
- Where does latency come from?

The goal is not to maximize technology keywords. The goal is to make the trade-offs visible, testable, and explainable.

## Long-running runtime

The batch engine is now backed by a separate `StreamingRuntime` that keeps partition workers alive for the lifetime of a service.

The runtime exposes:

- bounded per-partition ingress with natural async backpressure
- deterministic key → partition routing
- one consumer per partition
- idempotent event identity handling
- stale-sequence rejection
- materialized state + inverted search index
- runtime health/readiness/metrics snapshots
- WebSocket event fan-out
- graceful **Running → Draining → Stopped** lifecycle
- end-to-end enqueue-to-apply latency measurement

The HTTP service is therefore not a toy handler that mutates a map directly. Requests enter the same bounded streaming runtime that owns ordering and state transitions.

### Service surface

```
GET  /health
GET  /ready
GET  /metrics
GET  /partitions
POST /events
GET  /record/:key
GET  /search?q=term
WS   /ws
```

Run it with:

```
cargo run --bin server
```

Example ingestion:

```bash
curl -X POST http://127.0.0.1:8080/events \
  -H 'content-type: application/json' \
  -d '{"key":"wallet-42","payload":"swap SOL USDC"}'
```

Then query the same materialized state:

```bash
curl http://127.0.0.1:8080/record/wallet-42
curl 'http://127.0.0.1:8080/search?q=swap'
curl http://127.0.0.1:8080/metrics
```

## Guarantees vs. non-goals

| Property | Current lab |
|---|---|
| Per-key partition locality | Yes, fixed partition count |
| Per-partition sequential consumer | Yes |
| Bounded ingress | Yes |
| Duplicate delivery handling | Yes, by event ID |
| Stale sequence protection | Yes |
| Concurrent partitions | Yes |
| Materialized secondary index | Yes |
| Runtime health/readiness | Yes |
| WebSocket fan-out | Yes |
| Graceful drain | Yes |
| Replayable event log | Yes |
| Checkpoint primitive | Yes |
| Dynamic rebalancing | Not yet |
| Crash-consistent distributed transactions | No |
| Distributed consensus | No |
| Exactly-once delivery | No |
| Production Kafka/Redis/Postgres integration | Not claimed |

This distinction is intentional: the repository demonstrates systems fundamentals without pretending that a local lab is a production distributed system.

## Next engineering experiments

The next layer is deliberately measurable rather than another pile of dependencies:

1. **Load generation** — sustained producers, burst traffic, hot-key skew and controlled overload.
2. **Tail latency** — p50/p95/p99 enqueue-to-apply measurements under different partition counts.
3. **Failure injection** — worker crash, delayed partition, rejected ingress and corrupted checkpoint scenarios.
4. **Property testing** — invariants for ordering, idempotency, index consistency and replay.
5. **Persistence adapter** — benchmark a real PostgreSQL implementation against the in-memory materialized view.
6. **Kafka-compatible ingestion** — explicit offset/ack semantics using a local Redpanda experiment.
7. **Cache experiment** — Redis read-through cache with hit/miss and tail-latency measurements.
8. **Profiling** — CPU, allocation and lock-contention measurements with flamegraphs.

The project will only claim an integration after the integration is actually built, tested and measured.
