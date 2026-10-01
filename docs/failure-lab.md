# Failure and recovery lab

This repository intentionally makes failure behavior inspectable rather than claiming production-grade distributed guarantees.

## Retry behavior

The partitioned engine deterministically retries events whose sequence is divisible by 97. This gives tests and benchmarks a repeatable failure path.

## Backpressure

Every partition has a bounded Tokio channel. When a partition is full, the producer awaits send rather than growing an unbounded queue.

## Durable recovery

`examples/recovery_lab.rs` demonstrates: generate deterministic workload; append events; process a prefix; persist checkpoints; construct a fresh engine; replay the durable log after the checkpoint; process the remaining events.

Run: `cargo run --example recovery_lab`

## Deliberate limits

The lab does not claim exactly-once distributed delivery, crash-consistent transactions across log and state, consensus, automatic partition rebalancing, replicated storage, or production Kafka semantics.

The point is to show what is implemented, what is tested, and what remains an engineering problem.