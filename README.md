# Rust Streaming Systems Lab

An independent Rust systems-engineering lab built to learn and demonstrate the mechanics behind continuously running, high-throughput services.

This is **not** a copy of an Axiom system and does not claim prior production experience with Axiom, Solana, Kafka, Redis, ClickHouse, or PostgreSQL. It is a portfolio project that demonstrates transferable systems fundamentals honestly.

## What it demonstrates

- async Rust with Tokio
- bounded mpsc channels and explicit backpressure
- concurrent worker pools
- shared in-memory state with RwLock
- deterministic retry/failure injection
- graceful producer/worker shutdown
- ownership-safe task spawning
- atomic runtime metrics
- unit and integration-style async tests
- a Criterion benchmark target
- CI with formatting, linting, tests, and a release build

## Architecture

    event source
        |
        v
    bounded Tokio mpsc queue  <--- backpressure boundary
        |
        +---- worker 0 ----+
        +---- worker 1 ----+----> concurrent state store
        +---- worker 2 ----+
        +---- worker N ----+
                  |
                  +---- deterministic retry path
                  |
                  +---- metrics/report

The queue is intentionally bounded. Tokio's documentation emphasizes that concurrency and queuing should be explicitly bounded so a producer cannot grow an unbounded backlog and exhaust memory. This lab follows that principle.

## Run

    cargo run --release

The binary emits a JSON report containing submitted/processed events, retries, observed outstanding queue depth, and final key count.

## Test

    cargo fmt --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets --all-features

## Benchmark

    cargo bench

## Why this project exists

The goal is to build hands-on evidence of systems thinking: where work is queued, how concurrency is bounded, how shared state is protected, what happens when work fails, and how the service shuts down.

The design is deliberately small enough to audit end-to-end rather than hiding behavior behind a framework.

## Open-source learning notes

The project uses Tokio rather than reimplementing an async runtime. Its design is informed by the public Tokio tutorials on channels, streams, bounded queues, and asynchronous task scheduling. The implementation in this repository is original portfolio code.

Tokio's public channel tutorial covers bounded channels and backpressure; its streams tutorial covers asynchronous streams and adapters.

## Portfolio honesty

This repository should be described as:

> Independent Rust systems engineering project exploring async streaming, bounded backpressure, concurrent state, retries, and fault handling.

It should **not** be described as previous production experience with technologies or companies that are not actually represented here.
