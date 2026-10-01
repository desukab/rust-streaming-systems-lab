# Architecture Notes

## Invariants

1. **The input queue is bounded.** A producer must wait when the channel is full.
2. **Worker ownership is explicit.** Each task receives an owned event and does not borrow data across an await boundary.
3. **Shared state is synchronized.** The state store uses an async RwLock so readers do not race writers.
4. **Failure injection is deterministic.** Sequence numbers divisible by 97 exercise retries, making tests reproducible.
5. **Shutdown is observable.** Closing the sender drains the worker queue; the long-running TCP example additionally handles Ctrl-C.
6. **Metrics are atomic.** Counters do not require a global lock.

## Important trade-off

The basic Pipeline intentionally does not guarantee per-key ordering when multiple workers process the same key concurrently. A production indexer often needs partition-aware routing so all events for a key or partition are serialized while unrelated partitions remain concurrent.

That is an intentional next experiment, not an undocumented guarantee.

## Why bounded queues matter

A fast producer and slow consumer create a queueing problem. An unbounded queue converts overload into memory growth. A bounded queue converts overload into backpressure: the producer waits and the system exposes pressure at a controlled boundary.

## Next systems experiment

Implement partition-aware workers:

    source
      |
      v
    partition(key)
      |
      +--> partition worker 0
      +--> partition worker 1
      +--> ...
      |
      v
    state

The goal is to preserve ordering within a partition without forcing the entire service to become single-threaded.
