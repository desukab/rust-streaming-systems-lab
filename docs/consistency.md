# Consistency and event semantics

The engine intentionally makes its guarantees explicit.

## Partition ownership

For a fixed partition count, a key is routed to exactly one partition using deterministic FNV-1a hashing. Each partition has one consumer.

This gives partition-local ordering and avoids a global lock around event processing.

Changing the partition count changes the hash mapping. Dynamic rebalancing is therefore a separate migration problem.

## Event identity

Event.id is treated as the identity of a delivery. Within a partition, repeated delivery of the same ID is ignored after the first successful application.

This is idempotency at the materialized-state boundary. It is not a claim of distributed exactly-once delivery.

## Sequence ordering

Each key tracks its highest applied sequence.

An event whose sequence is older than the current sequence is considered stale and does not overwrite newer state.

Equal sequences are accepted, which keeps the rule deterministic but means sequence uniqueness is still an upstream responsibility.

## Recovery

The durable event log provides replayable input and checkpoints provide a recovery position. The current recovery experiment reconstructs state through the same normal processing path.

The implementation does not claim atomic distributed transactions between the event log, checkpoint store, and materialized state.

## Shutdown and backpressure

Queues are bounded. Producers awaiting a full queue provide natural backpressure rather than allowing unbounded buffering.

A graceful service should stop admission, drain accepted work, flush durable state, and then terminate. The current batch engine provides the primitives; the next service iteration can make lifecycle states explicit.