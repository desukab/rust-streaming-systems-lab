# Partition-Aware Stream Processing

The original pipeline intentionally used a shared receiver. That is useful for learning worker concurrency, but it cannot provide per-key ordering when several workers can process the same logical key concurrently.

The partitioned engine changes the topology:

    producer
       |
       +---- hash(key) ---> partition 0 ---> worker 0
       |
       +---- hash(key) ---> partition 1 ---> worker 1
       |
       +---- hash(key) ---> partition N ---> worker N
                                  |
                                  v
                           materialized state
                                  |
                                  v
                           inverted index

## Invariants

- A key maps to one partition for a fixed partition count.
- A partition has one consumer.
- Events in one partition are applied in channel receive order.
- Different partitions can make progress concurrently.
- Each partition has its own bounded queue.
- The materialized view and inverted index are updated together under coordinated locks.

This is a simplified form of a common stream-processing design: **partition for locality, serialize within the partition, parallelize across partitions**.

## Why this matters

A production service often cannot simply maximize the number of workers. If two updates for the same key are processed concurrently, the later event can be overwritten by the older one.

Partitioning gives us a place to make an explicit correctness/performance trade-off.

## Rebalancing limitation

This implementation uses a fixed partition count and deterministic hashing. It does not yet support dynamic partition ownership or consistent-hash migration.

That is deliberate. Rebalancing is a separate distributed-systems problem involving ownership, state transfer, checkpoints and failure recovery.
