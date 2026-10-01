# Durability and recovery

The lab now models a basic write-ahead-log/checkpoint pattern without claiming production durability semantics.

## Flow

    producer
       |
       v
    append-only event log
       |
       v
    partition queue
       |
       v
    materialized state
       |
       +--> partition checkpoint

The event log is newline-delimited JSON. A checkpoint records the latest applied
sequence for one partition.

## Recovery model

1. Open the event log.
2. Load the checkpoint for each partition.
3. Replay log records whose partition sequence is newer than the checkpoint.
4. Feed those events through the same state-application path.
5. Write a new checkpoint after successful application.

The repository currently provides the durable primitives and round-trip tests.
It deliberately does not claim crash-consistent multi-file transactions,
distributed consensus, dynamic partition migration, or exactly-once delivery.

Those are separate engineering problems that require stronger storage and
failure semantics than this small lab needs.
