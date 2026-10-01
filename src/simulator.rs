use crate::types::{Event, EventKind};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ChainEvent {
    pub slot: u64,
    pub transaction: u64,
    pub account: String,
    pub program: String,
    pub amount: u64,
}

impl ChainEvent {
    pub fn into_event(self) -> Event {
        Event {
            id: self.transaction,
            partition: 0,
            sequence: self.slot,
            kind: EventKind::Update,
            key: self.account,
            payload: format!(
                "program={} amount={} slot={}",
                self.program, self.amount, self.slot
            ),
        }
    }
}

/// Deterministic synthetic workload shaped like an on-chain event stream.
///
/// This is a simulator, not a Solana/EVM implementation. It exists to make
/// ordering, partitioning, indexing, and burst behavior reproducible.
pub fn generate(blocks: u64, events_per_block: u64) -> Vec<ChainEvent> {
    let mut events = Vec::with_capacity((blocks * events_per_block) as usize);

    for slot in 0..blocks {
        for offset in 0..events_per_block {
            let transaction = slot * events_per_block + offset;
            events.push(ChainEvent {
                slot,
                transaction,
                account: format!("account-{}", transaction % 10_000),
                program: if transaction % 3 == 0 {
                    "swap".into()
                } else if transaction % 3 == 1 {
                    "transfer".into()
                } else {
                    "stake".into()
                },
                amount: (transaction * 7919) % 1_000_000,
            });
        }
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_is_deterministic() {
        assert_eq!(generate(2, 3), generate(2, 3));
    }

    #[test]
    fn conversion_preserves_stream_identity() {
        let event = ChainEvent {
            slot: 42,
            transaction: 99,
            account: "account-1".into(),
            program: "swap".into(),
            amount: 123,
        };

        let converted = event.into_event();
        assert_eq!(converted.id, 99);
        assert_eq!(converted.sequence, 42);
        assert_eq!(converted.key, "account-1");
    }
}
