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
            payload: format!("program={} amount={} slot={}", self.program, self.amount, self.slot),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkloadConfig {
    pub blocks: u64,
    pub events_per_block: u64,
    pub accounts: u64,
    pub hot_key_ratio: u8,
    pub burst_size: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            blocks: 100,
            events_per_block: 100,
            accounts: 10_000,
            hot_key_ratio: 10,
            burst_size: 100,
        }
    }
}

/// Generate a reproducible event stream with configurable key skew and bursts.
///
/// This is intentionally a synthetic workload, not an implementation of a
/// particular chain protocol.
pub fn generate_configured(config: WorkloadConfig) -> Vec<ChainEvent> {
    assert!(config.accounts > 0);
    assert!(config.hot_key_ratio <= 100);

    let total = config.blocks.saturating_mul(config.events_per_block);
    let mut events = Vec::with_capacity(total as usize);

    for slot in 0..config.blocks {
        for offset in 0..config.events_per_block {
            let transaction = slot * config.events_per_block + offset;
            let burst_offset = if config.burst_size == 0 {
                0
            } else {
                (transaction / config.burst_size) % config.accounts
            };

            let hot = transaction % 100 < u64::from(config.hot_key_ratio);
            let account_number = if hot {
                burst_offset
            } else {
                transaction % config.accounts
            };

            let program = match transaction % 3 {
                0 => "swap",
                1 => "transfer",
                _ => "stake",
            };

            events.push(ChainEvent {
                slot,
                transaction,
                account: format!("account-{account_number}"),
                program: program.into(),
                amount: (transaction * 7919) % 1_000_000,
            });
        }
    }

    events
}

pub fn generate(blocks: u64, events_per_block: u64) -> Vec<ChainEvent> {
    generate_configured(WorkloadConfig {
        blocks,
        events_per_block,
        ..WorkloadConfig::default()
    })
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

    #[test]
    fn hot_key_configuration_creates_skew() {
        let events = generate_configured(WorkloadConfig {
            blocks: 10,
            events_per_block: 100,
            accounts: 100,
            hot_key_ratio: 80,
            burst_size: 10,
        });
        let hot = events.iter().filter(|event| event.account == "account-0").count();
        assert!(hot > 100);
    }
}
