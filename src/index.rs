use std::collections::{HashMap, HashSet};

use tokio::sync::RwLock;

use crate::types::{Event, EventKind};

/// A small materialized view plus inverted index.
///
/// The write lock covers both structures so a reader never observes a record
/// without its corresponding index entry (or vice versa).
#[derive(Debug, Default)]
pub struct IndexedState {
    records: RwLock<HashMap<String, String>>,
    inverted: RwLock<HashMap<String, HashSet<String>>>,
}

impl IndexedState {
    pub async fn apply(&self, event: &Event) {
        let mut records = self.records.write().await;
        let mut inverted = self.inverted.write().await;

        match event.kind {
            EventKind::Insert | EventKind::Update => {
                if let Some(old) = records.insert(event.key.clone(), event.payload.clone()) {
                    for token in tokens(&old) {
                        remove_posting(&mut inverted, &token, &event.key);
                    }
                }

                for token in tokens(&event.payload) {
                    inverted.entry(token).or_default().insert(event.key.clone());
                }
            }
            EventKind::Delete => {
                if let Some(old) = records.remove(&event.key) {
                    for token in tokens(&old) {
                        remove_posting(&mut inverted, &token, &event.key);
                    }
                }
            }
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        self.records.read().await.get(key).cloned()
    }

    pub async fn search(&self, term: &str) -> Vec<String> {
        let token = normalize(term);
        let postings = {
            let index = self.inverted.read().await;
            index.get(&token).cloned().unwrap_or_default()
        };

        let records = self.records.read().await;
        let mut hits: Vec<String> = postings
            .into_iter()
            .filter(|key| records.contains_key(key))
            .collect();
        hits.sort();
        hits
    }

    pub async fn len(&self) -> usize {
        self.records.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.records.read().await.is_empty()
    }
}

fn tokens(value: &str) -> impl Iterator<Item = String> + '_ {
    value
        .split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(normalize)
        .filter(|part| part.len() >= 2)
}

fn normalize(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn remove_posting(inverted: &mut HashMap<String, HashSet<String>>, token: &str, key: &str) {
    if let Some(postings) = inverted.get_mut(token) {
        postings.remove(key);
        if postings.is_empty() {
            inverted.remove(token);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Event, EventKind};

    fn event(id: u64, kind: EventKind, key: &str, payload: &str) -> Event {
        Event {
            id,
            partition: 0,
            sequence: id,
            kind,
            key: key.into(),
            payload: payload.into(),
        }
    }

    #[tokio::test]
    async fn update_replaces_old_index_entries() {
        let state = IndexedState::default();
        state
            .apply(&event(1, EventKind::Insert, "a", "Rust Tokio"))
            .await;

        let update = event(2, EventKind::Update, "a", "Rust streams");
        state.apply(&update).await;

        assert!(state.search("tokio").await.is_empty());
        assert_eq!(state.search("streams").await, vec!["a"]);
    }

    #[tokio::test]
    async fn delete_removes_record_and_postings() {
        let state = IndexedState::default();
        state
            .apply(&event(1, EventKind::Insert, "a", "bounded queue"))
            .await;

        state.apply(&event(2, EventKind::Delete, "a", "")).await;

        assert!(state.get("a").await.is_none());
        assert!(state.search("bounded").await.is_empty());
    }
}
