use std::io;
use std::path::{Path, PathBuf};

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

use crate::types::Event;

#[derive(Debug)]
pub struct EventLog {
    path: PathBuf,
    writer: Mutex<tokio::fs::File>,
}

impl EventLog {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            writer: Mutex::new(writer),
        })
    }

    pub async fn append(&self, event: &Event) -> io::Result<()> {
        let mut line = serde_json::to_vec(event)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&line).await?;
        writer.flush().await
    }

    pub async fn replay(&self) -> io::Result<Vec<Event>> {
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        let mut lines = BufReader::new(file).lines();
        let mut events = Vec::new();

        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<Event>(&line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            events.push(event);
        }

        Ok(events)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub partition: usize,
    pub sequence: u64,
}

#[derive(Debug, Clone)]
pub struct CheckpointStore {
    directory: PathBuf,
}

impl CheckpointStore {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let directory = path.as_ref().to_path_buf();
        fs::create_dir_all(&directory).await?;
        Ok(Self { directory })
    }

    pub async fn save(&self, checkpoint: &Checkpoint) -> io::Result<()> {
        let path = self.path_for(checkpoint.partition);
        let tmp = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(checkpoint)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        fs::write(&tmp, bytes).await?;
        fs::rename(tmp, path).await
    }

    pub async fn load(&self, partition: usize) -> io::Result<Option<Checkpoint>> {
        let path = self.path_for(partition);
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn path_for(&self, partition: usize) -> PathBuf {
        self.directory.join(format!("partition-{partition}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EventKind, Event};

    fn event(id: u64) -> Event {
        Event {
            id,
            partition: 0,
            sequence: id,
            kind: EventKind::Insert,
            key: format!("k-{id}"),
            payload: format!("v-{id}"),
        }
    }

    #[tokio::test]
    async fn log_round_trips_events() {
        let dir = tempfile::tempdir().unwrap();
        let log = EventLog::open(dir.path().join("events.log")).await.unwrap();

        log.append(&event(1)).await.unwrap();
        log.append(&event(2)).await.unwrap();

        let replayed = log.replay().await.unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[1].id, 2);
    }

    #[tokio::test]
    async fn checkpoint_is_atomic_and_loadable() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::open(dir.path()).await.unwrap();
        let checkpoint = Checkpoint {
            partition: 3,
            sequence: 99,
        };

        store.save(&checkpoint).await.unwrap();

        assert_eq!(store.load(3).await.unwrap(), Some(checkpoint));
    }
}
