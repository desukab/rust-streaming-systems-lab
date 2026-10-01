use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use rust_streaming_systems_lab::{Event, EventKind, StateStore};

async fn handle_connection(
    socket: TcpStream,
    tx: mpsc::Sender<Event>,
    next_id: Arc<tokio::sync::Mutex<u64>>,
) {
    let peer = socket.peer_addr().ok();
    let mut lines = BufReader::new(socket).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let id = {
            let mut guard = next_id.lock().await;
            let id = *guard;
            *guard += 1;
            id
        };

        let event = Event {
            id,
            partition: (id % 16) as u16,
            sequence: id + 1,
            kind: EventKind::Insert,
            key: line.split('=').next().unwrap_or("unknown").to_owned(),
            payload: line,
        };

        // A full channel makes the connection wait. This is the service-level
        // backpressure boundary: clients cannot create an unbounded heap queue.
        if tx.send(event).await.is_err() {
            break;
        }
    }

    info!(?peer, "connection closed");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .init();

    let listener = TcpListener::bind("127.0.0.1:7000").await?;
    let (tx, mut rx) = mpsc::channel::<Event>(256);
    let state = Arc::new(StateStore::default());
    let next_id = Arc::new(tokio::sync::Mutex::new(0_u64));

    let worker_state = Arc::clone(&state);
    let worker = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            worker_state.apply(&event).await;
            sleep(Duration::from_micros(50)).await;
        }
    });

    info!("line ingestion service listening on 127.0.0.1:7000");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((socket, _)) => {
                        let tx = tx.clone();
                        let next_id = Arc::clone(&next_id);
                        tokio::spawn(handle_connection(socket, tx, next_id));
                    }
                    Err(error) => warn!(%error, "accept failed"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown requested");
                break;
            }
        }
    }

    drop(tx);
    worker.await?;
    info!(keys = state.len().await, "service stopped");
    Ok(())
}
