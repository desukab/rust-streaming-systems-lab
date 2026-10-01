use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use rust_streaming_systems_lab::{Event, EventKind, IndexedState};

#[derive(Clone)]
struct AppState {
    state: Arc<IndexedState>,
    next_id: Arc<AtomicU64>,
    events: broadcast::Sender<Event>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
}

#[derive(Debug, Deserialize)]
struct NewEvent {
    key: String,
    payload: String,
    #[serde(default)]
    kind: Option<EventKind>,
}

#[derive(Debug, Serialize)]
struct Health {
    status: &'static str,
    events: u64,
    keys: usize,
}

pub fn router() -> Router {
    let (events, _) = broadcast::channel(1024);
    let state = AppState {
        state: Arc::new(IndexedState::default()),
        next_id: Arc::new(AtomicU64::new(1)),
        events,
    };

    Router::new()
        .route("/health", get(health))
        .route("/events", post(ingest))
        .route("/record/{key}", get(record))
        .route("/search", get(search))
        .route("/ws", get(websocket))
        .with_state(state)
}

async fn health(State(app): State<AppState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        events: app.next_id.load(Ordering::Relaxed).saturating_sub(1),
        keys: app.state.len().await,
    })
}

async fn ingest(State(app): State<AppState>, Json(input): Json<NewEvent>) -> impl IntoResponse {
    let id = app.next_id.fetch_add(1, Ordering::Relaxed);
    let event = Event {
        id,
        partition: 0,
        sequence: id,
        kind: input.kind.unwrap_or(EventKind::Insert),
        key: input.key,
        payload: input.payload,
    };

    app.state.apply(&event).await;
    let _ = app.events.send(event.clone());

    (StatusCode::ACCEPTED, Json(event))
}

async fn record(State(app): State<AppState>, Path(key): Path<String>) -> impl IntoResponse {
    match app.state.get(&key).await {
        Some(value) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "key": key,
                "value": value,
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "record not found",
            })),
        ),
    }
}

async fn search(
    State(app): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Json<Vec<String>> {
    Json(app.state.search(&query.q).await)
}

async fn websocket(ws: WebSocketUpgrade, State(app): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_session(socket, app.events.subscribe()))
}

async fn websocket_session(mut socket: WebSocket, mut events: broadcast::Receiver<Event>) {
    while let Ok(event) = events.recv().await {
        let payload = match serde_json::to_string(&event) {
            Ok(payload) => payload,
            Err(_) => break,
        };

        if socket.send(Message::Text(payload.into())).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_endpoint_reports_empty_store() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
