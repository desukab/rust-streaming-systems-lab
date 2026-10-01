use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::partitioned::stable_partition;
use crate::runtime::{RuntimeConfig, RuntimeSnapshot, RuntimeStatus, StreamingRuntime, SubmitError};
use crate::{Event, EventKind};

#[derive(Clone)]
struct AppState {
    runtime: Arc<StreamingRuntime>,
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery { q: String }

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
    runtime: RuntimeSnapshot,
}

pub fn router() -> Router {
    router_with_config(RuntimeConfig::default())
}

pub fn router_with_config(config: RuntimeConfig) -> Router {
    let runtime = StreamingRuntime::start(config);
    let app = AppState {
        runtime,
        next_id: Arc::new(AtomicU64::new(1)),
    };

    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/partitions", get(partitions))
        .route("/events", post(ingest))
        .route("/record/{key}", get(record))
        .route("/search", get(search))
        .route("/ws", get(websocket))
        .with_state(app)
}

async fn health(State(app): State<AppState>) -> Json<Health> {
    let runtime = app.runtime.snapshot().await;
    Json(Health { status: "ok", runtime })
}

async fn ready(State(app): State<AppState>) -> impl IntoResponse {
    let snapshot = app.runtime.snapshot().await;
    if snapshot.status == RuntimeStatus::Running {
        (StatusCode::OK, Json(snapshot))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(snapshot))
    }
}

async fn metrics(State(app): State<AppState>) -> Json<RuntimeSnapshot> {
    Json(app.runtime.snapshot().await)
}

async fn partitions(State(app): State<AppState>) -> Json<serde_json::Value> {
    let snapshot = app.runtime.snapshot().await;
    Json(serde_json::json!({
        "partitions": snapshot.partitions,
        "queue_capacity": snapshot.queue_capacity,
        "queue_depth": snapshot.queue_depth,
        "peak_queue_depth": snapshot.peak_queue_depth
    }))
}

async fn ingest(State(app): State<AppState>, Json(input): Json<NewEvent>) -> impl IntoResponse {
    let id = app.next_id.fetch_add(1, Ordering::Relaxed);
    let event = Event {
        id,
        partition: stable_partition(&input.key, app.runtime.snapshot().await.partitions) as u16,
        sequence: id,
        kind: input.kind.unwrap_or(EventKind::Insert),
        key: input.key,
        payload: input.payload,
    };

    match app.runtime.submit_and_wait(event.clone()).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(event)).into_response(),
        Err(SubmitError::Draining | SubmitError::Closed) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "ingestion unavailable"})),
        ).into_response(),
    }
}

async fn record(State(app): State<AppState>, Path(key): Path<String>) -> impl IntoResponse {
    match app.runtime.record(&key).await {
        Some(value) => (StatusCode::OK, Json(serde_json::json!({"key": key, "value": value}))).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "record not found"}))).into_response(),
    }
}

async fn search(State(app): State<AppState>, Query(query): Query<SearchQuery>) -> Json<Vec<String>> {
    Json(app.runtime.search(&query.q).await)
}

async fn websocket(ws: WebSocketUpgrade, State(app): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_session(socket, app.runtime.subscribe()))
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
    async fn health_endpoint_reports_runtime() {
        let response = router()
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_endpoint_is_live() {
        let response = router()
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
