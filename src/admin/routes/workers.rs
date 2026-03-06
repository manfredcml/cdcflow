use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

use crate::admin::AdminState;
use crate::worker_http::registration::{DeregisterRequest, HeartbeatRequest, RegisterRequest};

pub async fn list_workers(State(state): State<AdminState>) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.list_workers().await {
        Ok(workers) => (StatusCode::OK, Json(serde_json::json!(workers))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn register_worker(
    State(state): State<AdminState>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db
        .register_worker(
            &req.worker_id,
            &req.job_id,
            &req.address,
            &req.hostname,
            req.pid,
        )
        .await
    {
        Ok(worker) => (StatusCode::OK, Json(serde_json::json!(worker))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn heartbeat_worker(
    State(state): State<AdminState>,
    Json(req): Json<HeartbeatRequest>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    if let Err(e) = db.heartbeat_worker(&req.worker_id).await {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    if let Err(e) = db
        .insert_metrics(&req.worker_id, &req.metrics, "running")
        .await
    {
        tracing::warn!("failed to store metrics: {e}");
    }

    (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
}

pub async fn deregister_worker(
    State(state): State<AdminState>,
    Json(req): Json<DeregisterRequest>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.deregister_worker(&req.worker_id).await {
        Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn stop_worker(
    State(_state): State<AdminState>,
    Path(worker_id): Path<String>,
) -> impl IntoResponse {
    // Look up worker address and proxy the stop command
    let db = _state.db.lock().await;
    let worker = match db.get_worker(&worker_id).await {
        Ok(w) => w,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    drop(db); // Release lock before making HTTP call

    let client = reqwest::Client::new();
    let url = format!("http://{}/control/stop", worker.address);
    match client.post(&url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let db = _state.db.lock().await;
            let _ = db.deregister_worker(&worker_id).await;
            (
                StatusCode::OK,
                Json(serde_json::json!({"status": "stop sent"})),
            )
                .into_response()
        }
        Ok(resp) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("worker returned {}", resp.status())})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": format!("failed to reach worker: {e}")})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::sqlite::SqliteAdminStore;
    use crate::admin::store::CreateJobRequest;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::{get, post};
    use axum::Router;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state() -> AdminState {
        let db = SqliteAdminStore::open(":memory:").unwrap();
        AdminState {
            db: Arc::new(tokio::sync::Mutex::new(db)),
        }
    }

    fn test_router(state: AdminState) -> Router {
        Router::new()
            .route("/api/workers", get(list_workers))
            .route("/api/workers/register", post(register_worker))
            .route("/api/workers/heartbeat", post(heartbeat_worker))
            .route("/api/workers/deregister", post(deregister_worker))
            .route("/api/workers/{id}/stop", post(stop_worker))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let state = test_state();
        {
            let db = state.db.lock().await;
            let job = db
                .create_job(&CreateJobRequest {
                    name: "job".into(),
                    config_json: "{}".into(),
                    description: String::new(),
                })
                .await
                .unwrap();
            db.register_worker("w1", &job.id, "addr", "h", 1)
                .await
                .unwrap();
        }

        let app = test_router(state.clone());
        let body = serde_json::json!({
            "worker_id": "w1",
            "metrics": {
                "events_total": 50,
                "events_per_sec": 5.0,
                "batches_total": 3,
                "last_batch_size": 10,
                "errors_total": 0,
                "last_flush_at": 0,
                "uptime_secs": 10
            }
        });

        let resp = app
            .oneshot(
                Request::post("/api/workers/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify metrics were stored
        let db = state.db.lock().await;
        let latest = db.get_latest_metrics("w1").await.unwrap().unwrap();
        assert_eq!(latest.events_total, 50);
    }
}
