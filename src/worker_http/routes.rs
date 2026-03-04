use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::metrics::PipelineMetrics;

/// Shared state for all worker HTTP routes.
#[derive(Clone)]
pub struct WorkerState {
    pub metrics: Arc<PipelineMetrics>,
    pub shutdown: CancellationToken,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct StopResponse {
    message: &'static str,
}

pub async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

pub async fn metrics(State(state): State<WorkerState>) -> impl IntoResponse {
    Json(state.metrics.snapshot())
}

pub async fn stop(State(state): State<WorkerState>) -> impl IntoResponse {
    state.shutdown.cancel();
    (StatusCode::OK, Json(StopResponse { message: "shutdown initiated" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::Router;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    fn test_router() -> Router {
        let metrics = PipelineMetrics::new();
        let shutdown = CancellationToken::new();
        let state = WorkerState { metrics, shutdown };
        Router::new()
            .route("/health", get(health))
            .route("/metrics", get(super::metrics))
            .route("/control/stop", post(stop))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = test_router();
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let metrics = PipelineMetrics::new();
        metrics.record_batch(42);
        let shutdown = CancellationToken::new();
        let state = WorkerState { metrics, shutdown };
        let app = Router::new()
            .route("/metrics", get(super::metrics))
            .with_state(state);

        let resp = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["events_total"], 42);
        assert_eq!(json["batches_total"], 1);
    }

    #[tokio::test]
    async fn test_stop_endpoint() {
        let metrics = PipelineMetrics::new();
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let state = WorkerState { metrics, shutdown };
        let app = Router::new()
            .route("/control/stop", post(stop))
            .with_state(state);

        assert!(!shutdown_clone.is_cancelled());

        let resp = app
            .oneshot(Request::post("/control/stop").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(shutdown_clone.is_cancelled());
    }
}
