use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use crate::admin::AdminState;

#[derive(Deserialize)]
pub struct MetricsQuery {
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    100
}

pub async fn get_metrics_history(
    State(state): State<AdminState>,
    Path(worker_id): Path<String>,
    Query(query): Query<MetricsQuery>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.get_metrics_history(&worker_id, query.limit).await {
        Ok(metrics) => (StatusCode::OK, Json(serde_json::json!(metrics))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn get_latest_metrics(
    State(state): State<AdminState>,
    Path(worker_id): Path<String>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.get_latest_metrics(&worker_id).await {
        Ok(Some(m)) => (StatusCode::OK, Json(serde_json::json!(m))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no metrics found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::sqlite::SqliteAdminStore;
    use crate::admin::store::{AdminStore, CreateJobRequest};
    use crate::metrics::MetricsSnapshot;
    use std::sync::Arc;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    async fn test_state_with_data() -> AdminState {
        let db = SqliteAdminStore::open(":memory:").unwrap();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1).await.unwrap();
        for i in 0..5 {
            let snap = MetricsSnapshot {
                events_total: i * 10,
                events_per_sec: 0.0,
                batches_total: i,
                last_batch_size: 0,
                errors_total: 0,
                last_flush_at: 0,
                uptime_secs: i,
            };
            db.insert_metrics("w1", &snap, "running").await.unwrap();
        }
        AdminState {
            db: Arc::new(tokio::sync::Mutex::new(db)),
        }
    }

    fn test_router(state: AdminState) -> Router {
        Router::new()
            .route("/api/metrics/{worker_id}", get(get_metrics_history))
            .route("/api/metrics/{worker_id}/latest", get(get_latest_metrics))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_get_latest_metrics_not_found() {
        let state = test_state_with_data().await;
        let app = test_router(state);

        let resp = app
            .oneshot(
                Request::get("/api/metrics/nonexistent/latest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
