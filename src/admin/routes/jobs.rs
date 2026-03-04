use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

use crate::admin::store::{CreateJobRequest, UpdateJobRequest};
use crate::admin::AdminState;

pub async fn list_jobs(State(state): State<AdminState>) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.list_jobs().await {
        Ok(jobs) => (StatusCode::OK, Json(serde_json::json!(jobs))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

pub async fn create_job(
    State(state): State<AdminState>,
    Json(req): Json<CreateJobRequest>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.create_job(&req).await {
        Ok(job) => (StatusCode::CREATED, Json(serde_json::json!(job))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

pub async fn get_job(
    State(state): State<AdminState>,
    Path(id_or_name): Path<String>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    // Try by name first, then by id
    let job = match db.get_job_by_name(&id_or_name).await {
        Ok(j) => Ok(j),
        Err(_) => db.get_job(&id_or_name).await,
    };
    match job {
        Ok(job) => (StatusCode::OK, Json(serde_json::json!(job))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

pub async fn get_job_config(
    State(state): State<AdminState>,
    Path(id_or_name): Path<String>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    let job = match db.get_job_by_name(&id_or_name).await {
        Ok(j) => Ok(j),
        Err(_) => db.get_job(&id_or_name).await,
    };
    match job {
        Ok(job) => {
            // Return the raw config JSON
            match serde_json::from_str::<serde_json::Value>(&job.config_json) {
                Ok(config) => (StatusCode::OK, Json(config)).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": format!("invalid stored config: {e}")}))).into_response(),
            }
        }
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

pub async fn update_job(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateJobRequest>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    match db.update_job(&id, &req).await {
        Ok(job) => (StatusCode::OK, Json(serde_json::json!(job))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

pub async fn delete_job(
    State(state): State<AdminState>,
    Path(id_or_name): Path<String>,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    let job = match db.get_job_by_name(&id_or_name).await {
        Ok(j) => Ok(j),
        Err(_) => db.get_job(&id_or_name).await,
    };
    match job {
        Ok(job) => match db.delete_job(&job.id).await {
            Ok(()) => (StatusCode::OK, Json(serde_json::json!({"status": "deleted"}))).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
        },
        Err(e) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::sqlite::SqliteAdminStore;
    use std::sync::Arc;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn test_state() -> AdminState {
        let db = SqliteAdminStore::open(":memory:").unwrap();
        AdminState {
            db: Arc::new(tokio::sync::Mutex::new(db)),
        }
    }

    fn test_router(state: AdminState) -> Router {
        Router::new()
            .route("/api/jobs", get(list_jobs).post(create_job))
            .route("/api/jobs/{id}", get(get_job).put(update_job).delete(delete_job))
            .route("/api/jobs/{id}/config", get(get_job_config))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_get_job_config() {
        let state = test_state();
        let app = test_router(state.clone());

        // Create job
        let resp = app.clone()
            .oneshot(
                Request::post("/api/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"my-pipeline","config_json":"{\"key\":\"val\"}"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Get config by name
        let app2 = test_router(state);
        let resp = app2
            .oneshot(
                Request::get("/api/jobs/my-pipeline/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let config: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(config["key"], "val");
    }

}
