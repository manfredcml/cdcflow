pub mod jobs;
pub mod metrics;
pub mod workers;

use axum::routing::{get, post};
use axum::Router;

use crate::admin::AdminState;

/// Build the full admin API router.
pub fn admin_router(state: AdminState) -> Router {
    Router::new()
        // Job endpoints
        .route("/api/jobs", get(jobs::list_jobs).post(jobs::create_job))
        .route(
            "/api/jobs/{id}",
            get(jobs::get_job).put(jobs::update_job).delete(jobs::delete_job),
        )
        .route("/api/jobs/{id}/config", get(jobs::get_job_config))
        // Worker endpoints
        .route("/api/workers", get(workers::list_workers))
        .route("/api/workers/register", post(workers::register_worker))
        .route("/api/workers/heartbeat", post(workers::heartbeat_worker))
        .route("/api/workers/deregister", post(workers::deregister_worker))
        .route("/api/workers/{id}/stop", post(workers::stop_worker))
        // Metrics endpoints
        .route("/api/metrics/{worker_id}", get(metrics::get_metrics_history))
        .route("/api/metrics/{worker_id}/latest", get(metrics::get_latest_metrics))
        .with_state(state)
}
