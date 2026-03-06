//! Integration tests for the admin server.
//!
//! These tests start a real admin HTTP server on an ephemeral port and exercise
//! the full API: job CRUD, worker registration, heartbeat with metrics, config
//! pull, and stop-worker proxy.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use reqwest::Client;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;

use cdcflow::admin::routes::admin_router;
use cdcflow::admin::sqlite::SqliteAdminStore;
use cdcflow::admin::AdminState;

/// Helper: start an admin server on an ephemeral port and return its base URL.
async fn start_admin_server(shutdown: CancellationToken) -> String {
    let db = SqliteAdminStore::open(":memory:").expect("open in-memory db");
    let state = AdminState {
        db: Arc::new(tokio::sync::Mutex::new(db)),
    };

    let app = Router::new()
        .merge(admin_router(state))
        .layer(CorsLayer::permissive());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");

    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown_clone.cancelled().await })
            .await
            .unwrap();
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(30)).await;
    url
}

/// Helper: start a minimal mock worker HTTP server that exposes POST /control/stop.
/// Returns the worker address (e.g. "127.0.0.1:NNNNN") and a flag indicating whether
/// the stop endpoint was called.
async fn start_mock_worker() -> (String, Arc<std::sync::atomic::AtomicBool>) {
    use axum::routing::post;
    use std::sync::atomic::{AtomicBool, Ordering};

    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_clone = stopped.clone();

    let app = Router::new().route(
        "/control/stop",
        post(move || {
            let stopped = stopped_clone.clone();
            async move {
                stopped.store(true, Ordering::Relaxed);
                axum::Json(serde_json::json!({"status": "stopping"}))
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    (addr.to_string(), stopped)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_job_lifecycle() {
    let shutdown = CancellationToken::new();
    let base_url = start_admin_server(shutdown.clone()).await;
    let client = Client::new();

    // 1. List jobs — should be empty.
    let jobs: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(jobs.len(), 0);

    // 2. Create a job.
    let resp = client
        .post(format!("{base_url}/api/jobs"))
        .json(&serde_json::json!({
            "name": "pg-to-iceberg",
            "config_json": "{\"source\":{\"type\":\"stdout\"}}",
            "description": "Test job"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let job: serde_json::Value = resp.json().await.unwrap();
    let job_id = job["id"].as_str().unwrap().to_string();

    // 3. Get job by ID.
    let resp = client
        .get(format!("{base_url}/api/jobs/{job_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let j: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(j["name"], "pg-to-iceberg");

    // 4. Get raw config.
    let resp = client
        .get(format!("{base_url}/api/jobs/{job_id}/config"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 5. Update job.
    let resp = client
        .put(format!("{base_url}/api/jobs/{job_id}"))
        .json(&serde_json::json!({"description": "Updated description"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 6. Delete job.
    let resp = client
        .delete(format!("{base_url}/api/jobs/{job_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 7. Verify it's gone.
    let jobs: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/jobs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(jobs.len(), 0);

    shutdown.cancel();
}

#[tokio::test]
async fn test_worker_registration_heartbeat_and_deregister() {
    let shutdown = CancellationToken::new();
    let base_url = start_admin_server(shutdown.clone()).await;
    let client = Client::new();

    // Create a job first.
    let resp = client
        .post(format!("{base_url}/api/jobs"))
        .json(&serde_json::json!({
            "name": "test-job",
            "config_json": "{}",
        }))
        .send()
        .await
        .unwrap();
    let job: serde_json::Value = resp.json().await.unwrap();
    let job_id = job["id"].as_str().unwrap().to_string();

    // 1. Register a worker.
    let resp = client
        .post(format!("{base_url}/api/workers/register"))
        .json(&serde_json::json!({
            "worker_id": "w-abc-123",
            "job_id": job_id,
            "address": "127.0.0.1:9090",
            "hostname": "test-host",
            "pid": 42
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 2. Verify worker appears in list.
    let workers: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/workers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0]["id"], "w-abc-123");
    assert_eq!(workers[0]["status"], "registered");

    // 3. Send a heartbeat with metrics.
    let resp = client
        .post(format!("{base_url}/api/workers/heartbeat"))
        .json(&serde_json::json!({
            "worker_id": "w-abc-123",
            "metrics": {
                "events_total": 100,
                "events_per_sec": 10.0,
                "batches_total": 5,
                "last_batch_size": 20,
                "errors_total": 1,
                "last_flush_at": 0,
                "uptime_secs": 60
            }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 4. Verify metrics were stored.
    let latest: serde_json::Value = client
        .get(format!("{base_url}/api/metrics/w-abc-123/latest"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(latest["events_total"], 100);
    assert_eq!(latest["errors_total"], 1);

    // 5. Check metrics history.
    let history: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/metrics/w-abc-123?limit=10"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history.len(), 1);

    // 6. Deregister.
    let resp = client
        .post(format!("{base_url}/api/workers/deregister"))
        .json(&serde_json::json!({"worker_id": "w-abc-123"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 7. Verify worker is stopped.
    let workers: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/workers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(workers[0]["status"], "stopped");

    shutdown.cancel();
}

#[tokio::test]
async fn test_stop_worker_proxy() {
    let shutdown = CancellationToken::new();
    let base_url = start_admin_server(shutdown.clone()).await;
    let client = Client::new();

    // Start a mock worker server.
    let (worker_addr, stopped_flag) = start_mock_worker().await;

    // Create a job and register the worker at the mock address.
    let resp = client
        .post(format!("{base_url}/api/jobs"))
        .json(&serde_json::json!({
            "name": "proxy-test-job",
            "config_json": "{}",
        }))
        .send()
        .await
        .unwrap();
    let job: serde_json::Value = resp.json().await.unwrap();
    let job_id = job["id"].as_str().unwrap().to_string();

    client
        .post(format!("{base_url}/api/workers/register"))
        .json(&serde_json::json!({
            "worker_id": "w-proxy",
            "job_id": job_id,
            "address": worker_addr,
            "hostname": "test",
            "pid": 1
        }))
        .send()
        .await
        .unwrap();

    // Issue stop command through admin server — it should proxy to the mock worker.
    let resp = client
        .post(format!("{base_url}/api/workers/w-proxy/stop"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Verify the mock worker received the stop.
    assert!(
        stopped_flag.load(std::sync::atomic::Ordering::Relaxed),
        "mock worker should have received stop command"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_multiple_heartbeats_build_metrics_history() {
    let shutdown = CancellationToken::new();
    let base_url = start_admin_server(shutdown.clone()).await;
    let client = Client::new();

    // Setup: create job + register worker.
    let resp = client
        .post(format!("{base_url}/api/jobs"))
        .json(&serde_json::json!({
            "name": "history-job",
            "config_json": "{}",
        }))
        .send()
        .await
        .unwrap();
    let job: serde_json::Value = resp.json().await.unwrap();
    let job_id = job["id"].as_str().unwrap().to_string();

    client
        .post(format!("{base_url}/api/workers/register"))
        .json(&serde_json::json!({
            "worker_id": "w-hist",
            "job_id": job_id,
            "address": "127.0.0.1:1111",
            "hostname": "h",
            "pid": 1
        }))
        .send()
        .await
        .unwrap();

    // Send 5 heartbeats with increasing event counts.
    for i in 1..=5u64 {
        client
            .post(format!("{base_url}/api/workers/heartbeat"))
            .json(&serde_json::json!({
                "worker_id": "w-hist",
                "metrics": {
                    "events_total": i * 100,
                    "events_per_sec": (i * 10) as f64,
                    "batches_total": i,
                    "last_batch_size": 20,
                    "errors_total": 0,
                    "last_flush_at": 0,
                    "uptime_secs": i * 10
                }
            }))
            .send()
            .await
            .unwrap();
    }

    // Get metrics history.
    let history: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/metrics/w-hist?limit=100"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history.len(), 5);

    // Latest should be the last one sent (events_total=500).
    let latest: serde_json::Value = client
        .get(format!("{base_url}/api/metrics/w-hist/latest"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(latest["events_total"], 500);

    // With limit=3, only the 3 most recent.
    let limited: Vec<serde_json::Value> = client
        .get(format!("{base_url}/api/metrics/w-hist?limit=3"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(limited.len(), 3);

    shutdown.cancel();
}
