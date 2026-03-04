use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::error::{CdcError, Result};
use crate::metrics::PipelineMetrics;

/// Payload sent when registering a worker with the admin server.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub worker_id: String,
    pub job_id: String,
    pub address: String,
    pub hostname: String,
    pub pid: u32,
}

/// Payload sent on each heartbeat.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub worker_id: String,
    pub metrics: crate::metrics::MetricsSnapshot,
}

/// Payload sent when deregistering.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeregisterRequest {
    pub worker_id: String,
}

/// Response from the admin server after registration.
#[derive(Debug, Deserialize)]
pub struct RegisterResponse {
    pub status: String,
}

/// Client for communicating with the admin server.
pub struct AdminClient {
    base_url: String,
    client: reqwest::Client,
    worker_id: String,
}

impl AdminClient {
    pub fn new(base_url: &str, worker_id: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            worker_id: worker_id.to_string(),
        }
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Pull job configuration from the admin server.
    pub async fn pull_config(&self, job_name: &str) -> Result<Config> {
        let url = format!("{}/api/jobs/{}/config", self.base_url, job_name);
        let resp = self.client.get(&url).send().await
            .map_err(|e| CdcError::Http(format!("pull_config failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(CdcError::Http(format!(
                "pull_config returned {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        resp.json::<Config>().await
            .map_err(|e| CdcError::Http(format!("pull_config parse error: {e}")))
    }

    /// Register this worker with the admin server.
    pub async fn register(&self, job_id: &str, address: &str) -> Result<()> {
        let url = format!("{}/api/workers/register", self.base_url);
        let hostname = hostname();
        let pid = std::process::id();

        let req = RegisterRequest {
            worker_id: self.worker_id.clone(),
            job_id: job_id.to_string(),
            address: address.to_string(),
            hostname,
            pid,
        };

        let resp = self.client.post(&url).json(&req).send().await
            .map_err(|e| CdcError::Http(format!("register failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(CdcError::Http(format!(
                "register returned {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            )));
        }

        Ok(())
    }

    /// Send a heartbeat with current metrics.
    pub async fn heartbeat(&self, metrics: &PipelineMetrics) -> Result<()> {
        let url = format!("{}/api/workers/heartbeat", self.base_url);
        let req = HeartbeatRequest {
            worker_id: self.worker_id.clone(),
            metrics: metrics.snapshot(),
        };

        let resp = self.client.post(&url).json(&req).send().await
            .map_err(|e| CdcError::Http(format!("heartbeat failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(CdcError::Http(format!(
                "heartbeat returned {}",
                resp.status()
            )));
        }

        Ok(())
    }

    /// Deregister this worker from the admin server.
    pub async fn deregister(&self) -> Result<()> {
        let url = format!("{}/api/workers/deregister", self.base_url);
        let req = DeregisterRequest {
            worker_id: self.worker_id.clone(),
        };

        let resp = self.client.post(&url).json(&req).send().await
            .map_err(|e| CdcError::Http(format!("deregister failed: {e}")))?;

        if !resp.status().is_success() {
            tracing::warn!(
                "deregister returned {}, ignoring",
                resp.status()
            );
        }

        Ok(())
    }

    /// Spawn a background heartbeat loop. Returns a join handle.
    pub fn spawn_heartbeat(
        &self,
        metrics: Arc<PipelineMetrics>,
        interval: Duration,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let url = format!("{}/api/workers/heartbeat", self.base_url);
        let client = self.client.clone();
        let worker_id = self.worker_id.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip immediate first tick

            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::debug!("heartbeat loop shutting down");
                        break;
                    }
                    _ = ticker.tick() => {
                        let req = HeartbeatRequest {
                            worker_id: worker_id.clone(),
                            metrics: metrics.snapshot(),
                        };
                        match client.post(&url).json(&req).send().await {
                            Ok(resp) if resp.status().is_success() => {
                                tracing::trace!("heartbeat sent");
                            }
                            Ok(resp) => {
                                tracing::warn!("heartbeat returned {}", resp.status());
                            }
                            Err(e) => {
                                tracing::warn!("heartbeat failed: {e}");
                            }
                        }
                    }
                }
            }
        })
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Start a mock admin server that counts requests.
    async fn mock_admin_server() -> (String, Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>) {
        let register_count = Arc::new(AtomicU64::new(0));
        let heartbeat_count = Arc::new(AtomicU64::new(0));
        let deregister_count = Arc::new(AtomicU64::new(0));

        let rc = register_count.clone();
        let hc = heartbeat_count.clone();
        let dc = deregister_count.clone();

        #[derive(Clone)]
        struct MockState {
            register_count: Arc<AtomicU64>,
            heartbeat_count: Arc<AtomicU64>,
            deregister_count: Arc<AtomicU64>,
        }

        let state = MockState {
            register_count: rc,
            heartbeat_count: hc,
            deregister_count: dc,
        };

        let app = Router::new()
            .route("/api/workers/register", post({
                let state = state.clone();
                move |_body: Json<serde_json::Value>| {
                    let state = state.clone();
                    async move {
                        state.register_count.fetch_add(1, Ordering::Relaxed);
                        Json(serde_json::json!({"status": "ok"}))
                    }
                }
            }))
            .route("/api/workers/heartbeat", post({
                let state = state.clone();
                move |_body: Json<serde_json::Value>| {
                    let state = state.clone();
                    async move {
                        state.heartbeat_count.fetch_add(1, Ordering::Relaxed);
                        Json(serde_json::json!({"status": "ok"}))
                    }
                }
            }))
            .route("/api/workers/deregister", post({
                let state = state.clone();
                move |_body: Json<serde_json::Value>| {
                    let state = state.clone();
                    async move {
                        state.deregister_count.fetch_add(1, Ordering::Relaxed);
                        Json(serde_json::json!({"status": "ok"}))
                    }
                }
            }));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}");
        (url, register_count, heartbeat_count, deregister_count)
    }

    #[tokio::test]
    async fn test_register() {
        let (url, register_count, _, _) = mock_admin_server().await;
        let client = AdminClient::new(&url, "worker-1");

        client.register("job-1", "127.0.0.1:9090").await.unwrap();
        assert_eq!(register_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let (url, _, heartbeat_count, _) = mock_admin_server().await;
        let client = AdminClient::new(&url, "worker-1");
        let metrics = PipelineMetrics::new();
        metrics.record_batch(10);

        client.heartbeat(&metrics).await.unwrap();
        assert_eq!(heartbeat_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_deregister() {
        let (url, _, _, deregister_count) = mock_admin_server().await;
        let client = AdminClient::new(&url, "worker-1");

        client.deregister().await.unwrap();
        assert_eq!(deregister_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_loop() {
        let (url, _, heartbeat_count, _) = mock_admin_server().await;
        let client = AdminClient::new(&url, "worker-1");
        let metrics = PipelineMetrics::new();
        let shutdown = CancellationToken::new();

        let handle = client.spawn_heartbeat(
            metrics,
            Duration::from_millis(50),
            shutdown.clone(),
        );

        // Wait enough for ~3 heartbeats
        tokio::time::sleep(Duration::from_millis(180)).await;
        shutdown.cancel();
        handle.await.unwrap();

        let count = heartbeat_count.load(Ordering::Relaxed);
        assert!(count >= 2, "expected at least 2 heartbeats, got {count}");
    }

    #[tokio::test]
    async fn test_register_connection_refused() {
        let client = AdminClient::new("http://127.0.0.1:1", "worker-1");
        let result = client.register("job-1", "127.0.0.1:9090").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("register failed"));
    }

}
