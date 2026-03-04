pub mod client;
pub mod routes;
pub mod sqlite;
pub mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::error::{CdcError, Result};
use sqlite::SqliteAdminStore;
use store::AdminStore;

/// Shared state for admin server routes.
#[derive(Clone)]
pub struct AdminState {
    pub db: Arc<tokio::sync::Mutex<dyn AdminStore>>,
}

/// Configuration for the admin server.
pub struct AdminServerConfig {
    pub bind: String,
    pub port: u16,
    pub db_path: String,
    pub heartbeat_timeout_secs: u64,
    pub metrics_retention_days: u64,
}

/// The admin dashboard server.
pub struct AdminServer {
    config: AdminServerConfig,
    state: AdminState,
}

impl AdminServer {
    /// Open the database and prepare the server.
    pub fn new(config: AdminServerConfig) -> Result<Self> {
        let db = SqliteAdminStore::open(&config.db_path)?;
        let state = AdminState {
            db: Arc::new(tokio::sync::Mutex::new(db)),
        };
        Ok(Self { config, state })
    }

    /// Start the admin server. Runs until shutdown.
    pub async fn start(self, shutdown: CancellationToken) -> Result<()> {
        let addr: SocketAddr = format!("{}:{}", self.config.bind, self.config.port)
            .parse()
            .map_err(|e| CdcError::Admin(format!("invalid bind address: {e}")))?;

        // Background tasks
        self.spawn_background_tasks(shutdown.clone());

        let api_router = routes::admin_router(self.state.clone());

        let app = Router::new()
            .merge(api_router)
            .layer(CorsLayer::permissive())
            .layer(TraceLayer::new_for_http());

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| CdcError::Admin(format!("failed to bind {addr}: {e}")))?;

        tracing::info!("admin server listening on {addr}");

        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .map_err(|e| CdcError::Admin(format!("server error: {e}")))?;

        Ok(())
    }

    fn spawn_background_tasks(&self, shutdown: CancellationToken) {
        // Lost worker detection
        let db = self.state.db.clone();
        let timeout = self.config.heartbeat_timeout_secs;
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = shutdown_clone.cancelled() => break,
                    _ = interval.tick() => {
                        let db = db.lock().await;
                        match db.mark_lost_workers(timeout).await {
                            Ok(0) => {}
                            Ok(n) => tracing::warn!("{n} workers marked as lost"),
                            Err(e) => tracing::error!("lost worker check failed: {e}"),
                        }
                    }
                }
            }
        });

        // Metrics pruning
        let db = self.state.db.clone();
        let retention_days = self.config.metrics_retention_days;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // hourly
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let db = db.lock().await;
                        match db.prune_metrics(retention_days).await {
                            Ok(0) => {}
                            Ok(n) => tracing::info!("pruned {n} old metrics snapshots"),
                            Err(e) => tracing::error!("metrics pruning failed: {e}"),
                        }
                    }
                }
            }
        });
    }
}
