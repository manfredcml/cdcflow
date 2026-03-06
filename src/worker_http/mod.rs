pub mod registration;
pub mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::error::{CdcError, Result};
use crate::metrics::PipelineMetrics;

use routes::WorkerState;

/// Lightweight HTTP server embedded in each CDC worker process.
pub struct WorkerHttpServer {
    addr: SocketAddr,
    state: WorkerState,
}

impl WorkerHttpServer {
    /// Bind to the given port (0 = OS-assigned ephemeral port).
    pub async fn bind(
        port: u16,
        metrics: Arc<PipelineMetrics>,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| CdcError::Http(format!("failed to bind {addr}: {e}")))?;
        let actual_addr = listener
            .local_addr()
            .map_err(|e| CdcError::Http(format!("failed to get local addr: {e}")))?;
        // Drop listener — we'll re-bind in start(). This is just for port resolution.
        drop(listener);

        let state = WorkerState { metrics, shutdown };
        Ok(Self {
            addr: actual_addr,
            state,
        })
    }

    /// The bound address (useful when port was 0).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Start serving requests. Runs until the shutdown token is cancelled.
    pub async fn start(self) -> Result<()> {
        let shutdown = self.state.shutdown.clone();
        let router = Router::new()
            .route("/health", get(routes::health))
            .route("/metrics", get(routes::metrics))
            .route("/control/stop", post(routes::stop))
            .with_state(self.state);

        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| CdcError::Http(format!("failed to bind {}: {e}", self.addr)))?;

        tracing::info!("worker HTTP server listening on {}", self.addr);

        axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .map_err(|e| CdcError::Http(format!("server error: {e}")))?;

        Ok(())
    }
}
