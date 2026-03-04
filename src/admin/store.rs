use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metrics::MetricsSnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub config_json: String,
    pub description: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub id: String,
    pub job_id: String,
    pub address: String,
    pub hostname: String,
    pub pid: u32,
    pub status: String,
    pub registered_at: String,
    pub last_heartbeat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRow {
    pub id: i64,
    pub worker_id: String,
    pub timestamp: String,
    pub events_total: i64,
    pub events_per_sec: f64,
    pub batches_total: i64,
    pub last_batch_size: i64,
    pub errors_total: i64,
    pub uptime_secs: i64,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateJobRequest {
    pub name: String,
    pub config_json: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateJobRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub config_json: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[async_trait]
pub trait AdminStore: Send {
    async fn create_job(&self, req: &CreateJobRequest) -> Result<Job>;
    async fn get_job(&self, id: &str) -> Result<Job>;
    async fn get_job_by_name(&self, name: &str) -> Result<Job>;
    async fn list_jobs(&self) -> Result<Vec<Job>>;
    async fn update_job(&self, id: &str, req: &UpdateJobRequest) -> Result<Job>;
    async fn delete_job(&self, id: &str) -> Result<()>;

    async fn register_worker(
        &self,
        id: &str,
        job_id: &str,
        address: &str,
        hostname: &str,
        pid: u32,
    ) -> Result<Worker>;
    async fn get_worker(&self, id: &str) -> Result<Worker>;
    async fn list_workers(&self) -> Result<Vec<Worker>>;
    async fn heartbeat_worker(&self, id: &str) -> Result<()>;
    async fn deregister_worker(&self, id: &str) -> Result<()>;
    async fn mark_lost_workers(&self, timeout_secs: u64) -> Result<u64>;

    async fn insert_metrics(
        &self,
        worker_id: &str,
        snapshot: &MetricsSnapshot,
        status: &str,
    ) -> Result<()>;
    async fn get_latest_metrics(&self, worker_id: &str) -> Result<Option<MetricsRow>>;
    async fn get_metrics_history(&self, worker_id: &str, limit: u32) -> Result<Vec<MetricsRow>>;
    async fn prune_metrics(&self, days: u64) -> Result<u64>;
}
