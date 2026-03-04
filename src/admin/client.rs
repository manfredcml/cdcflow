use crate::admin::store::{CreateJobRequest, Job};
use crate::error::CdcError;

/// Thin HTTP client for admin server job CRUD operations.
pub struct AdminJobClient {
    base_url: String,
    http: reqwest::Client,
}

impl AdminJobClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn create_job(
        &self,
        name: &str,
        config_json: &str,
        description: &str,
    ) -> Result<Job, CdcError> {
        let req = CreateJobRequest {
            name: name.to_string(),
            config_json: config_json.to_string(),
            description: description.to_string(),
        };
        let resp = self
            .http
            .post(format!("{}/api/jobs", self.base_url))
            .json(&req)
            .send()
            .await
            .map_err(|e| CdcError::Config(format!("admin request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CdcError::Config(format!(
                "create job failed ({status}): {body}"
            )));
        }
        resp.json::<Job>()
            .await
            .map_err(|e| CdcError::Config(format!("failed to parse job response: {e}")))
    }

    pub async fn list_jobs(&self) -> Result<Vec<Job>, CdcError> {
        let resp = self
            .http
            .get(format!("{}/api/jobs", self.base_url))
            .send()
            .await
            .map_err(|e| CdcError::Config(format!("admin request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CdcError::Config(format!(
                "list jobs failed ({status}): {body}"
            )));
        }
        resp.json::<Vec<Job>>()
            .await
            .map_err(|e| CdcError::Config(format!("failed to parse jobs response: {e}")))
    }

    pub async fn get_job(&self, name: &str) -> Result<Job, CdcError> {
        let resp = self
            .http
            .get(format!("{}/api/jobs/{}", self.base_url, name))
            .send()
            .await
            .map_err(|e| CdcError::Config(format!("admin request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CdcError::Config(format!(
                "get job failed ({status}): {body}"
            )));
        }
        resp.json::<Job>()
            .await
            .map_err(|e| CdcError::Config(format!("failed to parse job response: {e}")))
    }

    pub async fn delete_job(&self, name: &str) -> Result<(), CdcError> {
        let resp = self
            .http
            .delete(format!("{}/api/jobs/{}", self.base_url, name))
            .send()
            .await
            .map_err(|e| CdcError::Config(format!("admin request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CdcError::Config(format!(
                "delete job failed ({status}): {body}"
            )));
        }
        Ok(())
    }
}
