use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{params, Connection};

use crate::error::{CdcError, Result};
use crate::metrics::MetricsSnapshot;

use super::store::{AdminStore, CreateJobRequest, Job, MetricsRow, UpdateJobRequest, Worker};

/// SQLite-backed implementation of [`AdminStore`].
///
/// The inner `Connection` is wrapped in a `std::sync::Mutex` to satisfy `Sync`,
/// which is required by `#[async_trait]`'s `Send` future bounds.
pub struct SqliteAdminStore {
    conn: Mutex<Connection>,
}

impl SqliteAdminStore {
    /// Open (or create) the database and run migrations.
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(path)
        }
        .map_err(|e| CdcError::Database(format!("failed to open db: {e}")))?;

        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                config_json TEXT NOT NULL,
                description TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL REFERENCES jobs(id),
                address TEXT NOT NULL,
                hostname TEXT DEFAULT '',
                pid INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'registered',
                registered_at TEXT DEFAULT (datetime('now')),
                last_heartbeat TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS metrics_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
                timestamp TEXT DEFAULT (datetime('now')),
                events_total INTEGER DEFAULT 0,
                events_per_sec REAL DEFAULT 0.0,
                batches_total INTEGER DEFAULT 0,
                last_batch_size INTEGER DEFAULT 0,
                errors_total INTEGER DEFAULT 0,
                uptime_secs INTEGER DEFAULT 0,
                status TEXT DEFAULT 'unknown'
            );
            ",
        )
        .map_err(|e| CdcError::Database(format!("migration failed: {e}")))?;
        Ok(())
    }

    /// Expose the inner connection for test-only use (e.g. backdating heartbeats).
    #[cfg(test)]
    pub(crate) fn raw_conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }
}

fn query_job(conn: &Connection, sql: &str, param: &str) -> Result<Job> {
    conn.query_row(sql, params![param], |row| {
        Ok(Job {
            id: row.get(0)?,
            name: row.get(1)?,
            config_json: row.get(2)?,
            description: row.get(3)?,
            created_at: row.get(4)?,
            updated_at: row.get(5)?,
        })
    })
    .map_err(|e| CdcError::Database(format!("query_job: {e}")))
}

fn query_worker(conn: &Connection, id: &str) -> Result<Worker> {
    conn.query_row(
        "SELECT id, job_id, address, hostname, pid, status, registered_at, last_heartbeat FROM workers WHERE id = ?1",
        params![id],
        |row| {
            Ok(Worker {
                id: row.get(0)?,
                job_id: row.get(1)?,
                address: row.get(2)?,
                hostname: row.get(3)?,
                pid: row.get(4)?,
                status: row.get(5)?,
                registered_at: row.get(6)?,
                last_heartbeat: row.get(7)?,
            })
        },
    )
    .map_err(|e| CdcError::Database(format!("get_worker: {e}")))
}

#[async_trait]
impl AdminStore for SqliteAdminStore {
    async fn create_job(&self, req: &CreateJobRequest) -> Result<Job> {
        let conn = self.conn.lock().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO jobs (id, name, config_json, description) VALUES (?1, ?2, ?3, ?4)",
            params![id, req.name, req.config_json, req.description],
        )
        .map_err(|e| CdcError::Database(format!("create_job: {e}")))?;
        query_job(
            &conn,
            "SELECT id, name, config_json, description, created_at, updated_at FROM jobs WHERE id = ?1",
            &id,
        )
    }

    async fn get_job(&self, id: &str) -> Result<Job> {
        let conn = self.conn.lock().unwrap();
        query_job(
            &conn,
            "SELECT id, name, config_json, description, created_at, updated_at FROM jobs WHERE id = ?1",
            id,
        )
    }

    async fn get_job_by_name(&self, name: &str) -> Result<Job> {
        let conn = self.conn.lock().unwrap();
        query_job(
            &conn,
            "SELECT id, name, config_json, description, created_at, updated_at FROM jobs WHERE name = ?1",
            name,
        )
    }

    async fn list_jobs(&self) -> Result<Vec<Job>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, config_json, description, created_at, updated_at FROM jobs ORDER BY created_at DESC")
            .map_err(|e| CdcError::Database(format!("list_jobs: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(Job {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    config_json: row.get(2)?,
                    description: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            })
            .map_err(|e| CdcError::Database(format!("list_jobs: {e}")))?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| CdcError::Database(format!("list_jobs: {e}")))
    }

    async fn update_job(&self, id: &str, req: &UpdateJobRequest) -> Result<Job> {
        let conn = self.conn.lock().unwrap();
        if let Some(name) = &req.name {
            conn.execute(
                "UPDATE jobs SET name = ?1, updated_at = datetime('now') WHERE id = ?2",
                params![name, id],
            )
            .map_err(|e| CdcError::Database(format!("update_job: {e}")))?;
        }
        if let Some(config_json) = &req.config_json {
            conn.execute(
                "UPDATE jobs SET config_json = ?1, updated_at = datetime('now') WHERE id = ?2",
                params![config_json, id],
            )
            .map_err(|e| CdcError::Database(format!("update_job: {e}")))?;
        }
        if let Some(description) = &req.description {
            conn.execute(
                "UPDATE jobs SET description = ?1, updated_at = datetime('now') WHERE id = ?2",
                params![description, id],
            )
            .map_err(|e| CdcError::Database(format!("update_job: {e}")))?;
        }
        query_job(
            &conn,
            "SELECT id, name, config_json, description, created_at, updated_at FROM jobs WHERE id = ?1",
            id,
        )
    }

    async fn delete_job(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM jobs WHERE id = ?1", params![id])
            .map_err(|e| CdcError::Database(format!("delete_job: {e}")))?;
        Ok(())
    }

    async fn register_worker(
        &self,
        id: &str,
        job_id: &str,
        address: &str,
        hostname: &str,
        pid: u32,
    ) -> Result<Worker> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO workers (id, job_id, address, hostname, pid, status) VALUES (?1, ?2, ?3, ?4, ?5, 'registered')",
            params![id, job_id, address, hostname, pid],
        )
        .map_err(|e| CdcError::Database(format!("register_worker: {e}")))?;
        query_worker(&conn, id)
    }

    async fn get_worker(&self, id: &str) -> Result<Worker> {
        let conn = self.conn.lock().unwrap();
        query_worker(&conn, id)
    }

    async fn list_workers(&self) -> Result<Vec<Worker>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, job_id, address, hostname, pid, status, registered_at, last_heartbeat FROM workers ORDER BY registered_at DESC")
            .map_err(|e| CdcError::Database(format!("list_workers: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(Worker {
                    id: row.get(0)?,
                    job_id: row.get(1)?,
                    address: row.get(2)?,
                    hostname: row.get(3)?,
                    pid: row.get(4)?,
                    status: row.get(5)?,
                    registered_at: row.get(6)?,
                    last_heartbeat: row.get(7)?,
                })
            })
            .map_err(|e| CdcError::Database(format!("list_workers: {e}")))?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| CdcError::Database(format!("list_workers: {e}")))
    }

    async fn heartbeat_worker(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let updated = conn
            .execute(
                "UPDATE workers SET last_heartbeat = datetime('now'), status = 'running' WHERE id = ?1",
                params![id],
            )
            .map_err(|e| CdcError::Database(format!("heartbeat_worker: {e}")))?;

        if updated == 0 {
            return Err(CdcError::Database(format!("worker {id} not found")));
        }
        Ok(())
    }

    async fn deregister_worker(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE workers SET status = 'stopped' WHERE id = ?1",
            params![id],
        )
        .map_err(|e| CdcError::Database(format!("deregister_worker: {e}")))?;
        Ok(())
    }

    async fn mark_lost_workers(&self, timeout_secs: u64) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let updated = conn
            .execute(
                "UPDATE workers SET status = 'lost' WHERE status IN ('registered', 'running') AND last_heartbeat < datetime('now', ?1)",
                params![format!("-{timeout_secs} seconds")],
            )
            .map_err(|e| CdcError::Database(format!("mark_lost_workers: {e}")))?;
        Ok(updated as u64)
    }

    async fn insert_metrics(
        &self,
        worker_id: &str,
        snapshot: &MetricsSnapshot,
        status: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO metrics_snapshots (worker_id, events_total, events_per_sec, batches_total, last_batch_size, errors_total, uptime_secs, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                worker_id,
                snapshot.events_total as i64,
                snapshot.events_per_sec,
                snapshot.batches_total as i64,
                snapshot.last_batch_size as i64,
                snapshot.errors_total as i64,
                snapshot.uptime_secs as i64,
                status,
            ],
        )
        .map_err(|e| CdcError::Database(format!("insert_metrics: {e}")))?;
        Ok(())
    }

    async fn get_latest_metrics(&self, worker_id: &str) -> Result<Option<MetricsRow>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT id, worker_id, timestamp, events_total, events_per_sec, batches_total, last_batch_size, errors_total, uptime_secs, status FROM metrics_snapshots WHERE worker_id = ?1 ORDER BY id DESC LIMIT 1",
            params![worker_id],
            |row| {
                Ok(MetricsRow {
                    id: row.get(0)?,
                    worker_id: row.get(1)?,
                    timestamp: row.get(2)?,
                    events_total: row.get(3)?,
                    events_per_sec: row.get(4)?,
                    batches_total: row.get(5)?,
                    last_batch_size: row.get(6)?,
                    errors_total: row.get(7)?,
                    uptime_secs: row.get(8)?,
                    status: row.get(9)?,
                })
            },
        );

        match result {
            Ok(row) => Ok(Some(row)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(CdcError::Database(format!("get_latest_metrics: {e}"))),
        }
    }

    async fn get_metrics_history(&self, worker_id: &str, limit: u32) -> Result<Vec<MetricsRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, worker_id, timestamp, events_total, events_per_sec, batches_total, last_batch_size, errors_total, uptime_secs, status FROM metrics_snapshots WHERE worker_id = ?1 ORDER BY id DESC LIMIT ?2",
            )
            .map_err(|e| CdcError::Database(format!("get_metrics_history: {e}")))?;

        let rows = stmt
            .query_map(params![worker_id, limit], |row| {
                Ok(MetricsRow {
                    id: row.get(0)?,
                    worker_id: row.get(1)?,
                    timestamp: row.get(2)?,
                    events_total: row.get(3)?,
                    events_per_sec: row.get(4)?,
                    batches_total: row.get(5)?,
                    last_batch_size: row.get(6)?,
                    errors_total: row.get(7)?,
                    uptime_secs: row.get(8)?,
                    status: row.get(9)?,
                })
            })
            .map_err(|e| CdcError::Database(format!("get_metrics_history: {e}")))?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| CdcError::Database(format!("get_metrics_history: {e}")))
    }

    async fn prune_metrics(&self, days: u64) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn
            .execute(
                "DELETE FROM metrics_snapshots WHERE timestamp < datetime('now', ?1)",
                params![format!("-{days} days")],
            )
            .map_err(|e| CdcError::Database(format!("prune_metrics: {e}")))?;
        Ok(deleted as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> SqliteAdminStore {
        SqliteAdminStore::open(":memory:").unwrap()
    }

    fn sample_snapshot() -> MetricsSnapshot {
        MetricsSnapshot {
            events_total: 100,
            events_per_sec: 10.0,
            batches_total: 5,
            last_batch_size: 20,
            errors_total: 0,
            last_flush_at: 0,
            uptime_secs: 10,
        }
    }

    #[tokio::test]
    async fn test_create_and_get_job() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "pg-to-iceberg".into(),
                config_json: r#"{"source":{}}"#.into(),
                description: "test job".into(),
            })
            .await
            .unwrap();

        assert_eq!(job.name, "pg-to-iceberg");
        assert_eq!(job.description, "test job");

        let fetched = db.get_job(&job.id).await.unwrap();
        assert_eq!(fetched.name, job.name);
    }

    #[tokio::test]
    async fn test_get_job_by_name() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "my-job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();

        let fetched = db.get_job_by_name("my-job").await.unwrap();
        assert_eq!(fetched.id, job.id);
    }

    #[tokio::test]
    async fn test_list_jobs() {
        let db = test_db();
        db.create_job(&CreateJobRequest {
            name: "job-1".into(),
            config_json: "{}".into(),
            description: String::new(),
        })
        .await
        .unwrap();
        db.create_job(&CreateJobRequest {
            name: "job-2".into(),
            config_json: "{}".into(),
            description: String::new(),
        })
        .await
        .unwrap();

        let jobs = db.list_jobs().await.unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[tokio::test]
    async fn test_update_job() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "old-name".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();

        let updated = db
            .update_job(
                &job.id,
                &UpdateJobRequest {
                    name: Some("new-name".into()),
                    config_json: None,
                    description: Some("updated desc".into()),
                },
            )
            .await
            .unwrap();

        assert_eq!(updated.name, "new-name");
        assert_eq!(updated.description, "updated desc");
    }

    #[tokio::test]
    async fn test_delete_job() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "doomed".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();

        db.delete_job(&job.id).await.unwrap();
        assert!(db.get_job(&job.id).await.is_err());
    }

    #[tokio::test]
    async fn test_duplicate_job_name_errors() {
        let db = test_db();
        db.create_job(&CreateJobRequest {
            name: "unique-name".into(),
            config_json: "{}".into(),
            description: String::new(),
        })
        .await
        .unwrap();

        let result = db
            .create_job(&CreateJobRequest {
                name: "unique-name".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_and_get_worker() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();

        let worker = db
            .register_worker("w1", &job.id, "127.0.0.1:9090", "host1", 1234)
            .await
            .unwrap();

        assert_eq!(worker.id, "w1");
        assert_eq!(worker.job_id, job.id);
        assert_eq!(worker.address, "127.0.0.1:9090");
        assert_eq!(worker.status, "registered");
    }

    #[tokio::test]
    async fn test_list_workers() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();

        db.register_worker("w1", &job.id, "addr1", "h1", 1)
            .await
            .unwrap();
        db.register_worker("w2", &job.id, "addr2", "h2", 2)
            .await
            .unwrap();

        let workers = db.list_workers().await.unwrap();
        assert_eq!(workers.len(), 2);
    }

    #[tokio::test]
    async fn test_heartbeat_worker() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();

        db.heartbeat_worker("w1").await.unwrap();
        let worker = db.get_worker("w1").await.unwrap();
        assert_eq!(worker.status, "running");
    }

    #[tokio::test]
    async fn test_heartbeat_unknown_worker() {
        let db = test_db();
        let result = db.heartbeat_worker("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_deregister_worker() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();

        db.deregister_worker("w1").await.unwrap();
        let worker = db.get_worker("w1").await.unwrap();
        assert_eq!(worker.status, "stopped");
    }

    #[tokio::test]
    async fn test_insert_and_get_latest_metrics() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();

        let snap = sample_snapshot();
        db.insert_metrics("w1", &snap, "running").await.unwrap();

        let latest = db.get_latest_metrics("w1").await.unwrap().unwrap();
        assert_eq!(latest.events_total, 100);
        assert_eq!(latest.status, "running");
    }

    #[tokio::test]
    async fn test_get_latest_metrics_none() {
        let db = test_db();
        let result = db.get_latest_metrics("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_metrics_history() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();

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

        let history = db.get_metrics_history("w1", 3).await.unwrap();
        assert_eq!(history.len(), 3);
        // Most recent first
        assert_eq!(history[0].events_total, 40);
    }

    #[tokio::test]
    async fn test_mark_lost_workers() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();
        // Backdate the heartbeat to simulate a stale worker
        db.raw_conn()
            .execute(
                "UPDATE workers SET last_heartbeat = datetime('now', '-60 seconds') WHERE id = 'w1'",
                [],
            )
            .unwrap();

        let count = db.mark_lost_workers(30).await.unwrap();
        assert_eq!(count, 1);

        let worker = db.get_worker("w1").await.unwrap();
        assert_eq!(worker.status, "lost");
    }

    #[tokio::test]
    async fn test_prune_metrics_nothing_to_prune() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();

        let snap = sample_snapshot();
        db.insert_metrics("w1", &snap, "running").await.unwrap();

        // Prune with 0 days — should delete nothing (metrics are from 'now')
        let deleted = db.prune_metrics(0).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn test_prune_metrics_deletes_old_data() {
        let db = test_db();
        let job = db
            .create_job(&CreateJobRequest {
                name: "job".into(),
                config_json: "{}".into(),
                description: String::new(),
            })
            .await
            .unwrap();
        db.register_worker("w1", &job.id, "addr", "h", 1)
            .await
            .unwrap();

        let snap = sample_snapshot();
        // Insert two metrics rows.
        db.insert_metrics("w1", &snap, "running").await.unwrap();
        db.insert_metrics("w1", &snap, "running").await.unwrap();

        // Backdate one row to 3 days ago.
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE metrics_snapshots SET timestamp = datetime('now', '-3 days') WHERE id = (SELECT MIN(id) FROM metrics_snapshots)",
                [],
            )
            .unwrap();
        }

        // Prune older than 1 day — should delete the backdated row.
        let deleted = db.prune_metrics(1).await.unwrap();
        assert_eq!(deleted, 1);

        // The recent row should still be there.
        let history = db.get_metrics_history("w1", 10).await.unwrap();
        assert_eq!(history.len(), 1);
    }
}
