use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::{CdcError, Result};
use crate::offset::OffsetStore;

/// SQLite-backed offset store. Persists the offset in a local SQLite database.
///
/// Uses `Arc<Mutex<Connection>>` so clones share the same underlying database —
/// matching the pattern used by `MemoryOffsetStore` (`Arc<Mutex>`) and `SqliteAdminStore`.
#[derive(Debug, Clone)]
pub struct SqliteOffsetStore {
    conn: Arc<Mutex<Connection>>,
    key: String,
}

impl SqliteOffsetStore {
    /// Create a new SQLite offset store.
    ///
    /// - `path`: Database file path, or `":memory:"` for an in-memory database
    /// - `key`: The key under which the offset is stored (allows multiple pipelines per DB)
    pub fn new(path: &str, key: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(path)
        }
        .map_err(|e| CdcError::Database(format!("failed to open offset db: {e}")))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS offsets (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )",
        )
        .map_err(|e| CdcError::Database(format!("failed to migrate offset db: {e}")))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            key: key.to_string(),
        })
    }
}

impl OffsetStore for SqliteOffsetStore {
    async fn load(&self) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT value FROM offsets WHERE key = ?1")
            .map_err(|e| CdcError::Database(e.to_string()))?;

        match stmt.query_row([&self.key], |row| row.get::<_, String>(0)) {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(CdcError::Database(e.to_string())),
        }
    }

    async fn save(&mut self, offset: String) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO offsets (key, value, updated_at) VALUES (?1, ?2, datetime('now'))",
            [&self.key, &offset],
        )
        .map_err(|e| CdcError::Database(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_nonexistent_key_returns_none() {
        let store = SqliteOffsetStore::new(":memory:", "missing-key").unwrap();
        assert_eq!(store.load().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_save_load_roundtrip() {
        let mut store = SqliteOffsetStore::new(":memory:", "pg-offset").unwrap();

        let offset = "0/16B3748".to_string();
        store.save(offset.clone()).await.unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(offset));
    }

    #[tokio::test]
    async fn test_overwrite_semantics() {
        let mut store = SqliteOffsetStore::new(":memory:", "pg-offset").unwrap();

        let offset1 = "0/1".to_string();
        let offset2 = "1/0".to_string();

        store.save(offset1.clone()).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(offset1));

        store.save(offset2.clone()).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(offset2));
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let mut store = SqliteOffsetStore::new(":memory:", "pg-offset").unwrap();
        let clone = store.clone();

        let offset = "0/42".to_string();
        store.save(offset.clone()).await.unwrap();

        // Clone should see the update since they share an Arc<Mutex<Connection>>
        assert_eq!(clone.load().await.unwrap(), Some(offset));
    }

    #[tokio::test]
    async fn test_mysql_offset_format() {
        let mut store = SqliteOffsetStore::new(":memory:", "mysql-offset").unwrap();

        let offset = "mysql-bin.000003:12345".to_string();
        store.save(offset.clone()).await.unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded, Some(offset));
    }

    #[tokio::test]
    async fn test_multiple_keys_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("offsets.db");
        let path_str = db_path.to_str().unwrap();

        let mut store_a = SqliteOffsetStore::new(path_str, "pipeline-a").unwrap();
        let mut store_b = SqliteOffsetStore::new(path_str, "pipeline-b").unwrap();

        store_a.save("0/AAA".to_string()).await.unwrap();
        store_b.save("0/BBB".to_string()).await.unwrap();

        assert_eq!(store_a.load().await.unwrap(), Some("0/AAA".to_string()));
        assert_eq!(store_b.load().await.unwrap(), Some("0/BBB".to_string()));
    }

    #[tokio::test]
    async fn test_file_persistence_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("offsets.db");
        let path_str = db_path.to_str().unwrap();

        let offset = "0/PERSIST".to_string();
        {
            let mut store = SqliteOffsetStore::new(path_str, "durable-key").unwrap();
            store.save(offset.clone()).await.unwrap();
            // store + connection dropped here
        }

        let store = SqliteOffsetStore::new(path_str, "durable-key").unwrap();
        assert_eq!(store.load().await.unwrap(), Some(offset));
    }
}
