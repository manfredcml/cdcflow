use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::offset::OffsetStore;

/// In-memory offset store, primarily for testing.
#[derive(Debug, Clone)]
pub struct MemoryOffsetStore {
    inner: Arc<Mutex<Option<String>>>,
}

impl MemoryOffsetStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Create with a pre-set offset (useful for testing resume scenarios).
    pub fn with_offset(offset: String) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(offset))),
        }
    }

    /// Read the current offset without async (test convenience).
    pub fn current(&self) -> Option<String> {
        self.inner.lock().unwrap().clone()
    }
}

impl Default for MemoryOffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetStore for MemoryOffsetStore {
    async fn load(&self) -> Result<Option<String>> {
        Ok(self.inner.lock().unwrap().clone())
    }

    async fn save(&mut self, offset: String) -> Result<()> {
        *self.inner.lock().unwrap() = Some(offset);
        Ok(())
    }
}

