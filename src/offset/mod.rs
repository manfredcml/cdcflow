pub mod memory;
pub mod sqlite;

use crate::error::Result;

/// Trait for persisting CDC progress as an opaque offset string.
/// Each source type defines its own offset format (e.g., PostgreSQL uses "XX/YY" LSN,
/// MySQL uses "filename:position"), but the store is source-agnostic.
pub trait OffsetStore: Send {
    /// Load the last saved offset. Returns None if no offset has been saved.
    fn load(&self) -> impl std::future::Future<Output = Result<Option<String>>> + Send;

    /// Save the current offset. Must be durable before returning.
    fn save(&mut self, offset: String) -> impl std::future::Future<Output = Result<()>> + Send;
}
