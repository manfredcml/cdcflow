use mongodb::bson::Document;
use mongodb::change_stream::event::{ChangeStreamEvent, ResumeToken};

use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, Lsn, TableId};

use super::bson_mapping::document_to_row;

/// Result of converting a change stream event.
pub enum ConvertResult {
    /// A CDC event to send downstream.
    Event(CdcEvent),
    /// A commit with serialized resume token as offset.
    Commit { offset: String },
    /// Skip this event (unsupported operation type).
    Skip,
}

/// Convert a MongoDB change stream event to a `CdcEvent`.
///
/// Returns `ConvertResult::Event` for insert/update/replace/delete,
/// `ConvertResult::Skip` for unsupported operations (drop, rename, etc.).
pub fn change_event_to_cdc_event(
    event: &ChangeStreamEvent<Document>,
    database: &str,
) -> Result<ConvertResult> {
    let op = match event.operation_type {
        mongodb::change_stream::event::OperationType::Insert => ChangeOp::Insert,
        mongodb::change_stream::event::OperationType::Update
        | mongodb::change_stream::event::OperationType::Replace => ChangeOp::Update,
        mongodb::change_stream::event::OperationType::Delete => ChangeOp::Delete,
        _ => return Ok(ConvertResult::Skip),
    };

    let collection = event
        .ns
        .as_ref()
        .and_then(|ns| ns.coll.as_deref())
        .unwrap_or("unknown");

    let timestamp_us = event
        .cluster_time
        .map(|ts| ts.time as i64 * 1_000_000)
        .or_else(|| event.wall_time.map(|wt| wt.timestamp_millis() * 1000))
        .unwrap_or(0);

    let new = event.full_document.as_ref().map(document_to_row);
    let old = event
        .full_document_before_change
        .as_ref()
        .map(document_to_row);

    let cdc_event = CdcEvent {
        lsn: Lsn(0),
        timestamp_us,
        xid: 0,
        table: TableId {
            schema: database.to_string(),
            name: collection.to_string(),
            oid: 0,
        },
        op,
        new,
        old,
        primary_key_columns: vec!["_id".to_string()],
    };

    Ok(ConvertResult::Event(cdc_event))
}

/// Convert a snapshot document to a `CdcEvent`.
pub fn snapshot_doc_to_cdc_event(doc: &Document, database: &str, collection: &str) -> CdcEvent {
    let row = document_to_row(doc);
    CdcEvent {
        lsn: Lsn(0),
        timestamp_us: chrono::Utc::now().timestamp_micros(),
        xid: 0,
        table: TableId {
            schema: database.to_string(),
            name: collection.to_string(),
            oid: 0,
        },
        op: ChangeOp::Snapshot,
        new: Some(row),
        old: None,
        primary_key_columns: vec!["_id".to_string()],
    }
}

/// Serialize a resume token to a JSON string for offset storage.
pub fn serialize_resume_token(token: &ResumeToken) -> Result<String> {
    serde_json::to_string(token)
        .map_err(|e| CdcError::Mongodb(format!("failed to serialize resume token: {e}")))
}

/// Deserialize a resume token from a JSON string.
pub fn deserialize_resume_token(s: &str) -> Result<ResumeToken> {
    serde_json::from_str(s)
        .map_err(|e| CdcError::Mongodb(format!("failed to deserialize resume token: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn test_snapshot_doc_to_cdc_event() {
        let doc = doc! {
            "_id": "abc123",
            "name": "Alice",
            "age": 30,
        };
        let event = snapshot_doc_to_cdc_event(&doc, "mydb", "users");
        assert_eq!(event.op, ChangeOp::Snapshot);
        assert_eq!(event.table.schema, "mydb");
        assert_eq!(event.table.name, "users");
        assert_eq!(event.primary_key_columns, vec!["_id"]);
        assert!(event.new.is_some());
        assert!(event.old.is_none());

        let row = event.new.unwrap();
        assert!(
            matches!(row.get("name"), Some(crate::event::ColumnValue::Text(s)) if s == "Alice")
        );
        assert_eq!(row.get("age"), Some(&crate::event::ColumnValue::Int(30)));
    }
}
