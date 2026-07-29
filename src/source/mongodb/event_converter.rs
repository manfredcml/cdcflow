use mongodb::bson::Document;
use mongodb::change_stream::event::ChangeStreamEvent;

use crate::error::Result;
use crate::event::{CdcEvent, ChangeOp, Lsn, TableId};

use super::bson_mapping::document_to_row;

/// Result of converting a change stream event.
pub enum ConvertResult {
    /// A CDC event to send downstream.
    Event(CdcEvent),
    /// Unsupported operation, or a CRUD event with no usable row.
    Skip,
}

/// Convert a MongoDB change stream event to a `CdcEvent`.
///
/// Skips unsupported operations (drop, rename, etc.). `old` comes from the
/// pre-image, falling back to `documentKey` so update/delete always carry `_id`.
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

    // Wall time is millisecond resolution; cluster time only seconds.
    let timestamp_us = event
        .wall_time
        .map(|wt| wt.timestamp_millis() * 1000)
        .or_else(|| event.cluster_time.map(|ts| ts.time as i64 * 1_000_000))
        .unwrap_or(0);

    let document_key_row = event.document_key.as_ref().map(document_to_row);

    let new = event.full_document.as_ref().map(document_to_row);
    let old = match op {
        ChangeOp::Update | ChangeOp::Delete => event
            .full_document_before_change
            .as_ref()
            .map(document_to_row)
            .or_else(|| document_key_row.clone()),
        _ => None,
    };

    // No full document: removed before `updateLookup` resolved it. The later
    // delete event covers it, so skip rather than emit a row-less change.
    if matches!(op, ChangeOp::Insert | ChangeOp::Update) && new.is_none() {
        tracing::warn!(
            collection,
            op_type = ?event.operation_type,
            "change event has no full document (document removed before lookup), skipping"
        );
        return Ok(ConvertResult::Skip);
    }

    if op == ChangeOp::Delete && old.is_none() {
        tracing::warn!(
            collection,
            "delete event has neither pre-image nor documentKey, skipping"
        );
        return Ok(ConvertResult::Skip);
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ColumnValue;
    use mongodb::bson::{doc, Timestamp};

    /// Build a `ChangeStreamEvent` from a raw change event document, the same
    /// way the driver deserializes events off the wire.
    fn event_from_doc(doc: Document) -> ChangeStreamEvent<Document> {
        mongodb::bson::from_document(doc).expect("failed to deserialize change event")
    }

    /// A minimal change event document with the given operation type.
    fn base_event(op_type: &str, coll: &str) -> Document {
        doc! {
            "_id": { "_data": "8265F1A2B3000000012B" },
            "operationType": op_type,
            "ns": { "db": "mydb", "coll": coll },
            "clusterTime": Timestamp { time: 1_700_000_000, increment: 1 },
        }
    }

    fn expect_event(result: ConvertResult) -> CdcEvent {
        match result {
            ConvertResult::Event(e) => e,
            ConvertResult::Skip => panic!("expected Event, got Skip"),
        }
    }

    #[test]
    fn test_insert_event_maps_to_insert() {
        let mut raw = base_event("insert", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1", "name": "Alice" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.op, ChangeOp::Insert);
        assert_eq!(event.table.schema, "mydb");
        assert_eq!(event.table.name, "users");
        assert_eq!(
            event.new.as_ref().and_then(|r| r.get("name")),
            Some(&ColumnValue::Text("Alice".into()))
        );
        assert!(event.old.is_none(), "insert must not carry an old row");
    }

    #[test]
    fn test_update_event_maps_to_update_with_full_document() {
        let mut raw = base_event("update", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1", "name": "Alice2" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.op, ChangeOp::Update);
        assert_eq!(
            event.new.as_ref().and_then(|r| r.get("name")),
            Some(&ColumnValue::Text("Alice2".into()))
        );
    }

    #[test]
    fn test_replace_event_maps_to_update() {
        let mut raw = base_event("replace", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1", "name": "Replaced" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.op, ChangeOp::Update);
    }

    #[test]
    fn test_update_without_pre_image_uses_document_key_as_old() {
        // Pre-images are disabled by default in MongoDB; sinks still need the
        // primary key to target the row being replaced.
        let mut raw = base_event("update", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1", "name": "Alice2" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        let old = event
            .old
            .expect("update should carry an old row with the pk");
        assert_eq!(old.get("_id"), Some(&ColumnValue::Text("u1".into())));
    }

    #[test]
    fn test_update_prefers_pre_image_over_document_key() {
        let mut raw = base_event("update", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1", "name": "new" });
        raw.insert(
            "fullDocumentBeforeChange",
            doc! { "_id": "u1", "name": "old" },
        );

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        let old = event.old.expect("update should carry a pre-image");
        assert_eq!(old.get("name"), Some(&ColumnValue::Text("old".into())));
    }

    #[test]
    fn test_delete_without_pre_image_uses_document_key_as_old() {
        let mut raw = base_event("delete", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.op, ChangeOp::Delete);
        assert!(event.new.is_none());
        let old = event.old.expect("delete must carry an old row with the pk");
        assert_eq!(old.get("_id"), Some(&ColumnValue::Text("u1".into())));
    }

    #[test]
    fn test_delete_with_pre_image_carries_full_old_row() {
        let mut raw = base_event("delete", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert(
            "fullDocumentBeforeChange",
            doc! { "_id": "u1", "name": "Alice" },
        );

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        let old = event.old.unwrap();
        assert_eq!(old.get("name"), Some(&ColumnValue::Text("Alice".into())));
    }

    #[test]
    fn test_delete_without_document_key_is_skipped() {
        let raw = base_event("delete", "users");

        let result = change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap();

        assert!(
            matches!(result, ConvertResult::Skip),
            "a delete with no way to identify the row must be skipped"
        );
    }

    #[test]
    fn test_update_without_full_document_is_skipped() {
        // The document was deleted before `updateLookup` could resolve it.
        let mut raw = base_event("update", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });

        let result = change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap();

        assert!(matches!(result, ConvertResult::Skip));
    }

    #[test]
    fn test_unsupported_operation_types_are_skipped() {
        for op_type in ["drop", "rename", "dropDatabase", "invalidate"] {
            let raw = base_event(op_type, "users");
            let result = change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap();
            assert!(
                matches!(result, ConvertResult::Skip),
                "expected {op_type} to be skipped"
            );
        }
    }

    #[test]
    fn test_missing_namespace_falls_back_to_unknown_collection() {
        let mut raw = base_event("insert", "users");
        raw.remove("ns");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.table.name, "unknown");
    }

    #[test]
    fn test_wall_time_is_preferred_over_cluster_time() {
        let mut raw = base_event("insert", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1" });
        // Wall time has millisecond resolution; cluster time only seconds.
        raw.insert(
            "wallTime",
            mongodb::bson::DateTime::from_millis(1_700_000_000_123),
        );

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.timestamp_us, 1_700_000_000_123 * 1000);
    }

    #[test]
    fn test_cluster_time_used_when_wall_time_missing() {
        let mut raw = base_event("insert", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.timestamp_us, 1_700_000_000 * 1_000_000);
    }

    #[test]
    fn test_primary_key_is_always_id() {
        let mut raw = base_event("insert", "users");
        raw.insert("documentKey", doc! { "_id": "u1" });
        raw.insert("fullDocument", doc! { "_id": "u1" });

        let event = expect_event(change_event_to_cdc_event(&event_from_doc(raw), "mydb").unwrap());

        assert_eq!(event.primary_key_columns, vec!["_id".to_string()]);
    }

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
