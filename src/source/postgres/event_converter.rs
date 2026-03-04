use std::collections::HashMap;

use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, Lsn, TableId};

use super::protocol::messages::PgOutputMessage;
use super::relation_cache::RelationCache;

/// Microseconds between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01).
/// PostgreSQL's logical replication protocol sends timestamps relative to its own epoch.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Result of converting a pgoutput message.
#[derive(Debug)]
pub enum ConvertResult {
    /// A CDC event ready to emit.
    Event(CdcEvent),
    /// A transaction was committed at this LSN.
    Commit(Lsn),
    /// A transaction began (informational).
    Begin(u32),
    /// Message was handled internally (e.g., Relation cached) — no output needed.
    Skip,
}

/// Tracks the current transaction context from Begin messages.
#[derive(Debug, Clone, Default)]
struct TxContext {
    xid: u32,
    timestamp_us: i64,
    final_lsn: u64,
}

/// Converts raw pgoutput messages into CdcEvents.
///
/// Maintains a RelationCache for schema resolution and tracks the current
/// transaction context (xid, timestamp, lsn) from Begin messages.
///
/// An optional `pk_cache` overrides WAL-based replica identity flags for
/// primary key detection, since `REPLICA IDENTITY FULL` causes all columns
/// to have `flags & 1` set.
pub struct EventConverter {
    cache: RelationCache,
    tx: Option<TxContext>,
    /// Primary key columns keyed by (schema, table), populated from pg_index at startup.
    pk_cache: HashMap<(String, String), Vec<String>>,
}

impl EventConverter {
    pub fn new(pk_cache: HashMap<(String, String), Vec<String>>) -> Self {
        Self {
            cache: RelationCache::new(),
            tx: None,
            pk_cache,
        }
    }

    /// Process a single pgoutput message and return the conversion results.
    ///
    /// Most messages produce a single result, but multi-table TRUNCATE
    /// emits one event per table.
    pub fn convert(&mut self, msg: PgOutputMessage) -> Result<Vec<ConvertResult>> {
        match msg {
            PgOutputMessage::Begin(b) => {
                self.tx = Some(TxContext {
                    xid: b.xid,
                    timestamp_us: b.timestamp + PG_EPOCH_OFFSET_US,
                    final_lsn: b.final_lsn,
                });
                Ok(vec![ConvertResult::Begin(b.xid)])
            }

            PgOutputMessage::Commit(c) => {
                self.tx = None;
                Ok(vec![ConvertResult::Commit(Lsn(c.end_lsn))])
            }

            PgOutputMessage::Relation(r) => {
                self.cache.update(r);
                Ok(vec![ConvertResult::Skip])
            }

            PgOutputMessage::Insert(i) => {
                let tx = self.require_tx()?;
                let table = self.make_table_id(i.rel_id)?;
                let new = self.cache.resolve_tuple(i.rel_id, &i.new_tuple)?;
                let pk_columns = self.actual_pk_columns(i.rel_id);
                Ok(vec![ConvertResult::Event(CdcEvent {
                    lsn: Lsn(tx.final_lsn),
                    timestamp_us: tx.timestamp_us,
                    xid: tx.xid,
                    table,
                    op: ChangeOp::Insert,
                    new: Some(new),
                    old: None,
                    primary_key_columns: pk_columns,
                })])
            }

            PgOutputMessage::Update(u) => {
                let tx = self.require_tx()?;
                let table = self.make_table_id(u.rel_id)?;
                let old = match &u.old_tuple {
                    Some(t) => Some(self.cache.resolve_tuple(u.rel_id, t)?),
                    None => None,
                };
                let new = self.cache.resolve_tuple(u.rel_id, &u.new_tuple)?;
                let pk_columns = self.actual_pk_columns(u.rel_id);
                Ok(vec![ConvertResult::Event(CdcEvent {
                    lsn: Lsn(tx.final_lsn),
                    timestamp_us: tx.timestamp_us,
                    xid: tx.xid,
                    table,
                    op: ChangeOp::Update,
                    new: Some(new),
                    old,
                    primary_key_columns: pk_columns,
                })])
            }

            PgOutputMessage::Delete(d) => {
                let tx = self.require_tx()?;
                let table = self.make_table_id(d.rel_id)?;
                let old = self.cache.resolve_tuple(d.rel_id, &d.old_tuple)?;
                let pk_columns = self.actual_pk_columns(d.rel_id);
                Ok(vec![ConvertResult::Event(CdcEvent {
                    lsn: Lsn(tx.final_lsn),
                    timestamp_us: tx.timestamp_us,
                    xid: tx.xid,
                    table,
                    op: ChangeOp::Delete,
                    new: None,
                    old: Some(old),
                    primary_key_columns: pk_columns,
                })])
            }

            PgOutputMessage::Truncate(t) => {
                let tx = self.require_tx()?;
                let mut results = Vec::with_capacity(t.rel_ids.len());
                for &rel_id in &t.rel_ids {
                    let table = self.make_table_id(rel_id)?;
                    let pk_columns = self.actual_pk_columns(rel_id);
                    results.push(ConvertResult::Event(CdcEvent {
                        lsn: Lsn(tx.final_lsn),
                        timestamp_us: tx.timestamp_us,
                        xid: tx.xid,
                        table,
                        op: ChangeOp::Truncate,
                        new: None,
                        old: None,
                        primary_key_columns: pk_columns,
                    }));
                }
                Ok(results)
            }
        }
    }

    fn require_tx(&self) -> Result<&TxContext> {
        self.tx
            .as_ref()
            .ok_or_else(|| CdcError::Protocol("data message outside transaction".into()))
    }

    fn make_table_id(&self, rel_id: u32) -> Result<TableId> {
        let (schema, name) = self.cache.table_info(rel_id).ok_or_else(|| {
            CdcError::Protocol(format!("no cached relation for OID {rel_id}"))
        })?;
        Ok(TableId {
            schema: schema.to_string(),
            name: name.to_string(),
            oid: rel_id,
        })
    }

    /// Get actual primary key columns for a relation, preferring the pk_cache
    /// (populated from pg_index) over WAL replica identity flags.
    fn actual_pk_columns(&self, rel_id: u32) -> Vec<String> {
        if let Some((schema, table)) = self.cache.table_info(rel_id) {
            if let Some(pk_cols) = self.pk_cache.get(&(schema.to_string(), table.to_string())) {
                return pk_cols.clone();
            }
        }
        // Fallback to WAL flags (correct when replica identity is DEFAULT/USING INDEX)
        self.cache.primary_key_columns(rel_id)
    }

    /// Access the relation cache (for testing or introspection).
    pub fn relation_cache(&self) -> &RelationCache {
        &self.cache
    }
}

impl Default for EventConverter {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ColumnValue;
    use crate::source::postgres::protocol::messages::*;

    fn make_relation_msg(rel_id: u32, name: &str, cols: &[&str]) -> PgOutputMessage {
        PgOutputMessage::Relation(RelationBody {
            rel_id,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: b'd',
            columns: cols
                .iter()
                .map(|&n| ColumnDef {
                    flags: 0,
                    name: n.into(),
                    type_oid: 25,
                    type_modifier: -1,
                })
                .collect(),
        })
    }

    fn make_begin_msg(xid: u32, final_lsn: u64, timestamp: i64) -> PgOutputMessage {
        PgOutputMessage::Begin(BeginBody {
            final_lsn,
            timestamp,
            xid,
        })
    }

    fn make_commit_msg(end_lsn: u64) -> PgOutputMessage {
        PgOutputMessage::Commit(CommitBody {
            flags: 0,
            commit_lsn: end_lsn,
            end_lsn,
            timestamp: 0,
        })
    }

    fn make_insert_msg(rel_id: u32, values: &[&str]) -> PgOutputMessage {
        PgOutputMessage::Insert(InsertBody {
            rel_id,
            new_tuple: TupleData {
                columns: values
                    .iter()
                    .map(|&v| TupleColumn::Text(v.into()))
                    .collect(),
            },
        })
    }

    /// Helper: convert and expect exactly one result.
    fn convert_one(converter: &mut EventConverter, msg: PgOutputMessage) -> ConvertResult {
        let mut results = converter.convert(msg).unwrap();
        assert_eq!(results.len(), 1, "expected 1 result, got {}", results.len());
        results.remove(0)
    }

    #[test]
    fn test_full_transaction_sequence() {
        let mut converter = EventConverter::new(HashMap::new());

        let result = convert_one(&mut converter, make_begin_msg(42, 0x100, 1_000_000));
        assert!(matches!(result, ConvertResult::Begin(42)));

        let result = convert_one(&mut converter, make_relation_msg(1, "users", &["id", "name"]));
        assert!(matches!(result, ConvertResult::Skip));

        let result = convert_one(&mut converter, make_insert_msg(1, &["1", "Alice"]));
        match result {
            ConvertResult::Event(e) => {
                assert_eq!(e.lsn, Lsn(0x100));
                // Begin message timestamp (1_000_000 PG-epoch μs) + PG→Unix offset
                assert_eq!(e.timestamp_us, 1_000_000 + PG_EPOCH_OFFSET_US);
                assert_eq!(e.xid, 42);
                assert_eq!(e.table.schema, "public");
                assert_eq!(e.table.name, "users");
                assert_eq!(e.table.oid, 1);
                assert!(!e.is_snapshot());
                assert_eq!(e.op, ChangeOp::Insert);
                let new = e.new.as_ref().unwrap();
                assert_eq!(new.get("id"), Some(&ColumnValue::Text("1".into())));
                assert_eq!(new.get("name"), Some(&ColumnValue::Text("Alice".into())));
            }
            _ => panic!("expected Event"),
        }

        let result = convert_one(&mut converter, make_commit_msg(0x200));
        match result {
            ConvertResult::Commit(lsn) => assert_eq!(lsn, Lsn(0x200)),
            _ => panic!("expected Commit"),
        }
    }

    #[test]
    fn test_update_with_old_tuple() {
        let mut converter = EventConverter::new(HashMap::new());

        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id", "name"]))
            .unwrap();

        let update = PgOutputMessage::Update(UpdateBody {
            rel_id: 1,
            old_tuple: Some(TupleData {
                columns: vec![
                    TupleColumn::Text("1".into()),
                    TupleColumn::Text("Alice".into()),
                ],
            }),
            new_tuple: TupleData {
                columns: vec![
                    TupleColumn::Text("1".into()),
                    TupleColumn::Text("Bob".into()),
                ],
            },
        });

        match convert_one(&mut converter, update) {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Update);
                let old = e.old.as_ref().unwrap();
                let new = e.new.as_ref().unwrap();
                assert_eq!(old.get("name"), Some(&ColumnValue::Text("Alice".into())));
                assert_eq!(new.get("name"), Some(&ColumnValue::Text("Bob".into())));
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_update_without_old_tuple() {
        let mut converter = EventConverter::new(HashMap::new());
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "t", &["id"]))
            .unwrap();

        let update = PgOutputMessage::Update(UpdateBody {
            rel_id: 1,
            old_tuple: None,
            new_tuple: TupleData {
                columns: vec![TupleColumn::Text("1".into())],
            },
        });

        match convert_one(&mut converter, update) {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Update);
                assert!(e.old.is_none());
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_delete() {
        let mut converter = EventConverter::new(HashMap::new());
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id", "name"]))
            .unwrap();

        let delete = PgOutputMessage::Delete(DeleteBody {
            rel_id: 1,
            old_tuple: TupleData {
                columns: vec![
                    TupleColumn::Text("1".into()),
                    TupleColumn::Text("Alice".into()),
                ],
            },
        });

        match convert_one(&mut converter, delete) {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Delete);
                let old = e.old.as_ref().unwrap();
                assert_eq!(old.get("id"), Some(&ColumnValue::Text("1".into())));
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_truncate_single_table() {
        let mut converter = EventConverter::new(HashMap::new());
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id"]))
            .unwrap();

        let truncate = PgOutputMessage::Truncate(TruncateBody {
            num_relations: 1,
            options: 0,
            rel_ids: vec![1],
        });

        let results = converter.convert(truncate).unwrap();
        assert_eq!(results.len(), 1);
        match &results[0] {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Truncate);
                assert_eq!(e.table.name, "users");
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_truncate_multiple_tables() {
        let mut converter = EventConverter::new(HashMap::new());
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id"]))
            .unwrap();
        converter
            .convert(make_relation_msg(2, "orders", &["id"]))
            .unwrap();

        let truncate = PgOutputMessage::Truncate(TruncateBody {
            num_relations: 2,
            options: 0,
            rel_ids: vec![1, 2],
        });

        let results = converter.convert(truncate).unwrap();
        assert_eq!(results.len(), 2);
        match &results[0] {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Truncate);
                assert_eq!(e.table.name, "users");
            }
            _ => panic!("expected Event for users"),
        }
        match &results[1] {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Truncate);
                assert_eq!(e.table.name, "orders");
            }
            _ => panic!("expected Event for orders"),
        }
    }

    #[test]
    fn test_truncate_empty_rel_ids() {
        let mut converter = EventConverter::new(HashMap::new());
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();

        let truncate = PgOutputMessage::Truncate(TruncateBody {
            num_relations: 0,
            options: 0,
            rel_ids: vec![],
        });

        let results = converter.convert(truncate).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_insert_before_relation_fails() {
        let mut converter = EventConverter::new(HashMap::new());
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();

        // Insert without a preceding Relation message for rel_id 999
        let result = converter.convert(make_insert_msg(999, &["1"]));
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_outside_transaction_fails() {
        let mut converter = EventConverter::new(HashMap::new());
        converter
            .convert(make_relation_msg(1, "t", &["id"]))
            .unwrap();

        // Insert without Begin
        let result = converter.convert(make_insert_msg(1, &["1"]));
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_change_mid_stream() {
        let mut converter = EventConverter::new(HashMap::new());

        // First transaction with 2-column schema
        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id", "name"]))
            .unwrap();
        converter
            .convert(make_insert_msg(1, &["1", "Alice"]))
            .unwrap();
        converter.convert(make_commit_msg(0x200)).unwrap();

        // Second transaction: schema changed to 3 columns
        converter.convert(make_begin_msg(2, 0x300, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id", "name", "email"]))
            .unwrap();

        match convert_one(&mut converter, make_insert_msg(1, &["2", "Bob", "bob@example.com"])) {
            ConvertResult::Event(e) => {
                assert_eq!(e.op, ChangeOp::Insert);
                let new = e.new.as_ref().unwrap();
                assert_eq!(new.len(), 3);
                assert_eq!(
                    new.get("email"),
                    Some(&ColumnValue::Text("bob@example.com".into()))
                );
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_multiple_tables_in_transaction() {
        let mut converter = EventConverter::new(HashMap::new());

        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        converter
            .convert(make_relation_msg(1, "users", &["id"]))
            .unwrap();
        converter
            .convert(make_relation_msg(2, "orders", &["id", "user_id"]))
            .unwrap();

        match convert_one(&mut converter, make_insert_msg(1, &["1"])) {
            ConvertResult::Event(e) => assert_eq!(e.table.name, "users"),
            _ => panic!("expected Event"),
        }

        match convert_one(&mut converter, make_insert_msg(2, &["100", "1"])) {
            ConvertResult::Event(e) => {
                assert_eq!(e.table.name, "orders");
                assert_eq!(e.op, ChangeOp::Insert);
                let new = e.new.as_ref().unwrap();
                assert_eq!(new.get("user_id"), Some(&ColumnValue::Text("1".into())));
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_pg_epoch_to_unix_epoch_conversion() {
        let mut converter = EventConverter::new(HashMap::new());

        // PG-epoch timestamp for 2026-03-03T08:00:00 UTC:
        // 2026-03-03 is 9558 days after 2000-01-01 → 9558 * 86400 = 825_811_200 seconds
        // + 8 hours = 825_840_000 seconds → 825_840_000_000_000 μs (PG epoch)
        let pg_timestamp_us: i64 = 825_840_000_000_000;

        converter
            .convert(make_begin_msg(1, 0x100, pg_timestamp_us))
            .unwrap();
        converter
            .convert(make_relation_msg(1, "t", &["id"]))
            .unwrap();

        match convert_one(&mut converter, make_insert_msg(1, &["1"])) {
            ConvertResult::Event(e) => {
                // Should be Unix epoch: PG timestamp + 946_684_800_000_000
                let expected_unix_us = pg_timestamp_us + PG_EPOCH_OFFSET_US;
                assert_eq!(e.timestamp_us, expected_unix_us);
                // Verify this is a reasonable 2026 timestamp (> 2025-01-01)
                assert!(e.timestamp_us > 1_735_689_600_000_000);
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_actual_pk_columns_from_cache() {
        // pk_cache entry should override WAL flags
        let pk_cache = HashMap::from([
            (("public".to_string(), "users".to_string()), vec!["id".to_string()]),
        ]);
        let mut converter = EventConverter::new(pk_cache);

        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        // Relation msg with flags=1 on ALL columns (simulating REPLICA IDENTITY FULL)
        converter
            .convert(PgOutputMessage::Relation(
                crate::source::postgres::protocol::messages::RelationBody {
                    rel_id: 1,
                    namespace: "public".into(),
                    name: "users".into(),
                    replica_identity: b'f',
                    columns: vec![
                        ColumnDef { flags: 1, name: "id".into(), type_oid: 23, type_modifier: -1 },
                        ColumnDef { flags: 1, name: "name".into(), type_oid: 25, type_modifier: -1 },
                        ColumnDef { flags: 1, name: "email".into(), type_oid: 25, type_modifier: -1 },
                    ],
                },
            ))
            .unwrap();

        match convert_one(&mut converter, make_insert_msg(1, &["1", "Alice", "a@b.com"])) {
            ConvertResult::Event(e) => {
                // pk_cache says only "id" is a PK, not all three columns
                assert_eq!(e.primary_key_columns, vec!["id"]);
            }
            _ => panic!("expected Event"),
        }
    }

    #[test]
    fn test_actual_pk_columns_fallback_to_wal_flags() {
        // No pk_cache entry for this table — should fall back to WAL flags
        let mut converter = EventConverter::new(HashMap::new());

        converter.convert(make_begin_msg(1, 0x100, 0)).unwrap();
        // Relation msg with flags=1 only on "id" (DEFAULT replica identity)
        converter
            .convert(PgOutputMessage::Relation(
                crate::source::postgres::protocol::messages::RelationBody {
                    rel_id: 1,
                    namespace: "public".into(),
                    name: "orders".into(),
                    replica_identity: b'd',
                    columns: vec![
                        ColumnDef { flags: 1, name: "id".into(), type_oid: 23, type_modifier: -1 },
                        ColumnDef { flags: 0, name: "amount".into(), type_oid: 23, type_modifier: -1 },
                    ],
                },
            ))
            .unwrap();

        match convert_one(&mut converter, make_insert_msg(1, &["1", "100"])) {
            ConvertResult::Event(e) => {
                // Falls back to WAL flags: only "id" has flags=1
                assert_eq!(e.primary_key_columns, vec!["id"]);
            }
            _ => panic!("expected Event"),
        }
    }

}
