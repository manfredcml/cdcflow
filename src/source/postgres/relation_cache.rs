use std::collections::HashMap;

use crate::error::{CdcError, Result};
use crate::event::{ColumnValue, Row};

use super::protocol::messages::{RelationBody, TupleColumn, TupleData};

/// Parse a PostgreSQL text-format value into a typed ColumnValue based on the column's type OID.
///
/// PostgreSQL logical replication sends all values as text. This function uses the type OID
/// from the Relation message to parse them into appropriate typed variants.
/// On parse failure, falls back to `ColumnValue::Text`.
pub(crate) fn parse_pg_text(type_oid: u32, text: &str) -> ColumnValue {
    match type_oid {
        // bool
        16 => ColumnValue::Bool(text == "t"),
        // int2, int4, int8, oid
        20 | 21 | 23 | 26 => match text.parse::<i64>() {
            Ok(n) => ColumnValue::Int(n),
            Err(_) => ColumnValue::Text(text.to_owned()),
        },
        // float4, float8
        700 | 701 => match text.parse::<f64>() {
            Ok(f) => ColumnValue::float(f),
            Err(_) => ColumnValue::Text(text.to_owned()),
        },
        // bytea — PostgreSQL sends hex-encoded: \x4445414442454546
        17 => {
            let hex = text.strip_prefix("\\x").unwrap_or(text);
            match (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                .collect::<std::result::Result<Vec<u8>, _>>()
            {
                Ok(bytes) => ColumnValue::Bytes(bytes),
                Err(_) => ColumnValue::Text(text.to_owned()),
            }
        }
        // date
        1082 => ColumnValue::Date(text.to_owned()),
        // time, timetz
        1083 | 1266 => ColumnValue::Time(text.to_owned()),
        // timestamp, timestamptz
        1114 | 1184 => ColumnValue::Timestamp(text.to_owned()),
        // text, varchar, json, jsonb, uuid, numeric, and everything else
        _ => ColumnValue::Text(text.to_owned()),
    }
}

/// Caches Relation messages by OID so that Insert/Update/Delete messages
/// can resolve their tuple data into named columns.
#[derive(Debug, Default)]
pub struct RelationCache {
    relations: HashMap<u32, RelationBody>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store or update a relation definition.
    pub fn update(&mut self, relation: RelationBody) {
        self.relations.insert(relation.rel_id, relation);
    }

    /// Look up a relation by OID.
    pub fn get(&self, rel_id: u32) -> Option<&RelationBody> {
        self.relations.get(&rel_id)
    }

    /// Resolve tuple data into a named Row using the cached schema for the given relation OID.
    pub fn resolve_tuple(&self, rel_id: u32, tuple: &TupleData) -> Result<Row> {
        let relation = self
            .relations
            .get(&rel_id)
            .ok_or_else(|| CdcError::Protocol(format!("no cached relation for OID {rel_id}")))?;

        if tuple.columns.len() != relation.columns.len() {
            return Err(CdcError::Protocol(format!(
                "column count mismatch for {}.{}: expected {}, got {}",
                relation.namespace,
                relation.name,
                relation.columns.len(),
                tuple.columns.len()
            )));
        }

        let row: Row = relation
            .columns
            .iter()
            .zip(tuple.columns.iter())
            .map(|(col_def, col_val)| {
                let value = match col_val {
                    TupleColumn::Null => ColumnValue::Null,
                    TupleColumn::UnchangedToast => ColumnValue::UnchangedToast,
                    TupleColumn::Text(s) => parse_pg_text(col_def.type_oid, s),
                };
                (col_def.name.clone(), value)
            })
            .collect();

        Ok(row)
    }

    /// Get the schema and table name for a relation OID.
    pub fn table_info(&self, rel_id: u32) -> Option<(&str, &str)> {
        self.relations
            .get(&rel_id)
            .map(|r| (r.namespace.as_str(), r.name.as_str()))
    }

    /// Get the primary key column names for a relation OID.
    ///
    /// In PostgreSQL's logical replication protocol, `ColumnDef.flags & 1` indicates
    /// the column is part of the table's replica identity (typically the primary key).
    pub fn primary_key_columns(&self, rel_id: u32) -> Vec<String> {
        match self.relations.get(&rel_id) {
            Some(rel) => rel
                .columns
                .iter()
                .filter(|col| col.flags & 1 != 0)
                .map(|col| col.name.clone())
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.relations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::postgres::protocol::messages::ColumnDef;

    fn make_relation(rel_id: u32, namespace: &str, name: &str, cols: &[&str]) -> RelationBody {
        RelationBody {
            rel_id,
            namespace: namespace.into(),
            name: name.into(),
            replica_identity: b'd',
            columns: cols
                .iter()
                .map(|&n| ColumnDef {
                    flags: 0,
                    name: n.into(),
                    type_oid: 25, // text
                    type_modifier: -1,
                })
                .collect(),
        }
    }

    fn make_typed_relation(
        rel_id: u32,
        namespace: &str,
        name: &str,
        cols: &[(&str, u32)],
    ) -> RelationBody {
        RelationBody {
            rel_id,
            namespace: namespace.into(),
            name: name.into(),
            replica_identity: b'd',
            columns: cols
                .iter()
                .map(|(n, oid)| ColumnDef {
                    flags: 0,
                    name: (*n).into(),
                    type_oid: *oid,
                    type_modifier: -1,
                })
                .collect(),
        }
    }

    #[test]
    fn test_cache_update_and_get() {
        let mut cache = RelationCache::new();
        assert!(cache.is_empty());

        let rel = make_relation(16384, "public", "users", &["id", "name"]);
        cache.update(rel.clone());

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        let cached = cache.get(16384).unwrap();
        assert_eq!(cached.name, "users");
        assert_eq!(cached.columns.len(), 2);
    }

    #[test]
    fn test_cache_miss() {
        let cache = RelationCache::new();
        assert!(cache.get(99999).is_none());
    }

    #[test]
    fn test_cache_overwrite() {
        let mut cache = RelationCache::new();

        let rel1 = make_relation(16384, "public", "users", &["id", "name"]);
        cache.update(rel1);

        // Schema change: add email column
        let rel2 = make_relation(16384, "public", "users", &["id", "name", "email"]);
        cache.update(rel2);

        let cached = cache.get(16384).unwrap();
        assert_eq!(cached.columns.len(), 3);
        assert_eq!(cache.len(), 1); // still just one relation
    }

    #[test]
    fn test_resolve_tuple() {
        let mut cache = RelationCache::new();
        cache.update(make_relation(1, "public", "t", &["id", "name"]));

        let tuple = TupleData {
            columns: vec![
                TupleColumn::Text("42".into()),
                TupleColumn::Text("Alice".into()),
            ],
        };

        let row = cache.resolve_tuple(1, &tuple).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row.get("id"), Some(&ColumnValue::Text("42".into())));
        assert_eq!(row.get("name"), Some(&ColumnValue::Text("Alice".into())));
    }

    #[test]
    fn test_resolve_tuple_with_null_and_toast() {
        let mut cache = RelationCache::new();
        cache.update(make_relation(1, "public", "t", &["id", "data", "blob"]));

        let tuple = TupleData {
            columns: vec![
                TupleColumn::Text("1".into()),
                TupleColumn::Null,
                TupleColumn::UnchangedToast,
            ],
        };

        let row = cache.resolve_tuple(1, &tuple).unwrap();
        assert_eq!(row.get("id"), Some(&ColumnValue::Text("1".into())));
        assert_eq!(row.get("data"), Some(&ColumnValue::Null));
        assert_eq!(row.get("blob"), Some(&ColumnValue::UnchangedToast));
    }

    #[test]
    fn test_resolve_tuple_missing_relation() {
        let cache = RelationCache::new();
        let tuple = TupleData {
            columns: vec![TupleColumn::Null],
        };
        assert!(cache.resolve_tuple(999, &tuple).is_err());
    }

    #[test]
    fn test_resolve_tuple_column_count_mismatch() {
        let mut cache = RelationCache::new();
        cache.update(make_relation(1, "public", "t", &["id", "name"]));

        let tuple = TupleData {
            columns: vec![TupleColumn::Text("1".into())], // only 1 column, expected 2
        };
        assert!(cache.resolve_tuple(1, &tuple).is_err());
    }

    #[test]
    fn test_table_info() {
        let mut cache = RelationCache::new();
        cache.update(make_relation(1, "myschema", "orders", &["id"]));

        assert_eq!(cache.table_info(1), Some(("myschema", "orders")));
        assert_eq!(cache.table_info(999), None);
    }

    #[test]
    fn test_multiple_relations() {
        let mut cache = RelationCache::new();
        cache.update(make_relation(1, "public", "users", &["id"]));
        cache.update(make_relation(2, "public", "orders", &["id", "user_id"]));

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(1).unwrap().name, "users");
        assert_eq!(cache.get(2).unwrap().name, "orders");
    }

    #[test]
    fn test_resolve_tuple_typed() {
        let mut cache = RelationCache::new();
        cache.update(make_typed_relation(
            1,
            "public",
            "t",
            &[("id", 23), ("name", 25), ("active", 16), ("score", 701)],
        ));

        let tuple = TupleData {
            columns: vec![
                TupleColumn::Text("42".into()),
                TupleColumn::Text("Alice".into()),
                TupleColumn::Text("t".into()),
                TupleColumn::Text("99.5".into()),
            ],
        };

        let row = cache.resolve_tuple(1, &tuple).unwrap();
        assert_eq!(row.get("id"), Some(&ColumnValue::Int(42)));
        assert_eq!(row.get("name"), Some(&ColumnValue::Text("Alice".into())));
        assert_eq!(row.get("active"), Some(&ColumnValue::Bool(true)));
        assert_eq!(row.get("score"), Some(&ColumnValue::float(99.5)));
    }

    #[test]
    fn test_primary_key_columns_single_pk() {
        let mut cache = RelationCache::new();
        let rel = RelationBody {
            rel_id: 1,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'd',
            columns: vec![
                ColumnDef {
                    flags: 1,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 0,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 0,
                    name: "email".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        cache.update(rel);
        assert_eq!(cache.primary_key_columns(1), vec!["id"]);
    }

    #[test]
    fn test_primary_key_columns_composite_pk() {
        let mut cache = RelationCache::new();
        let rel = RelationBody {
            rel_id: 1,
            namespace: "public".into(),
            name: "order_items".into(),
            replica_identity: b'd',
            columns: vec![
                ColumnDef {
                    flags: 1,
                    name: "order_id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 1,
                    name: "item_id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 0,
                    name: "quantity".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
            ],
        };
        cache.update(rel);
        assert_eq!(cache.primary_key_columns(1), vec!["order_id", "item_id"]);
    }

    #[test]
    fn test_primary_key_columns_no_pk() {
        let mut cache = RelationCache::new();
        cache.update(make_relation(1, "public", "logs", &["id", "message"]));
        // make_relation sets flags: 0 for all columns
        assert!(cache.primary_key_columns(1).is_empty());
    }

    #[test]
    fn test_primary_key_columns_missing_relation() {
        let cache = RelationCache::new();
        assert!(cache.primary_key_columns(999).is_empty());
    }

    #[test]
    fn test_parse_pg_text_bool() {
        assert_eq!(parse_pg_text(16, "t"), ColumnValue::Bool(true));
        assert_eq!(parse_pg_text(16, "f"), ColumnValue::Bool(false));
    }

    #[test]
    fn test_parse_pg_text_integers() {
        // int2 (OID 21)
        assert_eq!(parse_pg_text(21, "32767"), ColumnValue::Int(32767));
        // int4 (OID 23)
        assert_eq!(parse_pg_text(23, "42"), ColumnValue::Int(42));
        assert_eq!(parse_pg_text(23, "-1"), ColumnValue::Int(-1));
        // int8 (OID 20)
        assert_eq!(
            parse_pg_text(20, "9223372036854775807"),
            ColumnValue::Int(i64::MAX)
        );
        // oid (OID 26)
        assert_eq!(parse_pg_text(26, "16384"), ColumnValue::Int(16384));
        // parse failure falls back to Text
        assert_eq!(parse_pg_text(23, "abc"), ColumnValue::Text("abc".into()));
    }

    #[test]
    fn test_parse_pg_text_floats() {
        assert_eq!(parse_pg_text(700, "3.14"), ColumnValue::float(3.14));
        assert_eq!(parse_pg_text(701, "-1.5e10"), ColumnValue::float(-1.5e10));
        // parse failure
        assert_eq!(parse_pg_text(701, "NaN"), ColumnValue::float(f64::NAN));
        assert_eq!(
            parse_pg_text(700, "not_a_float"),
            ColumnValue::Text("not_a_float".into())
        );
    }

    #[test]
    fn test_parse_pg_text_bytea() {
        assert_eq!(
            parse_pg_text(17, "\\xDEADBEEF"),
            ColumnValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
        // Bad hex falls back to Text
        assert_eq!(
            parse_pg_text(17, "\\xZZZZ"),
            ColumnValue::Text("\\xZZZZ".into())
        );
    }

    #[test]
    fn test_parse_pg_text_temporal() {
        assert_eq!(
            parse_pg_text(1082, "2024-01-15"),
            ColumnValue::Date("2024-01-15".into())
        );
        assert_eq!(
            parse_pg_text(1083, "10:30:00"),
            ColumnValue::Time("10:30:00".into())
        );
        assert_eq!(
            parse_pg_text(1266, "10:30:00+05"),
            ColumnValue::Time("10:30:00+05".into())
        );
        assert_eq!(
            parse_pg_text(1114, "2024-01-15 10:30:00"),
            ColumnValue::Timestamp("2024-01-15 10:30:00".into())
        );
        assert_eq!(
            parse_pg_text(1184, "2024-01-15 10:30:00+00"),
            ColumnValue::Timestamp("2024-01-15 10:30:00+00".into())
        );
    }

    #[test]
    fn test_parse_pg_text_fallback() {
        // text (25), varchar (1043), json (114), jsonb (3802), uuid (2950), numeric (1700)
        for oid in [25, 1043, 114, 3802, 2950, 1700] {
            assert_eq!(
                parse_pg_text(oid, "some_value"),
                ColumnValue::Text("some_value".into())
            );
        }
    }
}
