use serde_json::json;

use crate::event::{CdcEvent, ChangeOp, ColumnValue, Row};

/// Convert a `ColumnValue` to an `Option<String>` for text-based storage.
///
/// Returns None for Null and UnchangedToast values.
pub fn column_value_to_string(value: Option<&ColumnValue>) -> Option<String> {
    match value {
        None | Some(ColumnValue::Null) | Some(ColumnValue::UnchangedToast) => None,
        Some(ColumnValue::Bool(b)) => Some(b.to_string()),
        Some(ColumnValue::Int(i)) => Some(i.to_string()),
        Some(ColumnValue::Float(f)) => {
            let v = f.into_inner();
            if v.is_nan() || v.is_infinite() {
                None
            } else {
                Some(v.to_string())
            }
        }
        Some(ColumnValue::Text(s)) => Some(s.clone()),
        Some(ColumnValue::Bytes(b)) => Some(b.iter().map(|byte| format!("{:02x}", byte)).collect()),
        Some(ColumnValue::Timestamp(s)) => Some(s.clone()),
        Some(ColumnValue::Date(s)) => Some(s.clone()),
        Some(ColumnValue::Time(s)) => Some(s.clone()),
    }
}

/// Convert a ColumnValue to a plain JSON value (no tagged enum wrappers).
///
/// Unlike serde's default serialization which produces `{"Int": 1}`,
/// this produces plain JSON: `1`, `"hello"`, `true`, `null`.
pub fn column_value_to_json(value: &ColumnValue) -> serde_json::Value {
    match value {
        ColumnValue::Null | ColumnValue::UnchangedToast => serde_json::Value::Null,
        ColumnValue::Bool(b) => json!(*b),
        ColumnValue::Int(i) => json!(*i),
        ColumnValue::Float(f) => {
            let v = f.into_inner();
            if v.is_nan() || v.is_infinite() {
                serde_json::Value::Null
            } else {
                json!(v)
            }
        }
        ColumnValue::Text(s) => json!(s),
        ColumnValue::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            json!(hex)
        }
        ColumnValue::Timestamp(s) => json!(s),
        ColumnValue::Date(s) => json!(s),
        ColumnValue::Time(s) => json!(s),
    }
}

/// Return the type name for a ColumnValue variant.
///
/// Used to populate the `types` map in the nested CDC output format,
/// so downstream consumers know the original type without schema inference.
pub fn column_value_type_name(value: &ColumnValue) -> &'static str {
    match value {
        ColumnValue::Null => "Null",
        ColumnValue::Bool(_) => "Bool",
        ColumnValue::Int(_) => "Int",
        ColumnValue::Float(_) => "Float",
        ColumnValue::Text(_) => "Text",
        ColumnValue::Bytes(_) => "Bytes",
        ColumnValue::Timestamp(_) => "Timestamp",
        ColumnValue::Date(_) => "Date",
        ColumnValue::Time(_) => "Time",
        ColumnValue::UnchangedToast => "UnchangedToast",
    }
}

/// Convert a Row to a plain JSON object (no tagged enum wrappers).
pub fn row_to_json(row: &Row) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, value) in row {
        map.insert(key.clone(), column_value_to_json(value));
    }
    serde_json::Value::Object(map)
}

/// Convert a Row to a typed JSON object with both values and type names.
///
/// Returns: `{"values": {"id": 123, ...}, "types": {"id": "Int", ...}}`
pub fn row_to_typed_json(row: &Row) -> serde_json::Value {
    let mut values_map = serde_json::Map::new();
    let mut types_map = serde_json::Map::new();
    for (key, value) in row {
        values_map.insert(key.clone(), column_value_to_json(value));
        types_map.insert(key.clone(), json!(column_value_type_name(value)));
    }
    json!({
        "values": serde_json::Value::Object(values_map),
        "types": serde_json::Value::Object(types_map),
    })
}

/// Convert a CdcEvent to a nested JSON object with `metadata` and `row` sections.
///
/// Produces:
/// ```json
/// {
///   "metadata": {
///     "op": "U",
///     "lsn": 12345,
///     "timestamp_us": 1772526950093907,
///     "snapshot": false,
///     "schema": "public",
///     "table": "users"
///   },
///   "row": {
///     "new": {"values": {"id": 123, "name": "Alice"}, "types": {"id": "Int", "name": "Text"}},
///     "old": null
///   }
/// }
/// ```
///
/// Row semantics:
/// - Insert:   `new: {...}, old: null`
/// - Update:   `new: {...}, old: {...}` (or `old: null` if unavailable)
/// - Delete:   `new: null,  old: {...}`
/// - Truncate: `new: null,  old: null`
/// - Snapshot: `new: {...}, old: null`
pub fn event_to_json(event: &CdcEvent) -> serde_json::Value {
    let metadata = json!({
        "op": event.op.as_op_code(),
        "lsn": event.lsn.0,
        "timestamp_us": event.timestamp_us,
        "snapshot": event.op == ChangeOp::Snapshot,
        "schema": event.table.schema,
        "table": event.table.name,
        "primary_key_columns": event.primary_key_columns,
    });

    let row_new = event.new.as_ref().map(|r| row_to_typed_json(r));
    let row_old = event.old.as_ref().map(|r| row_to_typed_json(r));

    json!({
        "metadata": metadata,
        "new": row_new,
        "old": row_old,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Lsn, TableId};
    use ordered_float::OrderedFloat;
    use std::collections::BTreeMap;

    #[test]
    fn test_column_value_to_string_all_variants() {
        assert_eq!(column_value_to_string(None), None);
        assert_eq!(column_value_to_string(Some(&ColumnValue::Null)), None);
        assert_eq!(
            column_value_to_string(Some(&ColumnValue::UnchangedToast)),
            None
        );
        assert_eq!(
            column_value_to_string(Some(&ColumnValue::Bool(true))),
            Some("true".into())
        );
        assert_eq!(
            column_value_to_string(Some(&ColumnValue::Bool(false))),
            Some("false".into())
        );
        assert_eq!(
            column_value_to_string(Some(&ColumnValue::Int(42))),
            Some("42".into())
        );
        assert_eq!(
            column_value_to_string(Some(&ColumnValue::Int(-1))),
            Some("-1".into())
        );
        assert_eq!(
            column_value_to_string(Some(&ColumnValue::Float(OrderedFloat(3.14)))),
            Some("3.14".into())
        );

        let text_val = ColumnValue::Text("hello".into());
        assert_eq!(
            column_value_to_string(Some(&text_val)),
            Some("hello".into())
        );

        let ts_val = ColumnValue::Timestamp("2024-01-01T00:00:00".into());
        assert_eq!(
            column_value_to_string(Some(&ts_val)),
            Some("2024-01-01T00:00:00".into())
        );

        let date_val = ColumnValue::Date("2024-01-01".into());
        assert_eq!(
            column_value_to_string(Some(&date_val)),
            Some("2024-01-01".into())
        );

        let time_val = ColumnValue::Time("12:30:00".into());
        assert_eq!(
            column_value_to_string(Some(&time_val)),
            Some("12:30:00".into())
        );
    }

    #[test]
    fn test_column_value_to_string_bytes() {
        let result = column_value_to_string(Some(&ColumnValue::Bytes(vec![0xDE, 0xAD])));
        assert_eq!(result, Some("dead".into()));
    }

    #[test]
    fn test_column_value_to_string_float_nan() {
        let result = column_value_to_string(Some(&ColumnValue::Float(OrderedFloat(f64::NAN))));
        assert_eq!(result, None);
    }

    #[test]
    fn test_column_value_to_string_float_infinity() {
        let result = column_value_to_string(Some(&ColumnValue::Float(OrderedFloat(f64::INFINITY))));
        assert_eq!(result, None);
    }

    #[test]
    fn test_column_value_to_json_all_variants() {
        assert_eq!(column_value_to_json(&ColumnValue::Null), json!(null));
        assert_eq!(
            column_value_to_json(&ColumnValue::UnchangedToast),
            json!(null)
        );
        assert_eq!(column_value_to_json(&ColumnValue::Bool(true)), json!(true));
        assert_eq!(column_value_to_json(&ColumnValue::Int(42)), json!(42));
        assert_eq!(
            column_value_to_json(&ColumnValue::Float(OrderedFloat(3.14))),
            json!(3.14)
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Text("hello".into())),
            json!("hello")
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Bytes(vec![0xDE, 0xAD])),
            json!("dead")
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Timestamp("2024-01-01T00:00:00".into())),
            json!("2024-01-01T00:00:00")
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Date("2024-01-01".into())),
            json!("2024-01-01")
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Time("12:30:00".into())),
            json!("12:30:00")
        );
    }

    #[test]
    fn test_column_value_to_json_float_nan() {
        assert_eq!(
            column_value_to_json(&ColumnValue::Float(OrderedFloat(f64::NAN))),
            json!(null)
        );
    }

    #[test]
    fn test_column_value_to_json_float_infinity() {
        assert_eq!(
            column_value_to_json(&ColumnValue::Float(OrderedFloat(f64::INFINITY))),
            json!(null)
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Float(OrderedFloat(f64::NEG_INFINITY))),
            json!(null)
        );
    }

    #[test]
    fn test_column_value_type_name_all_variants() {
        assert_eq!(column_value_type_name(&ColumnValue::Null), "Null");
        assert_eq!(column_value_type_name(&ColumnValue::Bool(true)), "Bool");
        assert_eq!(column_value_type_name(&ColumnValue::Int(42)), "Int");
        assert_eq!(
            column_value_type_name(&ColumnValue::Float(OrderedFloat(3.14))),
            "Float"
        );
        assert_eq!(
            column_value_type_name(&ColumnValue::Text("hello".into())),
            "Text"
        );
        assert_eq!(
            column_value_type_name(&ColumnValue::Bytes(vec![0xDE, 0xAD])),
            "Bytes"
        );
        assert_eq!(
            column_value_type_name(&ColumnValue::Timestamp("2024-01-01T00:00:00".into())),
            "Timestamp"
        );
        assert_eq!(
            column_value_type_name(&ColumnValue::Date("2024-01-01".into())),
            "Date"
        );
        assert_eq!(
            column_value_type_name(&ColumnValue::Time("12:30:00".into())),
            "Time"
        );
        assert_eq!(
            column_value_type_name(&ColumnValue::UnchangedToast),
            "UnchangedToast"
        );
    }

    #[test]
    fn test_row_to_json() {
        let row = BTreeMap::from([
            ("id".into(), ColumnValue::Int(1)),
            ("name".into(), ColumnValue::Text("Alice".into())),
            ("active".into(), ColumnValue::Bool(true)),
        ]);
        let json = row_to_json(&row);
        assert_eq!(json["id"], 1);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["active"], true);
    }

    #[test]
    fn test_row_to_json_with_null() {
        let row = BTreeMap::from([
            ("id".into(), ColumnValue::Int(1)),
            ("email".into(), ColumnValue::Null),
        ]);
        let json = row_to_json(&row);
        assert_eq!(json["id"], 1);
        assert!(json["email"].is_null());
    }

    #[test]
    fn test_row_to_typed_json() {
        let row = BTreeMap::from([
            ("id".into(), ColumnValue::Int(1)),
            ("name".into(), ColumnValue::Text("Alice".into())),
        ]);
        let json = row_to_typed_json(&row);
        assert_eq!(json["values"]["id"], 1);
        assert_eq!(json["values"]["name"], "Alice");
        assert_eq!(json["types"]["id"], "Int");
        assert_eq!(json["types"]["name"], "Text");
    }

    #[test]
    fn test_row_to_typed_json_with_null() {
        let row = BTreeMap::from([
            ("id".into(), ColumnValue::Int(1)),
            ("email".into(), ColumnValue::Null),
        ]);
        let json = row_to_typed_json(&row);
        assert_eq!(json["values"]["id"], 1);
        assert!(json["values"]["email"].is_null());
        assert_eq!(json["types"]["id"], "Int");
        assert_eq!(json["types"]["email"], "Null");
    }

    fn make_table_id() -> TableId {
        TableId {
            schema: "public".into(),
            name: "users".into(),
            oid: 0,
        }
    }

    fn make_event(
        op: ChangeOp,
        new: Option<BTreeMap<String, ColumnValue>>,
        old: Option<BTreeMap<String, ColumnValue>>,
    ) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(12345),
            timestamp_us: 1_000_000,
            xid: 1,
            table: make_table_id(),
            op,
            new,
            old,
            primary_key_columns: vec!["id".into()],
        }
    }

    #[test]
    fn test_event_to_json_insert() {
        let event = make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            None,
        );
        let json = event_to_json(&event);
        assert_eq!(json["metadata"]["op"], "I");
        assert_eq!(json["metadata"]["lsn"], 12345);
        assert_eq!(json["metadata"]["snapshot"], false);
        assert_eq!(json["metadata"]["schema"], "public");
        assert_eq!(json["metadata"]["table"], "users");
        assert_eq!(json["new"]["values"]["id"], 1);
        assert_eq!(json["new"]["values"]["name"], "Alice");
        assert_eq!(json["new"]["types"]["id"], "Int");
        assert_eq!(json["new"]["types"]["name"], "Text");
        assert!(json["old"].is_null());
    }

    #[test]
    fn test_event_to_json_update_with_old() {
        let event = make_event(
            ChangeOp::Update,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("OldAlice".into())),
            ])),
        );
        let json = event_to_json(&event);
        assert_eq!(json["metadata"]["op"], "U");
        assert_eq!(json["new"]["values"]["name"], "Alice");
        assert_eq!(json["old"]["values"]["name"], "OldAlice");
        assert_eq!(json["new"]["types"]["name"], "Text");
        assert_eq!(json["old"]["types"]["name"], "Text");
    }

    #[test]
    fn test_event_to_json_delete() {
        let event = make_event(
            ChangeOp::Delete,
            None,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
        );
        let json = event_to_json(&event);
        assert_eq!(json["metadata"]["op"], "D");
        assert!(json["new"].is_null());
        assert_eq!(json["old"]["values"]["id"], 1);
        assert_eq!(json["old"]["values"]["name"], "Alice");
    }

    #[test]
    fn test_event_to_json_snapshot() {
        let event = make_event(
            ChangeOp::Snapshot,
            Some(BTreeMap::from([("id".into(), ColumnValue::Int(1))])),
            None,
        );
        let json = event_to_json(&event);
        assert_eq!(json["metadata"]["op"], "S");
        assert_eq!(json["metadata"]["snapshot"], true);
        assert_eq!(json["new"]["values"]["id"], 1);
        assert!(json["old"].is_null());
    }

    #[test]
    fn test_event_to_json_truncate() {
        let event = make_event(ChangeOp::Truncate, None, None);
        let json = event_to_json(&event);
        assert_eq!(json["metadata"]["op"], "T");
        assert!(json["new"].is_null());
        assert!(json["old"].is_null());
    }
}
