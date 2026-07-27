use std::collections::BTreeMap;

use mongodb::bson::{Bson, Document};

use crate::event::{ColumnValue, Row};

/// Convert a BSON value to a `ColumnValue`.
///
/// Top-level fields are flattened: nested documents and arrays are serialized
/// as JSON text rather than recursed into.
pub fn bson_to_column_value(bson: &Bson) -> ColumnValue {
    match bson {
        Bson::Null => ColumnValue::Null,
        Bson::Boolean(b) => ColumnValue::Bool(*b),
        Bson::Int32(i) => ColumnValue::Int(*i as i64),
        Bson::Int64(i) => ColumnValue::Int(*i),
        Bson::Double(d) => ColumnValue::float(*d),
        Bson::String(s) => ColumnValue::Text(s.clone()),
        Bson::ObjectId(oid) => ColumnValue::Text(oid.to_hex()),
        Bson::DateTime(dt) => {
            // Convert millis to ISO 8601 via chrono. `from_timestamp_millis` handles
            // negative (pre-1970) values correctly — manual secs/nanos splitting does not.
            let ts = chrono::DateTime::from_timestamp_millis(dt.timestamp_millis())
                .unwrap_or_default()
                .to_rfc3339();
            ColumnValue::Timestamp(ts)
        }
        Bson::Binary(bin) => ColumnValue::Bytes(bin.bytes.clone()),
        Bson::Decimal128(d) => ColumnValue::Text(d.to_string()),
        Bson::Timestamp(ts) => {
            // BSON Timestamp is (seconds, increment) — used internally by oplog.
            // Format as seconds since epoch.
            ColumnValue::Text(format!("{}:{}", ts.time, ts.increment))
        }
        Bson::RegularExpression(re) => ColumnValue::Text(format!("/{}/{}", re.pattern, re.options)),
        // Nested documents and arrays → JSON text
        Bson::Document(_) | Bson::Array(_) => {
            let json = serde_json::to_string(bson).unwrap_or_default();
            ColumnValue::Text(json)
        }
        // Remaining BSON types → text representation
        _ => {
            let json = serde_json::to_string(bson).unwrap_or_default();
            ColumnValue::Text(json)
        }
    }
}

/// Convert a BSON document to a CDC `Row` (BTreeMap of field → ColumnValue).
pub fn document_to_row(doc: &Document) -> Row {
    let mut row: BTreeMap<String, ColumnValue> = BTreeMap::new();
    for (key, value) in doc {
        row.insert(key.clone(), bson_to_column_value(value));
    }
    row
}

/// Return the BSON type name string for schema inference.
pub fn bson_type_string(bson: &Bson) -> &'static str {
    match bson {
        Bson::Null => "null",
        Bson::Boolean(_) => "bool",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::ObjectId(_) => "objectid",
        Bson::DateTime(_) => "date",
        Bson::Binary(_) => "bindata",
        Bson::Decimal128(_) => "decimal",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Timestamp(_) => "string",
        Bson::RegularExpression(_) => "string",
        _ => "string",
    }
}

/// Merge two observed BSON type names for the same field into a single type
/// that can hold both.
///
/// MongoDB is schemaless, so the same field can hold different types across
/// documents. Rules:
/// - `null` carries no type information — the other type wins.
/// - Identical types stay as-is.
/// - Integer widening: `int` + `long` → `long`.
/// - Numeric widening: any int type + `double` → `double`.
/// - Anything else conflicting falls back to `string`, which maps to a text
///   column and can hold any stringified value.
pub fn merge_bson_types(existing: &str, incoming: &str) -> String {
    if existing == incoming {
        return existing.to_string();
    }
    match (existing, incoming) {
        ("null", other) | (other, "null") => other.to_string(),
        ("int", "long") | ("long", "int") => "long".to_string(),
        ("int", "double") | ("double", "int") => "double".to_string(),
        ("long", "double") | ("double", "long") => "double".to_string(),
        ("object", "array") | ("array", "object") => "object".to_string(),
        _ => "string".to_string(),
    }
}

/// Fold a document's top-level fields into an accumulating field → type map,
/// merging types when a field has already been seen with a different type.
pub fn merge_document_field_types(field_types: &mut BTreeMap<String, String>, doc: &Document) {
    for (key, value) in doc {
        let incoming = bson_type_string(value);
        match field_types.get_mut(key) {
            Some(existing) => {
                *existing = merge_bson_types(existing, incoming);
            }
            None => {
                field_types.insert(key.clone(), incoming.to_string());
            }
        }
    }
}

/// Infer schema from a set of documents by building a union of all top-level
/// fields. Types observed across documents are merged (see [`merge_bson_types`]),
/// so a field that is null in one document and an int in another is typed `int`.
pub fn infer_schema_from_documents(docs: &[Document]) -> Vec<(String, String)> {
    let mut field_types: BTreeMap<String, String> = BTreeMap::new();
    for doc in docs {
        merge_document_field_types(&mut field_types, doc);
    }
    field_types.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, oid::ObjectId, Binary, Decimal128, Regex};

    #[test]
    fn test_bson_to_column_value_primitives() {
        let cases: Vec<(Bson, ColumnValue)> = vec![
            (Bson::Null, ColumnValue::Null),
            (Bson::Boolean(true), ColumnValue::Bool(true)),
            (Bson::Boolean(false), ColumnValue::Bool(false)),
            (Bson::Int32(42), ColumnValue::Int(42)),
            (Bson::Int32(-1), ColumnValue::Int(-1)),
            (Bson::Int64(i64::MAX), ColumnValue::Int(i64::MAX)),
            (Bson::Double(3.14), ColumnValue::float(3.14)),
            (
                Bson::String("hello".into()),
                ColumnValue::Text("hello".into()),
            ),
        ];
        for (bson, expected) in cases {
            assert_eq!(bson_to_column_value(&bson), expected, "for {bson:?}");
        }
    }

    #[test]
    fn test_bson_to_column_value_objectid() {
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        let result = bson_to_column_value(&Bson::ObjectId(oid));
        assert_eq!(result, ColumnValue::Text("507f1f77bcf86cd799439011".into()));
    }

    #[test]
    fn test_bson_to_column_value_binary() {
        let bin = Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: vec![0xDE, 0xAD],
        };
        let result = bson_to_column_value(&Bson::Binary(bin));
        assert_eq!(result, ColumnValue::Bytes(vec![0xDE, 0xAD]));
    }

    #[test]
    fn test_bson_to_column_value_datetime() {
        let dt = mongodb::bson::DateTime::from_millis(1700000000000);
        let result = bson_to_column_value(&Bson::DateTime(dt));
        match result {
            ColumnValue::Timestamp(s) => {
                assert!(s.contains("2023"), "expected 2023 in timestamp: {s}");
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn test_bson_to_column_value_decimal128() {
        let d = Decimal128::from_bytes([0; 16]);
        let result = bson_to_column_value(&Bson::Decimal128(d));
        match result {
            ColumnValue::Text(_) => {} // Decimal128 stored as text
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn test_bson_to_column_value_nested_doc() {
        let bson = Bson::Document(doc! { "nested": "value" });
        let result = bson_to_column_value(&bson);
        match result {
            ColumnValue::Text(json) => {
                assert!(json.contains("nested"), "expected JSON: {json}");
            }
            other => panic!("expected Text (JSON), got {other:?}"),
        }
    }

    #[test]
    fn test_bson_to_column_value_array() {
        let bson = Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]);
        let result = bson_to_column_value(&bson);
        match result {
            ColumnValue::Text(json) => {
                assert_eq!(json, "[1,2]");
            }
            other => panic!("expected Text (JSON array), got {other:?}"),
        }
    }

    #[test]
    fn test_bson_to_column_value_regex() {
        let re = Regex {
            pattern: "^abc".into(),
            options: "i".into(),
        };
        let result = bson_to_column_value(&Bson::RegularExpression(re));
        assert_eq!(result, ColumnValue::Text("/^abc/i".into()));
    }

    #[test]
    fn test_document_to_row() {
        let doc = doc! {
            "_id": ObjectId::new(),
            "name": "Alice",
            "age": 30,
            "active": true,
            "score": 95.5,
        };
        let row = document_to_row(&doc);
        assert_eq!(row.len(), 5);
        assert!(matches!(row.get("name"), Some(ColumnValue::Text(s)) if s == "Alice"));
        assert_eq!(row.get("age"), Some(&ColumnValue::Int(30)));
        assert_eq!(row.get("active"), Some(&ColumnValue::Bool(true)));
        assert_eq!(row.get("score"), Some(&ColumnValue::float(95.5)));
    }

    #[test]
    fn test_document_to_row_empty() {
        let doc = doc! {};
        let row = document_to_row(&doc);
        assert!(row.is_empty());
    }

    #[test]
    fn test_bson_type_string() {
        let cases: Vec<(Bson, &str)> = vec![
            (Bson::Null, "null"),
            (Bson::Boolean(true), "bool"),
            (Bson::Int32(0), "int"),
            (Bson::Int64(0), "long"),
            (Bson::Double(0.0), "double"),
            (Bson::String("".into()), "string"),
            (Bson::ObjectId(ObjectId::new()), "objectid"),
            (Bson::DateTime(mongodb::bson::DateTime::now()), "date"),
            (
                Bson::Binary(Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: vec![],
                }),
                "bindata",
            ),
            (Bson::Document(doc! {}), "object"),
            (Bson::Array(vec![]), "array"),
            (Bson::Decimal128(Decimal128::from_bytes([0; 16])), "decimal"),
        ];
        for (bson, expected) in cases {
            assert_eq!(bson_type_string(&bson), expected, "for {bson:?}");
        }
    }

    #[test]
    fn test_infer_schema_from_documents() {
        let docs = vec![
            doc! { "_id": ObjectId::new(), "name": "Alice", "age": 30 },
            doc! { "_id": ObjectId::new(), "name": "Bob", "email": "bob@example.com" },
        ];
        let schema = infer_schema_from_documents(&docs);
        let names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
        // BTreeMap gives sorted order
        assert_eq!(names, vec!["_id", "age", "email", "name"]);

        // Types should be inferred from first seen
        let type_map: BTreeMap<&str, &str> = schema
            .iter()
            .map(|(n, t)| (n.as_str(), t.as_str()))
            .collect();
        assert_eq!(type_map["_id"], "objectid");
        assert_eq!(type_map["name"], "string");
        assert_eq!(type_map["age"], "int");
        assert_eq!(type_map["email"], "string");
    }

    #[test]
    fn test_infer_schema_empty() {
        let schema = infer_schema_from_documents(&[]);
        assert!(schema.is_empty());
    }

    #[test]
    fn test_bson_to_column_value_timestamp() {
        let ts = mongodb::bson::Timestamp {
            time: 1700000000,
            increment: 5,
        };
        let result = bson_to_column_value(&Bson::Timestamp(ts));
        assert_eq!(result, ColumnValue::Text("1700000000:5".into()));
    }

    #[test]
    fn test_bson_to_column_value_datetime_before_epoch() {
        // 1969-12-31T23:59:59.500Z — negative millis with a fractional part.
        let dt = mongodb::bson::DateTime::from_millis(-500);
        let result = bson_to_column_value(&Bson::DateTime(dt));
        match result {
            ColumnValue::Timestamp(s) => {
                assert!(
                    s.starts_with("1969-12-31T23:59:59.5"),
                    "expected a pre-epoch timestamp, got {s}"
                );
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn test_bson_to_column_value_min_max_key_fallback() {
        for bson in [Bson::MinKey, Bson::MaxKey, Bson::Undefined] {
            assert!(
                matches!(bson_to_column_value(&bson), ColumnValue::Text(_)),
                "expected Text fallback for {bson:?}"
            );
        }
    }

    #[test]
    fn test_bson_to_column_value_empty_binary() {
        let bin = Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: vec![],
        };
        assert_eq!(
            bson_to_column_value(&Bson::Binary(bin)),
            ColumnValue::Bytes(vec![])
        );
    }

    #[test]
    fn test_merge_bson_types_null_is_ignored() {
        assert_eq!(merge_bson_types("null", "int"), "int");
        assert_eq!(merge_bson_types("int", "null"), "int");
        assert_eq!(merge_bson_types("null", "null"), "null");
    }

    #[test]
    fn test_merge_bson_types_numeric_widening() {
        let cases = vec![
            (("int", "long"), "long"),
            (("long", "int"), "long"),
            (("int", "double"), "double"),
            (("double", "long"), "double"),
        ];
        for ((a, b), expected) in cases {
            assert_eq!(merge_bson_types(a, b), expected, "for ({a}, {b})");
        }
    }

    #[test]
    fn test_merge_bson_types_conflict_falls_back_to_string() {
        assert_eq!(merge_bson_types("int", "bool"), "string");
        assert_eq!(merge_bson_types("date", "objectid"), "string");
        assert_eq!(merge_bson_types("object", "array"), "object");
    }

    #[test]
    fn test_merge_bson_types_identical() {
        assert_eq!(merge_bson_types("string", "string"), "string");
    }

    #[test]
    fn test_infer_schema_ignores_null_when_typing_field() {
        let docs = vec![doc! { "score": Bson::Null }, doc! { "score": 42i32 }];
        let schema = infer_schema_from_documents(&docs);
        assert_eq!(schema, vec![("score".to_string(), "int".to_string())]);
    }

    #[test]
    fn test_infer_schema_widens_conflicting_types() {
        let docs = vec![
            doc! { "id": 1i32, "v": 1i32 },
            doc! { "id": "a", "v": 2i64 },
        ];
        let type_map: BTreeMap<String, String> =
            infer_schema_from_documents(&docs).into_iter().collect();
        assert_eq!(type_map["id"], "string"); // int + string → text column
        assert_eq!(type_map["v"], "long"); // int + long → long
    }

    #[test]
    fn test_infer_schema_all_null_field_stays_null() {
        let docs = vec![doc! { "x": Bson::Null }, doc! { "x": Bson::Null }];
        let schema = infer_schema_from_documents(&docs);
        assert_eq!(schema, vec![("x".to_string(), "null".to_string())]);
    }
}
