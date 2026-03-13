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
            // Convert millis to ISO 8601 via chrono
            let millis = dt.timestamp_millis();
            let secs = millis / 1000;
            let nsecs = ((millis % 1000) * 1_000_000) as u32;
            let ts = chrono::DateTime::from_timestamp(secs, nsecs)
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

/// Infer schema from a set of documents by building a union of all top-level
/// fields. The type for each field is taken from the first non-null value seen.
pub fn infer_schema_from_documents(docs: &[Document]) -> Vec<(String, String)> {
    let mut field_types: BTreeMap<String, String> = BTreeMap::new();
    for doc in docs {
        for (key, value) in doc {
            field_types
                .entry(key.clone())
                .or_insert_with(|| bson_type_string(value).to_string());
        }
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
}
