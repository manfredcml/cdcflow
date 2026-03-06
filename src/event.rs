use std::collections::BTreeMap;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::CdcError;

/// Log Sequence Number — PostgreSQL's WAL position identifier.
///
/// Internally stored as a u64. Displayed in the PostgreSQL `XX/YY` hex format
/// where XX is the upper 32 bits and YY is the lower 32 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let upper = (self.0 >> 32) as u32;
        let lower = self.0 as u32;
        write!(f, "{upper:X}/{lower:X}")
    }
}

impl FromStr for Lsn {
    type Err = CdcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (upper_str, lower_str) = s
            .split_once('/')
            .ok_or_else(|| CdcError::Parse(format!("invalid LSN format, expected XX/YY: {s}")))?;

        let upper = u32::from_str_radix(upper_str, 16)
            .map_err(|e| CdcError::Parse(format!("invalid LSN upper half '{upper_str}': {e}")))?;
        let lower = u32::from_str_radix(lower_str, 16)
            .map_err(|e| CdcError::Parse(format!("invalid LSN lower half '{lower_str}': {e}")))?;

        Ok(Lsn(((upper as u64) << 32) | (lower as u64)))
    }
}

/// Identifies a specific table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId {
    pub schema: String,
    pub name: String,
    pub oid: u32,
}

/// A column value from a change event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(OrderedFloat<f64>),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(String),
    Date(String),
    Time(String),
    UnchangedToast,
}

impl ColumnValue {
    /// Creates a `Float` variant from a plain `f64`.
    pub fn float(v: f64) -> Self {
        ColumnValue::Float(OrderedFloat(v))
    }
}

/// A row is a mapping of column names to values.
pub type Row = BTreeMap<String, ColumnValue>;

/// The type of change operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
    Truncate,
    Snapshot,
}

impl ChangeOp {
    /// Single-character CDC operation code used across all sinks.
    pub fn as_op_code(self) -> &'static str {
        match self {
            ChangeOp::Insert => "I",
            ChangeOp::Update => "U",
            ChangeOp::Delete => "D",
            ChangeOp::Truncate => "T",
            ChangeOp::Snapshot => "S",
        }
    }
}

/// A single CDC event representing a row-level change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcEvent {
    pub lsn: Lsn,
    pub timestamp_us: i64,
    pub xid: u32,
    pub table: TableId,
    pub op: ChangeOp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new: Option<Row>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old: Option<Row>,
    /// Primary key column names for this table. Empty if unknown or table has no PK.
    #[serde(default)]
    pub primary_key_columns: Vec<String>,
}

impl CdcEvent {
    /// Returns true if this event is from a snapshot.
    pub fn is_snapshot(&self) -> bool {
        self.op == ChangeOp::Snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_parse_display_roundtrip() {
        let cases = vec!["0/16B3748", "1/0", "0/0", "FF/FFFFFFFF", "A/BC"];
        for s in cases {
            let lsn: Lsn = s.parse().unwrap();
            let displayed = lsn.to_string();
            let reparsed: Lsn = displayed.parse().unwrap();
            assert_eq!(lsn, reparsed, "roundtrip failed for {s}");
        }
    }

    #[test]
    fn test_lsn_specific_values() {
        let lsn: Lsn = "0/16B3748".parse().unwrap();
        assert_eq!(lsn.0, 0x16B3748);

        let lsn: Lsn = "1/0".parse().unwrap();
        assert_eq!(lsn.0, 1 << 32);

        let lsn: Lsn = "0/0".parse().unwrap();
        assert_eq!(lsn, Lsn::ZERO);
    }

    #[test]
    fn test_lsn_display_format() {
        // Verify uppercase hex, no padding, slash separator
        let lsn: Lsn = Lsn(0x16B3748);
        assert_eq!(lsn.to_string(), "0/16B3748");

        let lsn: Lsn = Lsn(1 << 32);
        assert_eq!(lsn.to_string(), "1/0");

        let lsn: Lsn = Lsn(0);
        assert_eq!(lsn.to_string(), "0/0");

        let lsn: Lsn = Lsn(u64::MAX);
        assert_eq!(lsn.to_string(), "FFFFFFFF/FFFFFFFF");
    }

    #[test]
    fn test_lsn_invalid_input() {
        assert!("".parse::<Lsn>().is_err());
        assert!("noslash".parse::<Lsn>().is_err());
        assert!("G/0".parse::<Lsn>().is_err());
        assert!("0/G".parse::<Lsn>().is_err());
        assert!("/".parse::<Lsn>().is_err());
        assert!("0/".parse::<Lsn>().is_err());
    }

    #[test]
    fn test_cdc_event_backward_compat_deserialization() {
        // Events serialized before primary_key_columns existed should deserialize fine
        let json = r#"{"lsn":0,"timestamp_us":0,"xid":0,"table":{"schema":"public","name":"t","oid":1},"op":"insert","new":{"id":{"Int":1}}}"#;
        let event: CdcEvent = serde_json::from_str(json).unwrap();
        assert!(event.primary_key_columns.is_empty());
    }

    #[test]
    fn test_cdc_event_serialization() {
        let event = CdcEvent {
            lsn: "0/16B3748".parse().unwrap(),
            timestamp_us: 1_700_000_000_000_000,
            xid: 42,
            table: TableId {
                schema: "public".into(),
                name: "users".into(),
                oid: 16384,
            },
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
                ("active".into(), ColumnValue::Bool(true)),
                ("score".into(), ColumnValue::float(99.5)),
                ("deleted_at".into(), ColumnValue::Null),
            ])),
            old: None,
            primary_key_columns: vec!["id".into()],
        };

        let json = serde_json::to_string(&event).unwrap();
        // Verify flat structure — no nested "change" key
        assert!(json.contains("\"op\":\"insert\""));
        assert!(!json.contains("\"old\""));
        // Verify typed values in JSON
        assert!(json.contains("\"Int\":1"));
        assert!(json.contains("\"Bool\":true"));
        assert!(json.contains("\"Float\":99.5"));
        let deserialized: CdcEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_is_snapshot() {
        let make_event = |op| CdcEvent {
            lsn: Lsn::ZERO,
            timestamp_us: 0,
            xid: 0,
            table: TableId {
                schema: "s".into(),
                name: "t".into(),
                oid: 1,
            },
            op,
            new: None,
            old: None,
            primary_key_columns: vec![],
        };

        assert!(make_event(ChangeOp::Snapshot).is_snapshot());
        assert!(!make_event(ChangeOp::Insert).is_snapshot());
        assert!(!make_event(ChangeOp::Update).is_snapshot());
        assert!(!make_event(ChangeOp::Delete).is_snapshot());
        assert!(!make_event(ChangeOp::Truncate).is_snapshot());
    }

    #[test]
    fn test_as_op_code() {
        assert_eq!(ChangeOp::Insert.as_op_code(), "I");
        assert_eq!(ChangeOp::Update.as_op_code(), "U");
        assert_eq!(ChangeOp::Delete.as_op_code(), "D");
        assert_eq!(ChangeOp::Truncate.as_op_code(), "T");
        assert_eq!(ChangeOp::Snapshot.as_op_code(), "S");
    }

    #[test]
    fn test_column_value_variants() {
        let values = vec![
            ColumnValue::Null,
            ColumnValue::Bool(true),
            ColumnValue::Bool(false),
            ColumnValue::Int(42),
            ColumnValue::Int(-1),
            ColumnValue::float(3.14),
            ColumnValue::Text("hello".into()),
            ColumnValue::Bytes(vec![0xDE, 0xAD]),
            ColumnValue::Timestamp("2024-01-15T10:30:00Z".into()),
            ColumnValue::Date("2024-01-15".into()),
            ColumnValue::Time("10:30:00".into()),
            ColumnValue::UnchangedToast,
        ];
        for v in values {
            let json = serde_json::to_string(&v).unwrap();
            let back: ColumnValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back);
        }
    }
}
