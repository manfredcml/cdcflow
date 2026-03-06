pub mod discovery;
pub mod evolution;
pub mod type_mapping;
pub mod value;

use crate::config::SourceConnectionConfig;
use crate::event::ColumnValue;

/// Source database dialect for type mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDialect {
    Postgres,
    Mysql,
}

impl From<&SourceConnectionConfig> for SourceDialect {
    fn from(conn: &SourceConnectionConfig) -> Self {
        match conn {
            SourceConnectionConfig::Postgres { .. } => SourceDialect::Postgres,
            SourceConnectionConfig::Mysql { .. } => SourceDialect::Mysql,
        }
    }
}

/// Canonical type representation for CDC column types.
///
/// This is the intermediate representation between source database types
/// and sink-specific types. Source types are parsed into `CanonicalType`
/// by `type_mapping::parse_source_type()`, then each sink converts to
/// its own type system (e.g. Iceberg PrimitiveType, PG DDL string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalType {
    Boolean,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    VarString {
        length: Option<u32>,
    },
    FixedString {
        length: Option<u32>,
    },
    Text,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Binary,
    Uuid,
    Json,
    Year,
}

/// Column metadata from schema discovery.
///
/// Stores both the raw source type string (for PG-to-PG passthrough)
/// and the parsed canonical type (for cross-dialect mapping).
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    /// Raw type string from the source database (e.g. `format_type()` for PG,
    /// `column_type` for MySQL). Used for PG-to-PG passthrough DDL.
    pub source_type: String,
    /// Parsed canonical type for cross-dialect mapping.
    pub canonical_type: CanonicalType,
    pub is_nullable: bool,
    pub ordinal_position: i32,
}

/// Prefix for CDC metadata columns in flattened schemas.
pub const CDC_PREFIX: &str = "_cdc_";

/// Prefix for old-value columns in flattened schemas.
pub const OLD_PREFIX: &str = "_old_";

/// CDC metadata column names (in order) for flattened schemas.
pub const CDC_METADATA_COLUMNS: &[&str] = &[
    "_cdc_op",
    "_cdc_lsn",
    "_cdc_timestamp_us",
    "_cdc_snapshot",
    "_cdc_schema",
    "_cdc_table",
    "_cdc_primary_key_columns",
];

/// Infer a `CanonicalType` from a `ColumnValue` at runtime.
///
/// Used as a fallback when source DB metadata is unavailable.
pub fn infer_canonical_type(value: &ColumnValue) -> CanonicalType {
    match value {
        ColumnValue::Null => CanonicalType::Text,
        ColumnValue::Bool(_) => CanonicalType::Boolean,
        ColumnValue::Int(_) => CanonicalType::BigInt,
        ColumnValue::Float(_) => CanonicalType::Double,
        ColumnValue::Text(_) => CanonicalType::Text,
        ColumnValue::Bytes(_) => CanonicalType::Binary,
        ColumnValue::Timestamp(_) => CanonicalType::Timestamp,
        ColumnValue::Date(_) => CanonicalType::Date,
        ColumnValue::Time(_) => CanonicalType::Time,
        ColumnValue::UnchangedToast => CanonicalType::Text,
    }
}

/// Build the full list of CDC column names for a flattened schema.
///
/// Returns: 7 `_cdc_*` metadata columns + N source columns + N `_old_` columns.
pub fn build_cdc_column_names(source_columns: &[String]) -> Vec<String> {
    let mut names: Vec<String> = CDC_METADATA_COLUMNS.iter().map(|s| s.to_string()).collect();
    for col in source_columns {
        names.push(col.clone());
    }
    for col in source_columns {
        names.push(format!("{OLD_PREFIX}{col}"));
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_canonical_type() {
        use ordered_float::OrderedFloat;
        let cases = vec![
            (ColumnValue::Null, CanonicalType::Text),
            (ColumnValue::Bool(true), CanonicalType::Boolean),
            (ColumnValue::Int(42), CanonicalType::BigInt),
            (ColumnValue::Float(OrderedFloat(1.5)), CanonicalType::Double),
            (ColumnValue::Text("hello".into()), CanonicalType::Text),
            (ColumnValue::Bytes(vec![1, 2]), CanonicalType::Binary),
            (
                ColumnValue::Timestamp("2024-01-01".into()),
                CanonicalType::Timestamp,
            ),
            (ColumnValue::Date("2024-01-01".into()), CanonicalType::Date),
            (ColumnValue::Time("10:00:00".into()), CanonicalType::Time),
            (ColumnValue::UnchangedToast, CanonicalType::Text),
        ];
        for (value, expected) in cases {
            assert_eq!(infer_canonical_type(&value), expected, "for {value:?}");
        }
    }

    #[test]
    fn test_build_cdc_column_names() {
        let source_cols = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        let names = build_cdc_column_names(&source_cols);
        // 7 metadata + 3 source + 3 old = 13
        assert_eq!(names.len(), 13);
        assert_eq!(names[0], "_cdc_op");
        assert_eq!(names[6], "_cdc_primary_key_columns");
        assert_eq!(names[7], "id");
        assert_eq!(names[8], "name");
        assert_eq!(names[9], "email");
        assert_eq!(names[10], "_old_id");
        assert_eq!(names[11], "_old_name");
        assert_eq!(names[12], "_old_email");
    }

    #[test]
    fn test_build_cdc_column_names_empty_source() {
        let names = build_cdc_column_names(&[]);
        assert_eq!(names.len(), 7); // Just metadata
    }
}
