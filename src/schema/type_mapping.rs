use super::{CanonicalType, SourceDialect};

/// Parse a source database type string into its canonical representation.
///
/// This is the single source of truth for source type -> canonical type mapping.
/// Each sink then converts from `CanonicalType` to its own type system.
pub fn parse_source_type(dialect: SourceDialect, raw_type: &str) -> CanonicalType {
    let dt = raw_type.trim().to_lowercase();
    match dialect {
        SourceDialect::Postgres => parse_postgres_type(&dt),
        SourceDialect::Mysql => parse_mysql_type(&dt),
    }
}

/// Extract the content inside parentheses from a type string.
/// e.g. `varchar(255)` -> Some("255"), `decimal(10,2)` -> Some("10,2")
fn extract_parenthesized(type_str: &str) -> Option<&str> {
    let start = type_str.find('(')?;
    let end = type_str.find(')')?;
    if end > start + 1 {
        Some(&type_str[start + 1..end])
    } else {
        None
    }
}

/// Extract a single length value from parenthesized content.
/// e.g. `varchar(255)` -> Some(255)
fn extract_length(type_str: &str) -> Option<u32> {
    extract_parenthesized(type_str)?.parse().ok()
}

/// Extract precision and scale from parenthesized content.
/// e.g. `decimal(10,2)` -> (Some(10), Some(2)), `decimal` -> (None, None)
fn extract_precision_scale(type_str: &str) -> (Option<u32>, Option<u32>) {
    match extract_parenthesized(type_str) {
        Some(inner) => {
            let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
            let precision = parts.first().and_then(|s| s.parse().ok());
            let scale = parts.get(1).and_then(|s| s.parse().ok());
            (precision, scale)
        }
        None => (None, None),
    }
}

/// Strip the display width from an integer-like type: `int(11)` -> `int`
fn strip_display_width(dt: &str) -> &str {
    match dt.find('(') {
        Some(pos) => &dt[..pos],
        None => dt,
    }
}

fn parse_postgres_type(dt: &str) -> CanonicalType {
    // Handle types with length parameters first
    if dt.starts_with("numeric(") || dt.starts_with("decimal(") {
        let (precision, scale) = extract_precision_scale(dt);
        return CanonicalType::Decimal { precision, scale };
    }
    if dt.starts_with("character varying(") || dt.starts_with("varchar(") {
        let length = extract_length(dt);
        return CanonicalType::VarString { length };
    }
    if dt.starts_with("character(") || dt.starts_with("char(") {
        let length = extract_length(dt);
        return CanonicalType::FixedString { length };
    }

    match dt {
        "smallint" | "int2" => CanonicalType::SmallInt,
        "integer" | "int" | "int4" => CanonicalType::Int,
        "bigint" | "int8" => CanonicalType::BigInt,
        "boolean" | "bool" => CanonicalType::Boolean,
        "real" | "float4" => CanonicalType::Float,
        "double precision" | "float8" => CanonicalType::Double,
        "numeric" | "decimal" => CanonicalType::Decimal {
            precision: None,
            scale: None,
        },
        "text" | "name" => CanonicalType::Text,
        "varchar" | "character varying" => CanonicalType::VarString { length: None },
        "char" | "character" => CanonicalType::FixedString { length: None },
        "timestamp without time zone" | "timestamp" => CanonicalType::Timestamp,
        "timestamp with time zone" | "timestamptz" => CanonicalType::TimestampTz,
        "date" => CanonicalType::Date,
        "time" | "time without time zone" => CanonicalType::Time,
        "bytea" => CanonicalType::Binary,
        "uuid" => CanonicalType::Uuid,
        "json" | "jsonb" => CanonicalType::Json,
        _ => CanonicalType::Text, // Unknown types fall back to Text
    }
}

fn parse_mysql_type(dt: &str) -> CanonicalType {
    // MySQL BOOLEAN is an alias for TINYINT(1).
    if dt == "tinyint(1)" || dt == "bool" || dt == "boolean" {
        return CanonicalType::Boolean;
    }

    // Handle decimal with preserved precision before stripping parens
    let base = strip_display_width(dt);

    if base == "decimal" || base == "numeric" {
        let (precision, scale) = extract_precision_scale(dt);
        return CanonicalType::Decimal { precision, scale };
    }

    // Handle varchar/char with preserved length
    if base == "varchar" {
        let length = extract_length(dt);
        return CanonicalType::VarString { length };
    }
    if base == "char" {
        let length = extract_length(dt);
        return CanonicalType::FixedString { length };
    }

    match base {
        "int" | "integer" | "mediumint" => CanonicalType::Int,
        "bigint" => CanonicalType::BigInt,
        "smallint" => CanonicalType::SmallInt,
        "tinyint" => CanonicalType::SmallInt,

        "float" => CanonicalType::Float,
        "double" | "real" => CanonicalType::Double,

        "text" | "tinytext" | "mediumtext" | "longtext" => CanonicalType::Text,
        "enum" | "set" => CanonicalType::Text,

        "timestamp" | "datetime" => CanonicalType::Timestamp,
        "date" => CanonicalType::Date,
        "time" => CanonicalType::Time,
        "year" => CanonicalType::Year,

        "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
            CanonicalType::Binary
        }

        "json" => CanonicalType::Json,

        _ => CanonicalType::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_integer_types() {
        let cases = vec![
            ("integer", CanonicalType::Int),
            ("int", CanonicalType::Int),
            ("int4", CanonicalType::Int),
            ("smallint", CanonicalType::SmallInt),
            ("int2", CanonicalType::SmallInt),
            ("bigint", CanonicalType::BigInt),
            ("int8", CanonicalType::BigInt),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_source_type(SourceDialect::Postgres, input),
                expected,
                "failed for PG type '{input}'"
            );
        }
    }

    #[test]
    fn test_pg_boolean() {
        for dt in &["boolean", "bool"] {
            assert_eq!(
                parse_source_type(SourceDialect::Postgres, dt),
                CanonicalType::Boolean,
                "failed for PG type '{dt}'"
            );
        }
    }

    #[test]
    fn test_pg_float_types() {
        let cases = vec![
            ("real", CanonicalType::Float),
            ("float4", CanonicalType::Float),
            ("double precision", CanonicalType::Double),
            ("float8", CanonicalType::Double),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_source_type(SourceDialect::Postgres, input),
                expected,
                "failed for PG type '{input}'"
            );
        }
    }

    #[test]
    fn test_pg_decimal_types() {
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "numeric"),
            CanonicalType::Decimal {
                precision: None,
                scale: None
            }
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "decimal"),
            CanonicalType::Decimal {
                precision: None,
                scale: None
            }
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "numeric(10,2)"),
            CanonicalType::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "numeric(5,0)"),
            CanonicalType::Decimal {
                precision: Some(5),
                scale: Some(0)
            }
        );
    }

    #[test]
    fn test_pg_string_types() {
        let cases = vec![
            ("text", CanonicalType::Text),
            ("varchar", CanonicalType::VarString { length: None }),
            (
                "character varying",
                CanonicalType::VarString { length: None },
            ),
            (
                "character varying(255)",
                CanonicalType::VarString {
                    length: Some(255),
                },
            ),
            (
                "varchar(50)",
                CanonicalType::VarString { length: Some(50) },
            ),
            ("char", CanonicalType::FixedString { length: None }),
            ("character", CanonicalType::FixedString { length: None }),
            (
                "character(10)",
                CanonicalType::FixedString {
                    length: Some(10),
                },
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_source_type(SourceDialect::Postgres, input),
                expected,
                "failed for PG type '{input}'"
            );
        }
    }

    #[test]
    fn test_pg_json() {
        for dt in &["json", "jsonb"] {
            assert_eq!(
                parse_source_type(SourceDialect::Postgres, dt),
                CanonicalType::Json,
                "failed for PG type '{dt}'"
            );
        }
    }

    #[test]
    fn test_pg_timestamp_types() {
        let cases = vec![
            ("timestamp without time zone", CanonicalType::Timestamp),
            ("timestamp", CanonicalType::Timestamp),
            ("timestamp with time zone", CanonicalType::TimestampTz),
            ("timestamptz", CanonicalType::TimestampTz),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_source_type(SourceDialect::Postgres, input),
                expected,
                "failed for PG type '{input}'"
            );
        }
    }

    #[test]
    fn test_pg_date_time() {
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "date"),
            CanonicalType::Date
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "time"),
            CanonicalType::Time
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "time without time zone"),
            CanonicalType::Time
        );
    }

    #[test]
    fn test_pg_binary_and_uuid() {
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "bytea"),
            CanonicalType::Binary
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "uuid"),
            CanonicalType::Uuid
        );
    }

    #[test]
    fn test_pg_unknown_fallback() {
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "custom_enum"),
            CanonicalType::Text
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "xml"),
            CanonicalType::Text
        );
    }

    #[test]
    fn test_pg_case_insensitive() {
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "INTEGER"),
            CanonicalType::Int
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "Boolean"),
            CanonicalType::Boolean
        );
        assert_eq!(
            parse_source_type(SourceDialect::Postgres, "TEXT"),
            CanonicalType::Text
        );
    }

    #[test]
    fn test_mysql_boolean() {
        for dt in &["boolean", "bool", "tinyint(1)"] {
            assert_eq!(
                parse_source_type(SourceDialect::Mysql, dt),
                CanonicalType::Boolean,
                "failed for MySQL type '{dt}'"
            );
        }
    }

    #[test]
    fn test_mysql_integer_types() {
        let cases = vec![
            ("int", CanonicalType::Int),
            ("integer", CanonicalType::Int),
            ("mediumint", CanonicalType::Int),
            ("int(11)", CanonicalType::Int),
            ("mediumint(9)", CanonicalType::Int),
            ("bigint", CanonicalType::BigInt),
            ("bigint(20)", CanonicalType::BigInt),
            ("smallint", CanonicalType::SmallInt),
            ("smallint(6)", CanonicalType::SmallInt),
            ("tinyint", CanonicalType::SmallInt),
            ("tinyint(4)", CanonicalType::SmallInt),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_source_type(SourceDialect::Mysql, input),
                expected,
                "failed for MySQL type '{input}'"
            );
        }
    }

    #[test]
    fn test_mysql_float_types() {
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "float"),
            CanonicalType::Float
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "double"),
            CanonicalType::Double
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "real"),
            CanonicalType::Double
        );
    }

    #[test]
    fn test_mysql_decimal() {
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "decimal"),
            CanonicalType::Decimal {
                precision: None,
                scale: None
            }
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "decimal(10,2)"),
            CanonicalType::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "numeric(38,18)"),
            CanonicalType::Decimal {
                precision: Some(38),
                scale: Some(18)
            }
        );
    }

    #[test]
    fn test_mysql_string_types() {
        let cases = vec![
            (
                "varchar(255)",
                CanonicalType::VarString {
                    length: Some(255),
                },
            ),
            ("varchar", CanonicalType::VarString { length: None }),
            (
                "char(10)",
                CanonicalType::FixedString {
                    length: Some(10),
                },
            ),
            ("char", CanonicalType::FixedString { length: None }),
            ("text", CanonicalType::Text),
            ("tinytext", CanonicalType::Text),
            ("mediumtext", CanonicalType::Text),
            ("longtext", CanonicalType::Text),
            ("enum", CanonicalType::Text),
            ("set", CanonicalType::Text),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_source_type(SourceDialect::Mysql, input),
                expected,
                "failed for MySQL type '{input}'"
            );
        }
    }

    #[test]
    fn test_mysql_json() {
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "json"),
            CanonicalType::Json
        );
    }

    #[test]
    fn test_mysql_temporal_types() {
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "timestamp"),
            CanonicalType::Timestamp
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "datetime"),
            CanonicalType::Timestamp
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "date"),
            CanonicalType::Date
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "time"),
            CanonicalType::Time
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "year"),
            CanonicalType::Year
        );
    }

    #[test]
    fn test_mysql_binary_types() {
        for dt in &[
            "blob",
            "tinyblob",
            "mediumblob",
            "longblob",
            "binary",
            "varbinary",
            "binary(16)",
            "varbinary(255)",
        ] {
            assert_eq!(
                parse_source_type(SourceDialect::Mysql, dt),
                CanonicalType::Binary,
                "failed for MySQL type '{dt}'"
            );
        }
    }

    #[test]
    fn test_mysql_unknown_fallback() {
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "geometry"),
            CanonicalType::Text
        );
    }

    #[test]
    fn test_mysql_case_insensitive() {
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "VARCHAR(100)"),
            CanonicalType::VarString {
                length: Some(100)
            }
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "INT"),
            CanonicalType::Int
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "BIGINT(20)"),
            CanonicalType::BigInt
        );
        assert_eq!(
            parse_source_type(SourceDialect::Mysql, "TINYINT(1)"),
            CanonicalType::Boolean
        );
    }

}
