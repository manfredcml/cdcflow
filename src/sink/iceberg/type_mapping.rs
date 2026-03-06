use iceberg::spec::PrimitiveType;

use crate::schema::CanonicalType;

// Re-export SourceDialect from the shared schema module for backwards compat.
pub use crate::schema::SourceDialect;

/// Convert a CanonicalType to an Iceberg PrimitiveType.
///
/// This is the Iceberg-specific leg of the canonical type system.
/// Unknown/unmapped types fall back to String since CDC values arrive as text.
pub fn canonical_to_iceberg(ct: &CanonicalType) -> PrimitiveType {
    match ct {
        CanonicalType::Boolean => PrimitiveType::Boolean,
        CanonicalType::SmallInt => PrimitiveType::Int,
        CanonicalType::Int => PrimitiveType::Int,
        CanonicalType::BigInt => PrimitiveType::Long,
        CanonicalType::Float => PrimitiveType::Float,
        CanonicalType::Double => PrimitiveType::Double,
        CanonicalType::Decimal { precision, scale } => PrimitiveType::Decimal {
            precision: precision.unwrap_or(38),
            scale: scale.unwrap_or(18),
        },
        CanonicalType::VarString { .. } => PrimitiveType::String,
        CanonicalType::FixedString { .. } => PrimitiveType::String,
        CanonicalType::Text => PrimitiveType::String,
        CanonicalType::Timestamp => PrimitiveType::Timestamp,
        CanonicalType::TimestampTz => PrimitiveType::Timestamptz,
        CanonicalType::Date => PrimitiveType::Date,
        CanonicalType::Time => PrimitiveType::Time,
        CanonicalType::Binary => PrimitiveType::Binary,
        CanonicalType::Uuid => PrimitiveType::Uuid,
        CanonicalType::Json => PrimitiveType::String,
        CanonicalType::Year => PrimitiveType::Int,
    }
}

/// Map a source DB column type name directly to an Iceberg PrimitiveType.
///
/// Convenience function that parses through the canonical type system.
/// Used by schema_builder and schema_evolution.
pub fn map_type(dialect: SourceDialect, data_type: &str) -> PrimitiveType {
    let canonical = crate::schema::type_mapping::parse_source_type(dialect, data_type);
    canonical_to_iceberg(&canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_to_iceberg() {
        let cases = vec![
            (CanonicalType::Boolean, PrimitiveType::Boolean),
            (CanonicalType::SmallInt, PrimitiveType::Int),
            (CanonicalType::Int, PrimitiveType::Int),
            (CanonicalType::BigInt, PrimitiveType::Long),
            (CanonicalType::Float, PrimitiveType::Float),
            (CanonicalType::Double, PrimitiveType::Double),
            (
                CanonicalType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
                PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                CanonicalType::Decimal {
                    precision: None,
                    scale: None,
                },
                PrimitiveType::Decimal {
                    precision: 38,
                    scale: 18,
                },
            ),
            (
                CanonicalType::VarString { length: Some(255) },
                PrimitiveType::String,
            ),
            (
                CanonicalType::VarString { length: None },
                PrimitiveType::String,
            ),
            (
                CanonicalType::FixedString { length: Some(10) },
                PrimitiveType::String,
            ),
            (CanonicalType::Text, PrimitiveType::String),
            (CanonicalType::Timestamp, PrimitiveType::Timestamp),
            (CanonicalType::TimestampTz, PrimitiveType::Timestamptz),
            (CanonicalType::Date, PrimitiveType::Date),
            (CanonicalType::Time, PrimitiveType::Time),
            (CanonicalType::Binary, PrimitiveType::Binary),
            (CanonicalType::Uuid, PrimitiveType::Uuid),
            (CanonicalType::Json, PrimitiveType::String),
            (CanonicalType::Year, PrimitiveType::Int),
        ];
        for (canonical, expected) in cases {
            assert_eq!(
                canonical_to_iceberg(&canonical),
                expected,
                "failed for {:?}",
                canonical
            );
        }
    }
}
