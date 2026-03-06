use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

use crate::error::{CdcError, Result};
use crate::schema::ColumnInfo;

use super::type_mapping::canonical_to_iceberg;

// Re-export shared discovery functions for callers that used this module.
pub use crate::schema::discovery::{fetch_columns, fetch_primary_keys};

/// Build an Iceberg Schema for CDC mode with fixed 6-column schema.
///
/// Field IDs:
///   1: op       (String,  required)
///   2: lsn      (Long,    required)
///   3: timestamp_us (Long, required)
///   4: snapshot  (Boolean, required)
///   5: row_new   (String,  optional) — JSON-serialized `{"values":{...}, "types":{...}}`
///   6: row_old   (String,  optional) — JSON-serialized `{"values":{...}, "types":{...}}`
///   7: primary_key_columns (String, optional) — JSON-serialized array of PK column names
pub fn build_iceberg_schema() -> Result<Schema> {
    let fields: Vec<Arc<NestedField>> = vec![
        NestedField::required(1, "op", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::required(2, "lsn", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::required(3, "timestamp_us", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::required(4, "snapshot", Type::Primitive(PrimitiveType::Boolean)).into(),
        NestedField::optional(5, "row_new", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(6, "row_old", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(
            7,
            "primary_key_columns",
            Type::Primitive(PrimitiveType::String),
        )
        .into(),
    ];

    Schema::builder()
        .with_fields(fields)
        .with_schema_id(0)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("build schema: {e}")))
}

/// Build an Iceberg Schema for CDC mode with flattened typed columns.
///
/// Layout:
///   7 `_cdc_*` metadata fields (required) +
///   N source columns (optional)           +
///   N `_old_*` columns (optional)
///
/// All source and `_old_` columns are optional because:
/// - Inserts/snapshots have no old values
/// - Deletes have no new values
/// - Schema evolution may add columns that historical rows lack
pub fn build_flattened_cdc_schema(columns: &[ColumnInfo]) -> Result<Schema> {
    let mut fields: Vec<Arc<NestedField>> = Vec::new();
    let mut field_id = 1i32;

    // 7 CDC metadata fields (required)
    fields.push(
        NestedField::required(field_id, "_cdc_op", Type::Primitive(PrimitiveType::String)).into(),
    );
    field_id += 1;
    fields.push(
        NestedField::required(field_id, "_cdc_lsn", Type::Primitive(PrimitiveType::Long)).into(),
    );
    field_id += 1;
    fields.push(
        NestedField::required(
            field_id,
            "_cdc_timestamp_us",
            Type::Primitive(PrimitiveType::Long),
        )
        .into(),
    );
    field_id += 1;
    fields.push(
        NestedField::required(
            field_id,
            "_cdc_snapshot",
            Type::Primitive(PrimitiveType::Boolean),
        )
        .into(),
    );
    field_id += 1;
    fields.push(
        NestedField::required(
            field_id,
            "_cdc_schema",
            Type::Primitive(PrimitiveType::String),
        )
        .into(),
    );
    field_id += 1;
    fields.push(
        NestedField::required(
            field_id,
            "_cdc_table",
            Type::Primitive(PrimitiveType::String),
        )
        .into(),
    );
    field_id += 1;
    fields.push(
        NestedField::required(
            field_id,
            "_cdc_primary_key_columns",
            Type::Primitive(PrimitiveType::String),
        )
        .into(),
    );
    field_id += 1;

    // N source columns (optional)
    for col in columns {
        let iceberg_type = Type::Primitive(canonical_to_iceberg(&col.canonical_type));
        fields.push(NestedField::optional(field_id, &col.name, iceberg_type).into());
        field_id += 1;
    }

    // N _old_ columns (optional)
    for col in columns {
        let iceberg_type = Type::Primitive(canonical_to_iceberg(&col.canonical_type));
        let old_name = format!("_old_{}", col.name);
        fields.push(NestedField::optional(field_id, old_name, iceberg_type).into());
        field_id += 1;
    }

    Schema::builder()
        .with_fields(fields)
        .with_schema_id(0)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("build flattened CDC schema: {e}")))
}

/// Build an Iceberg Schema for replication mode (source columns only, no CDC metadata).
///
/// PK columns are marked as required and added to identifier_field_ids.
/// Field IDs start at 1.
pub fn build_replication_schema(columns: &[ColumnInfo], pk_columns: &[String]) -> Result<Schema> {
    let mut fields: Vec<Arc<NestedField>> = Vec::with_capacity(columns.len());
    let mut identifier_field_ids = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        let field_id = (i + 1) as i32;
        let iceberg_type = Type::Primitive(canonical_to_iceberg(&col.canonical_type));
        let is_pk = pk_columns.contains(&col.name);
        let field = if is_pk {
            identifier_field_ids.push(field_id);
            // PK columns must be required per Iceberg spec
            NestedField::required(field_id, &col.name, iceberg_type)
        } else if col.is_nullable {
            NestedField::optional(field_id, &col.name, iceberg_type)
        } else {
            NestedField::required(field_id, &col.name, iceberg_type)
        };
        fields.push(field.into());
    }

    Schema::builder()
        .with_fields(fields)
        .with_schema_id(0)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("build replication schema: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::CanonicalType;
    use iceberg::spec::PrimitiveType;

    fn sample_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".into(),
                source_type: "integer".into(),
                canonical_type: CanonicalType::Int,
                is_nullable: false,
                ordinal_position: 1,
            },
            ColumnInfo {
                name: "name".into(),
                source_type: "text".into(),
                canonical_type: CanonicalType::Text,
                is_nullable: true,
                ordinal_position: 2,
            },
            ColumnInfo {
                name: "created_at".into(),
                source_type: "timestamp".into(),
                canonical_type: CanonicalType::Timestamp,
                is_nullable: true,
                ordinal_position: 3,
            },
        ]
    }

    #[test]
    fn test_build_iceberg_schema_field_count() {
        let schema = build_iceberg_schema().unwrap();
        assert_eq!(schema.as_struct().fields().len(), 7);
    }

    #[test]
    fn test_build_iceberg_schema_metadata_fields() {
        let schema = build_iceberg_schema().unwrap();
        let fields = schema.as_struct().fields();

        assert_eq!(fields[0].name, "op");
        assert_eq!(fields[0].id, 1);
        assert!(fields[0].required);
        assert_eq!(
            *fields[0].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[1].name, "lsn");
        assert_eq!(fields[1].id, 2);
        assert!(fields[1].required);
        assert_eq!(*fields[1].field_type, Type::Primitive(PrimitiveType::Long));

        assert_eq!(fields[2].name, "timestamp_us");
        assert_eq!(fields[2].id, 3);
        assert!(fields[2].required);

        assert_eq!(fields[3].name, "snapshot");
        assert_eq!(fields[3].id, 4);
        assert!(fields[3].required);
        assert_eq!(
            *fields[3].field_type,
            Type::Primitive(PrimitiveType::Boolean)
        );
    }

    #[test]
    fn test_build_iceberg_schema_row_fields() {
        let schema = build_iceberg_schema().unwrap();
        let fields = schema.as_struct().fields();

        assert_eq!(fields[4].name, "row_new");
        assert_eq!(fields[4].id, 5);
        assert!(!fields[4].required);
        assert_eq!(
            *fields[4].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[5].name, "row_old");
        assert_eq!(fields[5].id, 6);
        assert!(!fields[5].required);
        assert_eq!(
            *fields[5].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[6].name, "primary_key_columns");
        assert_eq!(fields[6].id, 7);
        assert!(!fields[6].required);
        assert_eq!(
            *fields[6].field_type,
            Type::Primitive(PrimitiveType::String)
        );
    }

    #[test]
    fn test_build_iceberg_schema_no_cdc_prefix() {
        let schema = build_iceberg_schema().unwrap();
        let fields = schema.as_struct().fields();
        // No _cdc_ prefix on any field
        for field in fields {
            assert!(
                !field.name.starts_with("_cdc_"),
                "field {} has _cdc_ prefix",
                field.name
            );
        }
    }

    #[test]
    fn test_flattened_cdc_schema_field_count() {
        let cols = sample_columns(); // 3 columns
        let schema = build_flattened_cdc_schema(&cols).unwrap();
        // 7 metadata + 3 source + 3 old = 13
        assert_eq!(schema.as_struct().fields().len(), 13);
    }

    #[test]
    fn test_flattened_cdc_schema_metadata_fields() {
        let cols = sample_columns();
        let schema = build_flattened_cdc_schema(&cols).unwrap();
        let fields = schema.as_struct().fields();

        assert_eq!(fields[0].name, "_cdc_op");
        assert!(fields[0].required);
        assert_eq!(
            *fields[0].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[1].name, "_cdc_lsn");
        assert!(fields[1].required);
        assert_eq!(*fields[1].field_type, Type::Primitive(PrimitiveType::Long));

        assert_eq!(fields[2].name, "_cdc_timestamp_us");
        assert!(fields[2].required);
        assert_eq!(*fields[2].field_type, Type::Primitive(PrimitiveType::Long));

        assert_eq!(fields[3].name, "_cdc_snapshot");
        assert!(fields[3].required);
        assert_eq!(
            *fields[3].field_type,
            Type::Primitive(PrimitiveType::Boolean)
        );

        assert_eq!(fields[4].name, "_cdc_schema");
        assert!(fields[4].required);
        assert_eq!(
            *fields[4].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[5].name, "_cdc_table");
        assert!(fields[5].required);
        assert_eq!(
            *fields[5].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[6].name, "_cdc_primary_key_columns");
        assert!(fields[6].required);
        assert_eq!(
            *fields[6].field_type,
            Type::Primitive(PrimitiveType::String)
        );
    }

    #[test]
    fn test_flattened_cdc_schema_source_columns_optional() {
        let cols = sample_columns();
        let schema = build_flattened_cdc_schema(&cols).unwrap();
        let fields = schema.as_struct().fields();

        // Source columns at indices 7, 8, 9
        assert_eq!(fields[7].name, "id");
        assert!(!fields[7].required); // optional
        assert_eq!(*fields[7].field_type, Type::Primitive(PrimitiveType::Int));

        assert_eq!(fields[8].name, "name");
        assert!(!fields[8].required);
        assert_eq!(
            *fields[8].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[9].name, "created_at");
        assert!(!fields[9].required);
        assert_eq!(
            *fields[9].field_type,
            Type::Primitive(PrimitiveType::Timestamp)
        );
    }

    #[test]
    fn test_flattened_cdc_schema_old_columns() {
        let cols = sample_columns();
        let schema = build_flattened_cdc_schema(&cols).unwrap();
        let fields = schema.as_struct().fields();

        // Old columns at indices 10, 11, 12
        assert_eq!(fields[10].name, "_old_id");
        assert!(!fields[10].required);
        assert_eq!(*fields[10].field_type, Type::Primitive(PrimitiveType::Int));

        assert_eq!(fields[11].name, "_old_name");
        assert!(!fields[11].required);
        assert_eq!(
            *fields[11].field_type,
            Type::Primitive(PrimitiveType::String)
        );

        assert_eq!(fields[12].name, "_old_created_at");
        assert!(!fields[12].required);
        assert_eq!(
            *fields[12].field_type,
            Type::Primitive(PrimitiveType::Timestamp)
        );
    }

    #[test]
    fn test_flattened_cdc_schema_field_ids_sequential() {
        let cols = sample_columns();
        let schema = build_flattened_cdc_schema(&cols).unwrap();
        let fields = schema.as_struct().fields();
        for (i, field) in fields.iter().enumerate() {
            assert_eq!(field.id, (i + 1) as i32);
        }
    }

    #[test]
    fn test_flattened_cdc_schema_no_columns() {
        // Edge case: no source columns, just metadata
        let schema = build_flattened_cdc_schema(&[]).unwrap();
        assert_eq!(schema.as_struct().fields().len(), 7);
    }

    #[test]
    fn test_flattened_cdc_schema_no_identifier_field_ids() {
        // CDC mode has no PK concept in the Iceberg table
        let cols = sample_columns();
        let schema = build_flattened_cdc_schema(&cols).unwrap();
        assert_eq!(schema.identifier_field_ids().count(), 0);
    }

    #[test]
    fn test_build_replication_schema_no_cdc_columns() {
        let cols = sample_columns();
        let pk = vec!["id".to_string()];
        let schema = build_replication_schema(&cols, &pk).unwrap();
        let fields = schema.as_struct().fields();

        // Only source columns, no CDC metadata
        assert_eq!(fields.len(), 3);
        assert!(!fields.iter().any(|f| f.name.starts_with("_cdc_")));
    }

    #[test]
    fn test_build_replication_schema_field_ids_start_at_1() {
        let cols = sample_columns();
        let pk = vec!["id".to_string()];
        let schema = build_replication_schema(&cols, &pk).unwrap();
        let fields = schema.as_struct().fields();

        assert_eq!(fields[0].id, 1);
        assert_eq!(fields[1].id, 2);
        assert_eq!(fields[2].id, 3);
    }

    #[test]
    fn test_build_replication_schema_pk_required_and_identifier() {
        let cols = sample_columns();
        let pk = vec!["id".to_string()];
        let schema = build_replication_schema(&cols, &pk).unwrap();
        let fields = schema.as_struct().fields();

        // "id" is PK → required, identifier
        assert_eq!(fields[0].name, "id");
        assert!(fields[0].required);
        assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![1]);

        // "name" is nullable → optional, not identifier
        assert_eq!(fields[1].name, "name");
        assert!(!fields[1].required);
    }

    #[test]
    fn test_build_replication_schema_composite_pk() {
        let cols = vec![
            ColumnInfo {
                name: "tenant_id".into(),
                source_type: "integer".into(),
                canonical_type: CanonicalType::Int,
                is_nullable: true, // Even though nullable in source, PK forces required
                ordinal_position: 1,
            },
            ColumnInfo {
                name: "user_id".into(),
                source_type: "integer".into(),
                canonical_type: CanonicalType::Int,
                is_nullable: true,
                ordinal_position: 2,
            },
            ColumnInfo {
                name: "email".into(),
                source_type: "text".into(),
                canonical_type: CanonicalType::Text,
                is_nullable: true,
                ordinal_position: 3,
            },
        ];
        let pk = vec!["tenant_id".to_string(), "user_id".to_string()];
        let schema = build_replication_schema(&cols, &pk).unwrap();
        let fields = schema.as_struct().fields();

        assert!(fields[0].required); // tenant_id: PK → required
        assert!(fields[1].required); // user_id: PK → required
        assert!(!fields[2].required); // email: not PK → nullable
        let mut ids: Vec<_> = schema.identifier_field_ids().collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }
}
