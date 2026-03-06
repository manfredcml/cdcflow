use std::sync::Arc;

use iceberg::spec::{NestedField, Schema, Type};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use serde_json;

use crate::config::RestCatalogConfig;
use crate::error::{CdcError, Result};
use crate::schema::ColumnInfo;

use super::row_delta::build_commit_url;
use super::type_mapping::canonical_to_iceberg;

// Re-export the shared detect_new_columns that takes &[String].
// The Iceberg caller extracts field names from Arrow schema first.
pub use crate::schema::evolution::detect_new_columns as detect_new_columns_from_names;

/// Detect column names present in events but missing from the current Arrow schema.
///
/// This is a thin wrapper that extracts field names from the Arrow schema and
/// delegates to the shared `schema::evolution::detect_new_columns()`.
pub fn detect_new_columns(
    arrow_schema: &arrow_schema::Schema,
    events: &[crate::event::CdcEvent],
) -> Vec<String> {
    let known: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let event_refs: Vec<&crate::event::CdcEvent> = events.iter().collect();
    crate::schema::evolution::detect_new_columns(&known, &event_refs)
}

/// Build an evolved Iceberg schema that preserves all existing fields and appends
/// new columns as optional fields.
///
/// New field IDs continue from the highest existing field ID. The schema ID is
/// incremented by 1. Identifier field IDs (PKs) are carried forward unchanged.
pub fn build_evolved_iceberg_schema(
    current_schema: &Schema,
    new_columns: &[ColumnInfo],
) -> Result<Schema> {
    // Carry forward all existing fields with their original IDs
    let mut fields: Vec<Arc<NestedField>> = current_schema
        .as_struct()
        .fields()
        .to_vec();

    // Determine the next available field ID
    let mut next_id = current_schema.highest_field_id() + 1;

    for col in new_columns {
        let iceberg_type = Type::Primitive(canonical_to_iceberg(&col.canonical_type));
        // New columns are always optional — historical rows won't have values
        let field = NestedField::optional(next_id, &col.name, iceberg_type);
        fields.push(field.into());
        next_id += 1;
    }

    // Carry forward identifier field IDs (PK columns don't change during evolution)
    let identifier_ids: Vec<i32> = current_schema.identifier_field_ids().collect();

    let new_schema_id = current_schema.schema_id() + 1;

    let mut builder = Schema::builder()
        .with_fields(fields)
        .with_schema_id(new_schema_id);

    if !identifier_ids.is_empty() {
        builder = builder.with_identifier_field_ids(identifier_ids);
    }

    builder
        .build()
        .map_err(|e| CdcError::Iceberg(format!("build evolved schema: {e}")))
}

/// Build an evolved Iceberg schema for CDC mode that adds paired columns.
///
/// For each new source column, inserts the source column into the source section
/// and appends the `_old_` column at the end, maintaining the schema layout
/// invariant: `[7 metadata] [N source] [N _old_]`.
///
/// This is critical because `events_to_flattened_cdc_batch` relies on positional
/// indexing to split source vs `_old_` columns.
pub fn build_evolved_cdc_schema(
    current_schema: &Schema,
    new_columns: &[ColumnInfo],
) -> Result<Schema> {
    let mut fields: Vec<Arc<NestedField>> = current_schema
        .as_struct()
        .fields()
        .to_vec();

    let mut next_id = current_schema.highest_field_id() + 1;

    // Find the boundary where _old_ columns start
    let old_start = fields
        .iter()
        .position(|f| f.name.starts_with("_old_"))
        .unwrap_or(fields.len());

    let mut insert_at = old_start;

    for col in new_columns {
        let iceberg_type = Type::Primitive(canonical_to_iceberg(&col.canonical_type));

        // Insert source column before the _old_ section
        let field = NestedField::optional(next_id, &col.name, iceberg_type.clone());
        fields.insert(insert_at, field.into());
        insert_at += 1; // keep subsequent inserts in order
        next_id += 1;

        // Append paired _old_ column at the end
        let old_name = format!("_old_{}", col.name);
        let old_field = NestedField::optional(next_id, old_name, iceberg_type);
        fields.push(old_field.into());
        next_id += 1;
    }

    let new_schema_id = current_schema.schema_id() + 1;

    Schema::builder()
        .with_fields(fields)
        .with_schema_id(new_schema_id)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("build evolved CDC schema: {e}")))
}

/// Detect columns that are present in the Arrow schema but missing from events.
///
/// Only considers Insert/Update/Snapshot events (which carry full column sets).
/// Delete and Truncate events may only carry PK columns and would cause false positives.
///
/// Returns `(has_full_row_events, missing_column_names)`.
/// If no full-row events are found, returns `(false, vec![])`.
pub fn detect_dropped_columns_from_events(
    arrow_schema: &arrow_schema::Schema,
    events: &[crate::event::CdcEvent],
    is_cdc: bool,
) -> (bool, Vec<String>) {
    use crate::event::ChangeOp;
    use std::collections::HashSet;

    let mut event_columns: HashSet<String> = HashSet::new();
    let mut has_full_row_events = false;

    for event in events {
        match event.op {
            ChangeOp::Insert | ChangeOp::Update | ChangeOp::Snapshot => {
                has_full_row_events = true;
                if let Some(row) = event.new.as_ref() {
                    for col_name in row.keys() {
                        event_columns.insert(col_name.clone());
                    }
                }
            }
            _ => {}
        }
    }

    if !has_full_row_events {
        return (false, vec![]);
    }

    let missing: Vec<String> = arrow_schema
        .fields()
        .iter()
        .filter(|f| {
            let name = f.name();
            // Skip CDC metadata columns and _old_ paired columns
            if is_cdc && (name.starts_with("_cdc_") || name.starts_with("_old_")) {
                return false;
            }
            !event_columns.contains(name.as_str())
        })
        .map(|f| f.name().clone())
        .collect();

    (true, missing)
}

/// Build a new Iceberg schema with specified columns removed.
///
/// In CDC mode (`is_cdc=true`), also removes paired `_old_{col}` columns.
/// Preserves identifier field IDs for remaining fields only.
/// Increments schema_id by 1.
pub fn build_schema_without_columns(
    current_schema: &Schema,
    drop_names: &[String],
    is_cdc: bool,
) -> Result<Schema> {
    use std::collections::HashSet;

    let mut drop_set: HashSet<&str> = drop_names.iter().map(|s| s.as_str()).collect();

    // In CDC mode, also drop paired _old_ columns
    let old_names: Vec<String>;
    if is_cdc {
        old_names = drop_names.iter().map(|n| format!("_old_{n}")).collect();
        for name in &old_names {
            drop_set.insert(name.as_str());
        }
    }

    let fields: Vec<Arc<NestedField>> = current_schema
        .as_struct()
        .fields()
        .iter()
        .filter(|f| !drop_set.contains(f.name.as_str()))
        .cloned()
        .collect();

    // Preserve identifier field IDs only for remaining fields
    let remaining_ids: HashSet<i32> = fields.iter().map(|f| f.id).collect();
    let identifier_ids: Vec<i32> = current_schema
        .identifier_field_ids()
        .filter(|id| remaining_ids.contains(id))
        .collect();

    let new_schema_id = current_schema.schema_id() + 1;

    let mut builder = Schema::builder()
        .with_fields(fields)
        .with_schema_id(new_schema_id);

    if !identifier_ids.is_empty() {
        builder = builder.with_identifier_field_ids(identifier_ids);
    }

    builder
        .build()
        .map_err(|e| CdcError::Iceberg(format!("build schema without columns: {e}")))
}

/// Commit a schema evolution to the Iceberg REST catalog.
///
/// Uses `AddSchema` + `SetCurrentSchema { schema_id: -1 }` to atomically add
/// and activate the new schema. Optimistic concurrency is enforced via
/// `CurrentSchemaIdMatch` and `LastAssignedFieldIdMatch` requirements.
///
/// **Important — duplicate schema handling**: Iceberg keeps all historical
/// schemas. If a column is added, dropped, then the table reverts to a schema
/// that already exists in the history, the Java REST catalog's `AddSchema`
/// silently no-ops (the schema is already present). Because nothing was "added",
/// `SetCurrentSchema { schema_id: -1 }` ("last added schema") fails with
/// `"Cannot set last added schema: no schema has been added"`. We detect this
/// case by comparing the new schema against all existing schemas in the table
/// metadata and, if a match is found, issue a `SetCurrentSchema` with the
/// explicit schema ID instead.
///
/// **Important — `last-column-id` watermark**: iceberg-rust's
/// `TableUpdate::AddSchema` serializes `last-column-id` from
/// `schema.highest_field_id()`, but the Java REST catalog requires this value
/// to be >= the table's previous `last_column_id`. We patch the serialized JSON
/// to use `max(table.last_column_id, schema.highest_field_id)`.
pub async fn commit_schema_evolution(
    table: &Table,
    new_schema: Schema,
    catalog_config: &RestCatalogConfig,
    namespace: &NamespaceIdent,
    table_name: &str,
) -> Result<()> {
    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());
    let table_last_column_id = table.metadata().last_column_id();
    let effective_last_column_id =
        std::cmp::max(table_last_column_id, new_schema.highest_field_id());

    // Check if the new schema already exists in the table's schema history.
    // This happens when a column is added then dropped (or vice versa), reverting
    // to a previously committed schema.
    let existing_match = table.metadata().schemas_iter().find(|existing| {
        existing.as_struct() == new_schema.as_struct()
            && existing
                .identifier_field_ids()
                .eq(new_schema.identifier_field_ids())
    });

    let (requirements, updates) = if let Some(matched) = existing_match {
        let matched_id = matched.schema_id();
        if matched_id == table.metadata().current_schema_id() {
            tracing::info!(
                table = table_name,
                schema_id = matched_id,
                "schema evolution skipped: target schema is already current"
            );
            return Ok(());
        }
        tracing::info!(
            table = table_name,
            schema_id = matched_id,
            "schema evolution: reusing existing schema from history"
        );
        // Schema already exists — just switch to it, no AddSchema needed.
        let reqs = vec![TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: table.metadata().current_schema_id(),
        }];
        let ups = vec![TableUpdate::SetCurrentSchema {
            schema_id: matched_id,
        }];
        (reqs, ups)
    } else {
        // New schema — add and activate it.
        let reqs = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: table.metadata().current_schema_id(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: table_last_column_id,
            },
        ];
        let ups = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema {
                schema_id: -1, // Activate the last added schema
            },
        ];
        (reqs, ups)
    };

    let url = build_commit_url(
        &catalog_config.uri,
        &catalog_config.properties,
        namespace,
        table_name,
    );

    let request = CommitTableRequest {
        identifier: Some(table_ident),
        requirements,
        updates,
    };

    // Serialize to JSON Value, then patch `last-column-id` in each `add-schema`
    // update. iceberg-rust omits this field, but the Java REST catalog requires it
    // to be >= the previous last_column_id (the field ID watermark must never decrease).
    let mut body = serde_json::to_value(&request)
        .map_err(|e| CdcError::Iceberg(format!("serialize commit request: {e}")))?;

    if let Some(updates_arr) = body.get_mut("updates").and_then(|v| v.as_array_mut()) {
        for update in updates_arr.iter_mut() {
            if update.get("action").and_then(|a| a.as_str()) == Some("add-schema") {
                update["last-column-id"] =
                    serde_json::Value::Number(serde_json::Number::from(effective_last_column_id));
            }
        }
    }

    let timeout = std::time::Duration::from_secs(catalog_config.timeout_secs);
    let client = reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .map_err(|e| CdcError::Iceberg(format!("HTTP client: {e}")))?;

    let response = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| CdcError::Iceberg(format!("schema evolution commit request: {e}")))?;

    let status = response.status();
    if status.is_success() {
        tracing::info!(table = table_name, "schema evolution commit succeeded");
        Ok(())
    } else {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable>".into());
        Err(CdcError::Iceberg(format!(
            "schema evolution commit failed (HTTP {status}): {body}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ChangeOp, ColumnValue, Lsn, TableId};
    use crate::schema::CanonicalType;
    use iceberg::spec::PrimitiveType;
    use std::collections::BTreeMap;

    fn test_table_id() -> TableId {
        TableId {
            schema: "public".into(),
            name: "users".into(),
            oid: 1,
        }
    }

    fn make_event(
        op: ChangeOp,
        new: Option<BTreeMap<String, ColumnValue>>,
        old: Option<BTreeMap<String, ColumnValue>>,
    ) -> crate::event::CdcEvent {
        crate::event::CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op,
            new,
            old,
            primary_key_columns: vec![],
        }
    }

    fn test_iceberg_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .build()
            .unwrap()
    }

    #[test]
    fn test_evolved_schema_preserves_existing_fields() {
        let current = test_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 3,
        }];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        let fields = evolved.as_struct().fields();

        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].id, 1);
        assert!(fields[0].required);

        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].id, 2);
        assert!(!fields[1].required);
    }

    #[test]
    fn test_evolved_schema_appends_new_fields_as_optional() {
        let current = test_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: false, // Even if non-nullable in source, added as optional
            ordinal_position: 3,
        }];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        let fields = evolved.as_struct().fields();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[2].name, "email");
        assert!(!fields[2].required); // Always optional for schema evolution
        assert_eq!(
            *fields[2].field_type,
            Type::Primitive(PrimitiveType::String)
        );
    }

    #[test]
    fn test_evolved_schema_field_ids_continue_from_highest() {
        let current = test_iceberg_schema();
        let new_cols = vec![
            ColumnInfo {
                name: "email".into(),
                source_type: "text".into(),
                canonical_type: CanonicalType::Text,
                is_nullable: true,
                ordinal_position: 3,
            },
            ColumnInfo {
                name: "age".into(),
                source_type: "integer".into(),
                canonical_type: CanonicalType::Int,
                is_nullable: true,
                ordinal_position: 4,
            },
        ];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        let fields = evolved.as_struct().fields();

        assert_eq!(fields[2].id, 3);
        assert_eq!(fields[2].name, "email");
        assert_eq!(fields[3].id, 4);
        assert_eq!(fields[3].name, "age");
    }

    #[test]
    fn test_evolved_schema_preserves_identifier_field_ids() {
        let current = test_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 3,
        }];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        let ids: Vec<i32> = evolved.identifier_field_ids().collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn test_evolved_schema_increments_schema_id() {
        let current = test_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 3,
        }];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        assert_eq!(evolved.schema_id(), 1);
    }

    #[test]
    fn test_evolved_schema_type_mapping_correct() {
        let current = test_iceberg_schema();
        let new_cols = vec![
            ColumnInfo {
                name: "count".into(),
                source_type: "bigint".into(),
                canonical_type: CanonicalType::BigInt,
                is_nullable: true,
                ordinal_position: 3,
            },
            ColumnInfo {
                name: "active".into(),
                source_type: "boolean".into(),
                canonical_type: CanonicalType::Boolean,
                is_nullable: true,
                ordinal_position: 4,
            },
        ];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        let fields = evolved.as_struct().fields();

        assert_eq!(*fields[2].field_type, Type::Primitive(PrimitiveType::Long));
        assert_eq!(
            *fields[3].field_type,
            Type::Primitive(PrimitiveType::Boolean)
        );
    }

    fn test_cdc_iceberg_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "_cdc_op", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "_cdc_lsn", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(3, "_cdc_timestamp_us", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(4, "_cdc_snapshot", Type::Primitive(PrimitiveType::Boolean))
                    .into(),
                NestedField::required(5, "_cdc_schema", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(6, "_cdc_table", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(
                    7,
                    "_cdc_primary_key_columns",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
                NestedField::optional(8, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(9, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(10, "_old_id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(11, "_old_name", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .with_schema_id(0)
            .build()
            .unwrap()
    }

    #[test]
    fn test_evolved_cdc_schema_adds_paired_columns() {
        let current = test_cdc_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 3,
        }];

        let evolved = build_evolved_cdc_schema(&current, &new_cols).unwrap();
        let fields = evolved.as_struct().fields();

        assert_eq!(fields.len(), 13);

        // Layout must be [7 meta] [source cols] [_old_ cols]
        // email inserted before _old_ section (at index 9)
        assert_eq!(fields[9].name, "email");
        assert!(!fields[9].required);
        assert_eq!(
            *fields[9].field_type,
            Type::Primitive(PrimitiveType::String)
        );
        assert_eq!(fields[9].id, 12); // next after highest (11)

        // Original _old_ columns shifted right
        assert_eq!(fields[10].name, "_old_id");
        assert_eq!(fields[11].name, "_old_name");

        // _old_email appended at end
        assert_eq!(fields[12].name, "_old_email");
        assert!(!fields[12].required);
        assert_eq!(
            *fields[12].field_type,
            Type::Primitive(PrimitiveType::String)
        );
        assert_eq!(fields[12].id, 13);
    }

    #[test]
    fn test_evolved_cdc_schema_multiple_new_cols() {
        let current = test_cdc_iceberg_schema();
        let new_cols = vec![
            ColumnInfo {
                name: "email".into(),
                source_type: "text".into(),
                canonical_type: CanonicalType::Text,
                is_nullable: true,
                ordinal_position: 3,
            },
            ColumnInfo {
                name: "age".into(),
                source_type: "integer".into(),
                canonical_type: CanonicalType::Int,
                is_nullable: true,
                ordinal_position: 4,
            },
        ];

        let evolved = build_evolved_cdc_schema(&current, &new_cols).unwrap();
        let fields = evolved.as_struct().fields();

        // 11 + 4 (email + _old_email + age + _old_age) = 15
        assert_eq!(fields.len(), 15);

        // Layout: [7 meta] [id, name, email, age] [_old_id, _old_name, _old_email, _old_age]
        assert_eq!(fields[7].name, "id");
        assert_eq!(fields[8].name, "name");
        assert_eq!(fields[9].name, "email");
        assert_eq!(fields[10].name, "age");
        assert_eq!(fields[11].name, "_old_id");
        assert_eq!(fields[12].name, "_old_name");
        assert_eq!(fields[13].name, "_old_email");
        assert_eq!(fields[14].name, "_old_age");
        assert_eq!(*fields[10].field_type, Type::Primitive(PrimitiveType::Int));
        assert_eq!(*fields[14].field_type, Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn test_evolved_cdc_schema_increments_schema_id() {
        let current = test_cdc_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 3,
        }];

        let evolved = build_evolved_cdc_schema(&current, &new_cols).unwrap();
        assert_eq!(evolved.schema_id(), 1);
    }

    #[test]
    fn test_evolved_cdc_schema_no_identifier_ids() {
        let current = test_cdc_iceberg_schema();
        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 3,
        }];

        let evolved = build_evolved_cdc_schema(&current, &new_cols).unwrap();
        assert_eq!(evolved.identifier_field_ids().count(), 0);
    }

    #[test]
    fn test_evolved_schema_no_identifier_ids_when_none() {
        // Schema without identifier field IDs (CDC mode)
        let current = Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )
            .into()])
            .with_schema_id(0)
            .build()
            .unwrap();

        let new_cols = vec![ColumnInfo {
            name: "email".into(),
            source_type: "text".into(),
            canonical_type: CanonicalType::Text,
            is_nullable: true,
            ordinal_position: 2,
        }];

        let evolved = build_evolved_iceberg_schema(&current, &new_cols).unwrap();
        assert_eq!(evolved.identifier_field_ids().count(), 0);
    }

    fn test_iceberg_schema_3col() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "email", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .build()
            .unwrap()
    }

    #[test]
    fn test_build_schema_without_columns_single() {
        let current = test_iceberg_schema_3col();
        let result = build_schema_without_columns(&current, &["email".into()], false).unwrap();
        let fields = result.as_struct().fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "name");
    }

    #[test]
    fn test_build_schema_without_columns_preserves_other_fields() {
        let current = test_iceberg_schema_3col();
        let result = build_schema_without_columns(&current, &["email".into()], false).unwrap();
        let fields = result.as_struct().fields();
        // Original field IDs preserved
        assert_eq!(fields[0].id, 1);
        assert!(fields[0].required);
        assert_eq!(fields[1].id, 2);
        assert!(!fields[1].required);
    }

    #[test]
    fn test_build_schema_without_columns_preserves_identifier_ids() {
        let current = test_iceberg_schema_3col();
        let result = build_schema_without_columns(&current, &["email".into()], false).unwrap();
        let ids: Vec<i32> = result.identifier_field_ids().collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn test_build_schema_without_columns_increments_schema_id() {
        let current = test_iceberg_schema_3col();
        let result = build_schema_without_columns(&current, &["email".into()], false).unwrap();
        assert_eq!(result.schema_id(), 1);
    }

    #[test]
    fn test_build_schema_without_columns_cdc_drops_paired_old() {
        // CDC schema with paired _old_ columns
        let current = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "_cdc_op", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(4, "email", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(5, "_old_id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(6, "_old_name", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::optional(7, "_old_email", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .with_schema_id(0)
            .build()
            .unwrap();

        let result = build_schema_without_columns(&current, &["email".into()], true).unwrap();
        let field_names: Vec<&str> = result
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert!(!field_names.contains(&"email"));
        assert!(!field_names.contains(&"_old_email"));
        assert!(field_names.contains(&"_cdc_op"));
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"_old_id"));
        assert!(field_names.contains(&"_old_name"));
    }

    #[test]
    fn test_build_schema_without_columns_cdc_preserves_metadata() {
        let current = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "_cdc_op", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "_cdc_timestamp", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::optional(3, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(4, "email", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(5, "_old_id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(6, "_old_email", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .with_schema_id(0)
            .build()
            .unwrap();

        // Even if we try to drop _cdc_op, it won't match because _cdc_op isn't a source column
        let result = build_schema_without_columns(&current, &["email".into()], true).unwrap();
        let field_names: Vec<&str> = result
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert!(field_names.contains(&"_cdc_op"));
        assert!(field_names.contains(&"_cdc_timestamp"));
    }

    fn test_arrow_schema_3col() -> arrow_schema::Schema {
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("email", arrow_schema::DataType::Utf8, true),
        ])
    }

    #[test]
    fn test_detect_dropped_no_full_row_events() {
        let schema = test_arrow_schema_3col();
        let events = vec![make_event(
            ChangeOp::Delete,
            None,
            Some(BTreeMap::from([("id".into(), ColumnValue::Int(1))])),
        )];

        let (has_full, missing) = detect_dropped_columns_from_events(&schema, &events, false);
        assert!(!has_full);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_detect_dropped_missing_column() {
        let schema = test_arrow_schema_3col();
        let events = vec![make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            None,
        )];

        let (has_full, missing) = detect_dropped_columns_from_events(&schema, &events, false);
        assert!(has_full);
        assert_eq!(missing, vec!["email"]);
    }

    #[test]
    fn test_detect_dropped_skips_metadata_columns() {
        let cdc_schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("_cdc_op", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("_cdc_timestamp", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("_old_id", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("_old_name", arrow_schema::DataType::Utf8, true),
        ]);

        let events = vec![make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            None,
        )];

        // In CDC mode, _cdc_* and _old_* columns should not be flagged as dropped
        let (has_full, missing) = detect_dropped_columns_from_events(&cdc_schema, &events, true);
        assert!(has_full);
        assert!(missing.is_empty());
    }
}
