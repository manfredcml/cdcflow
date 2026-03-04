use tokio_postgres::Client;

use crate::error::{CdcError, Result};
use crate::schema::CanonicalType;

/// Convert a CanonicalType to a PostgreSQL DDL type string.
///
/// This is the PG-specific leg of the canonical type system.
/// For PG-to-PG passthrough, callers should use `ColumnInfo.source_type` directly
/// (which comes from `format_type()` and is already valid PG DDL).
pub fn canonical_to_pg_ddl(ct: &CanonicalType) -> String {
    match ct {
        CanonicalType::Boolean => "boolean".into(),
        CanonicalType::SmallInt => "smallint".into(),
        CanonicalType::Int => "integer".into(),
        CanonicalType::BigInt => "bigint".into(),
        CanonicalType::Float => "real".into(),
        CanonicalType::Double => "double precision".into(),
        CanonicalType::Decimal { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) => format!("numeric({p},{s})"),
            (Some(p), None) => format!("numeric({p})"),
            _ => "numeric".into(),
        },
        CanonicalType::VarString { length } => match length {
            Some(len) => format!("character varying({len})"),
            None => "character varying".into(),
        },
        CanonicalType::FixedString { length } => match length {
            Some(len) => format!("character({len})"),
            None => "character(1)".into(),
        },
        CanonicalType::Text => "text".into(),
        CanonicalType::Timestamp => "timestamp without time zone".into(),
        CanonicalType::TimestampTz => "timestamp with time zone".into(),
        CanonicalType::Date => "date".into(),
        CanonicalType::Time => "time without time zone".into(),
        CanonicalType::Binary => "bytea".into(),
        CanonicalType::Uuid => "uuid".into(),
        CanonicalType::Json => "jsonb".into(),
        CanonicalType::Year => "smallint".into(),
    }
}

/// Build a fully qualified table name: "schema"."table"
fn build_fqn(schema: &str, table: &str) -> String {
    format!("\"{}\".\"{}\"", schema, table)
}

/// Build the DDL string for creating a CDC-mode table with flattened typed columns.
///
/// Layout: 7 `_cdc_*` metadata columns + N source columns + N `_old_*` columns.
/// All source and `_old_*` columns are NULLable (inserts have no old values,
/// deletes have no new values, schema evolution may add new columns).
///
/// `source_columns` is a slice of `(name, sql_type)` pairs.
fn build_cdc_table_ddl(
    schema: &str,
    table: &str,
    source_columns: &[(String, String)],
) -> String {
    let fqn = build_fqn(schema, table);
    let mut col_defs = vec![
        "\"_cdc_op\" text NOT NULL".to_string(),
        "\"_cdc_lsn\" bigint NOT NULL".to_string(),
        "\"_cdc_timestamp_us\" bigint NOT NULL".to_string(),
        "\"_cdc_snapshot\" boolean NOT NULL".to_string(),
        "\"_cdc_schema\" text NOT NULL".to_string(),
        "\"_cdc_table\" text NOT NULL".to_string(),
        "\"_cdc_primary_key_columns\" text NOT NULL".to_string(),
    ];

    for (name, typ) in source_columns {
        col_defs.push(format!("\"{}\" {}", name, typ));
    }

    for (name, typ) in source_columns {
        col_defs.push(format!("\"_old_{}\" {}", name, typ));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        fqn,
        col_defs.join(", ")
    )
}

/// Build an INSERT SQL string for the flattened CDC schema.
///
/// Returns a parameterized INSERT with `$N::type` casts for all columns.
/// The parameter order matches: 7 metadata + N source + N `_old_*` columns.
fn build_cdc_insert_sql(
    target_fqn: &str,
    source_columns: &[String],
    column_types: &std::collections::HashMap<String, String>,
) -> String {
    let mut col_names = vec![
        "\"_cdc_op\"".to_string(),
        "\"_cdc_lsn\"".to_string(),
        "\"_cdc_timestamp_us\"".to_string(),
        "\"_cdc_snapshot\"".to_string(),
        "\"_cdc_schema\"".to_string(),
        "\"_cdc_table\"".to_string(),
        "\"_cdc_primary_key_columns\"".to_string(),
    ];

    let mut placeholders = vec![
        "$1::text".to_string(),
        "$2::bigint".to_string(),
        "$3::bigint".to_string(),
        "$4::boolean".to_string(),
        "$5::text".to_string(),
        "$6::text".to_string(),
        "$7::text".to_string(),
    ];

    let mut idx = 8;

    for col in source_columns {
        col_names.push(format!("\"{}\"", col));
        let typ = column_types.get(col).map(|t| t.as_str()).unwrap_or("text");
        placeholders.push(format!("${}::{}", idx, typ));
        idx += 1;
    }

    for col in source_columns {
        col_names.push(format!("\"_old_{}\"", col));
        let typ = column_types.get(col).map(|t| t.as_str()).unwrap_or("text");
        placeholders.push(format!("${}::{}", idx, typ));
        idx += 1;
    }

    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        target_fqn,
        col_names.join(", "),
        placeholders.join(", ")
    )
}

/// Build the DDL string for creating a replication-mode table.
///
/// `columns` is a slice of `(name, sql_type)` pairs.
fn build_replication_table_ddl(
    schema: &str,
    table: &str,
    columns: &[(String, String)],
    pk_columns: &[String],
) -> String {
    let fqn = build_fqn(schema, table);
    let col_defs: Vec<String> = columns
        .iter()
        .map(|(name, typ)| format!("\"{}\" {}", name, typ))
        .collect();
    let pk_list: Vec<String> = pk_columns.iter().map(|c| format!("\"{}\"", c)).collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
        fqn,
        col_defs.join(", "),
        pk_list.join(", ")
    )
}

/// Build the DDL string for adding columns to an existing table.
///
/// `new_columns` is a slice of `(name, sql_type)` pairs.
fn build_add_columns_ddl(schema: &str, table: &str, new_columns: &[(String, String)]) -> String {
    let fqn = build_fqn(schema, table);
    let alter_clauses: Vec<String> = new_columns
        .iter()
        .map(|(name, typ)| format!("ADD COLUMN IF NOT EXISTS \"{}\" {}", name, typ))
        .collect();
    format!("ALTER TABLE {} {}", fqn, alter_clauses.join(", "))
}

/// Build the DDL string for dropping columns from an existing table.
///
/// `columns` is the list of column names to drop.
fn build_drop_columns_ddl(schema: &str, table: &str, columns: &[String]) -> String {
    let fqn = build_fqn(schema, table);
    let alter_clauses: Vec<String> = columns
        .iter()
        .map(|name| format!("DROP COLUMN IF EXISTS \"{}\"", name))
        .collect();
    format!("ALTER TABLE {} {}", fqn, alter_clauses.join(", "))
}

/// Check whether a table exists in the given schema.
pub async fn table_exists(client: &Client, schema: &str, table: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &table],
        )
        .await?;
    Ok(row.get(0))
}

/// Create a CDC-mode table with flattened typed columns.
///
/// Layout: 7 `_cdc_*` metadata columns + N source + N `_old_*` columns.
/// `source_columns` is a slice of `(name, sql_type)` pairs.
pub async fn create_cdc_table(
    client: &Client,
    schema: &str,
    table: &str,
    source_columns: &[(String, String)],
) -> Result<()> {
    let ddl = build_cdc_table_ddl(schema, table, source_columns);
    client.execute(&ddl, &[]).await?;
    Ok(())
}

/// Build the INSERT SQL for a flattened CDC table.
pub fn build_cdc_insert(
    target_fqn: &str,
    source_columns: &[String],
    column_types: &std::collections::HashMap<String, String>,
) -> String {
    build_cdc_insert_sql(target_fqn, source_columns, column_types)
}

/// Create a replication-mode table with typed columns and a primary key.
///
/// `columns` is a slice of `(name, sql_type)` pairs.
pub async fn create_replication_table(
    client: &Client,
    schema: &str,
    table: &str,
    columns: &[(String, String)],
    pk_columns: &[String],
) -> Result<()> {
    if columns.is_empty() {
        return Err(CdcError::Config(
            "cannot create replication table with no columns".into(),
        ));
    }
    if pk_columns.is_empty() {
        return Err(CdcError::Config(
            "cannot create replication table without primary key columns".into(),
        ));
    }

    let ddl = build_replication_table_ddl(schema, table, columns, pk_columns);
    client.execute(&ddl, &[]).await?;
    Ok(())
}

/// Fetch column names and their SQL types from the target database.
pub async fn fetch_target_columns_with_types(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<(String, String)>> {
    let rows = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod)
             FROM pg_attribute a
             JOIN pg_class c ON a.attrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = $1
               AND c.relname = $2
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&schema, &table],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Fetch primary key column names from the target database.
pub async fn fetch_target_pk(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
             WHERE tc.constraint_type = 'PRIMARY KEY'
                 AND tc.table_schema = $1
                 AND tc.table_name = $2
             ORDER BY kcu.ordinal_position",
            &[&schema, &table],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Drop columns from an existing table.
///
/// Issues `ALTER TABLE ... DROP COLUMN IF EXISTS "col"` for each column,
/// making the operation idempotent (safe on replays/races).
pub async fn drop_columns(
    client: &Client,
    schema: &str,
    table: &str,
    columns: &[String],
) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }

    let ddl = build_drop_columns_ddl(schema, table, columns);
    client.execute(&ddl, &[]).await?;
    Ok(())
}

/// Add new typed columns to an existing replication-mode table.
///
/// Issues `ALTER TABLE ... ADD COLUMN IF NOT EXISTS "col" type` for each
/// new column, making the operation idempotent (safe on replays/races).
///
/// `new_columns` is a slice of `(name, sql_type)` pairs.
pub async fn add_columns(
    client: &Client,
    schema: &str,
    table: &str,
    new_columns: &[(String, String)],
) -> Result<()> {
    if new_columns.is_empty() {
        return Ok(());
    }

    let ddl = build_add_columns_ddl(schema, table, new_columns);
    client.execute(&ddl, &[]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_fqn() {
        assert_eq!(build_fqn("public", "users"), "\"public\".\"users\"");
        assert_eq!(
            build_fqn("my-schema", "my_table"),
            "\"my-schema\".\"my_table\""
        );
    }

    #[test]
    fn test_build_cdc_table_ddl() {
        let source_cols = vec![
            ("id".into(), "integer".into()),
            ("name".into(), "text".into()),
        ];
        let ddl = build_cdc_table_ddl("public", "cdc_users", &source_cols);
        assert!(ddl.contains("\"public\".\"cdc_users\""));
        // Metadata columns
        assert!(ddl.contains("\"_cdc_op\" text NOT NULL"));
        assert!(ddl.contains("\"_cdc_lsn\" bigint NOT NULL"));
        assert!(ddl.contains("\"_cdc_timestamp_us\" bigint NOT NULL"));
        assert!(ddl.contains("\"_cdc_snapshot\" boolean NOT NULL"));
        assert!(ddl.contains("\"_cdc_schema\" text NOT NULL"));
        assert!(ddl.contains("\"_cdc_table\" text NOT NULL"));
        assert!(ddl.contains("\"_cdc_primary_key_columns\" text NOT NULL"));
        // Source columns
        assert!(ddl.contains("\"id\" integer"));
        assert!(ddl.contains("\"name\" text"));
        // Old columns
        assert!(ddl.contains("\"_old_id\" integer"));
        assert!(ddl.contains("\"_old_name\" text"));
    }

    #[test]
    fn test_build_cdc_table_ddl_no_source_columns() {
        let ddl = build_cdc_table_ddl("public", "cdc_empty", &[]);
        // Should still have 7 metadata columns
        assert!(ddl.contains("\"_cdc_op\" text NOT NULL"));
        assert!(ddl.contains("\"_cdc_primary_key_columns\" text NOT NULL"));
        // No source or old columns
        assert!(!ddl.contains("\"_old_"));
    }

    #[test]
    fn test_build_cdc_insert_sql() {
        let source_cols = vec!["id".to_string(), "name".to_string()];
        let mut types = std::collections::HashMap::new();
        types.insert("id".to_string(), "integer".to_string());
        types.insert("name".to_string(), "text".to_string());
        let sql = build_cdc_insert_sql("\"public\".\"cdc_users\"", &source_cols, &types);
        // Check column names
        assert!(sql.contains("\"_cdc_op\""));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("\"_old_id\""));
        assert!(sql.contains("\"_old_name\""));
        // Check typed placeholders
        assert!(sql.contains("$1::text"));       // _cdc_op
        assert!(sql.contains("$8::integer"));    // id
        assert!(sql.contains("$9::text"));       // name
        assert!(sql.contains("$10::integer"));   // _old_id
        assert!(sql.contains("$11::text"));      // _old_name
    }

    #[test]
    fn test_build_replication_table_ddl() {
        let columns = vec![
            ("id".into(), "integer".into()),
            ("name".into(), "text".into()),
            ("email".into(), "character varying(255)".into()),
        ];
        let pk = vec!["id".into()];
        let ddl = build_replication_table_ddl("public", "users", &columns, &pk);
        assert!(ddl.contains("\"public\".\"users\""));
        assert!(ddl.contains("\"id\" integer"));
        assert!(ddl.contains("\"name\" text"));
        assert!(ddl.contains("\"email\" character varying(255)"));
        assert!(ddl.contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn test_build_replication_table_ddl_composite_pk() {
        let columns = vec![
            ("tenant_id".into(), "uuid".into()),
            ("user_id".into(), "bigint".into()),
            ("name".into(), "text".into()),
        ];
        let pk = vec!["tenant_id".into(), "user_id".into()];
        let ddl = build_replication_table_ddl("public", "users", &columns, &pk);
        assert!(ddl.contains("PRIMARY KEY (\"tenant_id\", \"user_id\")"));
    }

    #[test]
    fn test_build_replication_table_ddl_preserves_pg_types() {
        let columns = vec![
            ("id".into(), "integer".into()),
            ("is_active".into(), "boolean".into()),
            ("balance".into(), "numeric(10,2)".into()),
            ("tags".into(), "text[]".into()),
            ("metadata".into(), "jsonb".into()),
            ("created_at".into(), "timestamp with time zone".into()),
        ];
        let pk = vec!["id".into()];
        let ddl = build_replication_table_ddl("public", "accounts", &columns, &pk);
        assert!(ddl.contains("\"id\" integer"));
        assert!(ddl.contains("\"is_active\" boolean"));
        assert!(ddl.contains("\"balance\" numeric(10,2)"));
        assert!(ddl.contains("\"tags\" text[]"));
        assert!(ddl.contains("\"metadata\" jsonb"));
        assert!(ddl.contains("\"created_at\" timestamp with time zone"));
    }

    #[test]
    fn test_build_add_columns_ddl_single() {
        let cols = vec![("email".into(), "character varying(255)".into())];
        let ddl = build_add_columns_ddl("public", "users", &cols);
        assert_eq!(
            ddl,
            "ALTER TABLE \"public\".\"users\" ADD COLUMN IF NOT EXISTS \"email\" character varying(255)"
        );
    }

    #[test]
    fn test_build_add_columns_ddl_multiple() {
        let cols = vec![
            ("email".into(), "text".into()),
            ("is_paid".into(), "boolean".into()),
        ];
        let ddl = build_add_columns_ddl("public", "users", &cols);
        assert_eq!(
            ddl,
            "ALTER TABLE \"public\".\"users\" ADD COLUMN IF NOT EXISTS \"email\" text, ADD COLUMN IF NOT EXISTS \"is_paid\" boolean"
        );
    }

    #[test]
    fn test_build_drop_columns_ddl_single() {
        let cols = vec!["email".into()];
        let ddl = build_drop_columns_ddl("public", "users", &cols);
        assert_eq!(
            ddl,
            "ALTER TABLE \"public\".\"users\" DROP COLUMN IF EXISTS \"email\""
        );
    }

    #[test]
    fn test_build_drop_columns_ddl_multiple() {
        let cols = vec!["email".into(), "phone".into()];
        let ddl = build_drop_columns_ddl("public", "users", &cols);
        assert_eq!(
            ddl,
            "ALTER TABLE \"public\".\"users\" DROP COLUMN IF EXISTS \"email\", DROP COLUMN IF EXISTS \"phone\""
        );
    }

    #[test]
    fn test_canonical_to_pg_ddl() {
        let cases = vec![
            (CanonicalType::Boolean, "boolean"),
            (CanonicalType::SmallInt, "smallint"),
            (CanonicalType::Int, "integer"),
            (CanonicalType::BigInt, "bigint"),
            (CanonicalType::Float, "real"),
            (CanonicalType::Double, "double precision"),
            (
                CanonicalType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
                "numeric(10,2)",
            ),
            (
                CanonicalType::Decimal {
                    precision: None,
                    scale: None,
                },
                "numeric",
            ),
            (
                CanonicalType::VarString {
                    length: Some(255),
                },
                "character varying(255)",
            ),
            (
                CanonicalType::VarString { length: None },
                "character varying",
            ),
            (
                CanonicalType::FixedString {
                    length: Some(10),
                },
                "character(10)",
            ),
            (
                CanonicalType::FixedString { length: None },
                "character(1)",
            ),
            (CanonicalType::Text, "text"),
            (CanonicalType::Timestamp, "timestamp without time zone"),
            (CanonicalType::TimestampTz, "timestamp with time zone"),
            (CanonicalType::Date, "date"),
            (CanonicalType::Time, "time without time zone"),
            (CanonicalType::Binary, "bytea"),
            (CanonicalType::Uuid, "uuid"),
            (CanonicalType::Json, "jsonb"),
            (CanonicalType::Year, "smallint"),
        ];
        for (canonical, expected) in cases {
            assert_eq!(
                canonical_to_pg_ddl(&canonical),
                expected,
                "failed for {:?}",
                canonical
            );
        }
    }

    /// Verify that MySQL types mapped through the canonical system produce
    /// the same PG DDL as the old direct mysql_type_to_pg_type function.
    #[test]
    fn test_mysql_type_roundtrip_through_canonical() {
        use crate::schema::type_mapping::parse_source_type;
        use crate::schema::SourceDialect;

        let cases = vec![
            // MySQL type → expected PG DDL
            ("int(11)", "integer"),
            ("bigint(20)", "bigint"),
            ("smallint(5)", "smallint"),
            ("tinyint(4)", "smallint"),
            ("tinyint(1)", "boolean"),
            ("bool", "boolean"),
            ("float", "real"),
            ("double", "double precision"),
            ("decimal(10,2)", "numeric(10,2)"),
            ("varchar(255)", "character varying(255)"),
            ("char(10)", "character(10)"),
            ("text", "text"),
            ("datetime", "timestamp without time zone"),
            ("date", "date"),
            ("time", "time without time zone"),
            ("year", "smallint"),
            ("json", "jsonb"),
            ("blob", "bytea"),
            ("enum", "text"),
        ];
        for (mysql_type, expected_pg) in cases {
            let canonical = parse_source_type(SourceDialect::Mysql, mysql_type);
            let pg_ddl = canonical_to_pg_ddl(&canonical);
            assert_eq!(
                pg_ddl, expected_pg,
                "failed for MySQL type '{mysql_type}'"
            );
        }
    }
}
