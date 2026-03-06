use std::collections::HashSet;

use tokio_postgres::Client;

use crate::error::{CdcError, Result};

/// Quote a SQL identifier to prevent injection.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Build the CREATE PUBLICATION SQL statement.
/// If `tables` is empty, creates FOR ALL TABLES.
/// If `tables` is non-empty, creates FOR TABLE with quoted identifiers.
fn build_create_publication_sql(publication_name: &str, tables: &[String]) -> String {
    if tables.is_empty() {
        format!(
            "CREATE PUBLICATION {} FOR ALL TABLES",
            quote_ident(publication_name)
        )
    } else {
        let table_list: Vec<String> = tables
            .iter()
            .map(|t| {
                let parts: Vec<&str> = t.splitn(2, '.').collect();
                if parts.len() == 2 {
                    format!("{}.{}", quote_ident(parts[0]), quote_ident(parts[1]))
                } else {
                    quote_ident(t)
                }
            })
            .collect();
        format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            quote_ident(publication_name),
            table_list.join(", ")
        )
    }
}

/// Validate that the actual tables in a publication match the expected tables.
/// Comparison is order-independent (set equality).
fn validate_tables_match(
    publication_name: &str,
    expected: &[String],
    actual: &[String],
) -> Result<()> {
    let expected_set: HashSet<&str> = expected.iter().map(|s| s.as_str()).collect();
    let actual_set: HashSet<&str> = actual.iter().map(|s| s.as_str()).collect();

    if expected_set != actual_set {
        let missing: Vec<_> = expected_set.difference(&actual_set).collect();
        let extra: Vec<_> = actual_set.difference(&expected_set).collect();
        return Err(CdcError::Config(format!(
            "publication '{}' tables mismatch: missing {:?}, extra {:?}",
            publication_name, missing, extra,
        )));
    }

    Ok(())
}

/// Check if a publication exists.
pub async fn publication_exists(client: &Client, publication_name: &str) -> Result<bool> {
    let row = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .map_err(|e| CdcError::Protocol(format!("check publication '{publication_name}': {e}")))?;

    Ok(row.is_some())
}

/// Create a publication.
/// If `tables` is empty, creates FOR ALL TABLES.
/// If `tables` is non-empty, creates FOR TABLE with the specified tables.
pub async fn create_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> Result<()> {
    let sql = build_create_publication_sql(publication_name, tables);

    client
        .execute(&sql, &[])
        .await
        .map_err(|e| CdcError::Protocol(format!("create publication '{publication_name}': {e}")))?;

    if tables.is_empty() {
        tracing::info!(publication_name, "created publication for all tables");
    } else {
        tracing::info!(publication_name, ?tables, "created publication for tables");
    }

    Ok(())
}

/// Get the list of tables in a publication as `schema.table` strings.
pub async fn get_publication_tables(
    client: &Client,
    publication_name: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .map_err(|e| CdcError::Protocol(format!("query publication tables: {e}")))?;

    let tables: Vec<String> = rows
        .iter()
        .map(|r| {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            format!("{schema}.{table}")
        })
        .collect();

    tracing::info!(publication_name, ?tables, "discovered publication tables");
    Ok(tables)
}

/// Ensure the publication exists and is correctly configured.
///
/// - If `create_publication` is false and publication doesn't exist: error.
/// - If publication doesn't exist and `create_publication` is true: create it.
/// - If publication exists and `expected_tables` is non-empty: validate tables match.
/// - If publication exists and `expected_tables` is empty: skip validation.
pub async fn ensure_publication(
    client: &Client,
    publication_name: &str,
    create: bool,
    expected_tables: &[String],
) -> Result<()> {
    let exists = publication_exists(client, publication_name).await?;

    if !exists {
        if !create {
            return Err(CdcError::Config(format!(
                "publication '{}' does not exist and create_publication is disabled",
                publication_name,
            )));
        }
        create_publication(client, publication_name, expected_tables).await?;
        return Ok(());
    }

    // Publication exists — validate tables if expected list is non-empty
    if !expected_tables.is_empty() {
        let actual_tables = get_publication_tables(client, publication_name).await?;
        validate_tables_match(publication_name, expected_tables, &actual_tables)?;
    }

    tracing::info!(publication_name, "publication already exists");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_create_publication_sql_all_tables() {
        let sql = build_create_publication_sql("my_pub", &[]);
        assert_eq!(sql, r#"CREATE PUBLICATION "my_pub" FOR ALL TABLES"#);
    }

    #[test]
    fn test_build_create_publication_sql_specific_tables() {
        let tables = vec!["public.users".to_string(), "public.orders".to_string()];
        let sql = build_create_publication_sql("cdc_pub", &tables);
        assert_eq!(
            sql,
            r#"CREATE PUBLICATION "cdc_pub" FOR TABLE "public"."users", "public"."orders""#
        );
    }

    #[test]
    fn test_build_create_publication_sql_quotes_special_chars() {
        let tables = vec!["public.my\"table".to_string()];
        let sql = build_create_publication_sql("pub\"name", &tables);
        assert_eq!(
            sql,
            r#"CREATE PUBLICATION "pub""name" FOR TABLE "public"."my""table""#
        );
    }

    #[test]
    fn test_build_create_publication_sql_unqualified_table() {
        let tables = vec!["users".to_string()];
        let sql = build_create_publication_sql("my_pub", &tables);
        assert_eq!(sql, r#"CREATE PUBLICATION "my_pub" FOR TABLE "users""#);
    }

    #[test]
    fn test_validate_tables_match_same_order() {
        let expected = vec!["public.users".to_string(), "public.orders".to_string()];
        let actual = vec!["public.users".to_string(), "public.orders".to_string()];
        assert!(validate_tables_match("pub", &expected, &actual).is_ok());
    }

    #[test]
    fn test_validate_tables_match_different_order() {
        let expected = vec!["public.orders".to_string(), "public.users".to_string()];
        let actual = vec!["public.users".to_string(), "public.orders".to_string()];
        assert!(validate_tables_match("pub", &expected, &actual).is_ok());
    }

    #[test]
    fn test_validate_tables_match_mismatch() {
        let expected = vec!["public.users".to_string(), "public.orders".to_string()];
        let actual = vec!["public.users".to_string(), "public.products".to_string()];
        let err = validate_tables_match("my_pub", &expected, &actual).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("my_pub"),
            "error should mention publication name"
        );
        assert!(msg.contains("mismatch"), "error should mention mismatch");
    }

    #[test]
    fn test_validate_tables_match_extra_table() {
        let expected = vec!["public.users".to_string()];
        let actual = vec!["public.users".to_string(), "public.orders".to_string()];
        assert!(validate_tables_match("pub", &expected, &actual).is_err());
    }

    #[test]
    fn test_validate_tables_match_missing_table() {
        let expected = vec!["public.users".to_string(), "public.orders".to_string()];
        let actual = vec!["public.users".to_string()];
        assert!(validate_tables_match("pub", &expected, &actual).is_err());
    }
}
