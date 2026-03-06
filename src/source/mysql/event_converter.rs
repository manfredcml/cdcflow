use std::collections::{HashMap, HashSet};

use mysql_async::binlog::events::RowsEventData;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;

use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, ColumnValue, Lsn, Row, TableId};

use super::snapshot::parse_mysql_bytes_with_type;

/// Result of converting a MySQL binlog event.
#[derive(Debug)]
pub enum ConvertResult {
    /// A CDC event ready to emit.
    Event(CdcEvent),
    /// A transaction was committed — emit with this offset string.
    Commit { offset: String },
    /// Event was handled internally or is not relevant — skip.
    Skip,
}

/// Converts MySQL binlog events into CdcEvents.
///
/// Maintains a column name cache (populated during snapshot) and tracks the
/// current binlog filename for offset formatting.
pub struct EventConverter {
    /// (schema, table) → ordered column names
    column_cache: HashMap<(String, String), Vec<String>>,
    /// (schema, table) → set of column names that are TINYINT(1) (boolean)
    boolean_columns: HashMap<(String, String), HashSet<String>>,
    /// (schema, table) → primary key column names
    pk_cache: HashMap<(String, String), Vec<String>>,
    /// (schema, table) → (column_name → column_type) for type-aware Bytes parsing
    column_types: HashMap<(String, String), HashMap<String, String>>,
    /// Current binlog filename (updated on RotateEvent)
    binlog_filename: String,
    /// Whether we've already warned about missing TABLE_MAP metadata
    warned_no_metadata: bool,
}

impl EventConverter {
    pub fn new(
        binlog_filename: String,
        column_cache: HashMap<(String, String), Vec<String>>,
        boolean_columns: HashMap<(String, String), HashSet<String>>,
        pk_cache: HashMap<(String, String), Vec<String>>,
        column_types: HashMap<(String, String), HashMap<String, String>>,
    ) -> Self {
        Self {
            column_cache,
            boolean_columns,
            pk_cache,
            column_types,
            binlog_filename,
            warned_no_metadata: false,
        }
    }

    /// Update the binlog filename (called on RotateEvent).
    pub fn set_binlog_filename(&mut self, filename: String) {
        self.binlog_filename = filename;
    }

    /// Get the current binlog filename.
    pub fn binlog_filename(&self) -> &str {
        &self.binlog_filename
    }

    /// Format an offset string from the current filename and a position.
    pub fn format_offset(&self, position: u64) -> String {
        format_mysql_offset(&self.binlog_filename, position)
    }

    /// Convert a RowsEvent into CdcEvents.
    ///
    /// `rows_data` is the parsed RowsEventData.
    /// `schema` and `table` come from the TableMapEvent.
    /// `log_pos` is `header.log_pos()` — the position of the next event.
    pub fn convert_rows_event(
        &self,
        rows_data: &RowsEventData,
        schema: &str,
        table: &str,
        log_pos: u64,
        timestamp_us: i64,
        tme: &mysql_async::binlog::events::TableMapEvent,
    ) -> Result<Vec<ConvertResult>> {
        let table_id = TableId {
            schema: schema.to_string(),
            name: table.to_string(),
            oid: 0, // MySQL has no OID concept
        };

        let col_names = self.get_column_names(schema, table, tme.columns_count() as usize);
        let pk_columns = self.get_pk_columns(schema, table);

        let mut results = Vec::new();

        for row_result in rows_data.rows(tme) {
            let (before, after) = row_result
                .map_err(|e| CdcError::Mysql(format!("failed to read binlog row: {e}")))?;

            let (op, old, new) = match rows_data {
                RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                    let new =
                        self.binlog_row_to_row(after.as_ref().unwrap(), &col_names, schema, table)?;
                    (ChangeOp::Insert, None, Some(new))
                }
                RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => {
                    let old = self.binlog_row_to_row(
                        before.as_ref().unwrap(),
                        &col_names,
                        schema,
                        table,
                    )?;
                    let new =
                        self.binlog_row_to_row(after.as_ref().unwrap(), &col_names, schema, table)?;
                    (ChangeOp::Update, Some(old), Some(new))
                }
                RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                    let old = self.binlog_row_to_row(
                        before.as_ref().unwrap(),
                        &col_names,
                        schema,
                        table,
                    )?;
                    (ChangeOp::Delete, Some(old), None)
                }
                _ => {
                    // PartialUpdateRowsEvent or other — skip
                    continue;
                }
            };

            results.push(ConvertResult::Event(CdcEvent {
                lsn: Lsn(log_pos),
                timestamp_us,
                xid: 0,
                table: table_id.clone(),
                op,
                new,
                old,
                primary_key_columns: pk_columns.clone(),
            }));
        }

        Ok(results)
    }

    /// Check if a QueryEvent's SQL text is a TRUNCATE statement.
    /// Returns Some((schema, table)) if it is.
    ///
    /// Handles SQL comments (`/* ... */`) that MySQL clients (e.g. RustRover,
    /// DataGrip, MySQL Workbench) inject into statements. These comments are
    /// logged verbatim in the binlog QueryEvent.
    pub fn parse_truncate_query(query: &str, default_schema: &str) -> Option<(String, String)> {
        let stripped = strip_sql_comments(query);
        let trimmed = stripped.trim();
        let upper = trimmed.to_uppercase();

        if !upper.starts_with("TRUNCATE") {
            return None;
        }

        // Strip "TRUNCATE TABLE" or just "TRUNCATE"
        let rest = if upper.starts_with("TRUNCATE TABLE") {
            trimmed[14..].trim()
        } else {
            trimmed[8..].trim()
        };

        // Remove trailing semicolons, backticks, and whitespace
        let cleaned = rest.trim_end_matches(';').trim();

        // Parse schema.table or just table
        if let Some((schema, table)) = cleaned.split_once('.') {
            let schema = schema.trim().trim_matches('`');
            let table = table.trim().trim_matches('`');
            Some((schema.to_string(), table.to_string()))
        } else {
            let table = cleaned.trim_matches('`');
            Some((default_schema.to_string(), table.to_string()))
        }
    }

    /// Convert a TRUNCATE QueryEvent into a CdcEvent.
    pub fn make_truncate_event(
        schema: &str,
        table: &str,
        log_pos: u64,
        timestamp_us: i64,
        primary_key_columns: Vec<String>,
    ) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(log_pos),
            timestamp_us,
            xid: 0,
            table: TableId {
                schema: schema.to_string(),
                name: table.to_string(),
                oid: 0,
            },
            op: ChangeOp::Truncate,
            new: None,
            old: None,
            primary_key_columns,
        }
    }

    /// Check whether the cached column names for a table are stale.
    ///
    /// Returns `true` if the cache has no entry or the column count doesn't
    /// match, meaning the caller should refresh from `information_schema`.
    pub fn cache_needs_refresh(&self, schema: &str, table: &str, num_columns: usize) -> bool {
        let key = (schema.to_string(), table.to_string());
        match self.column_cache.get(&key) {
            Some(names) => names.len() != num_columns,
            None => true,
        }
    }

    /// Update the column name cache for a table.
    pub fn update_column_cache(&mut self, schema: &str, table: &str, columns: Vec<String>) {
        self.column_cache
            .insert((schema.to_string(), table.to_string()), columns);
    }

    /// Update the primary key column cache for a table.
    pub fn update_pk_cache(&mut self, schema: &str, table: &str, pk_columns: Vec<String>) {
        self.pk_cache
            .insert((schema.to_string(), table.to_string()), pk_columns);
    }

    /// Get primary key columns for a table from the cache.
    pub fn get_pk_columns(&self, schema: &str, table: &str) -> Vec<String> {
        self.pk_cache
            .get(&(schema.to_string(), table.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Update the boolean column cache for a table.
    pub fn update_boolean_cache(&mut self, schema: &str, table: &str, booleans: HashSet<String>) {
        self.boolean_columns
            .insert((schema.to_string(), table.to_string()), booleans);
    }

    /// Update the column type cache for a table.
    pub fn update_column_types(
        &mut self,
        schema: &str,
        table: &str,
        types: HashMap<String, String>,
    ) {
        self.column_types
            .insert((schema.to_string(), table.to_string()), types);
    }

    /// Try to extract column names from a TableMapEvent's optional metadata.
    /// Returns `Some(names)` if `binlog_row_metadata=FULL` is enabled on the server.
    pub fn extract_column_names_from_tme(
        tme: &mysql_async::binlog::events::TableMapEvent<'_>,
    ) -> Option<Vec<String>> {
        for field in tme.iter_optional_meta() {
            match field {
                Ok(mysql_async::binlog::events::OptionalMetadataField::ColumnName(col_names)) => {
                    let names: Vec<String> = col_names
                        .iter_names()
                        .filter_map(|r| r.ok())
                        .map(|cn| cn.name().into_owned())
                        .collect();
                    if names.is_empty() {
                        return None;
                    }
                    return Some(names);
                }
                Err(e) => {
                    tracing::debug!("failed to parse optional metadata field: {e}");
                }
                _ => {}
            }
        }
        None
    }

    /// Log a one-time warning when TABLE_MAP metadata does not contain column names.
    pub fn warn_no_metadata_once(&mut self, schema: &str, table: &str) {
        if !self.warned_no_metadata {
            tracing::warn!(
                schema,
                table,
                "TABLE_MAP metadata does not contain column names — \
                 falling back to information_schema query. \
                 Set binlog_row_metadata=FULL on the MySQL server to eliminate \
                 race conditions during schema changes."
            );
            self.warned_no_metadata = true;
        }
    }

    /// Get column names for a table, falling back to positional names.
    fn get_column_names(&self, schema: &str, table: &str, num_columns: usize) -> Vec<String> {
        let key = (schema.to_string(), table.to_string());
        if let Some(names) = self.column_cache.get(&key) {
            if names.len() == num_columns {
                return names.clone();
            }
        }
        // Fallback: positional names
        (0..num_columns).map(|i| format!("col_{i}")).collect()
    }

    /// Convert a BinlogRow into our Row type (BTreeMap<String, ColumnValue>).
    ///
    /// Uses the boolean column cache to convert `Int(0/1)` → `Bool` for
    /// columns that are `TINYINT(1)` in MySQL, and the column type cache
    /// for type-aware parsing of `Value::Bytes`.
    fn binlog_row_to_row(
        &self,
        row: &BinlogRow,
        col_names: &[String],
        schema: &str,
        table: &str,
    ) -> Result<Row> {
        let key = (schema.to_string(), table.to_string());
        let booleans = self.boolean_columns.get(&key);
        let col_types = self.column_types.get(&key);
        let mut result = Row::new();

        for (i, col_name) in col_names.iter().enumerate() {
            let mut value = match row.as_ref(i) {
                None => ColumnValue::Null,
                Some(bv) => binlog_value_to_column_value(bv, col_name, col_types),
            };
            // Convert Int(0/1) → Bool for TINYINT(1) columns
            if let Some(bools) = booleans {
                if bools.contains(col_name) {
                    if let ColumnValue::Int(n) = value {
                        value = ColumnValue::Bool(n != 0);
                    }
                }
            }
            result.insert(col_name.clone(), value);
        }

        Ok(result)
    }
}

/// Convert a BinlogValue to our ColumnValue type.
///
/// Uses optional column type metadata for type-aware parsing of `Value::Bytes`.
fn binlog_value_to_column_value(
    bv: &BinlogValue,
    col_name: &str,
    col_types: Option<&HashMap<String, String>>,
) -> ColumnValue {
    match bv {
        BinlogValue::Value(Value::NULL) => ColumnValue::Null,
        BinlogValue::Value(Value::Int(i)) => ColumnValue::Int(*i),
        BinlogValue::Value(Value::UInt(u)) => {
            if *u <= i64::MAX as u64 {
                ColumnValue::Int(*u as i64)
            } else {
                ColumnValue::Text(u.to_string())
            }
        }
        BinlogValue::Value(Value::Float(f)) => ColumnValue::float(*f as f64),
        BinlogValue::Value(Value::Double(d)) => ColumnValue::float(*d),
        BinlogValue::Value(Value::Bytes(b)) => {
            if let Some(col_type) = col_types.and_then(|ct| ct.get(col_name)) {
                parse_mysql_bytes_with_type(b, col_type)
            } else {
                ColumnValue::Text(String::from_utf8_lossy(b).to_string())
            }
        }
        BinlogValue::Value(Value::Date(y, mo, d, h, mi, s, us)) => {
            let formatted = format_mysql_date(*y, *mo, *d, *h, *mi, *s, *us);
            if *h == 0 && *mi == 0 && *s == 0 && *us == 0 {
                ColumnValue::Date(formatted)
            } else {
                ColumnValue::Timestamp(formatted)
            }
        }
        BinlogValue::Value(Value::Time(neg, days, h, mi, s, us)) => {
            ColumnValue::Time(format_mysql_time(*neg, *days, *h, *mi, *s, *us))
        }
        BinlogValue::Jsonb(j) => ColumnValue::Text(format!("{j:?}")),
        BinlogValue::JsonDiff(_) => {
            // Partial JSON updates — rare, treat as opaque
            ColumnValue::Text("[json_diff]".to_string())
        }
    }
}

/// Format a MySQL Date value as a string.
fn format_mysql_date(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> String {
    if h == 0 && mi == 0 && s == 0 && us == 0 {
        format!("{y:04}-{mo:02}-{d:02}")
    } else if us == 0 {
        format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
    } else {
        format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
    }
}

/// Format a MySQL Time value as a string.
fn format_mysql_time(neg: bool, days: u32, h: u8, mi: u8, s: u8, us: u32) -> String {
    let sign = if neg { "-" } else { "" };
    let total_hours = (days as u64) * 24 + (h as u64);
    if us == 0 {
        format!("{sign}{total_hours}:{mi:02}:{s:02}")
    } else {
        format!("{sign}{total_hours}:{mi:02}:{s:02}.{us:06}")
    }
}

/// Strip `/* ... */` block comments from a SQL string.
///
/// MySQL clients (RustRover, DataGrip, MySQL Workbench, etc.) inject comments
/// like `/* ApplicationName=RustRover 2025.3.4 */` into SQL statements. These
/// are logged verbatim in binlog QueryEvents and must be removed before parsing.
fn strip_sql_comments(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '/' && chars.peek() == Some(&'*') {
            chars.next(); // consume '*'
                          // Skip until closing "*/"
            loop {
                match chars.next() {
                    Some('*') if chars.peek() == Some(&'/') => {
                        chars.next(); // consume '/'
                        break;
                    }
                    Some(_) => continue,
                    None => break, // unterminated comment
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Format a MySQL offset string from filename and position.
pub fn format_mysql_offset(filename: &str, position: u64) -> String {
    format!("{filename}:{position}")
}

/// Parse a MySQL offset string into (filename, position).
pub fn parse_mysql_offset(offset: &str) -> Result<(String, u64)> {
    let (filename, pos_str) = offset
        .rsplit_once(':')
        .ok_or_else(|| CdcError::Parse(format!("invalid MySQL offset format: {offset}")))?;

    let position: u64 = pos_str
        .parse()
        .map_err(|e| CdcError::Parse(format!("invalid MySQL offset position '{pos_str}': {e}")))?;

    Ok((filename.to_string(), position))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_mysql_offset() {
        assert_eq!(
            format_mysql_offset("mysql-bin.000001", 12345),
            "mysql-bin.000001:12345"
        );
        assert_eq!(format_mysql_offset("binlog.000003", 4), "binlog.000003:4");
    }

    #[test]
    fn test_parse_mysql_offset_valid() {
        let (filename, pos) = parse_mysql_offset("mysql-bin.000001:12345").unwrap();
        assert_eq!(filename, "mysql-bin.000001");
        assert_eq!(pos, 12345);
    }

    #[test]
    fn test_parse_mysql_offset_invalid_no_colon() {
        assert!(parse_mysql_offset("mysql-bin.000001").is_err());
    }

    #[test]
    fn test_parse_mysql_offset_invalid_position() {
        assert!(parse_mysql_offset("mysql-bin.000001:notanumber").is_err());
    }

    fn s(a: &str, b: &str) -> Option<(String, String)> {
        Some((a.to_string(), b.to_string()))
    }

    #[test]
    fn test_parse_truncate_query_simple() {
        let result = EventConverter::parse_truncate_query("TRUNCATE TABLE users", "mydb");
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_with_schema() {
        let result = EventConverter::parse_truncate_query("TRUNCATE TABLE mydb.users", "default");
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_without_table_keyword() {
        let result = EventConverter::parse_truncate_query("TRUNCATE users", "mydb");
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_with_backticks() {
        let result =
            EventConverter::parse_truncate_query("TRUNCATE TABLE `mydb`.`users`", "default");
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_with_semicolon() {
        let result = EventConverter::parse_truncate_query("TRUNCATE TABLE users;", "mydb");
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_not_truncate() {
        assert!(EventConverter::parse_truncate_query("SELECT * FROM users", "mydb").is_none());
        assert!(
            EventConverter::parse_truncate_query("INSERT INTO users VALUES (1)", "mydb").is_none()
        );
        assert!(EventConverter::parse_truncate_query("DROP TABLE users", "mydb").is_none());
    }

    #[test]
    fn test_parse_truncate_query_case_insensitive() {
        let result = EventConverter::parse_truncate_query("truncate table users", "mydb");
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_with_leading_comment() {
        let result = EventConverter::parse_truncate_query(
            "/* ApplicationName=RustRover 2025.3.4 */ TRUNCATE TABLE users",
            "mydb",
        );
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_with_multiple_comments() {
        let result = EventConverter::parse_truncate_query(
            "/* app=test */ TRUNCATE TABLE /* inline */ `mydb`.`users` /* trailing */",
            "default",
        );
        assert_eq!(result, s("mydb", "users"));
    }

    #[test]
    fn test_parse_truncate_query_begin_with_comment_not_truncate() {
        assert!(EventConverter::parse_truncate_query(
            "/* ApplicationName=RustRover */ BEGIN",
            "mydb"
        )
        .is_none());
    }

    #[test]
    fn test_get_column_names_from_cache() {
        let mut cache = HashMap::new();
        cache.insert(
            ("mydb".to_string(), "users".to_string()),
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );

        let converter = EventConverter::new(
            "binlog.000001".to_string(),
            cache,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        let names = converter.get_column_names("mydb", "users", 3);
        assert_eq!(names, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_get_column_names_fallback_positional() {
        let converter = EventConverter::new(
            "binlog.000001".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        let names = converter.get_column_names("mydb", "users", 3);
        assert_eq!(names, vec!["col_0", "col_1", "col_2"]);
    }

    #[test]
    fn test_get_column_names_cache_mismatch_falls_back() {
        let mut cache = HashMap::new();
        cache.insert(
            ("mydb".to_string(), "users".to_string()),
            vec!["id".to_string(), "name".to_string()], // 2 columns
        );

        let converter = EventConverter::new(
            "binlog.000001".to_string(),
            cache,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        // Request 3 columns but cache has 2 — should fall back
        let names = converter.get_column_names("mydb", "users", 3);
        assert_eq!(names, vec!["col_0", "col_1", "col_2"]);
    }

    #[test]
    fn test_cache_needs_refresh_no_entry() {
        let converter = EventConverter::new(
            "binlog.000001".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        assert!(converter.cache_needs_refresh("mydb", "users", 3));
    }

    #[test]
    fn test_cache_needs_refresh_count_mismatch() {
        let mut cache = HashMap::new();
        cache.insert(
            ("mydb".to_string(), "users".to_string()),
            vec!["id".to_string(), "name".to_string()],
        );
        let converter = EventConverter::new(
            "binlog.000001".to_string(),
            cache,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        // Cache has 2, event has 3 — needs refresh
        assert!(converter.cache_needs_refresh("mydb", "users", 3));
    }

    #[test]
    fn test_cache_needs_refresh_count_matches() {
        let mut cache = HashMap::new();
        cache.insert(
            ("mydb".to_string(), "users".to_string()),
            vec!["id".to_string(), "name".to_string()],
        );
        let converter = EventConverter::new(
            "binlog.000001".to_string(),
            cache,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        assert!(!converter.cache_needs_refresh("mydb", "users", 2));
    }

    #[test]
    fn test_update_column_cache() {
        let mut converter = EventConverter::new(
            "binlog.000001".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        assert!(converter.cache_needs_refresh("mydb", "users", 3));

        converter.update_column_cache(
            "mydb",
            "users",
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );

        assert!(!converter.cache_needs_refresh("mydb", "users", 3));
        let names = converter.get_column_names("mydb", "users", 3);
        assert_eq!(names, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_update_column_cache_replaces_stale() {
        let mut cache = HashMap::new();
        cache.insert(
            ("mydb".to_string(), "users".to_string()),
            vec!["id".to_string(), "name".to_string()],
        );
        let mut converter = EventConverter::new(
            "binlog.000001".to_string(),
            cache,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        assert!(converter.cache_needs_refresh("mydb", "users", 3));

        // Refresh it
        converter.update_column_cache(
            "mydb",
            "users",
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );

        assert!(!converter.cache_needs_refresh("mydb", "users", 3));
        let names = converter.get_column_names("mydb", "users", 3);
        assert_eq!(names, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_warn_no_metadata_once_sets_flag() {
        let mut converter = EventConverter::new(
            "binlog.000001".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );
        assert!(!converter.warned_no_metadata);

        converter.warn_no_metadata_once("mydb", "users");
        assert!(converter.warned_no_metadata);

        // Second call should be a no-op (flag remains true, no duplicate warning)
        converter.warn_no_metadata_once("mydb", "other_table");
        assert!(converter.warned_no_metadata);
    }

    #[test]
    fn test_update_boolean_cache() {
        let mut converter = EventConverter::new(
            "binlog.000001".to_string(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        );

        let mut bools = HashSet::new();
        bools.insert("is_active".to_string());
        bools.insert("is_paid".to_string());
        converter.update_boolean_cache("mydb", "users", bools);

        let key = ("mydb".to_string(), "users".to_string());
        let cached = converter.boolean_columns.get(&key).unwrap();
        assert!(cached.contains("is_active"));
        assert!(cached.contains("is_paid"));
        assert!(!cached.contains("id"));
    }

    #[test]
    fn test_make_truncate_event_propagates_timestamp() {
        let event = EventConverter::make_truncate_event(
            "mydb",
            "users",
            12345,
            1_700_000_000_000_000,
            vec!["id".into()],
        );
        assert_eq!(event.timestamp_us, 1_700_000_000_000_000);
        assert_eq!(event.lsn, Lsn(12345));
        assert_eq!(event.op, ChangeOp::Truncate);
        assert_eq!(event.table.schema, "mydb");
        assert_eq!(event.table.name, "users");
    }

    #[test]
    fn test_binlog_value_null() {
        let result = binlog_value_to_column_value(&BinlogValue::Value(Value::NULL), "col", None);
        assert_eq!(result, ColumnValue::Null);
    }

    #[test]
    fn test_binlog_value_int() {
        let result = binlog_value_to_column_value(&BinlogValue::Value(Value::Int(42)), "col", None);
        assert_eq!(result, ColumnValue::Int(42));
    }

    #[test]
    fn test_binlog_value_uint() {
        // u64::MAX exceeds i64::MAX, falls back to Text
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::UInt(18446744073709551615)),
            "col",
            None,
        );
        assert_eq!(
            result,
            ColumnValue::Text("18446744073709551615".to_string())
        );
    }

    #[test]
    fn test_binlog_value_uint_fits_i64() {
        let result =
            binlog_value_to_column_value(&BinlogValue::Value(Value::UInt(100)), "col", None);
        assert_eq!(result, ColumnValue::Int(100));
    }

    #[test]
    fn test_binlog_value_float() {
        let result =
            binlog_value_to_column_value(&BinlogValue::Value(Value::Float(1.23)), "col", None);
        // f32 → f64 conversion introduces precision: 1.23f32 as f64 ≠ 1.23f64
        assert_eq!(result, ColumnValue::float(1.23f32 as f64));
    }

    #[test]
    fn test_binlog_value_double() {
        let result =
            binlog_value_to_column_value(&BinlogValue::Value(Value::Double(4.567)), "col", None);
        assert_eq!(result, ColumnValue::float(4.567));
    }

    #[test]
    fn test_binlog_value_bytes_no_type_info() {
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Bytes(b"hello".to_vec())),
            "col",
            None,
        );
        assert_eq!(result, ColumnValue::Text("hello".to_string()));
    }

    #[test]
    fn test_binlog_value_bytes_with_int_type() {
        let mut types = HashMap::new();
        types.insert("id".to_string(), "int(11)".to_string());
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Bytes(b"42".to_vec())),
            "id",
            Some(&types),
        );
        assert_eq!(result, ColumnValue::Int(42));
    }

    #[test]
    fn test_binlog_value_bytes_with_double_type() {
        let mut types = HashMap::new();
        types.insert("price".to_string(), "double".to_string());
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Bytes(b"9.99".to_vec())),
            "price",
            Some(&types),
        );
        assert_eq!(result, ColumnValue::float(9.99));
    }

    #[test]
    fn test_binlog_value_date_only() {
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Date(2024, 1, 15, 0, 0, 0, 0)),
            "col",
            None,
        );
        assert_eq!(result, ColumnValue::Date("2024-01-15".to_string()));
    }

    #[test]
    fn test_binlog_value_datetime() {
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Date(2024, 1, 15, 13, 45, 30, 0)),
            "col",
            None,
        );
        assert_eq!(
            result,
            ColumnValue::Timestamp("2024-01-15 13:45:30".to_string())
        );
    }

    #[test]
    fn test_binlog_value_datetime_with_microseconds() {
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Date(2024, 1, 15, 13, 45, 30, 123456)),
            "col",
            None,
        );
        assert_eq!(
            result,
            ColumnValue::Timestamp("2024-01-15 13:45:30.123456".to_string())
        );
    }

    #[test]
    fn test_binlog_value_time() {
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Time(false, 0, 13, 45, 30, 0)),
            "col",
            None,
        );
        assert_eq!(result, ColumnValue::Time("13:45:30".to_string()));
    }

    #[test]
    fn test_binlog_value_time_negative() {
        let result = binlog_value_to_column_value(
            &BinlogValue::Value(Value::Time(true, 1, 2, 30, 0, 0)),
            "col",
            None,
        );
        assert_eq!(result, ColumnValue::Time("-26:30:00".to_string()));
    }
}
