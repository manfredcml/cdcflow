use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use chrono::NaiveDateTime;

use crate::error::{CdcError, Result};
use crate::event::{CdcEvent, ChangeOp, ColumnValue, Row};

/// Convert CDC events into an Arrow RecordBatch for the fixed 7-column CDC schema.
///
/// Columns:
///   op           → StringArray  (from `as_op_code()`)
///   lsn          → Int64Array
///   timestamp_us → Int64Array
///   snapshot     → BooleanArray
///   row_new      → StringArray  (JSON-serialized `{"values":{...}, "types":{...}}` or null)
///   row_old      → StringArray  (JSON-serialized `{"values":{...}, "types":{...}}` or null)
///   primary_key_columns → StringArray (JSON-serialized array of PK column names, or null)
pub fn events_to_record_batch(
    events: &[CdcEvent],
    arrow_schema: &ArrowSchema,
) -> Result<RecordBatch> {
    let num_rows = events.len();

    let mut ops: Vec<String> = Vec::with_capacity(num_rows);
    let mut lsns: Vec<i64> = Vec::with_capacity(num_rows);
    let mut timestamps: Vec<i64> = Vec::with_capacity(num_rows);
    let mut snapshots: Vec<bool> = Vec::with_capacity(num_rows);
    let mut row_news: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut row_olds: Vec<Option<String>> = Vec::with_capacity(num_rows);
    let mut pk_cols: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for event in events {
        ops.push(event.op.as_op_code().into());
        lsns.push(event.lsn.0 as i64);
        timestamps.push(event.timestamp_us);
        snapshots.push(event.op == ChangeOp::Snapshot);

        row_news.push(
            event
                .new
                .as_ref()
                .map(|r| crate::schema::value::row_to_typed_json(r).to_string()),
        );
        row_olds.push(
            event
                .old
                .as_ref()
                .map(|r| crate::schema::value::row_to_typed_json(r).to_string()),
        );
        pk_cols.push(if event.primary_key_columns.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&event.primary_key_columns).unwrap_or_default())
        });
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(ops)),
        Arc::new(Int64Array::from(lsns)),
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(BooleanArray::from(snapshots)),
        Arc::new(StringArray::from(row_news)),
        Arc::new(StringArray::from(row_olds)),
        Arc::new(StringArray::from(pk_cols)),
    ];

    RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns)
        .map_err(|e| CdcError::Iceberg(format!("record batch: {e}")))
}

/// A source value that is present but cannot be represented in the target
/// column — a schemaless source changed the field's type after the table was
/// created. Writing NULL would drop the value without a trace.
fn type_mismatch(col_name: &str, data_type: &DataType, value: &ColumnValue) -> CdcError {
    CdcError::Iceberg(format!(
        "column {col_name} is {data_type} but the source sent {} ({}); \
         the source field type changed and the value cannot be converted",
        crate::schema::value::column_value_to_string(Some(value)).unwrap_or_default(),
        crate::schema::value::column_value_type_name(value),
    ))
}

fn convert_values<T>(
    col_name: &str,
    data_type: &DataType,
    values: &[Option<&ColumnValue>],
    convert: impl Fn(&ColumnValue) -> Option<T>,
) -> Result<Vec<Option<T>>> {
    values
        .iter()
        .map(|v| match v {
            None | Some(ColumnValue::Null) | Some(ColumnValue::UnchangedToast) => Ok(None),
            Some(value) => convert(value)
                .map(Some)
                .ok_or_else(|| type_mismatch(col_name, data_type, value)),
        })
        .collect()
}

/// Build an Arrow array for a source column from the extracted rows.
fn build_source_column(
    col_name: &str,
    data_type: &DataType,
    rows: &[Option<&Row>],
) -> Result<ArrayRef> {
    // For each row, find the column value by name
    let values: Vec<Option<&ColumnValue>> = rows
        .iter()
        .map(|row| row.and_then(|r| r.get(col_name)))
        .collect();

    match data_type {
        DataType::Int32 => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Int(n) => i32::try_from(*n).ok(),
                ColumnValue::Text(s) => s.parse::<i32>().ok(),
                _ => None,
            })?;
            Ok(Arc::new(Int32Array::from(vals)))
        }
        DataType::Int64 => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Int(n) => Some(*n),
                ColumnValue::Text(s) => s.parse::<i64>().ok(),
                _ => None,
            })?;
            Ok(Arc::new(Int64Array::from(vals)))
        }
        DataType::Float32 => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Float(f) => Some(f.into_inner() as f32),
                ColumnValue::Int(n) => Some(*n as f32),
                ColumnValue::Text(s) => s.parse::<f32>().ok(),
                _ => None,
            })?;
            Ok(Arc::new(Float32Array::from(vals)))
        }
        DataType::Float64 => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Float(f) => Some(f.into_inner()),
                ColumnValue::Int(n) => Some(*n as f64),
                ColumnValue::Text(s) => s.parse::<f64>().ok(),
                _ => None,
            })?;
            Ok(Arc::new(Float64Array::from(vals)))
        }
        DataType::Boolean => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Bool(b) => Some(*b),
                ColumnValue::Int(n) => Some(*n != 0),
                ColumnValue::Text(s) => match s.as_str() {
                    "t" | "true" | "TRUE" | "1" => Some(true),
                    "f" | "false" | "FALSE" | "0" => Some(false),
                    _ => None,
                },
                _ => None,
            })?;
            Ok(Arc::new(BooleanArray::from(vals)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Timestamp(s) | ColumnValue::Text(s) => parse_timestamp_us(s),
                _ => None,
            })?;
            let arr = TimestampMicrosecondArray::from(vals);
            let arr = match tz {
                Some(tz) => arr.with_timezone(tz.as_ref()),
                None => arr,
            };
            Ok(Arc::new(arr))
        }
        DataType::Date32 => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Date(s) | ColumnValue::Text(s) => {
                    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .ok()
                        .map(|d| {
                            (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days()
                                as i32
                        })
                }
                _ => None,
            })?;
            Ok(Arc::new(Date32Array::from(vals)))
        }
        DataType::Decimal128(precision, scale) => {
            let scale_i32 = *scale as i32;
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Text(s) => parse_decimal_i128(s, scale_i32),
                ColumnValue::Float(f) => {
                    let multiplier = 10_f64.powi(scale_i32);
                    Some((f.into_inner() * multiplier).round() as i128)
                }
                ColumnValue::Int(n) => {
                    let multiplier = 10_i128.pow(scale_i32 as u32);
                    Some(*n as i128 * multiplier)
                }
                _ => None,
            })?;
            let arr = Decimal128Array::from(vals)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| CdcError::Iceberg(format!("decimal array: {e}")))?;
            Ok(Arc::new(arr))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Time(s) | ColumnValue::Text(s) => parse_time_us(s),
                _ => None,
            })?;
            Ok(Arc::new(Time64MicrosecondArray::from(vals)))
        }
        DataType::LargeBinary => {
            let vals = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Bytes(b) => Some(b.clone()),
                _ => None,
            })?;
            let arr: LargeBinaryArray = vals
                .iter()
                .map(|v| v.as_ref().map(|b| b.as_slice()))
                .collect();
            Ok(Arc::new(arr))
        }
        DataType::FixedSizeBinary(16) => {
            let byte_values = convert_values(col_name, data_type, &values, |v| match v {
                ColumnValue::Text(s) => parse_uuid_bytes(s),
                ColumnValue::Bytes(b) if b.len() == 16 => {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(b);
                    Some(arr)
                }
                _ => None,
            })?;
            let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                byte_values.iter().map(|v| v.as_ref().map(|b| b.as_slice())),
                16,
            )
            .map_err(|e| CdcError::Iceberg(format!("uuid array: {e}")))?;
            Ok(Arc::new(arr))
        }
        // For Utf8 and anything else: store as string.
        _ => {
            let arr: StringArray = values
                .iter()
                .map(|v| match v {
                    Some(ColumnValue::Text(s)) => Some(s.clone()),
                    Some(ColumnValue::Timestamp(s)) => Some(s.clone()),
                    Some(ColumnValue::Date(s)) => Some(s.clone()),
                    Some(ColumnValue::Time(s)) => Some(s.clone()),
                    Some(ColumnValue::Int(n)) => Some(n.to_string()),
                    Some(ColumnValue::Float(f)) => Some(f.to_string()),
                    Some(ColumnValue::Bool(b)) => Some(b.to_string()),
                    Some(ColumnValue::Null) | Some(ColumnValue::UnchangedToast) | None => None,
                    Some(ColumnValue::Bytes(b)) => {
                        Some(b.iter().map(|byte| format!("{byte:02x}")).collect())
                    }
                })
                .collect();
            Ok(Arc::new(arr))
        }
    }
}

/// Convert CDC events into an Arrow RecordBatch for the flattened CDC schema.
///
/// Columns:
///   7 `_cdc_*` metadata columns  +
///   N source columns (from event.new) +
///   N `_old_*` columns (from event.old)
///
/// The `arrow_schema` must match the flattened layout: metadata fields first,
/// then source columns, then `_old_*` columns.
pub fn events_to_flattened_cdc_batch(
    events: &[CdcEvent],
    arrow_schema: &ArrowSchema,
) -> Result<RecordBatch> {
    // Build metadata arrays
    let ops: Vec<String> = events.iter().map(|e| e.op.as_op_code().into()).collect();
    let lsns: Vec<i64> = events.iter().map(|e| e.lsn.0 as i64).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_us).collect();
    let snapshots: Vec<bool> = events.iter().map(|e| e.op == ChangeOp::Snapshot).collect();
    let schemas: Vec<String> = events.iter().map(|e| e.table.schema.clone()).collect();
    let tables: Vec<String> = events.iter().map(|e| e.table.name.clone()).collect();
    let pk_cols: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(&e.primary_key_columns).unwrap_or_else(|_| "[]".into()))
        .collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

    // 7 metadata columns
    columns.push(Arc::new(StringArray::from(ops)));
    columns.push(Arc::new(Int64Array::from(lsns)));
    columns.push(Arc::new(Int64Array::from(timestamps)));
    columns.push(Arc::new(BooleanArray::from(snapshots)));
    columns.push(Arc::new(StringArray::from(schemas)));
    columns.push(Arc::new(StringArray::from(tables)));
    columns.push(Arc::new(StringArray::from(pk_cols)));

    let all_fields = arrow_schema.fields();
    let num_metadata = 7;
    let num_source = (all_fields.len() - num_metadata) / 2;

    // Source columns (from event.new)
    let new_rows: Vec<Option<&Row>> = events.iter().map(|e| e.new.as_ref()).collect();
    for i in 0..num_source {
        let field = &all_fields[num_metadata + i];
        columns.push(build_source_column(
            field.name(),
            field.data_type(),
            &new_rows,
        )?);
    }

    // _old_ columns (from event.old)
    let old_rows: Vec<Option<&Row>> = events.iter().map(|e| e.old.as_ref()).collect();
    for i in 0..num_source {
        let field = &all_fields[num_metadata + num_source + i];
        // Strip _old_ prefix to look up the column name in the old row
        let source_col_name = field.name().strip_prefix("_old_").unwrap_or(field.name());
        columns.push(build_source_column(
            source_col_name,
            field.data_type(),
            &old_rows,
        )?);
    }

    RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns)
        .map_err(|e| CdcError::Iceberg(format!("flattened CDC record batch: {e}")))
}

/// Identity of a row within a batch, or None when the event carries no usable PK.
fn row_key(event: &CdcEvent) -> Option<Vec<Option<String>>> {
    let row = event.new.as_ref()?;
    if event.primary_key_columns.is_empty() {
        return None;
    }
    Some(
        event
            .primary_key_columns
            .iter()
            .map(|c| crate::schema::value::column_value_to_string(row.get(c)))
            .collect(),
    )
}

/// Convert CDC events into a data RecordBatch for replication mode (source columns only).
///
/// Filters to Insert/Update/Snapshot events and uses `event.new` for row data.
/// Returns None if no matching events exist.
///
/// Only the last event per primary key survives: equality deletes never apply to
/// data files added by the same commit, so two versions of a key in one batch
/// would both remain in the table.
pub fn events_to_replication_data_batch(
    events: &[CdcEvent],
    arrow_schema: &ArrowSchema,
) -> Result<Option<RecordBatch>> {
    let mut data_events: Vec<&CdcEvent> = Vec::new();
    let mut positions: HashMap<Vec<Option<String>>, usize> = HashMap::new();

    for event in events.iter().filter(|e| {
        matches!(
            e.op,
            ChangeOp::Insert | ChangeOp::Update | ChangeOp::Snapshot
        )
    }) {
        match row_key(event) {
            Some(key) => match positions.get(&key) {
                Some(&pos) => data_events[pos] = event,
                None => {
                    positions.insert(key, data_events.len());
                    data_events.push(event);
                }
            },
            None => data_events.push(event),
        }
    }

    if data_events.is_empty() {
        return Ok(None);
    }

    let rows: Vec<Option<&Row>> = data_events.iter().map(|event| event.new.as_ref()).collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());
    for field in arrow_schema.fields() {
        columns.push(build_source_column(field.name(), field.data_type(), &rows)?);
    }

    let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns)
        .map_err(|e| CdcError::Iceberg(format!("replication data batch: {e}")))?;
    Ok(Some(batch))
}

/// Convert CDC events into an equality-delete RecordBatch (PK columns only from event.old).
///
/// Filters to Delete and Update events and extracts PK columns from `event.old`.
/// Returns None if no matching events exist.
pub fn events_to_delete_batch(
    events: &[CdcEvent],
    pk_arrow_schema: &ArrowSchema,
) -> Result<Option<RecordBatch>> {
    let delete_events: Vec<&CdcEvent> = events
        .iter()
        .filter(|e| matches!(e.op, ChangeOp::Delete | ChangeOp::Update))
        .collect();

    if delete_events.is_empty() {
        return Ok(None);
    }

    let rows: Vec<Option<&Row>> = delete_events
        .iter()
        .map(|event| event.old.as_ref())
        .collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(pk_arrow_schema.fields().len());
    for field in pk_arrow_schema.fields() {
        columns.push(build_source_column(field.name(), field.data_type(), &rows)?);
    }

    let batch = RecordBatch::try_new(Arc::new(pk_arrow_schema.clone()), columns)
        .map_err(|e| CdcError::Iceberg(format!("delete batch: {e}")))?;
    Ok(Some(batch))
}

/// Parse a PostgreSQL timestamp string into microseconds since Unix epoch.
///
/// Handles formats:
/// - `YYYY-MM-DD HH:MM:SS` (no timezone)
/// - `YYYY-MM-DD HH:MM:SS.ffffff` (with fractional seconds)
/// - `YYYY-MM-DD HH:MM:SS+HH` (with timezone offset)
fn parse_timestamp_us(s: &str) -> Option<i64> {
    // Try without timezone first (PostgreSQL `timestamp without time zone`)
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp_micros());
    }
    // Try with timezone (PostgreSQL `timestamp with time zone`)
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Some(dt.timestamp_micros());
    }
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z") {
        return Some(dt.timestamp_micros());
    }
    // ISO 8601 fallback
    if let Ok(dt) = s.parse::<chrono::DateTime<chrono::Utc>>() {
        return Some(dt.timestamp_micros());
    }
    None
}

/// Parse a decimal string (e.g. "123.45") into an i128 scaled value.
///
/// With `scale = 18`, "123.45" becomes `123_450_000_000_000_000_000`.
fn parse_decimal_i128(s: &str, scale: i32) -> Option<i128> {
    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s)
    };

    let parts: Vec<&str> = s.split('.').collect();
    let result = match parts.len() {
        1 => {
            // No decimal point: "123" with scale N → 123 * 10^N
            let integer: i128 = parts[0].parse().ok()?;
            let multiplier = 10_i128.checked_pow(scale as u32)?;
            integer.checked_mul(multiplier)
        }
        2 => {
            let frac_str = parts[1];
            let frac_len = frac_str.len() as i32;
            let combined_str = format!("{}{}", parts[0], frac_str);
            let combined: i128 = combined_str.parse().ok()?;

            if scale >= frac_len {
                let extra = 10_i128.checked_pow((scale - frac_len) as u32)?;
                combined.checked_mul(extra)
            } else {
                // More fractional digits than scale allows — truncate
                let divisor = 10_i128.checked_pow((frac_len - scale) as u32)?;
                Some(combined / divisor)
            }
        }
        _ => None,
    };

    result.map(|v| if negative { -v } else { v })
}

/// Parse a time string (e.g. "10:30:00" or "10:30:00.123456") into microseconds since midnight.
fn parse_time_us(s: &str) -> Option<i64> {
    let t = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .ok()
        .or_else(|| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S").ok())?;
    let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0)?;
    let duration = t - midnight;
    duration.num_microseconds()
}

/// Parse a UUID string (e.g. "550e8400-e29b-41d4-a716-446655440000") into 16 bytes.
fn parse_uuid_bytes(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Lsn, TableId};
    use arrow_array::Array;
    use arrow_schema::Field;
    use std::collections::BTreeMap;

    /// Fixed 7-column CDC Arrow schema.
    fn test_cdc_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("op", DataType::Utf8, false),
            Field::new("lsn", DataType::Int64, false),
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("snapshot", DataType::Boolean, false),
            Field::new("row_new", DataType::Utf8, true),
            Field::new("row_old", DataType::Utf8, true),
            Field::new("primary_key_columns", DataType::Utf8, true),
        ])
    }

    fn test_table_id() -> TableId {
        TableId {
            schema: "public".into(),
            name: "users".into(),
            oid: 1,
        }
    }

    fn make_insert_event(id: i64, name: &str) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(id)),
                ("name".into(), ColumnValue::Text(name.into())),
            ])),
            old: None,
            primary_key_columns: vec!["id".into()],
        }
    }

    #[test]
    fn test_single_insert_event() {
        let schema = test_cdc_arrow_schema();
        let events = vec![make_insert_event(1, "Alice")];
        let batch = events_to_record_batch(&events, &schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 7);

        // Check metadata columns
        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "I");

        let lsns = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lsns.value(0), 100);

        let timestamps = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1_000_000);

        let snapshots = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!snapshots.value(0));

        // Check row_new is JSON with values and types
        let row_new = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!row_new.is_null(0));
        let parsed: serde_json::Value = serde_json::from_str(row_new.value(0)).unwrap();
        assert_eq!(parsed["values"]["id"], 1);
        assert_eq!(parsed["values"]["name"], "Alice");
        assert_eq!(parsed["types"]["id"], "Int");
        assert_eq!(parsed["types"]["name"], "Text");

        // row_old should be null for inserts
        let row_old = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(row_old.is_null(0));
    }

    #[test]
    fn test_mixed_events() {
        let schema = test_cdc_arrow_schema();
        let events = vec![
            make_insert_event(1, "Alice"),
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: test_table_id(),
                op: ChangeOp::Update,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(1)),
                    ("name".into(), ColumnValue::Text("Bob".into())),
                ])),
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(1)),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(300),
                timestamp_us: 3_000_000,
                xid: 3,
                table: test_table_id(),
                op: ChangeOp::Delete,
                new: None,
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(1)),
                    ("name".into(), ColumnValue::Text("Bob".into())),
                ])),
                primary_key_columns: vec![],
            },
        ];

        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "I");
        assert_eq!(ops.value(1), "U");
        assert_eq!(ops.value(2), "D");

        // Update has both new and old
        let row_new = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row_old = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert!(!row_new.is_null(1));
        assert!(!row_old.is_null(1));
        let new_parsed: serde_json::Value = serde_json::from_str(row_new.value(1)).unwrap();
        assert_eq!(new_parsed["values"]["name"], "Bob");

        // Delete has no new, has old
        assert!(row_new.is_null(2));
        assert!(!row_old.is_null(2));
    }

    #[test]
    fn test_truncate_event() {
        let schema = test_cdc_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Truncate,
            new: None,
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "T");

        // Both row_new and row_old should be null for truncate
        let row_new = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row_old = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(row_new.is_null(0));
        assert!(row_old.is_null(0));
    }

    #[test]
    fn test_empty_events() {
        let schema = test_cdc_arrow_schema();
        let events: Vec<CdcEvent> = vec![];
        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 7);
    }

    #[test]
    fn test_snapshot_flag() {
        let schema = test_cdc_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 0,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Snapshot,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_record_batch(&events, &schema).unwrap();

        let snapshots = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(snapshots.value(0));

        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "S");
    }

    #[test]
    fn test_parse_timestamp_us_formats() {
        // No timezone
        assert!(parse_timestamp_us("2024-01-15 10:30:00").is_some());
        // With fractional seconds
        assert!(parse_timestamp_us("2024-01-15 10:30:00.123456").is_some());
        // With timezone offset
        assert!(parse_timestamp_us("2024-01-15 10:30:00+00").is_some());
        assert!(parse_timestamp_us("2024-01-15 10:30:00+05:30").is_some());
        // ISO 8601
        assert!(parse_timestamp_us("2024-01-15T10:30:00Z").is_some());
        // Invalid
        assert!(parse_timestamp_us("not-a-timestamp").is_none());
    }

    // ── Flattened CDC batch tests ────────────────────────────────────

    fn test_flattened_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("_cdc_op", DataType::Utf8, false),
            Field::new("_cdc_lsn", DataType::Int64, false),
            Field::new("_cdc_timestamp_us", DataType::Int64, false),
            Field::new("_cdc_snapshot", DataType::Boolean, false),
            Field::new("_cdc_schema", DataType::Utf8, false),
            Field::new("_cdc_table", DataType::Utf8, false),
            Field::new("_cdc_primary_key_columns", DataType::Utf8, false),
            // 2 source columns
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            // 2 old columns
            Field::new("_old_id", DataType::Int64, true),
            Field::new("_old_name", DataType::Utf8, true),
        ])
    }

    #[test]
    fn test_flattened_cdc_batch_insert() {
        let schema = test_flattened_arrow_schema();
        let events = vec![make_insert_event(1, "Alice")];
        let batch = events_to_flattened_cdc_batch(&events, &schema).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11); // 7 meta + 2 source + 2 old

        // Metadata
        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "I");
        let lsns = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lsns.value(0), 100);
        let snapshots = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!snapshots.value(0));
        let schemas = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(schemas.value(0), "public");
        let tables_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tables_col.value(0), "users");

        // Source columns
        let ids = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        let names = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");

        // Old columns should be null for inserts
        assert!(batch.column(9).is_null(0));
        assert!(batch.column(10).is_null(0));
    }

    #[test]
    fn test_flattened_cdc_batch_update() {
        let schema = test_flattened_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(200),
            timestamp_us: 2_000_000,
            xid: 2,
            table: test_table_id(),
            op: ChangeOp::Update,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Bob".into())),
            ])),
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            primary_key_columns: vec!["id".into()],
        }];

        let batch = events_to_flattened_cdc_batch(&events, &schema).unwrap();

        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "U");

        // New values
        let ids = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        let names = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Bob");

        // Old values
        let old_ids = batch
            .column(9)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(old_ids.value(0), 1);
        let old_names = batch
            .column(10)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(old_names.value(0), "Alice");
    }

    #[test]
    fn test_flattened_cdc_batch_delete() {
        let schema = test_flattened_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(300),
            timestamp_us: 3_000_000,
            xid: 3,
            table: test_table_id(),
            op: ChangeOp::Delete,
            new: None,
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            primary_key_columns: vec![],
        }];

        let batch = events_to_flattened_cdc_batch(&events, &schema).unwrap();

        let ops = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "D");

        // New values should be null for deletes
        assert!(batch.column(7).is_null(0));
        assert!(batch.column(8).is_null(0));

        // Old values present
        let old_ids = batch
            .column(9)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(old_ids.value(0), 1);
        let old_names = batch
            .column(10)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(old_names.value(0), "Alice");
    }

    #[test]
    fn test_flattened_cdc_batch_empty() {
        let schema = test_flattened_arrow_schema();
        let events: Vec<CdcEvent> = vec![];
        let batch = events_to_flattened_cdc_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn test_flattened_cdc_batch_pk_columns_serialized() {
        let schema = test_flattened_arrow_schema();
        let events = vec![make_insert_event(1, "Alice")]; // has pk_columns: ["id"]
        let batch = events_to_flattened_cdc_batch(&events, &schema).unwrap();

        let pk = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(pk.value(0), "[\"id\"]");
    }

    #[test]
    fn test_repl_batch_rejects_value_of_wrong_type() {
        let schema = ArrowSchema::new(vec![Field::new("age", DataType::Int64, true)]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([(
                "age".into(),
                ColumnValue::Text("thirty".into()),
            )])),
            old: None,
            primary_key_columns: vec![],
        }];

        let err = events_to_replication_data_batch(&events, &schema).unwrap_err();
        assert!(
            err.to_string().contains("thirty"),
            "error should name the offending value: {err}"
        );
    }

    #[test]
    fn test_repl_batch_accepts_null_for_typed_column() {
        let schema = ArrowSchema::new(vec![Field::new("age", DataType::Int64, true)]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([("age".into(), ColumnValue::Null)])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn test_repl_batch_keeps_only_last_event_per_pk() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let event = |op, id: i64, name: &str| CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(id)),
                ("name".into(), ColumnValue::Text(name.into())),
            ])),
            old: None,
            primary_key_columns: vec!["id".into()],
        };

        let events = vec![
            event(ChangeOp::Insert, 1, "first"),
            event(ChangeOp::Update, 1, "second"),
            event(ChangeOp::Update, 2, "other"),
            event(ChangeOp::Update, 1, "third"),
        ];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "third");
        assert_eq!(names.value(1), "other");
    }

    #[test]
    fn test_repl_batch_keeps_all_events_without_pk_columns() {
        let schema = ArrowSchema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let event = |name: &str| CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([(
                "name".into(),
                ColumnValue::Text(name.into()),
            )])),
            old: None,
            primary_key_columns: vec![],
        };

        let batch = events_to_replication_data_batch(&[event("a"), event("b")], &schema)
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
    }

    // ── Type conversion tests (via replication data batch) ──────────

    #[test]
    fn test_repl_decimal_column() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("price", DataType::Decimal128(38, 18), true),
        ]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("price".into(), ColumnValue::Text("123.45".into())),
            ])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        let decimals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimals.value(0), 123_450_000_000_000_000_000_i128);
    }

    #[test]
    fn test_repl_decimal_column_from_float() {
        let schema = ArrowSchema::new(vec![Field::new("price", DataType::Decimal128(38, 2), true)]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([(
                "price".into(),
                ColumnValue::float(19.99),
            )])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        let decimals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimals.value(0), 1999_i128);
    }

    #[test]
    fn test_repl_decimal_column_from_int() {
        let schema = ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(38, 2),
            true,
        )]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([("amount".into(), ColumnValue::Int(42))])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        let decimals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimals.value(0), 4200_i128);
    }

    #[test]
    fn test_parse_decimal_i128_formats() {
        // Integer
        assert_eq!(parse_decimal_i128("123", 2), Some(12300));
        // With decimal point
        assert_eq!(parse_decimal_i128("123.45", 2), Some(12345));
        // Negative
        assert_eq!(parse_decimal_i128("-99.5", 2), Some(-9950));
        // More fractional digits than scale — truncates
        assert_eq!(parse_decimal_i128("1.999", 2), Some(199));
        // Zero
        assert_eq!(parse_decimal_i128("0", 2), Some(0));
        assert_eq!(parse_decimal_i128("0.00", 2), Some(0));
        // Invalid
        assert_eq!(parse_decimal_i128("abc", 2), None);
    }

    #[test]
    fn test_repl_time_column() {
        let schema = ArrowSchema::new(vec![Field::new(
            "event_time",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        )]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([(
                "event_time".into(),
                ColumnValue::Time("10:30:00".into()),
            )])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        let times = batch
            .column(0)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(times.value(0), 37_800_000_000);
    }

    #[test]
    fn test_parse_time_us_formats() {
        // Basic
        assert_eq!(parse_time_us("10:30:00"), Some(37_800_000_000));
        // With fractional seconds
        assert_eq!(parse_time_us("10:30:00.500000"), Some(37_800_500_000));
        // Midnight
        assert_eq!(parse_time_us("00:00:00"), Some(0));
        // Invalid
        assert_eq!(parse_time_us("not-a-time"), None);
    }

    #[test]
    fn test_repl_uuid_column() {
        let schema = ArrowSchema::new(vec![Field::new(
            "uuid",
            DataType::FixedSizeBinary(16),
            true,
        )]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([(
                "uuid".into(),
                ColumnValue::Text("550e8400-e29b-41d4-a716-446655440000".into()),
            )])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        let uuids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            uuids.value(0),
            &[
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00
            ]
        );
    }

    #[test]
    fn test_parse_uuid_bytes_formats() {
        let expected = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        // Standard UUID with dashes
        assert_eq!(
            parse_uuid_bytes("550e8400-e29b-41d4-a716-446655440000"),
            Some(expected)
        );
        // Without dashes
        assert_eq!(
            parse_uuid_bytes("550e8400e29b41d4a716446655440000"),
            Some(expected)
        );
        // Invalid
        assert_eq!(parse_uuid_bytes("not-a-uuid"), None);
        assert_eq!(parse_uuid_bytes("550e8400"), None); // too short
    }

    #[test]
    fn test_repl_binary_column() {
        let schema = ArrowSchema::new(vec![Field::new("data", DataType::LargeBinary, true)]);

        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([(
                "data".into(),
                ColumnValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            )])),
            old: None,
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        let bins = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(bins.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    // ── Replication mode batch tests ───────────────────────────────

    fn replication_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("name", DataType::Utf8, true),
        ])
    }

    fn pk_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![arrow_schema::Field::new("id", DataType::Int64, false)])
    }

    #[test]
    fn test_replication_data_batch_filters_deletes() {
        let schema = replication_arrow_schema();
        let events = vec![
            CdcEvent {
                lsn: Lsn(100),
                timestamp_us: 1_000_000,
                xid: 1,
                table: test_table_id(),
                op: ChangeOp::Insert,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(1)),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: test_table_id(),
                op: ChangeOp::Delete,
                new: None,
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(1)),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(300),
                timestamp_us: 3_000_000,
                xid: 3,
                table: test_table_id(),
                op: ChangeOp::Snapshot,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(2)),
                    ("name".into(), ColumnValue::Text("Bob".into())),
                ])),
                old: None,
                primary_key_columns: vec![],
            },
        ];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        // Only Insert + Snapshot → 2 rows
        assert_eq!(batch.num_rows(), 2);
        // No CDC metadata columns
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "name");
    }

    #[test]
    fn test_replication_data_batch_includes_updates() {
        let schema = replication_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Update,
            new: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Updated".into())),
            ])),
            old: Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Old".into())),
            ])),
            primary_key_columns: vec![],
        }];

        let batch = events_to_replication_data_batch(&events, &schema)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Updated"); // Uses event.new
    }

    #[test]
    fn test_replication_data_batch_empty_on_deletes_only() {
        let schema = replication_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Delete,
            new: None,
            old: Some(BTreeMap::from([("id".into(), ColumnValue::Int(1))])),
            primary_key_columns: vec![],
        }];

        let result = events_to_replication_data_batch(&events, &schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_batch_pk_columns_only() {
        let pk_schema = pk_arrow_schema();
        let events = vec![
            CdcEvent {
                lsn: Lsn(100),
                timestamp_us: 1_000_000,
                xid: 1,
                table: test_table_id(),
                op: ChangeOp::Delete,
                new: None,
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(42)),
                    ("name".into(), ColumnValue::Text("Alice".into())),
                ])),
                primary_key_columns: vec![],
            },
            CdcEvent {
                lsn: Lsn(200),
                timestamp_us: 2_000_000,
                xid: 2,
                table: test_table_id(),
                op: ChangeOp::Update,
                new: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(42)),
                    ("name".into(), ColumnValue::Text("Updated".into())),
                ])),
                old: Some(BTreeMap::from([
                    ("id".into(), ColumnValue::Int(42)),
                    ("name".into(), ColumnValue::Text("Old".into())),
                ])),
                primary_key_columns: vec![],
            },
        ];

        let batch = events_to_delete_batch(&events, &pk_schema)
            .unwrap()
            .unwrap();
        // Both Delete and Update produce delete rows
        assert_eq!(batch.num_rows(), 2);
        // Only PK column
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "id");
    }

    #[test]
    fn test_delete_batch_empty_on_inserts_only() {
        let pk_schema = pk_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Insert,
            new: Some(BTreeMap::from([("id".into(), ColumnValue::Int(1))])),
            old: None,
            primary_key_columns: vec![],
        }];

        let result = events_to_delete_batch(&events, &pk_schema).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_batch_truncate_ignored() {
        let pk_schema = pk_arrow_schema();
        let events = vec![CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: test_table_id(),
            op: ChangeOp::Truncate,
            new: None,
            old: None,
            primary_key_columns: vec![],
        }];

        let result = events_to_delete_batch(&events, &pk_schema).unwrap();
        assert!(result.is_none());
    }
}
