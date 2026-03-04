use std::collections::HashSet;

use crate::event::{CdcEvent, ChangeOp};

/// Detect column names present in events but missing from the known column list.
///
/// Returns a deduplicated list of new column names preserving first-seen order.
/// Inspects `new` for Insert/Update/Snapshot, `old` for Delete, and skips Truncate.
pub fn detect_new_columns(known_columns: &[String], events: &[&CdcEvent]) -> Vec<String> {
    let known: HashSet<&str> = known_columns.iter().map(|s| s.as_str()).collect();
    let mut new_cols: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for event in events {
        let row = match event.op {
            ChangeOp::Insert | ChangeOp::Update | ChangeOp::Snapshot => event.new.as_ref(),
            ChangeOp::Delete => event.old.as_ref(),
            ChangeOp::Truncate => continue,
        };

        if let Some(row) = row {
            for col_name in row.keys() {
                if !known.contains(col_name.as_str()) && seen.insert(col_name.clone()) {
                    new_cols.push(col_name.clone());
                }
            }
        }
    }

    new_cols
}

/// Detect target columns that no longer exist in the source.
///
/// Returns column names present in `target_columns` but absent from `source_columns`.
/// Used after a source schema query confirms that columns were actually dropped
/// (as opposed to simply missing from a single batch of events).
pub fn detect_dropped_columns(target_columns: &[String], source_columns: &[String]) -> Vec<String> {
    let source: HashSet<&str> = source_columns.iter().map(|s| s.as_str()).collect();
    target_columns
        .iter()
        .filter(|col| !source.contains(col.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ColumnValue, Lsn, TableId};
    use std::collections::BTreeMap;

    fn make_table_id() -> TableId {
        TableId {
            schema: "public".into(),
            name: "users".into(),
            oid: 0,
        }
    }

    fn make_event(
        op: ChangeOp,
        new: Option<BTreeMap<String, ColumnValue>>,
        old: Option<BTreeMap<String, ColumnValue>>,
    ) -> CdcEvent {
        CdcEvent {
            lsn: Lsn(100),
            timestamp_us: 1_000_000,
            xid: 1,
            table: make_table_id(),
            op,
            new,
            old,
            primary_key_columns: vec![],
        }
    }

    #[test]
    fn test_no_new_columns() {
        let known = vec!["id".into(), "name".into()];
        let event = make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            None,
        );
        assert!(detect_new_columns(&known, &[&event]).is_empty());
    }

    #[test]
    fn test_single_new_column() {
        let known = vec!["id".into(), "name".into()];
        let event = make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
                ("email".into(), ColumnValue::Text("a@b.com".into())),
            ])),
            None,
        );
        assert_eq!(detect_new_columns(&known, &[&event]), vec!["email"]);
    }

    #[test]
    fn test_multiple_new_columns_deduplicated() {
        let known = vec!["id".into()];
        let e1 = make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("email".into(), ColumnValue::Text("a@b.com".into())),
            ])),
            None,
        );
        let e2 = make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(2)),
                ("email".into(), ColumnValue::Text("b@c.com".into())),
                ("phone".into(), ColumnValue::Text("123".into())),
            ])),
            None,
        );
        let result = detect_new_columns(&known, &[&e1, &e2]);
        assert_eq!(result, vec!["email", "phone"]);
    }

    #[test]
    fn test_delete_inspects_old() {
        let known = vec!["id".into()];
        let event = make_event(
            ChangeOp::Delete,
            None,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("email".into(), ColumnValue::Text("a@b.com".into())),
            ])),
        );
        assert_eq!(detect_new_columns(&known, &[&event]), vec!["email"]);
    }

    #[test]
    fn test_truncate_skipped() {
        let known = vec!["id".into()];
        let event = make_event(ChangeOp::Truncate, None, None);
        assert!(detect_new_columns(&known, &[&event]).is_empty());
    }

    #[test]
    fn test_update_uses_new() {
        let known = vec!["id".into()];
        let event = make_event(
            ChangeOp::Update,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("email".into(), ColumnValue::Text("new@b.com".into())),
            ])),
            Some(BTreeMap::from([("id".into(), ColumnValue::Int(1))])),
        );
        assert_eq!(detect_new_columns(&known, &[&event]), vec!["email"]);
    }

    #[test]
    fn test_snapshot_uses_new() {
        let known = vec!["id".into()];
        let event = make_event(
            ChangeOp::Snapshot,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("extra".into(), ColumnValue::Text("val".into())),
            ])),
            None,
        );
        assert_eq!(detect_new_columns(&known, &[&event]), vec!["extra"]);
    }

    #[test]
    fn test_empty_known_columns() {
        let known: Vec<String> = vec![];
        let event = make_event(
            ChangeOp::Insert,
            Some(BTreeMap::from([
                ("id".into(), ColumnValue::Int(1)),
                ("name".into(), ColumnValue::Text("Alice".into())),
            ])),
            None,
        );
        let result = detect_new_columns(&known, &[&event]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"id".to_string()));
        assert!(result.contains(&"name".to_string()));
    }

    #[test]
    fn test_no_dropped_columns() {
        let target = vec!["id".into(), "name".into()];
        let source = vec!["id".into(), "name".into()];
        assert!(detect_dropped_columns(&target, &source).is_empty());
    }

    #[test]
    fn test_single_dropped_column() {
        let target = vec!["id".into(), "name".into(), "email".into()];
        let source = vec!["id".into(), "name".into()];
        assert_eq!(
            detect_dropped_columns(&target, &source),
            vec!["email".to_string()]
        );
    }

    #[test]
    fn test_multiple_dropped_columns() {
        let target = vec!["id".into(), "name".into(), "email".into(), "phone".into()];
        let source = vec!["id".into()];
        assert_eq!(
            detect_dropped_columns(&target, &source),
            vec!["name".to_string(), "email".to_string(), "phone".to_string()]
        );
    }

    #[test]
    fn test_dropped_columns_with_new_columns_in_source() {
        let target = vec!["id".into(), "old_col".into()];
        let source = vec!["id".into(), "new_col".into()];
        assert_eq!(
            detect_dropped_columns(&target, &source),
            vec!["old_col".to_string()]
        );
    }
}
